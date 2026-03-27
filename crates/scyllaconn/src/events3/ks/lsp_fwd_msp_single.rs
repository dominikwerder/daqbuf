/*
IMPLEMENTATION PLAN
===================

Goal: A Stream that yields all LspEv timestamps for a given (ks, series_info, msp) within a
time range, in batches. Analog to `events3::mspfwd::ReadMspFwdStream` (which pages over MSP
timestamps), but operating one level down: over LSP timestamps within a single MSP bucket.

This type is the building block consumed by `ks::lsp_fwd_msp_multi::FwdMspMerged`.



BEGIN OF PLAN TO CHANGE:

Please use `async fn scyllaconn::worker::ScyllaQueueCluster::read_03_lsp_fwd` to implement the actual reading.
That probably means that you can skip the Part 2 "worker integration" ?
In the end, we want to deliver full events to the user including the value, not just timestamps.
So, using `scyllaconn::worker::ScyllaQueueCluster::read_03_lsp_fwd` makes sense.

Please ignore FwdMspMerged and remove it the current plan. We will work on that later, but focus now on the type in this file.

The type in this file should simply stream events for the given msp.
The queries will always return the lsp in order.
So there is no need to merge or sort in here I think.
But, the queries take a row limit parameter.
In the type in this file, we must therefore query repeatedly.
We must use the timestamp of the latest event from the current batch, and for the next query, ask for tslast + 1ns.
We do not need a begexcl flag in here.

END OF PLAN TO CHANGE.





── Data types ──────────────────────────────────────────────────────────────────────────────────

pub type Item = Result<VecDeque<LspEv>, Error>;

Inputs:
  ks: KeyspaceId
  series_info: SeriesInfo          — needed for shape (scalar/array) + scalar type to select
                                     the right prepared statement table
  msp: MspEv                       — the specific millisecond partition being scanned
  range: ScyllaSeriesRange         — absolute TsNano range [beg, end)
  begexcl: RangeExcl               — whether range.beg is exclusive (for cursor pagination)
  limit: u32                       — max LSPs per batch (pagination window)


── Part 1: Read03LspFwdSingle  (one-shot query, channel-based) ──────────────────────────────

Mirrors `events3::mspfwd::ReadMsp03Fwd`.

Fields:
  ks, series_info, msp, range, begexcl, limit, tx: async_channel::Sender<Item>

Constructor `new(...)` returns (Self, Receiver<Item>).

`exec(self, stmts: &StmtsEventsQueryOpts, scy: &Session)`:
  calls exec_inner and sends result to tx.

`exec_inner(...)`:
  1. Compute lsp_min: `msp.lsp(range.beg()).unwrap_or(LspEv::from_i64(0))`
     If begexcl.excl_beg() is true, add 1 to lsp_min (advance past the last-seen value),
     effectively: `LspEv::from_i64(lsp_min.to_i64() + 1)`.
  2. Compute lsp_max: `msp.lsp(range.end()).unwrap_or(LspEv::max())`
     If range.end() < msp.to_ts(LspEv::from_i64(0)), return Ok(VecDeque::new()) early.
  3. Select the prepared statement:
       `stmts.lsp(false, false)  // forward, timestamps only`
            `.shape(series_info.shape().is_array())`
            `.st(series_info.scalar_type().to_scylla_table_name_id())?`
  4. Execute: `scy.execute_iter(stmt, params).await?.rows_stream::<(i64,)>()?`
     Params: `(series_info.id().to_i64(), msp.to_i64(), lsp_min.to_i64(), lsp_max.to_i64())`
     NOTE: The existing `lsp_fwd_ts` query is:
       SELECT ts_lsp FROM ... WHERE series=? AND ts_msp=? AND ts_lsp>=? AND ts_lsp<? {opts}
     There is no LIMIT in this statement. To implement batching, collect up to `self.limit`
     rows from the row stream and stop early (the remaining rows in the pager are discarded).
  5. Collect up to `self.limit` LspEv values into a VecDeque and return.

`exec_mock(self, cltag: &str, ks: KeyspaceId)`:
  Mirrors `ReadMsp03Fwd::exec_mock`. For SERIES_ID_A / "mock1" / RetentionTime::Short:
    Filter `test_data::produce_full_event_set().by_msp.get(&self.msp)` entries:
      - Compute lsp_min and lsp_max as above (same excl logic)
      - Retain only entries where lsp_min <= lsp < lsp_max
      - Take the first `self.limit` entries
      - Map each (_, lsp, _, _) → LspEv and collect into VecDeque

Error variants needed: Send, Recv, NoKs, ScyllaPagerExecution, ScyllaTypeCheck,
ScyllaNextRow, Prepare (for the .st() call).


── Part 2: Worker integration ───────────────────────────────────────────────────────────────

In `worker.rs`:

a) Add to the `Job` enum:
     Read03LspFwdSingle(crate::events3::ks::lsp_fwd_msp_single::Read03LspFwdSingle),

b) Add dispatch in the worker's job handler match arm (where Read03LspLst is dispatched),
   calling either `job.exec(stmts, &scy)` or `job.exec_mock(cltag, ks)`.

c) Add to `ScyllaQueueCluster`:
     pub async fn read_03_lsp_fwd_single(
         &self,
         ks: KeyspaceId,
         series_info: SeriesInfo,
         msp: MspEv,
         range: ScyllaSeriesRange,
         begexcl: RangeExcl,
         limit: Option<u32>,
     ) -> Item {
         let limit = limit.unwrap_or(40).max(1).min(200);
         let (job, rx) = Read03LspFwdSingle::new(ks.clone(), series_info, msp, range, begexcl, limit);
         self.tx.send((ks, Job::Read03LspFwdSingle(job))).await?;
         Ok(rx.recv().await??)
     }


── Part 3: LspFwdMspSingleStream  (paginating Stream) ───────────────────────────────────────

Mirrors `events3::mspfwd::ReadMspFwdStream`.

Fields:
  ks: KeyspaceId
  series_info: SeriesInfo
  msp: MspEv
  range: ScyllaSeriesRange         — cursor: beg advances after each batch
  begexcl: RangeExcl               — set to RangeExcl::Beg after first batch
  limit: u32
  fut: Option<Pin<Box<dyn Future<Output = Item> + Send>>>
  scyqu: ScyllaQueueCluster

Constructor `new(...)` clamps limit to [1, 200].

`trigger_done(&mut self)`: sets `self.range = ScyllaSeriesRange::new(self.range.end(), self.range.end())`

Stream::poll_next:
  Loop:
    Some(fut):
      Ready(Ok(v)):
        self.fut = None;
        if v.len() < self.limit as usize:
          self.trigger_done();          // received a partial batch → exhausted
        else:
          // Full batch: advance cursor past the last LspEv
          let last_lsp = *v.back().unwrap();
          let abs_ts = self.msp.to_ts(last_lsp);  // MspEv::to_ts(LspEv) → TsNano
          self.range = ScyllaSeriesRange::new(abs_ts, self.range.end());
          self.begexcl = RangeExcl::Beg;
        break Ready(Some(Ok(v)));
      Ready(Err(e)):
        self.fut = None;
        self.trigger_done();
        break Ready(Some(Err(e)));
      Pending: break Pending;
    None:
      if self.range.beg() < self.range.end():
        spawn future via scyqu.read_03_lsp_fwd_single(...)
        self.fut = Some(Box::pin(fut));
      else:
        break Ready(None);            // cursor has reached the end → stream is done
*/
