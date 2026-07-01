# Binned Data

Binned data can be fetched this way:

```bash
curl "https://data-api.psi.ch/api/4/binned?backend=sf-databuffer&channelName=S10BC01-DBPM010:Q1&begDate=2024-02-15T00:00:00Z&endDate=2024-02-15T12:00:00Z&binWidth=5m"
```

Parameters:
- `backend`: the backend that the channel exists in, e.g. `sf-databuffer`.
- `channelName`: the name of the channel.
- `seriesId`: instead of a channel name, can specify the (unique within backend) series id
  as returned by channel search.
  Note that it is an error to provide both `channelName` and `seriesId`.
- `begDate`: start of the time range, inclusive. In ISO format e.g. `2024-02-15T12:41:00Z`.
- `endDate`: end of the time range, exclusive.
- `binWidth`: requested width of the bins, given with a unit suffix e.g. `10s` for 10 seconds, `2m` for 2 minutes,
  `1h` for 1 hour.
- `binCount`: requested number of bins, can not be combined with `binWidth`.
- `contentTimeout`: return the so-far computed results after the given timeout.
  When streaming content was requested (e.g. `json-framed`) the response will yield results in `contentTimeout` intervals.
- `allowLargeResult=true` **DEPRECATED, will be rejected in the future**
  indicates that the client is prepared to accept also larger responses compared to
  what might be suitable for a typical browser. Please download large result sets as
  framed json or framed cbor streams, see below.

This returns for each bin the average, minimum, maximum and count of events.

Note: it is an error to specify both `binWidth` and `binCount`.
The server may return more than `binCount` bins, and it will choose a `binWidth` from a set of
supported widths to best possibly match the requested width.

If the service was not able to complete a single bin within the timeout it returns HTTP 504.

For each bin both edges are returned (ts1 and ts2) in order to maybe support sparse and varying-width
binned responses in the future.

The json response contains timestamps in the form `TIMESTAMP = ANCHOR + MILLIS + NANOS`.

Example response, truncated to 4 bins:
```json
{
  "tsAnchor": 1726394700,
  "ts1Ms": [
    0,
    300000,
    600000,
    900000
  ],
  "ts2Ms": [
    300000,
    600000,
    900000,
    1200000
  ],
  "ts1Ns": [
    0,
    0,
    0,
    0
  ],
  "ts2Ns": [
    0,
    0,
    0,
    0
  ],
  "avgs": [
    0.010802877135574818,
    0.010565019212663174,
    0.01061472948640585,
    0.010656529106199741
  ],
  "counts": [
    3000,
    2999,
    3000,
    3000
  ],
  "maxs": [
    0.017492100596427917,
    0.016716860234737396,
    0.01769270747900009,
    0.01670699194073677
  ],
  "mins": [
    0.005227561108767986,
    0.0040797283872962,
    0.004329073242843151,
    0.004934651777148247
  ]
}
```

Note: the fields `ts1Ns` and `ts2Ns` may be omitted in the response if they are all zero.


## As framed JSON stream

To download larger amounts data as JSON it is recommended to use the `json-framed` content encoding.
Using this encoding, the server can send the requested events as a stream of json objects, where each
json object contains a batch of bins.
This content encoding is triggered via the `Accept: application/json-framed` header in the request.

The returned body looks like:
```
[JSON-frame]
[JSON-frame]
[JSON-frame]
... etc
```

where each `[JSON-frame]` looks like:
```
[number of bytes N of the following json-encoded data, as ASCII-encoded number]
[newline]
[JSON object: N bytes]
[newline]
```

Note: "data" objects are currently identified by the presence of the `ts1s` key.
There can be other types of objects, like keepalive, log messages or request metrics.
