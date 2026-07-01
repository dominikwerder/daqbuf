use std::fmt;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tracing::Event;
use tracing::Subscriber;
use tracing_log::NormalizeEvent;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormatEvent;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::registry::LookupSpan;

fn _dummyyyy() {
    let _ = tracing_subscriber::fmt::format::Full;
}

pub struct FormatTxt;

impl<S, N> FormatEvent<S, N> for FormatTxt
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(&self, ctx: &FmtContext<'_, S, N>, mut writer: Writer<'_>, event: &Event<'_>) -> fmt::Result {
        let normalized_meta = event.normalized_metadata();
        let meta = normalized_meta.as_ref().unwrap_or_else(|| event.metadata());
        // Without tracing-log:
        // let meta = event.metadata();
        // write!(w, "{}", datetime::DateTime::from(std::time::SystemTime::now()));
        // write!(writer, "{} ", FmtLevel::new(meta.level()))?;
        // Using crate `time` doing `DateTime<somehow-utc>.format_into(..)`

        // tracing_subscriber::fmt::time::datetime is private:
        // tracing_subscriber::fmt::time::datetime::DateTime::from(std::time::SystemTime::now());

        if false {
            // TODO restrict to milliseconds.
            // TODO there must be a better way than via cursor?
            let tsnow = OffsetDateTime::now_utc();
            let buf = [0u8; 64];
            let mut cr = std::io::Cursor::new(buf);
            let n = tsnow.format_into(&mut cr, &Rfc3339).unwrap();
            let buf = cr.into_inner();
            writer.write_str(std::str::from_utf8(&buf[..n]).unwrap())?;
            // writer.write_char(' ')?;
        }

        if true {
            const DATETIME_FMT_3MS: &str = "%Y-%m-%dT%H:%M:%S.%3fZ";
            let ts = chrono::Utc::now();
            let tsfmt = ts.format(DATETIME_FMT_3MS);
            writer.write_str(&tsfmt.to_string())?;
            // writer.write_char(' ')?;
        }

        write!(writer, " {:>5} ", meta.level().as_str())?;

        if false {
            writer.write_str("[THR ")?;
            let current_thread = std::thread::current();
            match current_thread.name() {
                Some(name) => {
                    let n = name.len();
                    let max = 32;
                    if n > max {
                        let pre = 3;
                        writer.write_str(&name[0..3])?;
                        writer.write_char('.')?;
                        writer.write_str(&name[name.len() + 1 + pre - max..])?;
                    } else {
                        writer.write_str(name)?;
                    }
                }
                None => {
                    // write!(writer, "{:0>2?} ", current_thread.id())?;
                    write!(writer, "{:?} ", current_thread.id())?;
                }
            }
            writer.write_char(' ')?;
        }

        {
            writer.write_str("[TGT ")?;
            writer.write_str(meta.target())?;
            writer.write_char(' ')?;
        }

        writer.write_str("[SCP ")?;
        if let Some(sc) = ctx.event_scope() {
            for (i, span) in sc.from_root().enumerate() {
                if i != 0 {
                    writer.write_char(',')?;
                }
                let meta = span.metadata();
                writer.write_str(meta.name())?;
                let ext = span.extensions();
                if let Some(fields) = ext.get::<FormattedFields<N>>() {
                    if fields.is_empty() {
                    } else {
                        writer.write_char('{')?;
                        writer.write_str(fields)?;
                        // write!(writer, "{{{}}}", fields)?;
                        writer.write_char('}')?;
                    }
                }
            }
        }
        writer.write_char(' ')?;

        if false {
            writer.write_str("[FIL ")?;
            if let Some(x) = meta.file() {
                writer.write_str(x)?;
                if let Some(x) = meta.line() {
                    write!(writer, ":{x}")?;
                }
            }
            writer.write_char(' ')?;
        }

        writer.write_str("[MSG ")?;
        ctx.format_fields(writer.by_ref(), event)?;

        writer.write_char('\n')?;
        Ok(())
    }
}
