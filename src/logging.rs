//! Rolling log file writer with two rotation strategies:
//!
//! - `DEBUG` or `WARN` level: rename to `<name>.1` and start a fresh file
//!   (single rolling backup, can rotate repeatedly without bound).
//! - Any other level: truncate in place and write a `--- log trimmed ---`
//!   marker so readers know output was discarded.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;

const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024; // 10 MB

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RotationMode {
    /// Rename to `.1` (overwriting any previous backup) and start fresh.
    Roll,
    /// Truncate in place and write a trim marker.
    Trim,
}

pub struct RollingWriter {
    path: PathBuf,
    file: File,
    written: u64,
    mode: RotationMode,
}

impl RollingWriter {
    fn open(path: PathBuf, mode: RotationMode) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            path,
            file,
            written,
            mode,
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        match self.mode {
            RotationMode::Roll => {
                let name = self
                    .path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();
                let backup = self.path.with_file_name(format!("{name}.1"));
                let _ = fs::rename(&self.path, &backup);
                self.file = File::create(&self.path)?;
            }
            RotationMode::Trim => {
                self.file = File::create(&self.path)?;
                let _ = writeln!(self.file, "--- log trimmed ---");
            }
        }
        self.written = 0;
        Ok(())
    }
}

impl Write for RollingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.written >= MAX_LOG_BYTES {
            if let Err(err) = self.rotate() {
                let _ = writeln!(self.file, "[log rotation failed: {err}]");
            }
        }
        let n = self.file.write(buf)?;
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// `MakeWriter` impl: clones the `Arc` on each call so every log event gets
/// its own short-lived lock guard, which is then written to and dropped.
#[derive(Clone)]
struct LogWriter(std::sync::Arc<std::sync::Mutex<RollingWriter>>);

impl io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log writer poisoned").write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().expect("log writer poisoned").flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogWriter {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Resolve the rotation mode from the effective log level string.
/// DEBUG and WARN keep a rolling backup; everything else trims in place.
fn rotation_mode(env_filter: &str) -> RotationMode {
    let level = env_filter.trim().to_lowercase();
    if level == "debug" || level == "warn" {
        RotationMode::Roll
    } else {
        RotationMode::Trim
    }
}

/// Initialise `tracing_subscriber` to write to a log file. Must be called
/// at most once.
pub fn init_file_logging(path: PathBuf) -> io::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let mode = rotation_mode(&filter.to_string());
    let writer = LogWriter(std::sync::Arc::new(std::sync::Mutex::new(
        RollingWriter::open(path, mode)?,
    )));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_level(true)
        .with_target(false)
        .with_ansi(false)
        .with_writer(writer)
        .init();
    Ok(())
}
