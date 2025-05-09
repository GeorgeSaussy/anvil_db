use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) enum LogLevel {
    Info,
    Error,
    #[allow(dead_code)]
    Debug,
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Error => write!(f, "ERROR"),
            LogLevel::Debug => write!(f, "DEBUG"),
        }
    }
}

pub(crate) trait Logger: Clone + Send + Sync + 'static {
    fn log(&self, level: LogLevel, file_name: &str, line_no: u32, column_no: u32, msg: &str);
    fn info(&self, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        self.log(LogLevel::Info, file_name, line_no, column_no, msg)
    }
    fn error(&self, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        self.log(LogLevel::Error, file_name, line_no, column_no, msg)
    }
    #[allow(dead_code)]
    fn debug(&self, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        self.log(LogLevel::Debug, file_name, line_no, column_no, msg)
    }
}

fn default_prefix(level: LogLevel, file_name: &str, line_no: u32, column_no: u32) -> String {
    let level_str = level.to_string();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("[{level_str}]\t{now} {file_name}:{line_no}:{column_no}")
}

fn prefix_lines(prefix: &str, msg: &str) -> String {
    let spaces = prefix
        .chars()
        .map(|c| if c == '\t' { c } else { ' ' })
        .collect::<String>();
    let formatted_lines = msg.lines().enumerate().map(|(i, line)| {
        if i == 0 {
            format!("{prefix} {line}",)
        } else {
            format!("{spaces} {line}",)
        }
    });
    formatted_lines.collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
impl Logger for () {
    fn log(&self, level: LogLevel, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        let prefix_1 = default_prefix(level, file_name, line_no, column_no);
        let prefix = format!("{prefix_1} (null-logger)");
        let formatted_message = prefix_lines(&prefix, msg);
        println!("{formatted_message}",);
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DefaultLogger {}

impl Logger for DefaultLogger {
    fn log(&self, level: LogLevel, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        let prefix = default_prefix(level, file_name, line_no, column_no);
        let formatted_message = prefix_lines(&prefix, msg);
        println!("{formatted_message}",);
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ArchiveLogger {
    name: String,
    clone_id: usize,
    history: Arc<RwLock<Vec<String>>>,
}

impl ArchiveLogger {
    #[allow(dead_code)]
    pub(crate) fn new(name: &str) -> Self {
        ArchiveLogger {
            name: name.to_string(),
            clone_id: 0,
            history: Arc::new(RwLock::new(Vec::new())),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn history_ref(&self) -> Vec<String> {
        self.history.read().unwrap().clone()
    }
}

unsafe impl Send for ArchiveLogger {}
unsafe impl Sync for ArchiveLogger {}

impl Clone for ArchiveLogger {
    fn clone(&self) -> Self {
        ArchiveLogger {
            name: self.name.clone(),
            clone_id: self.clone_id + 1,
            history: self.history.clone(),
        }
    }
}

impl Logger for ArchiveLogger {
    fn log(&self, level: LogLevel, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        let prefix_1 = default_prefix(level, file_name, line_no, column_no);
        let prefix = format!(
            "{prefix_1} {name}-{clone_id} ",
            name = self.name,
            clone_id = self.clone_id
        );
        let formatted_message = prefix_lines(&prefix, msg);
        self.history
            .write()
            .unwrap()
            .push(formatted_message.clone());
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct FixPrefixLogger {
    prefix: String,
}

unsafe impl Send for FixPrefixLogger {}
unsafe impl Sync for FixPrefixLogger {}

impl FixPrefixLogger {
    #[allow(dead_code)]
    pub(crate) fn new(prefix: &str) -> Self {
        FixPrefixLogger {
            prefix: prefix.to_string(),
        }
    }
}

impl Logger for FixPrefixLogger {
    fn log(&self, _level: LogLevel, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        let prefix_1 = format!("{file_name}:{line_no}:{column_no}");
        let prefix = if self.prefix.is_empty() {
            format!("{prefix_1}:")
        } else {
            format!("{} {}:", prefix_1, self.prefix)
        };
        let formatted_message = prefix_lines(&prefix, msg);
        println!("{formatted_message}",);
    }
}

macro_rules! info {
    ($logger:expr, $($arg:tt)*) => {
        crate::logging::Logger::info($logger, file!(), line!(), column!(), &format!($($arg)*));
    };
}
pub(crate) use info;

macro_rules! error {
    ($logger:expr, $($arg:tt)*) => {
        crate::logging::Logger::error($logger, file!(), line!(), column!(), &format!($($arg)*));
    };
}
pub(crate) use error;

#[allow(unused_macros)]
macro_rules! debug {
    ($logger:expr, $($arg:tt)*) => {
        #[cfg(test)]
        crate::logging::Logger::debug($logger, file!(), line!(), column!(), &format!($($arg)*));
    };
}
#[cfg(test)]
pub(crate) use debug;
