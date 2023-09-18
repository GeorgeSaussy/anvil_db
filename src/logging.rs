use std::{
    fmt::{Display, Formatter},
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) enum LogLevel {
    Info,
    Error,
    #[cfg(test)]
    Debug,
}

impl Display for LogLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Error => write!(f, "ERROR"),
            #[cfg(test)]
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
    #[cfg(test)]
    fn debug(&self, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        self.log(LogLevel::Debug, file_name, line_no, column_no, msg)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct DefaultLogger {}

impl Logger for DefaultLogger {
    fn log(&self, level: LogLevel, file_name: &str, line_no: u32, column_no: u32, msg: &str) {
        let level_str = level.to_string();
        let prefix = format!(
            "[{}]\t{} {}:{}:{} ",
            level_str,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            file_name,
            line_no,
            column_no,
        );
        let spaces = prefix
            .chars()
            .map(|c| if c == '\t' { c } else { ' ' })
            .collect::<String>();
        let formatted_lines = msg.lines().enumerate().map(|(i, line)| {
            if i == 0 {
                format!("{}{}", prefix, line)
            } else {
                format!("{}{}", spaces, line)
            }
        });
        let formatted_message = formatted_lines.collect::<Vec<_>>().join("\n");
        println!("{}", formatted_message);
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

#[cfg(test)]
macro_rules! debug {
    ($($arg:tt)*) => {
        crate::logging::Logger::debug(&crate::logging::DefaultLogger::default(), file!(), line!(), column!(), &format!($($arg)*));
    };
}
#[cfg(test)]
pub(crate) use debug;
