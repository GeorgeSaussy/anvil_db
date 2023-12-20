use std::fs::File;
use std::io::Read;

// TODO(t/1442): Add support for other OSes.

/// This trait is used to define the interface for the OS specific functions.
/// Ideally, the only work required to support a new OS is to implement this
/// trait for that OS and recompile.
pub(crate) trait OsInterface {
    fn free_ram(&self) -> Result<usize, String>;
}

pub(crate) struct LinuxInterface;

impl OsInterface for LinuxInterface {
    fn free_ram(&self) -> Result<usize, String> {
        let mut buf = [0u8; 1024];
        let mut file = File::open("/proc/meminfo").map_err(|e| e.to_string())?;
        file.read_exact(&mut buf).map_err(|e| e.to_string())?;
        let meminfo = String::from_utf8(buf.to_vec()).map_err(|e| e.to_string())?;
        let mut lines = meminfo.lines();
        let _ = lines
            .next()
            .ok_or_else(|| "could not read first line".to_string())?;
        let line = lines
            .next()
            .ok_or_else(|| "could not read free memory line".to_string())?;
        let mut parts = line.split_whitespace();
        let _name = parts
            .next()
            .ok_or_else(|| "(2) meminfo file is empty".to_string())?;
        let value = parts
            .next()
            .ok_or_else(|| "(3) meminfo file is empty".to_string())?;
        let value = value.parse::<usize>().map_err(|e| e.to_string())?;
        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_linux_free_ram() {
        let os_client = LinuxInterface;
        let free_ram = os_client.free_ram().unwrap();
        assert!(free_ram > 0);
    }
}
