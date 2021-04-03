use std::{
    env,
    fs,
    path::{Path,PathBuf},
};
use crate::tasks::task_status::TaskStatus;
use crate::tasks::query::Query;

/// Name of the environment var that holds a path to the process directory
pub const PROCESS_DIR_ENV_NAME: &str = "PROCESS_DIR";
/// Prefix of the process directory
pub const PROCESS_OUTPUT_DIR_PREFIX: &str = "process-output-";
/// stdout file name
pub const PROCESS_STDOUT_FILE_NAME: &str = "stdout";
/// stderr file name
pub const PROCESS_STDERR_FILE_NAME: &str = "stderr";
/// status file name
pub const PROCESS_STATUS_FILE_NAME: &str = "return_status";
/// Unix socket: hypervisor <-> monitor
pub const IPC_SOCKET: &str = "monitor.sock";
/// CWD directory name
pub const PROCESS_CWD_DIR_NAME: &str = "cwd";


/// holds path related to a monitor process
#[derive(Debug)]
pub struct MonitorHandle {
    /// process directory
    pub directory: PathBuf,
}

impl MonitorHandle {

    /// Create an Handle from a task ID. 
    /// (creates the task directory)
    pub fn from_task_id(task_id: i32) -> Self {
        // create temporary file for stdout
        // fetch $PROCESS_DIR_ENV variable
        let processes_dir: String = env::var(PROCESS_DIR_ENV_NAME)
            .unwrap_or_else(|_| env::temp_dir().to_string_lossy().to_string());
        // create process directory
        let output_dir = Path::new(&processes_dir)
            .join(format!("{}{}", &PROCESS_OUTPUT_DIR_PREFIX, task_id));
        Self {
            directory: output_dir.to_path_buf(),
        }
    }

    /// file containing the error output of a monitored process
    pub fn stderr_file(&self) -> PathBuf {
        self.directory.join(&PROCESS_STDERR_FILE_NAME)
    }

    /// file containing the standard output of a monitored process
    pub fn stdout_file(&self) -> PathBuf {
        self.directory.join(&PROCESS_STDOUT_FILE_NAME)
    }

    /// file containing the return status of a terminated monitored process
    pub fn status_file(&self) -> PathBuf {
        self.directory.join(&PROCESS_STATUS_FILE_NAME)
    }

    /// Path to an Unix Domain Socket that can be used to reach
    /// the monitor process.
    pub fn monitor_socket(&self) -> PathBuf {
        self.directory.join(&IPC_SOCKET)
    }

    /// the directory into which the monitoree process started
    pub fn working_directory(&self) -> PathBuf {
        self.directory.join(&PROCESS_CWD_DIR_NAME)
    }

    /// Create MonitorHandle directories, and path as absolute.
    /// NOTE: This function is blocking.
    pub fn create_directory(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all(&self.directory)
            .map_err(|e| format!("Can't create handle directory: '{:?}': {:?}", &self.directory, e))?;
        fs::create_dir_all(&self.working_directory())
            .map_err(|e| format!("Can't create cwd: '{:?}': {:?}", &self.working_directory(), e))?;

        self.directory = self.directory.canonicalize()?;
        Ok(())
    }

    pub async fn kill() -> Result<(), Box<dyn std::error::Error>> {
        let query: Vec<u8> = Query::Kill.to_bytes()?;
        todo!()
    }

    pub async fn terminate() -> Result<(), Box<dyn std::error::Error>> {
        let query: Vec<u8> = Query::Terminate.to_bytes()?;
        todo!()
    }

    pub async fn get_status() -> Result<TaskStatus, Box<dyn std::error::Error>> {
        todo!()
    }
}
