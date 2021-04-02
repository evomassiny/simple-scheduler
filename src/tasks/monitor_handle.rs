use std::{
    env,
    fs,
    path::{Path,PathBuf},
};

/// Name of the environment var that holds a path to the process directory
pub const PROCESS_DIR_ENV_NAME: &str = "PROCESS_DIR";
/// Prefix of the process directory
pub const PROCESS_OUTPUT_DIR_PREFIX: &str = "process-output-";
/// stdout file name
pub const PROCESS_STDOUT_FILE_NAME: &str = "stdout";
/// stderr file name
pub const PROCESS_STDERR_FILE_NAME: &str = "stderr";
/// status file name
pub const PROCESS_STATUS_FILE_NAME: &str = "status";
/// Unix socket: hypervisor <-> monitor
pub const IPC_SOCKET: &str = "monitor.sock";
/// CWD directory name
pub const PROCESS_CWD_DIR_NAME: &str = "cwd";


/// holds path related to a monitor process
#[derive(Debug)]
pub struct MonitorHandle {
    /// process directory
    pub directory: PathBuf,
    /// file containing the standard output of a monitored process
    pub stdout: PathBuf,
    /// file containing the error output of a monitored process
    pub stderr: PathBuf,
    /// file containing the return status of a terminated monitored process
    pub status: PathBuf,
    /// the directory into which the monitoree process started
    pub cwd: PathBuf,
    /// Path to an Unix Domain Socket that can be used to reach
    /// the monitor process.
    pub monitor_socket: PathBuf,
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
        // create process CWD directory, the process will live inside
        let cwd = output_dir.join(&PROCESS_CWD_DIR_NAME);

        // create process stdout/stderr file name
        let stdout_path = output_dir.join(&PROCESS_STDOUT_FILE_NAME);
        let stderr_path = output_dir.join(&PROCESS_STDERR_FILE_NAME);
        let status_path = output_dir.join(&PROCESS_STATUS_FILE_NAME);
        let monitor_socket = output_dir.join(&IPC_SOCKET);
        Self {
            directory: output_dir.to_path_buf(),
            stdout: stdout_path,
            stderr: stderr_path,
            status: status_path,
            cwd,
            monitor_socket,
        }
    }

    /// Create MonitorHandle directories, and path as absolute.
    /// NOTE: This function is blocking.
    pub fn create_directory(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all(&self.directory)
            .map_err(|e| format!("Can't create handle directory: '{:?}': {:?}", &self.directory, e))?;
        fs::create_dir_all(&self.cwd)
            .map_err(|e| format!("Can't create handle directory: '{:?}': {:?}", &self.cwd, e))?;

        self.directory = self.directory.canonicalize()?;
        self.cwd = self.cwd.canonicalize()?;

        self.stdout = self.directory.join(&PROCESS_STDOUT_FILE_NAME);
        self.stderr = self.directory.join(&PROCESS_STDERR_FILE_NAME);
        self.status = self.directory.join(&PROCESS_STATUS_FILE_NAME);
        self.monitor_socket = self.directory.join(&IPC_SOCKET);

        Ok(())
    }
}
