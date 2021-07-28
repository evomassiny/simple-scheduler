use rocket::tokio::{
    net::{UnixListener, UnixStream},
    self,
};
use crate::tasks::StatusUpdateMsg;
use crate::models::Task;
use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;
use std::error::Error;

pub use crate::scheduling::client::SchedulerClient;

pub struct SchedulerServer {
    socket: PathBuf,
    pool: SqlitePool,

}
impl SchedulerServer {

    pub fn new(socket: PathBuf, pool: SqlitePool) -> Self {
        Self { socket, pool }
    }

    /// Read a task status update from a monitor process through `stream`.
    async fn process_task_status_update(&self, stream: &mut UnixStream) -> Result<(), Box<dyn Error>> {
        let msg = StatusUpdateMsg::async_read_from(stream).await?;
        let task_handle: String = msg.task_handle
            .into_os_string()
            .to_string_lossy()
            .to_string();
        let mut conn = self.pool.acquire().await?;
        let _ = Task::select_by_handle_and_set_status(&task_handle, &msg.status, &mut conn)
            .await
            .map_err(|e| format!("update error: {:?}", e))?;
        Ok(())
    }

    pub fn client(&self) -> SchedulerClient {
        SchedulerClient { 
            socket: self.socket.clone(),
            pool: self.pool.clone(),
        }
    }

    /// Listen on a unix domain socket for monitors status update messages.
    /// Updates the Task status.
    /// 
    /// NOTE: Failure are only logged, this loop should live as long as the server.
    pub fn start(self) {
        tokio::task::spawn(async move {
            // remove the socket file
            let _ = std::fs::remove_file(&self.socket);
            let listener = UnixListener::bind(&self.socket).expect("Can bind to hypervisor socket.");

            loop {
                match listener.accept().await {
                    Ok((mut stream, _addr)) => {
                        if let Err(e) = self.process_task_status_update(&mut stream).await {
                            eprintln!("Error while processing update msg: {:?}", e);
                        }
                    }
                    Err(e) => { 
                        eprintln!(
                            "Connection to hypervisor socket '{:?}' failed: {:?}",
                            &self.socket,
                            e,
                        );
                    }
                }
            }
        });
    }

}
