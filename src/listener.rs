use rocket::tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
use crate::tasks::{TaskStatus, StatusUpdateMsg};
use crate::models::{Status,Task};
use sqlx::sqlite::SqlitePool;
use std::path::PathBuf;
use std::error::Error;



/// Read a task status update from a monitor process through `stream`.
async fn process_task_status_update(stream: &mut UnixStream, pool: &SqlitePool) -> Result<(), Box<dyn Error>> {
    let msg = StatusUpdateMsg::async_read_from(stream).await?;
    let task_handle: String = msg.task_handle
        .into_os_string()
        .to_string_lossy()
        .to_string();
    let mut conn = pool.acquire().await?;
    let _ = Task::select_by_handle_and_set_status(&task_handle, &msg.status, &mut conn)
        .await
        .map_err(|e| format!("update error: {:?}", e))?;
    Ok(())
}

/// Listen on a unix domain socket for monitors status update messages.
/// Updates the Task status.
/// 
/// NOTE: Failure are only logged, this loop should live as long as the server.
pub async fn listen_for_status_update(pool: SqlitePool, socket: PathBuf)  {
    // remove the socket file
    let _ = std::fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket).expect("Can bind to hypervisor socket.");
    loop {
        match listener.accept().await {
            Ok((mut stream, _addr)) => {
                if let Err(e) = process_task_status_update(&mut stream, &pool).await {
                    eprintln!("Error while processing update msg: {:?}", e);
                }
            }
            Err(e) => { 
                eprintln!(
                    "Connection to hypervisor socket '{:?}' failed: {:?}",
                    &socket,
                    e,
                );
            }
        }
    }
    //Ok(())
}
