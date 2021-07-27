use crate::tasks::handle::TaskHandle;
use crate::tasks::pipe::Fence;
use crate::tasks::query::{Query, Sendable};
use crate::tasks::task_status::{StatusUpdateMsg,TaskStatus};
use nix::{
    sys::{
        epoll::{epoll_create, epoll_ctl, epoll_wait, EpollEvent, EpollFlags, EpollOp},
        signal::{kill, SigSet, Signal},
        signalfd::{SfdFlags, SignalFd},
    },
    unistd::{close, Pid},
};
use std::{
    collections::HashMap,
    convert::TryInto,
    os::unix::{
        io::{AsRawFd, RawFd},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
};

/// Epoll will wait forever (unless an event happens) if this timeout value is provided
const WAIT_FOREVER_TIMEOUT: isize = -1;

pub struct Monitor {
    pub task_fence: Option<Fence>,
    pub spawn_fence: Option<Fence>,
    pub task: Pid,
    pub status: TaskStatus,
    pub handle: TaskHandle,
    pub hypervisor_socket: Option<PathBuf>,
}

impl Monitor {
    /// start task process (release blocking Fence).
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(rel) = self.task_fence.take() {
            rel.release_waiter()?;
            self.status = TaskStatus::Running;
        }
        Ok(())
    }

    /// Send SIGKILL signal to task process
    fn kill(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        kill(self.task, Some(Signal::SIGKILL))
            .map_err(|e| format!("Can't kill task: {:?}", e).into())
    }

    /// Send SIGTERM signal to task process
    fn terminate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        kill(self.task, Some(Signal::SIGTERM))
            .map_err(|e| format!("Can't terminate task: {:?}", e).into())
    }

    /// Send `self.status` into stream (blocking).
    fn send_status(&self, stream: &mut UnixStream) -> Result<(), Box<dyn std::error::Error>> {
        println!("Sending status");
        let _ = self.status.send_to(stream)?;
        stream.shutdown(std::net::Shutdown::Write)?;
        Ok(())
    }

    fn process_query(
        &mut self,
        query: Query,
        stream: &mut UnixStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match query {
            Query::Start => self.start()?,
            Query::Kill => self.kill()?,
            Query::Terminate => self.terminate()?,
            Query::GetStatus => self.send_status(stream)?,
            Query::SetHypervisorSocket(sock) => self.hypervisor_socket = sock,
        }
        Ok(())
    }

    /// connect to the hypervisor socket and send a status update
    fn notify_hypervisor(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref hypervisor_socket) = self.hypervisor_socket {
            let mut stream = UnixStream::connect(hypervisor_socket)?;
            let msg = StatusUpdateMsg {
                task_handle: self.handle.directory.clone(),
                status: self.status.clone(),
            };
            let _ = msg.send_to(&mut stream)?;
            stream.shutdown(std::net::Shutdown::Write)?;
        }
        Ok(())
    }

    /// This is the main monitor loop.
    ///
    /// It runs in a single thread while the process is running,
    /// it listens on a Unix Domain Socket for commands, and quits when the process is terminated.
    ///
    /// In details, it:
    /// * creates an Unix Domain socket to listen for hypervisor commands (`handle.monitor_socket()`),
    /// * poll for 2 kind of event: the child termination or connection to the socket
    /// * when a connection happens, polls the resulting stream as well
    /// * read data from the stream and interprets it as a command.
    /// * write reponse data into the same stream.
    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // create the Unix socket
        let listener = UnixListener::bind(&self.handle.monitor_socket()).map_err(|e| {
            format!(
                "Could not create monitor socket: '{:?}'. '{:?}'",
                &self.handle.monitor_socket(),
                e
            )
        })?;
        listener.set_nonblocking(true)?;
        let listener_fd: RawFd = listener.as_raw_fd();

        // open SIG_CHLD as a RawFd, so we can poll it
        let mut mask = SigSet::empty();
        mask.add(Signal::SIGCHLD);
        let mut sigchild_reader = SignalFd::with_flags(&mask, SfdFlags::SFD_NONBLOCK)?;
        let sigchild_fd: RawFd = sigchild_reader.as_raw_fd();

        // setup epoll
        let epoll_fd: RawFd = epoll_create()?;

        // submit SIGCHLD fd and socket fd to epoll
        let mut sigchild_event = EpollEvent::new(EpollFlags::EPOLLIN, sigchild_fd.try_into()?);
        epoll_ctl(
            epoll_fd,
            EpollOp::EpollCtlAdd,
            sigchild_fd,
            Some(&mut sigchild_event),
        )?;
        let mut listener_event = EpollEvent::new(EpollFlags::EPOLLIN, listener_fd.try_into()?);
        epoll_ctl(
            epoll_fd,
            EpollOp::EpollCtlAdd,
            listener_fd,
            Some(&mut listener_event),
        )?;

        // create a empty event array, that we will feed to epoll_wait()
        let mut events: Vec<EpollEvent> = (0..5).map(|_| EpollEvent::empty()).collect();

        // stores connection streams
        let mut streams_by_fd: HashMap<RawFd, UnixStream> = HashMap::new();
        let mut streams = listener.incoming();

        // release the spawner process
        if let Some(fence) = self.spawn_fence.take() {
            fence.release_waiter()?;
        }

        // start the event loop:
        'event_loop: while let Ok(event_count) =
            epoll_wait(epoll_fd, &mut events, WAIT_FOREVER_TIMEOUT) // wait for event completion
        {
            for event in events.iter().take(event_count) {
                // fetch the data we've associated with the event (file descriptors)
                let fd: RawFd = event.data().try_into()?;
                match fd {
                    // the task has terminated
                    fd if fd == sigchild_fd => {
                        self.status = match sigchild_reader.read_signal() {
                            Ok(Some(siginfo)) => {
                                TaskStatus::from_siginfo(&siginfo).unwrap_or(TaskStatus::Failed)
                            }
                            _ => TaskStatus::Failed, // assume failure, if nothing is returned
                        };
                        if self.status.is_terminated() {
                            break 'event_loop;
                        }
                    }
                    // a client connected to the socket
                    fd if fd == listener_fd => {
                        // won't block (epolled)
                        let stream = streams
                            .next()
                            .ok_or_else(|| "No stream available (unreachable)".to_string())??;
                        // register the stream to the epoll_fd
                        let stream_fd: RawFd = stream.as_raw_fd();
                        let mut event = EpollEvent::new(EpollFlags::EPOLLIN, stream_fd.try_into()?);
                        epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, stream_fd, Some(&mut event))?;
                        // store the stream
                        streams_by_fd.insert(stream_fd, stream);
                    }
                    // data is ready to read on one of the connection
                    stream_fd => {
                        // fetch the stream for the fd, and process the request
                        let mut stream = streams_by_fd
                            .remove(&stream_fd)
                            .ok_or_else(|| "No stream using this RawFD (unreachable)".to_string())?;
                        // unregister the stream from the epoll
                        epoll_ctl(epoll_fd, EpollOp::EpollCtlDel, stream_fd, None)?;
                        // read queries from the stream, ignore failures
                        match Query::read_from(&mut stream) {
                            Ok(query) => {
                                dbg!(&query);
                                self.process_query(query, &mut stream)?;
                            }
                            Err(e) => eprintln!("bad query: {:?}", e),
                        }
                    }
                }
            }
        }
        // Close all opened RawFds
        close(epoll_fd)?;
        // write status to file
        self.status.save_to_file(&self.handle.status_file())?;
        if let Err(error) = self.notify_hypervisor() {
            eprintln!(
                "Failed to send status to hypervisor through socket: '{:?}': '{:?}'",
                self.hypervisor_socket, error
            );
        }
        // remove the socket file
        std::fs::remove_file(&self.handle.monitor_socket())?;
        // remove the pid file
        std::fs::remove_file(&self.handle.pid_file())?;

        Ok(())
    }
}
