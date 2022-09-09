use crate::messaging::{ExecutorQuery, MonitorMsg, Sendable, TaskStatus};
use crate::models::TaskId;
use crate::tasks::handle::TaskHandle;
use crate::tasks::ipc::Barrier;
use nix::{
    libc,
    sys::{
        epoll::{epoll_create, epoll_ctl, epoll_wait, EpollEvent, EpollFlags, EpollOp},
        signal::{kill, SigSet, Signal},
        signalfd::{siginfo, SfdFlags, SignalFd},
    },
    unistd::{close, Pid},
};
use std::io::Read;
use std::{
    collections::HashMap,
    convert::TryInto,
    os::unix::{
        io::{AsRawFd, RawFd},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    time::Duration,
};

/// Epoll will wait forever (unless an event happens) if this timeout value is provided
const WAIT_FOREVER_TIMEOUT: isize = -1;

pub struct Monitor {
    pub task_barrier: Option<Barrier>,
    pub monitor_ready_barrier: Option<Barrier>,
    pub task_pid: Pid,
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub handle: TaskHandle,
    pub hypervisor_socket: Option<PathBuf>,
    pub update_message_count: usize,
}

impl Monitor {
    /// start task process (release blocking Fence).
    fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(rel) = self.task_barrier.take() {
            rel.release()?;
            self.status = TaskStatus::Running;
        }
        Ok(())
    }

    /// Send SIGKILL signal to task process
    fn kill(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        kill(self.task_pid, Some(Signal::SIGKILL))
            .map_err(|e| format!("Can't kill task: {:?}", e).into())
    }

    /// Send SIGTERM signal to task process
    fn terminate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        kill(self.task_pid, Some(Signal::SIGTERM))
            .map_err(|e| format!("Can't terminate task: {:?}", e).into())
    }

    /// Send `self.status` into stream (blocking).
    fn send_status(&self, stream: &mut UnixStream) -> Result<(), Box<dyn std::error::Error>> {
        println!("Sending status");
        let _ = self.status.send_to(stream)?;
        Ok(())
    }

    /// re-configure hypervisor socket address,
    /// and return an ACK to the sender.
    fn set_hypervisor_socket(
        &mut self,
        sock: PathBuf,
        stream: &mut UnixStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.hypervisor_socket = Some(sock);
        let msg = MonitorMsg::Ok;
        let _ = msg.send_to(stream)?;
        Ok(())
    }

    /// connect to the hypervisor socket and send a status update
    fn notify_hypervisor(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref hypervisor_socket) = self.hypervisor_socket {
            let msg = MonitorMsg::StatusBroadcast {
                task_handle: self.handle.directory.clone(),
                task_id: self.task_id,
                status: self.status,
                update_version: self.update_message_count,
            };
            self.update_message_count += 1;
            let hypervisor_socket = hypervisor_socket.clone();

            let mut stream = UnixStream::connect(&*hypervisor_socket)
                .map_err(|e| format!("Failed to connect to hypervisor {:?}", e))?;
            let _ = msg
                .send_to(&mut stream)
                .map_err(|e| format!("Failed to send status to hypervisor {:?}", e))?;
            // expect ACK from server
            match ExecutorQuery::read_from(&mut stream) {
                Ok(ExecutorQuery::Ok) => {
                    // here we are the client, so we should shutdown the connection
                    stream.shutdown(std::net::Shutdown::Both)?;
                }
                _ => eprintln!("Expecting ACK"),
            }
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
        let socket_listener_fd: RawFd = listener.as_raw_fd();

        // open SIG_CHLD as a RawFd, so we can poll it
        let mut mask = SigSet::empty();
        mask.add(Signal::SIGCHLD);
        let mut sigchild_reader = SignalFd::with_flags(&mask, SfdFlags::SFD_NONBLOCK)?;
        let sigchild_fd: RawFd = sigchild_reader.as_raw_fd();

        // setup epoll
        let epoll_fd: RawFd = epoll_create()?;

        // register SIGCHLD fd to epoll
        let mut sigchild_event = EpollEvent::new(EpollFlags::EPOLLIN, sigchild_fd.try_into()?);
        epoll_ctl(
            epoll_fd,
            EpollOp::EpollCtlAdd,
            sigchild_fd,
            Some(&mut sigchild_event),
        )?;
        // register socket listener fd to epoll
        let mut listener_event =
            EpollEvent::new(EpollFlags::EPOLLIN, socket_listener_fd.try_into()?);
        epoll_ctl(
            epoll_fd,
            EpollOp::EpollCtlAdd,
            socket_listener_fd,
            Some(&mut listener_event),
        )?;

        // create a empty event array, that we will feed to epoll_wait()
        let mut events: Vec<EpollEvent> = (0..5).map(|_| EpollEvent::empty()).collect();

        // stores connection streams
        let mut streams_by_fd: HashMap<RawFd, UnixStream> = HashMap::new();
        let mut streams = listener.incoming();

        // release the spawner process
        if let Some(barrier) = self.monitor_ready_barrier.take() {
            barrier.release()?;
        }

        // start the event loop:
        'wait_loop: while let Ok(event_count) =
            epoll_wait(epoll_fd, &mut events, WAIT_FOREVER_TIMEOUT)
        // wait for event completion
        {
            'process_loop: for event in events.iter().take(event_count) {
                // fetch the data we've associated with the event (file descriptors)
                let fd: RawFd = event.data().try_into()?;
                match fd {
                    // the child process has returned a status
                    fd if fd == sigchild_fd => {
                        if let Err(error) = self.process_child_status(&mut sigchild_reader) {
                            eprintln!("Failed to process SIGCHILD: {:?}", error);
                        }
                    }
                    // a client connected to the monitor socket
                    fd if fd == socket_listener_fd => {
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
                    // data is ready to be read on one of the connection
                    stream_fd => {
                        // fetch the stream for the fd, and process the request
                        let mut stream = streams_by_fd.remove(&stream_fd).ok_or_else(|| {
                            "No stream using this RawFD (unreachable)".to_string()
                        })?;
                        // unregister the stream from the epoll
                        epoll_ctl(epoll_fd, EpollOp::EpollCtlDel, stream_fd, None)?;
                        match self.process_incoming_request(&mut stream) {
                            Ok(true) => {
                                // Cleanup whole process directory.
                                let _ = std::fs::remove_dir_all(&self.handle.directory);
                                break 'wait_loop;
                            }
                            Ok(false) => continue 'process_loop,
                            Err(error) => eprintln!("Could not process query {:?}", error),
                        }
                    }
                }
            }
        }
        // Close all opened RawFds
        close(epoll_fd)?;
        Ok(())
    }

    /// Process an incoming request.
    /// (expects a `ExecutorQuery` object sent through `stream`).
    ///
    /// Return True, if the request ask for termination.
    fn process_incoming_request(
        &mut self,
        stream: &mut UnixStream,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // read queries from the stream, ignore failures
        let query = ExecutorQuery::read_from(&mut *stream)?;

        let mut must_quit: bool = false;
        match query {
            ExecutorQuery::Start => self.start()?,
            ExecutorQuery::Kill => self.kill()?,
            ExecutorQuery::Terminate => self.terminate()?,
            ExecutorQuery::GetStatus => self.send_status(stream)?,
            ExecutorQuery::SetHypervisorSocket(sock) => self.set_hypervisor_socket(sock, stream)?,
            ExecutorQuery::TerminateMonitor => {
                let msg = MonitorMsg::Ok;
                let _ = msg.send_to(stream)?;
                must_quit = true;
            }
            ExecutorQuery::RequestStatusNotification => {
                self.notify_hypervisor()?;
                let msg = MonitorMsg::Ok;
                let _ = msg.send_to(stream)?;
            }
            ExecutorQuery::Ok => unreachable!(),
        }
        // wait for the client to close the connection (for at most 10s)
        if let Err(e) = stream.set_read_timeout(Some(Duration::new(20, 0))) {
            eprintln!("Failed to set read timeout: {:?}", e)
        } else {
            // wait for a 0 byte read, eg: EOF
            let _ = stream.read(&mut []);
        }

        Ok(must_quit)
    }

    /// Read the status of the child process through `sigchild_reader`,
    /// and notify the hypervisor about it.
    /// Also store the status to file if the process is terminated.
    fn process_child_status(
        &mut self,
        sigchild_reader: &mut SignalFd,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.status = match sigchild_reader.read_signal() {
            Ok(Some(siginfo)) => TaskStatus::from_siginfo(&siginfo).unwrap_or(TaskStatus::Failed),
            _ => TaskStatus::Failed, // assume failure, if nothing is returned
        };
        // notify hypervisor
        if let Err(error) = self.notify_hypervisor() {
            eprintln!(
                "Failed to send status to hypervisor through socket: '{:?}': '{:?}'",
                self.hypervisor_socket, error
            );
        }
        // save termination status
        if self.status.is_terminated() {
            // write status to file
            self.status.save_to_file(&self.handle.status_file())?;
        }
        Ok(())
    }
}

impl TaskStatus {
    /// Build a Status from the siginfo struct returned by reading a SIGCHLD signalfd
    /// based on `man 2 sigaction`
    pub fn from_siginfo(info: &siginfo) -> Result<Self, String> {
        // check the signal that was bound to the signalfd this siginfo is the result of.
        if info.ssi_signo != libc::SIGCHLD as u32 {
            return Err("not a SIG_CHLD siginfo".to_string());
        }
        match info.ssi_code {
            libc::CLD_EXITED => match info.ssi_status {
                libc::EXIT_SUCCESS => Ok(Self::Succeed),
                libc::EXIT_FAILURE => Ok(Self::Failed),
                unknown => Err(format!(
                    "Unkown return status code '{}' in siginfo",
                    unknown
                )),
            },
            libc::CLD_KILLED => Ok(Self::Killed),
            libc::CLD_DUMPED => Ok(Self::Failed),
            libc::CLD_TRAPPED => Ok(Self::Failed),
            libc::CLD_STOPPED => Ok(Self::Stopped),
            libc::CLD_CONTINUED => Ok(Self::Running),
            unknown => Err(format!("Unkown status code '{}' in siginfo", unknown)),
        }
    }
}
