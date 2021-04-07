use nix::{
    unistd::{Pid, close},
    sys::{
        epoll::{epoll_create, epoll_ctl, epoll_wait, EpollOp, EpollEvent, EpollFlags},
        signalfd::{SignalFd, SfdFlags},
        signal::{Signal, SigSet},
    },
};
use std::{
    os::unix::{
        net::{UnixListener, UnixStream},
        io::{RawFd, AsRawFd},
    },
    convert::TryInto,
    collections::HashMap,
    io::Read,
    path::PathBuf,
};
use crate::tasks::monitor_handle::MonitorHandle;
use crate::tasks::task_status::TaskStatus;
use crate::tasks::query::{Sendable,Query};
use crate::tasks::pipe::Fence;

/// Epoll will wait forever (unless an event happens) if this timout value is provided
const WAIT_FOREVER_TIMEOUT: isize = -1;


pub struct Monitor {
    pub start_fence: Option<Fence>,
    pub monitoree: Pid,
    pub status: TaskStatus,
    pub handle: MonitorHandle,
}

impl Monitor {

    fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>  {
        println!("Starting task...");
        if let Some(rel) = self.start_fence.take() {
            rel.release_waiter()?;
            self.status = TaskStatus::Running;
        }
        Ok(())
    }

    fn process_query(&mut self, query: Query) -> Result<(), Box<dyn std::error::Error>> {
        match query {
            Query::Start => self.start()?,
            query => println!("{:?}", query)
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
        // TODO:
        // * notify the hypervisor when the child terminates using the `handle.monitor_socket`
        // * setup command parsing (if needed)

        // create the Unix socket
        let listener = UnixListener::bind(&self.handle.monitor_socket())
            .map_err(|e| format!("Could not create monitor socket: '{:?}'. '{:?}'", &self.handle.monitor_socket(), e))?;
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
        epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, sigchild_fd, Some(&mut sigchild_event))?;
        let mut listener_event = EpollEvent::new(EpollFlags::EPOLLIN, listener_fd.try_into()?);
        epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, listener_fd, Some(&mut listener_event))?;

        // create a empty event array, that we will feed to epoll_wait()
        let mut events: Vec<EpollEvent> = (0..2)
            .map(|_| EpollEvent::empty())
            .collect();

        // stores connection streams
        let mut streams_by_fd: HashMap<RawFd, UnixStream> = HashMap::new();
        let mut streams = listener.incoming();
        // start the event loop:
        'event_loop: while let Ok(event_count) = epoll_wait(epoll_fd, &mut events, WAIT_FOREVER_TIMEOUT) {
            for event_idx in 0..event_count {
                // fetch the data we've associated with the event (file descriptors)
                let fd: RawFd = events[event_idx].data().try_into()?;
                match fd {
                    // the monitoree has terminated
                    fd if fd == sigchild_fd => {
                        self.status = match sigchild_reader.read_signal() {
                            Ok(Some(siginfo)) => TaskStatus::from_siginfo(&siginfo)
                                .unwrap_or(TaskStatus::Failed),
                            _ => TaskStatus::Failed,  // assume failure, if nothing is returned
                        };
                        if self.status.is_terminated() {
                            self.status.save_to_file(&self.handle.status_file())?;
                            break 'event_loop;
                        }
                    },
                    // a client connected to the socket
                    fd if fd == listener_fd => { 
                        // won't block (epolled)
                        let stream = streams.next()
                            .ok_or(format!("No stream available (unreachable)"))??;
                        // register the stream to the epoll_fd
                        let stream_fd: RawFd = stream.as_raw_fd();
                        let mut event = EpollEvent::new(EpollFlags::EPOLLIN, stream_fd.try_into()?);
                        epoll_ctl(epoll_fd, EpollOp::EpollCtlAdd, stream_fd, Some(&mut event))?;
                        // store the stream
                        streams_by_fd.insert(stream_fd, stream);

                    },
                    // data is ready to read on one of the connection
                    stream_fd => {
                        // fetch the stream for the fd, and process the request
                        let mut stream = streams_by_fd.remove(&stream_fd)
                            .ok_or(format!("No stream using this RawFD (unreachable)"))?;
                        // unregister the stream from the epoll
                        epoll_ctl(epoll_fd, EpollOp::EpollCtlDel, stream_fd, None)?;
                        // read queries from the stream, ignore failures
                        if let Ok(query) = Query::read_from(&mut stream) {
                            self.process_query(query)?;
                        }
                    }
                }
            }
        }
        // Close all opened RawFds
        close(epoll_fd)?;
        // remove the socket file
        std::fs::remove_file(&self.handle.monitor_socket())?;

        Ok(())
    }
}


