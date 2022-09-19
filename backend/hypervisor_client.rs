//! The client:
//! * parse XML workflows, then:
//!   * sends it to the database writer to save them,
//!   * append them to the queue (through the queue_actor),
//! * forwards kill order to the queue actor,
//! * forwards task/job status requests to the Job status cache actor
//! * handle task output read requests itself
