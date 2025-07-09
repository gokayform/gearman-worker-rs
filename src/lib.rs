#![deny(missing_docs)]
//! # gearman-worker
//!
//! The `gearman-worker` crate provides a high level library to easily
//! implement gearman [`Worker`](struct.Worker.html)s.
//!
//! It handles registration of functions as jobs in the gearman queue
//! server, fetching of jobs and their workload.
//!
//! ## Usage
//!
//! ```ignore
//! use gearman_worker::WorkerBuilder;
//!
//! fn main() {
//!     let mut worker = WorkerBuilder::default().build();
//!     worker.connect().unwrap();
//!
//!     worker.register_function("greet", |input| {
//!         let hello = String::from_utf8_lossy(input);
//!         let response = format!("{} world!", hello);
//!         Ok(response.into_bytes())
//!     }).unwrap();
//!
//!     worker.run().unwrap();
//! }
//! ```

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io;
use std::io::prelude::*;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::process;
use uuid::Uuid;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use std::time::Duration;

const CAN_DO: u32 = 1;
const CANT_DO: u32 = 2;
// const RESET_ABILITIES: u32 = 3;
const PRE_SLEEP: u32 = 4;
const NOOP: u32 = 6;
const GRAB_JOB: u32 = 9;
const NO_JOB: u32 = 10;
const JOB_ASSIGN: u32 = 11;
// const WORK_STATUS: u32 = 12;
const WORK_COMPLETE: u32 = 13;
const WORK_FAIL: u32 = 14;
const SET_CLIENT_ID: u32 = 22;
// const CAN_DO_TIMEOUT: u32 = 23;
// const ALL_YOURS: u32 = 24;
const WORK_EXCEPTION: u32 = 25;
// const WORK_DATA: u32 = 28;
// const WORK_WARNING: u32 = 29;
// const GRAB_JOB_UNIQ: u32 = 30;
// const JOB_ASSIGN_UNIQ: u32 = 31;
// const GRAB_JOB_ALL: u32 = 39;
// const JOB_ASSIGN_ALL: u32 = 40;
const ERROR: u32 = 19;

/// A packet received from the gearman queue server.
struct Packet {
    /// The packet type representing the request intent
    cmd: u32,
    /// The data associated with the request
    data: Vec<u8>,
}

type WorkResult = Result<Vec<u8>, Option<Vec<u8>>>;
type Callback = Box<dyn Fn(&[u8]) -> WorkResult + 'static>;

struct CallbackInfo {
    callback: Callback,
    enabled: bool,
}

impl CallbackInfo {
    fn new<F: Fn(&[u8]) -> WorkResult + 'static>(callback: F) -> Self {
        Self {
            callback: Box::new(callback),
            enabled: true,
        }
    }
}

impl Packet {
    /// Decodes a packet from a stream received from the gearman server
    fn from_stream(stream: &mut TcpStream) -> io::Result<Self> {
        let mut magic = vec![0u8; 4];
        stream.read_exact(&mut magic)?;

        if magic != b"\0RES" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unexpected magic packet received from server",
            ));
        }

        let cmd = stream.read_u32::<BigEndian>()?;
        let size = stream.read_u32::<BigEndian>()?;
        let mut data = vec![0u8; size as usize];

        if size > 0 {
            stream.read_exact(&mut data)?;
        }

        Ok(Packet { cmd, data })
    }
}

struct Job {
    handle: String,
    function: String,
    workload: Vec<u8>,
}

impl Job {
    fn from_data(data: &[u8]) -> io::Result<Self> {
        let mut iter = data.split(|c| *c == 0);

        let handle = match iter.next() {
            Some(handle) => String::from_utf8_lossy(handle),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Could not decode handle id",
                ));
            }
        };

        let fun = match iter.next() {
            Some(fun) => String::from_utf8_lossy(fun),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Could not decode function name",
                ));
            }
        };

        let payload = &data[handle.len() + fun.len() + 2..];

        Ok(Self {
            handle: handle.to_string(),
            function: fun.to_string(),
            workload: payload.to_vec(),
        })
    }

    fn send_response(
        &self,
        server: &mut ServerConnection,
        response: &WorkResult,
    ) -> io::Result<()> {
        let (op, data) = match response {
            Ok(data) => (WORK_COMPLETE, Some(data)),
            Err(Some(data)) => (WORK_FAIL, Some(data)),
            Err(None) => (WORK_EXCEPTION, None),
        };

        let size = self.handle.len() + 1 + data.map_or(0, |b| b.len());

        let mut payload = Vec::with_capacity(size);
        payload.extend_from_slice(self.handle.as_bytes());
        if let Some(data) = data {
            payload.extend_from_slice(b"\0");
            payload.extend_from_slice(data);
        }
        server.send(op, &payload[..])
    }
}

/// The `Worker` processes jobs provided by the gearman queue server.
///
/// Building a worker requires a [`SocketAddr`](https://doc.rust-lang.org/std/net/enum.SocketAddr.html) to
/// connect to the gearman server (typically some ip address on port 4730).
///
/// The worker also needs a unique id to identify itself to the server.
/// This can be omitted letting the [`WorkerBuilder`](struct.WorkerBuilder.html) generate one composed
/// by the process id and a random uuid v4.
///
/// # Examples
///
/// Create a worker with all default options.
///
/// ```
/// use gearman_worker::WorkerBuilder;
/// let mut worker = WorkerBuilder::default().build();
/// ```
///
/// Create a worker with all explicit options.
///
/// ```
/// use gearman_worker::WorkerBuilder;
/// let mut worker = WorkerBuilder::new("my-worker-1", "127.0.0.1:4730".parse().unwrap()).build();
/// ```
pub struct Worker {
    /// the unique id of the worker
    id: String,
    server: ServerConnection,
    functions: HashMap<String, CallbackInfo>,
    should_shutdown: Arc<AtomicBool>,
}

/// Helps building a new [`Worker`](struct.Worker.html)
pub struct WorkerBuilder {
    id: String,
    addr: SocketAddr,
}

struct ServerConnection {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl ServerConnection {
    fn new(addr: SocketAddr) -> Self {
        Self { addr, stream: None }
    }

    fn connect(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(self.addr)?;
        self.stream = Some(stream);
        Ok(())
    }

    fn read_header(&mut self) -> io::Result<Packet> {
        let mut stream = match &mut self.stream {
            Some(ref mut stream) => stream,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Stream is not open...",
                ));
            }
        };

        Ok(Packet::from_stream(&mut stream)?)
    }

    fn send(&mut self, command: u32, param: &[u8]) -> io::Result<()> {
        let mut stream = match &self.stream {
            Some(ref stream) => stream,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Stream is not open...",
                ));
            }
        };

        stream.write_all(b"\0REQ")?;
        stream.write_u32::<BigEndian>(command)?;
        stream.write_u32::<BigEndian>(param.len() as u32)?;
        stream.write_all(param)?;

        Ok(())
    }
}

impl Worker {
    /// Registers a `callback` function that can handle jobs with the specified `name`
    /// provided by the gearman queue server.
    ///
    /// The callback has the signature `Fn(&[u8]) -> WorkResult + 'static` receiving a
    /// slice of bytes in input, which is the workload received from the gearmand server.
    ///
    /// It can return `Ok(Vec<u8>)` ([`WORK_COMPLETE`][protocol]) where the vector of
    /// bytes is the result of the job that will be transmitted back to the server,
    /// `Err(None)` ([`WORK_EXCEPTION`][protocol]) which will tell the server that the
    /// job failed with an unspecified error or `Err(Some(Vec<u8>))` ([`WORK_FAIL`][protocol])
    /// which will also represent a job failure but will include a payload of the error
    /// to the gearmand server.
    ///
    /// [protocol]: http://gearman.org/protocol
    pub fn register_function<S, F>(&mut self, name: S, callback: F) -> io::Result<()>
    where
        S: AsRef<str>,
        F: Fn(&[u8]) -> WorkResult + 'static,
    {
        let name = name.as_ref();
        self.server.send(CAN_DO, &name.as_bytes())?;
        self.functions
            .insert(name.to_string(), CallbackInfo::new(callback));
        Ok(())
    }

    /// Unregisters a previously registered function, notifying the server that
    /// this worker is not available anymore to process jobs with the specified `name`.
    pub fn unregister_function<S>(&mut self, name: S) -> io::Result<()>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        if let Some(func) = self.functions.remove(&name.to_string()) {
            if func.enabled {
                self.server.send(CANT_DO, &name.as_bytes())?;
            }
        }
        if self.functions.is_empty() {
            eprintln!("[gearman-worker] Warning: No functions registered. Worker will not receive jobs.");
        }
        Ok(())
    }

    /// Notify the gearman queue server that we are available/unavailable to process
    /// jobs with the specified `name`.
    pub fn set_function_enabled<S>(&mut self, name: S, enabled: bool) -> io::Result<()>
    where
        S: AsRef<str>,
    {
        let name = name.as_ref();
        match self.functions.get_mut(name) {
            Some(ref mut func) if func.enabled != enabled => {
                func.enabled = enabled;
                let op = if enabled { CAN_DO } else { CANT_DO };
                self.server.send(op, name.as_bytes())?;
            }
            Some(_) => eprintln!(
                "Function {} is already {}",
                name,
                if enabled { "enabled" } else { "disabled" }
            ),
            None => eprintln!("Unknown function {}", name),
        }
        if self.functions.is_empty() {
            eprintln!("[gearman-worker] Warning: No functions registered. Worker will not receive jobs.");
        }
        Ok(())
    }

    /// Let the server know that the worker identifies itself with the associated `id`.
    pub fn set_client_id(&mut self) -> io::Result<()> {
        self.server.send(SET_CLIENT_ID, self.id.as_bytes())
    }

    fn sleep(&mut self) -> io::Result<()> {
        self.server.send(PRE_SLEEP, b"")?;
        let resp = self.server.read_header()?;
        match resp.cmd {
            n if n == NOOP => Ok(()),
            n => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Worker was sleeping. NOOP was expected but packet {} was received instead.",
                    n
                ),
            )),
        }
    }

    fn grab_job(&mut self) -> io::Result<Option<Job>> {
        self.server.send(GRAB_JOB, b"")?;
        let resp = self.server.read_header()?;
        match resp.cmd {
            n if n == JOB_ASSIGN => Ok(Some(Job::from_data(&resp.data[..])?)),
            n if n == NO_JOB => Ok(None),
            n if n == ERROR => {
                // Parse error message from packet data
                let mut parts = resp.data.split(|c| *c == 0);
                let code = parts.next().map(|s| String::from_utf8_lossy(s).to_string()).unwrap_or_else(|| "<unknown>".to_string());
                let msg = parts.next().map(|s| String::from_utf8_lossy(s).to_string()).unwrap_or_else(|| "<no message>".to_string());
                eprintln!("[gearman-worker] Server sent ERROR packet: code='{}', message='{}'", code, msg);
                // Optionally, try to recover or reconnect here
                // For now, just return no job
                Ok(None)
            },
            n => {
                eprintln!("[gearman-worker] Unexpected packet {} received in grab_job. Data: {:?}", n, resp.data);
                Ok(None)
            }
        }
    }

    /// Ask the server to do some work. This will process one job that will be provided by
    /// the gearman queue server when available.
    pub fn do_work(&mut self) -> io::Result<u32> {
        let mut jobs = 0;

        if let Some(job) = self.grab_job()? {
            jobs += 1;
            match self.functions.get(&job.function) {
                Some(func) if func.enabled => {
                    job.send_response(&mut self.server, &(func.callback)(&job.workload))?
                }
                // gearmand should never pass us a job which was never advertised or unregistered
                Some(_) => eprintln!("Disabled job {:?}", job.function),
                None => eprintln!("Unknown job {:?}", job.function),
            }
        }

        Ok(jobs)
    }

    /// Process any available job as soon as the gearman queue server provides us with one
    /// in a loop, with reconnection, backoff, and graceful shutdown.
    pub fn run(&mut self) -> io::Result<()> {
        if self.functions.is_empty() {
            eprintln!("[gearman-worker] Warning: No functions registered before run(). Worker will not receive jobs.");
        }

        // Setup signal handler for graceful shutdown
        let shutdown_flag = self.should_shutdown.clone();
        {
            let shutdown_flag = shutdown_flag.clone();
            ctrlc::set_handler(move || {
                eprintln!("[gearman-worker] Received shutdown signal. Exiting loop soon...");
                shutdown_flag.store(true, Ordering::SeqCst);
            }).expect("Error setting Ctrl-C handler");
        }

        let mut reconnect_backoff = 1;
        let max_backoff = 32;
        loop {
            if self.should_shutdown.load(Ordering::SeqCst) {
                eprintln!("[gearman-worker] Shutdown flag set. Exiting main loop.");
                break;
            }
            let result = self.do_work();
            match result {
                Ok(done) => {
                    reconnect_backoff = 1; // reset backoff on success
                    if done == 0 {
                        if let Err(e) = self.sleep() {
                            eprintln!("[gearman-worker] Error during sleep: {}. Will attempt to reconnect...", e);
                            self.reconnect_and_reregister()?;
                            thread::sleep(Duration::from_secs(reconnect_backoff));
                            reconnect_backoff = (reconnect_backoff * 2).min(max_backoff);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[gearman-worker] Error in do_work: {}. Will attempt to reconnect...", e);
                    self.reconnect_and_reregister()?;
                    thread::sleep(Duration::from_secs(reconnect_backoff));
                    reconnect_backoff = (reconnect_backoff * 2).min(max_backoff);
                }
            }
        }
        Ok(())
    }

    /// Attempt to reconnect to the server and re-register all functions
    fn reconnect_and_reregister(&mut self) -> io::Result<()> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            eprintln!("[gearman-worker] Attempting to reconnect to server (attempt {})...", attempts);
            match self.server.connect() {
                Ok(_) => {
                    eprintln!("[gearman-worker] Reconnected to server.");
                    // Re-register all functions
                    for (name, func) in self.functions.iter() {
                        if func.enabled {
                            if let Err(e) = self.server.send(CAN_DO, name.as_bytes()) {
                                eprintln!("[gearman-worker] Failed to re-register function '{}': {}", name, e);
                            }
                        }
                    }
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("[gearman-worker] Reconnect failed: {}. Retrying in 2 seconds...", e);
                    thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }

    /// Estabilish a connection with the queue server and send it the ID of this worker.
    pub fn connect(&mut self) -> io::Result<&mut Self> {
        self.server.connect()?;
        Ok(self)
    }
}

impl WorkerBuilder {
    /// Create a new WorkerBuilder passing all options explicitly.
    pub fn new<S: Into<String>>(id: S, addr: SocketAddr) -> Self {
        Self {
            id: id.into(),
            addr,
        }
    }

    /// Set a specific ID for this worker. This should be unique to all workers!
    pub fn id<S: Into<String>>(&mut self, id: S) -> &mut Self {
        self.id = id.into();
        self
    }

    /// Define the socket address to connect to.
    pub fn addr(&mut self, addr: SocketAddr) -> &mut Self {
        self.addr = addr;
        self
    }

    /// Build the [`Worker`](struct.Worker.html).
    pub fn build(&self) -> Worker {
        Worker {
            id: self.id.clone(),
            server: ServerConnection::new(self.addr),
            functions: HashMap::new(),
            should_shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Default for WorkerBuilder {
    fn default() -> Self {
        let uniqid = Uuid::new_v4();
        Self::new(
            format!("{}-{}", process::id(), uniqid.to_hyphenated()),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4730),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, Instant};

    fn get_test_port() -> u16 {
        env::var("GEARMAN_TEST_PORT")
            .unwrap_or_else(|_| "4730".to_string())
            .parse()
            .expect("Invalid GEARMAN_TEST_PORT")
    }

    fn submit_job_async(func: &str, port: u16, input: &str) -> Result<String, String> {
        
        let container_name = env::var("GEARMAN_CONTAINER_NAME").unwrap_or_else(|_| "gearman-test-server".to_string());
        
        if env::var("GEARMAN_USE_CONTAINER").is_ok() {
            // Use gearman client inside the container with echo piping approach
            println!("Submitting job to container: func={}, input='{}'", func, input);
            let output = Command::new("podman")
                .arg("exec")
                .arg(&container_name)
                .arg("bash")
                .arg("-c")
                .arg(&format!("echo -n '{}' | timeout 10 gearman -p {} -f {}", 
                    input.replace("'", "'\"'\"'"), // Escape single quotes properly
                    port, 
                    func))
                .output()
                .map_err(|e| format!("Failed to spawn container command: {}", e))?;
            
            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            } else {
                Err(String::from_utf8_lossy(&output.stderr).to_string())
            }
        } else {
            // Use local gearman client with echo piping approach
            let output = Command::new("bash")
                .arg("-c")
                .arg(&format!("echo -n '{}' | timeout 10 gearman -p {} -f {}", 
                    input.replace("'", "'\"'\"'"), // Escape single quotes properly
                    port, 
                    func))
                .output()
                .map_err(|e| format!("Failed to spawn local command: {}", e))?;
            
            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).to_string())
            } else {
                Err(String::from_utf8_lossy(&output.stderr).to_string())
            }
        }
    }

    fn wait_for_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let max_attempts = 30;
        for _ in 0..max_attempts {
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }
        Err("Gearman server not available".into())
    }

    #[test]
    fn test_basic_job_processing() {
        let port = get_test_port();
        
        // Wait for server to be available
        wait_for_server(port).expect("Gearman server should be available");

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();

        let mut worker = WorkerBuilder::default()
            .addr(addr)
            .id("gearman-worker-rs-test")
            .build();

        worker
            .connect()
            .expect("Failed to connect to gearmand server");

        worker
            .register_function("testfun", |input| {
                println!("testfun called with input: {:?}", String::from_utf8_lossy(input));
                Ok(b"foobar".to_vec())
            })
            .expect("Failed to register test function");

        // Give the worker time to register the function
        thread::sleep(Duration::from_millis(500));

        // Submit job in background thread
        let port_clone = port;
        let job_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100)); // Let worker start first
            submit_job_async("testfun", port_clone, "test_input_data")
        });

        // Process the job
        let start = Instant::now();
        let mut done = 0;
        while done == 0 && start.elapsed() < Duration::from_secs(5) {
            done = worker.do_work().unwrap();
            if done == 0 {
                thread::sleep(Duration::from_millis(10));
            }
        }
        
        assert_eq!(1, done);

        // Get the result
        let result = job_handle.join().unwrap();
        match result {
            Ok(output) => {
                println!("Basic test output: '{}'", output.trim());
                assert_eq!("foobar", output.trim());
            }
            Err(e) => panic!("Job failed: {}", e),
        }
    }

    #[test]
    fn test_error_handling() {
        let port = get_test_port();
        
        // Wait for server to be available
        wait_for_server(port).expect("Gearman server should be available");

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();

        let mut worker = WorkerBuilder::default()
            .addr(addr)
            .id("gearman-worker-rs-error-test")
            .build();

        worker
            .connect()
            .expect("Failed to connect to gearmand server");

        worker
            .register_function("error_test", |_| {
                Err(Some(b"Test error".to_vec()))
            })
            .expect("Failed to register error test function");

        // Give the worker time to register the function
        thread::sleep(Duration::from_millis(500));

        // Submit job in background thread
        let port_clone = port;
        let job_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100)); // Let worker start first
            submit_job_async("error_test", port_clone, "test input")
        });

        // Process the job
        let start = Instant::now();
        let mut done = 0;
        while done == 0 && start.elapsed() < Duration::from_secs(5) {
            done = worker.do_work().unwrap();
            if done == 0 {
                thread::sleep(Duration::from_millis(10));
            }
        }
        
        assert_eq!(1, done);

        // Get the result - should be an error
        let result = job_handle.join().unwrap();
        match result {
            Ok(_) => panic!("Expected job to fail but it succeeded"),
            Err(e) => {
                println!("Got expected error: {}", e);
                // This is expected for a failing job
            }
        }
    }

    #[test]
    fn test_multiple_jobs() {
        let port = get_test_port();
        
        // Wait for server to be available
        wait_for_server(port).expect("Gearman server should be available");

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();

        let mut worker = WorkerBuilder::default()
            .addr(addr)
            .id("gearman-worker-rs-multi-test")
            .build();

        worker
            .connect()
            .expect("Failed to connect to gearmand server");

        worker
            .register_function("multi_test", |input| {
                let input_str = String::from_utf8_lossy(input);
                println!("Worker received input: '{}'", input_str);
                let response = format!("processed: {}", input_str);
                println!("Worker sending response: '{}'", response);
                Ok(response.into_bytes())
            })
            .expect("Failed to register multi test function");

        // Give the worker time to register the function
        thread::sleep(Duration::from_millis(500));
        
        // Submit jobs in background threads
        let port_clone1 = port;
        let port_clone2 = port;
        
        let job1_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            submit_job_async("multi_test", port_clone1, "job1_data")
        });
        
        let job2_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            submit_job_async("multi_test", port_clone2, "job2_data")
        });

        // Process both jobs
        let start = Instant::now();
        let mut total_done = 0;
        while total_done < 2 && start.elapsed() < Duration::from_secs(10) {
            let done = worker.do_work().unwrap();
            total_done += done;
            if done == 0 {
                thread::sleep(Duration::from_millis(10));
            }
        }
        
        assert_eq!(2, total_done);

        // Get the results
        let result1 = job1_handle.join().unwrap();
        let result2 = job2_handle.join().unwrap();
        
        match (result1, result2) {
            (Ok(output1), Ok(output2)) => {
                println!("Job 1 output: '{}'", output1.trim());
                println!("Job 2 output: '{}'", output2.trim());
                assert!(output1.trim().contains("processed: job1_data"));
                assert!(output2.trim().contains("processed: job2_data"));
            }
            (Err(e1), _) => panic!("Job 1 failed: {}", e1),
            (_, Err(e2)) => panic!("Job 2 failed: {}", e2),
        }
    }
}
