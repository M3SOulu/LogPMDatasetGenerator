pub mod matching {
    use std::thread::JoinHandle;
    use log::{debug, error};
    use regex::Regex;
    use lockfree::channel::{RecvErr, spmc};
    use lockfree::channel::mpsc;

    const UNKNOWN_THREAD_NAME: &str = "UNKNOWN_THREAD_NAME";

    #[derive(Debug)]
    pub enum Request {
        Parse(String),
        EndOfStream,
    }

    #[derive(Debug)]
    pub struct Response {
        pub msg: String,
        pub msk: String,
        pub idx: u16,
    }

    pub struct ThreadPoolInput {
        input: spmc::Sender<Request>,
        join_handles: Vec<JoinHandle<String>>,
    }

    pub struct ThreadPoolOutput {
        output: mpsc::Receiver<Response>,
    }

    pub struct ThreadPoolOutputIter {
        output_receiver: mpsc::Receiver<Response>,
    }

    impl Response {
        pub fn into_csv_record(self) -> [String; 3] {
            [self.msg, self.msk, self.idx.to_string()]
        }
    }

    impl ThreadPoolInput {
        pub fn submit(&mut self, msg: String) {
            self.input.send(Request::Parse(msg)).expect("Unable to submit job");
        }

        pub fn end_of_stream(&mut self) {
            for _ in 0..self.join_handles.len() {
                self.input.send(Request::EndOfStream).expect("Unable to send termination request");
            }
        }

        pub fn join(self) {
            for worker in self.join_handles {
                let name = worker.join().expect("Unable to join the thread");
                debug!("Worker thread with name '{}' joined the main thread", name);
            }
        }
    }

    impl IntoIterator for ThreadPoolOutput {
        type Item = Response;
        type IntoIter = ThreadPoolOutputIter;

        fn into_iter(self) -> Self::IntoIter {
            ThreadPoolOutputIter {
                output_receiver: self.output,
            }
        }
    }

    impl Iterator for ThreadPoolOutputIter {
        type Item = Response;

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                match self.output_receiver.recv() {
                    Ok(res) => { return Some(res); }
                    Err(RecvErr::NoMessage) => { continue; }
                    Err(RecvErr::NoSender) => { return None; }
                }
            }
        }
    }

    pub fn start_thread_pool(regex_vec: Vec<Regex>, worker_count: u8) -> (ThreadPoolInput, ThreadPoolOutput) {
        let (i_tx, i_rx) = spmc::create();
        let (o_tx, o_rx) = mpsc::create();
        let mut handles = Vec::new();

        for idx in 0..worker_count {
            let rx = i_rx.clone();
            let tx = o_tx.clone();
            let rv = regex_vec.clone();
            let handle = std::thread::Builder::new()
                .name(format!("LockFreeWorker {}", idx))
                .spawn(move || { worker_loop(rx, tx, rv) })
                .expect("Unable to spawn a thread");
            handles.push(handle);
        }

        (ThreadPoolInput {
            input: i_tx,
            join_handles: handles,
        }, ThreadPoolOutput {
            output: o_rx,
        })
    }

    fn worker_loop(rx: spmc::Receiver<Request>, tx: mpsc::Sender<Response>, regex_vec: Vec<Regex>) -> String {
        let current_thread = std::thread::current();
        let thread_name = current_thread.name().unwrap_or(UNKNOWN_THREAD_NAME);
        debug!("Worker thread started with name '{}'", thread_name);
        loop {
            match rx.recv() {
                Ok(Request::Parse(msg)) => {
                    match match_regex(&regex_vec, msg.as_str()) {
                        Ok((idx, msk)) => {
                            tx.send(Response {
                                msg,
                                msk,
                                idx: idx as u16,
                            })
                                .expect("Cannot send message");
                        }
                        Err(err) => { error!("{}", err) }
                    }
                }
                Ok(Request::EndOfStream) => { break; }
                Err(RecvErr::NoMessage) => { continue; }
                Err(RecvErr::NoSender) => { panic!("Sender channel closed before worker is finished") }
            }
        }
        thread_name.to_string()
    }

    fn match_regex(v: &[Regex], line: &str) -> Result<(isize, String), String> {
        let mut m: isize = -1;
        let mut mask = "0".repeat(line.len());
        for (i, re) in v.iter().enumerate() {
            if !re.is_match(line) {
                continue;
            }
            if m != -1 {
                return Err(format!("double match\n{}\n{}\n{}", line, v[m as usize], v[i]));
            }
            m = i as isize;
            let caps = re.captures(line).unwrap();
            for i in 1..caps.len() {
                if let Some(mat) = caps.get(i) {
                    mask.replace_range(mat.range(), "1".repeat(mat.end() - mat.start()).as_str());
                }
            }
        }
        if m != -1 {
            Ok((m, mask))
        } else {
            Err(format!("No match found for '{}'", line))
        }
    }
}

pub mod loading {
    use std::borrow::Borrow;
    use std::fs::File;
    use std::io::{BufRead, BufReader, Lines};
    use regex::Regex;
    use walkdir::{DirEntry, WalkDir};

    pub fn load_regex(file: &str) -> Vec<Regex> {
        let mut v = Vec::new();
        let file = match File::open(file) {
            Ok(val) => val,
            Err(err) => panic!("Invalid file {}", err),
        };
        let buf_reader = BufReader::new(file);
        for (i, l) in buf_reader.lines().enumerate() {
            let re = Regex::new(format!("^{}$", l.expect("Unable to read line").as_str()).as_str()).unwrap_or_else(|_| panic!("Unable to compile regex at {}", i));
            v.push(re);
        }
        v
    }

    pub fn load_loglines(dir: String) -> impl Iterator<Item=String> {
        WalkDir::new(dir).into_iter()
            .filter_map(|result| { result.ok() })
            .filter(is_log)
            .flat_map(buf_reader)
            .filter_map(|result| { result.ok() })
    }

    pub fn message_extractor(name: &String) -> impl Fn(String) -> Option<String> {
        match name.borrow() {
            "hadoop" => |line: String| {
                if line.len() > 29 {
                    let begin = line.find(']')? + 1;
                    let idx = line[begin..].find(':')? + 1;
                    Some(line[(begin + idx)..].trim().to_string())
                } else {
                    None
                }
            },
            "proxifier" => |line: String| {
                Some(line[17..].trim().to_string())
            },
            "ssh" => |line: String| {
                if line.len() > 29 {
                    let begin = line.find(']')?;
                    Some(line[(begin + 3)..].trim().to_string())
                } else {
                    None
                }
            },
            "linux" => |line: String| {
                if line.len() > 23 {
                    let begin = line[23..].find(':')? + 2;
                    let msg = line[23 + begin..].trim();
                    if !msg.is_empty() {
                        Some(msg.to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            },
            "openstack" => |line: String| {
                if line.len() > 29 {
                    let begin = line.find(']')?;
                    Some(line[(begin + 2)..].trim().to_string())
                } else {
                    None
                }
            },
            "hdfs" => |line: String| {
                Some(line.trim()
                    .splitn(6, ' ')
                    .last()?.to_string())
            },
            "android" => |line: String| {
                let msg = line[33..]
                    .splitn(2, ':')
                    .last()?;
                if msg.is_empty() {
                    None
                } else {
                    Some(msg.trim().to_string())
                }
            },
            "apache" => |line: String| {
                // let msg = line[28..]
                //     .splitn(2, ']')
                //     .last()?;
                let v: Vec<&str> = line[28..].splitn(2, ']').collect();
                if v.len() == 2 {
                    let msg = v[1].trim();
                    if msg.is_empty() {
                        None
                    } else {
                        Some(msg.trim().to_string())
                    }
                } else {
                    None
                }
            },
            "zookeeper" => |line: String| {
                Some(line.splitn(3, " - ").last()?.to_string())
            },
            "hpc" => |line: String| {
                let t= line.trim().splitn(7, ' ').last()?;
                if t.len() > 2 {
                    let first_char = t.chars().next().unwrap();
                    if first_char == '0' || first_char == '1' {
                        Some(t[2..].to_string())
                    } else {
                        Some(t.to_string())
                    }
                } else {
                    None
                }
            },
            _ => { panic!("Unsupported dataset!") }
        }
    }

    fn buf_reader(entry: DirEntry) -> Lines<BufReader<File>> {
        let f = File::open(entry.path()).expect("Unable to open file");
        BufReader::new(f).lines()
    }

    fn is_log(entry: &DirEntry) -> bool {
        entry.path().extension().unwrap_or_default() == "log"
    }
}