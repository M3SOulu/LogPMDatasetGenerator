use std::collections::{HashMap, HashSet};
use csv::Writer;
use log_pm_dataset_generator::matching::{start_thread_pool};
use log_pm_dataset_generator::loading::{message_extractor, load_loglines, load_regex};
use log::info;


const WORKER_COUNT: u8 = 4;

fn main() {

    // Init logging
    env_logger::init();

    // Reading environment variables and command line arguments
    let env: HashMap<String, String> = std::env::vars().collect();
    let dataset_name = {
        let args: Vec<String> = std::env::args().collect();
        args.get(1).expect("Dataset not provided in the command line args").to_string()
    };

    // Init crawler
    let dataset_path = format!("{}/{}",
                               env.get("LOG_DATASETS")
                                   .expect("LOG_DATASETS is not provided in the environment variables")
                                   .as_str(),
                               dataset_name);
    let message_extractor = message_extractor(&dataset_name);

    // Worker pool
    info!("Initiating worker pool");
    let (mut pool_input, pool_output) = {
        let regex_path = format!("{}/{}.regex",
                                 env.get("REGEX_DIRECTORY").expect("REGEX_DIRECTORY is not provided in the environment variables").as_str(),
                                 dataset_name);
        info!("Loading regexes from {}", regex_path);
        let regex_vec = load_regex(regex_path.as_str());
        start_thread_pool(regex_vec, WORKER_COUNT)
    };

    // Writer thread
    info!("Starting the writer thread");
    let mut csv_writer = Writer::from_path(format!("{}.csv", dataset_name).as_str()).unwrap();
    let writer_thread = std::thread::spawn(move || {
        info!("Writer thread started");
        let mut lines: u32 = 0;
        for res in pool_output {
            csv_writer.write_record(res.into_csv_record()).expect("unable to write");
            lines += 1;
        }
        csv_writer.flush().expect("Failed to flush");
        info!("Total of {} lines were written to the csv files", lines)
    });

    // Reading from crawler and writing to workers
    info!("Loading messages from {}", dataset_path);
    info!("Distributing messages among workers...");
    let mut distributed_lines: u32 = 0;
    let mut crawled_lines: u32 = 0;
    let mut message_set = HashSet::new();
    for msg in load_loglines(dataset_path)
        .filter_map(|line| { message_extractor(line) }) {
        crawled_lines += 1;
        let message = msg.to_string();
        if message_set.contains(&message) {
            continue;
        }
        message_set.insert(message.clone());
        pool_input.submit(msg);
        distributed_lines += 1;
    }
    info!("Total of {} lines were crawled and {} of them were distributed between workers", crawled_lines, distributed_lines);

    // Shutting down worker pool
    info!("Sending halt message to all threads");

    // Send end of stream to all threads
    pool_input.end_of_stream();

    // Join the writer thread
    info!("Joining writer thread");
    writer_thread.join().unwrap();

    // Join worker threads
    info!("Joining worker threads");
    pool_input.join();
}