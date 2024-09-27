use std::thread;
use std::time::{Duration, Instant};
use zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

struct DefaultWatcher;
impl Watcher for DefaultWatcher {
    fn handle(&self, _ev: WatchedEvent) {}
}

fn main() {
    ephemeral_test();
}

fn ephemeral_test() {
    let duration = Duration::new(10, 0);
    let start_time = Instant::now();

    let zk = ZooKeeper::connect("127.0.0.1:2181", Duration::from_secs(10), DefaultWatcher).unwrap();

    let mut handles = Vec::new();
    const BENCHMARK_PATH: &str = "/benchmark";

    // delete old benchmark node if it exists
    if let Ok(prev_benchmark_node) = zk.exists(BENCHMARK_PATH, false) {
        zk.delete(BENCHMARK_PATH, Some(prev_benchmark_node.unwrap().version))
            .expect("Failed to delete old benchmark node");
    }

    zk.create(
        BENCHMARK_PATH,
        vec![],
        Acl::open_unsafe().clone(),
        CreateMode::Persistent,
    )
    .expect("failed to create parent dir");

    // store connections in a vec (or else all ephemeral nodes get deleted when threads finish up)
    let mut zookeeper_connections = Vec::new();
    for _ in 0..50 {
        let start_time_clone = start_time;

        // spawn the 50 threads
        let handle = thread::spawn(move || {
            let zk = ZooKeeper::connect("127.0.0.1:2181", Duration::from_secs(10), DefaultWatcher)
                .unwrap();

            // create znodes in a loop for each thread
            while start_time_clone.elapsed() < duration {
                // create an ephemeral sequential znode
                zk.create(
                    format!("{}/node_", BENCHMARK_PATH).as_str(),
                    vec![],
                    Acl::open_unsafe().clone(),
                    CreateMode::EphemeralSequential,
                )
                .expect("Error creating znode");
            }
            zk
        });
        handles.push(handle);
    }

    // wait for all threads to complete
    for handle in handles {
        let zk_connection = handle.join().unwrap();
        zookeeper_connections.push(zk_connection);
    }

    // get number of children and print them
    let num_children = zk.get_children(BENCHMARK_PATH, false).unwrap().len();
    println!("Total znodes added: {}", num_children);
    println!(
        "Operations per second: {:.2}",
        num_children as f64 / duration.as_secs_f64()
    );
    // Close the ZooKeeper connection
    zk.close().unwrap();
}
