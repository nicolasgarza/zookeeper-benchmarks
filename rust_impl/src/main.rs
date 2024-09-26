use std::thread;
use std::time::{Duration, Instant};
use zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

struct DefaultWatcher;
impl Watcher for DefaultWatcher {
    fn handle(&self, _ev: WatchedEvent) {}
}

fn main() {
    let duration = Duration::new(5, 0);
    let start_time = Instant::now();

    let zk = ZooKeeper::connect("127.0.0.1:2181", Duration::from_secs(5), DefaultWatcher).unwrap();

    let mut handles = Vec::new();
    const BENCHMARK_PATH: &str = "/benchmark";

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

    for _ in 0..10 {
        let start_time_clone = start_time;

        let handle = thread::spawn(move || {
            let zk = ZooKeeper::connect("127.0.0.1:2181", Duration::from_secs(5), DefaultWatcher)
                .unwrap();

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
        });
        handles.push(handle);
    }

    // wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let num_children = zk.get_children(BENCHMARK_PATH, false).unwrap().len();
    println!("Total znodes added: {}", num_children);
    println!(
        "Operations per second: {:.2}",
        num_children as f64 / duration.as_secs_f64()
    );
    // Close the ZooKeeper connection
    zk.close().unwrap();
}
