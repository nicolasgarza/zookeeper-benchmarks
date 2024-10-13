#![allow(dead_code)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use zookeeper::{Acl, CreateMode, WatchedEvent, Watcher, ZooKeeper};

struct DefaultWatcher;
impl Watcher for DefaultWatcher {
    fn handle(&self, _ev: WatchedEvent) {}
}

fn main() {
    ephemeral_sequential_test();
    //ephemeral_test();
    //check_threads_running();
}

fn ephemeral_sequential_test() {
    let duration = Duration::new(10, 0);
    let start_time = Instant::now();

    let zk = Arc::new(
        ZooKeeper::connect("127.0.0.1:2181", Duration::from_secs(10), DefaultWatcher)
            .expect("Failed to connect to ZooKeeper"),
    );

    let mut handles = Vec::new();
    const BENCHMARK_PATH: &str = "/benchmark";

    // create benchmark node if it doesn't exist
    if zk.exists(BENCHMARK_PATH, false).unwrap().is_none() {
        zk.create(
            BENCHMARK_PATH,
            vec![],
            Acl::open_unsafe().clone(),
            CreateMode::Persistent,
        )
        .expect("Failed to create parent dir");
    }

    for _ in 0..1500 {
        let zk_clone = Arc::clone(&zk);
        let start_time_clone = start_time;

        let handle = thread::spawn(move || {
            while start_time_clone.elapsed() < duration {
                // Create an ephemeral sequential znode
                zk_clone
                    .create(
                        &format!("{}/node_", BENCHMARK_PATH),
                        vec![],
                        Acl::open_unsafe().clone(),
                        CreateMode::PersistentSequential,
                    )
                    .expect("Error creating znode");
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    thread::sleep(Duration::from_secs(1));
    // Get number of children and print them
    let num_children = zk.get_children(BENCHMARK_PATH, false).unwrap().len();
    println!("Total znodes added: {}", num_children);
    println!(
        "Operations per second: {:.2}",
        num_children as f64 / duration.as_secs_f64()
    );
}

fn ephemeral_test() {
    let duration = Duration::new(10, 0);
    let start_time = Instant::now();

    let zk = Arc::new(
        ZooKeeper::connect("127.0.0.1:2181", Duration::from_secs(10), DefaultWatcher)
            .expect("Failed to connect to ZooKeeper"),
    );

    let mut handles = Vec::new();
    const BENCHMARK_PATH: &str = "/benchmark";

    // Delete old benchmark node if it exists
    if let Ok(Some(prev_benchmark_node)) = zk.exists(BENCHMARK_PATH, false) {
        zk.delete(BENCHMARK_PATH, Some(prev_benchmark_node.version))
            .expect("Failed to delete old benchmark node");
    }

    zk.create(
        BENCHMARK_PATH,
        vec![],
        Acl::open_unsafe().clone(),
        CreateMode::Persistent,
    )
    .expect("Failed to create parent dir");

    let counter = Arc::new(AtomicUsize::new(0));

    for _ in 0..1000 {
        let zk_clone = Arc::clone(&zk);
        let start_time_clone = start_time;
        let counter_clone = Arc::clone(&counter);

        let handle = thread::spawn(move || {
            while start_time_clone.elapsed() < duration {
                let node_id = counter_clone.fetch_add(1, Ordering::Relaxed);
                // Create an ephemeral sequential znode
                zk_clone
                    .create(
                        &format!("{}/node_{}", BENCHMARK_PATH, node_id),
                        vec![],
                        Acl::open_unsafe().clone(),
                        CreateMode::Ephemeral,
                    )
                    .expect("Error creating znode");
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Get number of children and print them
    let num_children = zk.get_children(BENCHMARK_PATH, false).unwrap().len();
    println!("Total znodes added: {}", num_children);
    println!(
        "Operations per second: {:.2}",
        num_children as f64 / duration.as_secs_f64()
    );
    // Close the ZooKeeper connection
    zk.close().unwrap();
}

fn check_threads_running() {
    let mut handles = vec![];

    for i in 0..10000 {
        let handle = thread::spawn(move || {
            let thread_id = thread::current().id();
            println!("Thread {} is running with ID {:?}", i, thread_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
