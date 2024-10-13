package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;

public class ZookeeperBenchmark {

    private static final String ZOOKEEPER_ADDRESS = "127.0.0.1:2181";
    private static final int SESSION_TIMEOUT = 10000;
    private static final String BENCHMARK_PATH = "/benchmark";
    private static final int THREAD_COUNT = 1500;
    private static final Duration TEST_DURATION = Duration.ofSeconds(10);

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        new ZookeeperBenchmark().runBenchmark();
        System.out.println("Done");

    }

    private void runBenchmark() throws IOException, KeeperException, InterruptedException {
        // Connect to ZooKeeper
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, event -> {
            // Default watcher does nothing
        });

        // Ensure the benchmark node exists
        Stat stat = zk.exists(BENCHMARK_PATH, false);
        if (stat == null) {
            zk.create(BENCHMARK_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Start time
        Instant startTime = Instant.now();

        // Thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        // Submit tasks
        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    while (Duration.between(startTime, Instant.now()).compareTo(TEST_DURATION) < 0) {
                        // Create a sequential znode
                        zk.create(
                                BENCHMARK_PATH + "/node_",
                                new byte[0],
                                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                CreateMode.EPHEMERAL_SEQUENTIAL
                        );
                    }
                } catch (KeeperException | InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            });
        }

        // Shutdown executor and wait for tasks to finish
        executorService.shutdown();
        executorService.awaitTermination(TEST_DURATION.getSeconds() + 5, TimeUnit.SECONDS);

        // Wait a moment for any remaining operations
        Thread.sleep(1000);

        // Get the number of children nodes
        int numChildren = zk.getAllChildrenNumber(BENCHMARK_PATH);

        // Output results
        System.out.println("Total znodes added: " + numChildren);
        double opsPerSecond = numChildren / (double) TEST_DURATION.getSeconds();
        System.out.printf("Operations per second: %.2f%n", opsPerSecond);

        // Close ZooKeeper connection
        zk.close();
    }
}
