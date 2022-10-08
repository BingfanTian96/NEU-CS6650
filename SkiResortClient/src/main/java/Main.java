import models.LiftData;
import models.Phase;
import models.SendResult;
import threads.Consumer;
import threads.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @projectName: SkiResortClient
 * @package: PACKAGE_NAME
 * @className: Main
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10/2/22 11:10 PM
 * @version: 1.0
 */
public class Main {
    private static final int MAX_QUEUE_CAPACITY = 500;
    private static final int PHASE1_THREAD_COUNT = 32;
    private static final int PHASE2_THREAD_COUNT = 84;
    private static final int PHASE1_REQUEST_COUNT = 1000;
    private static final int PHASE2_REQUEST_COUNT = 2000;
    private static final int TOTAL_COUNT = 200000;

    public static void main(String[] args) throws InterruptedException {

        long start = System.currentTimeMillis();
        BlockingQueue<LiftData> blockingQueue = new LinkedBlockingDeque<>(MAX_QUEUE_CAPACITY);
        List<Thread> threads = new ArrayList<>();
        SendResult sendResult = new SendResult();
        // create 1 producer thread to generate lift data
        Producer producer = new Producer(blockingQueue, TOTAL_COUNT);
        Thread producerThread = new Thread(producer);
        producerThread.start();

        // create multi consumer thread to send request
        // phase 1
        runPhase(PHASE1_THREAD_COUNT, blockingQueue, PHASE1_REQUEST_COUNT, sendResult);
        System.out.println("------------------Phase1 output------------------");
        long phase1 = System.currentTimeMillis();
        long phase1WallTime = phase1 - start;
        int phase1Success = sendResult.getSuccessfulPosts();
        int phase1Failed = sendResult.getFailedPosts();
        long phase1Throughput = 1000L * (phase1Success + phase1Failed) / phase1WallTime;
        System.out.println("Phase 1 Time takes: " + phase1WallTime + "ms");
        System.out.println("Number of thread: " + PHASE1_THREAD_COUNT);
        System.out.println("Number of successful requests sent: " + phase1Success);
        System.out.println("Number of unsuccessful requests: " + phase1Failed);
        System.out.println("The phase 1 throughput in requests per second " + phase1Throughput);

        // phase 2
        runPhase(PHASE2_THREAD_COUNT, blockingQueue, PHASE2_REQUEST_COUNT, sendResult);
        System.out.println("------------------Phase2 output------------------");
        long phase2 = System.currentTimeMillis();
        long phase2WallTime = phase2 - phase1;
        int phase2Success = sendResult.getSuccessfulPosts() - phase1Success;
        int phase2Failed = sendResult.getFailedPosts() - phase1Failed;
        long phase2Throughput = 1000L * (phase1Success + phase1Failed) / phase1WallTime;
        System.out.println("Phase 2 Time takes: " + phase2WallTime + "ms");
        System.out.println("Number of thread: " + PHASE2_THREAD_COUNT);
        System.out.println("Number of successful requests sent: " + phase2Success);
        System.out.println("Number of unsuccessful requests: " + phase2Failed);
        System.out.println("The phase 2 throughput in requests per second " + phase2Throughput);

        // Part1 output
        long end = System.currentTimeMillis();
        System.out.println("------------------Part1 output------------------");
        long wallTime = end - start;
        int success = sendResult.getSuccessfulPosts();
        int failed = sendResult.getFailedPosts();
        long totalThroughput = 1000L * (success + failed) / wallTime;
        System.out.println("Time takes: " + (end - start) + "ms");
        System.out.println("Number of successful requests sent: " + success);
        System.out.println("Number of unsuccessful requests: " + failed);
        System.out.println("The total run time(wall time): " + wallTime + " milliseconds");
        System.out.println("The total throughput in requests per second " + totalThroughput);
    }

    private static void runPhase(int threadCount, BlockingQueue<LiftData> queue, int requestCount, SendResult result)
            throws InterruptedException {
        CountDownLatch phase1Latch = new CountDownLatch(threadCount);
        Phase phase = new Phase(threadCount, queue, requestCount, phase1Latch, result);
        phase.run();
        phase.await();
    }
}
