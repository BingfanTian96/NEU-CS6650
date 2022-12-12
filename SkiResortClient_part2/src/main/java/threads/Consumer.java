package threads;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import models.LiftData;
import models.Record;
import models.SendResult;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The type Consumer.
 *
 * @projectName: SkiResortClient
 * @package: NEU.cs6650.threads
 * @className: Customer
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 9 /28/22 2:54 PM
 * @version: 1.0
 */
public class Consumer implements Runnable{

    private BlockingQueue<LiftData> blockingQueue;
    private int reqCount;
    // total threads counter for this phase
    private CountDownLatch latch;
    // total requests counter for each thread
    private CountDownLatch subLatch;
    private SendResult result;
    private int RETRY_TIMES = 5;
    private String BASE_PATH = "http://localhost:8080/HW3_Server/";
//    private String BASE_PATH = "http://localhost:8080/HW2_Server_war_exploded/";
    private Queue<Record> records;

    private int success = 0;
    private int failed = 0;

    /**
     * Instantiates a new Consumer.
     *
     * @param blockingQueue the blocking queue
     * @param reqCount      the req count
     * @param latch         the latch
     * @param result        the result
     * @param records       the records
     */
    public Consumer(BlockingQueue<LiftData> blockingQueue, int reqCount, CountDownLatch latch,
                    SendResult result, Queue<Record> records) {
        this.blockingQueue = blockingQueue;
        this.reqCount = reqCount;
        this.latch = latch;
        this.subLatch = new CountDownLatch(this.reqCount);
        this.result = result;
        this.records = records;
    }


    @Override
    public void run() {
        consume();
    }

    private void consume() {
        while (subLatch.getCount() > 0) {
            try {
                LiftData value = blockingQueue.take();
                sendRequest(value);
                subLatch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
        for (int i = 0; i < success; i++)
            result.addSuccessfulPost(1);
        for (int i = 0; i < failed; i++)
            result.addFailedPost(1);
        latch.countDown();
    }

    private void sendRequest(LiftData tmp) {
        RETRY_TIMES = 5;
        if (tmp != null) {
            ApiClient client = new ApiClient();
            client.setBasePath(BASE_PATH);
            SkiersApi api = new SkiersApi();
            api.setApiClient(client);
            for (int i = 0; i < RETRY_TIMES; i++) {
                try {
                    long startTime = System.currentTimeMillis();
                    ApiResponse<Void> res = api.writeNewLiftRideWithHttpInfo(tmp.getLiftRide(),
                            tmp.getResortID(), tmp.getSeasonID(), tmp.getDayID(), tmp.getSkierID());
                    long endTime = System.currentTimeMillis();
                    if (res.getStatusCode() == 201 || res.getStatusCode() == 200) {
                        success++;
                        this.records.offer(new Record(startTime, "POST",
                                endTime - startTime, res.getStatusCode()));
                        break;
                    }
                    if (i == RETRY_TIMES - 1) {
                        failed++;
                        this.records.offer(new Record(startTime, "POST",
                                endTime - startTime, res.getStatusCode()));
                    }
                } catch (ApiException e) {
                    System.err.println("Exception when calling SkierApi#writeNewLiftRide, tried " + i + " times");
                    e.printStackTrace();
                }
            }
        }
    }
}
