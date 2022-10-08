package threads;

import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import models.LiftData;
import models.SendResult;

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
    private static int RETRY_TIMES = 5;
    private String BASE_PATH = "http://35.91.151.21:8080/SkiResortServlet_war/";

    /**
     * Instantiates a new Consumer.
     *
     * @param blockingQueue the blocking queue
     * @param reqCount      the req count
     * @param latch         the latch
     * @param result        the result
     */
    public Consumer(BlockingQueue<LiftData> blockingQueue, int reqCount, CountDownLatch latch, SendResult result) {
        this.blockingQueue = blockingQueue;
        this.reqCount = reqCount;
        this.latch = latch;
        this.subLatch = new CountDownLatch(this.reqCount);
        this.result = result;
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
        latch.countDown();
    }

    private void sendRequest(LiftData tmp) {
        if (tmp != null) {
            ApiClient client = new ApiClient();
            // todo: Update base path after deploy
            // localhost:
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
                        result.addSuccessfulPost(1);
                        // todo write successful records before break here
                        break;
                    }
                    if (i == RETRY_TIMES - 1) {
                        result.addFailedPost(1);
                    }
                } catch (ApiException e) {
                    // todo write failed records before break here
                    result.addFailedPost(1);
                    System.err.println("Exception when calling SkierApi#writeNewLiftRide, tried " + i + " times");
                    e.printStackTrace();
                }
            }
        }
    }
}
