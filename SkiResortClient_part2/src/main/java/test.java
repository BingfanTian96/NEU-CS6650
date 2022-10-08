import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import models.LiftData;

/**
 * @className: test
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10/4/22 2:34 AM
 */
public class test {
    public static void main(String[] args) {
        LiftRide liftRide = new LiftRide();
        liftRide.setLiftID(10);
        liftRide.setTime(10);
        LiftData tmp = new LiftData(1, "2022", "2", 12, liftRide);
        ApiClient client = new ApiClient();
        // todo: set base path after deploy
        String basePass = "http://35.91.151.21:8080/SkiResortServlet_war/";
        client.setBasePath(basePass);
        SkiersApi api = new SkiersApi();
        api.setApiClient(client);
        long startTime = System.currentTimeMillis();
        int success = 0;
        int failed = 0;
        for (int i = 0; i < 10000; i++) {
            try {
                ApiResponse<Void> res = api.writeNewLiftRideWithHttpInfo(tmp.getLiftRide(),
                        tmp.getResortID(), tmp.getSeasonID(), tmp.getDayID(), tmp.getSkierID());
                success++;
            } catch (ApiException e) {
                failed++;
                System.err.println("Exception when calling SkiersApi#writeNewLiftRideWithHttpInfo");
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        long wallTime = endTime - startTime;
        long throughPut = 1000L * (success + failed) / wallTime;

        System.out.println("10000 single thread Result:");
        System.out.println("-----------------------------------------------");
        System.out.println("Number of successful requests sent: " + success);
        System.out.println("Number of unsuccessful requests: " + failed);
        System.out.println("The total run time(wall time): " + wallTime + " milliseconds");
        System.out.println("The total throughput in requests per second " + throughPut);
    }
}
