import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The type Processor.
 *
 * @className: Thread
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10 /23/22 11:41 PM
 */
public class Processor implements Runnable {

    private final String QUEUE_NAME = "SkierPostQueue";
    /**
     * The Gson.
     */
    private Gson gson = new Gson();
    /**
     * The Map.
     */
    private JedisPool pool;

    /**
     * The Connection.
     */
    private Connection connection;

    /**
     * Instantiates a new Processor.
     *
     * @param connection the connection
     * @param pool       the pool
     */
    public Processor(Connection connection, JedisPool pool) {
        this.connection = connection;
        this.pool = pool;
    }

    @Override
    public void run() {
        try (Jedis jedis = pool.getResource()) {
            final Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            channel.basicQos(1);

            System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
//                System.out.println(" [x] Received '" + message + "'");
                // get json obj from the queue
                JsonObject json = gson.fromJson(message, JsonObject.class);
                // get key
                String skierId = String.valueOf(json.get("skierId"));
                String resortID = String.valueOf(json.get("resortID"));
                String seasonId = String.valueOf(json.get("seasonId"));
                String dayId = String.valueOf(json.get("dayId"));
                String liftId = String.valueOf(json.get("liftId"));
                String time = String.valueOf(json.get("time"));
                String vertical = String.valueOf(Integer.parseInt(liftId) * 10);

                if(map.containsKey(key)){
                    map.get(key).add(json.toString());
                } else{
                    List<String> value = new ArrayList<>();
                    value.add(json.toString());
                    map.put(key, value);
                }
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//                System.out.println( "Callback thread ID = " + java.lang.Thread.currentThread().getId() +
//                        " Received lift date for skier '" + key + "'");
            };
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });

        } catch (IOException e) {
            Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, e);
        }

    }
}
