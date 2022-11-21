import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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
    Gson gson = new Gson();
    /**
     * The Map.
     */
    HashMap<Integer, List<String>> map;

    /**
     * The Connection.
     */
    Connection connection;

    /**
     * Instantiates a new Processor.
     *
     * @param connection the connection
     * @param map        the map
     */
    public Processor(Connection connection, HashMap<Integer, List<String>> map) {
        this.connection = connection;
        this.map = map;
    }

    @Override
    public void run() {
        try {
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
                Integer key = Integer.valueOf(String.valueOf(json.get("skierId")));
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
