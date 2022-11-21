import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * The type Consumer.
 *
 * @className: Consumer
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10 /23/22 11:36 PM
 */
public class Consumer {
    private final static Integer NUM_THREADS = 8;

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException      the io exception
     * @throws TimeoutException the timeout exception
     */
    public static void main(String[] args) throws IOException, TimeoutException {


//        HashMap<Integer, List<String>> map = new HashMap<>();
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(32);
        JedisPool pool = new JedisPool(config, "localhost", 6379);
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();

        for(int i = 0; i < NUM_THREADS; i++) {
            Thread cur = new Thread(new Processor(connection, pool));
            cur.start();
        }

    }


}
