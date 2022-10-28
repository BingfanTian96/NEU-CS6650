package RMQPool;

import Models.LiftData;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * @className: Publisher
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10/27/22 1:40 PM
 */
public class Publisher {
    private ConnectionFactory factory;
    private ObjectPool<Channel> pool;
    private String hostName;
    private String QUEUE_NAME;

    private Connection connection;

    public Publisher(String hostName, String queue_Name) throws IOException, TimeoutException {
        this.factory = new ConnectionFactory();
        this.factory.setHost("localhost");
        this.factory.setPort(5672);
        this.factory.setUsername("guest");
        this.factory.setPassword("guest");
//        this.pool = new GenericObjectPool<Channel>(new RMQChannelFactory());
        this.hostName = hostName;
        this.QUEUE_NAME = queue_Name;
        this.connection = factory.newConnection();

    }

    public void send(LiftData liftData) {
        Channel channel = null;
        try {
            channel = pool.borrowObject();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            Gson gson = new Gson();
            channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,
                    gson.toJson(liftData).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException("Unable to borrow channel from pool" + e.toString());
        } finally {
            if (channel != null) {
                try {
                    pool.returnObject(channel);
                } catch (Exception e) {
                    System.out.println("Cannot return channel");
                }
            }
        }
    }

}
