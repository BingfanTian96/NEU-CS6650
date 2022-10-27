import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * The type Rmq channel factory.
 *
 * @className: RMQChannelFactory
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10 /22/22 5:06 PM
 */
public class RMQChannelFactory extends BasePooledObjectFactory<Channel>{

    /**
     * The Factory.
     */
    ConnectionFactory factory;

    private int count;

    /**
     * Instantiates a new Rmq channel factory.
     *
     */
    public RMQChannelFactory() {
        factory = new ConnectionFactory();
        this.count = 0;
        this.factory.setHost("localhost");
        this.factory.setPort(5672);
        this.factory.setUsername("admin");
        this.factory.setPassword("password");
    }

    @Override
    synchronized public Channel create() throws Exception {
        count++;
        Connection connection = factory.newConnection();
        return connection.createChannel();
    }

    @Override
    public PooledObject<Channel> wrap(Channel channel) {
        return new DefaultPooledObject<Channel>(channel);
    }

    @Override
    public void destroyObject(PooledObject<Channel> p) throws Exception {
        p.getObject().close();
    }

    /**
     * Gets channel count.
     *
     * @return the channel count
     */
    public int getChannelCount() {
        return count;
    }
}
