package RMQPool;

import com.rabbitmq.client.Channel;
import org.apache.commons.pool2.ObjectPool;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.pool2.impl.GenericObjectPool;

/**
 * @className: RMQChannelPool
 * @author: Bingfan Tian
 * @description: TODO
 * @date: 10/22/22 6:21 PM
 */
public class RMQChannelPool {

    // used to store and distribute channels
    private final BlockingQueue<Channel> pool;
    // fixed size pool
    private int capacity;
    // used to ceate channels
    private RMQChannelFactory factory;


    public RMQChannelPool(int maxSize, RMQChannelFactory factory) {
        this.capacity = maxSize;
        pool = new LinkedBlockingQueue<>(capacity);
        this.factory = factory;
        for (int i = 0; i < capacity; i++) {
            Channel channel;
            try {
                channel = factory.create();
                pool.put(channel);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(RMQChannelPool.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    public Channel borrowObject() throws IOException {
        try {
            return pool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error: no channels available" + e.toString());
        }
    }

    public void returnObject(Channel channel) throws Exception {
        if (channel != null) {
            pool.add(channel);
        }
    }

    public void close() {
        pool.clear();
    }
}
