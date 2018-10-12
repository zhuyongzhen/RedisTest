import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class RedisConfig {

    public JedisCluster jedisCluster (){
        Set<HostAndPort> set = new HashSet<>();

        set.add(new HostAndPort("172.19.14.2", 7180));
        set.add(new HostAndPort("172.19.14.2", 7181));
        set.add(new HostAndPort("172.19.14.4", 7180));
        set.add(new HostAndPort("172.19.14.4", 7181));
        set.add(new HostAndPort("172.19.14.5", 7180));
        set.add(new HostAndPort("172.19.14.5", 7181));
        JedisCluster jedisCluster = new JedisCluster(set, 1000, 1000, 1, "testHadoop", new GenericObjectPoolConfig());
        return jedisCluster;
    }

}