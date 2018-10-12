import org.junit.Test;
import redis.clients.jedis.Client;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.ScanResult;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisClusterTest {
    String prefix = "antifraud:rule";
    String KEY_SPLIT = ":"; //用于隔开缓存前缀与缓存键值
    String nameKey = "antifraud:rule";
    public void testCluster (){
        //ApplicationContext ctx = new AnnotationConfigApplicationContext(RedisClusterTest.class);
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        String prefix = "antifraud:rule";
        long a = jedisCluster.setnx(prefix, "2016-3-22");
        System.out.println(a);
        String value = jedisCluster.get("antifraud:rule");
        System.out.println(value);

        jedisCluster.del("antifraud:rule");
        try {
            jedisCluster.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * setnx : 如果key存在，返回0，如果不存在，则设置成功。
     * setnx的意思是set if not exist.
     */
    public void setnxTest(){
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        String nameKey = "antifraud:rule";
        System.out.println(jedisCluster.setnx(nameKey, "san"));//key不存在，返回值为1
        System.out.println(jedisCluster.get(nameKey));

        System.out.println(jedisCluster.setnx(nameKey, "张三"));//已经存在，返回值为0
        System.out.println(jedisCluster.get(nameKey));

        try {
            jedisCluster.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 简单字符串读写,带过期时间
     */
    @Test
    public void setexTest()  {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        System.out.println(jedisCluster.setex(nameKey, 3, "张三"));//时间单位是秒
        for(int i = 0 ; i < 5 ; i ++){
            System.out.println(jedisCluster.get(nameKey));//过期以后redis集群自动删除
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 操作子字符串
     */
    @Test
    public void setrangeTest() throws InterruptedException {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        System.out.println(jedisCluster.setex(nameKey, 20, "852026881@qq.com"));
        System.out.println(jedisCluster.get(nameKey));//结果：852026881@qq.com

        //从offset=8开始替换字符串value的值
        System.out.println(jedisCluster.setrange(nameKey, 8, "abc"));//结果：85202688abcq.com
        System.out.println(jedisCluster.get(nameKey));

        System.out.println(jedisCluster.setrange(nameKey, 8, "abcdefghhhhhh"));//结果：85202688abcdefghhhhhh
        System.out.println(jedisCluster.get(nameKey));

        //查询子串,返回startOffset到endOffset的字符
        System.out.println(jedisCluster.getrange(nameKey, 2, 5));//结果：2026
    }

    /**
     *  incrf:
     *  将 key 中储存的数字值增一。

     如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。

     如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。

     本操作的值限制在 64 位(bit)有符号数字表示之内。

     这是一个针对字符串的操作，因为 Redis 没有专用的整数类型，所以 key 内储存的字符串被解释为十进制 64 位有符号整数来执行 INCR 操作。

     返回值：     执行 INCR 命令之后 key 的值。

     这里有问题，最终数据结果大于10000    后续在研究 TODO
     这是因为设置的超时时间太小了，他去重试了，所以最终结果大于10000
     */
    @Test
    public void incrTest() throws InterruptedException {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        System.out.println(jedisCluster.setex(nameKey, 20, "852026881@qq.com"));
        /**
         * 测试线程安全
         */
        //JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        jedisCluster.del(nameKey);
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        for(int i = 0 ; i < 10 ; i ++){
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    //每个线程增加1000次，每次加1
                    for(int j = 0 ; j < 10 ; j ++){
                        atomicInteger.incrementAndGet();
                        jedisCluster.incr("incrNum");
                    }

                    countDownLatch.countDown();
                }
            });
        }

        countDownLatch.await();
        System.out.println(jedisCluster.get("incrNum"));
        System.out.println(atomicInteger);
    }
    /**
     * 模拟先进先出队列
     * 生产者 消费者
     */
    @Test
    public void queue() throws InterruptedException {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        String key = prefix + KEY_SPLIT + "queue";
        jedisCluster.del(key);

        System.out.println(jedisCluster.lpush(key, "1", "2", "3"));
        System.out.println(jedisCluster.lpush(key, "4"));
        System.out.println(jedisCluster.lpush(key, "5"));
        System.out.println("lrange:" + jedisCluster.lrange(key, 0, -1));

        System.out.println("lindex[2]:" + jedisCluster.lindex(key, 2));
        //在“3”的前面插入“100”
        System.out.println("linsert:" + jedisCluster.linsert(key, Client.LIST_POSITION.BEFORE, "3", "100"));
        System.out.println("lrange:" + jedisCluster.lrange(key, 0, -1));

        //写进去的顺序是12345，读取出来的也是12345
        for(int i = 0 ; i< 6 ; i ++){
            System.out.println(jedisCluster.rpop(key));
        }

//        如果想达到生产者消费者那种模式需要使用阻塞式队列才可。这个另外写多个客户端测试。
    }
    @Test
    public void hashTest() throws InterruptedException {
        String hashKey = "hashKey";
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        jedisCluster.del(hashKey);

        System.out.println(jedisCluster.hset(hashKey, "program_zhangsan", "111"));
        System.out.println(jedisCluster.hexists(hashKey, "program_zhangsan"));
        System.out.println(jedisCluster.hset(hashKey, "program_zhangsan", "222"));

        System.out.println(jedisCluster.hset(hashKey, "program_wangwu", "333"));
        System.out.println(jedisCluster.hset(hashKey, "program_lisi", "444"));
        ScanResult<Map.Entry<String, String>> me = jedisCluster.hscan(hashKey, 0);
        for(int i = 0; i < me.getResult().size(); i++){
            Map.Entry<String, String> entry= me.getResult().get(i);
            System.out.println("hscan entry:" + entry.getKey());
            System.out.println("hscan entry:" + entry.getValue());
        }

        System.out.println("hkeys:" + jedisCluster.hkeys(hashKey));

        System.out.println(jedisCluster.hgetAll(hashKey));
        System.out.println(jedisCluster.hincrBy(hashKey, "program_zhangsan", 2));
        System.out.println(jedisCluster.hmget(hashKey, "program_zhangsan", "program_lisi"));

        jedisCluster.hdel(hashKey, "program_wangwu");
        System.out.println(jedisCluster.hgetAll(hashKey));


        System.out.println("hsetnx:" + jedisCluster.hsetnx(hashKey, "program_lisi", "666"));

        System.out.println("hvals:" + jedisCluster.hvals(hashKey));

        System.out.println("expire:" + jedisCluster.expire(hashKey, 3));

        for(int i = 0 ; i < 5 ; i ++){
            System.out.println(jedisCluster.hgetAll(hashKey));
            Thread.sleep(1000);
        }

    }

    /**
     * Set集合
     */
    @Test
    public void setTest() throws InterruptedException {
        String prefix = "antifraud:rule";
        String KEY_SPLIT = ":"; //用于隔开缓存前缀与缓存键值
        String keyA = "{" + prefix  + "set}a";
        String keyB = "{" + prefix + "set}b";
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        jedisCluster.del(keyA);
        jedisCluster.del(keyB);

        System.out.println(jedisCluster.sadd(keyA, "a", "b", "c"));//给集合添加数据
        System.out.println(jedisCluster.sadd(keyA, "a"));//给集合添加数据.集合是不可以重复的
        System.out.println(jedisCluster.sadd(keyA, "d"));//给集合添加数据
        System.out.println(jedisCluster.smembers(keyA));//返回集合所有数据
        System.out.println(jedisCluster.scard(keyA));//返回集合长度
        System.out.println("c是否在集合A中：" + jedisCluster.sismember(keyA, "c"));//判断 member 元素是否集合 key 的成员。
        /*
        从 Redis 2.6 版本开始， SRANDMEMBER 命令接受可选的 count 参数：
如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
         */
        System.out.println(jedisCluster.srandmember(keyA));//返回集合中的一个随机元素。
        System.out.println(jedisCluster.spop(keyA)); //移除并返回集合中的一个随机元素。
        System.out.println(jedisCluster.smembers(keyA));//返回集合所有数据
        System.out.println("---------");

        /*
        SMOVE 是原子性操作。
如果 source 集合不存在或不包含指定的 member 元素，则 SMOVE 命令不执行任何操作，仅返回 0 。否则， member 元素从 source 集合中被移除，并添加到 destination 集合中去。
当 destination 集合已经包含 member 元素时， SMOVE 命令只是简单地将 source 集合中的 member 元素删除。
当 source 或 destination 不是集合类型时，返回一个错误。
注：不可以在redis-cluster中使用SMOVE：redis.clients.jedis.exceptions.JedisClusterException: No way to dispatch this command to Redis Cluster because keys have different slots.
解决办法可以参考上面的mset命令，使用“{}”来讲可以设置的同一个slot中
         */
        System.out.println(jedisCluster.smove(keyA, keyB, "a"));//返回集合所有数据
        System.out.println("keyA: "+jedisCluster.smembers(keyA));//返回集合所有数据
        System.out.println("keyB: "+jedisCluster.smembers(keyB));//返回集合所有数据

        System.out.println(jedisCluster.sadd(keyB, "a", "f", "c"));//给集合添加数据
        System.out.println(jedisCluster.sdiff(keyA, keyB));//差集 keyA-keyB
        System.out.println(jedisCluster.sinter(keyA, keyB));//交集
        System.out.println(jedisCluster.sunion(keyA, keyB));//并集
    }

    /**
     * sortedSet集合
     */
    @Test
    public void sortedSetTest() throws InterruptedException {
        String keyA = "{"+prefix + KEY_SPLIT + "sortedSet}a";
        String keyB = "{"+prefix + KEY_SPLIT + "sortedSet}b";
        String keyC = "{"+prefix + KEY_SPLIT + "sortedSet}c";
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        jedisCluster.del(keyA);
        jedisCluster.del(keyB);

        System.out.println(jedisCluster.zadd(keyA, 10, "aa"));
        Map<String, Double> map = new HashMap<>();
        map.put("b", 8.0);
        map.put("c", 4.0);
        map.put("d", 6.0);
        System.out.println(jedisCluster.zadd(keyA, map));
        System.out.println(jedisCluster.zcard(keyA));//返回有序集 key 的数量。
        System.out.println(jedisCluster.zcount(keyA, 3, 8));//返回有序集 key 中score某个范围的数量。
        System.out.println("zrange: "+jedisCluster.zrange(keyA, 0, -1));//返回有序集 key 中，指定区间内的成员。按score从小到大
        System.out.println("zrevrange: "+jedisCluster.zrevrange(keyA, 0, -1));//返回有序集 key 中，指定区间内的成员。按score从大到小
        System.out.println("zrangeWithScores: "+jedisCluster.zrangeWithScores(keyA, 0, -1));//返回有序集 key 中，指定区间内的成员。按score从小到大.包含分值

        System.out.println("zscore: "+jedisCluster.zscore(keyA, "aa"));

        /*
        返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score 值递增(从小到大)次序排列。
        具有相同 score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
         */
        System.out.println("zrangeByScore: "+jedisCluster.zrangeByScore(keyA, 3, 8));
        System.out.println("zrank: "+jedisCluster.zrank(keyA, "c"));//返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。
        System.out.println("zrevrank: "+jedisCluster.zrevrank(keyA, "c"));//返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从大到小)顺序排列。

        System.out.println("zrem: "+jedisCluster.zrem(keyA, "c", "a"));//移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。
        System.out.println("zrange: "+jedisCluster.zrange(keyA, 0, -1));



        System.out.println("zremrangeByRank: "+jedisCluster.zremrangeByRank(keyA, 1, 2));//按下标删除
        System.out.println("zrange: "+jedisCluster.zrange(keyA, 0, -1));
        System.out.println("zremrangeByScore: "+jedisCluster.zremrangeByScore(keyA, 1, 3));//按评分删除
        System.out.println("zrange: "+jedisCluster.zrange(keyA, 0, -1));

        /*
        接下来这几个操作，需要使用"{}"使得key落到同一个slot中才可以
         */
        System.out.println("-------");
        System.out.println(jedisCluster.zadd(keyB, map));
        System.out.println("zrange: "+jedisCluster.zrange(keyB, 0, -1));
        /*
        ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
计算给定的一个或多个有序集的并集，其中给定 key 的数量必须以 numkeys 参数指定，并将该并集(结果集)储存到 destination 。
默认情况下，结果集中某个成员的 score 值是所有给定集下该成员 score 值之 和 。
WEIGHTS
使用 WEIGHTS 选项，你可以为 每个 给定有序集 分别 指定一个乘法因子(multiplication factor)，每个给定有序集的所有成员的 score 值在传递给聚合函数(aggregation function)之前都要先乘以该有序集的因子。
如果没有指定 WEIGHTS 选项，乘法因子默认设置为 1 。
AGGREGATE
使用 AGGREGATE 选项，你可以指定并集的结果集的聚合方式。
默认使用的参数 SUM ，可以将所有集合中某个成员的 score 值之 和 作为结果集中该成员的 score 值；使用参数 MIN ，可以将所有集合中某个成员的 最小 score 值作为结果集中该成员的 score 值；而参数 MAX 则是将所有集合中某个成员的 最大 score 值作为结果集中该成员的 score 值。
         */
        System.out.println("zunionstore: "+jedisCluster.zunionstore(keyC, keyA, keyB));//合并keyA和keyB并保存到keyC中
        System.out.println("zrange: "+jedisCluster.zrange(keyC, 0, -1));
        System.out.println("zinterstore: "+jedisCluster.zinterstore(keyC, keyA, keyB));//交集
        System.out.println("zrange: "+jedisCluster.zrange(keyC, 0, -1));
    }

    /**
     * 列表 排序
     */
    @Test
    public void sort() throws InterruptedException {
        String key = prefix + KEY_SPLIT + "queue";
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        jedisCluster.del(key);

        System.out.println(jedisCluster.lpush(key, "1", "5", "3", "20", "6"));
        System.out.println(jedisCluster.lrange(key, 0, -1));

        System.out.println(jedisCluster.sort(key));
        System.out.println(jedisCluster.lrange(key, 0, -1));
    }

    @Test
    public void setList(){
        String keyredis = "testList";
        List list = new ArrayList();
        for (int i = 0;i<10;i++){
            IqProduct iqProduct = new IqProduct();
            iqProduct.setSort(1);
            iqProduct.setProductName(5555);
            list.add(iqProduct);
        }
        set(keyredis, list);
    }

    @Test
    public void getList(){
        String keyredis = "testList";
        Object object = get(keyredis);
        if(object !=null){
            List<IqProduct> iqProducts = (List<IqProduct>) object;
            for (int i = 0;i<iqProducts.size();i++){
                IqProduct iqProduct = iqProducts.get(i);
                System. out.println(iqProduct.getProductName());
                System. out.println(iqProduct.getSort());
            }
        }
    }

    public static void main(String[] args) {
        RedisClusterTest rt = new RedisClusterTest();
        //rt.testCluster();
        rt.setexTest();
    }

    /**set Object*/
    public String set(String key,Object object)
    {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        return jedisCluster.set(key.getBytes(), serialize(object));
    }

    /**get Object*/
    public Object get(String key)
    {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        byte[] value = jedisCluster.get(key.getBytes());
        return unSerialize(value);
    }

    /**delete a key**/
    public boolean del(String key)
    {
        JedisCluster jedisCluster = new RedisConfig().jedisCluster();
        return jedisCluster.del(key.getBytes())>0;
    }

    //序列化
    public static byte [] serialize(Object obj){
        ObjectOutputStream obi=null;
        ByteArrayOutputStream bai=null;
        try {
            bai=new ByteArrayOutputStream();
            obi=new ObjectOutputStream(bai);
            obi.writeObject(obj);
            byte[] byt=bai.toByteArray();
            return byt;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //反序列化
    public static Object unSerialize(byte[] byt){
        ObjectInputStream oii=null;
        ByteArrayInputStream bis=null;
        bis=new ByteArrayInputStream(byt);
        try {
            oii=new ObjectInputStream(bis);
            Object obj=oii.readObject();
            return obj;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }



}
