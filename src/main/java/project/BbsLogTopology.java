package project;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import project.bolt.MyCountBolt;
import project.bolt.MySplitBolt;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:日志实时计算程序
 * Storm-kafka-client 2.1.0
 * 参考: http://blog.itpub.net/31506529/viewspace-2215095/
 */
public class BbsLogTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
//        该类将传入的 kafka 记录转换为storm 的 tuple
        ByTopicRecordTranslator<String, String> brt = new ByTopicRecordTranslator<>((r) -> new Values(r.value(), r.topic()), new Fields("values", "topic"));
//        设置要消费的topic
        brt.forTopic("access_log", (r) -> new Values(r.value(), r.topic()), new Fields("values", "topic"));
        KafkaSpoutConfig<String, String> ksc = KafkaSpoutConfig
//                bootstrapServers 以及topic
                .builder("bigdata01:9092,bigdata02:9092,bigdata03:9092", "access_log")
//                设置groupid
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "group_1")
//                设置开始消费的起始位置
                .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
//                设置提交消费边界的时长间隔
                .setOffsetCommitPeriodMs(10_000)
//                Translator
                .setRecordTranslator(brt)
                .build();

        String SPOUT_ID = KafkaSpout.class.getSimpleName();
        String SPLIT_ID = MySplitBolt.class.getSimpleName();
        String COUNT_ID = MyCountBolt.class.getSimpleName();

//        spout的并行度和kafka中topic的分区个数保持一致即可
        builder.setSpout(SPOUT_ID, new KafkaSpout<>(ksc), 5);
        builder.setBolt(SPLIT_ID, new MySplitBolt(), 2).shuffleGrouping(SPOUT_ID);
        builder.setBolt(COUNT_ID, new MyCountBolt()).shuffleGrouping(SPLIT_ID);
        Config config = new Config();
        config.setNumWorkers(2);
        config.setNumAckers(0);
//        避免雪崩问题
        config.setMaxSpoutPending(1000);
//        调整这个topology所使用的worker内存大小
//        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS,"-Xmx1024m");

        String topology_name = BbsLogTopology.class.getSimpleName();
        StormTopology createTopology = builder.createTopology();
//        可以在这里调整worker的数量,或者使用rebalance动态调整
        config.setNumWorkers(2);
        if (args.length == 0) {
            try {
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology(topology_name, config, createTopology);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                StormSubmitter.submitTopology(topology_name, config, createTopology);
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}