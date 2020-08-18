package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:广播发送,对于每一个tuple,Bolts中的所有任务都会收到.
 */
public class LocalStromTopologyAllGrouping {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutid", new MySpout());
        builder.setBolt("bolt-1", new SumBolt(), 3).allGrouping("spoutid");
        StormTopology createTopology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        String topologyName = LocalStromTopologyAllGrouping.class.getSimpleName();
        Config config = new Config();
        config.put("name", "zs");
        localCluster.submitTopology(topologyName, config, createTopology);
    }

    public static class MySpout extends BaseRichSpout {

        private static final long serialVersionUID = 1L;
        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int i = 1;

        @Override
        public void nextTuple() {
            System.out.println("spout:" + i);
            this.collector.emit(new Values(i++));
            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

    public static class SumBolt extends BaseRichBolt {

        private static final long serialVersionUID = 1L;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        }

        int sum = 0;

        @Override
        public void execute(Tuple input) {
            Integer num = input.getIntegerByField("num");
            System.out.println(Thread.currentThread().getId() + "\t" + num);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
