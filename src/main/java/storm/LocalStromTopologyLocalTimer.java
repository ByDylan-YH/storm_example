package storm;

import org.apache.storm.Config;
import org.apache.storm.Constants;
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

import java.util.HashMap;
import java.util.Map;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:局部的定时器
 */
public class LocalStromTopologyLocalTimer {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutid", new MySpout());
        builder.setBolt("bolt-1", new SumBolt()).shuffleGrouping("spoutid");
        StormTopology createTopology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        String topologyName = LocalStromTopologyLocalTimer.class.getSimpleName();
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
            System.out.println("spout: " + i);
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
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
                System.out.println("执行定时任务");
            } else {
                Integer num = input.getIntegerByField("num");
                sum += num;
                System.out.println("sum: " + sum);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        //		给指定组件设置定时任务,局部的
        @Override
        public Map<String, Object> getComponentConfiguration() {
            HashMap<String, Object> hashMap = new HashMap<>();
            hashMap.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
            return hashMap;
        }
    }
}
