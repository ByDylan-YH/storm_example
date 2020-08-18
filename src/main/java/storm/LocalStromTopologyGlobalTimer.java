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

import java.util.Map;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:全局定时任务
 */
public class LocalStromTopologyGlobalTimer {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutid", new MySpout());
        builder.setBolt("bolt-1", new SumBolt()).shuffleGrouping("spoutid");
        builder.setBolt("bolt-2", new SumBolt2()).shuffleGrouping("bolt-1");
        StormTopology createTopology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        String topologyName = LocalStromTopologyGlobalTimer.class.getSimpleName();
        Config config = new Config();
//        设置定时任务时间间隔为 3 s
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
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

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        int sum = 0;

        @Override
        public void execute(Tuple input) {
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
                System.out.println("SumBolt 1 执行定时任务");
            } else {
                Integer num = input.getIntegerByField("num");
                sum += num;
                System.out.println("sum1: " + sum);
                this.collector.emit(new Values(sum));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sum"));
        }
    }

    public static class SumBolt2 extends BaseRichBolt {

        private static final long serialVersionUID = 1L;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        }

        @Override
        public void execute(Tuple input) {
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
                System.out.println("SumBolt 2 执行定时任务");
            } else {
                Integer sum = input.getIntegerByField("sum");
                System.out.println("sum2: " + sum);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
