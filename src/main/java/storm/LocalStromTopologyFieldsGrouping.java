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
 * Description:按字段分组,比如按userid来分组,具有同样userid的tuple会被分到同一任务,而不同的userid则会被分配到不同的任务
 */
public class LocalStromTopologyFieldsGrouping {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutid", new MySpout());
//        注意:如果bolt线程的数量大于spout中数据的分类数量,那么多余的线程不处理数据;
//        如果数据分类数量大于线程的数据,那么会存在一个线程处理多个分类的数据,数据不会丢失
        builder.setBolt("bolt-1", new SumBolt(), 3).fieldsGrouping("spoutid", new Fields("flag"));
        StormTopology createTopology = builder.createTopology();
        LocalCluster localCluster = new LocalCluster();
        String topologyName = LocalStromTopologyFieldsGrouping.class.getSimpleName();
        Config config = new Config();
        config.put("name", "zs");
        localCluster.submitTopology(topologyName, config, createTopology);
    }

    public static class MySpout extends BaseRichSpout {

        private static final long serialVersionUID = 1L;
        private SpoutOutputCollector collector;//收集器，主要负责向外面发射数据

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int i = 1;

        @Override
        public void nextTuple() {
            System.out.println("spout:" + i);
            this.collector.emit(new Values(i % 2, i++));
            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("flag", "num"));
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
