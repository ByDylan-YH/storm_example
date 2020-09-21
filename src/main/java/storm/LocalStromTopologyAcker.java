package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
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
 * Description:在spout产生一条 tuple 时,会向 acker 发送一条信息,让 acker 来进行跟踪
 */
public class LocalStromTopologyAcker {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spoutid", new MySpout());
        builder.setBolt("bolt-1", new SumBolt()).shuffleGrouping("spoutid");
        StormTopology createTopology = builder.createTopology();
        String topologyName = LocalStromTopologyAcker.class.getSimpleName();
        Config config = new Config();
//        设置acker数量
        config.setNumAckers(2);
//        这个参数表示当下游的 bolt 还有topology.max.spout.pending 个 tuple 没有消费完时,spout会停止调用nexttuple方法发射数据;
//        等待下游bolt去消费,当 tuple 的个数少于topology.max.spout.pending 个数时, spout 会继续发射数据
//        这个属性只对可靠消息处理有用,也就是说需要启用acker消息确认机制,在spout中emit数据的时候需要带有messageid
        config.setMaxSpoutPending(100);
        if (args.length == 0) {
            LocalCluster localCluster = new LocalCluster();
            config.put("name", "zs");
            localCluster.submitTopology(topologyName, config, createTopology);
        } else {
            try {
                StormSubmitter.submitTopology(topologyName, config, createTopology);
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
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
//            默认acker消息确认机制是没有开启的,想要开启的话需要在emit方法中增加一个参数(messageId)
//            我们可以认为数据这个 tuple 数据的一个主键的id,因为 bolt 组件在返回数据处理状态的时候,只会返回数据对应的 messageid
//            所以这个messageid和数据之间的状态也是需要我们程序员自己维护的,我们需要保证可以根据 messageid 获取到对应的 tuple数据内容
            this.collector.emit(new Values(i++), i - 1);
            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }

        @Override
        public void ack(Object msgId) {
            System.out.println("ack 被调用了..." + msgId);
        }

        @Override
        public void fail(Object msgId) {
//            针对处理失败的数据，可以在这里实现一定的业务逻辑
            System.out.println("fail 被调用了..." + msgId);
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
            try {
                Integer num = input.getIntegerByField("num");
                sum += num;
                System.out.println("sum: " + sum);
//                确认tuple处理成功了
                this.collector.ack(input);
            } catch (Exception e) {
//                说明tuple数据处理失败了
                this.collector.fail(input);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }
    }
}
