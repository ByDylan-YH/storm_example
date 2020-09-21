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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Author:BYDylan
 * Date:2020/5/10
 * Description:Storm 实现数字累加求和
 * 分析: 1.需要一个spout负责源源不断的产生从1开始的递增数字
 * 2.需要一个bolt负责对spout产生的数据进行累加求和,并且把结果打印到控制台
 * 3.最后把这spout和bolt组装成一个topology
 */
public class LocalStormTopology {
    public static Logger LOGGER = LoggerFactory.getLogger(LocalStormTopology.class);
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        // 组装spout,名字不能用__开头,在同一个topology中spout和bolt名称不能相同
        builder.setSpout("spoutid", new Mysput());
        // 组装bolt,并制定接收哪一个组件的数据
        builder.setBolt("bolt-1", new SumBolt()).shuffleGrouping("spoutid");
        StormTopology createTopology = builder.createTopology();
        // 创建本地集群
        LocalCluster localCluster = new LocalCluster();
        String topologyName = LocalStormTopology.class.getSimpleName();
        Config config = new Config();
        config.setNumWorkers(2);
        // 把代码提交到本地集群
        localCluster.submitTopology(topologyName, config, createTopology);
    }

    public static class Mysput extends BaseRichSpout {
        private static final long serialVersionUID = 1L;
//        配置信息
        private Map conf;
//        上下文
        private TopologyContext context;
//        收集器,负责向外部发射数据
        private SpoutOutputCollector collector;

        /**
         * 是一个初始化的方法，这个方法在本实例运行的之后，首先被调用，仅且仅被调用一次 所以这个方法内一般放一些初始化的代码
         * 例子：针对操作mysql数据的案例，使用jdbc获取数据库连接的代码需要放到这里面实现
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int i = 1;

//        会被循环调用
        @Override
        public void nextTuple() {
            System.out.println("Spout: " + i);
//            针对需要发射的数据,需要封装成tuple对象,可以使用Storm中的values对象快速封装tuple
            this.collector.emit(new Values(i++));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /**
         * 声明输出字段 定义两个组件之间数据传输的一个规则
         * 注意：只要这个组件(spout/spout)向外发射了数据，那么这个 declareOutputFields 就需要实现
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            这里的字段列表和values中的数据列表要一致,相当于列名
            declarer.declare(new Fields("num"));
        }
    }

    public static class SumBolt extends BaseRichBolt {
        private static final long serialVersionUID = 2L;

//        初始化方法
        @Override
        public void prepare(Map stormconf, TopologyContext context, OutputCollector collector) {
        }

        int sum = 0;

//        循环被调用
        @Override
        public void execute(Tuple input) {
            Integer num = input.getIntegerByField("num");
            // input.getInteger(0); //通过角标获取
            sum += num;
            System.out.println("sum: " + sum);
        }

//        这里也可以不写,因为这个bolt没有向下一个组件发送数据
        @Override
        public void declareOutputFields(OutputFieldsDeclarer arg0) {
        }
    }
}
