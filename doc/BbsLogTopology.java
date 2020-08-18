package tech.xuwei.StormProj;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import kafka.api.OffsetRequest;
import tech.xuwei.StormProj.bolt.MyCountBolt;
import tech.xuwei.StormProj.bolt.MySplitBolt;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:日志实时计算程序
 * Storm-kafka 1.2.3
 */
public class BbsLogTopology {
	
	public static void main(String[] args) {
		//	组装topology
		TopologyBuilder builder = new TopologyBuilder();
		
		BrokerHosts hosts = new ZkHosts("hadoop100:2181");//kafka的zk地址信息
		String topic = "access_log";//topic名称
		String zkRoot = "/kafkaSpoutData";//指定一个zk的根节点，后期storm会向这个节点下面存储信息
		String id = "group_1";//类似于groupid
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
		//指定数据的序列化方式-string类型
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		/*
		 * 如果是第一次提交topology，因为zk中还没有保存消费的offset，所以默认是会从最老的数据开始消费的
		 * 但是在实际项目中，只需要处理最新的数据即可，只需要从当前时刻消费最新的数据即可。
		 * 所以在这里可以这样设置，让storm第一次的时候消费最新的数据，
		 * 以后重复提交任务的时候，就会根据之前在zk中保存的offset来进行消费了
		 * 
		 * 注意：这个参数只对第一次提交的topology任务有效(或者是zkRoot、id发生了变化的时候)，
		 * 当后期zk中存在zkRoot节点的时候，就会从zkRoot中读取具体的偏移量数据了
		 * 【/zkRoot/id/partition_0】
		 * 
		 */
		spoutConf.startOffsetTime = OffsetRequest.LatestTime();
		String SPOUT_ID = KafkaSpout.class.getSimpleName();
		String SPLIT_ID = MySplitBolt.class.getSimpleName();
		String COUNT_ID = MyCountBolt.class.getSimpleName();
		
		/*
		 * 如何重跑kafka中的数据？
		 * 
		 * 只需要修改zk中存储的partition的offset信息
		 * 修改的命令如下[先停止对应的topology，然后修改zk节点的值，最后再重新提交topology]
		 * set /kafkaSpoutData/group_1/partition_0 {"topology":{"id":"BbsLogTopology-5-1559130069","name":"BbsLogTopology"},"offset":4222,"partition":0,"broker":{"host":"hadoop100","port":9092},"topic":"access_log"}
		 * ......需要对每个partition的文件都进行对应的修改
		 */
		
		builder.setSpout(SPOUT_ID, new KafkaSpout(spoutConf),5);//	spout的并行度和kafka中topic的分区个数保持一致即可
		builder.setBolt(SPLIT_ID, new MySplitBolt(),2).shuffleGrouping(SPOUT_ID);
		builder.setBolt(COUNT_ID, new MyCountBolt()).shuffleGrouping(SPLIT_ID);
		
		String topology_name = BbsLogTopology.class.getSimpleName();
		StormTopology createTopology = builder.createTopology();
		Config config = new Config();
		config.setMaxSpoutPending(1000);//	避免雪崩问题
		//config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx1024m");// 调整这个topology所使用的worker内存大小
		config.setNumWorkers(2);//可以在这里调整worker的数量，或者使用rebalance动态调整
		if(args.length==0) {//本地运行
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topology_name, config, createTopology);
		}else {//集群运行
			try {
				StormSubmitter.submitTopology(topology_name, config, createTopology);
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		}
		
	}

}
