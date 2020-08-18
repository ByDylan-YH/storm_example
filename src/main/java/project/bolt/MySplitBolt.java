package project.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import project.utils.PageUtil;

import java.util.Map;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:切割每一行数据,获取ip字段,根据ip获取对应的城市信息
 */
public class MySplitBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
//            根据声明的输出字段获取数据
//            27.19.74.143 - - [30/May/2018:17:38:20 +0800] "GET /static/image/common/faq.gif HTTP/1.1" 200 1127
//            String line = input.getStringByField("str");
//            不清楚直接用角标获取
            String line = input.getString(0);
            String[] words = line.split(" ");
            String ip = words[0];
//            根据ip获取对应的城市信息
//            ["中国","北京","北京","","鹏博士"]
            String content = PageUtil.getContent("https://freeapi.ipip.net/" + ip);
            String[] split = content.replaceAll("\"", "").split(",");
            if (split.length == 5) {
//                只保留国内的访问数据,并且过滤掉异常数据
                if (split[0].trim().equals("[中国") && !split[2].trim().equals("")) {
                    this.collector.emit(new Values(split[2]));
                }
            }
            this.collector.ack(input);
        } catch (Exception e) {
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("city"));
    }
}
