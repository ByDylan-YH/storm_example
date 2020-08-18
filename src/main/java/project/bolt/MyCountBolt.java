package project.bolt;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import project.utils.MyDateUtils;
import project.utils.MyDbUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Author:BYDylan
 * Date:2020/5/11
 * Description:负责对数据进行临时局部汇总，定时写入到mysql数据库中
 */
public class MyCountBolt extends BaseRichBolt {
    private HashMap<String, Integer> hashMap = new HashMap<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)) {
            System.err.println("时间到，开始入库...");
//            定时把局部汇总的数据保存到数据库中
            for (Entry<String, Integer> entry : hashMap.entrySet()) {
                System.out.println(entry);
                String key = entry.getKey();
                Integer value = entry.getValue();
                MyDbUtils.update("insert into city_view(city,count,time) values(?,?,?)", key, value, MyDateUtils.formatDate2(new Date()));
            }
//            最后一定要记得清空 hashmap 中的数据
            hashMap.clear();
        } else {
//            正常的tuple数据处理
            String city = input.getStringByField("city");
            Integer value = hashMap.get(city);
            if (value == null) {
                value = 0;
            }
            value++;
            hashMap.put(city, value);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    //    设置定时任务
    @Override
    public Map<String, Object> getComponentConfiguration() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return map;
    }
}
