package project;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class KafkaProducer {
    private static final String projectPath = System.getProperty("user.dir");

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop100:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "access_log";
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

//		读取文件中的数据,向kafka中输出数据
        BufferedReader buffr = new BufferedReader(new FileReader(projectPath + "\\doc\\access_2018_05_30.log"));
        String line;
        while ((line = buffr.readLine()) != null) {
            producer.send(new ProducerRecord<>(topic, line));
            Thread.sleep(1000);
        }
        buffr.close();
        producer.close();
    }
}
