package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/14 11:09
 * @LoginName:wang
 **/
public class MyKafkaUtils {

    private static final String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static final String DEFAULT_TOPIC = "default_topic";

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic , String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);

        return new FlinkKafkaConsumer<String>(
            topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[],byte[]> consumerRecord) throws Exception {
                        if (consumerRecord == null || consumerRecord.value() == null) {
                            return null;
                        } else{
                            return new String(consumerRecord.value());
                        }

                    }
                },
            properties
        );
    }
}
