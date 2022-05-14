package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.MyKafkaUtils;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/14 10:33
 * @LoginName:wang
 **/
public class DimApp {
    public static void main(String[] args) {
        // TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//生产环境下设置为kafka该主题的分区数

        // 1.1 开启checkpoint 生产环境必备
/*      env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));*/

        // 1.2 设置状态后端
/*      env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020//1126//ck");*/

        // TODO 2. 读取kafka的topic_db主题数据 创建主流

        String topic = "topic_db";
        String groupId = "dim_app_1126";
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtils.getKafkaConsumer(topic, groupId));

        // TODO 3. 过滤掉非json数据以及保留新增/变化/初始化的数据

        kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取数据中的类型
                    String type = jsonObject.getString("type");
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e){
                    System.out.println("发现脏数据了诶" + value);
                }

            }
        });
        // TODO 4. 使用FlinkCDC读取mysql配置信息表 创建配置流

        // TODO 5. 将配置流处理为广播流

        // TODO 6. 连接配置流与主流 得到广播连接流

        // TODO 7. 根据配置信息处理广播连接流

        // TODO 8. 将数据写到Phoenix

        // TODO 9. 启动任务

    }
}
