package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.CheckNewFunction;
import com.atguigu.gmall.realtime.util.MyKafkaUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/16 15:42
 * @LoginName:wang
 **/
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
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
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020//1126//ck");
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/


        // TODO 2. 读取kafka的topic_log主题数据 创建主流
        String topic = "topic_log";
        String groupId = "baselog_app_1126";
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtils.getKafkaConsumer(topic, groupId));

        // TODO 3. 过滤掉非json格式的数据 & 将每行数据转换为json对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });

        // TODO 4. 按照id分组
        KeyedStream<JSONObject, String> midKeySource = jsonObjDS.keyBy(value -> value.getJSONObject("common").getString("mid"));

        // TODO 5. 使用状态编程做新老用户校验
        SingleOutputStreamOperator<JSONObject> checkSource = midKeySource.map(new CheckNewFunction());
        // TODO 6. 使用侧输出流进行分流
        OutputTag<JSONObject> start = new OutputTag<JSONObject>("start"){};
        OutputTag<JSONObject> actions = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> displays = new OutputTag<JSONObject>("disploy"){};
        OutputTag<JSONObject> err = new OutputTag<JSONObject>("err"){};
        //第一种数据:{"common":{"ar":"530000","ba":"Huawei","ch":"web","is_new":"1","md":"Huawei Mate 30","mid":"mid_218013","os":"Android 10.0","uid":"328","vc":"v2.1.134"},
        // "start":{"entry":"icon","loading_time":13441,"open_ad_id":19,"open_ad_ms":6941,"open_ad_skip_ms":2256},
        // "ts":1592140987000}

        //第二种数据:"common":{"ar":"440000","ba":"Huawei","ch":"oppo","is_new":"1","md":"Huawei Mate 30","mid":"mid_183856","os":"Android 11.0","uid":"652","vc":"v2.1.132"},
        // "err":{"error_code":3753,"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)"}
        // ,"start":{"entry":"icon","loading_time":16766,"open_ad_id":15,"open_ad_ms":2988,"open_ad_skip_ms":1669},
        // "ts":1592140987000}

        //第三种数据:{"actions":[{"action_id":"favor_add","item":"31","item_type":"sku_id","ts":1592140990361},{"action_id":"get_coupon","item":"3","item_type":"coupon_id","ts":1592140991722}],
        // "common":{"ar":"530000","ba":"Huawei","ch":"web","is_new":"1","md":"Huawei Mate 30","mid":"mid_218013","os":"Android 10.0","uid":"328","vc":"v2.1.134"},
        // "displays":[{"display_type":"query","item":"14","item_type":"sku_id","order":1,"pos_id":4},{"display_type":"query","item":"24","item_type":"sku_id","order":2,"pos_id":4},{"display_type":"query","item":"10","item_type":"sku_id","order":3,"pos_id":2},{"display_type":"query","item":"6","item_type":"sku_id","order":4,"pos_id":4},{"display_type":"promotion","item":"9","item_type":"sku_id","order":5,"pos_id":4},{"display_type":"promotion","item":"12","item_type":"sku_id","order":6,"pos_id":3},{"display_type":"query","item":"5","item_type":"sku_id","order":7,"pos_id":2},{"display_type":"promotion","item":"3","item_type":"sku_id","order":8,"pos_id":5},{"display_type":"query","item":"33","item_type":"sku_id","order":9,"pos_id":3}],
        // "page":{"during_time":4084,"item":"31","item_type":"sku_id","last_page_id":"good_list","page_id":"good_detail","source_type":"promotion"},
        // "ts":1592140989000}

        //第四种数据:{"common":{"ar":"530000","ba":"Huawei","ch":"web","is_new":"1","md":"Huawei Mate 30","mid":"mid_218013","os":"Android 10.0","uid":"328","vc":"v2.1.134"},
        // "displays":[{"display_type":"activity","item":"1","item_type":"activity_id","order":1,"pos_id":4},{"display_type":"activity","item":"1","item_type":"activity_id","order":2,"pos_id":4},{"display_type":"promotion","item":"25","item_type":"sku_id","order":3,"pos_id":4},{"display_type":"promotion","item":"30","item_type":"sku_id","order":4,"pos_id":1},{"display_type":"query","item":"29","item_type":"sku_id","order":5,"pos_id":2},{"display_type":"query","item":"27","item_type":"sku_id","order":6,"pos_id":3},{"display_type":"promotion","item":"10","item_type":"sku_id","order":7,"pos_id":3},{"display_type":"query","item":"33","item_type":"sku_id","order":8,"pos_id":4}],
        // "page":{"during_time":15432,"page_id":"home"},
        // "ts":1592140987000}

        //第五种数据:{"actions":[{"action_id":"get_coupon","item":"1","item_type":"coupon_id","ts":1645536213259}],"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_296405","os":"iOS 13.3.1","uid":"255","vc":"v2.1.132"},
        // "displays":[{"display_type":"recommend","item":"21","item_type":"sku_id","order":1,"pos_id":4},{"display_type":"query","item":"33","item_type":"sku_id","order":2,"pos_id":2},{"display_type":"query","item":"7","item_type":"sku_id","order":3,"pos_id":2},{"display_type":"promotion","item":"14","item_type":"sku_id","order":4,"pos_id":3},{"display_type":"query","item":"16","item_type":"sku_id","order":5,"pos_id":4},{"display_type":"query","item":"31","item_type":"sku_id","order":6,"pos_id":1},{"display_type":"query","item":"2","item_type":"sku_id","order":7,"pos_id":5},{"display_type":"query","item":"4","item_type":"sku_id","order":8,"pos_id":4},{"display_type":"query","item":"9","item_type":"sku_id","order":9,"pos_id":1}],
        // "err":{"error_code":1357,"msg":" Exception in thread \\  java.net.SocketTimeoutException\\n \\tat com.atgugu.gmall2020.mock.bean.log.AppError.main(AppError.java:xxxxxx)"},"page":{"during_time":6518,"item":"14","item_type":"sku_id","last_page_id":"good_list","page_id":"good_detail","source_type":"activity"},
        // "ts":1645536210000}

        //第六种数据:{"common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"0","md":"iPhone 8","mid":"mid_296405","os":"iOS 13.3.1","uid":"255","vc":"v2.1.132"},
        // "page":{"during_time":3907,"item":"32","item_type":"sku_ids","last_page_id":"cart","page_id":"trade"},
        // "ts":1645536212000}
        SingleOutputStreamOperator<JSONObject> pageSource = checkSource.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                //先尝试提炼错误信息
                if (value.getString("err") != null) {
                    ctx.output(err, value);
                }

                //移除错误信息
                value.remove("err");

                //检查是否为启动日志
                if (value.getString("start") != null) {
                    ctx.output(start, value);
                } else {
                    //获取公共信息&页面id&时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    //尝试获取曝光数据
                    JSONArray displaysData = value.getJSONArray("displays");
                    if (displaysData != null) {
                        for (int i = 0; i < displaysData.size(); i++) {
                            JSONObject display = displaysData.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            ctx.output(displays, display);
                        }
                    }
                    //尝试获取动作数据
                    JSONArray actionsData = value.getJSONArray("actions");
                    if (actionsData != null && actionsData.size() > 0) {
                        //遍历曝光数据&写到display侧输出流
                        for (int i = 0; i < actionsData.size(); i++) {
                            JSONObject action = actionsData.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            ctx.output(actions, action);
                        }
                    }

                    //移除曝光和动作数据&写到页面日志主流
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value);

                }


            }
        });

        // TODO 7. 提取各个侧输出流数据
        SingleOutputStreamOperator<String> actionSideOutput = pageSource.getSideOutput(actions).map(value -> value.toJSONString());
        SingleOutputStreamOperator<String> errSideOutput = pageSource.getSideOutput(err).map(value -> value.toJSONString());
        SingleOutputStreamOperator<String> displaySideOutput = pageSource.getSideOutput(displays).map(value -> value.toJSONString());
        SingleOutputStreamOperator<String> startSideOutput = pageSource.getSideOutput(start).map(value -> value.toJSONString());

        // TODO 8. 将相应数据写入对应主题
        actionSideOutput.print("actions");
        errSideOutput.print("err");
        displaySideOutput.print("display");
        startSideOutput.print("start");
        SingleOutputStreamOperator<String> pageSideOutput = pageSource.map(value -> value.toJSONString());
        pageSideOutput.print("page");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        actionSideOutput.addSink(MyKafkaUtils.getKafkaProducer(action_topic));
        errSideOutput.addSink(MyKafkaUtils.getKafkaProducer(error_topic));
        displaySideOutput.addSink(MyKafkaUtils.getKafkaProducer(display_topic));
        startSideOutput.addSink(MyKafkaUtils.getKafkaProducer(start_topic));
        pageSideOutput.addSink(MyKafkaUtils.getKafkaProducer(page_topic));
        // TODO 9. 启动任务
        env.execute();
    }
}
