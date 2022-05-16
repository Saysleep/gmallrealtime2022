package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/16 21:04
 * @LoginName:wang
 **/
public class CheckNewFunction extends RichMapFunction<JSONObject, JSONObject> {

    private ValueState<String> valueState;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("is_new",String.class));
    }

    @Override
    // 待处理数据
    // {"common":{"ar":"530000","ba":"Huawei","ch":"web","is_new":"1","md":"Huawei Mate 30","mid":"mid_218013","os":"Android 10.0","uid":"328","vc":"v2.1.134"},
    // "start":{"entry":"icon","loading_time":13441,"open_ad_id":19,"open_ad_ms":6941,"open_ad_skip_ms":2256},
    // "ts":1592140987000}


    public JSONObject map(JSONObject value) throws Exception {
        // 新老用户校验处理逻辑
        // a) 如果is_new的值为1且键控状态为null 则将当前ts更新到状态中 (新用户)
        // b) 如果is_new的值为1且键控状态不为null 则要判断状态中的ts与当前ts是否为同一天 如果不是同一天 要改变为老用户 反之不做修改
        // c) 如果is_new的值为0且键控状态为null 说明是老用户但在该状态定义前就已登录,将状态中的值设为昨日


        //获取状态中的日期和当前数据的时间戳
        Long tsNow = value.getLong("ts");
        String dateLast = valueState.value();

        //开始判定is_new的值
        if ("1".equals(value.getJSONObject("common").getString("is_new"))) {
            if (dateLast == null) {
                valueState.update(DateFormatUtil.toDate(tsNow));
            } else {
                String dataNow = DateFormatUtil.toDate(tsNow);
                if (!dateLast.equals(dataNow)) {
                    value.getJSONObject("common").put("is_new", "0");
                }
            }
        } else if (dateLast == null) {
            valueState.update(DateFormatUtil.toDate(tsNow - 24 * 60 * 60 * 1000L));
        }

        return value;
    }
}
