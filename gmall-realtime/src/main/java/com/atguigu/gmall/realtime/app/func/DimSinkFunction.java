package com.atguigu.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.DruidUtil;
import com.atguigu.gmall.realtime.util.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/16 10:11
 * @LoginName:wang
 **/
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private static DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        druidDataSource = DruidUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        // 借一个连接
        DruidPooledConnection conn = druidDataSource.getConnection();

        // 写数据
        String sinkTable = value.getString("sinkTable");
        value.remove("sinkTable");
        JSONObject data = value;
        try {
            PhoenixUtil.upsertValue(conn,sinkTable,data);
        } catch (SQLException e) {
            e.printStackTrace();

        }

        // 把连接还回去
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
