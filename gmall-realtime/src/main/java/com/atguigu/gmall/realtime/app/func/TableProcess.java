package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.SourceTableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/14 19:00
 * @LoginName:wang
 **/
public class TableProcess extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    // 定义广播连接流的状态
    private MapStateDescriptor<String, SourceTableProcess> mapStateDescriptor;

    // 定义Phoenix的连接
    private Connection conn;

    // 初始化方法中创建与Phoenix的连接(一个并行度一个连接)
    @Override
    public void open(Configuration parameters) throws Exception {
        // Phoenix使用的为JDBC连接 使用JDBC前要注册驱动 -> 获取connection连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public TableProcess(MapStateDescriptor<String, SourceTableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //待处理数据(JSON)
    //{"database":"gmall-211126-flink","table":"base_trademark","type":"insert","ts":1652499161,"xid":167,"commit":true,"data":{"id":13,"tm_name":"atguigu","logo_url":"/aaa/aaa"}}
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        // 1. 获取广播的配置数据
        // 处理主流数据 获取表名与状态中的表名进行比对 获取到则进行下一步过滤 否则置空
        String sourceTable = jsonObj.getString("table");
        ReadOnlyBroadcastState<String, SourceTableProcess> tableConfigState = ctx.getBroadcastState(mapStateDescriptor);
        SourceTableProcess tableConfig = tableConfigState.get(sourceTable);
        if (tableConfig != null) {
            // 获取主流字段以及从状态中获取sinkTable
            JSONObject data = jsonObj.getJSONObject("data");
            String sinkTable = tableConfig.getSinkTable();


        // 2. 过滤字段filterColumn

            String sinkColumns = tableConfig.getSinkColumns();
            filterColumn(data,sinkColumns);

        // 3. 补充SinkTable字段输出

            data.put("sinkTable",sinkTable);
            out.collect(data);
        }




    }



    //待处理数据(String)
    //{"before":null,"after":{"source_table":"aa","sink_table":"bb","sink_columns":"cc","sink_pk":"id","sink_extend":"xxx"},
    // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1652513039549,"snapshot":"false","db":"gmall-211126-config","sequence":null,"table":"table_process","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},
    // "op":"r","ts_ms":1652513039551,"transaction":null}
    //
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        try {
            // 1. 获取并解析数据 方便主流操作
            JSONObject jsonObject = JSONObject.parseObject(value);
            String after = jsonObject.getString("after");
            SourceTableProcess tableObj = JSONObject.parseObject(after, SourceTableProcess.class);
            if (tableObj != null && tableObj.getSourceTable() != null) {
                String sourceTable = tableObj.getSourceTable();
                String sinkColumns = tableObj.getSinkColumns();
                String sinkPk = tableObj.getSinkPk();
                String sinkExtend = tableObj.getSinkExtend();
                String sinkTable = tableObj.getSinkTable();

            // 2. 校验表是否存在 如果不存在则需要在Phoenix中建表 checkTable
               checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);

            // 3. 写入状态
                BroadcastState<String, SourceTableProcess> state = ctx.getBroadcastState(mapStateDescriptor);
                state.put(tableObj.getSourceTable(),tableObj);
            }
        } catch (Exception e) {
            value = "";
        }

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        // 连接HBASE(Phoenix) 拼接建表sql
        StringBuilder sql = new StringBuilder();

        // sql : create table if not exists GMALL2022_REALTIME.sinkTable(
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA + ".").append(sinkTable).append("(\n");

        // 初始化主键与扩展信息
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        // 拆解sinkColumns 得到建表字段
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            sql.append(columns[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columns[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columns.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        //sql.append(sinkExtend);
        String createStatement = sql.toString();

        // 为数据库操作对象赋默认值，执行建表 SQL
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(createStatement);
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println("建表失败 Cause By + " + "\n" + createStatement);
            throw new RuntimeException("建表异常 程序终止");
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    System.out.println("数据释放异常");
                }
            }
        }


    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        // 获取源字段
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        // 获取过滤条件字段集合
        List<String> sinkColumnsList = Arrays.asList(sinkColumns.split(","));

        while (iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if(! sinkColumns.contains(next.getKey())){
                iterator.remove();
            }
        }

    }
}
