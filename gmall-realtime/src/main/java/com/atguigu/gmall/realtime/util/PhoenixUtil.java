package com.atguigu.gmall.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/16 8:52
 * @LoginName:wang
 **/
public class PhoenixUtil {

    public static void upsertValue(DruidPooledConnection conn, String sinkTable, JSONObject data) throws SQLException {

        PreparedStatement preparedStatement = null;
        // 1. 拼接sql语句
        // upsert into db.table (xx,xx) values (xx,xx);
        StringBuilder sb = new StringBuilder();
        sb.append("upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + " (");
        Set<String> keySet = data.keySet();
        Collection<Object> values = data.values();
        sb.append(StringUtils.join(keySet,",") + ") values ('");
        sb.append(StringUtils.join(values,"','") + "')");
        String sql = sb.toString();
        System.out.println(sql);
        // 2. 预编译sql
        preparedStatement = conn.prepareStatement(sql);

        // 3. 执行
        preparedStatement.execute();

        // 4. 释放资源
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                System.out.println("释放资源异常");
            }
        }


    }
}
