package com.atguigu.gmall.realtime.common;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/14 23:44
 * @LoginName:wang
 **/
public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL2022_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
