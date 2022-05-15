package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @Author:Sukichan
 * @Description:TODO
 * @DateTime:2022/5/14 15:06
 * @LoginName:wang
 **/

@Data
public class SourceTableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
