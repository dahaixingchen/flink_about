package com.wsxd.cfs.km.flink.realTimeDataWarehouse.config;

/**
 * @ClassName: GlobalConfig
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/10 17:54
 * @Version 1.0
 **/
public class GlobalConfig {
    /**
     * 数据库driver class
     */
    public static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
    /**
     * 数据库jdbc url
     */
    public static final String DB_URL = "jdbc:mysql://node-1:3306/test?useUnicode=true&characterEncoding=utf8";
    /**
     * 数据库user name
     */
    public static final String USER_MAME = "root";
    /**
     * 数据库password
     */
    public static final String PASSWORD = "Test@2019";
    /**
     * 批量提交size
     */
    public static final int BATCH_SIZE = 2;

    //HBase相关配置
//    public static final String HBASE_ZOOKEEPER_QUORUM = "master,slave1,slave2";
//    public static final String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181";
//    public static final String ZOOKEEPER_ZNODE_PARENT = "/hbase";

}
