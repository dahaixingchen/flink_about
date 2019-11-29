package com.wsxd.cfs.km.flink.source;

import com.wsxd.cfs.km.flink.pojo.TblKmTrace;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ClassName: Mysource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/21 11:35
 * @Version 1.0
 **/
public class MySourceFromMysql extends RichSourceFunction<TblKmTrace> {

    private static Logger logger = Logger.getLogger(MySourceFromMysql.class);
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (true){
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                TblKmTrace tblKmTrace = new TblKmTrace(
                        resultSet.getString("sys_date"),
                        resultSet.getString("merchantno"),
                        resultSet.getString("saledate"),
                        resultSet.getString("shop"),
                        resultSet.getString("id"),
                        resultSet.getString("name"),
                        resultSet.getString("qty"),
                        resultSet.getString("amount"),
                        resultSet.getString("refundqty"),
                        resultSet.getString("refundamt"),
                        resultSet.getString("create_time"),
                        resultSet.getString("update_time"));
                sourceContext.collect(tblKmTrace);
                //每秒去查询一次
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "SELECT * from cfsdb.tbl_km_trace";
        ps = this.connection.prepareStatement(sql);
    }

    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//        dataSource.setUrl("jdbc:mysql://192.168.91.1:3306/cfsdb?useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("coder"); //数据库用户名
        dataSource.setPassword("1qaz@WSX"); //数据库密码
        dataSource.setUrl("jdbc:mysql://192.168.91.4:3306/cfsdb?useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("chengf"); //数据库用户名
        dataSource.setPassword("chengf&^y34"); //数据库密码
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);
        logger.info("开始建立连接池");
        Connection con = null;
        try {
            con = dataSource.getConnection();
            logger.info("创建连接池：" + con);
        } catch (Exception e) {
            logger.info("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }

    @Override
    public void cancel() {

    }
}
