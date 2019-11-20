package com.chengfei.customSink;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;


public class MySinkToMySQL extends RichSinkFunction<Tuple3<String, String, Long>>  {
    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into app_history_about " +
                "values(?, ?, ?)";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
//    public void invoke(List<CodeRealtimeData> value, Context context) throws Exception {

    public void invoke(Tuple3<String, String, Long> value, Context context) throws Exception {
        ps.setString(1, value.f0);
        ps.setString(2, value.f1);
        ps.setLong(3, value.f2);
        ps.addBatch();
        int[] count = ps.executeBatch();
        System.out.println("成功插入了" + count.length + "行数据");
    }
    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
//        dataSource.setUrl("jdbc:mysql://192.168.52.100:3306/wxdata?useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUrl("jdbc:mysql://192.168.91.4:3306/market?useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("chengf"); //数据库用户名
        dataSource.setPassword("chengf&^y34"); //数据库密码
//        dataSource.setUsername("root"); //数据库用户名
//        dataSource.setPassword("123456"); //本地数据库密码
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);
        System.out.println("开始建立连接池");
        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }


}
