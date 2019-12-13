package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: Data2Base
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/12 17:01
 * @Version 1.0
 **/
public class Data2Hbase {
    //初始化Configuration对象
    private Configuration conf = null;
    //初始化链接
    private Connection conn = null;

    @Before
    public void init() throws Exception {
        conf = HBaseConfiguration.create();
        // 对于hbase的客户端来说，只需要知道hbase所使用的zookeeper集群地址就可以了
        // 因为hbase的客户端找hbase读写数据完全不用经过hmaster
        conf.set("hbase.zookeeper.quorum", "node-1:2181,node-2:2181,node-3:2181");
        //获取链接
        conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * 建表
     *
     * @throws Exception hbase shell------> create 'tableName','列族1'，'列族2'
     */

    @Test
    public void createTable() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //构造一个表描述器，并指定表名
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("t_user_info".getBytes()));
        //构造一个列族描述器，并指定列族名
        HColumnDescriptor hcd1 = new HColumnDescriptor("base_info");
        // 构造第二个列族描述器，并指定列族名
        HColumnDescriptor hcd2 = new HColumnDescriptor("extra_info");
        // 为该列族设定一个版本数量
        hcd2.setVersions(1, 3);


        // 将列族描述器添加到表描述器中
        tableDescriptor.addFamily(hcd1).addFamily(hcd2);

        //利用表的管理器创建表
        admin.createTable(tableDescriptor);
        //关闭
        admin.close();
        conn.close();
    }

    /**
     * 修改表
     *
     * @throws Exception
     */   //hbase shell   alter 't_user_info' ,'base_info',
    @Test
    public void modifyTable() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //获取表的描述器
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf("t_user_info"));
        //修改已有的ColumnFamily---extra_info最小版本数和最大版本数
        HColumnDescriptor hcd1 = tableDescriptor.getFamily("extra_info".getBytes());
        hcd1.setVersions(2, 5);

        // 添加新的ColumnFamily
        tableDescriptor.addFamily(new HColumnDescriptor("other_info"));

        //表的管理器admin 修改表
        admin.modifyTable(TableName.valueOf("t_user_info"), tableDescriptor);
        //关闭
        admin.close();
        conn.close();
    }

    /**
     * put添加数据
     *
     * @throws Exception hbase shell  put 't_user_info','rk00001','base_info:name','lisi'
     */
    @Test
    public void testPut() throws Exception {
        //构建一个 table对象，通过table对象来添加数据
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        //创建一个集合，用于存放Put对象
        ArrayList<Put> puts = new ArrayList<Put>();

        // 构建一个put对象（kv），指定其行键  例如hbase shell:  put '表名','rowkey','列族:列名称','值'
        Put put01 = new Put(Bytes.toBytes("user001"));  //"user001".getBytes()
        put01.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhangsan"));


        Put put02 = new Put("user001".getBytes());
        put02.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("password"), Bytes.toBytes("123456"));

        Put put03 = new Put("user002".getBytes());
        put03.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("lisi"));
        put03.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put04 = new Put("zhang_sh_01".getBytes());
        put04.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang01"));
        put04.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put05 = new Put("zhang_sh_02".getBytes());
        put05.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang02"));
        put05.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put06 = new Put("liu_sh_01".getBytes());
        put06.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("liu01"));
        put06.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put07 = new Put("zhang_bj_01".getBytes());
        put07.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang03"));
        put07.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        Put put08 = new Put("zhang_bj_01".getBytes());
        put08.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("username"), Bytes.toBytes("zhang04"));
        put08.addColumn(Bytes.toBytes("extra_info"), Bytes.toBytes("married"), Bytes.toBytes("false"));

        //把所有的put对象添加到一个集合中
        puts.add(put01);
        puts.add(put02);
        puts.add(put03);
        puts.add(put04);
        puts.add(put05);
        puts.add(put06);
        puts.add(put07);
        puts.add(put08);

        //一起提交所有的记录
        table.put(puts);

        table.close();
        conn.close();

    }

    /**
     * 读取数据  get：一次读一行
     *
     * @throws Exception hbase shell  :     get 't_user_info',"rowkey"
     */
    @Test
    public void testGet() throws Exception {
        //获取一个table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));


        // 构造一个get查询参数对象，指定要get的是哪一行
        Get get = new Get("user001".getBytes());
        //返回查询结果数据
        Result result = table.get(get);
        //获取结果中的所有cell
        List<Cell> cells = result.listCells();
        //遍历所有的cell
        for (Cell c : cells) {
            //获取行键
            byte[] rowArray = c.getRowArray();
            //获取列族
            byte[] familyArray = c.getFamilyArray();
            //获取列族下的列名称
            byte[] qualifierArray = c.getQualifierArray();
            //列字段的值
            byte[] valueArray = c.getValueArray();
            //打印rowArray、familyArray、qualifierArray、valueArray
//            System.out.println(new String(rowArray));
//            System.out.println(new String(familyArray));
//            System.out.println(new String(qualifierArray));
//            System.out.println(new String(valueArray));

            //按指定位置截取，获取rowArray、familyArray、qualifierArray、valueArray
            System.out.print(new String(rowArray, c.getRowOffset(), c.getRowLength()));
            System.out.print(" " + new String(familyArray, c.getFamilyOffset(), c.getFamilyLength()));
            System.out.print(":" + new String(qualifierArray, c.getQualifierOffset(), c.getQualifierLength()));
            System.out.println(" " + new String(valueArray, c.getValueOffset(), c.getValueLength()));

        }

        //关闭
        table.close();
        conn.close();

    }

    /**
     * scan 批量查询数据
     *
     * @throws Exception hbase shell  scan 't_user_info'
     */
    @Test
    public void testScan() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        //获取scan对象
        Scan scan = new Scan();
        //获取查询的数据
        ResultScanner scanner = table.getScanner(scan);
        //获取ResultScanner所有数据，返回迭代器
        Iterator<Result> iter = scanner.iterator();
        //遍历迭代器
        while (iter.hasNext()) {
            //获取当前每一行结果数据
            Result result = iter.next();
            //获取当前每一行中所有的cell对象
            List<Cell> cells = result.listCells();
            //迭代所有的cell
            for (Cell c : cells) {
                //获取行键
                byte[] rowArray = c.getRowArray();
                //获取列族
                byte[] familyArray = c.getFamilyArray();
                //获取列族下的列名称
                byte[] qualifierArray = c.getQualifierArray();
                //列字段的值
                byte[] valueArray = c.getValueArray();
                //打印rowArray、familyArray、qualifierArray、valueArray

                System.out.println(new String(rowArray, c.getRowOffset(), c.getRowLength()));
                System.out.print(new String(familyArray, c.getFamilyOffset(), c.getFamilyLength()));
                System.out.print(":" + new String(qualifierArray, c.getQualifierOffset(), c.getQualifierLength()));
                System.out.println(" " + new String(valueArray, c.getValueOffset(), c.getValueLength()));
            }
            System.out.println("-----------------------");
        }

        //关闭
        table.close();
        conn.close();

    }


    /**
     * 删除表中的列数据
     *
     * @throws Exception hbase shell  delete 't_user_info','user001','base_info:password'
     */
    @Test
    public void testDel() throws Exception {
        //获取table对象
        Table table = conn.getTable(TableName.valueOf("t_user_info"));
        //获取delete对象,需要一个rowkey
        Delete delete = new Delete("user001".getBytes());
        //在delete对象中指定要删除的列族-列名称
        delete.addColumn("base_info".getBytes(), "password".getBytes());
        //执行删除操作
        table.delete(delete);

        //关闭
        table.close();
        conn.close();
    }

    /**
     * 删除表
     *
     * @throws Exception hbase shell     disable 't_user_info'     drop 't_user_info'
     */
    @Test
    public void testDrop() throws Exception {
        //获取一个表的管理器
        Admin admin = conn.getAdmin();
        //删除表时先需要disable，将表置为不可用，然后在delete
        admin.disableTable(TableName.valueOf("t_user_info"));
        admin.deleteTable(TableName.valueOf("t_user_info"));
        admin.close();
        conn.close();
    }

}
