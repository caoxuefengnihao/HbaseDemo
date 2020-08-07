package cn.ithema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDemo {

    private static Configuration configuration ;
    private static Connection connection;
    private static Admin admin ;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","dshuju01");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {

        //判断表是否存在
        //System.out.println(tableName());
        //创建表
        //creattable("student","info");
        //删除表
        //deleteTable("student");
        // 增 改
        //upDate("student","1001","info","name","zhangsan");
        // 删

        //查
        getdata("student","1001","info","name");
    }

    /**
     * 想要判断表是否存在 那么 我们先得创建一个客户端来连接hbase
     *1 查找相关的api 发现HBaseAdmin 这个类 需要一个  配置参数
     * "hbase.zookeeper.quorum","dshuju01" 告诉客户端连接哪个hbase
     *2 boolean b = hBaseAdmin.tableExists("");  用来判断的方法
     * 但是 这个已经过时了 请看 下面的重载方法
     */
    public static boolean tableName() throws IOException {

        HBaseConfiguration entries = new HBaseConfiguration();
        entries.set("hbase.zookeeper.quorum","dshuju01");
        HBaseAdmin hBaseAdmin = new HBaseAdmin(entries);
        boolean b = hBaseAdmin.tableExists("student");
        return b;
    }
    public static boolean tableName(String tableName) throws IOException{
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;
    }

    /**
     * 创建表操作
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    public static void creattable(String tableName,String...cfs) throws IOException {
        //创建表描述器
        HTableDescriptor h = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            //创建列描述器
            HColumnDescriptor hc = new HColumnDescriptor(cf);
            //注意 这里有一个版本控制 的设置  默认是一个
            hc.setMaxVersions(3);
            h.addFamily(hc);
        }
        admin.createTable(h);
        admin.close();
    }
    /**
     *
     * 删除表操作
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        //使表下线
        admin.disableTable(TableName.valueOf(tableName));
        //删除表
        admin.deleteTables(tableName);
        admin.close();
    }

    /**
     *
     *  想要对表的数据进行操作
     * 1  创建表的对象  table  调用put方法
     * 2 创建 put对象 添加数据
     * 注意  在hbase 中  只有byte[] 一种类型 但是hbase 给我们提供了一个Bytes 工具类  用来与其他类型进行转换的
     * 3关闭资源
     * @param tableName
     * @param rowKey
     * @param CF
     * @param CN
     * @param value
     * @throws IOException
     */
    public static void upDate(String tableName,String rowKey,String CF,String CN,String value) throws IOException {
       // Table t = new HTable(configuration,TableName.valueOf(tableName));
        Table t = connection.getTable(TableName.valueOf(tableName));
        Put p = new Put(Bytes.toBytes(rowKey));
        p.addColumn(Bytes.toBytes(CF),Bytes.toBytes(CN),Bytes.toBytes(value));
        t.put(p);
        t.close();

    }

    /**
     * 想要对表进行删除数据操作
     * 1 创建表对象 table 调用delete方法
     * 2 创建delete对象 添加数据
     * 注意 如果只传rowkey的话 相对于把整行都删掉了 相当于deleteall
     *      d.addColumns  删除所有的版本
     *      d.addColumn   删除最新的一个版本  一般不用
     *      这两个方法 一个有s  一个没有s
     *      那有什么区别呢
     * @param tableName 表明
     * @param rowKey
     * @param CF
     * @param CN
     * @throws IOException
     */
    public static void deletedata(String tableName,String rowKey,String CF,String CN) throws IOException {
        Table t = connection.getTable(TableName.valueOf(tableName));
        Delete d = new Delete(Bytes.toBytes(rowKey));
        d.addColumns(Bytes.toBytes(CF),Bytes.toBytes(CN));
        t.delete(d);
        t.close();
    }

    /**
     * 想要对表进行查询操作  有两种方式  scan get
     * 1 首先 创建表对象 Table
     * 2 创建 Scan对象
     * @param tableName 表的名字
     * @param startRowKey 起始行键
     * @param stopRowKey 结束行键
     * @throws IOException
     */
    public static void GetScan(String tableName,String startRowKey,String stopRowKey) throws IOException {
        Table t = connection.getTable(TableName.valueOf(tableName));
        Scan s = new Scan();
        //发现返回一个结果类 查看api  发现这个类是一个数组
        ResultScanner scanner = t.getScanner(s);
        for (Result result : scanner) {
            //返回的是一个 cell数组  其实这也很容易理解 在第四章数据结构中我们已经学到 cell 是最小的单元了
            //而且无类型 就是字节数组 每行都有很多的cell 所以返回来的是数组
            // 所以 我们也要对数组进行遍历 来获取最终的数据结果
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                +"cf" + Bytes.toString(CellUtil.cloneFamily(cell))
                +"cn" + Bytes.toString(CellUtil.cloneQualifier(cell))
                +"value" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }

        }
            t.close();
    }

    /**
     * get方法获取数据
     * @param tableName  表的名字
     * @param rowKey  行键
     * @param CF 列族
     * @param CN 列名
     * @throws IOException
     */
    public static void getdata(String tableName,String rowKey,String CF,String CN) throws IOException {
        Table t = connection.getTable(TableName.valueOf(tableName));
        Get g = new Get(Bytes.toBytes(rowKey));
        Result result1 = t.get(g);
            Cell[] cells = result1.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell))
                        +",cf-" + Bytes.toString(CellUtil.cloneFamily(cell))
                        +",cn-" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",value-" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        t.close();
    }
}
