package cn.itheima.LoadData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * 我们通过编写这个类 将我们已将形成好的HFile文件加载到HBase的表中去
 *
 * 有一个类  LoadIncrementalHFiles 这个类就是用来加载文件的
 * 这个类下面有一个方法
 */
public class LoadToHbase {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "dshuju01");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table myUser2 = connection.getTable(TableName.valueOf("MyUser2"));
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
        load.doBulkLoad(new Path(""),connection.getAdmin(),myUser2,connection.getRegionLocator(TableName.valueOf("MyUser2")));

    }
}
