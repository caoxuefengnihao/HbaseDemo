package cn.itheima.LoadData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 现在有这么一个需求 我们怎么才能把大量的数据一次性写入到HBase中去呢 如果按照以前的方式 通过客户端写入的话
 * 那么，效率太低 注意 HBase 的数据的存储方式 是最终以HFile的文件 格式存储的 那么我们可不可以直接将文件编程HFile
 * 然后直接加载到HBase中国去呢
 * 可以
 *
 * 步骤
 * 1：我们先编写一个mr程序 形成一个HFile文件
 */
public class LoadApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("myuser2"));
        Job job= Job.getInstance(conf);
        job.setJarByClass(LoadApp.class);
        job.setMapperClass(LoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.configureIncrementalLoad(job,table,connection.getRegionLocator(TableName.valueOf("myuser2")));
        FileInputFormat.addInputPath(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));
        job.waitForCompletion(true);
    }
}
class LoadMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(split[2]));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])),put);
    }
}