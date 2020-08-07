package cn.itheima.mapper2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class Mapper2App extends Configuration implements Tool{

    Configuration configuration = null;
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(cn.itheima.mapper2.Mapper2App.class);
        job.setMapperClass(cn.itheima.mapper2.FruitMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);
        TableMapReduceUtil.initTableReducerJob("fruit2",FruitReduce.class,job);
        FileInputFormat.setInputPaths(job,strings[0]);
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration=configuration;

    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        ToolRunner.run(configuration, new Mapper2App(), args);
    }




}
class FruitMapper extends Mapper<LongWritable,Text,NullWritable,Put>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split("\t");
        Put put = new Put(Bytes.toBytes(split[0]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(split[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(split[2]));
        context.write(NullWritable.get(),put);
    }
}
class FruitReduce extends TableReducer <NullWritable,Put,NullWritable>{
    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        Iterator<Put> iterator = values.iterator();
        while (iterator.hasNext()){
            Put next = iterator.next();
            context.write(NullWritable.get(),next);
        }



    }
}