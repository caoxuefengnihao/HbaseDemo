package cn.itheima.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.Iterator;

/**
 * 官方推荐写法
 *
 */
public class FruitApp extends Configuration implements Tool{

    Configuration configuration = null;
    @Override
    public int run(String[] strings) throws Exception {
        //获取任务对象
        Job job = Job.getInstance(configuration);
        //指定Mapper
        TableMapReduceUtil.initTableMapperJob("fruit",new Scan(),FruitMapper.class,ImmutableBytesWritable.class,Put.class,job);
        //指定Reduce
        TableMapReduceUtil.initTableReducerJob("fruittable",FruitReducer.class,job);
        boolean b = job.waitForCompletion(true);
        return b?1:0;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        //运行run方法
        ToolRunner.run(configuration,new FruitApp(),args);
    }
}
class FruitMapper extends TableMapper<ImmutableBytesWritable,Put>{
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put t = new Put(key.get());
        Cell[] cells = value.rawCells();
        for (Cell cell : cells) {
            if ("name".equals(Bytes.toString(CellUtil.cloneValue(cell)))){
                t.add(cell);
            }
        }
        context.write(key,t);
    }
}
/**
 * 这个MapReduce虽然没有进行什么聚合操作  但是 一定要写 如果不写  需要自己定义OutPutFormat 很麻烦
 */
class FruitReducer extends TableReducer<ImmutableBytesWritable,Put,NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        Iterator<Put> iterator = values.iterator();
        while (iterator.hasNext()){
            Put next = iterator.next();
            context.write(NullWritable.get(),next);
        }
    }
}