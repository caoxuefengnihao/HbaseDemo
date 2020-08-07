
import java.util

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * 想要连接到HBase  我们首先要获取到一个客户端  想要一个客户端 就得先有一个连接 想要有一个连接 就先要有一个配置文件
  * 那么通过源码我们发现
  * 创建HBaseAdmin的实体类对象 可以连接到HBase 但是这个已经过时了
  * 我们该怎么办
  *
  * 其实 对Hbase 进行增删改查  对应了五个对象  Put Delete Scan Get
  */
object CheckTable {

  //获取配置对象
  val config = HBaseConfiguration.create()
  config.set("hbase.zookeeper.quorum", "dshuju01")
  //获取连接
  val connect:Connection = ConnectionFactory.createConnection(config)
  //获取客户端
  val admin: Admin = connect.getAdmin
  def main(args: Array[String]): Unit = {

    //print(admin.tableExists(TableName.valueOf("student")))

    //createTable("emp","info")
    //scandemo("student","1001","1001")
    //insertTable("emp","1001","info","name","zs")
    val string: String = getdemo("emp", "1001", "info", "name")
    println(string)
  }

  /**
    *  创建表操作
    * 我们想要创建一张表  我们得有 表的 名称 和 列族吧 列族还可以有多个吧（可变参数）
    * 第二 我们需要一个 表的描述器
    * 第三 我们还需要一个 列的描述器
    * 第四 我们还可以进行版本的控制
    * @param name 表的名称
    * @param CF  表的列族
    */
  def createTable (name:String,CF:String*)={
    //创建表描述器
    val h  = new HTableDescriptor(TableName.valueOf("emp"))
    for (elem <- CF) {
      //创建列描述器
      val hn = new HColumnDescriptor(elem)
      //在这里有一个版本控制
      hn.setMaxVersions(3)
      h.addFamily(hn)
    }
    admin.createTable(h)
  }

  /**
    *  我们想向一张表中插入数据
    *  1：用连接对象创建 一个表对象
    *  2：创建put对象 输入信息
    *  3：调用put方法
    * 如果这个列没有 会自动创建列
    *
    *
    * @param name 表名称
    * @param rowKey 行键
    * @param CF 列族
    * @param CN 列
    * @param value 想要插入的值
    * @return
    */
  def insertTable(name:String,rowKey:String,CF:String,CN:String,value:String) ={
    val table = connect.getTable(TableName.valueOf(name))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(CF),Bytes.toBytes(CN),Bytes.toBytes(value))
    table.put(put)
    table.close()
  }

  /**
    * 查询表数据有两种方法 Scan 与 Get
    * 他们两者的区别就是 Scan 是扫描整张表 而 Get 是根据行键进行查询
    * 步骤 ：
    * 1：想要查询一张表 我们先有一个表对象 Table
    * 2:需要一个Scan对象
    * 3：调用getScanner方法 获取结果 发现是一个数组
    * 4：遍历数组
    * @param name 表名
    * @param startRow 起始行键
    * @param stopRow 结束行键
    */
  def scandemo(name:String,startRow:String,stopRow:String)={
    val table = connect.getTable(TableName.valueOf(name))
    val scan = new Scan(/*Bytes.toBytes(startRow),Bytes.toBytes(stopRow)*/)
    /**
      * 注意这里 ResultScanner 是一个接口 继承了Iterator 所以我们要把他转换成 一个迭代器然后遍历
      */
     val value: util.Iterator[Result] = table.getScanner(scan).iterator()
    while(value.hasNext){
      val result: Result = value.next()
        val cells: Array[Cell] = result.rawCells()
        for (elem <- cells) {
          println(s"${Bytes.toString(CellUtil.cloneFamily(elem))} ----  ${Bytes.toString(CellUtil.cloneValue(elem))}")
        }
     }
  }

  /**
    *
    * 获取HBase数据的第二种方式  通过get方式获取一行数据
    * @param name
    * @param rowKey
    * @param CF
    * @param CN
    */
  def getdemo(name:String,rowKey:String,CF:String,CN:String)={
      val table = connect.getTable(TableName.valueOf(name))
      val get = new Get(Bytes.toBytes(rowKey))
      val result: Result = table.get(get)
      //这个方法可以获取一个单元格的数据
      var tmp = ""
      val bytes: Array[Byte] = result.getValue(Bytes.toBytes(CF),Bytes.toBytes(CN))
      if(bytes.size > 0 && bytes != null){
        tmp = Bytes.toString(bytes)
      }
      tmp
  }
}

