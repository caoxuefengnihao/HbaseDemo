

/**
  * 创建Kafka消费端 利用简单api的方式
  *
  *
  */


object KafkaLowerConsumer {

  val port = 9092
  val list = List("dshuju01","dshuju02","dshuju03")
  val partition = 0
  val offset = 0

  def main(args: Array[String]): Unit = {

  }

  /**
    * 1:想要找leader 首先 你得先连到kafka集群上吧 怎么连上呢 是不是通过ip +  port
    * 找leader的原因是我们要获取数据 而获取数据只能通过leader 而不能通过副本
    * 2:连上kafka集群后那么我是不是要找到 我想消费的哪个topic的信息吧
    * 3:知道了topic  如果有分区 我还不是还要知道我向要哪个分区上的信息吧
    * 4:分区找到了 我还得知道 我要从哪个偏移量开始读取吧 要不是不是重复消费了
    * @param list Kafka集群的IP地址
    * @param topic 消费的主题
    * @param partition 消费的分区
    * @param port Kafka端口号
    */
  def findLeader(list: List[String],topic:String,partition:Int,port:Int) = {
    for (elem <- list) {
      /**
        * 创建获取分区leader的消费者对象
        */
      /*val getleader = new SimpleConsumer(elem,port,100,1024*4,"yihao")*/

      /**
        * 获取topic元数据信息 (在zookeeper当中)
        * val versionId : scala.Short,
        * val correlationId : scala.Int,
        * val clientId : scala.Predef.String,
        * val topics : scala.Seq[scala.Predef.String]
        */
     // new TopicMetadataRequest()




    }


  }
}
