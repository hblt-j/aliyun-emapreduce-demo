package com.tc
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.mongodb.MongoClientOptions._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
//import kafka.utils.VerifiableProperties
object BigLED {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    var kafkaArgs: Array[String]=new Array[String](4)
    val config:Config=ConfigFactory.load()
    if (args.length == 4) {
      kafkaArgs=args
    }else if(config.getString("app.kafkaArgs.topics")=="tc_make_deal_topic") {
      kafkaArgs(0) = config.getString("app.kafkaArgs.brokers")
      kafkaArgs(1) = config.getString("app.kafkaArgs.group")
      kafkaArgs(2) = config.getString("app.kafkaArgs.topics")
      kafkaArgs(3) = config.getString("app.kafkaArgs.numThreads")
      //System.err.println("=====>kafkaArgs:"+kafkaArgs.mkString("[",",","]"))
    }else{
      System.err.println("default use appliction.conf,or Usage: KafkaParam <brokers> <group> <topics> <numThreads>")
      System.exit(1)
      /*kafkaArgs = Array("172.16.5.90:2181/kafka",
        //"zookeeper.tuanche.com:2181,zookeeper.tuanche.com:2182,zookeeper.tuanche.com:2183/kafka",
        "tuanche_code_autoshow", "tc_make_deal_topic", "1")*/
    }
    val appName= config.getString("app.appName")//"BigLED"
    val checkpointDirectory = config.getString("app.checkpointDirectory")//"BigLEDCheckPoint"
    val mongohost=config.getString("app.mongohost")//"172.16.60.65:27017"
    val credentials=config.getString("app.credentials")// "ljj,admin,123456"
    //System.err.println("=====>appconfig:"+appName+checkpointDirectory+mongohost+credentials)
    // Get StreamingContext from checkpoint data or create a new one
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
                  ()=> functionToCreateContext(appName,checkpointDirectory,kafkaArgs:Array[String],mongohost,credentials))

    ssc.start()
    ssc.awaitTermination()
  }

  //用checkpoint实现driver容错,并设置KAFKA并发读取和SPARK并行处理,实现毫秒级PB级响应
  def functionToCreateContext(appName:String,checkpointDirectory:String,kafkaArgs:Array[String],
                              mongohost:String,credentials:String):StreamingContext = {
    val sparkConf = new SparkConf().setAppName(appName)
          .set("spark.streaming.receiver.writeAheadLog.enable","true")
          .set("spark.eventLog.enabled","false")
    val sc=new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(3))
    val sqlContext =new HiveContext(sc)
    //val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)//rdd.sparkContext)
    import sqlContext.implicits._
    val Array(brokers, group, topics, numThreads) = kafkaArgs
    //Receiver方式
    //val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //val dstreams = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //kafka.utils.VerifiableProperties no found
    //val kafkaParams=Map("bootstrap.servers"->"172.16.60.65:9092","group.id"->group,"session.timeout.ms"->"30000")
    //val kafkaParams=Map("zookeeper.connect"->zkQuorum,"group.id"->group,"bootstrap.servers"->"172.16.60.65:9092",
    //        "zookeeper.session.timeout.ms"->"30000","zookeeper.connection.timeout.ms"->"30000")
    //val dstreams:ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,kafkaParams,topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //Direct方式
    def initKafkaParams = {
      Map[String, String](
        "metadata.broker.list" ->brokers,
        "group.id"->group,
        //"session.timeout.ms"->"30000",
        "fetch.message.max.bytes" -> "20971520",
        "auto.offset.reset" -> "smallest"
      )
    }
    // kafka参数
    val kafkaParams = initKafkaParams
    val manager = new KafkaManager(kafkaParams)
    val dstreams = manager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics))
    // 更新offsets
    //manager.updateZKOffsets(rdd)
    dstreams.checkpoint(Duration(60000L))
    val lines = dstreams.map(_._2).repartition(1)
    //System.err.println("=========>lines:"+lines.count()+"|"+lines.toString)
    //    lines.saveAsTextFiles("hdfs:///root/lines")
    updateOffLineData(sqlContext,checkpointDirectory,mongohost,credentials)//非Dstream操作不会流上频次执行
    lines.foreachRDD(rdd => {
      //rdd.foreachPartition { partitionOfRecords =>//sqlContext java.lang.NullPointerException :at org.apache.spark.sql.SQLConf.getConf(SQLConf.scala:647)
      //partitionOfRecords.foreach{record =>{
      import sqlContext.implicits._
      updateOffLineData(sqlContext,checkpointDirectory,mongohost,credentials)
      val df = sqlContext.read.json(rdd) //rdd.toDF
      //      System.err.println("=========>rdd:"+rdd.collect.mkString)
      System.err.println("=========>df.count:" + df.count())

      if(df.count()>0) {
        df.registerTempTable("makedealDstream0")
        val makedealDstream = sqlContext.sql(
          """select m.id,p.id periodsId,p.title,c.province,c.city,p.city_id cityId,
            |b.name brandName,b.id brandId,m.name,m.phone
            |from (select id,title,city_id from activity)p
            |left join makedealDstream0 m on (p.id=m.periods_id and m.status=0 and m.type="insert")
            |left join default.dimcity c on c.cid=p.city_id
            |left join che100.tc_brand b on b.id=m.secondary_brand_id
          """.stripMargin)
        makedealDstream.show
        //makedealDstream.registerTempTable("makedealDstream")
        System.err.println("=========>makedealDstream.count:" + makedealDstream.count())
        var opt: Map[String, String] = Map()
        if (credentials.length > 0) {
          opt = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "bigLedMakedeal", "credentials" -> credentials)
        } else {
          opt = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "bigLedMakedeal")
        }
        //val da1=sqlContext.sql(
        //  """select * from makedealDstream
        //    |union all
        //    |select * from makedeal
        //   """.stripMargin)
        // da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt).save()
        makedealDstream.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()
        val da1 = sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
        da1.registerTempTable("bigLedMakedeal")
        System.err.println("=========>bigLedMakedeal.count:" + da1.count())
        var opt1: Map[String, String] = Map()
        if (credentials.length > 0) {
          opt1 = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "bigLedCityBrandTop", "credentials" -> credentials)
        } else {
          opt1 = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "bigLedCityBrandTop")
        }
        val da2=sqlContext.sql(
          """select m.*,p.applycount from
            |(select periodsId,title,cityId,city,province,brandName,brandId,count(distinct id) makedealcount
            |from bigLedMakedeal group by periodsId,title,cityId,city,province,brandName,brandId) m
            |left join applycount p
            |on p.periodsId=m.periodsId and m.cityId=p.cid and m.brandId=p.brand_id
          """.stripMargin)
        da2.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt1).save()
        System.err.println("=========>bigLedCityBrandTop.count:" + da2.count())
      }//}}}
    })
    //参数优化参考，目前只有一个topic一个pation顾都不需要
    //val kafkaParams: Map[String, String] = Map("group.id" -> group)
    //val readParallelism = 3//KAFKA并发读取使用reserver进程数
    //val topics = Map("test" -> 1)//KAFKA并发读取每reserver使用线程数
    //val kafkaDStreams = (1 to readParallelism).map { _ =>
    //    KafkaUtils.createStream(ssc, kafkaParams, topics, ...)//改用directStream可实现 Exactly Once
    //  }
    // //> collection of 3 *input* DStreams = handled by 3 receivers/tasks
    // val unionDStream = ssc.union(kafkaDStreams) // often unnecessary, just showcasing how to do it
    // //> single DStream
    // val processingParallelism = 20
    // val processingDStream = unionDStream(processingParallelism)//SPARK并行处理
    // //> single DStream but now with 20 partitions
    ssc.checkpoint(checkpointDirectory) // set checkpoint directory，driver容错
    ssc
  }
  def updateOffLineData(sqlContext:HiveContext,checkpointDirectory:String, mongohost:String, credentials:String){
    import sqlContext.implicits._
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    val path=checkpointDirectory+"/refrash.parquet"
    val f: FileSystem = FileSystem.get(new Configuration)
    if(!f.exists(new Path(path))){
      //离线场次相关数据首次启动执行一次以后每天一刷
      readMakeDeal(sqlContext,mongohost,credentials)
      readApplyCount(sqlContext)
      //刷新flag
      val data=sqlContext.sql("select current_date")
      data.toDF.write.mode(SaveMode.Overwrite).parquet(path)
    }else{
      val olddata=sqlContext.read.parquet(path)
      val data=sqlContext.sql("select current_date")
      if(data.collect()(0)(0).toString != olddata.collect()(0)(0).toString) {
        //离线场次相关数据每天一刷
        readMakeDeal(sqlContext,mongohost,credentials)
        readApplyCount(sqlContext)
        //刷新flag
        data.toDF.write.mode(SaveMode.Overwrite).parquet(path)
      }
    }
  }
  
  def readMakeDeal(sqlContext:HiveContext, mongohost:String, credentials:String){
    import com.stratio.datasource.mongodb._
    import com.stratio.datasource.mongodb.config._
    import com.stratio.datasource.mongodb.config.MongodbConfig._
    import org.apache.spark.sql._
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    import java.text.SimpleDateFormat
    import org.joda.time.format._
    import org.joda.time._
    import java.sql.Timestamp
    System.err.println("=========>readmakedeal()")
    var opt: Map[String, String] = Map()
    if (credentials.length > 0) {
      opt = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "makedeal", "credentials" -> credentials)
    }else{
      opt = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "makedeal")
    }
    //    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    //    val sqlContext =new HiveContext(ssc.sparkContext)

    // def ISODateToSqlDate (s:String):Timestamp={//自定义函数
    //     var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //     Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
    // }
    // sqlContext.udf.register("ISODateToSqlDate", (s: String) => ISODateToSqlDate(s))
    // val da1=sqlContext.sql("select ISODateToSqlDate(timestamp) st,* from data")
    sqlContext.sql(
      """select * from admin.t_autoshow_periods p where p.is_delete = 0 and
        |p.begin_time<=current_date and current_date<=p.end_time
      """.stripMargin).registerTempTable("activity")
    sqlContext.cacheTable("activity")
    // da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt).save()
    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    // res1.show
    res1.registerTempTable("makedeal0")
    val makedeal=sqlContext.sql(
      """select m.id,p.id periodsId,p.title,c.province,c.city,p.city_id cityId,
        |b.name brandName,b.id brandId,m.name,m.phone
        |from activity p
        |left join makedeal0 m on (p.id=periodsId and m.status=0)
        |left join default.dimcity c on c.cid=p.city_id
        |left join che100.tc_brand b on b.id=m.secondaryBrandId
      """.stripMargin)
    makedeal.show
    makedeal.registerTempTable("bigLedMakedeal")
    //sqlContext.cacheTable("makedeal")
    var opt0: Map[String, String] = Map()
    if (credentials.length > 0) {
      opt0 = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "bigLedMakedeal", "credentials" -> credentials)
    }else{
      opt0 = Map("host" -> mongohost, "database" -> "bigdata", "collection" -> "bigLedMakedeal")
    }
    makedeal.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt0).save()
  }
  def readApplyCount(sqlContext:HiveContext){
    System.err.println("=========>readapplycount()")
    val applycount=sqlContext.sql(
      """select p.id periodsId,count(a.id) applycount,a.cid,a.brand_id
        |from (select id from activity) p
        |left join che100.tc_apply_detail t on t.groupby_num=p.id
        |INNER JOIN che100.tc_apply a ON a.del = 0 AND t.apply_id = a.id
        |group by p.id,a.cid,a.brand_id
      """.stripMargin)
    applycount.show
    applycount.registerTempTable("applycount")
    sqlContext.cacheTable("applycount")
  }
}
