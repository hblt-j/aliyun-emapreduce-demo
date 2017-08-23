package com.bigdata.lzlh
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.stratio.datasource.mongodb.config.MongodbConfig.{Collection, Database, Host}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, SQLContext, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.aliyun.logservice.LoghubUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat

//	Spark	--class com.aliyun.emr.example.LoghubSample --master yarn --deploy-mode cluster --driver-memory 4g --num-executors 2 --executor-memory 2g --executor-cores 2 ossref://tt-big-data/test/examples-1.1-shaded.jar tracklog first test cn-beijing-vpc.log.aliyuncs.com LTAIanYcMuGu2D3x Nvqs7Fc82ygzAyv9BIVRpDK84nkMLv 10
object pv_m{
  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        """Usage: bin/spark-submit --class LoghubSample examples-1.0-SNAPSHOT-shaded.jar
          |         <sls project> <sls logstore> <loghub group name> <sls endpoint>
          |         <access key id> <access key secret> <batch interval seconds>
        """.stripMargin)
      System.exit(1)
    }

    //屏蔽日志
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val loghubProject = args(0)
    val logStore = args(1)
    val loghubGroupName = args(2)
    val endpoint = args(3)
    val accessKeyId = args(4)
    val accessKeySecret = args(5)
    val batchInterval = Milliseconds(args(6).toInt * 1000)
    val numReceivers = 2
    val conf = new SparkConf().setAppName("pc streaming m leave").setMaster("yarn-client")
    val ssc = new StreamingContext(conf, batchInterval)
    val sqlContext = SQLContext.getOrCreate(ssc.sparkContext)
    val loghubStream = LoghubUtils.createStream(
      ssc,
      loghubProject,
      logStore,
      loghubGroupName,
      endpoint,
      numReceivers,
      accessKeyId,
      accessKeySecret,
      StorageLevel.MEMORY_AND_DISK)

    import sqlContext.implicits._
    sqlContext.udf.register("ISODateToSqlDate", (s: String) => {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
    })
    loadTim
//    loghubStream.mapPartitions(it=>{
//      it.next()
//    },false)
      loghubStream.foreachRDD(rdd =>{
        if(rdd.count()>0) {
          var inputdata: RDD[String] = rdd.map(bytes => {
            var line = new String(bytes, "utf-8")
            //println(line)
            line
          })
          println(inputdata.count())

          var date: String = null

          if (loadData(inputdata) > 0) {
            var beforday = befordays(date)
            if(clearData("pageview","lzlh","track")>0) {
              writeTrackpageview(beforday)
              println("lzlh:writeTrackpageview")
              writeMinuteLevelIndicators(beforday)
              println("lzlh:writeMinuteLevelIndicators")
              writeNewOldVisitors(beforday)
              println("lzlh:writeNewOldVisitors")
              writeNewOldVisitorsIndicators(beforday)
              println("lzlh:writeNewOldVisitorsIndicators")
              writePVLongIndicators(beforday)
              println("lzlh:writePVLongIndicators")
            }
            if(clearData("click","lzlh")>0) {
              writeMinuteLevelClickIndicators(beforday, "lzlh")
              println("lzlh:writeMinuteLevelClickIndicators")
            }
            if(clearData("pageview","lzlh_touch","track")>0) {
              writeTrackpageview(beforday,"lzlh_touch")
              println("lzlh_touch:writeTrackpageview")
              writeMinuteLevelIndicators(beforday,"lzlh_touch")
              println("lzlh_touch:writeMinuteLevelIndicators")
              writeNewOldVisitors(beforday,"lzlh_touch")
              println("lzlh_touch:writeNewOldVisitors")
              writeNewOldVisitorsIndicators(beforday,"lzlh_touch")
              println("lzlh_touch:writeNewOldVisitorsIndicators")
              writePVLongIndicators(beforday,"lzlh_touch")
              println("lzlh_touch:writePVLongIndicators")
            }
            if(clearData("click","lzlh_touch")>0) {
              writeMinuteLevelClickIndicators(beforday, "lzlh_touch")
              println("lzlh_touch:writeMinuteLevelClickIndicators")
            }
            sqlContext.uncacheTable("data")
          }
        }
      })
    //unCacheTim()
    def loadTim() = {
      val props = scala.collection.mutable.Map[String, String]();
      props += ("driver" -> "com.mysql.jdbc.Driver")
      props += ("url" -> "jdbc:mysql://172.16.2.114:3306/test?user=root&password=EMRroot1234") //拉取维度数据源 做跨平台 跨数据源分析
      props += ("dbtable" -> "date_dim") //默认一个rdd分区，可通过以下优化：
      // //sql 是查询语句，此查询语句必须包含两处占位符?来作为分割数据库ResulSet的参数，例如："select title, author from books where ? < = id and id <= ?"
      // props+=("partitionColumn" -> "id")//分区字段，须数字类型
      // props+=("lowerBound" -> "0")//分区下界
      // props+=("upperBound" -> "100")
      // //owerBound, upperBound, numPartitions 分别为第一、第二占位符，partition的个数。例如，给出lowebound 1，upperbound 20， numpartitions 2，则查询分别为(1, 10)与(11, 20)
      // props+=("numPartitions" -> "2")//分区数
      import scala.collection.JavaConverters._

      var jdbcDF = sqlContext.load("jdbc", props.asJava) //加载数据
      // jdbcDF.first
      // jdbcDF.cache //OOM
      // import org.apache.spark.storage.StorageLevel
      // jdbcDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

      jdbcDF.registerTempTable("date_dim") //注册表
      // sqlContext.cacheTable("customer_activity")
      sqlContext.cacheTable("date_dim") //缓存表
      //    sqlContext.sql("select * from date_dim limit 1").show

      //    props += ("dbtable" -> "area_dim")
      //    jdbcDF = sqlContext.load("jdbc", props.asJava) //加载注册多表
      //    jdbcDF.registerTempTable("area_dim")
      //    sqlContext.cacheTable("area_dim") //缓存表
      //    sqlContext.sql("select * from area_dim limit 1").show;

      props += ("dbtable" -> "time_dim")
      jdbcDF = sqlContext.load("jdbc", props.asJava) //加载注册多表
      jdbcDF.registerTempTable("time_dim")
      sqlContext.cacheTable("time_dim") //缓存表
      //    sqlContext.sql("select * from time_dim").show;

      props += ("dbtable" -> "minute_dim")
      jdbcDF = sqlContext.load("jdbc", props.asJava) //加载注册多表
      jdbcDF.registerTempTable("minute_dim")
      sqlContext.cacheTable("minute_dim") //缓存表
      //    sqlContext.sql("select * from time_dim").show;

      props += ("dbtable" -> "source_dim")

      jdbcDF = sqlContext.load("jdbc", props.asJava) //加载注册多表
      jdbcDF.registerTempTable("source_dim")
      sqlContext.cacheTable("source_dim") //缓存表
      //    sqlContext.sql("select * from source_dim").show;

    }
    def loadData(inputData: RDD[String]):Long ={
      var re: Long = 0
      if (inputData.count() > 0) {
        val spark = sqlContext
        val df0 = spark.read.json (inputData)
        if (df0.count () > 0) {
          val df = df0.select ("data")
          if (df.count () > 0) {
            val data = df.map (t => t (0).toString.replace ("=>", ":").replace ("$", "") ).filter (t => "" != t)
            //数据处理
            val da = spark.read.json (data)
            //da.show
            da.registerTempTable ("data") //注册表
            sqlContext.cacheTable("data")
            re = da.count
          }
        }
      }
      re
    }
    /*def clearData()={
      //da.count
      // sqlContext.sql("""
      // select eventid--,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,'%\\{\\[.*\\]\\}','') region_code,regexp_replace(addr,'%\\{\\[.*\\]\\}','') addr,ip
      // from data
      // where region_code is not null and token='lzlh' and type='track' and event='pageview'
      // order by region_code
      // """).count//数据清洗

      val da=sqlContext.sql("""
select eventid,timestamp,tkid,ssId,properties,version, region_code,ip,
case when s.type is null and d.properties.referrer_host='' then "其它" when s.type is null and d.properties.referrer_host!='' then "超级链接" else s.type end as sourcetype,
case when s.source is null and d.properties.referrer_host='' then "其它" when s.source is null and d.properties.referrer_host!='' then d.properties.referrer_host else s.source end as source,
case when properties.model='pc' or properties.model='mac' then 'pc' else 'mobi' end as devicestype,
case when area[0] is null then '' else area[0] end country,
case when area[1] is null then '' else area[1] end province,
case when area[2] is null then '' else area[2] end city from(
--select eventid,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,"%\\{\\[.*\\]\\[.*\\]\\}",'') region_code,split(regexp_replace(addr,"%\\{\\[.*\\]\\[.*\\]\\}",'')," ") area,ip
select eventid,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,"%\{\[.*\]\[.*\]\}",'') region_code,split(regexp_replace(addr,"%\{\[.*\]\[.*\]\}",'')," ") area,ip
from data where token='lzlh' and type='track' and event='pageview'
) d left join source_dim s on d.properties.referrer_host=s.match
""")
      //da.count//数据清洗
      da.registerTempTable("data")//注册表
      sqlContext.cacheTable("data")
    }*/
    def clearData(event:String,token:String="lzlh",Type:String="track"):Long={
      var da:org.apache.spark.sql.DataFrame=null
      var re: Long = 0
      try{
      if("click".equals(event)){
        da=sqlContext.sql("""
            select eventid,timestamp,tkid,ssId,properties,version, region_code,ip,nid,
            case when s.type is null and d.properties.referrer_host='' then "其它" when s.type is null and d.properties.referrer_host!='' then "超级链接" else s.type end as sourcetype,
            case when s.source is null and d.properties.referrer_host='' then "其它" when s.source is null and d.properties.referrer_host!='' then d.properties.referrer_host else s.source end as source,
            case when properties.model='pc' or properties.model='mac' then 'pc' else 'mobi' end as devicestype,
            case when area[0] is null then '' else area[0] end country,
            case when area[1] is null then '' else area[1] end province,
            case when area[2] is null then '' else area[2] end city from(
            --select eventid,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,"%\\{\\[.*\\]\\[.*\\]\\}",'') region_code,split(regexp_replace(addr,"%\\{\\[.*\\]\\[.*\\]\\}",'')," ") area,ip,nid
            select eventid,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,"%\{\[.*\]\[.*\]\}",'') region_code,split(regexp_replace(addr,"%\{\[.*\]\[.*\]\}",'')," ") area,ip,nid
            from data where token='"""+token+"' and type='"+Type+"' and event='"+event+"' and nid is not null"+
          """) d left join source_dim s on d.properties.referrer_host=s.match
          """)
      }else{
        da=sqlContext.sql("""
                select eventid,timestamp,tkid,ssId,properties,version, region_code,ip,
                case when s.type is null and d.properties.referrer_host='' then "其它" when s.type is null and d.properties.referrer_host!='' then "超级链接" else s.type end as sourcetype,
                case when s.source is null and d.properties.referrer_host='' then "其它" when s.source is null and d.properties.referrer_host!='' then d.properties.referrer_host else s.source end as source,
                case when properties.model='pc' or properties.model='mac' then 'pc' else 'mobi' end as devicestype,
                case when area[0] is null then '' else area[0] end country,
                case when area[1] is null then '' else area[1] end province,
                case when area[2] is null then '' else area[2] end city from(
                --select eventid,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,"%\\{\\[.*\\]\\[.*\\]\\}",'') region_code,split(regexp_replace(addr,"%\\{\\[.*\\]\\[.*\\]\\}",'')," ") area,ip
                select eventid,timestamp,tkid,ssId,properties,version, regexp_replace(region_code,"%\{\[.*\]\[.*\]\}",'') region_code,split(regexp_replace(addr,"%\{\[.*\]\[.*\]\}",'')," ") area,ip
                from data where token='"""+token+"' and type='"+Type+"' and event='"+event+"'"+
          """) d left join source_dim s on d.properties.referrer_host=s.match
          """)
      }
      re=da.count//数据清洗
      da.registerTempTable(token+event+"data")//注册表
//      sqlContext.cacheTable(token+event+"data")
    }catch {
      case ex: AnalysisException =>{
        println(token+event+":ignore empty or error date insert")
      }
    }
      re
    }
    def befordays(datestr:String=null):Int={
      if(datestr==null){0}else {
        import java.text.SimpleDateFormat
        import java.util.Date
        var format = new SimpleDateFormat("yyyy-MM-dd");
        var date = format.parse(datestr);
        // 计算日期间隔天数
        val diff = new Date().getTime() - date.getTime()
        (diff / (1000 * 60 * 60 * 24)).toInt
      }
    }
    def writeTrackpageview(beforday:Int,token:String="lzlh",event:String="pageview")={
      import com.stratio.datasource.mongodb.config.MongodbConfig._
      import org.apache.spark.sql._
      import com.stratio.datasource.mongodb._
      import com.stratio.datasource.mongodb.config._

      import org.apache.spark.sql.SQLContext
      import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
      import java.text.SimpleDateFormat
      import org.joda.time.format._
      import org.joda.time._
      import java.sql.Timestamp
      import sqlContext.implicits._
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "trackpageview","credentials"-> "root,admin,mongo2tiger")
      //http://blog.csdn.net/liuguangfudan/article/details/53304368 内置函数

      //    def ISODateToSqlDate (s:String):Timestamp={
      //      var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //      Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
      //    }
      // val da3=da2.map(s=>{(ISODateToSqlDate(s.getAs[String]("timestamp")),""+s(0))})
      // da3.toDF("timestamp","date").show

      //    sqlContext.udf.register("ISODateToSqlDate", (s: String) => ISODateToSqlDate(s))
      val data=token+event+"data"
      val da1=sqlContext.sql(s"""select ISODateToSqlDate(timestamp) st,* from $data where substr(timestamp,1,10)=date_sub(now(),$beforday)""")
      //    da1.count()
      this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }
    def writeMinuteLevelIndicators(beforday:Int,token:String="lzlh",event:String="pageview")={
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "MinuteLevelIndicators","credentials"-> "root,admin,mongo2tiger")
      val data=token+event+"data"
      val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,t1.time,t3.m,source,devicestype,province,count(t2.eventid) pv,count(distinct t2.ssId) sv ,count(distinct t2.ip) ips,count(distinct tkid) uv from
(select eventid,ssId,source,devicestype,province,timestamp,ip,tkid from $data where substr(timestamp,1,10)=date_sub(now(),$beforday)) t2
left join time_dim t1 on t1.time=substr(timestamp,12,2) left join minute_dim t3 on t3.m=substr(timestamp,15,2) --and timestamp is not null
group by t3.m,t1.time,date_sub(now(),0),source,devicestype,province
order by t1.time,t3.m
""")
      this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }
    def writeMinuteLevelClickIndicators(beforday:Int,token:String="lzlh",event:String="click")={
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "MinuteLevelClickIndicators","credentials"-> "root,admin,mongo2tiger")
      val data=token+event+"data"
      val da1=sqlContext.sql(s"""
select nid,date_sub(now(),$beforday) date,t1.time,t3.m,source,devicestype,province,count(t2.eventid) pv,count(distinct t2.ssId) sv ,count(distinct t2.ip) ips,count(distinct tkid) uv from
(select eventid,ssId,source,devicestype,province,timestamp,ip,tkid,nid from $data where substr(timestamp,1,10)=date_sub(now(),$beforday)) t2
left join time_dim t1 on t1.time=substr(timestamp,12,2) left join minute_dim t3 on t3.m=substr(timestamp,15,2) --and timestamp is not null
group by t3.m,t1.time,date_sub(now(),0),source,devicestype,province,nid
order by t1.time,t3.m
""")
      this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }
    def writeNewOldVisitors(beforday:Int,token:String="lzlh",event:String="pageview",overwriterflag:Int=0)={
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "NewOldVisitors","credentials"-> "root,admin,mongo2tiger")
      val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      res1.registerTempTable("NewOldVisitors")
      var da1:org.apache.spark.sql.DataFrame=null
      val data=token+event+"data"
      if(overwriterflag==0){
        da1=sqlContext.sql(s"""
        select date_sub(now(),$beforday) as date,substr(timestamp,12,2) time,substr(timestamp,15,2) m,c.tkid,case when d.isnew is null then 1 else 0 end isnew from (
            select distinct a.tkid,timestamp from ${data} a where substr(a.timestamp,1,10)=date_sub(now(),$beforday)) c
            left join (select distinct tkid,0 isnew from NewOldVisitors where substr(date,1,10)<=date_sub(now(),$beforday+1)) d on d.tkid=c.tkid
            """)
        // da1.show

        this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      }else{
        da1=sqlContext.sql(s"""
        select date_sub(now(),$beforday) date,substr(timestamp,12,2) time,substr(timestamp,12,2) m,c.tkid,case when d.isnew is null then 1 else 0 end isnew from (
            select distinct a.tkid,timestamp from ${data} a where substr(a.timestamp,1,10)=date_sub(now(),$beforday)) c
            left join (select distinct tkid,0 isnew from $data where substr(timestamp,1,10)<=date_sub(now(),$beforday+1)) d on d.tkid=c.tkid
            """)

        this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt).save()}

      }
      // val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //res1.show
      //res1.registerTempTable("NewOldVisitors")
    }
    def writeNewOldVisitorsIndicators(beforday:Int,token:String="lzlh",event:String="pageview")={
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "NewOldVisitors","credentials"-> "root,admin,mongo2tiger")
      val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      res1.registerTempTable("NewOldVisitors")
      val data=token+event+"data"
      val opt1=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "NewOldVisitorsIndicators","credentials"-> "root,admin,mongo2tiger")
      var da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,substr(timestamp,12,2) time, substr(timestamp,15,2) m ,isnew,source,devicestype,province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
from NewOldVisitors a left join $data d on a.tkid=d.tkid and substr(d.timestamp,1,10)=date_sub(now(),$beforday) and substr(a.date,1,10)=date_sub(now(),$beforday)
group by substr(timestamp,15,2),substr(timestamp,12,2), date_sub(now(),$beforday),isnew,source,devicestype,province
""")
      // da1.show
      this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt1).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }
    def writePVLongIndicators(beforday:Int,token:String="lzlh",event:String="pageview")={
      var opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "NewOldVisitors","credentials"-> "root,admin,mongo2tiger")
      val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      res1.registerTempTable("NewOldVisitors")
      val data=token+event+"data"
      opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "PVLongIndicators","credentials"-> "root,admin,mongo2tiger")
      val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,substr(timestamp,12,2) time, substr(timestamp,15,2) m,d.tkid,isnew,sourcetype,source,devicestype,province,ssId ssId,
first(case when d.properties.url_domain='twotiger.com' then d.properties.url end) firsturl, last(case when d.properties.url_domain='twotiger.com' then d.properties.url end) lasturl,
count(ssId) p,max(unix_timestamp(ISODateToSqlDate(timestamp)))-min(unix_timestamp(ISODateToSqlDate(timestamp))) vstimes ,(max(unix_timestamp(ISODateToSqlDate(timestamp)))-min(unix_timestamp(ISODateToSqlDate(timestamp))))/count(ssId) avgpagevstimes
from $data d left join NewOldVisitors a on a.tkid=d.tkid and substr(d.timestamp,1,10)=date_sub(now(),$beforday) and substr(a.date,1,10)=date_sub(now(),$beforday)
where substr(d.timestamp,1,10)=date_sub(now(),$beforday)
group by ssId,isnew,substr(d.timestamp,12,2), substr(d.timestamp,15,2),date_sub(now(),$beforday),d.tkid,sourcetype,source,devicestype,province
order by date desc,time desc,m desc
""")
      // da1.show
      this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }
    def unCacheTim(): Unit ={
      sqlContext.uncacheTable("date_dim")
      sqlContext.uncacheTable("time_dim")
      sqlContext.uncacheTable("minute_dim")
      sqlContext.uncacheTable("source_dim")
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
