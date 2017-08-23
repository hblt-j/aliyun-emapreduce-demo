package com.bigdata.lzlh
import java.io.{FileNotFoundException, IOException}

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

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object pv {
  val conf=new SparkConf().setAppName("pv job 1h").setMaster("yarn-client")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    import sqlContext.implicits._
    sqlContext.udf.register("ISODateToSqlDate", (s: String) => {
      var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
    })
    //sqlContext.udf.register("ISODateToSqlDate",this.ISODateToSqlDate _)
    var date:String=null
    if (args.length > 0) {
      date=args(0)
    }

    // var i=0
    // for(i <- 0 to 2
    // ){
    //     val date="2017-06-13 1"+i+":00"
    //     println(date)

    loadTim
    if (loadData(getPathIn(date)) > 0) {
      if(clearData()>0) {
        //      var beforday=befordays(date)
        //      writeDayLevelIndicators(beforday)
        //      println("====================track end:writeDayLevelIndicators")
        //      //sqlContext.sql(s"select ISODateToSqlDate(timestamp) st,* from data where substr(timestamp,1,10)=date_sub(now(),$beforday)").show()
        //      writeTrackpageview(beforday)
        //      writePVLongIndicators(beforday)
        //
        //        writeDayLevelNewOldVisitors(beforday,1)
        //        writeDayLevelNewOldVisitorsIndicators(beforday)
        //
        //        writeDayLevelSourceIndicators(beforday)
        //        writeDayLevelDevicesIndicators(beforday)
        //        writeDayLevelIndicators(beforday)
        //        writeHourLevelIndicators(beforday)
        //
        //        writeDayLevelRegionIndicators(beforday)
        //        writeDayLevelPageIndicators(beforday)
        //          writeDayLevelFirstPageIndicators(beforday)
        //         writeDayLevelLastPageIndicators(beforday)
        println("=================\"pageview\",\"lzlh\"=================")
        dayLevelAppend(befordays(date))
        println("====================track begin:writepvProfile")
        writepvProfile()
        println("====================track end:writepvProfile")
      }
      if(clearData("pageview","lzlh_touch")>0) {
        println("=================\"pageview\",\"lzlh_touch\"====================")
        dayLevelAppend(befordays(date),"pageview","lzlh_touch")
        println("====================track begin:writepvProfile")
        writepvProfile("pageview","lzlh_touch")
        println("====================track end:writepvProfile")
      }
      unCacheTim()
      // writeWeekLevelIndicators(befordays(date))
      // writeMonthLevelIndicators(befordays(date))
      // writeQuarterLevelIndicators(befordays(date))
    }
    // }
    sc.stop()
  }

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

    props += ("dbtable" -> "source_dim")

    jdbcDF = sqlContext.load("jdbc", props.asJava) //加载注册多表
    jdbcDF.registerTempTable("source_dim")
    sqlContext.cacheTable("source_dim") //缓存表
    //    sqlContext.sql("select * from source_dim").show;

  }
  def loadData(pathIn:String):Long={
    var re:Long=0
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("io.compression.codec.snappy.native", "true")//加载 日志样例
    //val pathIn = "oss://LTAIanYcMuGu2D3x:Nvqs7Fc82ygzAyv9BIVRpDK84nkMLv@trackstore.vpc100-oss-cn-beijing.aliyuncs.com/track_/2017/03/05/00/09_1488643787004340868_32297553.snappy"
    //val pathIn="oss://LTAIanYcMuGu2D3x:Nvqs7Fc82ygzAyv9BIVRpDK84nkMLv@tt-big-data.vpc100-oss-cn-beijing.aliyuncs.com/C-0A780AAFD528EF4E/172.16.2.113/log/http-request.log"
    //val pathIn = "oss://logstash-json/track_/2017/06/0*/*"//,oss://logstash-json/track_/2017/05/19/*"//,oss://logstash-json/track_/2017/05/20/*"//只支持一级目录下的文件，子目录不支持5
    if(fileExists(pathIn.substring(0,pathIn.length-2))) {//时级
      val inputData = sc.textFile(pathIn)
      if (inputData.count() > 0) {
        import sqlContext.implicits._
        import sun.misc.{BASE64Encoder, BASE64Decoder}
        import java.net.{URLDecoder, URLEncoder}
        import scala.util.parsing.json.{JSON, JSONObject}
        //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        val spark = sqlContext
        val df0 = spark.read.json(inputData)
        //.as[PV]
        if (df0.count() > 0) {
          val df = df0.select("data")
          if (df.count() > 0) {
            val data = df.map(t => t(0).toString.replace("=>", ":").replace("$", "")).filter(t => "" != t)
            //数据处理
            val da = spark.read.json(data)
            //da.show
            da.registerTempTable("data0") //注册表
            re = da.count
          }
        }
      }
    }
    re
    // sqlContext.cacheTable("data")
    // import java.text.SimpleDateFormat
    //  import org.joda.time.format._
    //  import org.joda.time._
    // import java.sql.Timestamp
    // import org.apache.spark.sql._
    // val da2=da.select("ip","timestamp")

    // def ISODateToSqlDate (s:String):Timestamp={
    //     var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //     Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
    // }

    // val da3=da2.rdd.map(s=>{(ISODateToSqlDate(s.getAs[String]("timestamp")),""+s(0))})//red 处理并转dataFrame
    // da3.toDF("timestamp","ip").show
    // sqlContext.udf.register("ISODateToSqlDate", (s: String) => ISODateToSqlDate(s))//二、注册自定义查询函数
    // sqlContext.sql("select ip,ISODateToSqlDate(timestamp) st from data").show
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
  def clearData(event:String="pageview",token:String="lzlh",Type:String="track"):Long={
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
            from data0 where token='"""+token+"' and type='"+Type+"' and event='"+event+"' and nid is not null"+
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
                from data0 where token='"""+token+"' and type='"+Type+"' and event='"+event+"'"+
          """) d left join source_dim s on d.properties.referrer_host=s.match
          """)
      }
      re=da.count//数据清洗
      da.registerTempTable("data")//注册表
      sqlContext.cacheTable("data")
    }catch {
      case ex: AnalysisException =>{
        println(token+event+":ignore empty or error date insert")
      }
    }
    re
  }
  def writeDayLevelNewOldVisitors(beforday:Int,event:String="pageview",token:String="lzlh",overwriterflag:Int=0)={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelNewOldVisitors","credentials"-> "root,admin,mongo2tiger")
    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res1.registerTempTable("DayLevelNewOldVisitors")
    if(overwriterflag==0){
      val da1=sqlContext.sql("select date_sub(now(),"+beforday+") date,c.tkid,case when d.isnew is null then 1 else 0 end isnew from ("
        +"select distinct a.tkid from data a where substr(a.timestamp,1,10)=date_sub(now(),"+beforday+")) c "
        //+"left join (select distinct tkid,0 isnew from data where substr(timestamp,1,10)<=date_sub(now(),"+beforday+1+")) d on d.tkid=c.tkid"
        +"left join (select distinct tkid,0 isnew from DayLevelNewOldVisitors where substr(date,1,10)<=date_sub(now(),"+beforday+1+")) d on d.tkid=c.tkid")//.show;

      da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()
    }else{
      val da1=sqlContext.sql("select date_sub(now(),"+beforday+") date,c.tkid,case when d.isnew is null then 1 else 0 end isnew from ("
        +"select distinct a.tkid from data a where substr(a.timestamp,1,10)=date_sub(now(),"+beforday+")) c "
        +"left join (select distinct tkid,0 isnew from data where substr(timestamp,1,10)<=date_sub(now(),"+beforday+1+")) d on d.tkid=c.tkid")
      //+"left join (select distinct tkid,0 isnew from DayLevelNewOldVisitors where date<=date_sub(now(),"+beforday+1+")) d on d.tkid=c.tkid")//.show;

      this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt).save()}

    }
    //val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //res1.show
    //res1.registerTempTable("DayLevelNewOldVisitors")
  }
  def writeDayLevelNewOldVisitorsIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelNewOldVisitors","credentials"-> "root,admin,mongo2tiger")
    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res1.registerTempTable("DayLevelNewOldVisitors")
    val opt1=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelNewOldVisitorsIndicators","credentials"-> "root,admin,mongo2tiger")
    var da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date ,isnew,source,devicestype,province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
from DayLevelNewOldVisitors a left join data d on a.tkid=d.tkid and substr(d.timestamp,1,10)=date_sub(now(),$beforday) and substr(a.date,1,10)=date_sub(now(),$beforday)
group by isnew,source,devicestype,province
""")//.show

    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt1).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def writeDayLevelIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select t.y,t.q,t.m,t.w,t.date date,d.source,d.devicestype,d.province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
from data d
right join date_dim t on substr(d.timestamp,1,10)=t.date and t.date=date_sub(now(),$beforday)
where substr(d.timestamp,1,10) = date_sub(now(),$beforday)
group by t.y,t.q,t.m,t.w,t.date,d.source,d.devicestype,d.province
""")
    //da1.count()
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def writeDayLevelSourceIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelSourceIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,sourcetype ,source,devicestype,province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct tkid) uv
from data d WHERE substr(d.timestamp,1,10)=date_sub(now(),$beforday)
--left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
group by sourcetype,source,devicestype,province
""")
      //da1.count()

    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def writeDayLevelDevicesIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelDevicesIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,devicestype,province,properties.model,properties.os,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct tkid) uv
from data d where substr(d.timestamp,1,10)=date_sub(now(),$beforday)
--left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
group by properties.os,properties.model,devicestype,province
""")//.show;

    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }


  def writeHourLevelIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "HourLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,t1.time,source,devicestype,province,count(t2.eventid) pv,count(distinct t2.ssId) sv ,count(distinct t2.ip) ips,count(distinct tkid) uv from
(select eventid,ssId,source,devicestype,province,timestamp,ip,tkid from data where substr(timestamp,1,10)=date_sub(now(),$beforday)) t2 right join time_dim t1 on t1.time=substr(timestamp,12,2)
group by t1.time,date_sub(now(),$beforday),source,devicestype,province
order by t1.time
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }

  def writeDayLevelRegionIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelRegionIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,province,count(eventid) pv,count(distinct ssId) sv,count(distinct ip) ips,count(distinct tkid) uv
from data where substr(timestamp,1,10)=date_sub(now(),$beforday)
--left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
group by province
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  @deprecated
  def writePVLongIndicators_old(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "PVLongIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select date_sub(now(),$beforday) date,tkid,sourcetype,source,devicestype,province,ssId ssId,count(ssId) p,max(unix_timestamp(ISODateToSqlDate(timestamp)))-min(unix_timestamp(ISODateToSqlDate(timestamp))) vstimes ,(max(unix_timestamp(ISODateToSqlDate(timestamp)))-min(unix_timestamp(ISODateToSqlDate(timestamp))))/count(ssId) avgpagevstimes
from data
where substr(timestamp,1,10)=date_sub(now(),$beforday)
--left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
group by ssId,date_sub(now(),$beforday),tkid,sourcetype,source,devicestype,province order by date_sub(now(),$beforday) desc
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }


  def writeDayLevelPageIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelPageIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select first(date) date,url,source,devicestype,province,count(eventid) pagepv,count(distinct tkid) pageuv,count(distinct ssId) pagenum,count(distinct ip) ips,count(distinct ssId)/count(distinct tkid) avgssIdpagenum--,sum(vstimes)/count(distinct ssId) avgvstimes--,count(case when p=1 and n=1 then p end)/count(distinct ssId)*100 jumpv
from (
    -- select a.date,a.ssId,a.url,count(a.ssId) p,a.tkid,a.ip,unix_timestamp(ISODateToSqlDate(last(a.timestamp)))-unix_timestamp(ISODateToSqlDate(first(a.timestamp))) vstimes
    -- from (
        select eventid,date_sub(now(),$beforday) date,ssId ssId,source,devicestype,province,properties.url,properties.url_domain,tkid,ip,timestamp
        from data
        where substr(timestamp,1,10) = date_sub(now(),$beforday)
        --left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
        order by timestamp asc
    -- ) a
    -- group by a.ssId,a.date,a.url,a.tkid,a.ip
) b
group by url,source,devicestype,province
order by pagepv desc
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def writeDayLevelFirstPageIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelFirstPageIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select first(b.date) date,firsturl,source,devicestype,province,sum(p) firstpagepv,count(distinct tkid) firstpageuv,count(firsturl) firstpagenum,count(distinct ip) ips,count(firsturl)/count(distinct tkid) avgpagenum,sum(vstimes)/count(firsturl) avgvstimes,count(case p when 1 then p end)/count(firsturl)*100 jumpv
from (
    select a.date date,a.source,a.devicestype,a.province,a.ssId,first(case when a.url_domain='twotiger.com' then url end) firsturl, last(case when a.url_domain='twotiger.com' then url end) lasturl,count(a.ssId) p,a.tkid,a.ip,unix_timestamp(ISODateToSqlDate(last(a.timestamp)))-unix_timestamp(ISODateToSqlDate(first(a.timestamp))) vstimes
    from (
        select date_sub(now(),$beforday) date,source,devicestype,province,ssId ssId,properties.url,properties.url_domain,tkid,ip,timestamp
        from data
        where substr(timestamp,1,10) = date_sub(now(),$beforday)
        --left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
        order by timestamp asc
    ) a
    group by a.ssId,a.date,a.tkid,a.ip,a.source,a.devicestype,a.province
    having firsturl is not null
) b
group by firsturl,source,devicestype,province
order by firstpagepv desc
""")
//    da1.count()
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def writeDayLevelLastPageIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "DayLevelLastPageIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select first(b.date) date,lasturl,source,devicestype,province,sum(p) lastpagepv,count(distinct tkid) lastpageuv,count(lasturl) lastpagenum,count(distinct ip) ips,count(lasturl)/count(distinct tkid) avgpagenum,sum(vstimes)/count(lasturl) avgvstimes,count(case p when 1 then p end)/count(lasturl)*100 jumpv
from (
    select a.date,a.source,a.devicestype,a.province,a.ssId,first(case when a.url_domain='twotiger.com' then url end) firsturl, last(case when a.url_domain='twotiger.com' then url end) lasturl,count(a.ssId) p,a.tkid,a.ip,unix_timestamp(ISODateToSqlDate(last(a.timestamp)))-unix_timestamp(ISODateToSqlDate(first(a.timestamp))) vstimes
    from (
        select date_sub(now(),$beforday) date,source,devicestype,province,ssId ssId,properties.url,properties.url_domain,tkid,ip,timestamp
        from data
        where substr(timestamp,1,10)=date_sub(now(),$beforday)
        --left join date_dim on substr(timestamp,1,10)=date_dim.date and date_dim.date=date_sub(now(),$beforday)  --join维度表
        order by timestamp asc
    ) a
    group by a.ssId,a.date,a.tkid,a.ip,a.source,a.devicestype,a.province
    having lasturl is not null
) b
group by lasturl,source,devicestype,province
order by lastpagepv desc
""")
//    da1.count()
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def writepvProfile(event:String="pageview",token:String="lzlh")={
    var opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "HourLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res1.registerTempTable("HourLevelIndicators")
    opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "pvProfile","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql("""
           select 'today' as date,sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) = date_sub(now(),0) group by date
           union all
           select 'yesterday' as date,sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) = date_sub(now(),1) group by date
           union all
           select t1.date,round(t1.pv*t2.pv,0) pv,round(t1.sv*t2.sv,0) sv,round(t1.ips*t2.ips,0) ips,round(t1.uv*t2.uv,0) uv from
           (select 'forecast' as date,sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) = date_sub(now(),0) group by date) t1 ,
           (
               select sum(case when pv is not null then pv end)/(case when sum(case when hour<=hour(now()) and pv is not null  then pv end ) is null  then 1 else sum(case when hour<=hour(now()) and pv is not null  then pv end ) end ) pv,
               sum(case when sv is not null then sv end)/(case when sum(case when hour<=hour(now()) and sv is not null  then sv end ) is null  then 1 else sum(case when hour<=hour(now()) and sv is not null  then sv end ) end ) sv,
               sum(case when uv is not null then uv end)/(case when sum(case when hour<=hour(now()) and uv is not null  then uv end ) is null  then 1 else sum(case when hour<=hour(now()) and uv is not null  then uv end ) end ) uv ,
               sum(case when ips is not null then ips end)/(case when sum(case when hour<=hour(now()) and ips is not null  then ips end ) is null  then 1 else sum(case when hour<=hour(now()) and ips is not null  then ips end ) end ) ips from
                   (select time hour,sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) >= date_sub(now(),7) and substr(date,1,10) <= date_sub(now(),1) group by time) a
           ) t2
           union all
           select 'yesterdaythistime' as date,sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) = date_sub(now(),1) and time<=hour(now()) group by date
           --union all
           --select 'total' as date,sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from (select sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) >= date_sub(now(),6) group by date) t
           union all
           select 'dayavg' as date,avg(pv) pv,avg(sv) sv,avg(ips) ips,avg(uv) uv from (select sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) >= date_sub(now(),6) group by date) t
           union all
           select 'daymax' as date,max(pv) pv,max(sv) sv,max(ips) ips,max(uv) uv from (select sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) >= date_sub(now(),6) group by date) t
           --union all
           --select 'daymin' as date,min(pv) pv,min(sv) sv,min(ips) ips,min(uv) uv from (select sum(pv) pv,sum(sv) sv,sum(ips) ips,sum(uv) uv from HourLevelIndicators where substr(date,1,10) >= date_sub(now(),6) group by date) t
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  @deprecated
  def writeWeekLevelIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    // val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "WeekLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    // val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    // res1.registerTempTable("WeekLevelIndicators")
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "WeekLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select t.y,t.w,t.date date,d.source,d.devicestype,d.province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
from data d
right join date_dim t on substr(d.timestamp,1,10)=t.date and t.date=date_sub(now(),$beforday)
where substr(d.timestamp,1,10) = date_sub(now(),$beforday)
group by t.y,t.w,t.date,d.source,d.devicestype,d.province
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  @deprecated
  def writeMonthLevelIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "MonthLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select t.y,t.m,t.date date,d.source,d.devicestype,d.province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
from data d
right join date_dim t on substr(d.timestamp,1,10)=t.date and t.date=date_sub(now(),$beforday)
where substr(d.timestamp,1,10) = date_sub(now(),$beforday)
group by t.y,t.m,t.date,d.source,d.devicestype,d.province
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  @deprecated
  def writeQuarterLevelIndicators(beforday:Int,event:String="pageview",token:String="lzlh")={
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> token,Collection -> "QuarterLevelIndicators","credentials"-> "root,admin,mongo2tiger")
    val da1=sqlContext.sql(s"""
select t.y,t.q,t.date date,d.source,d.devicestype,d.province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
from data d
right join date_dim t on substr(d.timestamp,1,10)=t.date and t.date=date_sub(now(),$beforday)
where substr(d.timestamp,1,10) = date_sub(now(),$beforday)
group by t.y,t.q,t.date,d.source,d.devicestype,d.province
""")
    this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
    //     select t.y,t.q,t.date date,d.source,d.devicestype,d.province,count(distinct d.eventid) as pv,count(distinct d.ssId) as sv ,count(distinct d.ip) ips,count(distinct d.tkid) uv
    // from data d --(select timestamp,source,devicestype,province,eventid,ssId,ip,tkid from data where substr(timestamp,1,10) = date_sub(now(),11)) d
    // right join date_dim t on substr(d.timestamp,1,10)=t.date and t.date=date_sub(now(),11)
    // where substr(d.timestamp,1,10) = date_sub(now(),11)
    // group by t.y,t.q,t.date,d.source,d.devicestype,d.province
    //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    //    res1.show
  }
  def dayLevelAppend(beforday:Int,event:String="pageview",token:String="lzlh")= {
    import org.apache.spark.sql.SQLContext
    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
    import sqlContext.implicits._
//    println("====================track begin:writeTrackpageview")
//    writeTrackpageview(beforday)
//    println("====================track end:writeTrackpageview")
//    println("====================track begin:writePVLongIndicators")
//    writePVLongIndicators_old(beforday)
//    println("====================track end:writePVLongIndicators")

    println("====================track begin:writeDayLevelNewOldVisitors")
    writeDayLevelNewOldVisitors(beforday,event,token)
    println("====================track end:writeDayLevelNewOldVisitors")
    println("====================track begin:writeDayLevelNewOldVisitorsIndicators")
    writeDayLevelNewOldVisitorsIndicators(beforday,event,token)
    println("====================track end:writeDayLevelNewOldVisitorsIndicators")

    println("====================track begin:writeDayLevelIndicators")
    writeDayLevelIndicators(beforday,event,token)
    println("====================track end:writeDayLevelIndicators")
    println("====================track begin:writeHourLevelIndicators")
    writeHourLevelIndicators(beforday,event,token)
    println("====================track end:writeHourLevelIndicators")
    println("====================track begin:writeDayLevelSourceIndicators")
    writeDayLevelSourceIndicators(beforday,event,token)
    println("====================track end:writeDayLevelSourceIndicators")
    println("====================track begin:writeDayLevelDevicesIndicators")
    writeDayLevelDevicesIndicators(beforday,event,token)
    println("====================track end:writeDayLevelDevicesIndicators")

    println("====================track begin:writeDayLevelRegionIndicators")
    writeDayLevelRegionIndicators(beforday,event,token)
    println("====================track end:writeDayLevelRegionIndicators")
    println("====================track begin:writeDayLevelPageIndicators")
    writeDayLevelPageIndicators(beforday,event,token)
    println("====================track end:writeDayLevelPageIndicators")
    println("====================track begin:writeDayLevelFirstPageIndicators")
    writeDayLevelFirstPageIndicators(beforday,event,token)
    println("====================track end:writeDayLevelFirstPageIndicators")
    println("====================track begin:writeDayLevelLastPageIndicators")
    writeDayLevelLastPageIndicators(beforday,event,token)
    println("====================track end:writeDayLevelLastPageIndicators")
    sqlContext.uncacheTable("data")
  }
  //date格式yyyy-MM-dd HH:mm:ss,level三种： d|h|m分别代表天时分级
  def getPathIn(datestr:String=null,level:String="h"):String={
    import java.text.SimpleDateFormat
    import java.util.Date
    var format = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    var date=new Date()
    if(datestr != null){
      date = format.parse(datestr);
    }
    var toFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd")
    var pathIn = "oss://logstash-json/track_/"

    if(level=="h"){
      toFormat= new SimpleDateFormat("yyyy/MM/dd/HH")
      pathIn+=toFormat.format(date)+"/*"
    }else if(level=="m"){
      toFormat = new SimpleDateFormat("yyyy/MM/dd/HH/mm")
      pathIn+=toFormat.format(date)+"*"
    }else{
      toFormat = new SimpleDateFormat("yyyy/MM/dd")
      pathIn+=toFormat.format(date)+"/*"
    }
    println("====================track begin:"+pathIn)
    pathIn
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
  def fileExists(dir:String):Boolean={
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{Path, FileSystem}
    var res:Boolean=false
    try{
      val path = new Path(dir.substring(0,dir.length-3))
      //    println(path.toString+"|"+dir)
      val conf = new Configuration()
      conf.set("fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
      val fs = FileSystem.get(path.toUri, conf)
      val fl=fs.listStatus(path)
      fl.foreach(f=>if(f.getPath.toString.startsWith(dir))res=true)
    }catch{
      case ex: FileNotFoundException =>{
        println("Missing file exception")
      }
      case ex: IOException => {
        println("IO Exception")
      }
    }
    res
  }
  def unCacheTim(): Unit ={
    sqlContext.uncacheTable("date_dim")
    sqlContext.uncacheTable("time_dim")
//    sqlContext.uncacheTable("minute_dim")
    sqlContext.uncacheTable("source_dim")
  }
}
