package com.bigdata.lzlh

import java.io.{FileNotFoundException, IOException}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat
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
/**
  * Created by ljj on 2017/7/17.
  */
object mobiTrack {
  val conf=new SparkConf().setAppName("mobi job 10m").setMaster("yarn-client")
  val sc = new SparkContext(conf)
  //val sqlContext = new SQLContext(sc)
  val sqlContext: SQLContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    import sqlContext.implicits._
    //    sqlContext.udf.register("ISODateToSqlDate", (s: String) => {
    //      var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //      Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
    //    })
    var date:String=null
    if (args.length > 0) {
      date=args(0)
    }
    /*for(j <- 5 to 17){
      for(i <- 1 to 23 ){
        var date="2017-07-0"+j+" 0"+i+":00"

        if(loadData(getPathIn(date,"h"),"h")>0){
          clearData
          // import sqlContext.implicits._
          // sqlContext.udf.register("ISODateToSqlDate", (s: String) => ISODateToSqlDate(s))

          // var beforday=befordays(date)


          writeEvent()
          println("writeEvent")
          writeProp()
          println("writeProp")
        }
      }
    }*/
    //date="2017-07-13 16:02"
    var i=0
    var pathIn=getPathIn(date,"m")
    for(i <- 0 to 9){//分级 每10分钟
    var pathin=pathIn.substring(0,pathIn.length-2)+i+"*"
      if(loadData(pathin)>0){
        clearData()
        println(pathin)
        writeEvent()
        println("writeEvent")
        writeProp()
        println("writeProp")
      }
    }
    sc.stop()
  }
  def getAppName(app:String="twotiger"):String={
    var appName=""
    if(app=="twotiger"){
      appName="lzlh"
    }else{
      appName=app
    }
    appName+"Mobi"
  }
  def writeEvent(app:String="twotiger"){
    try{
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "pageviewEvent","credentials"-> "root,admin,mongo2tiger")
      val da1=sqlContext.sql(s"""
    select id,type,event,version,tkid,ssId,uid,channel,time,className,activeTime from event where type="track" and event="pageview"
    """)
      if(da1.count>0)this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }catch {
      case ex: AnalysisException =>{
        println("pageview：ignore empty or error date insert")
      }
    }
    try{
      val opt1=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "clickEvent","credentials"-> "root,admin,mongo2tiger")
      val da2=sqlContext.sql(s"""
    select id,type,event,version,tkid,ssId,uid,channel,time,eventId from event where type="track" and event="click"
    """)
      if(da2.count>0)this.synchronized {da2.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt1).save()}
    }catch {
      case ex: AnalysisException =>{
        println("clickEvent：ignore empty or error date insert")
      }
    }
    try{
      val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "webviewEvent","credentials"-> "root,admin,mongo2tiger")
      val da3=sqlContext.sql(s"""
    select id,type,event,version,tkid,ssId,uid,channel,time,url,title from event where type="track" and event="webview"
    """)
      if(da3.count>0)this.synchronized {da3.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt2).save()}
    }catch {
      case ex: AnalysisException =>{
        println("webviewEvent：ignore empty or error date insert")
      }
    }
    try{
      val opt3=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "errorEvent","credentials"-> "root,admin,mongo2tiger")
      val da4=sqlContext.sql(s"""
    select id,type,event,version,tkid,ssId,uid,channel,time,errorType,errorInfo from event where type="track" and event="error"
    """)
      if(da4.count>0)this.synchronized {da4.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt3).save()}
    }catch {
      case ex: AnalysisException =>{
        println("errorEvent：ignore empty or error date insert")
      }
    }
  }
  def writeProp(app:String="twotiger"){
    try{
      val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "prop","credentials"-> "root,admin,mongo2tiger")
      val da1=sqlContext.sql(s"""
select id,token,ip,addr,region_code,deviceName,deviceBrand,deviceModel,isSocketProxy,canGps,location,language,networkState,netCarrier,devicePixel,appVersion,osVersion,imsi,wifi_mac,imei,androidId,isRoot,mac,isSimulator,
    cpu_abi,fingerprint,serial,isPrisonBreak,openUdid,adfa,adfv from data0
""")
      if(da1.count>0)this.synchronized {da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt).save()}
      //    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
      //    res1.show
    }catch {
      case ex: AnalysisException =>{
        println("prop：ignore empty or error date insert")
      }
    }
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
    var pathIn = "oss://logstash-json/app/"

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
      val conf = new Configuration()
      conf.set("fs.oss.impl", "com.aliyun.fs.oss.nat.NativeOssFileSystem")
      val fs = FileSystem.get(path.toUri,conf)
      //println(path.toUri+"|"+dir)
      val fl=fs.listStatus(path)
      fl.foreach(f=>{
        if(f.getPath.toString.startsWith(dir))res=true
        //   println(f.getPath.toString)
      })
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
  def clearData(app:String="twotiger"){
    val da0=sqlContext.sql(s"""
    select regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") id,* from data where split(token,"_")[0]="$app"
    """)
    da0.registerTempTable("data0")//注册表
    // val da1=sqlContext.sql("""
    // select * from data0
    // """)
    // da1.registerTempTable("prop")//注册表
    val da2=sqlContext.sql("select id,ls.* from(select id,explode(list) ls from data0) d")
    da2.registerTempTable("event")//注册表

  }
  def loadData(pathIn:String,level:String="m"):Long={
    var re:Long=0
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("io.compression.codec.snappy.native", "true")//加载 日志样例
    if(fileExists(pathIn.substring(0,if("h"==level)pathIn.length-2 else pathIn.length-1))) {
      val inputData = sc.textFile(pathIn)
      if (inputData.count() > 0) {
        val spark = sqlContext
        val df0 = spark.read.json(inputData)
        if (df0.count() > 0) {
          val df = df0.select("data")
          if (df.count() > 0) {
            val data = df.map(t => t(0).toString.replace("=>", ":").replace("$", "")).filter(t => "" != t)
            //数据处理
            val da = spark.read.json(data)
            //da.show
            da.registerTempTable("data") //注册表
            re = da.count
          }
        }
      }
    }
    re
  }
}
