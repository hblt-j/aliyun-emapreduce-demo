package com.bigdata.lzlh

import java.io.{FileNotFoundException, IOException}
import java.text.SimpleDateFormat
import java.util.Date

import com.stratio.datasource.mongodb.config.MongodbConfig._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ljj on 2017/7/17.
  */
object mobiIndicator {
  val conf=new SparkConf().setAppName("mobi job 1h").setMaster("yarn-client")
  val sc = new SparkContext(conf)
  //val sqlContext = new SQLContext(sc)
  val sqlContext: SQLContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //    sqlContext.udf.register("ISODateToSqlDate", (s: String) => {
    //      var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //      Timestamp.valueOf(sdf.format(ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.getDefault()).parseDateTime(s).toDate))
    //    })
    var date:String=null
    if (args.length > 0) {
      date=args(0)
    }
    // for(j <- 20 to 24){
    // for(i <- 0 to 23 ){
    // var date="2017-07-0"+j+" 0"+i+":00"
    var beforday=befordays(date)
    writeErrIndicator(beforday)
    println("twotiger:writeErrIndicator")
    writeUserIndicator(beforday)
    println("twotiger:writeUserIndicator")
    writeNewOldVisitors(beforday)
    println("twotiger:writeNewOldVisitors")
    writeNewOldVisitorsIndicators(beforday)
    println("twotiger:writeNewOldVisitorsIndicators")
    writeNewVisitorsProfileIndicator()
    println("twotiger:writeNewVisitorsProfileIndicator")
    writePVProfileIndicator()
    println("twotiger:writePVProfileIndicator")
    writeProfileIndicator()
    println("twotiger:writeProfileIndicator")
    writeErrorProfileIndicator()
    println("twotiger:writeErrorProfileIndicator")
    // }
    // }
    sc.stop()
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
  def getAppName(app:String="twotiger"):String={
    var appName=""
    if(app=="twotiger"){
      appName="lzlh"
    }else{
      appName=app
    }
    appName+"Mobi"
  }

  def writeNewOldVisitors(beforday:Int,app:String="twotiger",overwriterflag:Int=0){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "pageviewEvent","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("pageviewEvent")
    val opt1=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "NewOldVisitors","credentials"-> "root,admin,mongo2tiger")
    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt1).load
    res1.registerTempTable("NewOldVisitors")
    var da1:org.apache.spark.sql.DataFrame=null
    if(overwriterflag==0){
      da1=sqlContext.sql(s"""
        select date_sub(now(),$beforday) date,substr(time,12,2) h,substr(time,15,2) m,c.tkid,case when d.isnew is null then 1 else 0 end isnew from (
            select distinct a.tkid,time from pageviewEvent a where substr(a.time,1,10)=date_sub(now(),$beforday)) c
            left join (select distinct tkid,0 isnew from NewOldVisitors where substr(date,1,10)<=date_sub(now(),$beforday+1)) d on d.tkid=c.tkid
            """)
      da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt1).save()
    }else{
      da1=sqlContext.sql(s"""
        select date_sub(now(),$beforday) date,substr(time,12,2) h,substr(time,12,2) m,c.tkid,case when d.isnew is null then 1 else 0 end isnew from (
            select distinct a.tkid,time from pageviewEvent a where substr(a.time,1,10)=date_sub(now(),$beforday)) c
            left join (select distinct tkid,0 isnew from pageviewEvent where substr(time,1,10)<=date_sub(now(),$beforday+1)) d on d.tkid=c.tkid
            """)
      da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt1).save()
    }
  }
  def writeNewOldVisitorsIndicators(beforday:Int,app:String="twotiger"){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "pageviewEvent","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("pageviewEvent")
    val opt0=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "NewOldVisitors","credentials"-> "root,admin,mongo2tiger")
    val res1=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt0).load
    res1.registerTempTable("NewOldVisitors")
    val opt1=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "NewOldVisitorsIndicators","credentials"-> "root,admin,mongo2tiger")
    var da1=sqlContext.sql(s"""
            select date_sub(now(),$beforday) date,substr(time,12,2) h, substr(time,15,2) m ,isnew,count(id) num,count(distinct d.tkid) uv,count(distinct ssId) pv,count(distinct uid) auv,count(id)/count(distinct ssId) avgPvNum,
            count(id)/count(distinct d.tkid) avgUvNum,count(id)/count(distinct uid) avgAuvNum,
            count(distinct d.tkid)/count(distinct uid) avgAuvUv,sum(activeTime) timeLen,sum(activeTime)/count(distinct ssId) avgPvTimeLen,sum(activeTime)/count(distinct d.tkid) avgUvTimeLen,sum(activeTime)/count(distinct uid) avgAuvTimeLen,
            first(time) firstTime,last(time) lastTime,channel,version --,tkid,ssId,uid
            from NewOldVisitors a left join pageviewEvent d on a.tkid=d.tkid and a.tkid is not null and substr(d.time,1,10)=date_sub(now(),$beforday) and substr(a.date,1,10)=date_sub(now(),$beforday)
            group by substr(time,15,2),substr(time,12,2), date_sub(now(),$beforday),isnew,channel,version
            """)
    da1.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt1).save()
  }
  def writeUserIndicator(beforday:Int,app:String="twotiger"){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "pageviewEvent","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("pageviewEvent")
    val da=sqlContext.sql(s"""
        select count(id) num,count(distinct tkid) uv,count(distinct ssId) pv,count(distinct uid) auv,count(id)/count(distinct ssId) avgPvNum,count(id)/count(distinct tkid) avgUvNum,count(id)/count(distinct uid) avgAuvNum,
            count(distinct tkid)/count(distinct uid) avgAuvUv,sum(activeTime) timeLen,sum(activeTime)/count(distinct ssId) avgPvTimeLen,sum(activeTime)/count(distinct tkid) avgUvTimeLen,sum(activeTime)/count(distinct uid) avgAuvTimeLen,
            first(time) firstTime,last(time) lastTime,channel,version,tkid,ssId,uid,date_sub(now(),$beforday) date,substr(time,12,2) h  --,className, substr(time,15,2) m
        from pageviewEvent
        where substr(time,1,10)=date_sub(now(),$beforday)
        group by channel,version,tkid,ssId,uid,date_sub(now(),$beforday),substr(time,12,2) --, substr(time,15,2)
        """)//.show
    val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "userIndicator","credentials"-> "root,admin,mongo2tiger")
    if(da.count>0)this.synchronized {da.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt2).save()}
  }
  def writeErrIndicator(beforday:Int,app:String="twotiger"){
    val opt3=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "errorEvent","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt3).load
    res.registerTempTable("errorEvent")
    val da4=sqlContext.sql(s"""
        select count(id) num,count(distinct tkid) uv,count(distinct ssId) pv,count(distinct uid) auv,count(id)/count(distinct ssId) avgPvNum,count(id)/count(distinct tkid) avgUvNum,count(id)/count(distinct uid) avgAuvNum,
            count(distinct tkid)/count(distinct uid) avgAuvUv,first(time) firstTime,last(time) lastTime,channel,errorType,version,tkid,ssId,uid,date_sub(now(),$beforday) date,substr(time,12,2) h
        from errorEvent
        where substr(time,1,10)=date_sub(now(),$beforday)
        group by errorType,channel,version,tkid,ssId,uid,date_sub(now(),$beforday),substr(time,12,2)
        """)//.show
    val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "errorIndicator","credentials"-> "root,admin,mongo2tiger")
    if(da4.count>0)this.synchronized {da4.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(opt2).save()}
  }
  def writeProfileIndicator(app:String="twotiger"){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "NewOldVisitorsIndicators","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("NewOldVisitorsIndicators")
    val da=sqlContext.sql(s"""
       select 'newVisitorsTotal' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 --group by date
       union all
       select 'oldVisitorsTotal' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=0 --group by date
       union all
       select 'total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators --group by date
        """)//.show
    val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "profile","credentials"-> "root,admin,mongo2tiger")
    if(da.count>0)this.synchronized {da.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt2).save()}
  }
  def writeNewVisitorsProfileIndicator(app:String="twotiger"){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "NewOldVisitorsIndicators","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("NewOldVisitorsIndicators")
    val da=sqlContext.sql(s"""
        select 'today' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) = date_sub(now(),0) group by date
        union all
        select 'yesterday' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) = date_sub(now(),1) group by date
        union all
        select t1.date,round(t1.pv*t2.pv,0) pv,round(t1.num*t2.num,0) num,round(t1.auv*t2.auv,0) auv,round(t1.uv*t2.uv,0) uv,round(t1.timeLen*t2.timeLen,0) timeLen,round(t1.avgPvTimeLen*t2.avgPvTimeLen,0) avgPvTimeLen from
        (select 'forecast' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) = date_sub(now(),0) group by date) t1 ,
        (
          select sum(case when pv is not null then pv end)/(case when sum(case when hour<=hour(now()) and pv is not null  then pv end ) is null then 1 else sum(case when hour<=hour(now()) and pv is not null then pv end ) end ) pv,
          sum(case when num is not null then num end)/(case when sum(case when hour<=hour(now()) and num is not null then num end ) is null then 1 else sum(case when hour<=hour(now()) and num is not null then num end ) end ) num,
          sum(case when uv is not null then uv end)/(case when sum(case when hour<=hour(now()) and uv is not null  then uv end ) is null then 1 else sum(case when hour<=hour(now()) and uv is not null then uv end ) end ) uv ,
          sum(case when timeLen is not null then timeLen end)/(case when sum(case when hour<=hour(now()) and timeLen is not null then timeLen end ) is null then 1 else sum(case when hour<=hour(now()) and timeLen is not null then timeLen end ) end ) timeLen ,
          sum(case when avgPvTimeLen is not null then avgPvTimeLen end)/(case when sum(case when hour<=hour(now()) and avgPvTimeLen is not null then avgPvTimeLen end ) is null then 1 else sum(case when hour<=hour(now()) and avgPvTimeLen is not null then avgPvTimeLen end ) end ) avgPvTimeLen ,
          sum(case when auv is not null then auv end)/(case when sum(case when hour<=hour(now()) and auv is not null  then auv end ) is null then 1 else sum(case when hour<=hour(now()) and auv is not null then auv end ) end ) auv from
              (select h hour,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),7) and substr(date,1,10) <= date_sub(now(),1) group by h) a
        ) t2
        union all
        select 'yesterdaythistime' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) = date_sub(now(),1) and h<=hour(now()) group by date
        union all
        select '7total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(timeLen) timeLen,max(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(timeLen) timeLen,min(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '30total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(timeLen) timeLen,max(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(timeLen) timeLen,min(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),30) group by date) t
         union all
        select '60total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(timeLen) timeLen,max(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(timeLen) timeLen,min(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from NewOldVisitorsIndicators where isnew=1 and substr(date,1,10) >= date_sub(now(),60) group by date) t
 """)//.show
    val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "newVisitorsProfile","credentials"-> "root,admin,mongo2tiger")
    if(da.count>0)this.synchronized {da.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt2).save()}
  }
  def writePVProfileIndicator(app:String="twotiger"){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "userIndicator","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("userIndicator")
    val da=sqlContext.sql(s"""
        select 'today' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) = date_sub(now(),0) group by date
        union all
        select 'yesterday' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) = date_sub(now(),1) group by date
        union all
        select t1.date,round(t1.pv*t2.pv,0) pv,round(t1.num*t2.num,0) num,round(t1.auv*t2.auv,0) auv,round(t1.uv*t2.uv,0) uv,round(t1.timeLen*t2.timeLen,0) timeLen,round(t1.avgPvTimeLen*t2.avgPvTimeLen,0) avgPvTimeLen from
        (select 'forecast' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) = date_sub(now(),0) group by date) t1 ,
        (
          select sum(case when pv is not null then pv end)/(case when sum(case when hour<=hour(now()) and pv is not null  then pv end ) is null then 1 else sum(case when hour<=hour(now()) and pv is not null then pv end ) end ) pv,
          sum(case when num is not null then num end)/(case when sum(case when hour<=hour(now()) and num is not null then num end ) is null then 1 else sum(case when hour<=hour(now()) and num is not null then num end ) end ) num,
          sum(case when uv is not null then uv end)/(case when sum(case when hour<=hour(now()) and uv is not null  then uv end ) is null then 1 else sum(case when hour<=hour(now()) and uv is not null then uv end ) end ) uv ,
          sum(case when timeLen is not null then timeLen end)/(case when sum(case when hour<=hour(now()) and timeLen is not null then timeLen end ) is null then 1 else sum(case when hour<=hour(now()) and timeLen is not null then timeLen end ) end ) timeLen ,
          sum(case when avgPvTimeLen is not null then avgPvTimeLen end)/(case when sum(case when hour<=hour(now()) and avgPvTimeLen is not null then avgPvTimeLen end ) is null then 1 else sum(case when hour<=hour(now()) and avgPvTimeLen is not null then avgPvTimeLen end ) end ) avgPvTimeLen ,
          sum(case when auv is not null then auv end)/(case when sum(case when hour<=hour(now()) and auv is not null  then auv end ) is null then 1 else sum(case when hour<=hour(now()) and auv is not null then auv end ) end ) auv from
              (select h hour,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),7) and substr(date,1,10) <= date_sub(now(),1) group by h) a
        ) t2
        union all
        select 'yesterdaythistime' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) = date_sub(now(),1) and h<=hour(now()) group by date
        union all
        select '7total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(timeLen) timeLen,max(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(timeLen) timeLen,min(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '30total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(timeLen) timeLen,max(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(timeLen) timeLen,min(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '60total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(timeLen) timeLen,max(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(timeLen) timeLen,min(avgPvTimeLen) avgPvTimeLen from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(timeLen) timeLen,avg(avgPvTimeLen) avgPvTimeLen from userIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        """)//.show
    val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "PVProfile","credentials"-> "root,admin,mongo2tiger")
    if(da.count>0)this.synchronized {da.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt2).save()}
  }
  def writeErrorProfileIndicator(app:String="twotiger"){
    val opt=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "errorIndicator","credentials"-> "root,admin,mongo2tiger")
    val res=sqlContext.read.format("com.stratio.datasource.mongodb").options(opt).load
    res.registerTempTable("errorIndicator")
    val da=sqlContext.sql(s"""
        select 'today' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) = date_sub(now(),0) group by date
        union all
        select 'yesterday' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) = date_sub(now(),1) group by date
        union all
        select t1.date,round(t1.pv*t2.pv,0) pv,round(t1.num*t2.num,0) num,round(t1.auv*t2.auv,0) auv,round(t1.uv*t2.uv,0) uv,round(t1.avgAuvUv*t2.avgAuvUv,0) avgAuvUv,round(t1.avgPvNum*t2.avgPvNum,0) avgPvNum from
        (select 'forecast' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) = date_sub(now(),0) group by date) t1 ,
        (
          select sum(case when pv is not null then pv end)/(case when sum(case when hour<=hour(now()) and pv is not null  then pv end ) is null then 1 else sum(case when hour<=hour(now()) and pv is not null then pv end ) end ) pv,
          sum(case when num is not null then num end)/(case when sum(case when hour<=hour(now()) and num is not null then num end ) is null then 1 else sum(case when hour<=hour(now()) and num is not null then num end ) end ) num,
          sum(case when uv is not null then uv end)/(case when sum(case when hour<=hour(now()) and uv is not null  then uv end ) is null then 1 else sum(case when hour<=hour(now()) and uv is not null then uv end ) end ) uv ,
          sum(case when avgAuvUv is not null then avgAuvUv end)/(case when sum(case when hour<=hour(now()) and avgAuvUv is not null then avgAuvUv end ) is null then 1 else sum(case when hour<=hour(now()) and avgAuvUv is not null then avgAuvUv end ) end ) avgAuvUv ,
          sum(case when avgPvNum is not null then avgPvNum end)/(case when sum(case when hour<=hour(now()) and avgPvNum is not null then avgPvNum end ) is null then 1 else sum(case when hour<=hour(now()) and avgPvNum is not null then avgPvNum end ) end ) avgPvNum ,
          sum(case when auv is not null then auv end)/(case when sum(case when hour<=hour(now()) and auv is not null  then auv end ) is null then 1 else sum(case when hour<=hour(now()) and auv is not null then auv end ) end ) auv from
              (select h hour,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),7) and substr(date,1,10) <= date_sub(now(),1) group by h) a
        ) t2
        union all
        select 'yesterdaythistime' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) = date_sub(now(),1) and h<=hour(now()) group by date
        union all
        select '7total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(avgAuvUv) avgAuvUv,max(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '7daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(avgAuvUv) avgAuvUv,min(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),6) group by date) t
        union all
        select '30total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(avgAuvUv) avgAuvUv,max(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
        union all
        select '30daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(avgAuvUv) avgAuvUv,min(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),30) group by date) t
         union all
        select '60total' as date,sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60dayavg' as date,avg(pv) pv,avg(num) num,avg(auv) auv,avg(uv) uv,avg(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60daymax' as date,max(pv) pv,max(num) num,max(auv) auv,max(uv) uv,max(avgAuvUv) avgAuvUv,max(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        union all
        select '60daymin' as date,min(pv) pv,min(num) num,min(auv) auv,min(uv) uv,min(avgAuvUv) avgAuvUv,min(avgPvNum) avgPvNum from (select sum(pv) pv,sum(num) num,sum(auv) auv,sum(uv) uv,sum(avgAuvUv) avgAuvUv,avg(avgPvNum) avgPvNum from errorIndicator where substr(date,1,10) >= date_sub(now(),60) group by date) t
        """)//.show
    val opt2=Map(Host ->"dds-2ze629d32df5dd941.mongodb.rds.aliyuncs.com:3717", Database -> getAppName(app),Collection -> "ErrorProfile","credentials"-> "root,admin,mongo2tiger")
    if(da.count>0)this.synchronized {da.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Overwrite).options(opt2).save()}
  }
}
