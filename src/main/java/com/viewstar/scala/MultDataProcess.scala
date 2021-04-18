package com.viewstar.scala


import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
  * 用户属性+用户分群+用户标签，按userid和运营商id合并.
  * 以处理数据当天所有分群id和所有标签id为数据列
  */
object MultDataProcess extends Serializable{
  def process(processDate: String, sc: SparkContext, usermultdata: String, clusterdata: String, clusternum: Integer,
              layerdata: String, layernum: Integer, resultpath: String, actionpath: String): Unit = {
    val useractioncache = sc.textFile(actionpath + processDate).persist(StorageLevel.DISK_ONLY)
    val useractiontxt = useractioncache.map(m=>(m.split(",")(0)+"_"+m.split(",")(1), m.substring(m.indexOf(",") + 2).replaceAll(",","\001")))
    useractiontxt.take(3).foreach(x=>(println(x)))
    val usermultcache = sc.textFile(usermultdata + processDate).persist(StorageLevel.DISK_ONLY)
    val usermulttxt = usermultcache.map(m => (m.split("\001")(0)+"_"+m.split("\001")(9), m))
    val clustercache = sc.textFile(clusterdata + processDate).persist(StorageLevel.DISK_ONLY)
    // 格式(userid_typeid,\001clusterid1\001cluserid2)
    val clustertxt = clustercache.map(m => (m.split("\001")(0)+"_"+m.split("\001")(1), m.substring(m.indexOf("\001")+2)))
    clustertxt.take(3).foreach(x=>(println(x)))
    val layercache = sc.textFile(layerdata + processDate).persist(StorageLevel.DISK_ONLY)
    // 格式(userid_typeid,\001layerid1\001layerid2)
    val layertxt = layercache.map(m => (m.split("\001")(0)+"_"+m.split("\001")(1), m.substring(m.indexOf("\001")+2)))
    layertxt.take(3).foreach(x=>(println(x)))
    var clustertxtnum = "";
    var layertxtnum = "";
    var actiontxtnum = "";
    // 判断是否都有值，空值补0
    val cluster_empty = clustercache.isEmpty()
    if(!cluster_empty) {
      clustertxtnum = ("\001" + "0") * clusternum
    }
    val layer_empty = layercache.isEmpty()
    if(!layer_empty) {
      layertxtnum = ("\001" + "0") * layernum
    }
    actiontxtnum = ("\001" + "0") * 12
    // 没有分群信息
    if("".equals(clustertxtnum)) {
      if("".equals(layertxtnum)) { // 没有分群信息且没有标签信息
        usermulttxt.leftOuterJoin(useractiontxt).map(user => user._2._1.toString + user._2._2.getOrElse(actiontxtnum)).saveAsTextFile(resultpath + processDate+"/")
      } else { // 没有分群信息且有标签信息
        usermulttxt.leftOuterJoin(useractiontxt).leftOuterJoin(layertxt).map(user => user._2._1._1.toString
          + user._2._1._2.getOrElse(actiontxtnum) + user._2._2.getOrElse(layertxtnum)).saveAsTextFile(resultpath + processDate+"/")
      }
    } else {
      if("".equals(layertxtnum)) { // 有分群信息且没有标签信息
        usermulttxt.leftOuterJoin(useractiontxt).leftOuterJoin(clustertxt).map(user => user._2._1._1.toString
          + user._2._1._2.getOrElse(actiontxtnum) + user._2._2.getOrElse(clustertxtnum)).saveAsTextFile(resultpath + processDate+"/")
      } else { // 有分群信息且有标签信息
        usermulttxt.leftOuterJoin(useractiontxt).leftOuterJoin(clustertxt).leftOuterJoin(layertxt).map(user =>
          user._2._1._1._1.toString + user._2._1._1._2.getOrElse(actiontxtnum)
          + user._2._1._2.getOrElse(clustertxtnum)
          + user._2._2.getOrElse(layertxtnum)).saveAsTextFile(resultpath + processDate+"/")
      }
    }
    usermulttxt.unpersist()
    clustertxt.unpersist()
    layertxt.unpersist()
    useractiontxt.unpersist()
  }

}
