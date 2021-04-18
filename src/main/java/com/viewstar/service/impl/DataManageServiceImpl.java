package com.viewstar.service.impl;

import com.bigdata.springcloud.openfeignclient.Hdfs53;
import com.bigdata.springcloud.openfeignclient.HiveSql53;
import com.sun.media.jfxmedia.logging.Logger;
import com.viewstar.scala.MultDataProcess;
import com.viewstar.service.DataManageService;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * 用户处理接口实现.
 * 实现hive中的MVCC隔离级别.
 * KLS 2020/01/07
 */
@Configuration
@Service
public class DataManageServiceImpl implements DataManageService{
    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hivejdbc;

    @Autowired
    @Qualifier("sparkJdbcTemplate")
    private JdbcTemplate sparkjdbc;

    @Autowired
    private SparkContext sc;

    @Autowired
    private HiveSql53 hiveSql53;

    @Autowired
    private Hdfs53 hdfs53;

    @Resource
    public DataManageService dataManageServiceImpl;
    /**
     * 用户信息表数据文件地址1
     */
    private String hdfsPath = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/profileresult/";
    /**
     * 用户信息表数据文件地址2
     */
    private String hdfsPathBak = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/profileresultbak/";
    // 属性数据文件路径
    private String multdataPath = "hdfs://10.0.9.53:8020/databases/iptv/usermultinfo/";
    // 标签层数据路径
    private String layerdataPath = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/personlayerdata/";
    // 分群数据路径
    private String clusterPath = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/personclusterdata/";
    // 用户行为路径
    private String actionPath = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/personactiondata/";
    // 属性表字段.
    private String multColumn = "userid string,userDate string,userArea string,userAreaId string,stbtype string," +
            "userPub string,userPubId string,userActive string,userService string,userServiceId string," +
            "userType string,userTypeId string,userPlat string,userPlatId string,userModel string," +
            "userModelId string,userVersion string,userVersionId string,userActiveDays string,activecount string,channelcount string," +
            "channeltime string,vodcount string,vodtime string,tvodcount string,tvodtime string,pagecount string," +
            "pagetime string,userordercount string,usercancelcount string,logincount string";
    /**
     * 获取hive表数据的hdfs地址.
     * @param tableName 表名
     * @param time 时间
     * @return hive表数据的hdfs地址
     */
    @Override
    public String getDataPath(String tableName, String time) {
        String path = "";
        String isexits = hdfs53.exists(hdfsPath + time);
        System.out.println(hdfsPath + time+"是否存在："+isexits);
        if("\"true\"".equals(isexits)) {
            // 查询表的sql
            StringBuffer sqlBuffer = new StringBuffer("show create table ");
            sqlBuffer.append(tableName);
            // 查询
            System.out.println("====="+sqlBuffer.toString());
            List<String> strList = hivejdbc.queryForList(sqlBuffer.toString(), String.class);
            // 截取LOCATION信息
            for (int i = 0; i < strList.size(); i++) {
                if ("LOCATION".equals(strList.get(i))) {
                    path = strList.get(i + 1).trim();
                    path = path.substring(1, path.lastIndexOf("/") + 1);
                    break;
                }
            }
        }
        System.out.println(tableName + "获取数据路径：" + path);
        return path;
    }

    /**
     * 重命名hive表
     * @param oldName 旧表名
     * @param newName 新表名
     */
    @Override
    public void renameTable(String oldName, String newName) {
        // sql
        StringBuffer sqlBuffer = new StringBuffer("");
        sqlBuffer.append("ALTER TABLE ").append(oldName).append(" RENAME TO ").append(newName);
        // 执行重命名
        excuteSql(sqlBuffer.toString());
    }

    /**
     * 删除hive表
     * @param tableName 表名
     */
    @Override
    public void deleteTable(String tableName) {
        // sql
        StringBuffer sqlBuffer = new StringBuffer("");
        sqlBuffer.append("DROP TABLE IF EXISTS ").append(tableName);
        // 执行重命名
        excuteSql(sqlBuffer.toString());
    }

    /**
     * 执行无返回值的sql语句.
     * @param sql sql语句.
     */
    public void excuteSql(String sql) {
        try {
            System.out.println("执行：" + sql);
            System.out.println(hiveSql53.execute(sql));
        } catch (Exception e) {
            System.out.println("执行：" + sql + "失败");
            e.printStackTrace();
        }
    }
    /**
     * 表的透视处理，纵表变横表.
     * @param tablename 表名.
     * @param columnname 列名.
     * @param ptime 处理时间.
     * @param hdfspath 结果存储路径.
     * @return 转置后的新表.
     */
    public List<String> datapivot(String tablename, String columnname, String ptime, String hdfspath) {
        // 得到新表的列名
        String columnSql = "select " + columnname + " from " + tablename + " where ptime=" + ptime
                + " group by " + columnname + " order by " + columnname;
        System.out.println("columnSql:" + columnSql);
        List<String> columnList = sparkjdbc.queryForList(columnSql, String.class);
        System.out.println("columnList:" + columnList);
        // 拼接透视处理语句
        StringBuffer pivotSql = new StringBuffer("");
        pivotSql.append("insert overwrite DIRECTORY '").append(hdfspath +"/" + ptime + "/")
                .append("' select userid,type,");
        columnList.stream().forEach(column-> {
            pivotSql.append("case when (instr(concat_ws(',',collect_list(").append(columnname).append(")), ")
                    .append(column).append(")=0) then 0 else 1 end as a").append(columnList.indexOf(column)).append(",");
        });
        pivotSql.deleteCharAt(pivotSql.length()-1);
        pivotSql.append(" from (select userid,type,").append(columnname).append(" from ").append(tablename)
                .append(" where ptime=").append(ptime).append(")a group by userid,type");
        System.out.println("pivotSql:" + pivotSql);
        hivejdbc.execute(pivotSql.toString());
        System.out.println(tablename+"透视转换完成!");
        return columnList;
    }

    /**
     * 处理用户画像基础数据.
     * @param time 日期.
     * @param resultDataPath 需要处理的表数据存取地址.
     */
    public void dataManager(String time, String resultDataPath) {
        if("".contains(time)) {
            System.out.println("未传入时间值");
        } else {
            System.out.println(hdfs53.delete(resultDataPath + time + "/"));
            System.out.println(hdfs53.delete(layerdataPath + time + "/"));
            System.out.println(hdfs53.delete(clusterPath + time + "/"));
            // 刷新表
            System.out.println("刷新表iptv.personlayerdatapar:"+sparkjdbc.queryForList("refresh Table iptv.personlayerdatapar"));
            System.out.println("刷新表iptv.personclusterdatapar:"+sparkjdbc.queryForList("refresh Table iptv.personclusterdatapar"));
            // 属性表、分群表、标签表转透视表
            List<String> layerColumn = datapivot("iptv.personlayerdatapar","playerid",time,"/databases/iptv/userprofile/personlayerdata");
            List<String> clusterColumn = datapivot("iptv.personclusterdatapar","pclusterid",time,"/databases/iptv/userprofile/personclusterdata");
            // 处理表数据到hdfs
            MultDataProcess.process(time,sc,multdataPath,clusterPath,clusterColumn.size(),
                    layerdataPath,layerColumn.size(),resultDataPath,actionPath);
            // 建表
            StringBuffer cols = new StringBuffer();
            cols.append(multColumn).append(",");
            clusterColumn.stream().forEach(cluster-> {
                cols.append("C").append(cluster).append(" string,");
            });
            layerColumn.stream().forEach(layer-> {
                cols.append("L").append(layer).append(" string,");
            });
            cols.deleteCharAt(cols.length()-1);
            // 转列式存储新备份表
            hiveSql53.createExternalTable("iptv.userprofile" + time + "bak", cols.toString(), null,"\001", resultDataPath+"/"+time+"/");
            hiveSql53.createColumnStoreExternalTable("iptv.userprofilepar" + time + "bak", cols.toString(),null,"\001", resultDataPath+"/"+time+"/");
            // 删除目标数据文件和数据表
            dataManageServiceImpl.deleteTable("iptv.userprofile" + time);
            dataManageServiceImpl.deleteTable("iptv.userprofilepar" + time);
            // 新备份表代替原表
            dataManageServiceImpl.renameTable("iptv.userprofile" + time + "bak", "iptv.userprofile" + time);
            dataManageServiceImpl.renameTable("iptv.userprofilepar" + time + "bak", "iptv.userprofilepar" + time);
            // 刷新表
            System.out.println("刷新表iptv.userprofile"+time+":"+sparkjdbc.queryForList("refresh Table iptv.userprofile" + time));
            System.out.println("刷新表iptv.userprofilepar"+time+":"+sparkjdbc.queryForList("refresh Table iptv.userprofilepar" + time));
            System.out.println(time + "用户画像基础数据处理完毕");
        }
    }
    public void test() {
        String time = "19000101";
        // 处理表数据到hdfs
        MultDataProcess.process(time,sc,multdataPath,clusterPath,8,
                layerdataPath,8,hdfsPath,actionPath);
    }
}
