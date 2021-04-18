package com.viewstar.thread;

import com.bigdata.springcloud.openfeignclient.Hdfs53;
import com.bigdata.springcloud.openfeignclient.HiveSql53;
import com.viewstar.model.RunState;
import com.viewstar.scala.MultDataProcess;
import com.viewstar.service.DataManageService;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import javax.annotation.Resource;
import java.util.List;

/**
 * 处理用户画像表userprofilepar子线程
 */
public class UserProfileMangeTread extends Thread {
    private JdbcTemplate hivejdbc;

    private JdbcTemplate sparkjdbc;

    private JdbcTemplate oraclejdbc;

    private SparkContext sc;

    private HiveSql53 hiveSql53;

    private Hdfs53 hdfs53;

    public DataManageService dataManageServiceImpl;

    // 合并后文件路径1
    private String profileResultPath = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/profileresult/";
    // 合并后文件路径2
    private String profileResultPathBak = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/profileresultbak/";

    // 处理时间
    private String time;
    // 待处理对象
    private List<RunState> runStateList;

    /**
     * 构造函数
     * @param time 处理时间
     * @param runStateList 待处理对象
     */
    public UserProfileMangeTread(JdbcTemplate hivejdbc, JdbcTemplate sparkjdbc, JdbcTemplate oraclejdbc, SparkContext sc, HiveSql53 hiveSql53, Hdfs53 hdfs53, DataManageService dataManageServiceImpl, String time, List<RunState> runStateList) {
        this.hivejdbc = hivejdbc;
        this.sparkjdbc = sparkjdbc;
        this.oraclejdbc = oraclejdbc;
        this.sc = sc;
        this.hiveSql53 = hiveSql53;
        this.dataManageServiceImpl = dataManageServiceImpl;
        this.time = time;
        this.runStateList = runStateList;
        this.hdfs53 = hdfs53;
    }

    /**
     * 重处理数据.
     */
    @Override
    public void run() {
        // 1---查出现有表数据的hdfs存储地址，确定重处理所用的存储地址
        String newTablePath = ""; // 新表数据hdfs地址
        StringBuffer tableName = new StringBuffer("iptv.userprofilepar").append(time); // 正式使用的表名.
        System.out.println("开始处理表" + tableName);
        //try {
            String tablepath = dataManageServiceImpl.getDataPath(tableName.toString(), time);
            System.out.println(tablepath);
            // 正式地址和备用地址轮换使用存放数据文件.
            if ("".equals(tablepath) || profileResultPathBak.equals(tablepath))  {
                newTablePath = profileResultPath;
            }else if (profileResultPath.equals(tablepath)) {
                newTablePath = profileResultPathBak;
            }
            // 判断是否得到了新的表数据地址，如果没有则直接返回
            if("".equals(newTablePath)) {
                System.out.println("没有得到新的表数据地址");
                return;
            }
        /*}catch (Exception e) {
            e.printStackTrace();
        }*/
        // 2---重新处理数据
        System.out.println("重新处理地址：" + newTablePath);
        dataManageServiceImpl.dataManager(time, newTablePath);
        // 3---成功后删除runnstate表中记录
        String commonSql = "DELETE FROM PERSONAS_DATA_RUNSTATE WHERE ID=";
        runStateList.stream().forEach(runState -> {
            String deleteSql = commonSql + runState.getId();
            try {
                oraclejdbc.execute(deleteSql.toString());
            } catch (Exception e) {
                System.out.println("执行 '" + deleteSql.toString() + "' 失败");
                e.printStackTrace();
            }
        });
        System.out.println("处理" + time + "线程结束！");
    }


}
