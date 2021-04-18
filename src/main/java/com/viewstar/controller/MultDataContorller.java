package com.viewstar.controller;

import com.bigdata.springcloud.openfeignclient.Hdfs53;
import com.bigdata.springcloud.openfeignclient.HiveSql53;
import com.google.gson.Gson;
import com.viewstar.model.RunState;
import com.viewstar.scala.MultDataProcess;
import com.viewstar.service.DataManageService;
import com.viewstar.thread.UserProfileMangeTread;
import org.apache.commons.collections.list.SynchronizedList;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.xml.bind.SchemaOutputResolver;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 用户画像基础表处理.
 */
@RestController
public class MultDataContorller {

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hivejdbc;

    @Autowired
    @Qualifier("sparkJdbcTemplate")
    private JdbcTemplate sparkjdbc;

    @Autowired
    @Qualifier("oracleJdbcTemplate")
    private JdbcTemplate oraclejdbc;

    @Autowired
    private SparkContext sc;

    @Autowired
    private HiveSql53 hiveSql53;

    @Autowired
    private Hdfs53 hdfs53;

    @Resource
    public DataManageService dataManageServiceImpl;

    private Gson gson = new Gson();
    // 合并后文件路径1
    private String profileResultPath = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/profileresult/";
    // 合并后文件路径2
    private String profileResultPathBak = "hdfs://10.0.9.53:8020/databases/iptv/userprofile/profileresultbak/";
    /**
     * test.
     */
    @RequestMapping(value = "/test")
    public String test() {
        return "userprofile is OK!";
    }

    /**
     * 定时重处理表.
     */
    
    @RequestMapping(value = "/userProfileDataMult")
    public void userProfileDataMult() {
        // 0---查表，读取待处理时间数据
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        List<RunState> responseList = new ArrayList<RunState>();
        String sql = "SELECT RUN_ID, TYPE, RUN_STATE, UPDATE_STATE, TO_CHAR(TIME, 'YYYY-MM-DD') AS TIME, ID FROM " +
                "PERSONAS_DATA_RUNSTATE WHERE RUN_STATE = 1 and time not in " +
                "(select time from PERSONAS_DATA_RUNSTATE where run_state = 0 or UPDATE_STATE = 0 group by time)";
        System.out.println(sql);
        try {
            resultList = oraclejdbc.queryForList(sql);
            resultList.stream().forEach(map-> {
                RunState runState = new RunState(map);
                responseList.add(runState);
            });
        } catch (Exception e) {
            System.out.println("查询 '"+sql+"' 失败");
            e.printStackTrace();
        }
        // 1---锁表记录，更新表中字段，标记此时正在跑数据,并按时间分组
        Map<String, List<RunState>> timeMap = new HashMap<String, List<RunState>>();
        if(responseList.size()>0) {
            responseList.stream().forEach(re-> {
                StringBuffer updateSql = new StringBuffer("UPDATE PERSONAS_DATA_RUNSTATE SET UPDATE_STATE = 0 "
                        + "WHERE ID=").append(re.getId());
                try {
                    // 锁记录
                    oraclejdbc.execute(updateSql.toString());
                } catch (Exception e) {
                    System.out.println("执行 '" + updateSql.toString() + "' 失败");
                    e.printStackTrace();
                }
                // 按时间分组
                List<RunState> timeList = new ArrayList<RunState>();
                if(timeMap.containsKey(re.getTime())) {
                    timeList = timeMap.get(re.getTime());
                }
                timeList.add(re);
                timeMap.put(re.getTime(), timeList);
            });
        }
        // 2---根据合并的时间，多线程跑数据
        timeMap.keySet().stream().forEach(time-> {
            System.out.println("开始处理userprofilepar"+time.replaceAll("-","")+"表数据");
            System.out.println(gson.toJson(timeMap.get(time)));
            UserProfileMangeTread userProfileMangeTread = new UserProfileMangeTread(hivejdbc, sparkjdbc, oraclejdbc, sc, hiveSql53, hdfs53, dataManageServiceImpl, time.replaceAll("-",""), timeMap.get(time));
            userProfileMangeTread.start();
        });
    }

    /**
     * 处理用户画像基础数据.
     * @param time 日期.
     */
    
    @RequestMapping(value = "/userProfileData")
    public void userProfileData(@RequestParam("time") String time) {
        if ("".contains(time)) {
            System.out.println("未传入时间值");
        } else {
            // 1---查出现有表数据的hdfs存储地址，确定重处理所用的存储地址
            String newTablePath = ""; // 新表数据hdfs地址
            StringBuffer tableName = new StringBuffer("iptv.userprofilepar").append(time); // 正式使用的表名.
            System.out.println("开始处理表" + tableName);
            try {
                String tablepath = dataManageServiceImpl.getDataPath(tableName.toString(), time);
                System.out.println(tablepath);
                // 正式地址和备用地址轮换使用存放数据文件.
                if ("".equals(tablepath) || profileResultPathBak.equals(tablepath))  {
                    newTablePath = profileResultPath;
                }else if (profileResultPath.equals(tablepath)) {
                    newTablePath = profileResultPathBak;
                }
                // 判断是否得到了新的表数据地址，如果没有则直接返回
                if ("".equals(newTablePath)) {
                    System.out.println("没有得到新的表数据地址");
                    return;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            dataManageServiceImpl.dataManager(time, newTablePath);
        }
    }
    @RequestMapping(value = "/usertest")
    public void usertest() {
        dataManageServiceImpl.test();
    }
}
