package com.viewstar.controller;

import com.google.gson.Gson;
import com.viewstar.model.ExpressionCombine;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 用户画像管理.
 */
@RestController
public class ProfileManagementContoller {
    @Autowired
    @Qualifier("mysqlJdbcTemplate")
    private JdbcTemplate mysqljdbc;
    @Autowired
    @Qualifier("oracleJdbcTemplate")
    private JdbcTemplate oraclejdbc;
    Gson gson = new Gson();
    private ExpressionCombine expressionCombine = new ExpressionCombine();
    private SimpleDateFormat sfd = new SimpleDateFormat("yyyyMMdd");
    
    @RequestMapping(value = "/createProfileSQL")
    public String createProfile(@RequestParam("param") String param) {
        StringBuffer sql = new StringBuffer("");
        StringBuffer attrSql = new StringBuffer("");
        StringBuffer actionSql = new StringBuffer("");
        StringBuffer sequenceSql = new StringBuffer("");
        List<String> sqlList = new ArrayList<String>();
        String today = sfd.format(new Date());
        Map<String, String> paramMap = gson.fromJson(param, Map.class);
        Map<String, Boolean> serviceFilterMap = new HashMap<>();
        // 未传递参数，直接返回failure.
        if("".equals(param)) {
            return "failure";
        }
        // 计算用户表中的最近记录时间.
        String date = "#date#";
        // 开始处理参数.
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(param);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        // 通过属性创建用户画像列表
        if(paramMap.containsKey("attrs")) {
            System.out.println("通过属性创建用户画像列表---begin");
            try {
                attrSql.append("select userid, type from (select userid,userserviceid as type from iptv.userprofilepar").append(date).append(" where ");
                attrSql.append(expressionCombine.buildExpression("attr", jsonObject.getString("attrs"), date, serviceFilterMap,oraclejdbc));
                attrSql.append(") attr ");
                System.out.println(attrSql.toString());
                System.out.println("通过属性创建用户画像列表---end");
                sqlList.add(attrSql.toString());
            } catch (JSONException e) {
                System.out.println("通过属性创建用户画像列表--读取失败");
                e.printStackTrace();
            }
        }
        // 优化sql，特别过滤运营商条件
        boolean cucc = false;
        boolean cmcc = false;
        boolean ctcc = false;
        if(attrSql.toString().contains("userserviceid = '1'")) {
            cucc = true;
        }
        if(attrSql.toString().contains("userserviceid = '2'")) {
            cmcc = true;
        }
        if(attrSql.toString().contains("userserviceid = '3'")) {
            cmcc = true;
        }
        if(!attrSql.toString().contains("userserviceid = '1'")&&!attrSql.toString().contains("userserviceid = '2'")&&!attrSql.toString().contains("userserviceid = '3'")) {
            cucc = true;
            cmcc = true;
            ctcc = true;
        }
        serviceFilterMap.put("cucc", cucc);
        serviceFilterMap.put("ctcc", ctcc);
        serviceFilterMap.put("cmcc", cmcc);
        // 通过用户行为满足条件创建用户画像列表
        if(paramMap.containsKey("actions")) {
            System.out.println("通过用户行为满足条件创建用户画像列表---begin");
            try {
                actionSql.append("select userid, type from ");
                actionSql.append(expressionCombine.buildExpression("action", jsonObject.getString("actions"), date, serviceFilterMap, oraclejdbc));
                actionSql.append(" action");
                System.out.println(actionSql.toString());
                System.out.println("通过用户行为满足条件创建用户画像列表---end");
                sqlList.add(actionSql.toString());
            } catch (Exception e) {
                System.out.println("通过用户行为满足条件创建用户画像列表--读取失败");
                e.printStackTrace();
            }
        }
        if(paramMap.containsKey("sequences")) {
            System.out.println("通过用户行为序列创建用户画像列表---begin");
            try {
                sequenceSql.append("select userid,type from (");
                sequenceSql.append(expressionCombine.buildExpression("sequence", jsonObject.getString("sequences"), date, serviceFilterMap, oraclejdbc));
                sequenceSql.append(") sequence");
                System.out.println(sequenceSql.toString());
                System.out.println("通过用户行为序列创建用户画像列表---end");
                sqlList.add(sequenceSql.toString());
            } catch (Exception e) {
                System.out.println("通过用户行为序列创建用户画像列表--读取失败");
                e.printStackTrace();
            }
        }
        String switchStr = " " + paramMap.get("switch") + " ";
        sql.append(String.join(switchStr, sqlList));
        System.out.println(sql.toString());
        Map<String, String> resultmap = new HashMap<>();
        resultmap.put("result", sql.toString());
        return gson.toJson(resultmap);
    }

    public static void main(String[] args) {
        String param = "{\"switch\":\"intersect\",\"actions\":{\"switch\":\"intersect\",\"list\":[{\"switch\":\"intersect\",\"list\":[{\"id\":\"plSi9AZH4q\",\"index\":\"1\",\"customDate\":{\"type\":\"specific\",\"startTime\":\"2020-02-21 00:00:00\",\"endTime\":\"2020-02-23 23:59:59\",\"customStartTimeBtn\":\"time\",\"customEndTimeBtn\":\"today\",\"customStartInputValue\":1,\"customEndInputValue\":2},\"did\":\"yes\",\"oper\":\"channeluserparquet\",\"result\":\"count\",\"relation\":\">=\",\"value\":\"1\",\"key\":\"plSi9AZH4q\",\"filters\":{\"filtersswitch\":\"and\",\"list\":[{\"id\":\"B3xxBOMgBx\",\"attr\":\"channelid\",\"relation\":\"=\",\"value\":\"1,2\",\"type\":\"event\",\"valueType\":\"string\",\"index\":\"1\"}]}}]}]}}";
        // String param = "{\n" + "\t\"switch\":\"and\", \n" + "\t\"attrs\": {\n" + "\t\t\"switch\": \"and\",\n" + "\t\t\"list\": [{\n" + "\t\t\t\"switch\": \"\",\n" + "\t\t\t\"list\": [{\n" + "\t\t\t\t\"id\": \"111\",\n" + "\t\t\t\t\"index\": \"1\",\n" + "\t\t\t\t\"attr\": \"userareaid\",\n" + "                                \"type\":\"attr\",\n" + "\t\t\t\t\"relation\": \"=\",\n" + "\t\t\t\t\"value\": \"1,2,3,4\"\n" + "\t\t\t}]\n" + "\t\t}, {\n" + "\t\t\t\"switch\": \"or\",\n" + "\t\t\t\"list\": [{\n" + "\t\t\t\t\"id\": \"222\",\n" + "\t\t\t\t\"index\": \"1\",\n" + "\t\t\t\t\"type\": \"attr\",\n" + "\t\t\t\t\"attr\": \"userserviceid\",\n" + "\t\t\t\t\"relation\": \"=\",\n" + "\t\t\t\t\"value\": \"1\"\n" + "\t\t\t}, {\n" + "\t\t\t\t\"id\": \"3333\",\n" + "\t\t\t\t\"index\": \"2\",\n" + "\t\t\t\t\"type\": \"attr\",\n" + "\t\t\t\t\"attr\": \"userserviceid\",\n" + "\t\t\t\t\"relation\": \"=\",\n" + "\t\t\t\t\"value\": \"2\"\n" + "\t\t\t}]\n" + "\t\t}]\n" + "\t}\n" + "}";
        Gson gson = new Gson();
        ProfileManagementContoller profileManagementContoller = new ProfileManagementContoller();
        profileManagementContoller.createProfile(param);
    }
}
