package com.viewstar.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.mysql.cj.xdevapi.JsonString;
import com.viewstar.model.Customdate;
import com.viewstar.util.DateUtil;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.eclipse.jetty.util.ajax.JSON;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.messaging.Message;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import scala.tools.cmd.gen.AnyVals;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 用户画像相关业务
 */
@RestController
public class PersonProfileController {
    @Autowired
    @Qualifier("oracleJdbcTemplate")
    private JdbcTemplate oraclejdbc;

    @Autowired
    @Qualifier("sparkJdbcTemplate")
    private JdbcTemplate sparkjdbc;

    @Autowired
    @Qualifier("mysqlJdbcTemplate")
    private JdbcTemplate mysqljdbc;

    private Gson gson = new Gson();

    private DateUtil dateUtil = new DateUtil();

    /**
     * 获取所有标签列表.
     * @return json
     */
   
    @RequestMapping(value = "/getProfileLable")
    public String getProfileLable(@RequestParam("userid") String userid) {
        List<Map<String, Object>> profileList = new ArrayList<Map<String, Object>>();
        // 属性信息
        List<Map<String, Object>> attrList = new ArrayList<Map<String, Object>>();
        attrList = oraclejdbc.queryForList("SELECT * FROM PERSONAS_ATTR");
        // 初始化属性map
        Map<String,Map<String, String>> attrSubMap = new HashMap<String, Map<String, String>>();
        attrSubMap = getInitLabelMap();
        // 标签信息
        List<Map<String, Object>> labelList = new ArrayList<Map<String, Object>>();
        if("superRole".equals(userid)) {
            labelList = oraclejdbc.queryForList("SELECT A.CATALOG_ID,CATALOG_NAME,TAG_ID,TAG_NAME " + "FROM PERSONAS_TAG A, PERSONAS_CATALOG B WHERE A.CATALOG_ID = B.CATALOG_ID ORDER BY A.CATALOG_ID,TAG_ID");
        } else {
            labelList = oraclejdbc.queryForList("SELECT A.CATALOG_ID,CATALOG_NAME,TAG_ID,TAG_NAME " + "FROM PERSONAS_TAG A, PERSONAS_CATALOG B WHERE A.CATALOG_ID = B.CATALOG_ID " + "AND (A.USER_ID='" + userid + "' OR A.USER_ID='自定义')  AND (B.USER_ID='" + userid + "' OR B.USER_ID='自定义') ORDER BY A.CATALOG_ID,TAG_ID");
        }
        // 分群信息
        List<Map<String, Object>> clusterList = new ArrayList<Map<String, Object>>();
        if("superRole".equals(userid)) {
            clusterList = oraclejdbc.queryForList("SELECT CLUSTER_ID,CLUSTER_NAME FROM PERSONAS_CLUSTER ORDER BY CLUSTER_ID");
        } else {
            clusterList = oraclejdbc.queryForList("SELECT CLUSTER_ID,CLUSTER_NAME FROM PERSONAS_CLUSTER " + "WHERE CLUSTER_CREATEAUTHOR='" + userid + "' OR  CLUSTER_CREATEAUTHOR='自定义' ORDER BY CLUSTER_ID");
        }
        // 组合json
        // 属性Map
        Map<String, Object> attrMap = new HashMap<String, Object>();
        attrMap.put("name", "用户属性(" + attrList.size() + ")");
        List<Map<String, Object>> attrChildren = new ArrayList<Map<String, Object>>();
        Map<String, Map<String, String>> finalAttrSubMap = attrSubMap;
        attrList.stream().forEach(attr-> {
            Map<String, Object> tempmap = new HashMap<String, Object>();
            tempmap.put("label", attr.get("ATTR_NAME").toString());
            tempmap.put("value", attr.get("ATTR_ID").toString());
            tempmap.put("type", "attr");
            List<Map<String, String>> childList = new ArrayList<Map<String, String>>();
            if(finalAttrSubMap.containsKey(tempmap.get("value"))) {
                childList = finalAttrSubMap.get(tempmap.get("value")).entrySet().stream().map(a-> {
                    Map<String, String> attrTemp = new HashMap<>();
                    attrTemp.put("value", a.getKey());
                    attrTemp.put("label", a.getValue());
                    return attrTemp;
                }).collect(Collectors.toList());
            }
            tempmap.put("children", childList.size()<=20?childList:childList.subList(0,20));
            attrChildren.add(tempmap);
        });
        attrMap.put("children", attrChildren);
        profileList.add(attrMap);
        // 标签Map
        Map<String, Object> labelMap = new HashMap<String, Object>();
        labelMap.put("name", "标签(" + labelList.size() + ")");
        List<Map<String, Object>> labelChildren = new ArrayList<Map<String, Object>>();
        Map<String, List<Map<String,Object>>> catalogMap = new HashMap<String, List<Map<String,Object>>>();
        labelList.stream().forEach(label-> {
            List<Map<String,Object>> tempList = new ArrayList<Map<String,Object>>();
            if(catalogMap.containsKey(label.get("CATALOG_ID")+","+label.get("CATALOG_NAME"))) {
                tempList = catalogMap.get(label.get("CATALOG_ID")+","+label.get("CATALOG_NAME"));
            }
            Map<String, Object> tempMap = new HashMap<String, Object>();
            String tag_id = label.get("TAG_ID").toString();
            tempMap.put("label", label.get("TAG_NAME").toString());
            tempMap.put("value", tag_id);
            tempMap.put("type", "tag");
            List<Map<String, String>> childList = new ArrayList<Map<String, String>>();
            childList = getLayerIdByTagID(tag_id).stream().map(a-> {
                Map<String, String> map = new HashMap<>();
                map.put("value", a.get("id").toString());
                map.put("label", a.get("name").toString());
                return map;
            }).collect(Collectors.toList());
            tempMap.put("children", childList.size()<=20?childList:childList.subList(0,20));
            tempList.add(tempMap);
            catalogMap.put(label.get("CATALOG_ID")+","+label.get("CATALOG_NAME"), tempList);
        });
        catalogMap.keySet().stream().forEach(catalog-> {
            Map<String, Object> tempmap = new HashMap<String, Object>();
            tempmap.put("label", catalog.split(",")[1]);
            tempmap.put("value", catalog.split(",")[0]);
            tempmap.put("children", catalogMap.get(catalog));
            labelChildren.add(tempmap);
        });
        labelMap.put("children", labelChildren);
        profileList.add(labelMap);
        // 分群Map
        Map<String, Object> clusterMap = new HashMap<String, Object>();
        clusterMap.put("name", "用户分群(" + clusterList.size() + ")");
        List<Map<String, String>> clusterChildren = new ArrayList<Map<String, String>>();
        clusterList.stream().forEach(cluster-> {
            Map<String, String> tempmap = new HashMap<String, String>();
            tempmap.put("label", cluster.get("CLUSTER_NAME").toString());
            tempmap.put("value", "c"+cluster.get("CLUSTER_ID").toString());
            tempmap.put("type", "cluster");
            clusterChildren.add(tempmap);
        });
        clusterMap.put("children", clusterChildren);
        profileList.add(clusterMap);
        return gson.toJson(profileList);
    }

    /**
     * 获取用户行为下拉列表.
     * @return json
     */
    
    @RequestMapping(value = "/getActionLabel")
    public String getActionLabel() {
        List<Map<String, Object>> actionList = new ArrayList<Map<String, Object>>();
        actionList = oraclejdbc.queryForList("SELECT ACTION_ID AS VALUE,ACTION_NAME AS LABEL FROM PERSONAS_ACTION");
        return gson.toJson(actionList);
    }

    /**
     * 获取用户画像结果.
     * @param param json参数串. {"type":"创建类型",
     *              "date":"数据查询时间","tagId":"标签id","layerId":"标签层id",
     *              "requst-条件":{"by":"userareaid","type":"attr"}}
     * @return 根据不同查询类型的结果.
     *
     */
    
    @RequestMapping(value = "/getUserGroupResult")
    public String getUserGroupResult(@RequestParam("param") String param) {
        String resultJson = "";
        //Map<String, Object> paramMap = gson.fromJson(param, Map.class);
        JSONObject paramMap = new JSONObject();
        try {
            paramMap = new JSONObject(param);
            System.out.println(paramMap);
            String type = paramMap.get("type").toString();
            String date = paramMap.get("date").toString().replaceAll("-","").substring(0,8);
            String tagId = paramMap.get("tagId").toString();
            String layerId = paramMap.get("layerId").toString();
            String creatType = paramMap.get("creatType").toString();
            String clusterId = paramMap.get("clusterId").toString();
            StringBuffer profile_sql = new StringBuffer("");
            JSONObject requst = paramMap.getJSONObject("requst");
            int create_type = 0; //创建方式：0-条件创建;1-结果创建.
            // 刷新表
            sparkjdbc.queryForList("refresh Table iptv.personportraitdatapar");
            sparkjdbc.queryForList("refresh Table iptv.userprofilepar"+date);
            Map<String,Map<String, String>> attrMap = new HashMap<String, Map<String, String>>();
            attrMap = getInitLabelMap();
            // 获取总人数
            StringBuffer sparkSql_allUserCount = new StringBuffer();
            sparkSql_allUserCount.append("select count(*) as allusercount from ");
            // 通过条件创建用户画像
            if(paramMap.has("personasId")) {
                create_type = 0;
                profile_sql.append("(select * from (select * from")
                        .append("(select * from iptv.userprofilepar").append(date).append(") A ")
                        .append(" left join (select userid as puserid,type as ptype,portraitid ")
                        .append("from iptv.personportraitdatapar where time=")
                        .append(date).append(" and portraitid=").append(paramMap.get("personasId")).append(") B")
                        .append(" on A.userid=B.puserid and A.userserviceid=B.ptype) C where portraitid is not null) S");
                sparkSql_allUserCount.append(profile_sql);
            } else { // 通过标签结果创建用户画像.
                create_type = 1;
                profile_sql.append("iptv.userprofilepar").append(date);
                if("1".equals(creatType)) {
                    if ("all".equals(layerId)) {
                        List<Map<String, Object>> tagList = getLayerIdByTagID(tagId);
                        List<String> conditionList = tagList.stream().map(a -> a.get("id").toString()).collect(Collectors.toList());
                        sparkSql_allUserCount.append(profile_sql).append(" where (").append(String.join("=1 or ", conditionList)).append("=1)");
                    } else {
                        sparkSql_allUserCount.append(profile_sql).append(" where l").append(layerId).append("=1");
                    }
                } else if("2".equals(creatType)) {
                    sparkSql_allUserCount.append(profile_sql).append(" where c").append(clusterId).append("=1");
                }
            }
            Integer allusercount = new Integer(0);
            System.out.println(sparkSql_allUserCount);
            allusercount = Integer.valueOf(sparkjdbc.queryForMap(sparkSql_allUserCount.toString()).get("ALLUSERCOUNT").toString());
            System.out.println("总用户数：" + allusercount);
            if("1".equals(type)) { // 获取标签属性分布结果
                resultJson = getLabelUserGroup(requst, tagId, layerId, create_type, attrMap,
                        allusercount, profile_sql.toString(), creatType, clusterId);
            } else if("2".equals(type)) { // 获取用户行为指标结果
                System.out.println(requst.get("customDate").toString());
                resultJson = getActionUserGroup(requst, tagId, layerId, create_type, attrMap,
                        allusercount, profile_sql.toString(), creatType, clusterId);
            } else if("3".equals(type)) { // 获取用户特征分布结果
                resultJson = getAttrUserGroup(requst, tagId, layerId, create_type, attrMap,
                        allusercount, profile_sql.toString(), creatType, clusterId);
            }
        } catch (Exception e) {
            System.out.println("返回resultJson失败");
        }
        System.out.println("resultJson="+resultJson);
        return resultJson;
    }

    /**
     * 获取最后计算状态
     * @param param {"date":"2019-12-14 00:00:00","tagId":"标签id","layerId":"标签层id",
     *              "type":1-标签结果;2-分群结果,"clusterId":分群id}
     * @return {"total":"总用户数","count":"目标用户数","time":"2019-12-24 17:23:45"}
     */
    
    @RequestMapping(value = "/getPrimaryInfo")
    public String getPrimaryInfo(@RequestParam("param") String param) throws Exception {
        Map<String, Object> paramMap = gson.fromJson(param, Map.class);
        System.out.println(paramMap);
        // 结果map.
        Map<String, String> resultMap = new HashMap<String, String>();
        // 结果时间是数据库中最新数据的时间.
        resultMap.put("time", paramMap.get("date").toString());
        String date = paramMap.get("date").toString().replaceAll("-","").substring(0,8);
        String type = paramMap.get("type").toString();
        Integer totalCount = new Integer(0);
        String totalSql = "select count(*)  as total from iptv.userprofilepar"+date;
        try {
            totalCount = Integer.valueOf(sparkjdbc.queryForMap(totalSql).get("total").toString());
        } catch (Exception e) {
            System.out.println(totalSql+"查询失败...");
        }
        // 获取总人数
        StringBuffer sparkSql_allUserCount = new StringBuffer();
        sparkSql_allUserCount.append("select count(userid) as allusercount from ");
        List<String> layerList = new ArrayList<String>();
        // 画像总信息.
        if(paramMap.containsKey("personasId")) {
            sparkSql_allUserCount.append("(select * from (select * from")
                    .append("(select * from iptv.userprofilepar").append(date).append(") A ")
                    .append(" left join (select userid as puserid,type as ptype,portraitid ")
                    .append("from iptv.personportraitdatapar where time=")
                    .append(date).append(" and portraitid=").append(paramMap.get("personasId")).append(") B")
                    .append(" on A.userid=B.puserid and A.userserviceid=B.ptype) C where portraitid is not null) S");
        } else {// 标签总信息.
            sparkSql_allUserCount.append(" iptv.userprofilepar").append(date);
            if("1".equals(type)) {
                String tagId = paramMap.get("tagId").toString();
                String layerId = paramMap.get("layerId").toString();
                if("all".equals(layerId)) {
                    List<Map<String, Object>> tagList = getLayerIdByTagID(tagId);
                    List<String> conditionList = tagList.stream().map(a->a.get("id").toString()).collect(Collectors.toList());
                    sparkSql_allUserCount.append(" where (")
                            .append(String.join("=1 or ", conditionList))
                            .append("=1)");
                } else {
                    sparkSql_allUserCount.append(" where l").append(layerId).append("=1");
                }
            } else if("2".equals(type)) {
                String clusterId = paramMap.get("clusterId").toString();
                sparkSql_allUserCount.append(" where c").append(clusterId).append("=1");
            }
        }
        Integer allusercount = new Integer(0);
        try {
            System.out.println(sparkSql_allUserCount);
            allusercount = Integer.valueOf(sparkjdbc.queryForMap(sparkSql_allUserCount.toString()).get("allusercount").toString());
        } catch (Exception e) {
            System.out.println(sparkSql_allUserCount+"查询失败...");
        }
        // 返回json
        resultMap.put("count", String.valueOf(allusercount));
        resultMap.put("total", String.valueOf(totalCount));
        return gson.toJson(resultMap);
    }
    /**
     * 获取标签属性分布结果.
     * @param requst 参数.{"type":"查询类型","date":"2019-12-08 00:00:00","tagId":"标签id","layerId":"标签层id",
     *               "create_type":1-标签结果；2-分群结果,"clusterId":"分群id",
     *               "requst条件":{"by":"userareaid","type":"attr"}}
     * @return json.(name,detail,percent)
     */
    public String getLabelUserGroup(JSONObject requst, String tagId,
                                    String layerId, int create_type, Map<String,Map<String, String>> attrMap,
                                    Integer allusercount, String profile_sql, String createType, String clusterId) {
        Map<String, Object> result = new HashMap<String, Object>();
        try {
           // 获取参数
           String by = requst.get("by").toString();
           String type = requst.get("type").toString();
           Map<String, String> name_id_map = new HashMap<String, String>();
           name_id_map = getNameById(type, attrMap, by);
           // 按标签分类
           String sparkSql_groupUserCount = getGroupUserCount(type, by, create_type, layerId, tagId, allusercount,
                   profile_sql, createType, clusterId, attrMap);
           List<Map<String, Object>> userCountList = new ArrayList<Map<String, Object>>();
           try {
               System.out.println(sparkSql_groupUserCount);
               userCountList = sparkjdbc.queryForList(sparkSql_groupUserCount);
           } catch (Exception e) {
               System.out.println(sparkSql_groupUserCount + "查询失败...");
           }
           System.out.println("userCountList:" + userCountList);
           // 组装结果数据
           BigDecimal allPercent = new BigDecimal("0");
           List<Map<String, Object>> detailList = new ArrayList<Map<String, Object>>();
           BigDecimal percent = new BigDecimal(100);
           // 用户活跃属性除外
           if (!"useractivedays".equals(by)) {
               for (int i = 0; i < userCountList.size(); i++) {
                   Map<String, Object> user = userCountList.get(i);
                   if (name_id_map.containsKey(user.get("groupid"))) {
                       Map<String, Object> userMap = new HashMap<String, Object>();
                       userMap.put("item", name_id_map.get(user.get("groupid")));
                       userMap.put("count", user.get("usercount"));
                       userMap.put("ratio", user.get("userpercent"));
                       detailList.add(userMap);
                       allPercent = allPercent.add(new BigDecimal(user.get("userpercent").toString()));
                   }
               }
               // 组装结果数据--计算没有该标签的用户的百分比
               if (allPercent.compareTo(new BigDecimal(1)) == -1) {
                   Map<String, Object> userMap = new HashMap<String, Object>();
                   userMap.put("item", "未知");
                   userMap.put("ratio", new BigDecimal(1).subtract(allPercent));
                   userMap.put("count", new BigDecimal(allusercount)
                           .multiply(new BigDecimal(1).subtract(allPercent)).toBigInteger());
                   percent = allPercent.multiply(new BigDecimal(100)).setScale(2, BigDecimal.ROUND_HALF_UP);
                   detailList.add(userMap);
               }
               // 组装结果数据--组装最终json数据.
               if ("cluster".equals(type)) {
                   result.put("name", attrMap.get("labelid").get(by.substring(1).toLowerCase()));
               } else {
                   result.put("name", attrMap.get("labelid").get(by.toLowerCase()));
               }
           }
           // 用户活跃属性组装结果
           else {
               int maxUserCount = 0;
               for (int i = 0; i < userCountList.size(); i++) {
                   Map<String, Object> user = userCountList.get(i);
                   Map<String, Object> userMap = new HashMap<String, Object>();
                   userMap.put("item", user.get("groupid"));
                   userMap.put("count", user.get("usercount"));
                   userMap.put("ratio", user.get("userpercent"));
                   if (maxUserCount < Integer.valueOf(userMap.get("count").toString())) {
                       maxUserCount = Integer.valueOf(userMap.get("count").toString());
                   }
                   detailList.add(userMap);
               }
               percent = new BigDecimal((float) maxUserCount / allusercount * 100)
                       .setScale(2, BigDecimal.ROUND_HALF_UP);
               result.put("name", "活跃天数");
           }
           result.put("percent", percent.toString().concat("%"));
           result.put("detail", detailList);
       }catch (Exception e) {
           e.printStackTrace();
       }
        return gson.toJson(result);
    }

    public static void main(String[] args) {
        PersonProfileController personProfileController = new PersonProfileController();
        String param = "{\"type\":\"2\",\"date\":\"2020-02-19 00:00\",\"tagId\":\"\",\"layerId\":\"\",\"requst\":{\"customDate\":{\"type\":\"lastWeek\",\"startTime\":\"2020-02-17 08:00:00\",\"endTime\":\"2020-02-23 08:59:59\",\"customStartTimeBtn\":\"time\",\"customEndTimeBtn\":\"today\",\"customStartInputValue\":1,\"customEndInputValue\":2},\"oper\":\"channeluserparquet\",\"result\":\"count\",\"range\":[{\"name\":\"0~0\",\"min\":\"0\",\"max\":\"0\"},{\"name\":\"1~10\",\"min\":\"1\",\"max\":\"10\"},{\"name\":\"11~100\",\"min\":\"11\",\"max\":\"100\"},{\"name\":\"101~500\",\"min\":\"101\",\"max\":\"500\"},{\"name\":\">500\",\"min\":\"501\",\"max\":\"max\"}]},\"creatType\":\"0\",\"clusterId\":\"\",\"personasId\":\"46668\"}";
        Gson gson = new Gson();
        Map<String, Object> paramMap = gson.fromJson(param, Map.class);
        System.out.println(paramMap);
        String type = (String) paramMap.get("type");
        String date = paramMap.get("date").toString().replaceAll("-","").substring(0,8);
        String tagId = paramMap.get("tagId").toString();
        String layerId = paramMap.get("layerId").toString();
        String creatType = paramMap.get("creatType").toString();
        String clusterId = paramMap.get("clusterId").toString();
        StringBuffer profile_sql = new StringBuffer("");
        Map<String, Object> requst = (Map<String, Object>) paramMap.get("requst");
        try {
            JSONObject jsonObject = new JSONObject(param);
            JSONObject requstjson = jsonObject.getJSONObject("requst");
            JSONObject custom = jsonObject.getJSONObject("requst").getJSONObject("customDate");
            System.out.println(custom.get("startTime"));
            //List<Map<String, String>> jsonArray = (List<Map<String, String>>) requst.get("range");
            JSONArray jsonArray = requstjson.getJSONArray("range");
            List<Map<String, String>> list = personProfileController.jsonArrayToList(jsonArray);
            System.out.println(jsonArray);

        } catch (Exception e) {
            e.printStackTrace();
        }

        /*PersonProfileController personProfileController = new PersonProfileController();
        System.out.println(personProfileController.getUserGroupResult(param));*/
    }

    /**
     * 获取用户行为指标结果.
     * @param requst 参数.
     * @return json.
     */
    public String getActionUserGroup(JSONObject requst, String tagId,
                                     String layerId, int create_type, Map<String, Map<String, String>> attrMap,
                                     Integer allusercount, String profile_sql, String createType, String clusterId) {
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 获取参数
        Map<String, Object> customMap = new HashMap<String, Object>();
        List<Map<String, Object>> detail = new ArrayList<>();
        try {
            // 获取当天日期
            Date date = new Date();
            String today = "";
            today = dateUtil.formatDate(date, "yyyy-MM-dd");
            JSONObject jsonObject = requst.getJSONObject("customDate");
            Customdate customdate = new Customdate(jsonObject);
            customMap = customdate.getTimeSpanMap();
            String startTime = customMap.get("starttime").toString();
            String endTime = customMap.get("endtime").toString();
            String startday = customMap.get("startday").toString();
            String endday = customMap.get("endday").toString();
            String length = "unix_timestamp("+ endTime+",'yyyyMMddHHmmss')-unix_timestamp("
                    +startTime+",'yyyyMMddHHmmss')+1";
            length = length.replaceAll("#today#", today);
            System.out.println("customMap:"+customMap);
            String tablename = requst.get("oper").toString();
            String searchtype = requst.get("result").toString();
            JSONArray rangeJsonAarray = requst.getJSONArray("range");
            List<Map<String, String>> range = jsonArrayToList(rangeJsonAarray);
            if(allusercount == 0) {
                resultMap.put("name", jsonObject.get("startTime").toString()+"-"+jsonObject.get("endTime"));
                resultMap.put("total", 0);
                resultMap.put("aver",0);
                resultMap.put("detail", detail);
            } else {
                // 计算查询结果
                // 待计算用户群
                StringBuffer alluserBuffer = new StringBuffer("select userid, userserviceid from ").append(profile_sql);
                if (create_type == 1) {
                    if ("1".equals(createType)) {
                        if ("all".equals(layerId)) {
                            List<Map<String, Object>> tagList = getLayerIdByTagID(tagId);
                            List<String> conditionList = tagList.stream().map(a -> a.get("id").toString()).collect(Collectors.toList());
                            alluserBuffer.append(" where (").append(String.join("=1 or ", conditionList)).append("=1)");
                        } else {
                            alluserBuffer.append(" where l").append(layerId).append("=1");
                        }
                    } else if ("2".equals(createType)) {
                        alluserBuffer.append(" where c").append(clusterId).append("=1");
                    }
                }
                // 计算用户分群结果
                // 加入跨天逻辑，包括直播、点播、回看、页面次数统计数据，限制flag=1
                String spanDay = "";
                if ("channeluserparquet".equals(tablename) || "voduserparquet".equals(tablename) || "tvoduserparquet".equals(tablename) || "pagedataparquet".equals(tablename)) {
                    spanDay = " and flag = 1";
                }
                StringBuffer sqlStr = new StringBuffer("select groupcount,count(userid) as usercount," +
                        "round(count(userid)/").append(allusercount).append(",4) as userpercent")
                        .append(" from ( select userid, case ");
                StringBuffer condition = new StringBuffer("");
                StringBuffer whereStr = new StringBuffer("");
                if ("times".equals(searchtype)) { // 总时长
                    condition.append(" round(sum(seconds)/3600,4) ");
                } else { // 次数，订购次数，退订次数
                    condition.append(" count(starttime) ");
                }
                List<Map<String, String>> rangeList = chageRange(range, length, searchtype);
                whereStr.append(" where ptime>=").append(startday)
                        .append(" and ptime<=").append(endday).append(spanDay).append(" and ((starttime >= cast(")
                        .append(startTime).append(" as bigint) and starttime <= cast(")
                        .append(endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(startTime).append(" as bigint) and endtime <= cast(")
                        .append(endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(startTime).append(" as bigint) and endtime > cast(")
                        .append(endTime).append(" as bigint)))");
                if ("ordercount".equals(searchtype)) {
                    whereStr.append(" and chargetype=16 ");
                } else if ("cancelcount".equals(searchtype)) {
                    whereStr.append(" and chargetype=31 ");
                }
                rangeList.stream().forEach(rangeone -> {
                    sqlStr.append(" when count").append(rangeone.get("condition"))
                            .append(" then '").append(rangeone.get("name")).append("'");
                });
                // 计算用户分群结果
                StringBuffer customconditon = new StringBuffer("");
                customconditon.append("select userid,starttime,").append("(case when starttime <= searchstarttime and " +
                        "endtime >= searchendtime then searchlength when starttime >= searchstarttime and " +
                        "endtime <= searchendtime then seconds when starttime <= searchstarttime and " +
                        "endtime <= searchendtime and endtime >= searchstarttime then " +
                        "unix_timestamp(cast(endtime as string),'yyyyMMddHHmmss') - " +
                        "unix_timestamp(cast(searchstarttime as string),'yyyyMMddHHmmss') + 1 " +
                        "when starttime <= searchendtime and starttime >= searchstarttime " +
                        "and endtime >= searchendtime " +
                        "then unix_timestamp(cast(searchendtime as string),'yyyyMMddHHmmss') - " +
                        "unix_timestamp(cast(starttime as string),'yyyyMMddHHmmss') + 1 else 0 end) as seconds")
                        .append(" from(")
                        .append("select *, cast(").append(startTime).append(" as bigint) as searchstarttime,cast(")
                        .append(endTime).append(" as bigint) as searchendtime,").append(length).append(" as searchlength");
                if ("times".equals(searchtype)) { // 总时长
                    sqlStr.append(" end as groupcount from ( select a.userid,NVL(b.count,0) as count from (")
                            .append(alluserBuffer).append(")a left join ( select userid,'1' as userserviceid,").append(condition)
                            .append(" as count from (").append(customconditon).append(" from iptv.").append(tablename)
                            .append(whereStr).append(")s)t group by userid").append(" union all ")
                            .append("select userid,'2' as userserviceid,").append(condition)
                            .append(" as count from (").append(customconditon).append(" from cmcc.").append(tablename)
                            .append(whereStr).append(")s)t group by userid").append(" union all ")
                            .append("select userid,'3' as userserviceid,").append(condition)
                            .append(" as count from (").append(customconditon).append(" from ctcc.").append(tablename)
                            .append(whereStr).append(")s)t group by userid")
                            .append(")b on a.userid = b.userid and a.userserviceid = b.userserviceid)k)c group by groupcount");
                } else {
                    sqlStr.append(" end as groupcount from ( select a.userid,NVL(b.count,0) as count from (")
                            .append(alluserBuffer).append(")a left join ( select userid,userserviceid,").append(condition)
                            .append(" as count from (select *,'1' as userserviceid from iptv.").append(tablename)
                            .append(whereStr).append(" union all select *,'2' as userserviceid from cmcc.").append(tablename)
                            .append(whereStr).append(" union all select *,'3' as userserviceid from ctcc.").append(tablename)
                            .append(whereStr).append(") uniontable")
                            .append(" group by userid,userserviceid )b on a.userid = b.userid and a.userserviceid = b.userserviceid)k)c group by groupcount");
                }
                // 计算总数
                StringBuffer totalStr = new StringBuffer("");
                if ("times".equals(searchtype)) { // 总时长
                    totalStr.append("select sum(count) as total from ( select a.userid,NVL(b.count,0) as count from (")
                            .append(alluserBuffer).append(")a left join ( select userid,'1' as userserviceid,").append(condition)
                            .append(" as count from (").append(customconditon).append(" from iptv.").append(tablename)
                            .append(whereStr).append(")s)t group by userid").append(" union all ")
                            .append("select userid,'2' as userserviceid,").append(condition)
                            .append(" as count from (").append(customconditon).append(" from cmcc.").append(tablename)
                            .append(whereStr).append(")s)t group by userid").append(" union all ")
                            .append("select userid,'3' as userserviceid,").append(condition)
                            .append(" as count from (").append(customconditon).append(" from ctcc.").append(tablename)
                            .append(whereStr).append(")s)t group by userid")
                            .append(")b on a.userid = b.userid and a.userserviceid = b.userserviceid)k");
                } else {
                    totalStr.append("select sum(count) as total from ( select a.userid,NVL(b.count,0) as count from (")
                            .append(alluserBuffer).append(")a left join ( select userid,userserviceid,").append(condition)
                            .append(" as count from (select *,'1' as userserviceid from iptv.").append(tablename)
                            .append(whereStr).append(" union all select *,'2' as userserviceid from cmcc.").append(tablename)
                            .append(whereStr).append(" union all select *,'3' as userserviceid from ctcc.").append(tablename)
                            .append(whereStr).append(") uniontable")
                            .append(" group by userid,userserviceid )b on a.userid = b.userid and a.userserviceid = b.userserviceid)k");
                }
                BigDecimal total = new BigDecimal(0);
                String totalsql = totalStr.toString().replaceAll("#today#", today);
                System.out.println(totalsql);
                try {
                    total = new BigDecimal(sparkjdbc.queryForMap(totalsql).get("total").toString());
                } catch (Exception e) {
                    System.out.println(totalsql + "计算总数查询失败...");
                }
                // 计算均值
                DecimalFormat df2 = new DecimalFormat("#.00");
                BigDecimal aver = total.divide(new BigDecimal(allusercount), 2, BigDecimal.ROUND_HALF_UP);
                // 计算分组结果
                List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
                String usersql = sqlStr.toString().replaceAll("#today#", today);
                System.out.println(usersql);
                try {
                    result = sparkjdbc.queryForList(usersql);
                } catch (Exception e) {
                    System.out.println(usersql + "计算分组结果查询失败...");
                }
                // 组装json
                Map<String, Map<String, Object>> resultToMap = new HashMap<String, Map<String, Object>>();
                resultToMap = result.stream().collect(Collectors.toMap(a -> a.get("groupcount").toString(), a -> a));
                Map<String, Map<String, Object>> finalResultToMap = resultToMap;
                rangeList.stream().forEach(a -> {
                    if (finalResultToMap.containsKey(a.get("name"))) {
                        Map<String, Object> map = new HashMap<>();
                        map.put("item", a.get("name"));
                        map.put("count", finalResultToMap.get(a.get("name")).get("usercount"));
                        map.put("ratio", finalResultToMap.get(a.get("name")).get("userpercent"));
                        detail.add(map);
                    }
                });
                resultMap.put("name", jsonObject.get("startTime").toString() + "-" + jsonObject.get("endTime"));
                resultMap.put("total", df2.format(total));
                resultMap.put("aver", aver);
                resultMap.put("detail", detail);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return gson.toJson(resultMap);
    }

    /**
     * 获取用户特征分布结果.
     * @param requst 参数.
     * @return json.
     */
    public String getAttrUserGroup(JSONObject requst, String tagId,
                                   String layerId, int create_type, Map<String,Map<String, String>> attrMap,
                                   Integer allusercount, String profile_sql, String createType, String clusterId) {
        Map<String, Object> result = new HashMap<String, Object>();
        try {
            // 获取参数
            String byx = requst.get("byx").toString();
            String byy = requst.get("byy").toString();
            String byxtype = requst.get("byxtype").toString();
            String byytype = requst.get("byytype").toString();
            Map<String, String> name_id_map_x = new HashMap<String, String>();
            Map<String, String> name_id_map_y = new HashMap<String, String>();
            name_id_map_x = getNameById(byxtype, attrMap, byx);
            name_id_map_y = getNameById(byytype, attrMap, byy);
            // 交叉计算分组人数
            StringBuffer sql = new StringBuffer("");
            StringBuffer selecttr = new StringBuffer("");
            StringBuffer groupbytr = new StringBuffer("");
            StringBuffer whereStr = new StringBuffer("");
            if (create_type == 1) {
                if ("1".equals(createType)) {
                    if ("all".equals(layerId)) {
                        List<Map<String, Object>> tagList = getLayerIdByTagID(tagId);
                        List<String> conditionList = tagList.stream().map(a -> a.get("id").toString()).collect(Collectors.toList());
                        whereStr.append(" where (").append(String.join("=1 or ", conditionList)).append("=1) ");
                    } else {
                        whereStr.append(" where l").append(layerId).append("=1 ");
                    }
                } else if ("2".equals(createType)) {
                    whereStr.append(" where c").append(clusterId).append("=1 ");
                }
            }
            List<String> byxList = getConditionStr(byxtype, byx);
            List<String> byyList = getConditionStr(byytype, byy);
            // select条件和where条件
            if (byxList.size() == 1) {
                selecttr.append(byxList.get(0)).append(" as xdata,");
            } else if (byxList.size() > 1) {
                selecttr.append("concat(").append(String.join(",',',", byxList)).append(") as xdata,");
                whereStr.append(" and (").append(String.join("=1 or ", byxList)).append("=1").append(")");
                // 字段对应名称
                Map<String, String> resultColumnx = new HashMap<String, String>();
                String[] a = new String[byxList.size()];
                Arrays.fill(a, "0");
                Map<String, String> finalName_id_map_x = name_id_map_x;
                byxList.stream().forEach(x -> {
                    Arrays.fill(a, "0");
                    a[byxList.indexOf(x)] = "1";
                    resultColumnx.put(String.join(",", Arrays.asList(a)), finalName_id_map_x.get(x));
                });
                name_id_map_x = resultColumnx;
            }
            if (byyList.size() == 1) {
                selecttr.append(byyList.get(0)).append(" as ydata,");
            } else if (byyList.size() > 1) {
                selecttr.append("concat(").append(String.join(",',',", byyList)).append(") as ydata,");
                whereStr.append(" and ").append("(").append(String.join("=1 or ", byyList)).append("=1").append(")");
                // 字段对应名称
                Map<String, String> resultColumny = new HashMap<String, String>();
                String[] a = new String[byyList.size()];
                Arrays.fill(a, "0");
                Map<String, String> finalName_id_map_y = name_id_map_y;
                byyList.stream().forEach(y -> {
                    Arrays.fill(a, "0");
                    a[byyList.indexOf(y)] = "1";
                    resultColumny.put(String.join(",", Arrays.asList(a)), finalName_id_map_y.get(y));
                });
                name_id_map_y = resultColumny;
            }
            // group by 条件
            groupbytr.append(String.join(",", byxList)).append(",").append(String.join(",", byyList));
            // 组装sql
            sql.append("select ").append(selecttr).append(" count(userid) as usercount, round(count(userid)/")
                    .append(allusercount).append(",4) as userpercent from ").append(profile_sql)
                    .append(whereStr)
                    .append(" group by ").append(groupbytr).append(" order by xdata,ydata");
            // 查询数据.
            List<Map<String, Object>> userGroupList = new ArrayList<Map<String, Object>>();
            try {
                System.out.println(sql.toString());
                userGroupList = sparkjdbc.queryForList(sql.toString());
            } catch (Exception e) {
                System.out.println(sql.toString() + "查询失败...");
            }
            // 组装结果
            List<Map<String, Object>> detail = new ArrayList<Map<String, Object>>();
            Map<String, String> map_y = name_id_map_y;
            Map<String, String> map_x = name_id_map_x;
            Map<String, Map<String, Object>> detailMap = new HashMap<String, Map<String, Object>>();
            userGroupList.stream().forEach(data -> {
                String xdata = data.get("xdata").toString();
                String ydata = data.get("ydata").toString();
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("itemY", map_y.containsKey(ydata) ? map_y.get(ydata) : "未知");
                map.put("itemX", map_x.containsKey(xdata) ? map_x.get(xdata) : "未知");
                map.put("count", data.get("usercount"));
                map.put("ratio", data.get("userpercent"));
                detailMap.put(xdata + "_" + ydata, map);
            });
            // 补全没有用户数据的组合
            map_x.keySet().stream().forEach(x -> {
                map_y.keySet().stream().forEach(y -> {
                    String tempKey = x + "_" + y;
                    if (detailMap.containsKey(tempKey)) {
                        detail.add(detailMap.get(tempKey));
                    } else {
                        Map<String, Object> map = new HashMap<String, Object>();
                        map.put("itemY", map_y.containsKey(y) ? map_y.get(y) : "未知");
                        map.put("itemX", map_x.containsKey(x) ? map_x.get(x) : "未知");
                        map.put("count", 0);
                        map.put("ratio", 0);
                        detail.add(map);
                    }
                });
            });
            result.put("detail", detail);
            result.put("name", attrMap.get("labelid").get(byx.toLowerCase()) + "×" + attrMap.get("labelid").get(byy.toLowerCase()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return gson.toJson(result);
    }

    /**
     * Id对应Name关系.
     * @return
     */
    private Map<String,Map<String,String>> getInitLabelMap() {
        Map<String,Map<String,String>> resultMap = new HashMap<String,Map<String,String>>();
        // 区域表
        Map<String, String> areaMap = new LinkedHashMap<String, String>();
        areaMap = getTabelIdName("user_areagroup", "areaId", "areaName", "sortid");
        resultMap.put("userareaid", areaMap);
        // 平台表
        Map<String, String> platMap = new LinkedHashMap<String, String>();
        platMap = getTabelIdName("user_plat","platId","platName","platId");
        resultMap.put("userplatid", platMap);
        // 运营商表
        Map<String, String> serviceMap = new LinkedHashMap<String, String>();
        serviceMap = getTabelIdName("user_service", "serviceId","serviceName", "sortid");
        resultMap.put("userserviceid", serviceMap);
        // 类型表
        Map<String, String> typeMap = new LinkedHashMap<String, String>();
        typeMap = getTabelIdName("user_type","typeId","typeName","typeId");
        resultMap.put("usertypeid", typeMap);
        // 型号表v
        Map<String, String> modelMap = new LinkedHashMap<String, String>();
        modelMap = getTabelIdName("user_model","modelId","modelName","modelId");
        resultMap.put("usermodelid", modelMap);
        // 版本表
        Map<String, String> versionMap = new LinkedHashMap<String, String>();
        versionMap = getTabelIdName("user_version","versionId","versionName","versionId");
        resultMap.put("userversionid", versionMap);
        // 行业表
        Map<String, String> pubMap = new LinkedHashMap<String, String>();
        pubMap = getTabelIdName("user_pub","pubId","pubName","pubId");
        resultMap.put("userpubid", pubMap);
        // 活跃表
        Map<String, String> activeMap =  new LinkedHashMap<String, String>();
        activeMap = getTabelIdName("user_active","activeId","activeName", "sortid");
        resultMap.put("useractivedays", activeMap);
        // 下拉列表整合表
        Map<String, String> labelMap = new LinkedHashMap<String, String>();
        String sql = "SELECT ACTION_ID AS ID,ACTION_NAME AS NAME FROM PERSONAS_ACTION\n" +
                "UNION ALL\n" +
                "SELECT ATTR_ID AS ID,ATTR_NAME AS NAME FROM PERSONAS_ATTR\n" +
                "UNION ALL\n" +
                "SELECT TO_CHAR(TAG_ID) AS ID,TAG_NAME AS NAME FROM PERSONAS_TAG\n" +
                "UNION ALL\n" +
                "SELECT TO_CHAR(CLUSTER_ID) AS ID,CLUSTER_NAME AS NAME FROM PERSONAS_CLUSTER";
        labelMap = getTabelIdNameBySql(sql, "id","name");
        resultMap.put("labelid", labelMap);
        return resultMap;
    }
    /**
     * 获取用户分组后的结果sql.
     * @param type attr-按属性分组 tag-按标签分组 cluster-按分群分组
     * @param by 分组id
     * @param create_type 创建用户画像方式：0-条件创建;1-结果创建.
     * @param layerId 待分析用户所属标签层
     * @param tagId 待分析用户所属标签
     * @param allusercount 用户总数
     * @param profile_sql 查询表
     * @param createType 1是标签保存 2是分群保存
     * @param clusterId 分群id.
     * @return sql查询语句
     */
    private String getGroupUserCount(String type, String by, int create_type, String layerId,
                                     String tagId, int allusercount, String profile_sql,
                                     String createType, String clusterId, Map<String,Map<String, String>> attrMap) {
        System.out.println("getGroupUserCount:allusercount=" + allusercount);
        StringBuffer sparkSql_groupUserCount = new StringBuffer();
        StringBuffer commonBuffer = new StringBuffer();
        commonBuffer.append("count(userid) as usercount, round(count(userid)/")
                .append(allusercount).append(",4) as userpercent from ").append(profile_sql);
        if(create_type == 1) {
            if("1".equals(createType)) { // 标签结果.
                if ("all".equals(layerId)) {
                    List<Map<String, Object>> tagList = getLayerIdByTagID(tagId);
                    List<String> conditionList = tagList.stream().map(a -> a.get("id").toString()).collect(Collectors.toList());
                    commonBuffer.append(" where (").append(String.join("=1 or ", conditionList)).append("=1)");
                } else {
                    commonBuffer.append(" where l").append(layerId).append("=1");
                }
            } else if("2".equals(createType)) { // 分群结果.
                commonBuffer.append(" where c").append(clusterId).append("=1");
            }
        }
        // 按属性标签求用户分组.
        if("attr".equals(type)) {
            String condition = create_type==0 ? " where " : " and ";
            if("useractivedays".equals(by)) {
                List<String> activeList = new ArrayList<String>();
                Map<String, String> activeMap = attrMap.get("useractivedays");
                activeMap.keySet().forEach(key-> {
                    StringBuffer activeSql = new StringBuffer();
                    activeSql.append("select '").append(activeMap.get(key)).append("' as groupid,").append(commonBuffer)
                            .append(condition).append(" useractivedays ").append(key);
                    activeList.add(activeSql.toString());
                });
                sparkSql_groupUserCount.append(String.join(" union all ", activeList))
                        .append(" order by userpercent ");
            } else {
                sparkSql_groupUserCount.append("select ").append(by)
                        .append(" as groupid,").append(commonBuffer)
                        .append(" group by ").append(by).append(" order by userpercent desc");
            }
        }
        // 按标签求用户分组.
        else if("tag".equals(type)) {
            List<Map<String, Object>> tagList = getLayerIdByTagID(by);
            List<String> layerList = tagList.stream().map(a->a.get("id").toString()).collect(Collectors.toList());
            layerList.stream().forEach(layer-> {
                sparkSql_groupUserCount.append("select '").append(layer)
                        .append("' as groupid,").append(commonBuffer);
                if(create_type == 1) {
                    sparkSql_groupUserCount.append(" and ");
                } else {
                    sparkSql_groupUserCount.append(" where ");
                }
                sparkSql_groupUserCount.append(layer).append("=1")
                        .append(" union all ");
            });
            if(sparkSql_groupUserCount.toString().contains(" union all ")) {
                sparkSql_groupUserCount
                        .delete(sparkSql_groupUserCount.lastIndexOf(" union all "), sparkSql_groupUserCount.length());
            }
        }
        // 按分群求用户分组.
        else if("cluster".equals(type)) {
            sparkSql_groupUserCount.append("select ").append("'1'")
                    .append(" as groupid,").append(commonBuffer);
            if(create_type == 1) {
                sparkSql_groupUserCount.append(" and ");
            } else {
                sparkSql_groupUserCount.append(" where ");
            }
            sparkSql_groupUserCount.append(by).append("=1 union all ")
                    .append("select ").append("'0'")
                    .append(" as groupid,").append(commonBuffer);
            if(create_type == 1) {
                sparkSql_groupUserCount.append(" and ");
            } else {
                sparkSql_groupUserCount.append(" where ");
            }
            sparkSql_groupUserCount.append(by).append("=0");
        }
        return sparkSql_groupUserCount.toString();
    }
    /**
     * 根据表名获得id和name对应关系.
     * @param tablename 表名.
     * @param idcolumn id名.
     * @param namecolumn name名
     * @return Map(id,name)
     */
    private Map<String,String> getTabelIdName(String tablename, String idcolumn, String namecolumn, String sortid) {
        Map<String, String> id_name_Map = new LinkedHashMap<String, String>();
        StringBuffer sql = new StringBuffer();
        sql.append("select ").append(idcolumn).append(" as id,").append(namecolumn).append(" as name from ")
                .append(tablename).append(" group by ").append(idcolumn).append(",").append(namecolumn).append(",").append(sortid)
                .append(" order by ").append(sortid);
        List<Map<String, Object>> list = mysqljdbc.queryForList(sql.toString());
        for(int i = 0; i<list.size();i++) {
            id_name_Map.put(list.get(i).get("id").toString(), list.get(i).get("name").toString());
        }
        return id_name_Map;
    }
    private Map<String,String> getTabelIdNameBySql(String sqlString, String idcolumn, String namecolumn) {
        Map<String, String> id_name_Map = new HashMap<String, String>();
        List<Map<String, Object>> list = oraclejdbc.queryForList(sqlString);
        id_name_Map = list.stream()
                .collect(Collectors.toMap(column->column.get(idcolumn).toString(), column->column.get(namecolumn).toString()));
        return id_name_Map;
    }
    /**
     * 通过分群用户类型，给定显示内容名称
     * @param type  attr-按属性分组 tag-按标签分组 cluster-按分群分组
     * @param attrMap 属性总Map
     * @return
     */
    private Map<String, String> getNameById(String type, Map<String,Map<String, String>> attrMap, String labelId) {
        Map<String, String> name_id_map = new HashMap<String, String>();
        if("attr".equals(type)) {
            name_id_map = attrMap.get(labelId);
        } else if("tag".equals(type)) {
            name_id_map = getLayerIdByTagID(labelId).stream()
                    .collect(Collectors.toMap(a->a.get("id").toString(),a->a.get("name").toString()));
        } else if("cluster".equals(type)) {
            name_id_map.put("1","1");
            name_id_map.put("0","0");
        }
        return name_id_map;
    }

    /**
     * 获取条件list
     * @param type attr-按属性分组 tag-按标签分组 cluster-按分群分组
     * @param id label_id
     * @return List
     */
    private List<String> getConditionStr(String type, String id) {
        List<String> conditionList = new ArrayList<String>();
        if("attr".equals(type)) {
            conditionList.add(id);
        } else if("tag".equals(type)) {
            conditionList = getLayerIdByTagID(id).stream().map(a->a.get("id").toString()).collect(Collectors.toList());
        } else if("cluster".equals(type)) {
            conditionList.add('c'+id);
        }
        return conditionList;
    }

    /**
     * 通过标签id获取标签层id List
     * @param tagId 标签id
     * @return List<Map<"l30001","活跃用户">
     */
    private List<Map<String, Object>> getLayerIdByTagID(String tagId) {
        List<Map<String, Object>> tagList = new ArrayList<Map<String, Object>>();
        tagList = oraclejdbc
                .queryForList("select 'l'||layer_id as id,layer_name as name from personas_layer where tag_id="
                        +tagId);
        return tagList;
    }
    /**
     * 根据查询天数放大range.
     * @param range 原始range.
     * @param length 动态时间框开始时间与结束时间相差秒数sql.
     * @param searchType 查询指标（次数和时间计算区间方法不同）
     * @return 新的range.
     */
    private List<Map<String,String>> chageRange(List<Map<String, String>> range, String length, String searchType) {
        String sql = "SELECT "+ length + "  AS SECONDSDIFF";
        System.out.println("length="+sql);
        long secondsdiff = Long.valueOf(sparkjdbc.queryForMap("SELECT "+ length + "  AS SECONDSDIFF").get("SECONDSDIFF").toString());
        BigDecimal diffDate = new BigDecimal(secondsdiff/86400).setScale(0,BigDecimal.ROUND_HALF_UP);
        List<Map<String, String>> finalRangList = new ArrayList<Map<String, String>>();
        if("times".equals(searchType)) {
            range.stream().forEach(r-> {
                String min = r.get("min");
                String max = r.get("max");
                String newmin = finalRangList.size() == 0 ? min : new BigDecimal(Integer.valueOf(min)).multiply(diffDate).setScale(0, BigDecimal.ROUND_HALF_UP).toString();
                String newmax = "max".equals(max)? "max":new BigDecimal(Integer.valueOf(max)).multiply(diffDate).setScale(0, BigDecimal.ROUND_HALF_UP).toString();
                Map<String, String> tempmap = new HashMap<String, String>();
                tempmap.put("name", newmax.equals("max")?">="+newmin:newmin+"~"+newmax);
                tempmap.put("min", newmin);
                tempmap.put("max", newmax);
                if("max".equals(newmax)) {
                    tempmap.put("condition", ">="+newmin);
                } else {
                    tempmap.put("condition", " >= "+newmin+" and count < " + newmax);
                }
                finalRangList.add(tempmap);
            });
        } else {
            range.stream().forEach(r-> {
                String min = r.get("min");
                String max = r.get("max");
                String newmin = finalRangList.size()==0?
                        min :
                        String.valueOf(Integer.valueOf(finalRangList.get(finalRangList.size()-1).get("max"))+1);
                String newmax = "max".equals(max)?
                        "max":
                        new BigDecimal(Integer.valueOf(max)).multiply(diffDate).setScale(0, BigDecimal.ROUND_HALF_UP).toString();
                Map<String, String> tempmap = new HashMap<String, String>();
                tempmap.put("name", newmax.equals("max")?">="+newmin:newmin+"~"+newmax);
                tempmap.put("min", newmin);
                tempmap.put("max", newmax);
                if("max".equals(newmax)) {
                    tempmap.put("condition", ">="+newmin);
                } else {
                    tempmap.put("condition", " between "+newmin+" and "+newmax);
                }
                finalRangList.add(tempmap);
            });
        }
        return finalRangList;
    }

    /**
     * JSONArray 转 List
     * @param rangeArray
     * @return
     */
    private List<Map<String, String>> jsonArrayToList(JSONArray rangeArray) {
        List<Map<String, String>> list = new ArrayList<>();
        try {
            for (int i = 0; i < rangeArray.length(); i++) {
                Map<String, String> map = new HashMap<String, String>();
                JSONObject jsonObject = rangeArray.getJSONObject(i);
                map.put("min", jsonObject.get("min").toString());
                map.put("max", jsonObject.get("max").toString());
                map.put("name", jsonObject.get("name").toString());
                list.add(map);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }



}
