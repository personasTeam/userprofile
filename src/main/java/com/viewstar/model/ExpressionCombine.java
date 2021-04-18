package com.viewstar.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.viewstar.util.HttpClientUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.w3c.dom.Attr;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 组合创建用户画像条件.
 */
public class ExpressionCombine {
    public Gson gson = new Gson();
    Gson gs = new GsonBuilder()
            //.setPrettyPrinting()
            .disableHtmlEscaping()
            .create();
    HttpClientUtil httpClientUtil = new HttpClientUtil();
    public String SEQUENCE_PATH = "http://10.0.9.42:8767/sparkUseranalysis/saveSeqRouteData";
    //public String SEQUENCE_PATH = "http://192.168.18.220:8767/sparkUseranalysis/saveSeqRouteData";

    /**
     * 递归解析条件表达式.
     * @param expType attr属性条件,action行为条件,sequence行为队列条件.
     * @param jsonStr 待解析json串.
     * @param date 日期参数.
     * @return 条件表达式.
     */
    public String buildExpression(String expType, String jsonStr, String date,
                                  Map<String, Boolean> serviceFilterMap,JdbcTemplate oraclejdbc ) {
        StringBuffer expression = new StringBuffer("");
        try {
            JSONObject jsonObject = new JSONObject(jsonStr);
            String subjson = jsonObject.getString("list");
            if(!subjson.contains("\"switch\"")) {
                if(expType.equals("attr")) { // 用户满足属性
                    List<AttrExpression> expList = new ArrayList<AttrExpression>();
                    JSONArray jsonList = jsonObject.getJSONArray("list");
                    // 解析json中list条件，循环创建AttrExpression对象.
                    for(int i = 0; i < jsonList.length(); i++) {
                        AttrExpression attrExpression = new AttrExpression((JSONObject) jsonList.get(i));
                        expList.add(attrExpression);
                    }
                    // 循环解析成条件语句并组成List
                    List<String> strList = expList.stream().map(attr->attr.getExpression()).collect(Collectors.toList());
                    // 循环拼写where条件，用switch关系符连接List中的条件.
                    expression.append("(")
                            .append(String.join(" " + jsonObject.getString("switch") + " ", strList))
                            .append(")");
                } else if(expType.equals("action")) { // 用户行为满足
                    List<ActionExpression> expList = new ArrayList<ActionExpression>();
                    JSONArray jsonList = jsonObject.getJSONArray("list");
                    // 解析json中list条件，循环创建ActionExpression对象.
                    for(int i = 0; i < jsonList.length(); i++) {
                        ActionExpression actionExpression = new ActionExpression((JSONObject) jsonList.get(i), date, serviceFilterMap, oraclejdbc);
                        expList.add(actionExpression);
                    }
                    // 循环解析成条件语句并组成List
                    List<String> strList = expList.stream().map(attr->attr.getExpression()).collect(Collectors.toList());
                    // 循环拼写where条件，用switch关系符连接List中的条件.
                    expression.append("select userid,type from (")
                            .append(String.join(" " + jsonObject.getString("switch") + " ", strList))
                            .append(") S");
                } else if(expType.equals("sequence")) { // 行为序列.
                    List<Map<String, List<String>>> sequenceList = new ArrayList<Map<String, List<String>>>();
                    String switchstr = jsonObject.getString("switch");
                    JSONArray jsonList = jsonObject.getJSONArray("list");
                    for(int i = 0; i < jsonList.length(); i++) {
                        SequenceExpression sequenceExpression = new SequenceExpression((JSONObject) jsonList.get(i),
                                date, serviceFilterMap);
                        List<String> expressionList = sequenceExpression.getExpression();
                        Map<String, List<String>> map = new HashMap<>();
                        map.put("sql", expressionList);
                        sequenceList.add(map);
                    }
                    Map<String, Object> map = new HashMap<>();
                    map.put("switch","and");
                    map.put("list",sequenceList);
                    String sqllist = gs.toJson(map);
                    System.out.println("用户序列list：" + sqllist);
                    try {
                        sqllist = URLEncoder.encode(sqllist, "UTF-8");
                        String sql = httpClientUtil.getData(SEQUENCE_PATH, "sqllist="+sqllist);
                        System.out.println("httpClientUtil sql:" + sql);
                        expression.append(sql);
                        System.out.println("sequence expression:" + expression.toString());
                    } catch (Exception e) {
                        System.out.println("请求接口失败"+SEQUENCE_PATH + sqllist);
                    }
                }
                return expression.toString();
            } else {
                JSONArray list = jsonObject.getJSONArray("list");
                List<String> tempList = new ArrayList<String>();
                for(int i = 0; i < list.length(); i++) {
                    tempList.add(list.getString(i));
                }
                // 递归调用处理函数
                expression.append("(")
                        .append(String.join(" " + jsonObject.getString("switch") + " ",
                                tempList.stream().map(a->
                                        buildExpression(expType, a, date, serviceFilterMap, oraclejdbc)).collect(Collectors.toList())))
                        .append(")");
                return expression.toString();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return expression.toString();
    }

    public static void main(String[] args) {
        JdbcTemplate oraclejdbc = null;
        Map<String, Boolean> map = new HashMap<String, Boolean>();
        map.put("cucc", true);
        map.put("cmcc", false);
        map.put("ctcc", false);
        String json = "{\"switch\":\"intersect\",\"list\":[{\"switch\":\"intersect\",\"list\":[{\"id\":\"u82WHw4TbR\",\"index\":\"1\",\"customDate\":{\"type\":\"specific\",\"startTime\":\"2020-02-01 00:00:00\",\"endTime\":\"2020-02-03 23:59:59\",\"customStartTimeBtn\":\"time\",\"customEndTimeBtn\":\"today\",\"customStartInputValue\":1,\"customEndInputValue\":2},\"did\":\"yes\",\"oper\":\"channeluserparquet\",\"result\":\"count\",\"relation\":\">=\",\"value\":\"1\",\"key\":\"u82WHw4TbR\",\"filters\":{\"filtersswitch\":\"and\",\"list\":[{\"id\":\"JcomLiqWpa\",\"attr\":\"channelid\",\"relation\":\"=\",\"value\":\"1,301\",\"type\":\"event\",\"valueType\":\"string\",\"index\":\"1\"}]}}]}]}";
        //String json = " {\n" + "        \"switch\":\"intersect \",\n" + "        \"list\":[\n" + "            {\n" + "                \"switch\":\"intersect \",\n" + "                \"list\":[\n" + "                    {\n" + "                        \"id\":\"111\",\n" + "                        \"index\":\"1\",\n" + "                        \"startTime\":\"2019-10-01 00:00:00\",\n" + "                        \"endTime\":\"2019-10-02 00:00:00\",\n" + "                        \"did\":\"yes\",\n" + "                        \"oper\":\"channeluserparquet\",\n" + "                        \"result\":\"count\",\n" + "                        \"relation\":\"in\",\n" + "                        \"value\":\"1@5\",\n" + "                        \"filters\":{\n" + "                            \"filtersswitch\":\"or\",\n" + "                            \"list\":[\n" + "                                {\n" + "                                    \"id\":\"1\",\n" + "                                    \"index\":\"1\",\n" + "                                    \"type\":\"attr\",\n" + "                                    \"attr\":\"userareaid\",\n" + "                                    \"relation\":\"=\",\n" + "                                    \"value\":\"1,2,3\"\n" + "                                }\n" + "                            ]\n" + "                        }\n" + "                    }\n" + "                ]\n" + "            },\n" + "            {\n" + "                \"switch\":\"union\",\n" + "                \"list\":[\n" + "                    {\n" + "                        \"id\":\"111\",\n" + "                        \"index\":\"1\",\n" + "                        \"startTime\":\"2019-10-01 00:00:00\",\n" + "                        \"endTime\":\"2019-10-02 00:00:00\",\n" + "                        \"did\":\"yes\",\n" + "                        \"oper\":\"channeluserparquet\",\n" + "                        \"result\":\"count\",\n" + "                        \"relation\":\">=\",\n" + "                        \"value\":\"10\"\n" + "                    },\n" + "                    {\n" + "                        \"id\":\"111\",\n" + "                        \"index\":\"1\",\n" + "                        \"startTime\":\"2019-10-01 00:00:00\",\n" + "                        \"endTime\":\"2019-10-02 00:00:00\",\n" + "                        \"did\":\"yes\",\n" + "                        \"oper\":\"channeluserparquet\",\n" + "                        \"result\":\"count\",\n" + "                        \"relation\":\"top\",\n" + "                        \"value\":\"in@1@100@ratio\",\n" + "                        \"filters\":{\n" + "                            \"filtersswitch\":\"or\",\n" + "                            \"list\":[\n" + "                                {\n" + "                                    \"id\":\"1\",\n" + "                                    \"index\":\"1\",\n" + "                                    \"type\":\"attr\",\n" + "                                    \"attr\":\"userareaid\",\n" + "                                    \"relation\":\"=\",\n" + "                                    \"value\":\"1,2,3\"\n" + "                                }\n" + "                            ]\n" + "                        }\n" + "                    }\n" + "                ]\n" + "            }\n" + "        ]    \n" + "\t}\n";
        // String json = "{\n" + "        \"switch\":\"intersect \",\n" + "        \"list\":[\n" + "            {\n" + "                \"switch\":\"intersect \",\n" + "                \"list\":[\n" + "                    {\n" + "                        \"id\":\"111\",\n" + "                        \"index\":\"1\",\n" + "                        \"startTime\":\"2019-10-01 00:00:00\",\n" + "                        \"endTime\":\"2019-10-02 00:00:00\",\n" + "                        \"did\":\"yes\",\n" + "                        \"oper\":\"channeluserparquet\",\n" + "                        \"result\":\"count\",\n" + "                        \"relation\":\"in\",\n" + "                        \"value\":\"1@5\",\n" + "                        \"filters\":{\n" + "                            \"filtersswitch\":\"or\",\n" + "                            \"list\":[\n" + "                                {\n" + "                                    \"id\":\"1\",\n" + "                                    \"index\":\"1\",\n" + "                                    \"type\":\"attr\",\n" + "                                    \"attr\":\"userareaid\",\n" + "                                    \"relation\":\"=\",\n" + "                                    \"value\":\"1,2,3\"\n" + "                                }\n" + "                            ]\n" + "                        }\n" + "                    }\n" + "                ]\n" + "            },\n" + "            {\n" + "                \"switch\":\"union\",\n" + "                \"list\":[\n" + "                    {\n" + "                        \"id\":\"111\",\n" + "                        \"index\":\"1\",\n" + "                        \"startTime\":\"2019-10-01 00:00:00\",\n" + "                        \"endTime\":\"2019-10-02 00:00:00\",\n" + "                        \"did\":\"yes\",\n" + "                        \"oper\":\"channeluserparquet\",\n" + "                        \"result\":\"count\",\n" + "                        \"relation\":\">=\",\n" + "                        \"value\":\"10\"\n" + "                    },\n" + "                    {\n" + "                        \"id\":\"111\",\n" + "                        \"index\":\"1\",\n" + "                        \"startTime\":\"2019-10-01 00:00:00\",\n" + "                        \"endTime\":\"2019-10-02 00:00:00\",\n" + "                        \"did\":\"yes\",\n" + "                        \"oper\":\"channeluserparquet\",\n" + "                        \"result\":\"count\",\n" + "                        \"relation\":\"top\",\n" + "                        \"value\":\"in@1@100@ratio\"\n" + "                    }\n" + "                ]\n" + "            }\n" + "        ]    \n" + "\t}";
        String jsonType = "action";
        ExpressionCombine expressionCombine = new ExpressionCombine();
        String s = expressionCombine.buildExpression(jsonType, json, "20191118",map, oraclejdbc);
        System.out.println(s);
    }
}
