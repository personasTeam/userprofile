package com.viewstar.model;

import com.google.gson.Gson;
import org.apache.avro.generic.GenericData;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用户行为表达式.
 */
public class ActionExpression extends Expression {
    private Customdate customdate; // 时间框
    private String did; // 是否做过 yes/no
    private String oper; // 查询的表名.
    private String result; // 查询的结果字段.
    private String relation; // 查询条件
    private String value; // 条件值
    private String filters; // 小漏斗里的json串
    private String date; // 最近数据时间
    private String startTime;
    private String endTime;
    private String startday;
    private String endday;
    private String length;
    private Map<String, Boolean> serviceFilterMap;
    private JdbcTemplate oraclejdbc;

    /**ActionExpression
     * 构造函数.
     * @param map 参数map
     * @param date 查询时间
     * @throws JSONException 参数解析异常.
     */
    public ActionExpression(JSONObject map, String date,Map<String, Boolean> serviceFilterMap,JdbcTemplate oraclejdbc) throws Exception {
        super(map.get("id").toString(), map.get("index").toString());
        System.out.println("ActionExpression--customDate:" + map.get("customDate").toString());
        JSONObject jsonObject = new JSONObject(map.get("customDate").toString());
        this.customdate = new Customdate(jsonObject);
        Map<String, Object> customMap = this.customdate.getTimeSpanMap();
        this.startTime = customMap.get("starttime").toString();
        this.endTime = customMap.get("endtime").toString();
        this.startday = customMap.get("startday").toString();
        this.endday = customMap.get("endday").toString();
        this.length = "unix_timestamp("+ this.endTime+",'yyyyMMddHHmmss')-unix_timestamp("
                +this.startTime+",'yyyyMMddHHmmss')+1";
        this.did = map.get("did").toString();
        this.oper = map.get("oper").toString();
        this.result = map.get("result").toString();
        this.relation = map.get("relation").toString();
        this.value = map.get("value").toString();
        if(map.has("filters")) {
            this.filters = map.get("filters").toString();
        } else {
            this.filters = "";
        }
        this.date = date;
        this.serviceFilterMap = serviceFilterMap;
        this.oraclejdbc = oraclejdbc;
    }

    /**
     * 拼接表达式.
     * @return 表达式string.
     */
    @Override
    public String getExpression() {
        StringBuffer sql = new StringBuffer("");
        StringBuffer actionsql = new StringBuffer("");
        StringBuffer actionsql_iptv = new StringBuffer("");
        StringBuffer actionsql_cmcc = new StringBuffer("");
        StringBuffer actionsql_ctcc = new StringBuffer("");
        StringBuffer alluserSql = new StringBuffer("");
        String startDate = this.startday;
        String endDate = this.endday;
        // 加入跨天逻辑，包括直播、点播、回看、页面次数统计数据，限制flag=1
        String spanDay = "";
        if("channeluserparquet".equals(oper)||"voduserparquet".equals(oper)||"tvoduserparquet".equals(oper)||"pagedataparquet".equals(oper)) {
            spanDay = " and flag = 1";
        }
        try {
            alluserSql.append("select * from iptv.userprofilepar").append(date).append(" ");
            actionsql_iptv.append("select userid,'1' as groupid");
            actionsql_cmcc.append("select userid,'2' as groupid");
            actionsql_ctcc.append("select userid,'3' as groupid");
            if("count".equals(result)||"ordercount".equals(result)||"cancelcount".equals(result)) {
                actionsql_iptv.append(",count(starttime) as ").append(result).append(" from (");
                actionsql_cmcc.append(",count(starttime) as ").append(result).append(" from (");
                actionsql_ctcc.append(",count(starttime) as ").append(result).append(" from (");
                actionsql_iptv.append("select userid,starttime")
                        .append(" from iptv.").append(oper)
                        .append(" where ptime between ").append(startDate).append(" and ").append(endDate).append(spanDay)
                        .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                        .append(this.endTime).append(" as bigint)))");
                actionsql_cmcc.append("select userid,starttime")
                        .append(" from cmcc.").append(oper)
                        .append(" where ptime between ").append(startDate).append(" and ").append(endDate).append(spanDay)
                        .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                        .append(this.endTime).append(" as bigint)))");
                actionsql_ctcc.append("select userid,starttime")
                        .append(" from ctcc.").append(oper)
                        .append(" where ptime between ").append(startDate).append(" and ").append(endDate).append(spanDay)
                        .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                        .append(this.endTime).append(" as bigint)))");
            } else if("times".equals(result)) {
                actionsql_iptv.append(",sum(seconds) as ").append(result).append(" from (");
                actionsql_cmcc.append(",sum(seconds) as ").append(result).append(" from (");
                actionsql_ctcc.append(",sum(seconds) as ").append(result).append(" from (");
                actionsql_iptv.append("select userid,starttime,").append("(case when starttime <= searchstarttime and " +
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
                        .append("select *, cast(").append(this.startTime).append(" as bigint) as searchstarttime,cast(")
                        .append(this.endTime).append(" as bigint) as searchendtime,").append(this.length).append(" as searchlength")
                        .append(" from iptv.").append(oper)
                        .append(" where ptime between ").append(startDate).append(" and ").append(endDate)
                        .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                        .append(this.endTime).append(" as bigint)))");
                actionsql_cmcc.append("select userid,starttime,").append("(case when starttime <= searchstarttime and " +
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
                        .append("select *, cast(").append(this.startTime).append(" as bigint) as searchstarttime,cast(")
                        .append(this.endTime).append(" as bigint) as searchendtime,").append(this.length).append(" as searchlength")
                        .append(" from cmcc.").append(oper)
                        .append(" where ptime between ").append(startDate).append(" and ").append(endDate)
                        .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                        .append(this.endTime).append(" as bigint)))");
                actionsql_ctcc.append("select userid,starttime,").append("(case when starttime <= searchstarttime and " +
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
                        .append("select *, cast(").append(this.startTime).append(" as bigint) as searchstarttime,cast(")
                        .append(this.endTime).append(" as bigint) as searchendtime,").append(this.length).append(" as searchlength")
                        .append(" from ctcc.").append(oper)
                        .append(" where ptime between ").append(startDate).append(" and ").append(endDate)
                        .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                        .append(this.endTime).append(" as bigint))")
                        .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                        .append(this.endTime).append(" as bigint)))");
            }
            if("ordercount".equals(result)) {
                actionsql_iptv.append(" and chargetype=16");
                actionsql_cmcc.append(" and chargetype=16");
                actionsql_ctcc.append(" and chargetype=16");
            }
            if("cancelcount".equals(result)) {
                actionsql_iptv.append(" and chargetype=31");
                actionsql_cmcc.append(" and chargetype=31");
                actionsql_ctcc.append(" and chargetype=31");
            }
            // 解析漏斗过滤条件
            String outCondition = "";
            List<AttrExpression> outConditionList = new ArrayList<AttrExpression>();
            String inCondition_cucc = "";
            String inCondition_cmcc = "";
            String inCondition_ctcc = "";
            List<AttrExpression> inConditionList_cucc = new ArrayList<AttrExpression>();
            List<AttrExpression> inConditionList_ctcc = new ArrayList<AttrExpression>();
            List<AttrExpression> inConditionList_cmcc = new ArrayList<AttrExpression>();
            if(!"".equals(filters)) {
                JSONObject jsonObject = new JSONObject(filters);
                String switchstr = jsonObject.getString("filtersswitch");
                JSONArray jsonList = jsonObject.getJSONArray("list");
                for(int i = 0; i < jsonList.length(); i++) {
                    JSONObject filter = (JSONObject) jsonList.get(i);
                    AttrExpression attrExpression = new AttrExpression(filter);
                    if(!"event".equals(filter.get("type"))) {
                        outConditionList.add(attrExpression);
                    } else {
                        // 订购需要单独处理
                        if("sp".equals(attrExpression.getAttr())) {
                            List<String> spsql_cucc = new ArrayList<>();
                            List<String> spsql_cmcc = new ArrayList<>();
                            List<String> spsql_ctcc = new ArrayList<>();
                            StringBuffer expressSql = new StringBuffer("select chargs_sp_id as productid,dist_domain_flag as service")
                                    .append(" from charges_detail where ")
                                    .append(attrExpression.getExpression().replaceAll("sp","group_id"));
                            List<Map<String ,Object>> splist = oraclejdbc.queryForList(expressSql.toString());
                            for(int j = 0; j<splist.size();j++) {
                                Map<String, Object> sp = splist.get(j);
                                if("1".equals(sp.get("SERVICE").toString())) {
                                    spsql_cucc.add(sp.get("PRODUCTID").toString());
                                } else if("2".equals(sp.get("SERVICE").toString())) {
                                    spsql_cmcc.add(sp.get("PRODUCTID").toString());
                                } else if("3".equals(sp.get("SERVICE").toString())) {
                                    spsql_ctcc.add(sp.get("PRODUCTID").toString());
                                }
                            }
                            AttrExpression attrExpression_cucc = new AttrExpression(filter);
                            attrExpression_cucc.setValue(String.join(",", spsql_cucc));
                            inConditionList_cucc.add(attrExpression_cucc);
                            AttrExpression attrExpression_cmcc = new AttrExpression(filter);
                            attrExpression_cmcc.setValue(String.join(",", spsql_cmcc));
                            inConditionList_cmcc.add(attrExpression_cmcc);
                            AttrExpression attrExpression_ctcc = new AttrExpression(filter);
                            attrExpression_ctcc.setValue(String.join(",", spsql_ctcc));
                            inConditionList_ctcc.add(attrExpression_ctcc);
                        } else {
                            AttrExpression attrExpressioncucc = new AttrExpression(filter);
                            inConditionList_cucc.add(attrExpressioncucc);
                            AttrExpression attrExpressioncmcc = new AttrExpression(filter);
                            inConditionList_cmcc.add(attrExpressioncmcc);
                            AttrExpression attrExpressionctcc = new AttrExpression(filter);
                            inConditionList_ctcc.add(attrExpressionctcc);
                        }
                    }
                }
                outCondition = String.join(switchstr,
                        outConditionList.stream().map(attr->attr.getExpression()).collect(Collectors.toList()));
                inCondition_cucc = String.join(switchstr,
                        inConditionList_cucc.stream().map(attrcucc->attrcucc.getExpression()).collect(Collectors.toList()));
                inCondition_cmcc = String.join(switchstr,
                        inConditionList_cmcc.stream().map(attrcmcc->attrcmcc.getExpression()).collect(Collectors.toList()));
                inCondition_ctcc = String.join(switchstr,
                        inConditionList_ctcc.stream().map(attrctcc->attrctcc.getExpression()).collect(Collectors.toList()));
            }
            if(!"".equals(inCondition_cucc)) {
                actionsql_iptv.append(" and (").append(inCondition_cucc).append(")");
            }
            if(!"".equals(inCondition_cmcc)) {
                actionsql_cmcc.append(" and (").append(inCondition_cmcc).append(")");
            }
            if(!"".equals(inCondition_ctcc)) {
                actionsql_ctcc.append(" and (").append(inCondition_ctcc).append(")");
            }
            if("count".equals(result)||"ordercount".equals(result)||"cancelcount".equals(result)) {
                actionsql_iptv.append(")t group by userid");
                actionsql_cmcc.append(")t group by userid");
                actionsql_ctcc.append(")t group by userid");
            } else if("times".equals(result)) {
                actionsql_iptv.append(")s)t group by userid");
                actionsql_cmcc.append(")s)t group by userid");
                actionsql_ctcc.append(")s)t group by userid");
            }
            // 根据属性中的运营商选择情况优化SQL
            List<String> actionsqlList = new ArrayList<>();
            if(serviceFilterMap.get("cucc")) {
                actionsqlList.add(actionsql_iptv.toString());
            }
            if(serviceFilterMap.get("cmcc")) {
                actionsqlList.add(actionsql_cmcc.toString());
            }
            if(serviceFilterMap.get("ctcc")) {
                actionsqlList.add(actionsql_ctcc.toString());
            }
            if(actionsqlList.size()>0) {
                actionsql = actionsql.append(String.join(" union all ", actionsqlList));
            }
            if(!"".equals(outCondition)) {
                alluserSql.append(" where ").append(outCondition);
            }
            // 拼写外层查询条件
            did = did.equals("yes")?"not":"";
            if(!"top".equals(relation)) { // 不是TOP N的情况
                sql.append("select A.userid,A.userserviceid as type,B.").append(result).append(" from (").append(alluserSql)
                        .append(" ) A left join (").append(actionsql)
                        .append(" ) B on (A.userid = B.userid and A.userserviceid = B.groupid)");
                sql.append("where ").append(result).append(" is ").append(did).append(" null ");
                if("not".equals(did)) {
                    if(value.contains("@")) { // 区间的情况
                        sql.append("and ").append(result).append(" between ").append(value.split("@")[0])
                                .append(" and ").append(value.split("@")[1]);
                    } else {
                        sql.append("and ").append(result).append(" ").append(relation).append(" ").append(value);
                    }
                }
            } else { // TOP N的情况
                String[] valueArray = value.split("@");
                String order = valueArray[0].equals("back")?"desc":"";
                int start = 0;
                int end = Integer.valueOf(valueArray[valueArray.length-2]);
                if(valueArray.length == 4) {
                    start = Integer.valueOf(valueArray[1]);
                }
                StringBuffer rankSql = new StringBuffer("");
                rankSql.append("select A.userid,A.userserviceid,rank() over(order by ").append(result).append(" ")
                        .append(order)
                        .append(") as rank from (").append(alluserSql).append(") A left join (").append(actionsql)
                        .append(" ) B on (A.userid = B.userid and A.userserviceid = B.groupid)");
                rankSql.append(" where ").append(result).append(" is ").append(did).append(" null");
                if(value.contains("ratio")) { // 百分比
                    sql.append("select userid,userserviceid as type from (").append(rankSql)
                            .append(") C,(select count(*) as usercount from (")
                            .append(rankSql).append(")S) D where C.rank between ").append(start)
                            .append("/100*D.usercount and ")
                            .append(end).append("/100*D.usercount");
                } else { // 名次
                    sql.append("select userid,userserviceid as type from (").append(rankSql)
                            .append(") C where C.rank between ").append(start)
                            .append(" and ").append(end);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sql.toString();
    }
    public void setCustomdate(Customdate customdate) {
        this.customdate = customdate;
    }

    public void setDid(String did) {
        this.did = did;
    }

    public void setOper(String oper) {
        this.oper = oper;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setFilters(String filters) {
        this.filters = filters;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public void setStartday(String startday) {
        this.startday = startday;
    }

    public void setEndday(String endday) {
        this.endday = endday;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public void setServiceFilterMap(Map<String, Boolean> serviceFilterMap) {
        this.serviceFilterMap = serviceFilterMap;
    }

    public Customdate getCustomdate() {
        return customdate;
    }

    public String getDid() {
        return did;
    }

    public String getOper() {
        return oper;
    }

    public String getResult() {
        return result;
    }

    public String getRelation() {
        return relation;
    }

    public String getValue() {
        return value;
    }

    public String getFilters() {
        return filters;
    }

    public String getDate() {
        return date;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getStartday() {
        return startday;
    }

    public String getEndday() {
        return endday;
    }

    public String getLength() {
        return length;
    }

    public Map<String, Boolean> getServiceFilterMap() {
        return serviceFilterMap;
    }

    public static void main(String[] args) {
        String filters = "{\"filtersswitch\":\"and\",\"list\":[{\"id\":\"mbeDojDlLM\",\"attr\":\"sp\",\"relation\":\"=\",\"value\":\"49,47,48\",\"type\":\"event\",\"valueType\":\"string\",\"index\":\"1\"}]}";
        try {
            JSONObject jsonObject = new JSONObject(filters);
            String switchstr = jsonObject.getString("filtersswitch");
            JSONArray jsonList = jsonObject.getJSONArray("list");
            for (int i = 0; i < jsonList.length(); i++) {
                JSONObject filter = (JSONObject) jsonList.get(i);
                AttrExpression attrExpression = new AttrExpression(filter);
                //System.out.println(attrExpression.getExpression().replaceAll("sp","group_id"));

                StringBuffer expressSql = new StringBuffer("select chargs_sp_id as productid,dist_domain_flag as service")
                        .append(" from charges_detail where ").append(attrExpression.getExpression().replaceAll("sp","group_id"));
                System.out.println(expressSql.toString());
               // List<Map<String ,String>> list = oraclejdbc.queryForList(expressSql.toString());
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
