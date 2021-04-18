package com.viewstar.model;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用户行为队列.
 */
public class Sequence extends Expression{
    private String id; // 条件id
    private String index; // 条件索引
    private String value; // 查询表名
    private String filters; // 小漏斗json
    private int listIndex; // 序列索引
    private String date; // 查询时间
    private String startTime; // 查询条件的开始时间
    private String endTime; // 查询条件的结束时间
    private String startday;
    private String endday;
    private Map<String, Boolean> serviceFilterMap;

    /**
     * 构造函数.
     * @param map 参数map
     * @param date 查询时间
     * @param listIndex 序列索引
     * @param startTime 查询条件的开始时间
     * @param endTime 查询条件的结束时间
     * @throws JSONException 解析异常
     */
    public Sequence(JSONObject map, String date, int listIndex, String startTime, String endTime, String startday,
                    String endday, Map<String, Boolean> serviceFilterMap) throws JSONException {
        super(map.get("id").toString(), map.get("index").toString());
        this.value = map.get("value").toString();
        if(map.has("filters")) {
            this.filters = map.get("filters").toString();
        } else {
            this.filters = "";
        }
        this.listIndex = listIndex;
        this.date = date;
        this.startTime = startTime;
        this.endTime = endTime;
        this.startday = startday;
        this.endday = endday;
        this.serviceFilterMap = serviceFilterMap;
    }

    /**
     * 拼接表达式.
     * @return 表达式string
     */
    @Override
    public String getExpression() {
        StringBuffer sql = new StringBuffer("");
        StringBuffer cucc_sql = new StringBuffer("");
        StringBuffer cmcc_sql = new StringBuffer("");
        StringBuffer ctcc_sql = new StringBuffer("");
        List<String> sqlList = new ArrayList<>();
        try {
            // 解析漏斗过滤条件
            String outCondition = "";
            List<AttrExpression> outConditionList = new ArrayList<AttrExpression>();
            String inCondition = "";
            List<AttrExpression> inConditionList = new ArrayList<AttrExpression>();
            if (!"".equals(filters)) {
                JSONObject jsonObject = new JSONObject(filters);
                String switchstr = jsonObject.getString("filtersswitch");
                JSONArray jsonList = jsonObject.getJSONArray("list");
                for (int i = 0; i < jsonList.length(); i++) {
                    JSONObject filter = (JSONObject) jsonList.get(i);
                    AttrExpression attrExpression = new AttrExpression(filter);
                    if (!"event".equals(filter.get("type"))) {
                        outConditionList.add(attrExpression);
                    } else {
                        inConditionList.add(attrExpression);
                    }
                }
                outCondition = String.join(switchstr, outConditionList.stream()
                        .map(attr -> attr.getExpression()).collect(Collectors.toList()));
                inCondition = String.join(switchstr, inConditionList.stream()
                        .map(attr -> attr.getExpression()).collect(Collectors.toList()));
            }
            sql.append("select A.userid, B.ptime, 'route").append(listIndex).append("' as behaviorseq,")
                    .append("starttime as behaviortime, A.userserviceid as type from (")
                    .append("select userid, userserviceid from iptv.userprofilepar").append(date);
            if(!"".equals(outCondition)) {
                sql.append(" where ").append(outCondition);
            }
            sql.append(" ) A left join (");
            cucc_sql.append("select userid, starttime,'1' as type,ptime from iptv.").append(value)
                    .append(" where ptime between ").append(startday).append(" and ").append(endday)
                    .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                    .append(this.endTime).append(" as bigint))")
                    .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                    .append(this.endTime).append(" as bigint))")
                    .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                    .append(this.endTime).append(" as bigint)))");
            cmcc_sql.append("select userid, starttime,'2' as type,ptime from cmcc.").append(value)
                    .append(" where ptime between ").append(startday).append(" and ").append(endday)
                    .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                    .append(this.endTime).append(" as bigint))")
                    .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                    .append(this.endTime).append(" as bigint))")
                    .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                    .append(this.endTime).append(" as bigint)))");
            ctcc_sql.append("select userid, starttime,'3' as type,ptime from ctcc.").append(value)
                    .append(" where ptime between ").append(startday).append(" and ").append(endday)
                    .append(" and ((starttime >= cast(").append(this.startTime).append(" as bigint) and starttime <= cast(")
                    .append(this.endTime).append(" as bigint))")
                    .append(" or ").append("(endtime >= cast(").append(this.startTime).append(" as bigint) and endtime <= cast(")
                    .append(this.endTime).append(" as bigint))")
                    .append(" or ").append("(starttime < cast(").append(this.startTime).append(" as bigint) and endtime > cast(")
                    .append(this.endTime).append(" as bigint)))");
            if(!"".equals(inCondition)) {
                cucc_sql.append(" and (").append(inCondition).append(")");
                cmcc_sql.append(" and (").append(inCondition).append(")");
                ctcc_sql.append(" and (").append(inCondition).append(")");
            }
            // 根据属性中的运营商选择情况优化SQL
            if(serviceFilterMap.get("cucc")) {
                sqlList.add(cucc_sql.toString());
            }
            if(serviceFilterMap.get("cmcc")) {
                sqlList.add(cmcc_sql.toString());
            }
            if(serviceFilterMap.get("ctcc")) {
                sqlList.add(ctcc_sql.toString());
            }
            if(sqlList.size()>0) {
                sql.append(String.join(" union all ", sqlList));
            }
            sql.append(") B on (A.userid=B.userid and A.userserviceid=B.type) where ptime is not null");
        } catch (Exception e) {
            System.out.println("解析失败");
            e.printStackTrace();
        }
        return sql.toString();
    }

}
