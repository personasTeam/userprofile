package com.viewstar.model;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 行为队列表达式.
 */
public class SequenceExpression {
    private Customdate customdate; // 时间框
    private String startTime; // 开始时间
    private String endTime; // 结束时间
    private JSONArray list; // 行为list
    private String date;
    private String startday; // 开始日期
    private String endday; // 结束日期
    private Map<String, Boolean> serviceFilterMap;

    public SequenceExpression(JSONObject map, String date,Map<String, Boolean> serviceFilterMap)  throws Exception {
        JSONObject jsonObject = new JSONObject(map.get("customDate").toString());
        this.customdate = new Customdate(jsonObject);
        Map<String, Object> customMap = this.customdate.getTimeSpanMap();
        this.startTime = customMap.get("starttime").toString();
        this.endTime = customMap.get("endtime").toString();
        this.startday = customMap.get("startday").toString();
        this.endday = customMap.get("endday").toString();
        this.list = (JSONArray) map.get("list");
        this.date = date;
        this.serviceFilterMap = serviceFilterMap;
    }

    public List<String> getExpression() throws JSONException {
        List<String> expressionList = new ArrayList<String>();
        for(int i = 0; i < list.length(); i++) {
            JSONObject s = (JSONObject) list.get(i);
            try {
                Sequence sequence = new Sequence(s, date, i+1, startTime, endTime, startday, endday, serviceFilterMap);
                expressionList.add(sequence.getExpression());
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return expressionList;
    }
}
