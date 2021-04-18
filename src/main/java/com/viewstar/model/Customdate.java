package com.viewstar.model;

import com.viewstar.util.DateUtil;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 动态时间控件
 */
public class Customdate {
    /**
     * 动态时间框类型：当type=specific（选择具体时间段）；当type=custom（设置动态时间段）
     * today:今日
     * yesterday:昨日
     * thisWeek:本周
     * lastWeek:上周
     * thisMonth:本月
     * lastMonth:上月
     * thisYear:本年
     * lastYear:去年
     * last7Day:过去 7 天
     * last30Day:过去 30 天
     * last90Day:过去 90 天
     * specific:选择具体时间段
     * custom:设置动态时间段
     */
    private String type;
    /**
     * 开始时间 时间格式："yyyy-MM-DD hh:MI:SS"；type=custom，时间格式为:"yyyy-MM-DD"
     */
    private String startTime;
    /**
     * 结束时间 时间格式："yyyy-MM-DD hh:MI:SS"；type=custom，时间格式为:"yyyy-MM-DD"
     */
    private String endTime;
    /**
     * 开始日期的规则，字段包含以下情况
     * time：具体时间,取startTime得值
     * range：过去N天,取customStartInputValue的值
     */
    private String customStartTimeBtn;
    /**
     * 结束日期的规则，字段包含以下情况
     * today:今日
     * yesterday：昨日
     * range：过去N天,取customEndInputValue的值
     */
    private String customEndTimeBtn;
    /**
     * 开始日期动态时间值：过去1天--1；过去n天--n
     */
    private int customStartInputValue;
    /**
     * 结束日期动态时间值：过去1天--1；过去n天--n
     */
    private int customEndInputValue;
    /**
     * 日期工具类
     */
    private DateUtil dateUtil;

    /**
     * 初始化参数.
     * @param map 参数
     * @throws JSONException
     */
    public Customdate (JSONObject map) throws JSONException {
        System.out.println("Customdate:"+map);
        if(map.has("type")) {
            this.type = map.get("type").toString();
        }
        if(map.has("startTime")) {
            this.startTime = map.get("startTime").toString();
        }
        if(map.has("endTime")) {
            this.endTime = map.get("endTime").toString();
        }
        if(map.has("customStartTimeBtn")) {
            this.customStartTimeBtn = map.get("customStartTimeBtn").toString();
        }
        if(map.has("customEndTimeBtn")) {
            this.customEndTimeBtn = map.get("customEndTimeBtn").toString();
        }
        if(map.has("customStartInputValue")) {
            this.customStartInputValue = Integer.valueOf(map.get("customStartInputValue").toString());
        }
        if(map.has("customEndInputValue")) {
            this.customEndInputValue = Integer.valueOf(map.get("customEndInputValue").toString());
        }
    }



    /**
     * 根据逻辑转义开始时间和结束时间字符串
     * @return map (starttime,"") (endtime,"")
     */
    public Map<String, Object> getTimeSpanMap() throws Exception {
        Map<String, Object> timeSpan = new HashMap<String, Object>();
        // 组合后的开始时间和结束时间 格式：yyyyMMddHHmmss
        String starttime = "";
        String endtime = "";
        String startday = "";
        String endday = "";
        // 开始时间和结束时间的日期 格式：yyyy-MM-dd
        String startdate = "";
        String enddate = "";
        // 开始时间的小时，分，秒
        String starthh = "00";
        String startmm = "00";
        String startss = "00";
        // 结束时间的小时，分，秒
        String endhh = "00";
        String endmm = "00";
        String endss = "00";
        // 拆分starttime和endtime
        if(!"custom".equals(type)) {
            startdate = this.startTime.split(" ")[0];
            starthh = this.startTime.split(" ")[1].split(":")[0];
            startmm = this.startTime.split(" ")[1].split(":")[1];
            startss = this.startTime.split(" ")[1].split(":")[2];
            enddate = this.endTime.split(" ")[0];
            endhh = this.endTime.split(" ")[1].split(":")[0];
            endmm = this.endTime.split(" ")[1].split(":")[1];
            endss = this.endTime.split(" ")[1].split(":")[2];
        } else {
            startdate = starttime;
            enddate = endtime;
        }
        /**
         * 开始解析
         * yesterday:昨日
         * thisWeek:本周
         * lastWeek:上周
         * thisMonth:本月
         * lastMonth:上月
         * thisYear:本年
         * lastYear:去年
         * last7Day:过去 7 天
         * last30Day:过去 30 天
         * last90Day:过去 90 天
         */
        if("specific".equals(this.type)) { // 选择具体时间段 type = specific
            starttime = new StringBuffer("'").append(startdate.replaceAll("-",""))
                    .append(starthh).append(startmm).append(startss).append("'").toString();
            endtime = new StringBuffer("'").append(enddate.replaceAll("-",""))
                    .append(endhh).append(endmm).append(endss).append("'").toString();
            startday = "'"+ startdate.replaceAll("-","")+"'";
            endday = "'"+ enddate.replaceAll("-","")+"'";
        } else if("today".equals(this.type)) { // 选择今日 type = today
            starttime = "concat(regexp_replace('#today#','-',''),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(regexp_replace('#today#','-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "regexp_replace('#today#','-','')";
            endday = startday;
        } else if("yesterday".equals(this.type)) { // 选择昨日 type = yesterday
            starttime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "date_format(date_sub('#today#', 1), 'yyyyMMdd')";
            endday = startday;
        } else if("thisWeek".equals(this.type)) { // 选择本周 type = thisWeek
            starttime = "concat(regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)),'-',''),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(regexp_replace('#today#','-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)),'-','')";
            endday = "regexp_replace('#today#','-','')";
        } else if("lastWeek".equals(this.type)) { // 选择上周 type = lastWeek
            starttime = "concat(regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)+7),'-',''),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)+1),'-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)+7),'-','')";
            endday = "regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)+1),'-','')";
        } else if("thisMonth".equals(this.type)) { // 选择本月 type = thisMonth
            starttime = "concat(substring(regexp_replace('#today#','-',''),0,6),'01','"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(regexp_replace('#today#','-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "concat(substring(regexp_replace('#today#','-',''),0,6),'01')";
            endday = "regexp_replace('#today#','-','')";
        } else if("lastMonth".equals(this.type)) { // 选择上月 type = lastMonth
            starttime = "concat(date_format(date_sub(concat(substring('#today#',0,8),'01'),1),'yyyyMM01'),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(date_format(date_sub(concat(substring('#today#',0,8),'01'),1),'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "date_format(date_sub(concat(substring('#today#',0,8),'01'),1),'yyyyMM01')";
            endday = "date_format(date_sub(concat(substring('#today#',0,8),'01'),1),'yyyyMMdd')";
        } else if("thisYear".equals(this.type)) { // 选择本年 type = thisYear
            starttime = "concat(substring('#today#',0,4),'0101','"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(regexp_replace('#today#','-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "concat(substring('#today#',0,4),'0101')";
            endday = "regexp_replace('#today#','-','')";
        } else if("lastYear".equals(this.type)) { // 选择去年 type = lastYear
            starttime = "concat(date_format(add_months(concat(substring('#today#',0,4),'-01-01'), -12), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(date_format(add_months(concat(substring('#today#',0,4),'-12-31'), -12), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "date_format(add_months(concat(substring('#today#',0,4),'-01-01'), -12), 'yyyyMMdd')";
            endday = "date_format(add_months(concat(substring('#today#',0,4),'-12-31'), -12), 'yyyyMMdd')";
        } else if("last7Day".equals(this.type)) { // 选择过去7天
            starttime = "concat(date_format(date_sub('#today#', 7), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "date_format(date_sub('#today#', 7), 'yyyyMMdd')";
            endday = "date_format(date_sub('#today#', 1), 'yyyyMMdd')";
        } else if("last30Day".equals(this.type)) { // 选择过去30天
            starttime = "concat(date_format(date_sub('#today#', 30), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "date_format(date_sub('#today#', 30), 'yyyyMMdd')";
            endday = "date_format(date_sub('#today#', 1), 'yyyyMMdd')";
        } else if("last90Day".equals(this.type)) { // 选择过去90天
            starttime = "concat(date_format(date_sub('#today#', 90), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
            endtime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
            startday = "date_format(date_sub('#today#', 90), 'yyyyMMdd')";
            endday = "date_format(date_sub('#today#', 1), 'yyyyMMdd')";
        } else if("custom".equals(this.type)) { // 选择设置动态时间段
            if("time".equals(this.customStartTimeBtn)) {
                starttime = startdate.replaceAll("-","") + "000000";
                startday = startdate.replaceAll("-","");
            } else if("range".equals(this.customStartTimeBtn)) {
                int range = this.customStartInputValue - 1;
                starttime = "concat(date_format(date_sub('#today#', "+range+"), 'yyyyMMdd'),'000000')";
                startday = "date_format(date_sub('#today#', "+range+"), 'yyyyMMdd')";
            }
            if("today".equals(this.customEndTimeBtn)) {
                endtime = "concat(regexp_replace('#today#','-',''),'235959')";
                endday = "regexp_replace('#today#','-','')";
            } else if("yesterday".equals(this.customEndTimeBtn)) {
                endtime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'235959')";
                endday = "date_format(date_sub('#today#', 1), 'yyyyMMdd')";
            } else if("range".equals(this.customEndTimeBtn)) {
                int range = this.customEndInputValue - 1;
                endtime = "concat(date_format(date_sub('#today#', "+range+"), 'yyyyMMdd'),'235959')";
                endday = "date_format(date_sub('#today#', "+range+"), 'yyyyMMdd')";
            }
        }
        timeSpan.put("starttime", starttime);
        timeSpan.put("endtime",endtime);
        timeSpan.put("startday", startday);
        timeSpan.put("endday", endday);
        return timeSpan;
    }


    public String getType() {
        return type;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getCustomStartTimeBtn() {
        return customStartTimeBtn;
    }

    public String getCustomEndTimeBtn() {
        return customEndTimeBtn;
    }

    public int getCustomStartInputValue() {
        return customStartInputValue;
    }

    public int getCustomEndInputValue() {
        return customEndInputValue;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public void setCustomStartTimeBtn(String customStartTimeBtn) {
        this.customStartTimeBtn = customStartTimeBtn;
    }

    public void setCustomEndTimeBtn(String customEndTimeBtn) {
        this.customEndTimeBtn = customEndTimeBtn;
    }

    public void setCustomStartInputValue(int customStartInputValue) {
        this.customStartInputValue = customStartInputValue;
    }

    public void setCustomEndInputValue(int customEndInputValue) {
        this.customEndInputValue = customEndInputValue;
    }
}
