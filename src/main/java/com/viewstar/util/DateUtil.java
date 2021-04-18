package com.viewstar.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 日期类工具函数.
 */
public class DateUtil {
    /**
     * 得到两个日期之间的日期列表
     * @param beginTime 开始日期
     * @param endTime 结束日期
     * @return 日期列表yyyy-MM-dd
     * @throws Exception
     */
    public List<String> getTimeSpan(String beginTime, String endTime)
            throws Exception {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        List<String> list = new ArrayList<String>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(sf.parse(beginTime));
        while (!sf.parse(endTime).before(cal.getTime())) {
            list.add(sf.format(cal.getTime()));
            cal.add(cal.DATE, 1);
        }
        return list;
    }

    /**
     * 得到两个日期之间的日期列表
     * @param beginTime 开始日期
     * @param endTime 结束日期
     * @return 日期列表yyyy-MM-dd
     * @throws Exception
     */
    public List<String> getTimeSpanString(String beginTime, String endTime, String commonStr)
            throws Exception {
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");
        List<String> list = new ArrayList<String>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(sf.parse(beginTime));
        while (!sf.parse(endTime).before(cal.getTime())) {
            list.add(commonStr + sf.format(cal.getTime()));
            cal.add(cal.DATE, 1);
        }
        return list;
    }

    /**
     * 指定两天相差的天数
     * @param date1 开始日期
     * @param date2 结束日期
     * @return 两天相差的天数
     */
    public int getDateDiff(String date1, String date2) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        int diff = 0;
        try {
            diff = (int) ((sf.parse(date2).getTime() - sf.parse(date1)
                    .getTime()) / 1000 / 3600 / 24) + 1;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return diff;
    }

    /**
     * 格式化指定日期
     *
     * @param date 日期
     * @param targetFormat 要转换的日期格式
     * @return 格式化的指定日期
     * @throws Exception
     */
    public String formatDate(Date date, String targetFormat) throws Exception {
        SimpleDateFormat target = new SimpleDateFormat(targetFormat);
        String newdate = "";
        try {
            newdate = target.format(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newdate;
    }

    /**
     * 字符串类型转date.
     * @param today 日期，格式：yyyy-mm-dd
     * @return 日期类型
     */
    public Date getDateByString(String today) {
        Date date = new Date();
        try {
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
            date = sdf2.parse(today);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 获得日期是星期几.
     * @param today 日期 格式：yyyy-mm-dd
     * @return 星期一--1；星期二---2..星期日--7
     */
    public int getDayOfWeek(String today){
        Date date = getDateByString(today);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int w = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if(w==0) {
            w = 7;
        }
        return w;
    }

    /**
     * 获得某日期所在周的周一日期
     * @param today 指定日期
     * @return 指定日期所在周的周一.
     */
    public Date getFirstDayOfWeek(String today) {
        Date date = getDateByString(today);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        // 判断要计算的日期是否是周日，如果是则减一天计算周六的，否则会出问题，计算到下一周去了
        int dayWeek = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
        if (1 == dayWeek) {
            cal.add(Calendar.DAY_OF_MONTH, -1);
        }
        cal.setFirstDayOfWeek(Calendar.MONDAY);// 设置一个星期的第一天，按中国的习惯一个星期的第一天是星期一
        int day = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
        cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day);// 根据日历的规则，给当前日期减去星期几与一个星期第一天的差值
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return cal.getTime();
    }

    /**
     * 指定日期相隔n天的日期.
     * @param date 指定日期.
     * @param add 相隔天数.
     * @return 结果.
     */
    public Date addDays(Date date, int add) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(cal.DATE, add);
        return cal.getTime();
    }

    /**
     * 获取两个时间相差多少秒.
     * @param starttime 开始时间 yyyyMMddhhmmss
     * @param endtime 结束时间 yyyyMMddhhmmss
     * @return 相差的秒数.
     */
    public long getTimeSpanSeconds(String starttime, String endtime) {
        long span = 0;
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddhhmmss");
        Date startdate = new Date();
        Date enddate = new Date();
        try {
            startdate = sdf2.parse(starttime);
            enddate = sdf2.parse(endtime);
        } catch (Exception e) {
            e.printStackTrace();
        }
        span = (enddate.getTime() - startdate.getTime())/1000 + 1;
        return span;
    }
    public static void main(String[] args) throws Exception {
        DateUtil dateUtil = new DateUtil();
        /*dateUtil.getFirstDayOfWeek("2020-02-16");
        int starttimeOfWeek = 6;
        String starthh = "03";
        String startmm = "20";
        String startss = "20";
        String endhh = "03";
        String endmm = "20";
        String endss = "20";
        String startdate="2020-02-16";
        String enddate = "2020-02-18";
        int diffDay = dateUtil.getTimeSpan(startdate,enddate).size()-1;
        String starttime = "concat(regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)),'-',''),'"+starthh+"','"+startmm+"','"+startss+"')";
        String endtime = "concat(regexp_replace('#today#','-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
        starttime = "concat(regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)+7),'-',''),'"+starthh+"','"+startmm+"','"+startss+"')";
        endtime = "concat(regexp_replace(date_sub('#today#',pmod(datediff('#today#','2008-01-07'),7)+1),'-',''),'"+endhh+"','"+endmm+"','"+endss+"')";
        starttime = "concat(substring('#today#',0,4),'"+"0101','"+starthh+"','"+startmm+"','"+startss+"')";
        starttime = "concat(date_format(add_months(concat(substring('#today#',0,4),'-01-01'), -12), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
        endtime = "concat(date_format(add_months(concat(substring('#today#',0,4),'-12-31'), -12), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
        starttime = "concat(date_format(date_sub('#today#', 7), 'yyyyMMdd'),'"+starthh+"','"+startmm+"','"+startss+"')";
        endtime = "concat(date_format(date_sub('#today#', 1), 'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
        starttime = "concat(date_format(date_sub(concat(substring('#today#',0,8),'01'),1),'yyyyMM01'),'"+starthh+"','"+startmm+"','"+startss+"')";
        endtime = "concat(date_format(date_sub(concat(substring('#today#',0,8),'01'),1),'yyyyMMdd'),'"+endhh+"','"+endmm+"','"+endss+"')";
        starttime = "concat(substring(regexp_replace('#today#','-',''),0,6),'01','"+starthh+"','"+startmm+"','"+startss+"')";
        System.out.println(starttime);
        System.out.println(endtime);*/
        System.out.println(dateUtil.getTimeSpanSeconds("20200218101700","20200218101700"));
    }
}
