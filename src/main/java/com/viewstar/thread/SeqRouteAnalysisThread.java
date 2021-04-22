package com.viewstar.thread;

import com.alibaba.fastjson.JSON;
import com.viewstar.service.SparkSqlServiceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 用户序列服务类
 */
@Component
public class SeqRouteAnalysisThread {
    @Autowired
    private SparkSqlServiceDao sparkSqlServiceDao;


    /**
     * 生成用户序列查询语句
     * @param json 由前端传入逻辑语句
     * @return 可执行用户序列SQL
     */
    //    @Async
    public String asyncSaveSeqRouteData(String json) {
        List<String> parseSqlList = new ArrayList<>();
        //解析JSON
        Map jsonMap = JSON.parseObject(json, Map.class);
        String sqlList = jsonMap.get("list").toString();
        List seqList = JSON.parseObject(sqlList, List.class);
        //拼写json中的LIST标签包含的SQL
        for (int i = 0; i < seqList.size(); i++) {
            String seq = seqList.get(i).toString();
            Map seqMap = JSON.parseObject(seq, Map.class);
            List s = JSON.parseObject(seqMap.get("sql").toString(), List.class);
            //生成behaviorseq字符串进行匹配
            List<String> behaviorseqList = new ArrayList<>();
            for (int j = 0; j < s.size(); j++) {
                int temp = j+1;
                behaviorseqList.add("route"+temp);
            }

            StringBuffer sqlBuf = new StringBuffer("");
            sqlBuf.append("select userid,type from (");
            sqlBuf.append("select * from(\n" +
                "select userid,type,regexp_replace(concat_ws(',',sort_array(collect_list(concat_ws(':',lpad(cast(rn as string),5,'0'),cast(behaviorseq as string))))),'\\\\d+\\:','') as behaviorseq from(\n" +
                "select userid,type,behaviorseq,row_number() over (partition by userid,type order by behaviortime) as rn from( ");
            sqlBuf.append(String.join(" union all ",s));
            sqlBuf.append(")a)b\n" +
                    "group by userid,type)c\n");
            sqlBuf.append("where ");
            StringBuffer locateBuf = new StringBuffer("");

            //用户序列排序相关语句
            for (int j = s.size(); j > 0; j--) {

                if(j == 1){
                    locateBuf.append("locate('route"+(j)+"',behaviorseq)");
                }else{
                    locateBuf.append("locate('route"+(j)+"',behaviorseq,");
                }
            }

            for (int j = 0; j < s.size()-1; j++) {
                locateBuf.append(")");
            }

            sqlBuf.append(locateBuf);
            sqlBuf.append(">0 ");
            sqlBuf.append(" and userid is not null ");
            sqlBuf.append(")a"+i);
            parseSqlList.add(sqlBuf.toString());
        }

        //根据switch标签拼接SQL
        String sql = "";
        String aswitch = jsonMap.get("switch").toString();
        if("or".equals(aswitch)){
             sql = sql + String.join("\n union \n",parseSqlList);
        }else if("and".equals(aswitch)){
             sql = sql + String.join("\nintersect\n",parseSqlList);
        }
        return sql;
    }


    private String jsonToObject(String json) {

        Map jsonMap = JSON.parseObject(json, Map.class);
        String sqlList = jsonMap.get("list").toString();
        List list = JSON.parseObject(sqlList, List.class);
        for (int i = 0; i < list.size(); i++) {
            String s = list.get(i).toString();
            Map sqlMap = JSON.parseObject(s, Map.class);
            List sql = JSON.parseObject(sqlMap.get("sql").toString(), List.class);
        }
        return "";
    }

    public static void main(String[] args) {
        SeqRouteAnalysisThread a = new SeqRouteAnalysisThread();
        String json = "{\"list\":[{\"sql\":[\"select A.userid, B.ptime, 'route1' as behaviorseq,starttime as behaviortime, A.userserviceid as type from (select userid, userserviceid from iptv.userprofilepar#date# ) A left join (select userid, starttime,'1' as type,ptime from iptv.pagedataparquet where ptime between 20191109 and 20191225 and (((pagename like '%美食盛宴%'))) union all select userid, starttime,'2' as type,ptime from cmcc.pagedataparquet where ptime between 20191109 and 20191225 and (((pagename like '%美食盛宴%'))) union all select userid, starttime,'3' as type,ptime from ctcc.pagedataparquet where ptime between 20191109 and 20191225 and (((pagename like '%美食盛宴%')))) B on (A.userid=B.userid and A.userserviceid=B.type) where ptime is not null\",\"select A.userid, B.ptime, 'route2' as behaviorseq,starttime as behaviortime, A.userserviceid as type from (select userid, userserviceid from iptv.userprofilepar#date# ) A left join (select userid, starttime,'1' as type,ptime from iptv.voduserparquet where ptime between 20191109 and 20191225 and (((cmscategory = '20001110000000000000000000013916'))) union all select userid, starttime,'2' as type,ptime from cmcc.voduserparquet where ptime between 20191109 and 20191225 and (((cmscategory = '20001110000000000000000000013916'))) union all select userid, starttime,'3' as type,ptime from ctcc.voduserparquet where ptime between 20191109 and 20191225 and (((cmscategory = '20001110000000000000000000013916')))) B on (A.userid=B.userid and A.userserviceid=B.type) where ptime is not null\",\"select A.userid, B.ptime, 'route3' as behaviorseq,starttime as behaviortime, A.userserviceid as type from (select userid, userserviceid from iptv.userprofilepar#date# ) A left join (select userid, starttime,'1' as type,ptime from iptv.orderchargeparquet where ptime between 20191109 and 20191225 and (((sp like '%gdprdt149%'))or((sp like '%gdprdtbstxhsejdb@201%'))) union all select userid, starttime,'2' as type,ptime from cmcc.orderchargeparquet where ptime between 20191109 and 20191225 and (((sp like '%gdprdt149%'))or((sp like '%gdprdtbstxhsejdb@201%'))) union all select userid, starttime,'3' as type,ptime from ctcc.orderchargeparquet where ptime between 20191109 and 20191225 and (((sp like '%gdprdt149%'))or((sp like '%gdprdtbstxhsejdb@201%')))) B on (A.userid=B.userid and A.userserviceid=B.type) where ptime is not null\"]}],\"switch\":\"and\"}";
        System.out.println(json);
        String s = a.asyncSaveSeqRouteData(json);
        System.out.println(s);


    }
}
