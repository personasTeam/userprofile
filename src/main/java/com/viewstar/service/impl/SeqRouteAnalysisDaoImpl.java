package com.viewstar.service.impl;

import com.viewstar.service.SeqRouteAnalysisDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Repository
public class SeqRouteAnalysisDaoImpl implements SeqRouteAnalysisDao {

    @Autowired
    @Qualifier("oracleJdbcTemplate")
    private JdbcTemplate oracleJdbcTemplate;

    @Override
    public int saveSeqRoute(String id, String jsoninfo) {
        String sql = "insert into BEHAVIOR_SEQ(ID,JSONINFO) VALUES(?,?)";
        int update = oracleJdbcTemplate.update(sql, id, jsoninfo);
        return update;
    }

    @Override
    public int updateSeqRoute(String id, String jsoninfo) {
        String sql = "update BEHAVIOR_SEQ set JSONINFO = ? where id = ?";
        int update = oracleJdbcTemplate.update(sql, jsoninfo,id);
        return update;
    }

    @Override
    public String getID() {
        String sql = "select SEQ_BEHAVIOR_SEQ.NEXTVAL as ID FROM DUAL";
        Map<String, Object> stringObjectMap = oracleJdbcTemplate.queryForMap(sql);
        return stringObjectMap.get("ID").toString();
    }
}
