package com.viewstar.service;

import org.springframework.dao.DataAccessException;

import java.util.List;
import java.util.Map;

public interface SparkSqlServiceDao {
    public List<Map<String, Object>> queryForList(String database, String sql) throws DataAccessException;

    public List<Map<String, Object>> queryForList(String sql) throws DataAccessException;

    public void execute(String sql)throws DataAccessException;
}
