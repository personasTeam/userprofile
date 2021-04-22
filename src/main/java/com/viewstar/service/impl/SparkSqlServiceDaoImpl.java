package com.viewstar.service.impl;

import com.viewstar.service.SparkSqlServiceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@Repository
public class SparkSqlServiceDaoImpl implements SparkSqlServiceDao{
    @Autowired
    @Qualifier("sparkSqlJdbcTemplate")
    private JdbcTemplate sparkSqlJdbcTemplate;

    public List<Map<String, Object>> queryForList(String sql) throws DataAccessException {
        List<Map<String, Object>> maps = sparkSqlJdbcTemplate.queryForList(sql);
        return maps;
    }

    public List<Map<String, Object>> queryForList(String databaseName,
                                                  String sql) throws DataAccessException {
        return query(databaseName, sql, getColumnMapRowMapper());
    }

    protected RowMapper<Map<String, Object>> getColumnMapRowMapper() {
        return new ColumnMapRowMapper();
    }

    public <T> List<T> query(String databaseName, String sql,
                             RowMapper<T> rowMapper) throws DataAccessException {
        return query(databaseName, sql, new RowMapperResultSetExtractor<T>(
                rowMapper));
    }

    public <T> T query(final String databaseName, final String sql,
                       final ResultSetExtractor<T> rse) throws DataAccessException {

        class QueryStatementCallback implements StatementCallback<T>, SqlProvider {
            QueryStatementCallback() {
            }

            @Nullable
            public T doInStatement(Statement stmt) throws SQLException {
                ResultSet rs = null;

                Object var3;
                try {
                    rs = stmt.executeQuery(sql);
                    var3 = rse.extractData(rs);
                } finally {
                    JdbcUtils.closeResultSet(rs);
                }

                return (T) var3;
            }

            public String getSql() {
                return sql;
            }
        }
        return sparkSqlJdbcTemplate.execute(new QueryStatementCallback());
    }

    @Override
    public void execute(String sql) throws DataAccessException {
        sparkSqlJdbcTemplate.execute(sql);
    }
}
