package com.viewstar.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Created by arnold.zhu on 6/13/2017.
 */
@Configuration
public class OracleDataSource {

    private Logger logger = LoggerFactory.getLogger(OracleDataSource.class);

    @Value("${spring.datasource.oracle.url}")
    private String dbUrl;

    @Value("${spring.datasource.oracle.username}")
    private String username;

    @Value("${spring.datasource.oracle.password}")
    private String password;

    @Value("${spring.datasource.oracle.driver-class-name}")
    private String driverClassName;

    @Value("${spring.datasource.oracle.initialSize}")
    private int initialSize;

    @Value("${spring.datasource.oracle.minIdle}")
    private int minIdle;

    @Value("${spring.datasource.oracle.maxActive}")
    private int maxActive;

    @Value("${spring.datasource.oracle.maxWait}")
    private int maxWait;

    @Value("${spring.datasource.oracle.timeBetweenEvictionRunsMillis}")
    private int timeBetweenEvictionRunsMillis;

    @Value("${spring.datasource.oracle.minEvictableIdleTimeMillis}")
    private int minEvictableIdleTimeMillis;

    @Value("${spring.datasource.oracle.validationQuery}")
    private String validationQuery;

    @Value("${spring.datasource.oracle.testWhileIdle}")
    private boolean testWhileIdle;

    @Value("${spring.datasource.oracle.testOnBorrow}")
    private boolean testOnBorrow;

    @Value("${spring.datasource.oracle.testOnReturn}")
    private boolean testOnReturn;

    @Value("${spring.datasource.oracle.filters}")
    private String filters;

    @Value("${spring.datasource.oracle.logSlowSql}")
    private String logSlowSql;

    @Value("${spring.datasource.oracle.dbType}")
    private String dbType;

    @Bean(name = "oracleJdbcDataSource")
    @Qualifier("oracleJdbcDataSource")
    public DataSource dataSource() {
        DruidDataSource datasource = new DruidDataSource();
        datasource.setUrl(dbUrl);
        datasource.setUsername(username);
        datasource.setPassword(password);
        datasource.setDriverClassName(driverClassName);
        datasource.setInitialSize(initialSize);
        datasource.setMinIdle(minIdle);
        datasource.setMaxActive(maxActive);
        datasource.setMaxWait(maxWait);
        datasource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        datasource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        datasource.setValidationQuery(validationQuery);
        datasource.setTestWhileIdle(testWhileIdle);
        datasource.setTestOnBorrow(testOnBorrow);
        datasource.setTestOnReturn(testOnReturn);
        datasource.setDbType(dbType);
        //此功能不支持hive
//        try {
//            datasource.setFilters(filters);
//        } catch (SQLException e) {
//            logger.error("druid configuration initialization filter", e);
//        }
        return datasource;
    }

    @Bean(name = "oracleJdbcTemplate")
    public JdbcTemplate oracleJdbcTemplate(@Qualifier("oracleJdbcDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}