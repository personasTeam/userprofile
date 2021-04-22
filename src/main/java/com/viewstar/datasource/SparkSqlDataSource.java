package com.viewstar.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Created by arnold.zhu on 6/13/2017.
 */
@Configuration
public class SparkSqlDataSource {

    private Logger logger = LoggerFactory.getLogger(SparkSqlDataSource.class);

    @Value("${spring.datasource.sparksql.url}")
    private String dbUrl;

    @Value("${spring.datasource.sparksql.username}")
    private String username;

    @Value("${spring.datasource.sparksql.password}")
    private String password;

    @Value("${spring.datasource.sparksql.driver-class-name}")
    private String driverClassName;

    @Value("${spring.datasource.sparksql.initialSize}")
    private int initialSize;

    @Value("${spring.datasource.sparksql.minIdle}")
    private int minIdle;

    @Value("${spring.datasource.sparksql.maxActive}")
    private int maxActive;

    @Value("${spring.datasource.sparksql.maxWait}")
    private int maxWait;

    @Value("${spring.datasource.sparksql.timeBetweenEvictionRunsMillis}")
    private int timeBetweenEvictionRunsMillis;

    @Value("${spring.datasource.sparksql.minEvictableIdleTimeMillis}")
    private int minEvictableIdleTimeMillis;

    @Value("${spring.datasource.sparksql.validationQuery}")
    private String validationQuery;

    @Value("${spring.datasource.sparksql.testWhileIdle}")
    private boolean testWhileIdle;

    @Value("${spring.datasource.sparksql.testOnBorrow}")
    private boolean testOnBorrow;

    @Value("${spring.datasource.sparksql.testOnReturn}")
    private boolean testOnReturn;

    @Value("${spring.datasource.sparksql.filters}")
    private String filters;

    @Value("${spring.datasource.sparksql.logSlowSql}")
    private String logSlowSql;

    @Value("${spring.datasource.sparksql.dbType}")
    private String dbType;

    @Bean(name = "sparkSqlJdbcDataSource")
    @Qualifier("sparkSqlJdbcDataSource")
    @Primary
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

    @Bean(name = "sparkSqlJdbcTemplate")
    @Primary
    public JdbcTemplate hiveJdbcTemplate(@Qualifier("sparkSqlJdbcDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}