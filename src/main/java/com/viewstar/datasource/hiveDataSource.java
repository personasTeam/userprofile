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
public class hiveDataSource {

    private Logger logger = LoggerFactory.getLogger(hiveDataSource.class);

    @Value("${spring.datasource.hive.url}")
    private String dbUrl;

    @Value("${spring.datasource.hive.username}")
    private String username;

    @Value("${spring.datasource.hive.password}")
    private String password;

    @Value("${spring.datasource.hive.driver-class-name}")
    private String driverClassName;

    @Value("${spring.datasource.hive.initialSize}")
    private int initialSize;

    @Value("${spring.datasource.hive.minIdle}")
    private int minIdle;

    @Value("${spring.datasource.hive.maxActive}")
    private int maxActive;

    @Value("${spring.datasource.hive.maxWait}")
    private int maxWait;

    @Value("${spring.datasource.hive.timeBetweenEvictionRunsMillis}")
    private int timeBetweenEvictionRunsMillis;

    @Value("${spring.datasource.hive.minEvictableIdleTimeMillis}")
    private int minEvictableIdleTimeMillis;

    @Value("${spring.datasource.hive.validationQuery}")
    private String validationQuery;

    @Value("${spring.datasource.hive.testWhileIdle}")
    private boolean testWhileIdle;

    @Value("${spring.datasource.hive.testOnBorrow}")
    private boolean testOnBorrow;

    @Value("${spring.datasource.hive.testOnReturn}")
    private boolean testOnReturn;

    @Value("${spring.datasource.hive.filters}")
    private String filters;

    @Value("${spring.datasource.hive.logSlowSql}")
    private String logSlowSql;

    @Value("${spring.datasource.hive.dbType}")
    private String dbType;

    @Bean(name = "hiveJdbcDataSource")
    @Qualifier("hiveJdbcDataSource")
    public DataSource dataSourceHive() {
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

    @Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}