package com.viewstar.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Primary
    @Bean(name = "hivedatasource")
    @ConfigurationProperties(prefix = "spring.datasource.druid.hive53")
    public DruidDataSource srcDs() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean(name = "sparkdatasource")
    @ConfigurationProperties(prefix = "spring.datasource.druid.spark53")
    public DruidDataSource targetDs() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean(name = "oracledatasource")
    @ConfigurationProperties(prefix = "spring.datasource.druid.oracle")
    public DruidDataSource oracleDs() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean(name = "mysqldatasource")
    @ConfigurationProperties(prefix = "spring.datasource.druid.mysql")
    public DruidDataSource mysqlDs() {
        return DruidDataSourceBuilder.create().build();
    }

    @Bean(name = "hiveJdbcTemplate")
    public JdbcTemplate srcJdbcTemplate(
            @Qualifier("hivedatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "sparkJdbcTemplate")
    public JdbcTemplate targetJdbcTemplate(
            @Qualifier("sparkdatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "oracleJdbcTemplate")
    public JdbcTemplate oracleJdbcTemplate(
            @Qualifier("oracledatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "mysqlJdbcTemplate")
    public JdbcTemplate mysqlJdbcTemplate(
            @Qualifier("mysqldatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}