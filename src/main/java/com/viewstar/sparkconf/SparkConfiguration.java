package com.viewstar.sparkconf;

import com.viewstar.tools.WebToolUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * spark配置类.
 */
@Service
public class SparkConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkConfiguration.class);
    /**
     * 环境配置
     */
    private String environment = "prod";

    /**
     * 获取spark对象.
     * @return spark配置对象.
     */
    @Bean
    public SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName("userprofile").setMaster("local");
//                .setMaster("yarn").setSparkHome("/app/spark").set("spark.submit.deployMode", "client").
//                        set("spark.testing.memory", "2147480000").
//                        set("spark.sql.hive.verifyPartitionPath", "true").
//                        set("spark.yarn.executor.memoryOverhead", "1024m").
//                        set("spark.dynamicAllocation.enabled", "true").
//                        set("spark.shuffle.service.enabled", "true").
//                        set("spark.dynamicAllocation.executorIdleTimeout", "60").
//                        set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "18000").
//                        set("spark.dynamicAllocation.initialExecutors", "4").
//                        set("spark.dynamicAllocation.maxExecutors", "4").
//                        set("spark.dynamicAllocation.minExecutors", "4").
//                        set("spark.dynamicAllocation.schedulerBacklogTimeout", "10").
//                        set("spark.eventLog.enabled", "true").
//                        set("spark.executor.instances", "4").
//                        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
//                        set("spark.hadoop.yarn.resourcemanager.hostname", "sparkdis1")
//                .set("spark.hadoop.yarn.resourcemanager.address", "sparkdis1:8032")
//                .set("spark.hadoop.fs.defaultFS", "hdfs://sparkdis1:8020")
//                .set("spark.yarn.access.namenodes", "hdfs://sparkdis1:8020")
//                .set("spark.history.fs.logDirectory", "hdfs://sparkdis1:8020/spark/historyserverforSpark")
//                .set("spark.cores.max", "4")
//                .set("spark.executor.cores", "2")
//                .set("spark.executor.memory", "3g")
//                .set("spark.eventLog.dir", "hdfs://sparkdis1:8020/spark/eventLog")
//                .set("spark.sql.parquet.cacheMetadata", "false")
//                .set("spark.sql.hive.verifyPartitionPath", "true")
//                .set("spark.scheduler.listenerbus.eventqueue.size", "100000")
//                .set("spark.rdd.compress","true")
//                .set("spark.scheduler.listenerbus.eventqueue.capacity", "100000")
//                .set("spark.rdd.compress", "true");
        if ("prod".equals(environment)) {
            sparkConf.setJars(new String[]{"userprofile.jar"});
        } else if ("dev".equals(environment)) {
            sparkConf.setJars(new String[]{"userprofile.jar"});
        } else if ("test".equals(environment)) {
            // 本地调试用local[1] 表示1核，local[*]表示n核心，会用光本地机器的cpu core
            sparkConf.setMaster("local[1]").setJars(new String[]{"userprofile.jar"})
                    .setAppName("local-spark");
        }
        String hostname = null;
        try {
            hostname = WebToolUtils.getLocalIP();
        } catch (UnknownHostException | SocketException e) {
            e.printStackTrace();
        }
        sparkConf.set("spark.driver.host", hostname);
        LOGGER.info("初始化SparkConf...");
        return sparkConf;
    }

    /**
     * 初始化SparkContext
     *
     * @param sparkConf
     * @return
     */
    @Bean
    public SparkContext getSparkContext(@Autowired SparkConf sparkConf, @Autowired Configuration conf) {
        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
        for (Map.Entry<String, String> next : conf) {
            String key = next.getKey();
            String value = next.getValue();
            sparkContext.hadoopConfiguration().set(key, value);
            System.out.println(key+"\t"+value);
        }
        LOGGER.info("初始化SparkContext...");
        return sparkContext;
    }
}
