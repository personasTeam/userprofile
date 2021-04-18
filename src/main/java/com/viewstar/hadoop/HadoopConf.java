package com.viewstar.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
public class HadoopConf {
    @Bean
    public Configuration getHadoopConf() {
        Configuration conf = new Configuration();
        conf.addResource("conf/core-site.xml");
        conf.addResource("conf/hdfs-site.xml");
        conf.addResource("conf/hive-site.xml");
        conf.addResource("conf/yarn-site.xml");
        conf.addResource("conf/mapred-site.xml");
        return conf;
    }

}
