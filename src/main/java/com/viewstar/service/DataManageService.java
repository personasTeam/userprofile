package com.viewstar.service;


import java.util.List;

/**
 * 用户处理接口.
 * 实现hive中的MVCC隔离级别.
 * KLS 2020/01/07
 */
public interface DataManageService {
    /**
     * 获取hive表数据的hdfs地址.
     * @param tableName 表名
     * @param time 时间
     * @return hive表数据的hdfs地址
     */
    public String getDataPath (String tableName, String time);

    /**
     * 重命名hive表
     * @param oldName 旧表名
     * @param newName 新表名
     */
    public void renameTable(String oldName, String newName);

    /**
     * 删除hive表
     * @param tableName 表名
     */
    public void deleteTable(String tableName);

    /**
     * 表的透视处理，纵表变横表.
     * @param tablename 表名.
     * @param columnname 列名.
     * @param ptime 处理时间.
     * @param hdfspath 结果存储路径.
     * @return 转置后的新表.
     */
    public List<String> datapivot(String tablename, String columnname, String ptime, String hdfspath);

    /**
     * 处理用户画像基础数据.
     * @param time 日期.
     * @param resultDataPath 需要处理的表数据存取地址.
     */
    public void dataManager(String time, String resultDataPath);
    public void test();
}
