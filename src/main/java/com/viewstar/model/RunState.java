package com.viewstar.model;

import java.util.Map;

/**
 * 处理数据请求对象
 */
public class RunState {
    public int run_id; // 标签ID或是分群ID
    public String type; // tag/cluster   标签、分群
    public String run_state; // 标签、分群数据处理状态    0是处理中，1是已完成
    public String update_state; // 画像表（大表）更新状态    0是处理中，1是已完成
    public String time; // 时间，确定是哪一天的画像表
    public int id; // 主键ID

    /**
     * 构造函数
     * @param runMap 传入的map转化为RunState对象.
     */
    public RunState(Map<String, Object> runMap) {
        // 初始化run_id
        if(runMap.containsKey("run_id") && runMap.get("run_id")!=null) {
            this.run_id = Integer.valueOf(runMap.get("run_id").toString());
        } else {
            this.run_id = -1;
        }
        // 初始化type
        if(runMap.containsKey("type") && runMap.get("type")!=null) {
            this.type = runMap.get("type").toString();
        } else {
            this.type = "";
        }
        // 初始化run_state
        if(runMap.containsKey("run_state") && runMap.get("run_state")!=null) {
            this.run_state = runMap.get("run_state").toString();
        } else {
            this.run_state = "";
        }
        // 初始化update_state
        if(runMap.containsKey("update_state") && runMap.get("update_state")!=null) {
            this.update_state = runMap.get("update_state").toString();
        } else {
            this.update_state = "";
        }
        // 初始化time
        if(runMap.containsKey("time") && runMap.get("time")!=null) {
            this.time = runMap.get("time").toString();
        } else {
            this.time = "";
        }
        // 初始化id
        if(runMap.containsKey("id") && runMap.get("id")!=null) {
            this.id = Integer.valueOf(runMap.get("id").toString());
        } else {
            this.id = -1;
        }
    }

    /**
     * set方法
     */
    public void setRun_id(int run_id) {
        this.run_id = run_id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setRun_state(String run_state) {
        this.run_state = run_state;
    }

    public void setUpdate_state(String update_state) {
        this.update_state = update_state;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public void setId(int id) {
        this.id = id;
    }
    /**
     * get方法
     */
    public int getRun_id() {
        return run_id;
    }

    public String getType() {
        return type;
    }

    public String getRun_state() {
        return run_state;
    }

    public String getUpdate_state() {
        return update_state;
    }

    public String getTime() {
        return time;
    }

    public int getId() {
        return id;
    }
}
