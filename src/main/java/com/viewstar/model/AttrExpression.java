package com.viewstar.model;

import org.json.JSONException;
import org.json.JSONObject;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 属性表达式.
 * create by KLS.
 */
public class AttrExpression extends Expression {
    private String attr; // 属性字段
    private String relation; // 表达式关系
    private String value; // 表达式值
    private String type; // 创建方式type
    private String valueType; // 条件值是str还是num

    /**
     * 构造函数.
     * @param map 参数map
     * @throws JSONException 解析异常
     */
    public AttrExpression(JSONObject map) throws JSONException {
        super(map.get("id").toString(), map.get("index").toString());
        this.type = map.get("type").toString();
        this.attr = map.get("attr").toString();
        this.relation = map.get("relation").toString();
        this.value = map.get("value").toString();
        // 用户活跃单独处理
        if("useractivedays".equals(this.attr)) {
            if(this.value.contains("<")) {
                this.relation = "<";
                this.value = map.get("value").toString().replaceAll("<","");
            } else if(this.value.contains(">")) {
                this.relation = ">";
                this.value = map.get("value").toString().replaceAll(">","");
            }
        }
        if(map.has("valueType")) {
            this.valueType = map.get("valueType").toString();
        } else {
            this.valueType = "num";
        }
    }

    /**
     * 根据字段连接成表达式.
     * @return 表达式字符串"userareaid = 1" 前后无空格.
     */
    @Override
    public String getExpression() {
        String expression = "";
        try {
            if("string".equals(valueType) && "event".equals(type)) {
                if(value.contains(",")) {
                    value = value.replaceAll(",", "','");
                }
                value = "'" + value + "'";
            }
            if("attr".equals(type) || "event".equals(type)) {
                Operator operator = new Operator(attr, relation,
                        value.endsWith(".0")?value.replaceAll(".0",""):value);
                expression = operator.getExpression();
            } else if("tag".equals(type)) {
                List<String> valueList = Arrays.asList(value.split(","));
                if("equal".equals(relation)) {
                    valueList = valueList.stream().map(a-> {
                        return "("+ a +"=1)";
                    }).collect(Collectors.toList());
                } else if("notequal".equals(relation)) {
                    valueList = valueList.stream().map(a-> {
                        return "("+ a +"=0)";
                    }).collect(Collectors.toList());
                }
                expression = String.join(" or ", valueList);
            } else if("cluster".equals(type)) {
                Operator operator = new Operator(attr, relation,value);
                expression = operator.getExpression();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return expression;
    }

    public String getAttr() {
        return attr;
    }

    public void setAttr(String attr) {
        this.attr = attr;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
