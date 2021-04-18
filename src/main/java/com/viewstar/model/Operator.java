package com.viewstar.model;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 { key: 1, label: '等于', value: 'equal'},
 { key: 2, label: '不等于', value: 'notequal'},
 { key: 3, label: '包含', value: 'like'},
 { key: 4, label: '不包含', value: 'notlike'},
 { key: 5, label: '开头是', value: 'likebefore'},
 { key: 6, label: '结尾是', value: 'likeafter'},
 { key: 9, label: '为空', value: 'null'},
 { key: 10, label: '不为空', value: 'notnull'}
 { key: 13, label: '为真', value: 'true'},
 { key: 14, label: '为假', value: 'false'},
 { key: 2, label: '>', value: '>'},
 { key: 2, label: '>=', value: '>='},
 { key: 2, label: '<', value: '<'},
 { key: 2, label: '<=', value: '<='},
 { key: 2, label: '=', value: '='},
 { key: 2, label: '!=', value: '!='}
 */
public class Operator {
    private String attr;
    private String op;
    private String value;
    public Operator(String attr, String op, String value) {
        this.attr = attr;
        this.op = op;
        this.value = value;
    }

    /**
     * 翻译表达式.
     * @return 表达式字符串.
     */
    public String getExpression() {
        StringBuffer lastexpression = new StringBuffer("");
        List<String> expList = Arrays.asList(value.split(",")).stream().map(val-> {
            StringBuffer expression = new StringBuffer("");
            switch (op) {
                case "equal" : expression.append("(").append(attr).append(" = '").append(val).append("')");
                    break;
                case "notequal": expression.append("(").append(attr).append(" != '").append(val).append("')");
                    break;
                case "like": expression.append("(").append(attr).append(" like '%")
                        .append(val.replaceAll("'","")).append("%')");
                    break;
                case "notlike": expression.append("(").append(attr).append(" not like '%")
                        .append(val.replaceAll("'","")).append("%')");
                    break;
                case "likebefore": expression.append("(").append(attr).append(" like '")
                        .append(val.replaceAll("'","")).append("%')");
                    break;
                case "likeafter": expression.append("(").append(attr).append(" like '%")
                        .append(val.replaceAll("'","")).append("')");
                    break;
                case "null": expression.append("(").append(attr).append(" is null)");
                    break;
                case "notnull": expression.append("(").append(attr).append(" is not null)");
                    break;
                case "true": expression.append("(").append(attr).append(" = 1)");
                    break;
                case "false": expression.append("(").append(attr).append(" = 0)");
                    break;
                case "=": expression.append("(").append(attr).append(" = ").append(val).append(")");
                    break;
                case "!=": expression.append("(").append(attr).append(" != ").append(val).append(")");
                    break;
                case ">": expression.append("(").append(attr).append(" > ").append(val).append(")");
                    break;
                case ">=": expression.append("(").append(attr).append(" >= ").append(val).append(")");
                    break;
                case "<": expression.append("(").append(attr).append(" < ").append(val).append(")");
                    break;
                case "<=": expression.append("(").append(attr).append(" <= ").append(val).append(")");
                    break;
            }
            return expression.toString();
        }).collect(Collectors.toList());
        if("!=".equals(op) || "notequal".equals(op)) {
            lastexpression.append("(").append(String.join(" and ", expList)).append(")");
        } else {
            lastexpression.append("(").append(String.join(" or ", expList)).append(")");
        }
        return lastexpression.toString();
    }

    public static void main(String[] args) {
        Operator op = new Operator("attr", "=", "0,1,2");
        System.out.println(op.getExpression());
    }
}
