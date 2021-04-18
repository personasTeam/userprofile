package com.viewstar.model;

abstract class Expression {
    private String id;
    private String index;
    private String type;

    public Expression(String id, String index) {
        this.id = id;
        this.index = index;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public abstract String getExpression();
}
