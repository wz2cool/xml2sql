package com.github.wz2cool.tool.xml2sql.model;

import java.util.ArrayList;
import java.util.List;

public class SqlTemplate {
    private String sqlExpression;
    private List<String> params = new ArrayList<>();

    public String getSqlExpression() {
        return sqlExpression;
    }

    public void setSqlExpression(String sqlExpression) {
        this.sqlExpression = sqlExpression;
    }

    public List<String> getParams() {
        return params;
    }
}
