package com.github.wz2cool.tool.xml2sql.model;

import java.util.ArrayList;
import java.util.List;

public class DbTable {
    private String tableName;
    private List<String> columns = new ArrayList<>();

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getColumns() {
        return columns;
    }
}
