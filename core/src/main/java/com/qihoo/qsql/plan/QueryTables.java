package com.qihoo.qsql.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * 待查询SQL中的表信息
 */
public class QueryTables {

    /**
     * SQL中包含的表名
     */
    public List<String> tableNames = new ArrayList<>();

    /**
     * 是否是DML语句
     */
    private boolean isDml = false;

    public boolean isDml() {
        return isDml;
    }

    public void isDmlActually() {
        this.isDml = true;
    }

    void add(String tableName) {
        this.tableNames.add(tableName);
    }
}