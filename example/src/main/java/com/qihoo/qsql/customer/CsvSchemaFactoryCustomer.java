package com.qihoo.qsql.customer;

import com.qihoo.qsql.org.apache.calcite.schema.Schema;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaFactory;
import com.qihoo.qsql.org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * @author yancy
 * @date 2020/3/9
 */
public class CsvSchemaFactoryCustomer implements SchemaFactory {
    public CsvSchemaFactoryCustomer() {
    }

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return null;
    }
}
