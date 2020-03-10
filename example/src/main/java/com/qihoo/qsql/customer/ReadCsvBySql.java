package com.qihoo.qsql.customer;

import com.google.common.collect.ImmutableMap;
import com.qihoo.qsql.org.apache.calcite.adapter.csv.CsvSchema;
import com.qihoo.qsql.org.apache.calcite.adapter.csv.CsvSchemaFactory;
import com.qihoo.qsql.org.apache.calcite.adapter.csv.CsvTable;
import com.qihoo.qsql.org.apache.calcite.schema.Schema;

import java.io.File;
import java.sql.*;

/**
 * @author yancy
 * @date 2020/3/9
 */
public class ReadCsvBySql {
    static String sql = "select * from foodmart.sales_fact_1997 as s join hr.emps as e on e.empid = s.cust_id";

    static String csvPathName = "";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.qihoo.qsql.org.apache.calcite.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:calcite:");

        //todo 设置元数据信息
        File file = new File(csvPathName);
        CsvSchema csvSchema = new CsvSchema(file, CsvTable.Flavor.SCANNABLE);

//        Schema schema = CsvSchemaFactory.INSTANCE.create(null, null, ImmutableMap.of());

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
