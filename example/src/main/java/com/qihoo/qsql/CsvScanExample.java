package com.qihoo.qsql;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.env.RuntimeEnv;
import java.io.IOException;

public class CsvScanExample {

    /**
     * If you want to execute in the IDE, adjust the scope of the spark package in the parent pom to compile.
     * 如果希望在IDE中执行spark和flink的样例代码，请调整父pom中的spark、flink的scope值为compile。
     * @param args nothing
     */
    public static void main(String[] args) throws IOException {
//        RuntimeEnv.init();
//        String sql = "select * from depts";
        String sql = "select * from depts";

//        SqlRunner.Builder.RunnerType runnerType = RunnerType.value(args.length < 1 ? "spark" : args[0]);
        SqlRunner.Builder.RunnerType runnerType = RunnerType.JDBC;
        SqlRunner runner = SqlRunner.builder()
            .setTransformRunner(runnerType)
                .setSchemaPath(RuntimeEnv.metadata)
            .setAppName("test_csv_app")
            .setAcceptedResultsNum(100)
            .ok();

        //感觉有点问题
//        runner.sql(sql).show().run(); quickSQL提供的方式
//        runner.sql(sql).run().show();

//        1.解析SQL获取表名列表 大致明白，调了Apache Calcite的解析器
//        2.根据表名查询与表相关的元数据信息
//        3.创建执行计划
//        4.选择合适的Pipeline,如何根据执行计划选择pipeLine的？
        runner.sql(sql).show();
        System.exit(0);
    }
}
