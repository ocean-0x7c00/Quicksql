package com.qihoo.qsql;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.env.RuntimeEnv;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.result.PipelineResult;

import java.io.IOException;

public class CsvScanExample {
    public static void main(String[] args) throws IOException {
        //初始化es
//        RuntimeEnv.init();

        String sql = "select * from depts";
        SqlRunner.Builder.RunnerType runnerType = RunnerType.DEFAULT;
        SqlRunner runner = SqlRunner.builder()
                .setTransformRunner(runnerType)
                .setSchemaPath(RuntimeEnv.metadata)
                .setAppName("test_csv_app")
                .setAcceptedResultsNum(100)
                .ok();

        //感觉有点问题
//        runner.sql(sql).show().run(); quickSQL提供的方式
//        runner.sql(sql).run().show();

//        1.解析SQL获取表名列表
//        2.根据表名查询与表相关的元数据信息
//        3.创建执行计划
//        4.选择合适的Pipeline
        AbstractPipeline pipeline = runner.sql(sql);

        //执行SQL获取结果，并显示结果
        PipelineResult pipelineResult = pipeline.show();

        //打印结果，关闭资源
        pipelineResult.run();

        System.exit(0);
    }
}
