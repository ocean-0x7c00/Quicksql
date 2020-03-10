package com.qihoo.qsql.api;

import com.qihoo.qsql.exec.AbstractPipeline;
import org.junit.Test;

/**
 * @author yancy
 * @date 2020/3/5
 */
public class SqlRunnerMainTest {

    @Test
    public void testSingleQueryForMySql() {
        String sql = "SELECT dep_id FROM edu_manage.department";
        AbstractPipeline pipeline = buildDynamicSqlRunner().sql(sql);
        pipeline.show();

    }

    private SqlRunner buildDynamicSqlRunner() {
        return SqlRunner.builder()
                .setTransformRunner(SqlRunner.Builder.RunnerType.DEFAULT)
                .setAppName("Test for DynamicSqlRunner")
                .setAcceptedResultsNum(20)
                .ok();
    }
}
