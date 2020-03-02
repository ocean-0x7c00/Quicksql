package com.qihoo.qsql.api;

import com.qihoo.qsql.api.SqlRunner.Builder.RunnerType;
import com.qihoo.qsql.exception.QsqlException;
import com.qihoo.qsql.metadata.MetadataPostman;
import com.qihoo.qsql.plan.QueryProcedureProducer;
import com.qihoo.qsql.plan.QueryTables;
import com.qihoo.qsql.plan.proc.DirectQueryProcedure;
import com.qihoo.qsql.plan.proc.ExtractProcedure;
import com.qihoo.qsql.plan.proc.PreparedExtractProcedure;
import com.qihoo.qsql.plan.proc.QueryProcedure;
import com.qihoo.qsql.exec.AbstractPipeline;
import com.qihoo.qsql.exec.JdbcPipeline;
import com.qihoo.qsql.exec.flink.FlinkPipeline;
import com.qihoo.qsql.exec.spark.SparkPipeline;
import com.qihoo.qsql.utils.SqlUtil;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main execution class of qsql task.
 * qsql任务的主要执行类
 * <p>
 * 1.解析SQL获取表名列表
 * 2.根据表名查询与表相关的元数据信息
 * 3.创建执行计划
 * 4.选择合适的Pipeline
 * <p>
 * <p>
 * A sql will be parsed to get table names firstly when it is passed to this
 * class.
 * <p>
 * Then qsql will fetch related metadata through {@link MetadataPostman}
 * based on those table names.
 * <p>
 * In the next step, {@link QueryProcedure} will be
 * created, which is the basic data structure and it can decide which
 * {@link AbstractPipeline} should be used.
 * </p>
 * <p>
 * Certainly, a special {@link AbstractPipeline} can be chosen without
 * executing such a complex process described above.
 * Consider that it may need more experience and practise instead, Dynamic
 * RunnerType of {@link SqlRunner} choice is the
 * most recommended.
 * </p>
 */
public class DynamicSqlRunner extends SqlRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicSqlRunner.class);

    /**
     * todo 迷惑
     */
    private AbstractPipeline pipeline = null;

    /**
     * SQL中的所有表名
     */
    private List<String> tableNames;

    /**
     * 调用父类构造器构造配置信息
     *
     * @param builder
     */
    DynamicSqlRunner(Builder builder) {
        super(builder);
    }

    /**
     * 根据表名获取查找对应的数据源资产
     *
     * @return
     */
    private String getFullSchemaFromAssetDataSource() {
        return "inline: " + MetadataPostman.getCalciteModelSchema(tableNames);
    }

    /**
     * 创建SQL执行计划
     *
     * @param sql
     * @return
     */
    private QueryProcedure createQueryPlan(String sql) {
        //在指定路径获取配置信息
        String schema = environment.getSchemaPath();

        if (schema.isEmpty()) {
            LOGGER.info("Read schema from " + "embedded database.");
        } else {
            LOGGER.info("Read schema from " + ("manual schema, schema or path" +
                    " is: " + schema));
        }

        //判断配置信息路径是否为空
        if (environment.getSchemaPath().isEmpty()) {
            //获取默认schema ，怎么获取？
            schema = getFullSchemaFromAssetDataSource();
        }

        //校验Schema是否是CSV文件，schema以inline开头
        if (schema.equals("inline: ")) {
            schema = JdbcPipeline.CSV_DEFAULT_SCHEMA;
        }
        QueryProcedure queryProcedure = new QueryProcedureProducer(schema, environment).createQueryProcedure(sql);
        return queryProcedure;
    }

    /**
     * todo 迷惑
     *
     * @param sql
     * @return
     */
    @Override
    public AbstractPipeline sql(String sql) {
        LOGGER.info("The SQL that is ready to execute is: \n" + sql);

        //获取待执行SQL中的表名
        QueryTables tables = SqlUtil.parseTableName(sql);
        tableNames = tables.tableNames;

        //判断SQL是否是DML语句，如果是Set running engine for spark
        if (tables.isDml()) {
            environment.setTransformRunner(RunnerType.SPARK);
        }

        LOGGER.debug("Parsed table names for upper SQL are: {}", tableNames);

        //创建SQL执行计划
        QueryProcedure procedure = createQueryPlan(sql);

        LOGGER.debug("Created query plan, the complete plan is: \n{}", procedure.digest(new StringBuilder(), new ArrayList<>()));

        //根据执行计划选择pipeline
        AbstractPipeline abstractPipeline = chooseAdaptPipeline(procedure);
        return abstractPipeline;
    }

    /**
     * Choose a suitable pipeline based on QueryProcedure.
     * <p>
     * 根据SQL执行计划选择合适的Pipeline
     *
     * @param procedure QueryProcedure, which is created based on metadata
     * @return Suitable pipeline for the procedure
     */
    public AbstractPipeline chooseAdaptPipeline(QueryProcedure procedure) {
        //is single engine
        if (procedure instanceof DirectQueryProcedure && (environment.isDefaultMode() || environment.isJdbcMode())) {
            ExtractProcedure extractProcedure = (ExtractProcedure) procedure.next();

            LOGGER.debug("Choose specific runner {} to execute query", extractProcedure.getCategory());

            //specially for hive
            if (extractProcedure instanceof PreparedExtractProcedure.HiveExtractor) {
                if (environment.isJdbcMode()) {
                    throw new QsqlException("Hive cannot run in jdbc runner! Please use Spark runner instead");
                }
                return getOrCreateClusterPipeline(procedure);
            }

            //need a JDBC connection pool
            pipeline = new JdbcPipeline(extractProcedure, tableNames, environment);
            return pipeline;
        } else {
            if (LOGGER.isDebugEnabled()) {
                if (environment.isFlink()) {
                    LOGGER.debug("Choose mixed runner " + "Flink" + " to execute query");
                } else {
                    LOGGER.debug("Choose mixed runner " + "Spark" + " to execute query");
                }
            }
            return getOrCreateClusterPipeline(procedure);
        }
    }

    @Override
    public void stop() {
        LOGGER.info("Exiting runner...");
        if (pipeline != null) {
            pipeline.shutdown();
        }
        LOGGER.info("Exited runner, bye!!");
    }

    /**
     * 获取或创建一个集群模式的pipeline
     * 集群模式的pipeline是什么?
     *
     * @param procedure
     * @return
     */
    private AbstractPipeline getOrCreateClusterPipeline(QueryProcedure
                                                                procedure) {
        if (this.pipeline == null) {
            //runner type can't be changed
            if (environment.isFlink()) {
                this.pipeline = new FlinkPipeline(procedure, environment);
            } else {
                this.pipeline = new SparkPipeline(procedure, environment);
            }
        }

        return this.pipeline;
    }
    //TODO extract all of SqlParser for parsing by one config
    //TODO test set identifier escape in dialect
    //TODO adjust code architecture, make jdbc and runner perform in the same
    // way.(always translate to a new lang)
}
