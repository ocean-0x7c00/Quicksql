package com.qihoo.qsql.exec;

import com.qihoo.qsql.api.SqlRunner;
import com.qihoo.qsql.exec.result.PipelineResult;
import com.qihoo.qsql.codegen.IntegratedQueryWrapper;
import com.qihoo.qsql.plan.proc.QueryProcedure;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * 介于SqlRunner和PipelineResult之间，为用户提供一系列的接口来控制SQL的查询
 * <p>
 * A pipeline between {@link SqlRunner} and {@link PipelineResult}, which provides series Apis to user that can control
 * a sql query execution.
 */
public abstract class AbstractPipeline {

    /**
     * todo 迷惑
     */
    protected IntegratedQueryWrapper wrapper;

    /**
     * 数据源信息
     */
    protected SqlRunner.Builder builder;

    /**
     * SQL查询计划
     */
    protected QueryProcedure procedure;

    /**
     * 根据执行计划，数据源信息构建pipeline
     * AbstractPipeline constructor.
     *
     * @param procedure QueryProcedure
     * @param builder   SqlRunner Builder
     */
    public AbstractPipeline(QueryProcedure procedure, SqlRunner.Builder builder) {
        this.builder = builder;
        this.procedure = procedure;
    }

    /*定义一系列操作，来控制SQL的查询*/

    public abstract void run();

    public abstract PipelineResult show();

    public abstract PipelineResult asTextFile(String clusterPath, String deliminator);

    public abstract PipelineResult asJsonFile(String clusterPath);

    public abstract AbstractPipeline asTempTable(String tempTableName);

    public IntegratedQueryWrapper getWrapper() {
        return wrapper;
    }

    public abstract void shutdown();

    /**
     * todo 迷惑
     *
     * @param wrapper
     * @param argument
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    protected Requirement compileRequirement(IntegratedQueryWrapper wrapper, Object argument, Class clazz) {
        Class requirementClass = wrapper.compile();
        try {
            final Constructor<Requirement> constructor = requirementClass.getConstructor(clazz);
            return constructor.newInstance(argument);
        } catch (NoSuchMethodException | IllegalAccessException
                | InvocationTargetException | InstantiationException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
}
