package com.qihoo.qsql.api;


import com.qihoo.qsql.exec.AbstractPipeline;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * SqlRunner is the main class for building and executing qsql task.
 * 用于构建和执行QSQL任务
 * <p>
 * <p>
 * SqlRunner实现了与数据源相关的构建信息
 * <p>
 * <p>
 * As the parent class for {@link DynamicSqlRunner}, SqlRunner takes works of initializing and managing environment
 * params mostly.
 * </p>
 */
public abstract class SqlRunner {

    /**
     * 数据信息
     */
    protected Builder environment;

    public SqlRunner(Builder builder) {
        this.environment = builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Connection getConnection() throws SQLException {
        return new AutomaticConnection();
    }

    public static Connection getConnection(String schema) throws SQLException {
        return new AutomaticConnection(schema);
    }

    /**
     * 数据源配置信息构建类
     */
    public static class Builder {

        /**
         * 查询引擎类型
         */
        private RunnerType runner = RunnerType.DEFAULT;

        /**
         * 查询引擎配置信息
         */
        private Properties runnerProperties = new Properties();

        /**
         * 返回结果条数
         */
        private Integer resultsNum = 1000;

        /**
         * 配置信息路径
         */
        private String confPath = "";

        /**
         * 应用名称
         */
        private String appName = "QSQL-" + System.currentTimeMillis();

        /**
         *
         */
        private String master = "local[*]";

        /**
         * Hive引擎是否可用
         */
        private Boolean enableHive = false;

        private Builder() {
        }

        /**
         * Set running engine.
         * <p>
         * 设置查询引擎
         *
         * @param engine Running engine
         * @return SqlRunner builder, which holds several environment params
         */
        public Builder setTransformRunner(RunnerType engine) {
            switch (engine) {
                case FLINK:
                    this.runner = RunnerType.FLINK;
                    break;
                case SPARK:
                    this.runner = RunnerType.SPARK;
                    break;
                case JDBC:
                    this.runner = RunnerType.JDBC;
                    break;
                default:
                    this.runner = RunnerType.DEFAULT;
            }

            return this;
        }

        /**
         * 判断查询引擎是否为spark
         *
         * @return
         */
        public boolean isSpark() {
            return this.runner == RunnerType.SPARK || this.runner == RunnerType.DEFAULT;
        }

        /**
         * 判断查询引擎是否为flink
         *
         * @return
         */
        public boolean isFlink() {
            return this.runner == RunnerType.FLINK;
        }

        /**
         * 判断是否使用默认的查询引擎
         *
         * @return
         */
        public boolean isDefaultMode() {
            return this.runner == RunnerType.DEFAULT;
        }

        /**
         * 判断查询引擎是否为jdbc
         *
         * @return
         */
        public boolean isJdbcMode() {
            return this.runner == RunnerType.JDBC;
        }

        /**
         * 设置属性文件
         *
         * @param properties
         * @return
         */
        public Builder setProperties(Properties properties) {
            this.runnerProperties.putAll(properties);
            return this;
        }

        public String getAppName() {
            return appName;
        }

        public Builder setAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Boolean getEnableHive() {
            return enableHive;
        }

        public Builder setEnableHive(Boolean enable) {
            this.enableHive = enable;
            return this;
        }

        public String getMaster() {
            return master;
        }

        public Builder setMaster(String master) {
            this.master = master;
            return this;
        }

        public Integer getAcceptedResultsNum() {
            return resultsNum;
        }

        /**
         * Set number of query result which should be showed in console.
         *
         * @param num Number of query result showed in console
         * @return SqlRunner builder, which holds several environment params
         */
        public Builder setAcceptedResultsNum(Integer num) {
            if (num <= 0) {
                this.resultsNum = 1000;
            } else {
                this.resultsNum = num;
            }
            return this;
        }

        public Properties getRunnerProperties() {
            return runnerProperties;
        }

        public String getSchemaPath() {
            return confPath;
        }

        public Builder setSchemaPath(String schemaPath) {
            this.confPath = schemaPath;
            return this;
        }

        /**
         * 根据配置信息创建SqlRunner对象
         *
         * @return
         */
        public SqlRunner ok() {
            return new DynamicSqlRunner(this);
        }

        public RunnerType getRunner() {
            return runner;
        }

        /**
         * 定义查询引擎类型
         */
        public enum RunnerType {
            SPARK, FLINK, JDBC, DEFAULT;

            /**
             * convert to RunnerType from string.
             *
             * @param value runner string value
             * @return enum RunnerType
             */
            public static RunnerType value(String value) {
                switch (value.toUpperCase()) {
                    case "SPARK":
                        return SPARK;
                    case "FLINK":
                        return FLINK;
                    case "JDBC":
                        return JDBC;
                    default:
                        return DEFAULT;
                }
            }
        }
    }

    /**
     * todo 迷惑
     *
     * @param sql
     * @return
     */
    public abstract AbstractPipeline sql(String sql);

    public abstract void stop();


}