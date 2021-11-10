package org.sustain.util;

public class Constants {
    public static final String GIS_JOIN = "GISJOIN";

    public static class Kubernetes {
        public static final String NODE_HOSTNAME = System.getenv("NODE_HOSTNAME");
        public static final String POD_NAME = System.getenv("POD_NAME");
    }

    public static class Server {
        public static final String  HOST = System.getenv("HOSTNAME");
        public static final Integer PORT = Integer.parseInt(System.getenv("SERVER_PORT"));
    }

    public static class DB {
        public static final String  NAME     = System.getenv("DB_NAME");
        public static final String  USERNAME = System.getenv("DB_USERNAME");
        public static final String  PASSWORD = System.getenv("DB_PASSWORD");
        public static final String  HOST     = System.getenv("DB_HOST");
        public static final Integer PORT     = Integer.parseInt(System.getenv("DB_PORT"));
    }

    public static class Spark {
        public static final String MASTER = System.getenv("SPARK_MASTER");
        public static final String EXECUTOR_CORES = System.getenv("SPARK_EXECUTOR_CORES");
        public static final String EXECUTOR_MEMORY = System.getenv("SPARK_EXECUTOR_MEMORY");
        public static final String INITIAL_EXECUTORS = System.getenv("SPARK_INITIAL_EXECUTORS");
        public static final String MIN_EXECUTORS = System.getenv("SPARK_MIN_EXECUTORS");
        public static final String MAX_EXECUTORS = System.getenv("SPARK_MAX_EXECUTORS");
        public static final String BACKLOG_TIMEOUT = System.getenv("SPARK_BACKLOG_TIMEOUT");
        public static final String IDLE_TIMEOUT = System.getenv("SPARK_IDLE_TIMEOUT");
    }

    public static class Druid {
        public static final String QUERY_HOST = System.getenv("DRUID_QUERY_HOST");
        public static final String QUERY_PORT = System.getenv("DRUID_QUERY_PORT");
    }
}
