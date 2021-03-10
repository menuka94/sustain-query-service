package org.sustain.util;

public class Constants {
    public static final String GIS_JOIN = "GISJOIN";

    public static class Server {
        public static final String  HOST = System.getenv("SERVER_HOST");
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
    }
}
