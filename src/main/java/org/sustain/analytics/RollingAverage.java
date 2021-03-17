package org.sustain.analytics;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.avg;

public class RollingAverage {
    protected static final Logger log = LogManager.getLogger(RollingAverage.class);

    private JavaSparkContext sparkContext;

    private String gisJoin;
    private String [] features;

    public RollingAverage(String master, String mongoUri, String database, String collection){
        initSparkSession(master, mongoUri, database, collection);
    }

    public void setGisJoin(String gisJoin){
        this.gisJoin = gisJoin;
    }

    public void setFeatures(String[] features) {
        this.features = features;
    }

    /**
     * Configures and builds a SparkSession and JavaSparkContext, then adds required dependency JARs to the cluster.
     * @param master URI of the Spark master. Format: spark://<hostname>:<port>
     * @param mongoUri URI of the Mongo database router. Format: mongodb://<hostname>:<port>
     * @param database Name of the Mongo database to use.
     * @param collection Name of the Mongo collection to import from above database.
     */
    private void initSparkSession(String master, String mongoUri, String database, String collection) {
        log.info("Initializing SparkSession using:\n\tmaster={}\n\tspark.mongodb.input.uri={}" +
                        "\n\tspark.mongodb.input.database={}\n\tspark.mongodb.input.collection={}",
                master, mongoUri, database, collection);

        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName("SUSTAIN Rolling Average Calc")
                .config("spark.mongodb.input.uri", mongoUri) // mongodb://lattice-46:27017
                .config("spark.mongodb.input.database", database) // sustaindb
                .config("spark.mongodb.input.collection", collection) // future_heat
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        addClusterDependencyJars();
    }

    /**
     * Adds required dependency jars to the Spark Context member.
     */
    private void addClusterDependencyJars() {
        String[] jarPaths = {
                "build/libs/mongo-spark-connector_2.12-3.0.1.jar",
                "build/libs/spark-core_2.12-3.0.1.jar",
                "build/libs/spark-sql_2.12-3.0.1.jar",
                "build/libs/bson-4.0.5.jar",
                "build/libs/mongo-java-driver-3.12.5.jar"
        };

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkContext.addJar(jar);
        }
    }

    /**
     * Compiles a List<String> of column names we desire from the loaded collection, using the features String array.
     * @return A Scala Seq<String> of desired column names.
     */
    private Seq<String> desiredColumns() {
        List<String> cols = new ArrayList<>();
        cols.add("GISJOIN");
        Collections.addAll(cols, this.features);
        return convertListToSeq(cols);
    }

    /**
     * Converts a Java List<String> of inputs to a Scala Seq<String>
     * @param inputList The Java List<String> we wish to transform
     * @return A Scala Seq<String> representing the original input list
     */
    public Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }


    /**
     * Calculates the average of the 'cases' column from covid_county_formatted given a valid UTC date range
     * @param start Starting UTC date string
     * @param end Ending UTC date string
     */
    private void calculateAverageOverDates(String start, String end){
        String isoDatePattern = "yyyy-MM-dd HH:mm:ss.SSS ZZZ";
        Date formattedStart = null;
        Date formattedEnd = null;
        try {
            formattedStart = new SimpleDateFormat(isoDatePattern).parse(start);
            formattedEnd = new SimpleDateFormat(isoDatePattern).parse(end);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        java.sql.Date sqlStart = new java.sql.Date(formattedStart.getTime());
        java.sql.Date sqlEnd = new java.sql.Date(formattedEnd.getTime());

        ReadConfig readConfig = ReadConfig.create(sparkContext);

        // Lazy-load the collection in as a DF
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();
        log.info("Loaded data frame");

        // Select just the columns we want, discard the rest
        Dataset<Row> selected = collection.select("_id", desiredColumns());
        log.info("Filtered desired columns");

        //Filter on GIS-Join to get proper location
        Dataset<Row> gisDataset = selected.filter(selected.col("GISJOIN").$eq$eq$eq(gisJoin));
        log.info("Filtered by county");

        //Filter on given date range
        Dataset<Row> dateFiltered = gisDataset.filter(gisDataset.col("date")
                .$greater$eq(sqlStart)
                .$amp$amp(gisDataset.col("date")
                        .$less$eq(sqlEnd)));
        log.info("Filtered by daterange");

        Dataset<Row> meanData = dateFiltered
                .agg(avg("cases"));
        log.info("Calculated average");

        meanData.show();
    }

    public static void main(String[] args){
        String gisJoin = "G0600850"; //Santa Clara, CA
        RollingAverage covid_cases_avg = new RollingAverage("spark://lattice-165:8079",
                "mongodb://lattice-46:27017",
                "sustaindb",
                "covid_county_formatted");

        covid_cases_avg.setGisJoin(gisJoin);
        covid_cases_avg.setFeatures(new String[] {"date", "cases"}); //Grab date, case count data entries

        covid_cases_avg.calculateAverageOverDates("2020-01-27 00:00:00.000 UTC","2020-02-02 00:00:00.000 UTC");
    }
}
