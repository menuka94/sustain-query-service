/* ---------------------------------------------------------------------------------------------------------------------
 * LinearRegressionModel.java -
 *      Defines a generalized linear regression model that can be
 *      built and executed over a set of MongoDB documents.
 *
 * Author: Caleb Carlson
 *
 * LEGAL/SOFTWARE LICENSE STATEMENT
 *
 * Software in the Sustain Ecosystem are Released Under Terms of Apache Software License.
 * This research has been supported by funding from the US National Science Foundation's CSSI program through
 * awards 1931363, 1931324, 1931335, and 1931283. The project is a joint effort involving Colorado State University,
 * Arizona State University, the University of California-Irvine, and the University of Maryland - Baltimore County. All
 * redistributions of the software must also include this information.
 *
 * TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
 *
 * 1. Definitions.
 *
 *   "License" shall mean the terms and conditions for use, reproduction, and distribution as defined by Sections 1
 *   through 9 of this document.
 *
 *   "Licensor" shall mean the copyright owner or entity authorized by the copyright owner that is granting the License.
 *
 *   "Legal Entity" shall mean the union of the acting entity and all other entities that control, are controlled by, or
 *   are under common control with that entity. For the purposes of this definition, "control" means (i) the power,
 *   direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii)
 *   ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.
 *
 *   "You" (or "Your") shall mean an individual or Legal Entity exercising permissions granted by this License.
 *
 *   "Source" form shall mean the preferred form for making modifications, including but not limited to software source
 *   code, documentation source, and configuration files.
 *
 *   "Object" form shall mean any form resulting from mechanical transformation or translation of a Source form,
 *   including but not limited to compiled object code, generated documentation, and conversions to other media types.
 *
 *   "Work" shall mean the work of authorship, whether in Source or Object form, made available under the License, as
 *   indicated by a copyright notice that is included in or attached to the work (an example is provided in the
 *   Appendix below).
 *
 *   "Derivative Works" shall mean any work, whether in Source or Object form, that is based on (or derived from) the
 *   Work and for which the editorial revisions, annotations, elaborations, or other modifications represent, as a
 *   whole, an original work of authorship. For the purposes of this License, Derivative Works shall not include works
 *   that remain separable from, or merely link (or bind by name) to the interfaces of, the Work and Derivative Works
 *   thereof.
 *
 *   "Contribution" shall mean any work of authorship, including the original version of the Work and any modifications
 *   or additions to that Work or Derivative Works thereof, that is intentionally submitted to Licensor for inclusion in
 *   the Work by the copyright owner or by an individual or Legal Entity authorized to submit on behalf of the copyright
 *   owner. For the purposes of this definition, "submitted" means any form of electronic, verbal, or written
 *   communication sent to the Licensor or its representatives, including but not limited to communication on electronic
 *   mailing lists, source code control systems, and issue tracking systems that are managed by, or on behalf of, the
 *   Licensor for the purpose of discussing and improving the Work, but excluding communication that is conspicuously
 *   marked or otherwise designated in writing by the copyright owner as "Not a Contribution."
 *
 *   "Contributor" shall mean Licensor and any individual or Legal Entity on behalf of whom a Contribution has been
 *   received by Licensor and subsequently incorporated within the Work.
 *
 * 2. Grant of Copyright License. Subject to the terms and conditions of this License, each Contributor hereby grants to
 *    You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce,
 *    prepare Derivative Works of, publicly display, publicly perform, sublicense, and distribute the Work and such
 *    Derivative Works in Source or Object form.
 *
 * 3. Grant of Patent License. Subject to the terms and conditions of this License, each Contributor hereby grants to
 *    You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section)
 *    patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such
 *    license applies only to those patent claims licensable by such Contributor that are necessarily infringed by their
 *    Contribution(s) alone or by combination of their Contribution(s) with the Work to which such Contribution(s) was
 *    submitted. If You institute patent litigation against any entity (including a cross-claim or counterclaim in a
 *    lawsuit) alleging that the Work or a Contribution incorporated within the Work constitutes direct or contributory
 *    patent infringement, then any patent licenses granted to You under this License for that Work shall terminate as
 *    of the date such litigation is filed.
 *
 * 4. Redistribution. You may reproduce and distribute copies of the Work or Derivative Works thereof in any medium,
 *    with or without modifications, and in Source or Object form, provided that You meet the following conditions:
 *      - You must give any other recipients of the Work or Derivative Works a copy of this License; and
 *      - You must cause any modified files to carry prominent notices stating that You changed the files; and
 *      - You must retain, in the Source form of any Derivative Works that You distribute, all copyright, patent,
 *          trademark, and attribution notices from the Source form of the Work, excluding those notices that do not
 *          pertain to any part of the Derivative Works; and
 *      - If the Work includes a "NOTICE" text file as part of its distribution, then any Derivative Works that You
 *          distribute must include a readable copy of the attribution notices contained within such NOTICE file,
 *          excluding those notices that do not pertain to any part of the Derivative Works, in at least one of the
 *          following places: within a NOTICE text file distributed as part of the Derivative Works; within the Source
 *          form or documentation, if provided along with the Derivative Works; or, within a display generated by the
 *          Derivative Works, if and wherever such third-party notices normally appear. The contents of the NOTICE file
 *          are for informational purposes only and do not modify the License. You may add Your own attribution notices
 *          within Derivative Works that You distribute, alongside or as an addendum to the NOTICE text from the Work,
 *          provided that such additional attribution notices cannot be construed as modifying the License.
 *
 *    You may add Your own copyright statement to Your modifications and may provide additional or different license
 *    terms and conditions for use, reproduction, or distribution of Your modifications, or for any such Derivative
 *    Works as a whole, provided Your use, reproduction, and distribution of the Work otherwise complies with the
 *    conditions stated in this License.
 *
 * 5. Submission of Contributions. Unless You explicitly state otherwise, any Contribution intentionally submitted for
 *    inclusion in the Work by You to the Licensor shall be under the terms and conditions of this License, without any
 *    additional terms or conditions. Notwithstanding the above, nothing herein shall supersede or modify the terms of
 *    any separate license agreement you may have executed with Licensor regarding such Contributions.
 *
 * 6. Trademarks. This License does not grant permission to use the trade names, trademarks, service marks, or product
 *    names of the Licensor, except as required for reasonable and customary use in describing the origin of the Work
 *    and reproducing the content of the NOTICE file.
 *
 * 7. Disclaimer of Warranty. Unless required by applicable law or agreed to in writing, Licensor provides the Work
 *    (and each Contributor provides its Contributions) on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *    KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE,
 *    NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining
 *    the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of
 *    permissions under this License.
 *
 * 8. Limitation of Liability. In no event and under no legal theory, whether in tort (including negligence), contract,
 *    or otherwise, unless required by applicable law (such as deliberate and grossly negligent acts) or agreed to in
 *    writing, shall any Contributor be liable to You for damages, including any direct, indirect, special, incidental,
 *    or consequential damages of any character arising as a result of this License or out of the use or inability to
 *    use the Work (including but not limited to damages for loss of goodwill, work stoppage, computer failure or
 *    malfunction, or any and all other commercial damages or losses), even if such Contributor has been advised of the
 *    possibility of such damages.
 *
 * 9. Accepting Warranty or Additional Liability. While redistributing the Work or Derivative Works thereof, You may
 *    choose to offer, and charge a fee for, acceptance of support, warranty, indemnity, or other liability obligations
 *    and/or rights consistent with this License. However, in accepting such obligations, You may act only on Your own
 *    behalf and on Your sole responsibility, not on behalf of any other Contributor, and only if You agree to
 *    indemnify, defend, and hold each Contributor harmless for any liability incurred by, or claims asserted against,
 *    such Contributor by reason of your accepting any such warranty or additional liability.
 *
 * END OF TERMS AND CONDITIONS
 * ------------------------------------------------------------------------------------------------------------------ */

package org.sustain.modeling;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.collection.JavaConverters;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Provides an interface for building generalized Linear Regression
 * models on data pulled in using Mongo's Spark Connector.
 */
public class SustainLinearRegression {

    protected static final Logger log = LogManager.getLogger(SustainLinearRegression.class);

    private JavaSparkContext sparkContext;
    private String[]         features, gisJoins;
    private String           label, loss, solver;
    private Integer          aggregationDepth, maxIterations;
    private Double           elasticNetParam, epsilon, regularizationParam, convergenceTolerance;
    private Boolean          fitIntercept, setStandardization;


    /**
     * The default, overloaded constructor for the SustainLinearRegression class. Defines Mongo and Spark connection
     * parameters, as well as what data we are using for the model.
     * @param master The Spark master in the form spark://<host>:<port>
     * @param appName The human-readable application name of the job we are launching
     * @param mongoUri The MongoDB endpoint, most likely a Mongo Router, in the form mongodb://<host>:<port>
     * @param database The name of the MongoDB database we are accessing
     * @param collection The name of the MongoDB collection we are using for the model
     */
    public SustainLinearRegression(String master, String appName, String mongoUri, String database, String collection) {
        log.info("LinearRegressionModel constructor invoked");
        initSparkSession(master, appName, mongoUri, database, collection);

        // Set default model parameters before user explicitly defines them
        // Uses Spark's default values which can be found in the documentation here:
        // https://spark.apache.org/docs/latest/api/java/org/apache/spark/ml/regression/LinearRegression.html
        this.setLoss("squaredError");
        this.setSolver("auto");
        this.setAggregationDepth(2);
        this.setMaxIterations(10);
        this.setElasticNetParam(0.0);
        this.setEpsilon(1.35);
        this.setRegularizationParam(0.0);
        this.setConvergenceTolerance(1E-6); // 1E-6 = 0.000001
        this.setFitIntercept(true);
        this.setSetStandardization(true);

        addClusterDependencyJars();
    }

    /**
     * Configures and builds a SparkSession and JavaSparkContext, then adds required dependency JARs to the cluster.
     * @param master URI of the Spark master. Format: spark://<hostname>:<port>
     * @param appName Human-readable name of the application.
     * @param mongoUri URI of the Mongo database router. Format: mongodb://<hostname>:<port>
     * @param database Name of the Mongo database to use.
     * @param collection Name of the Mongo collection to import from above database.
     */
    private void initSparkSession(String master, String appName, String mongoUri, String database, String collection) {
        log.info("Initializing SparkSession using:\n\tmaster={}\n\tappName={}\n\tspark.mongodb.input.uri={}" +
                "\n\tspark.mongodb.input.database={}\n\tspark.mongodb.input.collection={}",
                master, appName, mongoUri, database, collection);

        SparkSession sparkSession = SparkSession.builder()
                .master(master)
                .appName(appName)
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
            "build/libs/spark-mllib_2.12-3.0.1.jar",
            "build/libs/spark-sql_2.12-3.0.1.jar",
            "build/libs/bson-4.0.5.jar",
            "build/libs/mongo-java-driver-3.12.5.jar"
            //"build/libs/mongodb-driver-core-4.0.5.jar",
            //"build/libs/scala-library-2.12.11.jar"
        };

        for (String jar: jarPaths) {
            log.info("Adding dependency JAR to the Spark Context: {}", jar);
            sparkContext.addJar(jar);
        }
    }

    public void setFeatures(String[] features) {
        this.features = features;
    }

    public void setGisJoins(String[] gisJoins) {
        this.gisJoins = gisJoins;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setLoss(String loss) {
        this.loss = loss;
    }

    public void setSolver(String solver) {
        this.solver = solver;
    }

    public void setAggregationDepth(Integer aggregationDepth) {
        this.aggregationDepth = aggregationDepth;
    }

    public void setMaxIterations(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }

    public void setElasticNetParam(Double elasticNetParam) {
        this.elasticNetParam = elasticNetParam;
    }

    public void setEpsilon(Double epsilon) {
        this.epsilon = epsilon;
    }

    public void setRegularizationParam(Double regularizationParam) {
        this.regularizationParam = regularizationParam;
    }

    public void setConvergenceTolerance(Double convergenceTolerance) {
        this.convergenceTolerance = convergenceTolerance;
    }

    public void setFitIntercept(Boolean fitIntercept) {
        this.fitIntercept = fitIntercept;
    }

    public void setSetStandardization(Boolean setStandardization) {
        this.setStandardization = setStandardization;
    }

    /**
     * Compiles a List<String> of column names we desire from the loaded collection, using the features String array.
     * @return A Scala Seq<String> of desired column names.
     */
    private Seq<String> desiredColumns() {
        List<String> cols = new ArrayList<>();
        cols.add("gis_join");
        Collections.addAll(cols, this.features);
        cols.add(this.label);
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

    public void buildAndRunModel() {
        log.info("Running Models...");
        ReadConfig readConfig = ReadConfig.create(sparkContext);

        // Lazy-load the collection in as a DF
        Dataset<Row> collection = MongoSpark.load(sparkContext, readConfig).toDF();

        // Select just the columns we want, discard the rest
        Dataset<Row> selected = collection.select("_id", desiredColumns());

        // Loop over GISJoins and create a model for each one
        for (String gisJoin: this.gisJoins) {

            log.info(">>> Building model for GISJoin {}", gisJoin);

            // Filter by the current GISJoin so we only get records corresponding to the current GISJoin
            //FilterFunction<Row> ff = row -> row.getAs("gis_join") == gisJoin;

            Dataset<Row> gisDataset = selected.filter(selected.col("gis_join").$eq$eq$eq(gisJoin))
                    .withColumnRenamed(this.label, "label"); // Rename the chosen label column to "label"

            // Create a VectorAssembler to assemble all the feature columns into a single column vector named "features"
            VectorAssembler vectorAssembler = new VectorAssembler()
                    .setInputCols(this.features)
                    .setOutputCol("features");

            // Transform the gisDataset to have the new "features" column vector
            Dataset<Row> mergedDataset = vectorAssembler.transform(gisDataset);
            mergedDataset.show();
            /*
            // Create Linear Regression object using user-specified parameters
            LinearRegression linearRegression = new LinearRegression()
                    .setLoss(this.loss)
                    .setSolver(this.solver)
                    .setAggregationDepth(this.aggregationDepth)
                    .setMaxIter(this.maxIterations)
                    .setEpsilon(this.epsilon)
                    .setElasticNetParam(this.elasticNetParam)
                    .setRegParam(this.regularizationParam)
                    .setTol(this.convergenceTolerance)
                    .setFitIntercept(this.fitIntercept)
                    .setStandardization(this.setStandardization);

            // Fit the dataset with the "features" and "label" columns
            LinearRegressionModel lrModel = linearRegression.fit(mergedDataset);

            // View training summary
            LinearRegressionTrainingSummary summary = lrModel.summary();
            log.info("================== SUMMARY ====================");
            log.info("Model Slope Coefficients: {}", lrModel.coefficients());
            log.info("Model Intercept: {}", lrModel.intercept());
            log.info("Total Iterations: {}", summary.totalIterations());
            log.info("Objective History: {}", Vectors.dense(summary.objectiveHistory()));

            // Show residuals and accuracy metrics
            summary.residuals().show();
            log.info("RMSE: {}", summary.rootMeanSquaredError());
            log.info("R2: {}", summary.r2());
            log.info("===============================================");
             */



        }

        // Don't forget to close Spark Context!
        sparkContext.close();
    }

    /**
     * Used exclusively for testing and running a linear model directly, without having to interface with gRPC.
     * @param args Usually not used.
     */
    public static void main(String[] args) {
        String[] features = {"timestamp"};
        String label = "max_specific_humidity";
        String[] gisJoins = {"G0100290"};

        SustainLinearRegression lrModel = new SustainLinearRegression("spark://lattice-165:8079", "testApplication",
                "mongodb://lattice-46:27017", "sustaindb", "macav2");

        lrModel.setFeatures(features);
        lrModel.setLabel(label);
        lrModel.setGisJoins(gisJoins);

        lrModel.buildAndRunModel();
    }

}
