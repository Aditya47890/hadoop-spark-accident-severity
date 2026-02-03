package com.company.accident;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions;
import org.apache.spark.ml.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class TrainPipeline implements Serializable {

    // Exact header-derived column names (from your CSV)
    private static final String COL_CRASH_DATETIME = "CRASH DATETIME";
    private static final String COL_DAY_OF_WEEK_CODE = "DAY OF WEEK CODE";
    private static final String COL_DAY_OF_WEEK_DESC = "DAY OF WEEK DESCRIPTION";
    private static final String COL_CRASH_CLASS_CODE = "CRASH CLASSIFICATION CODE";
    private static final String COL_CRASH_CLASS_DESC = "CRASH CLASSIFICATION DESCRIPTION";
    private static final String COL_COLLISION_PRIVATE = "COLLISION ON PRIVATE PROPERTY";
    private static final String COL_PEDESTRIAN = "PEDESTRIAN INVOLVED";
    private static final String COL_MANNER_IMPACT_CODE = "MANNER OF IMPACT CODE";
    private static final String COL_MANNER_IMPACT_DESC = "MANNER OF IMPACT DESCRIPTION";
    private static final String COL_ALCOHOL = "ALCOHOL INVOLVED";
    private static final String COL_DRUG = "DRUG INVOLVED";
    private static final String COL_ROAD_SURF_CODE = "ROAD SURFACE CODE";
    private static final String COL_ROAD_SURF_DESC = "ROAD SURFACE DESCRIPTION";
    private static final String COL_LIGHTING_CODE = "LIGHTING CONDITION CODE";
    private static final String COL_LIGHTING_DESC = "LIGHTING CONDITION DESCRIPTION";
    private static final String COL_WEATHER1_CODE = "WEATHER 1 CODE";
    private static final String COL_WEATHER1_DESC = "WEATHER 1 DESCRIPTION";
    private static final String COL_WEATHER2_CODE = "WEATHER 2 CODE";
    private static final String COL_WEATHER2_DESC = "WEATHER 2 DESCRIPTION";
    private static final String COL_SEATBELT = "SEATBELT USED";
    private static final String COL_MOTORCYCLE = "MOTORCYCLE INVOLVED";
    private static final String COL_MOTOR_HELMET = "MOTORCYCLE HELMET USED";
    private static final String COL_BICYCLED = "BICYCLED INVOLVED";
    private static final String COL_BICYCLE_HELMET = "BICYCLE HELMET USED";
    private static final String COL_LAT = "LATITUDE";
    private static final String COL_LON = "LONGITUDE";
    private static final String COL_PRIMARY_CONTR_CODE = "PRIMARY CONTRIBUTING CIRCUMSTANCE CODE";
    private static final String COL_PRIMARY_CONTR_DESC = "PRIMARY CONTRIBUTING CIRCUMSTANCE DESCRIPTION";
    private static final String COL_SCHOOL_BUS_CODE = "SCHOOL BUS INVOLVED CODE";
    private static final String COL_SCHOOL_BUS_DESC = "SCHOOL BUS INVOLVED DESCRIPTION";
    private static final String COL_WORK_ZONE = "WORK ZONE";
    private static final String COL_WORK_ZONE_LOC_CODE = "WORK ZONE LOCATION CODE";
    private static final String COL_WORK_ZONE_LOC_DESC = "WORK ZONE LOCATION DESCRIPTION";
    private static final String COL_WORK_ZONE_TYPE_CODE = "WORK ZONE TYPE CODE";
    private static final String COL_WORK_ZONE_TYPE_DESC = "WORK ZONE TYPE DESCRIPTION";
    private static final String COL_WORKERS_PRESENT = "WORKERS PRESENT";
    private static final String COL_GEOM = "the_geom";

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        log("🔵 Starting TrainPipeline...");

        Map<String, String> amap = parseArgs(args);
        final String inputPath = amap.getOrDefault("--input", "");
        final String hdfsRoot = amap.getOrDefault("--hdfsRoot", "hdfs:///projects/project_accident_seriousness");
        final int minCatCardinality = Integer.parseInt(amap.getOrDefault("--minCatCardinality", "60"));
        final int hashSize = Integer.parseInt(amap.getOrDefault("--hashSize", "1024"));
        final int topK = Integer.parseInt(amap.getOrDefault("--topK", "50"));

        if (inputPath.isEmpty()) {
            System.err.println("❌ ERROR: Missing --input <path>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf()
                .setAppName("VehicleAccident_TrainPipeline")
                .set("spark.sql.shuffle.partitions", "8"); // tuned for small dataset

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        try {
            log("📂 Reading CSV from HDFS...");
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("mode", "DROPMALFORMED")
                    .csv(inputPath);

            final String TARGET = COL_CRASH_CLASS_DESC;

            // Ensure target exists
            if (!Arrays.asList(df.columns()).contains(TARGET)) {
                log("❌ Target column not found: " + TARGET);
                writeErrorToHdfs(hdfsRoot + "/models/vehicle_accident/error.txt",
                        "Target column missing: " + TARGET, spark);
                System.exit(2);
            }

            log("ℹ Columns loaded: " + String.join(", ", df.columns()));

            // Drop rows where target null
            log("🧹 Dropping null target rows...");
            df = df.filter(df.col(TARGET).isNotNull());

            // Clean CRASH CLASSIFICATION CODE mixed types: cast to string and optionally drop (leaky)
            if (Arrays.asList(df.columns()).contains(COL_CRASH_CLASS_CODE)) {
                log("🔧 Cleaning column: " + COL_CRASH_CLASS_CODE);
                df = df.withColumn(COL_CRASH_CLASS_CODE, df.col(COL_CRASH_CLASS_CODE).cast("string"));
            }

            // Drop leaky & useless
            List<String> dropCols = Arrays.asList(COL_CRASH_CLASS_CODE, COL_GEOM);
            for (String col : dropCols) {
                if (Arrays.asList(df.columns()).contains(col)) {
                    df = df.drop(col);
                    log("➖ Dropped column: " + col);
                }
            }

            // ===== DATETIME PARSING (robust) =====
            if (Arrays.asList(df.columns()).contains(COL_CRASH_DATETIME)) {
                log("⏱ Parsing CRASH DATETIME into crash_ts...");
                // Primary parse with explicit format in your CSV: "MM/dd/yyyy hh:mm:ss a Z"
                df = df.withColumn("crash_ts",
                        functions.to_timestamp(df.col(COL_CRASH_DATETIME), "MM/dd/yyyy hh:mm:ss a Z"));

                // Fallback: if still null, try without timezone or let Spark infer
                df = df.withColumn("crash_ts",
                        functions.when(df.col("crash_ts").isNull(),
                                functions.to_timestamp(df.col(COL_CRASH_DATETIME), "MM/dd/yyyy hh:mm:ss a"))
                                .otherwise(df.col("crash_ts")));

                df = df.withColumn("crash_ts",
                        functions.when(df.col("crash_ts").isNull(),
                                functions.to_timestamp(df.col(COL_CRASH_DATETIME)))
                                .otherwise(df.col("crash_ts")));

                // Extract features
                df = df.withColumn("crash_hour", functions.hour(df.col("crash_ts")));
                df = df.withColumn("crash_day", functions.dayofmonth(df.col("crash_ts")));
                df = df.withColumn("crash_month", functions.month(df.col("crash_ts")));
                df = df.withColumn("crash_year", functions.year(df.col("crash_ts")));

                // safe is_weekend fallback from DAY OF WEEK CODE if exists
                if (Arrays.asList(df.columns()).contains(COL_DAY_OF_WEEK_CODE)) {
                    df = df.withColumn("is_weekend",
                            functions.when(df.col(COL_DAY_OF_WEEK_CODE).equalTo(1)
                                    .or(df.col(COL_DAY_OF_WEEK_CODE).equalTo(7)), 1).otherwise(0));
                } else {
                    df = df.withColumn("is_weekend",
                            functions.when(functions.dayofweek(df.col("crash_ts")).isin(1, 7), 1).otherwise(0));
                }

                // drop original datetime to avoid confusion
                df = df.drop(COL_CRASH_DATETIME);
            } else {
                log("⚠ CRASH DATETIME column not found; skipping datetime extraction.");
            }

            // ===== DETECT NUMERIC & CATEGORICAL safely =====
            log("🔍 Detecting numeric & categorical columns...");
            List<String> numericCols = new ArrayList<>();
            List<String> categoricalCols = new ArrayList<>();

            Set<String> skipCols = new HashSet<>(Arrays.asList(
                    TARGET, "crash_ts" // skip target and internal ts
            ));

            for (StructField f : df.schema().fields()) {
                String col = f.name();
                if (skipCols.contains(col)) continue;
                DataType dt = f.dataType();

                // Treat Y/N or 1/0 as categorical first; we'll convert known ones later.
                if (dt instanceof IntegerType || dt instanceof DoubleType || dt instanceof LongType || dt instanceof FloatType) {
                    numericCols.add(col);
                } else {
                    categoricalCols.add(col);
                }
            }

            log("ℹ Numeric columns detected: " + numericCols);
            log("ℹ Categorical columns detected: " + categoricalCols);

            // Force cast latitude/longitude to double if present
            for (String geo : Arrays.asList(COL_LAT, COL_LON)) {
                if (Arrays.asList(df.columns()).contains(geo)) {
                    df = df.withColumn(geo, functions.when(df.col(geo).isNull(), functions.lit(Double.NaN))
                            .otherwise(df.col(geo).cast("double")));
                    if (!numericCols.contains(geo)) numericCols.add(geo);
                }
            }

            // Convert all categorical columns to string and fill nulls
            for (String c : categoricalCols) {
                if (!Arrays.asList(df.columns()).contains(c)) continue;
                df = df.withColumn(c, df.col(c).cast("string"));
                df = df.withColumn(c, functions.when(df.col(c).isNull(), "__UNKNOWN__").otherwise(df.col(c)));
            }

            // ===== Convert known Y/N-like columns to binary =====
            log("🔄 Converting known Y/N-like columns to binary...");
            String[] ynCandidates = new String[] {
                    COL_COLLISION_PRIVATE, COL_PEDESTRIAN, COL_ALCOHOL, COL_DRUG,
                    COL_SEATBELT, COL_MOTORCYCLE, COL_MOTOR_HELMET,
                    COL_BICYCLED, COL_BICYCLE_HELMET, COL_WORK_ZONE
            };
            List<String> binaryCols = new ArrayList<>();
            for (String c : ynCandidates) {
                if (Arrays.asList(df.columns()).contains(c)) {
                    String newcol = c + "_bin";
                    df = df.withColumn(newcol,
                            functions.when(df.col(c).isin("Y", "Yes", "1", "TRUE", "True"), 1)
                                    .otherwise(0));
                    binaryCols.add(newcol);
                    // remove original to avoid duplication in features (optional)
                    df = df.drop(c);
                }
            }

            // WORKERS_PRESENT: contains '1','2','N','9' in sample -> create binary presence and numeric
            if (Arrays.asList(df.columns()).contains(COL_WORKERS_PRESENT)) {
                // create bin (present yes/no)
                df = df.withColumn("WORKERS_PRESENT_bin",
                        functions.when(df.col(COL_WORKERS_PRESENT).equalTo("N"), 0).otherwise(1));
                binaryCols.add("WORKERS_PRESENT_bin");
                // drop original textual column
                df = df.drop(COL_WORKERS_PRESENT);
            }

            // SCHOOL BUS: code and desc may exist. keep code numeric if present in numericCols
            // PRIMARY CONTRIBUTING CIRCUMSTANCE CODE is numeric-like already.

            // Recompute categorical list after drops & casts
            List<String> currCols = Arrays.asList(df.columns());
            List<String> currCategoricals = new ArrayList<>();
            List<String> currNumerics = new ArrayList<>();
            for (StructField f : df.schema().fields()) {
                String col = f.name();
                if (col.equals(TARGET) || col.equals("crash_ts")) continue;
                DataType dt = f.dataType();
                if (dt instanceof IntegerType || dt instanceof DoubleType || dt instanceof FloatType || dt instanceof LongType) {
                    currNumerics.add(col);
                } else {
                    currCategoricals.add(col);
                }
            }
            log("ℹ Recomputed numerics: " + currNumerics);
            log("ℹ Recomputed categoricals: " + currCategoricals);

            // Compute cardinalities for categoricals (but guard expensive call with limit)
            log("📊 Computing cardinalities for categorical columns (safe)...");
            Map<String, Long> cardMap = new HashMap<>();
            for (String c : currCategoricals) {
                try {
                    long card = df.select(c).distinct().count();
                    cardMap.put(c, card);
                } catch (Exception ex) {
                    log("⚠ Could not compute cardinality for " + c + " : " + ex.getMessage());
                    cardMap.put(c, Long.MAX_VALUE);
                }
            }

            // Build pipeline stages
            List<PipelineStage> stages = new ArrayList<>();

            // Impute numeric columns (median). Exclude binaryCols from numeric imputation list.
            log("🧮 Imputing numeric null values...");
            List<String> numericForImputer = new ArrayList<>();
            for (String n : currNumerics) {
                if (!binaryCols.contains(n)) numericForImputer.add(n);
            }

            String[] numIn = numericForImputer.toArray(new String[0]);
            String[] numOut = Arrays.stream(numIn).map(s -> s + "_imp").toArray(String[]::new);

            if (numIn.length > 0) {
                Imputer imputer = new Imputer()
                        .setInputCols(numIn)
                        .setOutputCols(numOut)
                        .setStrategy("median");
                stages.add(imputer);
            }

            // Categorical encoding split by cardinality
            log("🔠 Encoding categoricals (OneHot/Index/Hash)...");
            List<String> oheCols = new ArrayList<>();
            List<String> idxCols = new ArrayList<>();
            List<String> hashedCols = new ArrayList<>();

            for (String c : currCategoricals) {
                // skip columns we already dropped or that are internal
                if (!currCols.contains(c) || c.equals(TARGET)) continue;
                long card = cardMap.getOrDefault(c, 0L);

                // Treat tiny cardinality (like Y/N formerly) already handled
                if (card <= 60) {
                    // safe index + onehot (OneHotEncoder supports array)
                    String idx = c + "_idx";
                    String oh = c + "_oh";
                    StringIndexer si = new StringIndexer()
                            .setInputCol(c)
                            .setOutputCol(idx)
                            .setHandleInvalid("keep");
                    OneHotEncoder ohe = new OneHotEncoder()
                            .setInputCols(new String[]{idx})
                            .setOutputCols(new String[]{oh})
                            .setHandleInvalid("keep");
                    stages.add(si);
                    stages.add(ohe);
                    oheCols.add(oh);
                } else if (card <= 2000) {
                    // only index (string -> numeric)
                    String idx = c + "_idx";
                    StringIndexer si = new StringIndexer()
                            .setInputCol(c)
                            .setOutputCol(idx)
                            .setHandleInvalid("keep");
                    stages.add(si);
                    idxCols.add(idx);
                } else {
                    // high-cardinality: hash
                    hashedCols.add(c);
                }
            }

            // FeatureHasher for very large cardinalities
            if (!hashedCols.isEmpty()) {
                FeatureHasher fh = new FeatureHasher()
                        .setInputCols(hashedCols.toArray(new String[0]))
                        .setOutputCol("hashed_features")
                        .setNumFeatures(hashSize);
                stages.add(fh);
            }

            // Assemble features: imputed numerics + binaryCols + oheCols + idxCols + hashed_features + datetime features
            log("🧱 Assembling feature vector...");
            List<String> featuresList = new ArrayList<>();
            featuresList.addAll(Arrays.asList(numOut));
            featuresList.addAll(binaryCols);
            featuresList.addAll(oheCols);
            featuresList.addAll(idxCols);
            if (!hashedCols.isEmpty()) featuresList.add("hashed_features");

            // add datetime derived features if present
            if (Arrays.asList(df.columns()).contains("crash_hour")) featuresList.add("crash_hour");
            if (Arrays.asList(df.columns()).contains("crash_day")) featuresList.add("crash_day");
            if (Arrays.asList(df.columns()).contains("crash_month")) featuresList.add("crash_month");
            if (Arrays.asList(df.columns()).contains("crash_year")) featuresList.add("crash_year");
            if (Arrays.asList(df.columns()).contains("is_weekend")) featuresList.add("is_weekend");

            // Defensive: ensure featuresList not empty
            if (featuresList.isEmpty()) {
                log("❌ No features detected! Exiting.");
                writeErrorToHdfs(hdfsRoot + "/models/vehicle_accident/error.txt",
                        "No features detected after preprocessing", spark);
                System.exit(3);
            }

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(featuresList.toArray(new String[0]))
                    .setOutputCol("assembled")
                    .setHandleInvalid("keep");
            stages.add(assembler);

            // Standard scaler
            StandardScaler scaler = new StandardScaler()
                    .setInputCol("assembled")
                    .setOutputCol("features")
                    .setWithMean(false)
                    .setWithStd(true);
            stages.add(scaler);

            // Label indexing (target)
            log("🏷 Indexing target column...");
            String labelIdx = "label_idx";
            StringIndexer labelIndexer = new StringIndexer()
                    .setInputCol(TARGET)
                    .setOutputCol(labelIdx)
                    .setHandleInvalid("keep");
            stages.add(labelIndexer);

            // Classifier: RandomForest (no CV)
            log("🌲 Training RandomForest (no CV)...");
            RandomForestClassifier rf = new RandomForestClassifier()
                    .setLabelCol(labelIdx)
                    .setFeaturesCol("features")
                    .setNumTrees(80)
                    .setMaxDepth(10)
                    .setSeed(42);
            stages.add(rf);

            // Build pipeline and fit
            Pipeline pipeline = new Pipeline().setStages(stages.toArray(new PipelineStage[0]));

            log("✂ Splitting train/test...");
            Dataset<Row>[] split = df.randomSplit(new double[]{0.8, 0.2}, 42);
            Dataset<Row> train = split[0];
            Dataset<Row> test = split[1];

            log("🚀 Fitting pipeline (this will log progress)...");
            PipelineModel model = pipeline.fit(train);

            log("🧪 Evaluating on test set...");
            Dataset<Row> preds = model.transform(test);
            MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                    .setLabelCol(labelIdx)
                    .setPredictionCol("prediction")
                    .setMetricName("accuracy");
            double acc = 0.0;
            try {
                acc = evaluator.evaluate(preds);
            } catch (Exception e) {
                log("⚠ Evaluation failed: " + e.getMessage());
            }
            log("✅ Test Accuracy = " + acc);

            // Save model to HDFS
            String modelPath = hdfsRoot + "/models/vehicle_accident/pipelineModel";
            log("💾 Saving pipeline model to HDFS: " + modelPath);
            try {
                model.write().overwrite().save(modelPath);
            } catch (Exception e) {
                log("❌ Failed to save model: " + e.getMessage());
                writeErrorToHdfs(hdfsRoot + "/models/vehicle_accident/error.txt",
                        "Failed to save model: " + e.getMessage(), spark);
                throw e;
            }

            // Build metadata.json for GUI (features, binaryCols, labels, topK)
            log("💾 Building metadata.json ...");
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("features", featuresList);
            metadata.put("binaryCols", binaryCols);
            metadata.put("target", TARGET);

            // Extract label names from labelIndexer stage inside model
            String[] labels = new String[0];
            for (PipelineStage s : model.stages()) {
                if (s instanceof StringIndexerModel) {
                    StringIndexerModel sim = (StringIndexerModel) s;
                    if (sim.getOutputCol().equals(labelIdx)) {
                        labels = sim.labels();
                        break;
                    }
                }
            }
            metadata.put("labels", Arrays.asList(labels));

            // Top-K frequent values for original categorical columns (for GUI dropdowns)
            Map<String, List<String>> topKMap = new HashMap<>();
            for (String c : currCategoricals) {
                if (!Arrays.asList(df.columns()).contains(c)) continue;
                try {
                    List<Row> topVals = df.groupBy(c).count().orderBy(functions.desc("count")).limit(topK).collectAsList();
                    List<String> vals = new ArrayList<>();
                    for (Row r : topVals) {
                        Object v = r.get(0);
                        vals.add(v == null ? "__UNKNOWN__" : v.toString());
                    }
                    topKMap.put(c, vals);
                } catch (Exception ex) {
                    log("⚠ Could not compute topK for " + c + " : " + ex.getMessage());
                }
            }
            metadata.put("topK", topKMap);

            // Save metadata.json to HDFS
            writeJsonToHdfs(metadata, hdfsRoot + "/models/vehicle_accident/metadata.json", spark);

            long end = System.currentTimeMillis();
            log("🎉 Training completed and saved successfully! Total time: " + ((end - start) / 1000) + "s");
            spark.stop();

        } catch (Exception ex) {
            log("❌ Pipeline failed with exception: " + ex.getMessage());
            try {
                writeErrorToHdfs(amap.getOrDefault("--hdfsRoot", "hdfs:///projects/project_accident_seriousness") + "/models/vehicle_accident/exception.txt",
                        ex.toString(), spark);
            } catch (Exception e2) {
                log("⚠ Failed to write exception to HDFS: " + e2.getMessage());
            }
            spark.stop();
            throw ex;
        }
    }

    // helpers
    private static Map<String,String> parseArgs(String[] args) {
        Map<String,String> m = new HashMap<>();
        for (int i=0;i<args.length-1;i++)
            if (args[i].startsWith("--")) m.put(args[i], args[i+1]);
        return m;
    }

    private static void writeJsonToHdfs(Object obj, String path, SparkSession spark) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        Path p = new Path(path);
        Configuration hconf = spark.sparkContext().hadoopConfiguration();
        FileSystem fs = p.getFileSystem(hconf);
        if (!fs.exists(p.getParent())) fs.mkdirs(p.getParent());
        try (FSDataOutputStream out = fs.create(p, true)) {
            out.write(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void writeErrorToHdfs(String path, String text, SparkSession spark) {
        try {
            Path p = new Path(path);
            Configuration hconf = spark.sparkContext().hadoopConfiguration();
            FileSystem fs = p.getFileSystem(hconf);
            if (!fs.exists(p.getParent())) fs.mkdirs(p.getParent());
            try (FSDataOutputStream out = fs.create(p, true)) {
                out.write((text + "\n").getBytes(StandardCharsets.UTF_8));
            }
            log("📝 Wrote error info to HDFS: " + path);
        } catch (Exception e) {
            log("⚠ Failed to write error to HDFS: " + e.getMessage());
        }
    }

    private static void log(String msg) {
        System.out.println("[" + new Date() + "] " + msg);
    }
}
