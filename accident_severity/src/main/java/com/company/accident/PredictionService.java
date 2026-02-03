package com.company.accident;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.springframework.stereotype.Service;

@Service
public class PredictionService {

    private final SparkSession spark;
    private final PipelineModel model;

    public PredictionService() {

        spark = SparkSession.builder()
                .appName("AccidentSeverityAPI")
                //.master("local[*]")   // ❌ removed
                .config("spark.hadoop.fs.defaultFS", "hdfs://master:9000")
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .getOrCreate();

        model = PipelineModel.load(
            "hdfs:///projects/project_accident_seriousness/models/vehicle_accident/pipelineModel"
        );
    }

    public double predict(int dayCode, double lat, double lon, int crashHour, int weekend) {

        StructType schema = new StructType(new StructField[]{
                new StructField("DAY_OF_WEEK_CODE", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("LATITUDE", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("LONGITUDE", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("CRASH_HOUR", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("IS_WEEKEND", DataTypes.IntegerType, false, Metadata.empty())
        });

        Row row = RowFactory.create(dayCode, lat, lon, crashHour, weekend);

        Dataset<Row> df = spark.createDataFrame(
                java.util.Collections.singletonList(row), schema
        );

        Dataset<Row> result = model.transform(df);

        return result.select("prediction").first().getDouble(0);
    }
}

