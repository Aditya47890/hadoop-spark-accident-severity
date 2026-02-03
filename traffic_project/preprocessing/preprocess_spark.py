# preprocess_spark.py
# Robust, production-level preprocessing for the provided accident CSV schema.
# Compatible with: PySpark 3.3.2, Python 3.9, Hadoop 3.3.6, Spark-on-YARN cluster.

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_timestamp, year, month, dayofmonth, hour, dayofweek,
    lower, regexp_replace, lit, concat_ws
)
from pyspark.sql.types import DoubleType, IntegerType
import re
import sys

# ---------------------------
# 0. Start Spark
# ---------------------------
spark = SparkSession.builder \
    .appName("TrafficAccidentPreprocessing") \
    .getOrCreate()

print("🔥 Spark session started")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# ---------------------------
# 1. Paths (edit if needed)
# ---------------------------
INPUT = "hdfs:///traffic/raw/accidents.csv"      # raw CSV already uploaded
OUTPUT_PARQUET = "hdfs:///traffic/cleaned/parquet/"
OUTPUT_SAMPLE_CSV = "hdfs:///traffic/gui/sample_cleaned.csv"  # small CSV for GUI/dev

# ---------------------------
# 2. Read CSV (header + infer)
# ---------------------------
df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT)
print(f"📥 Loaded raw rows = {df.count()}")

# ---------------------------
# 3. Normalize column names to safe snake_case
# ---------------------------
def norm(name: str) -> str:
    return re.sub(r'[^0-9a-z_]', '',
                  re.sub(r'\s+', '_', name.strip().lower()).replace('-', '_').replace('/', '_'))

for c in df.columns:
    newc = norm(c)
    if newc != c:
        df = df.withColumnRenamed(c, newc)

# Now expected columns (lower/underscore) include:
# crash_datetime, day_of_week_code, day_of_week_description, crash_classification_code,
# crash_classification_description, collision_on_private_property, pedestrian_involved, ...
# latitude, longitude, the_geom, etc.

# ---------------------------
# 4. Robust timestamp parsing
# Try several common formats and keep first non-null
# ---------------------------
fmt_list = [
    "yyyy-MM-dd HH:mm:ss",   # 2023-02-26 14:30:00
    "d/M/yyyy H:mm:ss",      # 26/2/2023 14:30:00
    "d/M/yyyy H:mm",         # 26/2/2023 14:30
    "dd/MM/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss"
]

# attempt each format in sequence
df = df.withColumn("crash_datetime_raw", col("crash_datetime"))
df = df.withColumn("crash_ts", to_timestamp(col("crash_datetime"), fmt_list[0]))
for fmt in fmt_list[1:]:
    df = df.withColumn("crash_ts", when(col("crash_ts").isNull(), to_timestamp(col("crash_datetime"), fmt)).otherwise(col("crash_ts")))

# if still null and 'crash_datetime' looks like epoch millis/seconds, try cast
df = df.withColumn("crash_ts", when(col("crash_ts").isNull() & col("crash_datetime").cast("long").isNotNull(),
                                    to_timestamp((col("crash_datetime").cast("long")))).otherwise(col("crash_ts")))

# final guard: keep original field if parsing failed (we will drop later)
print("🕒 Timestamp parsing done. Null timestamps:", df.filter(col("crash_ts").isNull()).count())

# ---------------------------
# 5. If latitude/longitude missing but the_geom present (WKT), extract coordinates
# Supports WKT POINT (lon lat) or POINT(lon lat)
# ---------------------------
if "latitude" not in df.columns or "longitude" not in df.columns:
    print("ℹ latitude/longitude not found as columns; will attempt to parse the_geom if present")
if "the_geom" in df.columns:
    # create lat/lon if missing
    def parse_wkt(wkt):
        if wkt is None: 
            return (None, None)
        # find numbers in WKT "POINT (lon lat)" or "POINT(lon lat)"
        m = re.search(r'POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)', wkt, re.IGNORECASE)
        if m:
            lon = float(m.group(1)); lat = float(m.group(2))
            return (lat, lon)
        return (None, None)
    from pyspark.sql.types import StructType, StructField
    from pyspark.sql.functions import udf
    parse_wkt_udf = udf(lambda s: parse_wkt(s), "struct<latitude:double,longitude:double>")
    parsed = df.select("the_geom").rdd.map(lambda r: None).count()  # noop to ensure serialization works
    # apply UDF and merge
    df = df.withColumn("__geom_parsed", parse_wkt_udf(col("the_geom")))
    if "latitude" not in df.columns:
        df = df.withColumn("latitude", col("__geom_parsed.latitude"))
    else:
        df = df.withColumn("latitude", when(col("latitude").isNull(), col("__geom_parsed.latitude")).otherwise(col("latitude")))
    if "longitude" not in df.columns:
        df = df.withColumn("longitude", col("__geom_parsed.longitude"))
    else:
        df = df.withColumn("longitude", when(col("longitude").isNull(), col("__geom_parsed.longitude")).otherwise(col("longitude")))
    df = df.drop("__geom_parsed")

# ---------------------------
# 6. Cast lat/lon to double and filter invalid coords
# ---------------------------
if "latitude" in df.columns:
    df = df.withColumn("latitude", col("latitude").cast(DoubleType()))
if "longitude" in df.columns:
    df = df.withColumn("longitude", col("longitude").cast(DoubleType()))

# remove obviously invalid rows (nulls or zeros or out-of-range)
if "latitude" in df.columns and "longitude" in df.columns:
    df = df.filter(
        (col("latitude").isNotNull()) &
        (col("longitude").isNotNull()) &
        (col("latitude") >= -90) & (col("latitude") <= 90) &
        (col("longitude") >= -180) & (col("longitude") <= 180)
    )
print("📍 Rows after coord validation:", df.count())

# ---------------------------
# 7. Time features: year, month, day, hour, day_of_week, is_weekend, season
# ---------------------------
df = df.withColumn("year", year(col("crash_ts")))
df = df.withColumn("month", month(col("crash_ts")))
df = df.withColumn("day", dayofmonth(col("crash_ts")))
df = df.withColumn("hour", hour(col("crash_ts")))
# dayofweek returns 1 = Sunday ... 7 = Saturday in Spark SQL; convert to 1=Mon..7=Sun if needed
df = df.withColumn("dofw", dayofweek(col("crash_ts")))
# weekend when dofw in (1,7) depending on your locale; use common approach: weekend = Sat(7) or Sun(1)
df = df.withColumn("is_weekend", when(col("dofw").isin(1,7), lit(1)).otherwise(lit(0)))
df = df.withColumn("season",
                   when(col("month").isin(12,1,2), lit("winter"))
                   .when(col("month").isin(3,4,5), lit("spring"))
                   .when(col("month").isin(6,7,8), lit("summer"))
                   .when(col("month").isin(9,10,11), lit("autumn"))
                   .otherwise(lit("unknown"))
                  )

# ---------------------------
# 8. Binary conversion helper (covers 'Y', 'Yes', '1', 'True', 'T', 'N', 'No', '0', etc.)
# ---------------------------
def binify(colname):
    return when(col(colname).rlike("(?i)^(y|yes|true|t|1)$"), lit(1)).when(col(colname).rlike("(?i)^(n|no|false|f|0)$"), lit(0)).otherwise(lit(None))

bin_cols = [
    "collision_on_private_property","pedestrian_involved","alcohol_involved","drug_involved",
    "seatbelt_used","motorcycle_involved","motorcycle_helmet_used","bicycled_involved","bicycle_helmet_used",
    "school_bus_involved_code","work_zone","workers_present"
]

for b in bin_cols:
    if b in df.columns:
        df = df.withColumn(b + "_bin", binify(b))

# ---------------------------
# 9. Map crash_classification_description -> severity_label (3 = fatal, 2 = major/serious, 1 = slight/minor, 0 unknown)
# ---------------------------
if "crash_classification_description" in df.columns:
    df = df.withColumn("severity_label",
        when(col("crash_classification_description").rlike("(?i)fatal"), lit(3))
        .when(col("crash_classification_description").rlike("(?i)serious|major|severe"), lit(2))
        .when(col("crash_classification_description").rlike("(?i)slight|minor"), lit(1))
        .otherwise(lit(0))
    )
elif "crash_classification_code" in df.columns:
    # fallback: treat numeric codes >=3 as severe etc. (adjust if your codes differ)
    df = df.withColumn("severity_label", when(col("crash_classification_code").cast("int") >= 3, lit(2)).otherwise(lit(1)))

print("📊 severity distribution (sample):")
df.groupBy("severity_label").count().show(10,False)

# ---------------------------
# 10. Risk score: an interpretable, tunable formula
# ---------------------------
# Simple formula: base = severity_label * 10, plus weights for alcohol, pedestrian, drug, work_zone, motorcycle, seatbelt absence
w_alcohol = 5
w_drug = 5
w_ped = 7
w_work = 4
w_motor = 3
w_no_seatbelt = 4

expr = col("severity_label") * lit(10)
if "alcohol_involved_bin" in df.columns:
    expr = expr + col("alcohol_involved_bin") * lit(w_alcohol)
if "drug_involved_bin" in df.columns:
    expr = expr + col("drug_involved_bin") * lit(w_drug)
if "pedestrian_involved_bin" in df.columns:
    expr = expr + col("pedestrian_involved_bin") * lit(w_ped)
if "work_zone_bin" in df.columns:
    expr = expr + col("work_zone_bin") * lit(w_work)
if "motorcycle_involved_bin" in df.columns:
    expr = expr + col("motorcycle_involved_bin") * lit(w_motor)
# seatbelt_used_bin: if 0 (not used), add risk
if "seatbelt_used_bin" in df.columns:
    expr = expr + when(col("seatbelt_used_bin") == 0, lit(w_no_seatbelt)).otherwise(lit(0))

df = df.withColumn("risk_score", expr)

# ---------------------------
# 11. Impute numeric nulls with median (approx)
# ---------------------------
numeric_types = ("double","int","bigint","float","long")
num_cols = [c for c,t in df.dtypes if t in numeric_types]
# remove columns we don't want to median-impute (like long text numeric codes), but safe approach:
for c in num_cols:
    try:
        median = df.approxQuantile(c, [0.5], 0.01)[0]
        if median is None:
            continue
        df = df.withColumn(c, when(col(c).isNull(), lit(float(median))).otherwise(col(c)))
    except Exception as e:
        # some columns won't support approxQuantile (non-numeric); ignore
        pass

# ---------------------------
# 12. Fill string nulls with 'unknown'
# ---------------------------
str_cols = [c for c,t in df.dtypes if t == "string"]
for c in str_cols:
    df = df.withColumn(c, when(col(c).isNull(), lit("unknown")).otherwise(col(c)))

# ---------------------------
# 13. Select final columns to save (keeps useful columns + bins + engineered)
# ---------------------------
keep = [
    "crash_ts", "year", "month", "day", "hour", "dofw", "is_weekend", "season",
    "latitude", "longitude",
    "severity_label", "risk_score"
]

# add binary flags present
keep += [c for c in df.columns if c.endswith("_bin")]

# add some important descriptive columns if present
desc_cols = [
    "primary_contributing_circumstance_description",
    "manner_of_impact_description",
    "road_surface_description",
    "lighting_condition_description",
    "weather_1_description",
    "weather_2_description",
    "work_zone_type_description",
    "work_zone_location_description"
]
keep += [c for c in desc_cols if c in df.columns]

# ensure we only keep columns that actually exist
keep = [c for c in keep if c in df.columns]

df_final = df.select(*keep)

print("✅ Final columns to write:", keep)

# ---------------------------
# 14. Write Parquet partitioned by year/month (snappy)
# ---------------------------
df_final = df_final.withColumnRenamed("crash_ts", "timestamp")
df_final.write.mode("overwrite").partitionBy("year","month").option("compression","snappy").parquet(OUTPUT_PARQUET)
print("📦 Written cleaned parquet to:", OUTPUT_PARQUET)

# Also write small sample CSV (first 50k rows) to HDFS for GUI dev (coalesce to 1 file)
sample = df_final.orderBy(col("risk_score").desc()).limit(50000)
sample.coalesce(1).write.mode("overwrite").option("header", True).csv(OUTPUT_SAMPLE_CSV)
print("🧾 Written GUI sample CSV to:", OUTPUT_SAMPLE_CSV)

# ---------------------------
# 15. Quick verification prints
# ---------------------------
print("Rows in cleaned dataset:", spark.read.parquet(OUTPUT_PARQUET).count())
spark.read.parquet(OUTPUT_PARQUET).select("severity_label").groupBy("severity_label").count().show()

# ---------------------------
# 16. Stop
# ---------------------------
spark.stop()
print("✨ Preprocessing finished")
