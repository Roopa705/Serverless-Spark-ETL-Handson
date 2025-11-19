import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, coalesce, lit
from awsglue.dynamicframe import DynamicFrame

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Define S3 Paths (Updated with your new names) ---
s3_input_path = "s3://roopa-handsonfinallanding/"
s3_processed_path = "s3://roopa-handsonfinalprocessed/processed-data/"
s3_analytics_path = "s3://roopa-handsonfinalprocessed/Athena Results/"

# --- Read the data from the S3 landing zone ---
print(f"Reading data from {s3_input_path}...")
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
)

# Convert to a standard Spark DataFrame for easier transformation
df = dynamic_frame.toDF()

# --- Perform Transformations ---
# 1. Cast 'rating' to integer and fill null values with 0
df_transformed = df.withColumn("rating", coalesce(col("rating").cast("integer"), lit(0)))

# 2. Convert 'review_date' string to a proper date type
df_transformed = df_transformed.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

# 3. Fill null review_text with a default string
df_transformed = df_transformed.withColumn("review_text",
    coalesce(col("review_text"), lit("No review text")))

# 4. Convert product_id to uppercase for consistency
df_transformed = df_transformed.withColumn("product_id_upper", upper(col("product_id")))

# --- Write the full transformed data to S3 (Good practice) ---
print(f"Writing processed data to {s3_processed_path}...")
glue_processed_frame = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_df")
glueContext.write_dynamic_frame.from_options(
    frame=glue_processed_frame,
    connection_type="s3",
    connection_options={"path": s3_processed_path},
    format="csv"
)

# --- Run Spark SQL Query within the Job ---

# 1. Create a temporary view in Spark's memory for all subsequent queries
df_transformed.createOrReplaceTempView("product_reviews")
print("Temporary view 'product_reviews' created.")


# --- Existing Query: Product Average Rating and Count ---
print(f"Running Query 1: Product Average Rating and Count...")
df_analytics_result_1 = spark.sql("""
    SELECT 
        product_id_upper, 
        AVG(rating) as average_rating,
        COUNT(*) as review_count
    FROM product_reviews
    GROUP BY product_id_upper
    ORDER BY average_rating DESC
""")

# Write the query's result DataFrame to S3
path_q1 = s3_analytics_path + "product-avg-rating/"
print(f"Writing Query 1 results to {path_q1}...")
analytics_result_frame_1 = DynamicFrame.fromDF(df_analytics_result_1.repartition(1), glueContext, "analytics_df_1")
glueContext.write_dynamic_frame.from_options(
    frame=analytics_result_frame_1,
    connection_type="s3",
    connection_options={"path": path_q1},
    format="csv"
)


# 2. Date wise review count: This query calculates the total number of reviews submitted per day.
print(f"Running Query 2: Date Wise Review Count...")
df_analytics_result_2 = spark.sql("""
    SELECT
        review_date,
        COUNT(*) as daily_review_count
    FROM product_reviews
    GROUP BY review_date
    ORDER BY review_date
""")

# Write the query's result DataFrame to S3
path_q2 = s3_analytics_path + "daily-review-count/"
print(f"Writing Query 2 results to {path_q2}...")
analytics_result_frame_2 = DynamicFrame.fromDF(df_analytics_result_2.repartition(1), glueContext, "analytics_df_2")
glueContext.write_dynamic_frame.from_options(
    frame=analytics_result_frame_2,
    connection_type="s3",
    connection_options={"path": path_q2},
    format="csv"
)


# 3. Top 5 Most Active Customers: This query identifies your "power users" by finding the customers who have submitted the most reviews.
print(f"Running Query 3: Top 5 Most Active Customers...")
df_analytics_result_3 = spark.sql("""
    SELECT
        customer_id,
        COUNT(*) as review_count
    FROM product_reviews
    GROUP BY customer_id
    ORDER BY review_count DESC
    LIMIT 5
""")

# Write the query's result DataFrame to S3
path_q3 = s3_analytics_path + "top-customers/"
print(f"Writing Query 3 results to {path_q3}...")
analytics_result_frame_3 = DynamicFrame.fromDF(df_analytics_result_3.repartition(1), glueContext, "analytics_df_3")
glueContext.write_dynamic_frame.from_options(
    frame=analytics_result_frame_3,
    connection_type="s3",
    connection_options={"path": path_q3},
    format="csv"
)


# 4. Overall Rating Distribution: This query shows the count for each star rating (1-star, 2-star, etc.).
# Note: Rating 0 is included due to the transformation filling nulls with 0.
print(f"Running Query 4: Overall Rating Distribution...")
df_analytics_result_4 = spark.sql("""
    SELECT
        rating,
        COUNT(*) as rating_count
    FROM product_reviews
    GROUP BY rating
    ORDER BY rating
""")

# Write the query's result DataFrame to S3
path_q4 = s3_analytics_path + "rating-distribution/"
print(f"Writing Query 4 results to {path_q4}...")
analytics_result_frame_4 = DynamicFrame.fromDF(df_analytics_result_4.repartition(1), glueContext, "analytics_df_4")
glueContext.write_dynamic_frame.from_options(
    frame=analytics_result_frame_4,
    connection_type="s3",
    connection_options={"path": path_q4},
    format="csv"
)

print("All queries completed and results saved to S3.")
job.commit()