# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd

# Spark SQL
df = spark.table("workspace.default.salon_spa_raw_data")

display(df.limit(50))


# COMMAND ----------

# DBTITLE 1,Total revenue per service
from pyspark.sql.functions import sum

revenue_df = df.filter(df.status == "booked") \
               .groupBy("service_name") \
               .agg(sum("amount_spent").alias("total_revenue"))

display(revenue_df)

# COMMAND ----------

# DBTITLE 1,Average service duration by city
from pyspark.sql.functions import avg

avg_duration_df = df.filter(df.status == "booked") \
                    .groupBy("city") \
                    .agg(avg("duration").alias("avg_duration_minutes"))

display(avg_duration_df)

# COMMAND ----------

# DBTITLE 1,Most frequently booked services
from pyspark.sql.functions import count

popular_services_df = df.filter(df.status == "booked") \
                        .groupBy("service_name") \
                        .agg(count("*").alias("total_bookings")) \
                        .orderBy("total_bookings", ascending=False)

display(popular_services_df)

# COMMAND ----------

df.createOrReplaceTempView("salon_spa")

# COMMAND ----------

# DBTITLE 1,Services with highest cancellation/no-show rates
# MAGIC %sql
# MAGIC SELECT service_name,
# MAGIC        SUM(CASE WHEN status IN ('cancelled','no-show') THEN 1 ELSE 0 END) AS cancellations,
# MAGIC        COUNT(*) AS total_bookings,
# MAGIC        ROUND(SUM(CASE WHEN status IN ('cancelled','no-show') THEN 1 ELSE 0 END) / COUNT(*) * 100,2) AS cancellation_rate
# MAGIC FROM salon_spa
# MAGIC GROUP BY service_name
# MAGIC ORDER BY cancellation_rate DESC;

# COMMAND ----------

# DBTITLE 1,Peak booking times (by hour, day of week)
# MAGIC %sql
# MAGIC SELECT hour(booking_time) AS booking_hour,
# MAGIC        date_format(booking_date, 'EEEE') AS day_of_week,
# MAGIC        COUNT(*) AS total_bookings
# MAGIC FROM salon_spa
# MAGIC WHERE status = 'booked'
# MAGIC GROUP BY booking_hour, day_of_week
# MAGIC ORDER BY total_bookings DESC;

# COMMAND ----------

# DBTITLE 1,Seasonal/monthly revenue trends
# MAGIC %sql
# MAGIC SELECT date_format(booking_date, 'yyyy-MM') AS month,
# MAGIC        city,
# MAGIC        service_name,
# MAGIC        SUM(amount_spent) AS total_revenue
# MAGIC FROM salon_spa
# MAGIC WHERE status = 'booked'
# MAGIC GROUP BY date_format(booking_date, 'yyyy-MM'), city, service_name
# MAGIC ORDER BY month;