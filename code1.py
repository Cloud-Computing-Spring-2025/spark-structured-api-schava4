from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, hour, desc, current_date, weekofyear, row_number,
    min as spark_min, max as spark_max
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("MusicListenerAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read the datasets (adjust the file paths if needed)
logs_df = spark.read.option("header", "true").csv("listening_logs.csv")
songs_df = spark.read.option("header", "true").csv("songs_metadata.csv")

# Cast columns to appropriate data types
logs_df = logs_df.withColumn("duration_sec", col("duration_sec").cast("integer")) \
                 .withColumn("timestamp", col("timestamp").cast("timestamp"))
# Enrich logs with song metadata
enriched_df = logs_df.join(songs_df, on="song_id", how="left")

# Save enriched logs (for reference)
enriched_df.write.format("csv").mode("overwrite").save("output/enriched_logs")

# Debug: Print timestamp range to help verify date filtering
print("Timestamp Range in Data:")
enriched_df.select(
    spark_min("timestamp").alias("min_timestamp"),
    spark_max("timestamp").alias("max_timestamp")
).show(truncate=False)

# --------------------------
# Task 1: Favorite Genre per User
# Count plays per user and genre
genre_count_df = enriched_df.groupBy("user_id", "genre") \
    .agg(count("song_id").alias("play_count"))

# Define a window to rank genres by play count within each user group
windowSpec = Window.partitionBy("user_id").orderBy(desc("play_count"))
favorite_genre_df = genre_count_df.withColumn("rank", row_number().over(windowSpec)) \
                                  .filter(col("rank") == 1) \
                                  .drop("rank")

# Save favorite genres per user
favorite_genre_df.write.format("csv").mode("overwrite").save("output/user_favorite_genres")
print("Favorite Genre per User:")
favorite_genre_df.show(truncate=False)

# --------------------------
# Task 2: Average Listen Time per Song
avg_listen_time_df = enriched_df.groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration_sec"))

# Save average listen times
avg_listen_time_df.write.format("csv").mode("overwrite").save("output/avg_listen_time_per_song")
print("Average Listen Time per Song:")
avg_listen_time_df.show(truncate=False)

# --------------------------
# Task 3: Top 10 Most Played Songs This Week
# Filter records for the current week
logs_this_week_df = enriched_df.filter(weekofyear(col("timestamp")) == weekofyear(current_date()))
print("Logs This Week Count: ", logs_this_week_df.count())

top_songs_df = logs_this_week_df.groupBy("song_id", "title", "artist") \
    .agg(count("song_id").alias("play_count")) \
    .orderBy(desc("play_count")) \
    .limit(10)

# Save top songs of this week
top_songs_df.write.format("csv").mode("overwrite").save("output/top_songs_this_week")
print("Top 10 Most Played Songs This Week:")
top_songs_df.show(truncate=False)

# --------------------------
# Task 4: Recommend “Happy” Songs to Users who Mostly Listen to “Sad” Songs
# 1. Calculate the mood distribution per user
user_mood_counts = enriched_df.groupBy("user_id", "mood") \
    .agg(count("song_id").alias("play_count"))
total_plays = enriched_df.groupBy("user_id") \
    .agg(count("song_id").alias("total_plays"))

# Get count of "Sad" plays per user and calculate ratio
sad_plays = user_mood_counts.filter(col("mood") == "Sad") \
    .select("user_id", col("play_count").alias("sad_play_count"))
sad_ratio_df = sad_plays.join(total_plays, on="user_id") \
    .withColumn("sad_ratio", col("sad_play_count") / col("total_plays"))

print("Users' Sad Ratios:")
sad_ratio_df.show(truncate=False)

# Assume users with >= 50% "Sad" plays are candidates for recommendations
sad_users_df = sad_ratio_df.filter(col("sad_ratio") >= 0.5).select("user_id")
print("Users with Sad Ratio >= 0.5:")
sad_users_df.show(truncate=False)

# 2. Get list of "Happy" songs from song metadata
happy_songs_df = songs_df.filter(col("mood") == "Happy").select("song_id", "title", "artist")
print("Happy Songs:")
happy_songs_df.show(truncate=False)

# 3. Identify songs each user has already listened to
user_songs_df = enriched_df.select("user_id", "song_id").distinct()

# 4. For each sad user, join with happy songs then exclude songs already played
sad_users_recs = sad_users_df.crossJoin(happy_songs_df) \
    .join(user_songs_df, on=["user_id", "song_id"], how="left_anti")

# Limit recommendations to up to 3 songs per user using a window function
windowSpecRec = Window.partitionBy("user_id").orderBy("song_id")
recommendations_df = sad_users_recs.withColumn("rank", row_number().over(windowSpecRec)) \
    .filter(col("rank") <= 3) \
    .drop("rank")

# Save recommendations
recommendations_df.write.format("csv").mode("overwrite").save("output/happy_recommendations")
print("Happy Song Recommendations for Users with High Sad Ratio:")
recommendations_df.show(truncate=False)

# --------------------------
# Task 5: Compute Genre Loyalty Score per User
# Calculate total plays per user
user_total_plays = enriched_df.groupBy("user_id").agg(count("song_id").alias("total_plays"))

# Reusing favorite_genre_df which has the play_count for the top genre per user
user_loyalty_df = favorite_genre_df.join(user_total_plays, on="user_id") \
    .withColumn("loyalty_score", col("play_count") / col("total_plays"))

print("User Genre Loyalty Scores:")
user_loyalty_df.select("user_id", "genre", "play_count", "total_plays", "loyalty_score").show(truncate=False)

# Select users with loyalty scores above 0.8
loyal_users_df = user_loyalty_df.filter(col("loyalty_score") > 0.8)
loyal_users_df.write.format("csv").mode("overwrite").save("output/genre_loyalty_scores")
print("Users with Loyalty Score > 0.8:")
loyal_users_df.select("user_id", "genre", "loyalty_score").show(truncate=False)

# --------------------------
# Task 6: Identify Night Owl Users (Listening between 12 AM and 5 AM)
night_owl_df = enriched_df.withColumn("hour", hour(col("timestamp"))) \
    .filter((col("hour") >= 0) & (col("hour") < 5)) \
    .select("user_id").distinct()

# Save night owl users
night_owl_df.write.format("csv").mode("overwrite").save("output/night_owl_users")
print("Night Owl Users:")
night_owl_df.show(truncate=False)

# Stop the Spark session
spark.stop()