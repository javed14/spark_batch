from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import functions as F


def hotels_data():
    topicName = "hotels_topic"
    sparkSession = SparkSession.builder.master("local").appName("GetKafkaTopicData").getOrCreate()

    df = sparkSession.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("test/resource/hotels.csv")

    interval = df.select(F.col("value").cast("string")).alias("csv").select("csv.*")

    return interval \
        .selectExpr("split(value,',')[0] as Id" \
                    , "split(value,',')[1] as Name" \
                    , "split(value,',')[2] as Country" \
                    , "split(value,',')[3] as City" \
                    , "split(value,',')[4] as Address" \
                    , "split(value,',')[5] as Latitude" \
                    , "split(value,',')[5] as Longitude" \
                    ).toDF("Id", "Name", "Country", "City", "Address", "Latitude", "Longitude")




def expedia_data():
    sparkSession = pyspark.sql.SparkSession \
        .builder \
        .appName("root") \
        .getOrCreate()
    return sparkSession.read.csv("test/resource/expedia.csv")


def calculate_idle_days(expedia_data):
    return expedia_data.select(F.col("srch_ci"), F.col("srch_co"),
                               F.datediff(F.col("srch_co"), F.col("srch_ci")).alias("datediff"))


def remove_booking_data(expedia_data):
    return expedia_data.select(F.col("srch_ci"), F.col("srch_co"),
                               F.datediff(F.col("srch_co"), F.col("srch_ci")).alias("datediff")) \
        .filter((F.col("datediff") >= 2) & (F.col("datediff") <= 30))


def joinData(expedia_data, hotels_data):
    return expedia_data.join(hotels_data.withColumn("id", F.col("Id")), on=['id'], how='inner')


def invalid_hotels(joinData):
    return joinData.select(F.col("Name"), F.col("Country"), F.col("City"), F.col("srch_ci"), F.col("srch_co"),
                           F.datediff(F.col("srch_co"), F.col("srch_ci")).alias("datediff")) \
        .filter(~(F.col("datediff") >= 2) & (F.col("datediff") <= 30))


def group_hotels(joinData):
    return joinData.groupBy(F.col("Country"), F.col("City")).count()



if __name__ == '__main__':
    expedia_data = expedia_data()
    hotels_data = hotels_data()
    #   weather_data = weather_data()
    # print("expedia_data :", expedia_data.show(), "\n")
    #   print("hotel_data :", hotels_data.count(), "\n")
    #   print("weather_data", weather_data.count(), "\n")
    # print(calculate_idle_days(expedia_data))
   # expedia_data.repartition(1).write.option("header", "true").csv("expedia_data")
    print(remove_booking_data(expedia_data).show())
    joinData = joinData(expedia_data, hotels_data)
# print("Joined Data: ", joinData.count())
    invalid_hotels(joinData).show()
    group_hotels(joinData).show()
