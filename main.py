import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from src import get_range_dates_and_temps

START_DATE = '2010-11-06'
END_DATE = '2019-10-10'
CITIES = ['Kyiv', 'Lviv', 'Kolomyia', 'Dnipro']
DATA_FILE = os.path.join('data', 'temperatures.csv')
GENERATE_NEW_DATASET = True
SAVE_RESULTS = False  # TODO: resolve problems with saving result csv's

if __name__ == '__main__':
    if not os.path.isdir('./data'):
        os.makedirs('data')

    if GENERATE_NEW_DATASET or not os.path.isfile(DATA_FILE):
        temperatures = get_range_dates_and_temps(CITIES, START_DATE, END_DATE)
        temperatures.to_csv(DATA_FILE, index=False)

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    df = spark.read.format("csv").option("header", "true").load(DATA_FILE)
    print('Source data frame:')
    df.show()
    df = df.withColumn("temperatureTmp", df.temperature.cast('float')) \
        .drop("temperature") \
        .withColumnRenamed("temperatureTmp", "temperature")

    df = df.withColumn('month', F.month('date'))

    print('Min, max and avg temperature for each month for each city:')
    temp_df = df.groupBy('city', 'month').agg(F.max(F.col('temperature')).alias('max_temperature'),
                                              F.min(F.col('temperature')).alias('min_temperature'),
                                              F.avg(F.col('temperature')).alias('avg_temperature')).orderBy('month',
                                                                                                            'city')
    temp_df.show()

    if SAVE_RESULTS:
        temp_df.write.csv('data/agg_city_month.csv')

    N = 255  # min examples to take
    print('Min, max and avg temperature for each month for each city having at least {} records:'.format(N))
    temp_df = df.groupBy('city', 'month').agg(F.max(F.col('temperature')).alias('max_temperature'),
                                              F.min(F.col('temperature')).alias('min_temperature'),
                                              F.avg(F.col('temperature')).alias('avg_temperature'),
                                              F.count(F.col('temperature')).alias('count_temperature')) \
        .orderBy('month', 'city')
    temp_df = temp_df.where(temp_df.count_temperature >= N)
    temp_df.show()

    if SAVE_RESULTS:
        temp_df.write.csv('data/agg_city_month_n.csv')

    print('Differences between values and aggregated values:')
    wind = Window.partitionBy('city', 'month')
    max_difference = (F.max(df['temperature']).over(wind) - df['temperature'])
    min_difference = (F.min(df['temperature']).over(wind) - df['temperature'])
    avg_difference = (F.avg(df['temperature']).over(wind) - df['temperature'])

    temp_df = df.select(df['city'], df['date'], df['temperature'],
                        max_difference.alias("diff_max"),
                        min_difference.alias("diff_min"),
                        avg_difference.alias("diff_avg"))
    temp_df.show()

    if SAVE_RESULTS:
        temp_df.write.csv('data/agg_differences.csv')

    cities_to_take = ['Kyiv', 'Lviv']
    print('Min, max and avg temperature for all time for set of cities: {}'.format(cities_to_take))
    temp_df = df.where(df.city.isin(cities_to_take)) \
        .groupBy('city') \
        .agg(F.max(F.col('temperature')).alias('max_temperature'),
             F.min(F.col('temperature')).alias('min_temperature'),
             F.avg(F.col('temperature')).alias('avg_temperature'))
    temp_df.show()

    if SAVE_RESULTS:
        temp_df.write.csv('data/agg_city_selection.csv')
