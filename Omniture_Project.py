from pyspark.sql import *

from pyspark.sql.types import StructField, StructType, TimestampType, StringType

spark = SparkSession \
    .builder \
    .config("spark.executor.memory", "10G") \
    .config("spark.crossJoin.enabled", "True") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.logConf", "True") \
    .config("spark.cores.max", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("INFO")
omniture_path = "<file path here>"
regusers_path = "<file path here>"
url_map_path = "<file path here>"

regusersDF = spark.read.csv(path=regusers_path, header=True, inferSchema=True, sep='\t')
url_map_pathDF = spark.read.csv(url_map_path, inferSchema=True, header=True, sep='\t')
omnitureDF = spark.read.csv(omniture_path, sep='\t')
schma = StructType([StructField('timeStamp', StringType(), True),
                    StructField('ipAddress', StringType(), True),
                    StructField('url', StringType(), True),
                    StructField('sw_id', StringType(), True),
                    StructField('city', StringType(), True),
                    StructField('country', StringType(), True),
                    StructField('state', StringType(), True)
                    ])
# as per req. i need below columns only
cols = ['_c1', '_c7', '_c12', '_c13', '_c49', '_c50', '_c52']
omnitureFinalDF = omnitureDF.select(*cols)

omnitureFinalSchemaDF = spark.createDataFrame(omnitureFinalDF.rdd, schema=schma)

omnitureFinalSchemaDF.createOrReplaceTempView('omniture_table')
omnitureFinalSchemaDF_timeStampCasted_data = spark.sql(
    'select cast(timeStamp as timestamp) as TimeStamp, ipAddress, url, substr(sw_id, 2, length(sw_id) - 2) as sw_id, '
    'city, country from omniture_table')
omnitureFinalSchemaDF_timeStampCasted_data.createOrReplaceTempView("omniture")
print(spark.sql('select timeStamp from omniture limit 1').show())
regusersDF.createOrReplaceTempView("regusers")
print(spark.sql('select birth_dt from regusers limit 1').show())
url_map_pathDF.createOrReplaceTempView("url_map")
print("Dataframe has been registered as temp table.......")
final_ouput = spark.sql(''' 
SELECT to_date(omniture.TimeStamp) as logdate,
    omniture.url,
        omniture.ipAddress,
            omniture.city,
                UPPER(omniture.city) as state,
                    omniture.country,
                p.category,
            CAST(
                DATEDIFF( FROM_UNIXTIME(UNIX_TIMESTAMP()),
                            FROM_UNIXTIME(UNIX_TIMESTAMP(u.birth_dt,'dd-MMM-yy')))/365 AS INT) as age,
        u.gender_cd 
FROM omniture
INNER JOIN url_map p
ON omniture.url = p.url
LEFT OUTER JOIN regusers u
ON omniture.sw_id = u.SWID ''')


final_ouput.repartition(1).write.format("delta").save("C:\\spark-2.4.5-bin-hadoop2.7\\data\\tmp\\OmnitureDeltaTable")
print(final_ouput.show())
