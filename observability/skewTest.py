#****************************************************************************
# (C) Cloudera, Inc. 2020-2025
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import random
import string

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OutOfMemoryExample") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

def generate_skewed_data(num_records, skew_factor=100):
    """
    Generates skewed data by creating many records for a few 'id' values.
    """
    data = []
    for _ in range(num_records):
        if random.random() < 0.05:  # 5% chance for a 'popular' id
            id_value = 'POPULAR_ID'
        else:
            id_value = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        amount = random.randint(1, 1000)
        data.append((id_value, amount))
    return spark.createDataFrame(data, ["id", "amount"])

# Generate random data
def generate_data(num_records, num_partitions=4):
    """
    Generates two datasets with random values. This will simulate a large shuffle
    when a join operation is performed later.
    """
    data = []
    for _ in range(num_records):
        id_value = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        amount = random.randint(1, 1000)
        data.append((id_value, amount))
    return spark.createDataFrame(data, ["id", "amount"])

def generate_oom():
    """
    Generates two datasets with random values. This will simulate a large shuffle
    when a join operation is performed later.
    """
    data = []
    for _ in range(1000000000000000000):
        id_value = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        amount = random.randint(1, 1000)
        data.append((id_value, amount))
    return spark.createDataFrame(data, ["id", "amount"])

# Create two large datasets
ifSkew = 1

if ifSkew < 2:
    oom_df = generate_oom()
    try:
        collected_data = oom_df.collect()
        print(f"Collected {len(collected_data)} rows")
    except Exception as e:
        print(f"Error {e}")

elif ifSkew < 8:
    # AQE Disabled
    spark.conf.set("spark.sql.adaptive.enabled", False)
    df1 = generate_skewed_data(1000000)
    df2 = generate_skewed_data(1000000)
    print("Skewed Data Created")

elif ifSkew < 28:
    # Create a skewed dataset
    df1 = generate_skewed_data(1000000)
    df2 = generate_skewed_data(1000000)
    print("Skewed Data Created")

else:
    df1 = generate_data(1000000)  # Dataset 1 with 1 million records
    df2 = generate_data(1000000)  # Dataset 2 with 1 million records
    print("Uniformly Distributed Data Created")

# Show schema to confirm the data
df1.printSchema()
df2.printSchema()

# Perform a large shuffle
def large_shuffle_example(df1, df2):
    # Join df1 and df2 on the 'id' column (this will trigger a large shuffle)
    joined_df = df1.drop("amount").join(df2, "id", "inner")

    # After the join, perform a groupBy operation on the 'id' column (another shuffle)
    shuffled_df = joined_df.groupBy("id").agg(
        count("amount").alias("transaction_count"),
        count("*").alias("total_count")
    )

    # Show the result
    shuffled_df.show(10)

# Trigger a large shuffle by performing the operations
large_shuffle_example(df1, df2)

# Stop the Spark session
spark.stop()
