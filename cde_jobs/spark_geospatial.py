#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
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
import pyspark.sql.functions as F
from sedona.spark import *
import sys

#---------------------------------------------------
#               CREATE SPARK SESSION WITH ICEBERG
#---------------------------------------------------

spark = SparkSession \
    .builder \
    .appName("IOT DEVICES LOAD") \
    .config("spark.kubernetes.access.hadoopFileSystems", "s3a://go01-demo")\
    .getOrCreate()

#---------------------------------------------------
#               PRINT SYS ARGS
#---------------------------------------------------

print("PRINT ALL ARGS: ")
print(sys.argv)
print("\nPRINT FIRST ARG: ")
print(sys.argv[0])

#---------------------------------------------------
#               CREATE SEDONA CONTEXT
#---------------------------------------------------

config = SedonaContext.builder().getOrCreate()

sedona = SedonaContext.create(spark)

sc = sedona.sparkContext
sc.setSystemProperty("sedona.global.charset", "utf8")

#-----------------------------------------------------------------------------------
# CREATE TABLE FROM DATA IN CDE FILES RESOURCE
#-----------------------------------------------------------------------------------

print("CREATING COUNTRIES RDD FROM FILE \n")
print("\n")

countries_rdd = ShapefileReader.readToGeometryRDD(sc, "/app/mount/myData")

print("CREATING COUNTRIES DF FROM RDD \n")
print("\n")

countries_df = Adapter.toDf(countries_rdd, sedona)
countries_df.printSchema()
