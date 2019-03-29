# # Apache Spark UDFs


# ## Setup

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("spark-udfs").getOrCreate()


# ## Generate Test Data

from pyspark.sql.functions import rand

n = 10000000
df1 = spark.range(n).withColumn("x", rand(seed=12345))


# ## Built-in Functions

from pyspark.sql.functions import log, exp, sum

df2 = df1.withColumn("y", log(col("x") / (1.0 - col("x"))))
df3 = df2.withColumn("z", 1.0 / (1.0 + exp(-col("y"))))

df3.show()

%time df3.select(sum("x"), sum("z")).show()


# ## Scalar Python UDFs

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def logit(x):
  from math import log
  return log(x / (1.0 - x))

logit_udf = udf(logit, returnType=DoubleType())

@udf("double")
def expit_udf(x):
  from math import exp
  return 1.0 / (1.0 + exp(-x))

df4 = df1.withColumn("y", logit_udf("x"))
df5 = df4.withColumn("z", expit_udf("y"))

%time df5.select(sum("x"), sum("z")).show()


# ## Vector Python UDFs

# **Note:** `numpy` and `PyArrow` must be installed on the worker nodes.

from pyspark.sql.functions import pandas_udf, PandasUDFType

def logit_pandas(x):
  from numpy import log
  return log(x / (1.0 - x))
logit_pandas_udf = pandas_udf(logit_pandas, returnType=DoubleType(), functionType=PandasUDFType.SCALAR)

@pandas_udf("double", PandasUDFType.SCALAR)
def expit_pandas_udf(x):
  from numpy import exp
  return 1.0 / (1.0 + exp(-x))

df6 = df1.withColumn("y", logit_pandas_udf("x"))
df7 = df6.withColumn("z", expit_pandas_udf("y"))

%time df7.select(sum("x"), sum("z")).show()


# ## Scala UDFs

!cat spark-defaults.conf

spark.udf.registerJavaFunction("logit_scala_udf", "com.cloudera.education.spark.udfs.Logit", DoubleType())
spark.udf.registerJavaFunction("expit_scala_udf", "com.cloudera.education.spark.udfs.Expit", DoubleType())

from pyspark.sql.functions import expr
df_scala = df1 \
  .withColumn("y", expr("logit_scala_udf(x)")) \
  .withColumn("z", expr("expit_scala_udf(y)"))

%time df_scala.select(sum("x"), sum("z")).show()


# ## Cleanup
                     
spark.stop()