from pyspark.sql import SparkSession


class DataLakeClient(SparkSession):
  def __init__(self, access_key, secret_key):
    self.spark = (SparkSession
    .builder
    .appName('datasus')
    .config('spark.jars.packages', 'com.amazonaws:aws-java-sdk-bundle:1.11.819,org.apache.hadoop:hadoop-aws:3.2.0')
    .config("spark.hadoop.fs.s3a.endpoint", "https://sfo3.digitaloceanspaces.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.committer.name", "directory")
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")
    .config("spark.hadoop.mapreduce.reduce.speculative","false")
    .config("spark.hadoop.fs.s3a.access.key", access_key)
    .config("spark.hadoop.fs.s3a.secret.key", secret_key)
    .getOrCreate())
    
    self.base_uri = "s3a://datasus/"
    
    self.bases = {
      "SIM_DORES": self.spark.read.option("header", "true").csv(self.base_uri + "Base=SIM_DORES" + "/"), 
      "SIM_DOFET": self.spark.read.option("header", "true").csv(self.base_uri + "Base=SIM_DOFET" + "/")
    }
    
    self.bases["SIM_DORES"].registerTempTable()
    
    self.tables = self.bases.keys()
    
    

df = spark.read.csv("s3a://datasus/Base=SINASC/Tabela=DN/UF=AC/Ano=1996/DNAC1996.csv.gz")

df.show()

df.write.mode("overWrite").parquet("s3a://datasus/testParquet.parquet")


from inovalife_data import DataLakeClient

access_key = "ABC"
secret_key = "SHHHH"

data = DataLakeClient(access_key, secret_key)

Check all SQL tables we have:
data.tables
# ["table1", "table2", "table3"]

# Query a table in a Spark DF format:
df = data.sql("select * from table1")

# Transform it to Pandas:
df = df.toPandas()
