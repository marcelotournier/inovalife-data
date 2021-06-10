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

    self.sql = self.spark.sql

    # Atualizar as bases e suas respectivas tabelas aqui:
    self.tabelas = {
      "CNES": ["DC", "EE", "EF", "EP", "EQ", "GM", "HB", "IN", "LT"],
      "SIA": ["AB", "ABO", "ACF", "AD", "AM", "AMP", "AN", "AQ", "AR", "ATD", "BI"],
      "SIH": ["CH", "CM", "ER", "RD", "RJ", "SP"],
      "SIM": ["DO_EXT", "DO_FET", "DO_INF", "DO_MAT", "DO"],
      "SINASC": ["DN"]
    }
    
    # Anos - a partir de 1996:
    self.ano_inicial = 1996
    self.ano_mais_recente = 2019
    self.anos_disponiveis = list(range(1996, self.ano_mais_recente + 1))

  def carregar_tabela(self, base, tabela, ano):
    df = self.spark.read.option("header", "true").csv(self.base_uri + f"Base={base}/Tabela={tabela}/Ano={ano}")
    tabela_sql = f"tabela_{base}_{tabela}_{ano}"
    df.registerTempTable(tabela_sql)
    print(tabela_sql, "carregada.")

