# INOVAlife datalake client

PySpark client to access the INOVAlife datalake.

Current access is restricted.

To get keys to access the datalake, please contact contato@inova.life by email.

### Installing
- Clone or download this repo
- Run `python setup.py install`

### Using

```python
from inovalife_data import DataLakeClient

access_key = "ABC"
secret_key = "SHHHH"

data = DataLakeClient(access_key, secret_key)

# Check all SQL tables we have, and the available years:
data.tabelas
# {'SINASC': ['DN'], 'SIM': ['DO_EXT', 'DO_FET', 'DO_INF', 'DO_MAT', 'DO']}
data.anos_disponiveis
# ... list from 1996 to 2019

# Create a SQL table to query:
data.carregar_tabela("SINASC","DN","1996")
# tabela_SINASC_DN_1996 carregada.


# Query a table in a Spark Dataframe format:
df = data.sql("select contador,IDADEMAE,PESO,UF from tabela_SINASC_DN_1996")

# Check table:
df.show()
"""
+--------+--------+----+---+
|contador|IDADEMAE|PESO| UF|
+--------+--------+----+---+
|       1|      20|9999| SP|
|       2|      23|3500| SP|
|       3|      99|3290| SP|
|       4|      22|3880| SP|
|       5|      15|3780| SP|
|       6|      20|2900| SP|
|       7|      16|3600| SP|
|       8|      26|2350| SP|
|       9|      28|3050| SP|
|      10|      30|3050| SP|
|      11|      22|3500| SP|
|      12|      32|4150| SP|
|      13|      21|3160| SP|
|      14|      19|4010| SP|
|      15|      28|2960| SP|
|      16|      30|3540| SP|
|      17|      30|3200| SP|
|      18|      99|3010| SP|
|      19|      23|3250| SP|
|      20|      22|2860| SP|
+--------+--------+----+---+
only showing top 20 rows
"""

# Transform it to Pandas:
df = df.toPandas()
```
