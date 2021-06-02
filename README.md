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

Check all SQL tables we have:
data.tables
# ["table1", "table2", "table3"]

# Query a table in a Spark DF format:
df = data.sql("select * from table1")

# Transform it to Pandas:
df = df.toPandas()
```
