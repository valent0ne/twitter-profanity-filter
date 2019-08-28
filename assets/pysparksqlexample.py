from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

cols = [
    ('A moocci xua', 'xx', 'D', 'vv', 4),
    ('C cullo ululo pillulo', 'xxx', 'D', 'vv', 10),
    ('AAAA pizzizzi lollollo', 'x', 'A', 'xx', 3)]
df = spark.createDataFrame(cols, ['col1', 'col2', 'col3', 'col4', 'd'])


df.show()
words = ["mormooccitacci", "spasso", "C", "ecci",
         "zuzzu", "pippo", "ciccio", "lollollo"]

df.filter((df.col1.rlike('(^|\s)(' + '|'.join(words) + ')(\s|$)') == False)).show()
