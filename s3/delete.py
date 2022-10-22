from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

columns = ["language", "users_count", "Added", "updated", "deletd"]
data = [("Java", "20000", '2022', '2022-10-02', '2022-10-02'),
        ("Python", "2000", '2022', '2022', None), ("Scala", "3000", '2022', '2022', '2021'),("Pythonc", "2000", '2022', '2022', None)]
df_new = spark.createDataFrame(data, columns)

columns = ["language", "users_count", "Added", "updated", "deletd",'a']
data = [("Java1", "000", '2022-10-02', '2022-10-02', '2022-10-02','a'),
        ("Python", "200", '2022-09-02', '2022', None,'b'), ("Python", "500", '2022-09-03', '2022', None,'c'),("Scala", "3000", '2022-09-05', '2022', '2021','m')]
df_old = spark.createDataFrame(data, columns)
columns_missing = list(set(df_new.columns) - set(df_old.columns))
print(columns_missing)
for column in columns_missing:
        df_old = df_old.withColumn(column,lit(None))

columns_missing = list(set(df_old.columns) - set(df_new.columns))
print(columns_missing)
for column in columns_missing:
        df_new = df_new.withColumn(column,lit(None))

df_old_without_drop = df_old
df_new_without_drop = df_new
df_old= df_old.withColumn('Added',to_date(df_old['Added'],'yyyy-MM-dd'))
df_old = df_old.sort(df_old.Added.desc())




df_old = df_old.dropDuplicates(['language'])

df_old.show();
df_old = df_old.drop('Added', 'updated', "deletd")
df_new = df_new.drop('Added', 'updated', "deletd")



df_modfiled = df_old.subtract(df_new)
print("modifiled or delte")
df_modfiled.show()
df_changed = df_modfiled.join(df_new, df_modfiled['language'] == df_new['language'], "leftsemi")


ids=df_changed.select('language').rdd.flatMap(lambda x: x).collect()



df_old_without_drop = df_old_without_drop.withColumn('updated', when(df_old_without_drop['language'].isin(ids),lit(current_date())))
df_new_without_drop = df_new_without_drop.withColumn('updated', when(df_new_without_drop['language'].isin(ids),lit(current_date())))


df_delted = df_modfiled.join(df_new, df_modfiled['language'] == df_new['language'], "leftanti")

ids=df_delted.select('language').rdd.flatMap(lambda x: x).collect()
df_old_without_drop = df_old_without_drop.withColumn('deletd', when(df_old_without_drop['language'].isin(ids),current_date()))
print("before drop")
df_old_without_drop.show()
df_old_without_drop= df_old_without_drop.na.drop(how='all',subset=["deletd","updated"])
df_old_without_drop.show()


df_new_without_drop.unionByName(df_old_without_drop,allowMissingColumns=True).show()



# df3=df_old.alias('a').join(df_new.alias('b'),df_new['language']==df_old['language'] ,"left")
#
# df3 = df3.select(["a.*","Updated_Record"])
# df3.show()
# df3=df3.withColumn("deleted",when(df3.Updated_Record.isNull(),True))
# df3 = df3.filter(df3.deleted.isNotNull())
#
# df_new =df_new.drop(col('Updated_Record'))
# df3 =df3.drop(col('Updated_Record'))
# df3 = df3.unionByName(df_new,allowMissingColumns=True)
# df3.show()
#
#
# df3 =df_old.subtract(df_new)
# df3.show()
# df4 = df.join(df3,df['language']==df3['language'] ,"leftsemi")
# df5 = df3.join(df,df['language']==df3['language'] ,"leftanti")
# df4.show()
# df5.show()
