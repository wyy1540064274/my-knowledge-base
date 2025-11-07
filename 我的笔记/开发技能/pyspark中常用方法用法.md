1. SparkSession (入口点)
python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# 主要方法
spark.sql()           # 执行SQL查询
spark.createDataFrame() # 创建DataFrame
spark.read            # 读取数据
spark.stop()          # 停止Session
spark.catalog         # 元数据操作
2. DataFrame 核心操作
转换操作 (Transformations)
python
df = spark.createDataFrame([...])

# 列操作
df.select()           # 选择列
df.withColumn()       # 添加/修改列
df.withColumnRenamed() # 重命名列
df.drop()             # 删除列

# 过滤和排序
df.filter() / df.where() # 条件过滤
df.distinct()         # 去重
df.orderBy() / df.sort() # 排序

# 分组聚合
df.groupBy()          # 分组
df.rollup()           # 多维分组
df.cube()             # 全维度分组
df.pivot()            # 数据透视

# 连接操作
df.join()             # 表连接
df.union() / df.unionAll() # 合并
df.intersect()        # 交集
df.exceptAll()        # 差集

# 窗口函数
from pyspark.sql.window import Window
df.withColumn(..., Window.partitionBy(...).orderBy(...))
行动操作 (Actions)
python
df.show()             # 显示数据
df.count()            # 计数
df.collect()          # 收集到本地
df.take() / df.head() # 获取前n行
df.first()            # 获取第一行
df.describe()         # 统计描述
df.printSchema()      # 打印schema
3. Column 操作方法
python
from pyspark.sql import functions as F

# 列表达式
col("name")           # 引用列
F.col("name")         # 同上

# 数学函数
F.abs(), F.sqrt(), F.exp(), F.log()
F.round(), F.ceil(), F.floor()

# 字符串函数
F.length(), F.upper(), F.lower()
F.trim(), F.ltrim(), F.rtrim()
F.substring(), F.split()
F.regexp_extract(), F.regexp_replace()

# 日期时间函数
F.current_date(), F.current_timestamp()
F.year(), F.month(), F.dayofmonth()
F.date_add(), F.date_sub()
F.datediff(), F.months_between()

# 聚合函数
F.count(), F.sum(), F.avg(), F.mean()
F.min(), F.max(), F.first(), F.last()
F.collect_list(), F.collect_set()

# 条件函数
F.when().otherwise()
F.coalesce()
F.isnull(), F.isnan()
4. RDD 操作 (弹性分布式数据集)
python
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# 转换操作
rdd.map()             # 映射
rdd.flatMap()         # 扁平映射
rdd.filter()          # 过滤
rdd.distinct()        # 去重
rdd.sample()          # 采样
rdd.union()           # 合并
rdd.intersection()    # 交集
rdd.groupByKey()      # 按键分组
rdd.reduceByKey()     # 按键聚合
rdd.sortBy()          # 排序

# 行动操作
rdd.collect()         # 收集
rdd.count()           # 计数
rdd.first()           # 首元素
rdd.take()            # 取前n个
rdd.reduce()          # 归约
rdd.foreach()         # 遍历
5. Spark SQL 函数
python
# 注册临时表
df.createOrReplaceTempView("table")

# SQL查询
spark.sql("SELECT * FROM table WHERE condition")
spark.sql("SELECT count(*) as cnt FROM table")
6. MLlib 机器学习
python
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.regression import *
from pyspark.ml.clustering import *

# 特征工程
VectorAssembler()      # 特征向量化
StringIndexer()        # 字符串索引
OneHotEncoder()        # 独热编码
StandardScaler()       # 标准化

# 机器学习算法
LogisticRegression()   # 逻辑回归
RandomForestClassifier() # 随机森林
LinearRegression()     # 线性回归
KMeans()               # K均值聚类

# 流水线
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[...])
7. Streaming 流处理
python
from pyspark.sql.streaming import *

# 读取流数据
spark.readStream.format("kafka")...
spark.readStream.format("socket")...

# 写入流数据
df.writeStream.format("console")...
df.writeStream.format("parquet")...

# 流操作
query = df.writeStream \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()
8. 数据源读写
python
# 读取
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("path/to/file")

# 支持的格式
spark.read.csv()
spark.read.json()
spark.read.parquet()
spark.read.orc()
spark.read.jdbc()
spark.read.table()

# 写入
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("output/path")

# 写入模式
.mode("append")       # 追加
.mode("overwrite")    # 覆盖
.mode("ignore")       # 忽略
.mode("error")        # 报错(默认)
9. 实用工具方法
python
# 配置管理
spark.conf.set()      # 设置配置
spark.conf.get()      # 获取配置

# 缓存管理
df.cache()            # 缓存
df.persist()          # 持久化
df.unpersist()        # 取消持久化

# 分区操作
df.repartition()      # 重新分区
df.coalesce()         # 合并分区
获取完整API文档的方法
官方文档：

bash
https://spark.apache.org/docs/latest/api/python/
交互式查看：

python
help(spark.sql)
dir(df)
源码查看：

python
import pyspark
print(pyspark.__file__)
