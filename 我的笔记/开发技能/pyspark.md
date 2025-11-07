由于 DataFrame API 是目前的主流和推荐用法，我们会重点讲解它。

---

### 1. RDD (Resilient Distributed Datasets) API 方法举例

RDD 是 Spark 最基础的数据抽象，代表一个不可变、可分区的元素集合。

#### a. 转换操作（Transformations）

转换是惰性操作，它们只记录转换关系，并不会立即执行。

**1. `map(func)`**

* **功能** ：对 RDD 中的每个元素应用函数 `func`。
* **举例** ：将每个数字乘以 2。
  **python**

```
  from pyspark import SparkContext
  sc = SparkContext("local", "map example")

  data = [1, 2, 3, 4, 5]
  rdd = sc.parallelize(data)
  # 使用 lambda 函数
  mapped_rdd = rdd.map(lambda x: x * 2)
  print(mapped_rdd.collect()) # 输出: [2, 4, 6, 8, 10]
```

**2. `filter(func)`**

* **功能** ：返回一个由通过函数 `func` 筛选的元素组成的新 RDD。
* **举例** ：筛选出偶数。
  **python**

```
  filtered_rdd = rdd.filter(lambda x: x % 2 == 0)
  print(filtered_rdd.collect()) # 输出: [2, 4]
```

**3. `flatMap(func)`**

* **功能** ：首先对每个元素应用函数 `func`，然后将结果“扁平化”。`func` 应该返回一个序列（如 list），而不是单个元素。
* **举例** ：将每行文本分割成单词。
  **python**

```
  lines = ["Hello World", "Apache Spark"]
  lines_rdd = sc.parallelize(lines)
  words_rdd = lines_rdd.flatMap(lambda line: line.split(" "))
  print(words_rdd.collect()) # 输出: ['Hello', 'World', 'Apache', 'Spark']
```

**4. `distinct()`**

* **功能** ：返回一个包含源 RDD 中不重复元素的新 RDD。
* **举例** ：去重。
  **python**

```
  data_with_dup = [1, 2, 2, 3, 4, 4, 5]
  rdd_dup = sc.parallelize(data_with_dup)
  distinct_rdd = rdd_dup.distinct()
  print(distinct_rdd.collect()) # 输出: [1, 2, 3, 4, 5] (顺序可能不同)
```

#### b. 行动操作（Actions）

行动操作会触发 Spark 作业的执行，并返回结果或保存结果到外部存储。

**1. `collect()`**

* **功能** ：以数组形式返回 RDD 中的所有元素。
* **举例** ：上面已经多次使用。 **注意** ：数据量很大时不要用，会导致 Driver 内存溢出。

**2. `count()`**

* **功能** ：返回 RDD 中的元素个数。
* **举例** ：
  **python**

```
  print(rdd.count()) # 输出: 5
```

**3. `take(n)`**

* **功能** ：返回 RDD 中的前 n 个元素。
* **举例** ：
  **python**

```
  print(rdd.take(3)) # 输出: [1, 2, 3]
```

**4. `reduce(func)`**

* **功能** ：使用函数 `func`（接受两个参数，返回一个同类型值）并行聚合 RDD 中的所有元素。
* **举例** ：求和。
  **python**

```
  sum_val = rdd.reduce(lambda a, b: a + b)
  print(sum_val) # 输出: 15
```

---

### 2. DataFrame / Dataset API 方法举例

DataFrame 是以命名列组织的分布式数据集，类似于关系型数据库中的表或 Python 中的 Pandas DataFrame。

首先，创建一个 SparkSession 和示例 DataFrame：

**python**

```
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import * # 导入内置函数

spark = SparkSession.builder.appName("DataFrameExamples").getOrCreate()

# 创建数据
data = [
    ("Alice", "HR", 60000, 35),
    ("Bob", "Engineering", 80000, 32),
    ("Charlie", "Engineering", 90000, 45),
    ("David", "Marketing", 70000, 28),
    ("Eve", "HR", 65000, 40)
]

# 定义模式
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

# 创建DataFrame
df = spark.createDataFrame(data=data, schema=schema)
print("原始 DataFrame:")
df.show()
```

#### a. 基础操作与 SQL 类似的操作

**1. `select()` / `selectExpr()`**

* **功能** ：选择指定的列。
* **举例** ：
  **python**

```
  df.select("Name", "Salary").show()
  # 使用 selectExpr 进行表达式计算
  df.selectExpr("Name", "Salary", "Salary * 0.1 as Bonus").show()
```

**2. `filter()` / `where()`**

* **功能** ：条件过滤。
* **举例** ：
  **python**

```
  df.filter(df.Department == "Engineering").show()
  # 或者使用 where，两者等价
  df.where((df.Salary > 70000) & (df.Age < 40)).show() # 注意条件组合用 &, |, ~
```

**3. `groupBy()`**

* **功能** ：根据一列或多列进行分组，然后进行聚合。
* **举例** ：
  **python**

```
  # 计算每个部门的平均薪资和最大年龄
  df.groupBy("Department").agg(
      avg("Salary").alias("AvgSalary"),
      max("Age").alias("MaxAge")
  ).show()
```

**4. `orderBy()` / `sort()`**

* **功能** ：排序。
* **举例** ：
  **python**

```
  df.orderBy(df.Salary.desc()).show() # 按薪资降序
  df.sort("Department", "Age").show() # 先按部门，再按年龄升序
```

**5. `withColumn()`**

* **功能** ：添加新列或替换同名列。
* **举例** ：添加一列，表示薪资等级。
  **python**

```
  df_with_grade = df.withColumn("Grade", 
                                when(df.Salary < 70000, "Junior")
                                .when(df.Salary < 85000, "Senior")
                                .otherwise("Expert"))
  df_with_grade.show()
```

**6. `drop()`**

* **功能** ：删除指定的列。
* **举例** ：
  **python**

```
  df_dropped = df.drop("Age")
  df_dropped.show()
```

#### b. 复杂操作与连接

**1. `join()`**

* **功能** ：连接两个 DataFrame。
* **举例** ：
  **python**

```
  # 创建另一个部门信息的 DataFrame
  dept_data = [("HR", "New York"), ("Engineering", "San Francisco"), ("Marketing", "Chicago")]
  dept_df = spark.createDataFrame(dept_data, ["Department", "Location"])

  # 内连接
  joined_df = df.join(dept_df, on="Department", how="inner")
  joined_df.show()
```

**2. `union()` / `unionByName()`**

* **功能** ：合并两个结构相同的 DataFrame。
* **举例** ：
  **python**

```
  new_data = [("Frank", "Engineering", 95000, 50)]
  new_df = spark.createDataFrame(new_data, schema)
  combined_df = df.union(new_df)
  combined_df.show()
```

**3. `withColumnRenamed()`**

* **功能** ：重命名列。
* **举例** ：
  **python**

```
  df_renamed = df.withColumnRenamed("Name", "Employee_Name")
  df_renamed.show()
```

#### c. 行动操作

**1. `show()`**

* **功能** ：打印前 n 行数据。

**2. `count()`**

* **功能** ：返回行数。
  **python**

```
  print(df.count())
```

**3. `collect()`**

* **功能** ：以列表形式返回所有数据（Row 对象）。 **慎用** 。

**4. `describe()`**

* **功能** ：计算数值列的统计信息（计数、均值、标准差、最小最大值）。
  **python**

```
  df.describe("Salary", "Age").show()
```

#### d. 使用 SQL 函数 (`pyspark.sql.functions`)

PySpark 提供了大量内置函数，用于在 `select()`, `withColumn()`, `filter()` 等中进行复杂计算。

**1. 字符串函数：`concat`, `substring`, `lower`, `upper`**

**python**

```
df.select("Name", 
          concat(df.Name, lit("_"), df.Department).alias("ID"),
          lower(df.Department).alias("DeptLower")
         ).show()
```

**2. 日期时间函数：`current_date`, `year`, `month`**

**python**

```
df.withColumn("CurrentDate", current_date()).show()
```

**3. 聚合函数：`sum`, `avg`, `count`, `countDistinct`**

**python**

```
df.agg(
    sum("Salary").alias("TotalSalary"),
    countDistinct("Department").alias("NumDepts")
).show()
```

**4. 窗口函数 (`Window`)**

* **功能** ：对一组行（窗口）进行计算，并为每个输入行返回一个值。
* **举例** ：计算每个部门内的薪资排名。
  **python**

```
  from pyspark.sql.window import Window

  windowSpec = Window.partitionBy("Department").orderBy(df.Salary.desc())
  df_with_rank = df.withColumn("rank", rank().over(windowSpec))
  df_with_rank.show()
```

### 总结

| 特性               | RDD API                   | DataFrame API                                       |
| ------------------ | ------------------------- | --------------------------------------------------- |
| **抽象级别** | 低级，面向对象            | 高级，声明式（像SQL）                               |
| **数据格式** | 非结构化/半结构化         | 结构化（有Schema）                                  |
| **性能**     | 较差，Java 序列化，无优化 | **优秀** ，Catalyst 优化器，Tungsten 执行引擎 |
| **易用性**   | 相对复杂                  | 简单，类似 Pandas/SQL                               |
| **使用场景** | 复杂的自定义函数处理      | 大部分结构化数据处理，ETL，数据分析                 |

 **建议** ：对于新项目，应 **优先使用 DataFrame/Dataset API** ，以获得最佳性能和开发效率。只有在需要非常底层的控制或处理非结构化数据时，才考虑使用 RDD API。
