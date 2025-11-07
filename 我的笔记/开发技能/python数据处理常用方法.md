## 1. Pandas 常用方法深度解析

### 数据读取和查看

**python**

```
import pandas as pd

# 读取CSV（最常用）
df = pd.read_csv('data.csv', encoding='utf-8', parse_dates=['date_column'])
"""
关键参数：
- encoding: 解决中文编码问题
- parse_dates: 自动解析日期列
- na_values: 指定缺失值标识
- usecols: 只读取指定列
- nrows: 只读取前n行（测试用）
"""

# 查看数据基本信息
df.info()        # 数据类型、内存使用
df.describe()    # 数值列统计描述
df.head()        # 前5行预览
df.shape         # 数据形状 (行数, 列数)
```

### 数据选择和过滤

**python**

```
# 列选择（最常用）
df['column_name']           # 选择单列 → Series
df[['col1', 'col2']]        # 选择多列 → DataFrame

# 行选择
df.loc[0]                   # 按标签选择单行
df.iloc[0]                  # 按位置选择单行
df.loc[0:5, ['col1', 'col2']]  # 选择特定行列

# 布尔索引（极其重要）
# 单个条件
df[df['age'] > 30]
# 多个条件
df[(df['age'] > 30) & (df['salary'] > 5000)]
# 包含某些值
df[df['category'].isin(['A', 'B'])]
# 字符串包含
df[df['name'].str.contains('张')]
```

### 数据清洗实战方法

**python**

```
# 处理缺失值
df.isnull().sum()           # 查看每列缺失值数量
df.dropna()                 # 删除包含缺失值的行
df.dropna(axis=1)           # 删除包含缺失值的列
df.fillna(0)                # 用0填充
df.fillna(method='ffill')   # 用前一个值填充
df.fillna({'col1': 0, 'col2': 'unknown'})  # 按列填充不同值

# 处理重复值
df.duplicated().sum()       # 统计重复行数
df.drop_duplicates()        # 删除完全重复的行
df.drop_duplicates(subset=['col1', 'col2'])  # 基于指定列去重

# 数据类型转换
df['date_col'] = pd.to_datetime(df['date_col'])  # 转日期
df['category'] = df['category'].astype('category')  # 转分类
```

### 数据变换和特征工程

**python**

```
# 排序
df.sort_values('salary', ascending=False)          # 按薪资降序
df.sort_values(['dept', 'salary'], ascending=[True, False])  # 多列排序

# 重命名列
df.rename(columns={'old_name': 'new_name'}, inplace=True)

# 应用函数（非常灵活）
df['age_group'] = df['age'].apply(lambda x: '青年' if x < 35 else '中年')
df['salary_log'] = np.log(df['salary'])  # 对数变换

# 字符串操作
df['name_upper'] = df['name'].str.upper()          # 大写
df['name_length'] = df['name'].str.len()           # 长度
df['email_domain'] = df['email'].str.split('@').str[1]  # 分割提取
```

### 分组聚合（数据分析核心）

**python**

```
# 基本分组统计
df.groupby('department')['salary'].mean()          # 各部门平均薪资
df.groupby('department')['salary'].agg(['mean', 'std', 'count'])  # 多统计量

# 多维度分组
result = df.groupby(['year', 'department']).agg({
    'sales': 'sum',
    'profit': 'mean',
    'employee_id': 'count'
}).reset_index()

# 数据透视表（类似Excel）
pivot_table = df.pivot_table(
    values='sales',
    index='region',
    columns='quarter',
    aggfunc='sum',
    fill_value=0
)
```

## 2. NumPy 常用方法解析

### 数组创建和操作

**python**

```
import numpy as np

# 创建数组
arr = np.array([1, 2, 3, 4, 5])          # 从列表
zeros = np.zeros((3, 4))                 # 全0数组
ones = np.ones((2, 3))                   # 全1数组
rand_arr = np.random.randn(100)          # 正态分布随机数

# 数组操作
arr.reshape(2, 3)                        # 改变形状
arr.flatten()                            # 展平
np.concatenate([arr1, arr2])             # 连接数组
```

### 数学和统计运算

**python**

```
# 基本统计
arr.mean()              # 平均值
arr.std()               # 标准差
arr.sum()               # 总和
arr.min(), arr.max()    # 最小最大值
np.percentile(arr, 95)  # 95%分位数

# 数学运算
np.sqrt(arr)            # 平方根
np.log(arr)             # 自然对数
np.exp(arr)             # 指数
np.abs(arr)             # 绝对值

# 条件运算
np.where(arr > 0, arr, 0)               # 条件替换
np.select([cond1, cond2], [choice1, choice2])  # 多条件选择
```

## 3. 数据可视化常用方法

**python**

```
import matplotlib.pyplot as plt
import seaborn as sns

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# 常用图表
plt.figure(figsize=(10, 6))

# 1. 折线图 - 趋势分析
plt.plot(df['date'], df['sales'])
plt.title('销售趋势')
plt.xlabel('日期')
plt.ylabel('销售额')

# 2. 柱状图 - 分类比较
df.groupby('category')['sales'].sum().plot(kind='bar')

# 3. 散点图 - 相关性分析
plt.scatter(df['ad_cost'], df['sales'])
plt.xlabel('广告投入')
plt.ylabel('销售额')

# 4. 箱线图 - 分布和异常值
df.boxplot(column='salary', by='department')

# 5. 直方图 - 分布情况
df['age'].hist(bins=20)

plt.tight_layout()
plt.show()
```

## 4. 实际工作流示例

### 完整的数据分析流程

**python**

```
def complete_data_analysis(file_path):
    """完整的数据分析流程"""
  
    # 1. 数据加载
    df = pd.read_csv(file_path)
    print(f"原始数据形状: {df.shape}")
  
    # 2. 数据探索
    print("\n=== 数据基本信息 ===")
    print(df.info())
    print("\n=== 缺失值情况 ===")
    print(df.isnull().sum())
  
    # 3. 数据清洗
    # 删除重复值
    df = df.drop_duplicates()
  
    # 处理缺失值
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
  
    categorical_cols = df.select_dtypes(include=['object']).columns
    df[categorical_cols] = df[categorical_cols].fillna('Unknown')
  
    # 4. 特征工程
    df['sales_per_employee'] = df['total_sales'] / df['employee_count']
    df['profit_margin'] = df['profit'] / df['revenue']
  
    # 5. 数据分析
    print("\n=== 各部门销售统计 ===")
    dept_stats = df.groupby('department').agg({
        'sales': ['sum', 'mean', 'std'],
        'profit': 'mean',
        'employee_id': 'count'
    }).round(2)
    print(dept_stats)
  
    # 6. 可视化
    plt.figure(figsize=(12, 8))
  
    plt.subplot(2, 2, 1)
    df.groupby('department')['sales'].sum().plot(kind='bar')
    plt.title('各部门总销售额')
  
    plt.subplot(2, 2, 2)
    plt.scatter(df['ad_budget'], df['sales'])
    plt.xlabel('广告预算')
    plt.ylabel('销售额')
    plt.title('广告效果分析')
  
    plt.subplot(2, 2, 3)
    df['profit_margin'].hist(bins=20)
    plt.xlabel('利润率')
    plt.title('利润率分布')
  
    plt.tight_layout()
    plt.show()
  
    return df

# 使用示例
# processed_data = complete_data_analysis('sales_data.csv')
```

### 常用的数据验证方法

**python**

```
def data_quality_check(df):
    """数据质量检查"""
  
    checks = {
        '总行数': len(df),
        '总列数': len(df.columns),
        '缺失值数量': df.isnull().sum().sum(),
        '重复行数': df.duplicated().sum(),
        '数值列': df.select_dtypes(include=[np.number]).columns.tolist(),
        '分类列': df.select_dtypes(include=['object']).columns.tolist()
    }
  
    # 数值列统计
    numeric_stats = df.select_dtypes(include=[np.number]).describe()
  
    return checks, numeric_stats

# 使用
# checks, stats = data_quality_check(df)
```
