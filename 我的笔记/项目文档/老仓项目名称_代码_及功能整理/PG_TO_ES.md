### pg_to_es 项目代码详细分析

根据项目文件结构和内容，我对 **pg_to_es** 项目进行了全面分析。该项目主要实现从 PostgreSQL 数据库（数据仓库，简称 PG DW）到 Elasticsearch（简称 ES）的增量数据同步功能，专注于金融、股票、公司等领域的结构化数据迁移和索引。项目使用 Python 脚本实现核心逻辑，通过 Azkaban 工作流调度和 Spark 提交方式运行，配置灵活，支持多个表的字段映射和转换。以下是详细分析，包括项目整体架构、文件功能、核心实现逻辑、数据流和潜在优化点。

#### 1. 项目整体架构和目的

* 主要功能：实现 PG DW 中的数据表增量同步到 ES 索引中。同步基于配置（表映射、字段转换），支持 upsert 操作（更新或插入），以确保 ES 中的数据实时更新。项目名称 “pg_to_es” 直译为 “PostgreSQL to Elasticsearch”，表明其核心是 ETL（Extract-Transform-Load）管道。
* 适用场景：适用于数据仓库到搜索引擎的同步，例如构建搜索服务、数据分析或 BI 系统。数据主题主要涉及股票代码、金融报表、公司排名、临床试验等金融/医药领域（从配置中可见）。
* 技术栈：
* 数据库：PostgreSQL（源端，配置在 **settings.py** 中）。
* 搜索引擎：Elasticsearch（目标端，硬编码在脚本中，地址为 **192.168.100.99:9200**）。
* 脚本语言：Python 3（使用 psycopg2、elasticsearch、pyspark 等库）。
* 调度工具：Azkaban（工作流定义在 **dw.flow**）。
* 执行环境：Spark（通过 **spark.sh** 提交，但脚本中 SparkSession 未实际使用，可能仅用于分布式环境或未来扩展）。
* 运行模式：增量同步，每天拉取前一天更新的数据（基于 **update_time** 字段）。支持批量处理（bulk API）和字段转换（如翻译、条件映射）。
* 数据规模：配置支持大量表（**table_field_mapping_config.py** 中 config 列表很长，超过 25000 tokens），表明项目设计用于大规模数据同步。

#### 2. 文件功能详细 breakdown

项目根目录下有 **dw.flow**、**test.project** 和 **src/** 子目录。**src/** 包含核心代码。

* dw.flow：
* 这是一个 Azkaban 工作流配置文件（YAML 格式）。
* 定义了一个名为 “sync_script” 的节点，类型为 “command”。
* 命令：**sh src/spark.sh ${azkaban.job.id}.py** **${date}**。
* 作用：调度同步任务，传入作业 ID 和日期参数，调用 Spark 脚本运行 Python 同步逻辑。
* 分析：这是项目的入口点，用于自动化调度（如每日运行）。
* test.project：
* 一个简单的 Azkaban 项目配置文件，仅指定版本 **azkaban-flow-version: 2.0**。
* 作用：定义 Azkaban 项目元数据，可能用于测试环境。
* src/settings.py：
* 配置 PostgreSQL 和 Greenplum（GP）数据库连接参数。
* PG 配置：主机 **192.168.201.23**，端口 **5432**，数据库 **dw**，用户 **bigdata**，密码（加密）。
* GP 配置：类似，但数据库为 **dm**，用户 **gpdev**（未在同步脚本中使用）。
* 作用：提供数据库连接字符串和属性字典。同步脚本使用 PG 配置读取源数据。
* 分析：GP 配置可能为备用或扩展，未实际使用；PG 是核心源端。
* src/spark.sh：
* 一个 Bash 脚本，用于 Spark 提交。
* 命令：**spark-submit** **--deploy-mode cluster --py-files='src/table_field_mapping_config.py' src/$1 $2**。
* 作用：以集群模式提交 Python 脚本（$1 为脚本名，如 **sync_script.py**），传入参数 $2（日期），并依赖 **table_field_mapping_config.py**。
* 分析：尽管提交到 Spark，但同步脚本未利用 Spark 的分布式计算（仅创建 SparkSession 未使用），可能仅借用 Spark 环境运行 Python。
* src/sync_script.py：
* 核心脚本，实现数据同步逻辑。
* 依赖：导入 **table_field_mapping_config.py** 中的 **config** 和 **PG_PROPERTIES**；使用 **psycopg2** 连接 PG、**elasticsearch** 连接 ES、**hashlib** 生成 ID。
* 主要函数：
* **pg_dw_connection()**：创建 PG 连接。
* **es_connection()**：创建 ES 连接（硬编码地址、认证）。
* **get_hashid()**：生成 MD5 哈希 ID。
* **make_sql()**：构建增量查询 SQL（基于源表、映射、join 条件、update_time > 昨天）。
* **transform_to_string()**：转换字段值（字符串化、应用自定义函数）。
* **make_one_id()**：基于自然主键生成唯一 ID。
* **sycn_to_es()**：核心同步函数：
* 执行 SQL 从 PG 拉取数据（使用游标批量 fetch）。
* 转换数据（应用 func、添加 SYS_dm_update_time）。
* 生成 ES action（update with doc_as_upsert）。
* 使用 **helpers.bulk** 批量写入 ES。
* 主逻辑：循环 config 中的每个配置项，构建 SQL、同步数据。默认同步前一天数据（可通过参数传入日期）。
* 特殊处理：针对特定表（如 **dm_map_1232_company_qualification**）提取年份字段。
* 错误处理：捕获异常，继续下一个配置。
* 分析：脚本高效，支持大批量数据（fetchmany + bulk），但未使用 Spark 功能，可能有优化空间（如并行处理多个表）。
* src/table_field_mapping_config.py：
* 最大文件，定义了同步配置。
* 核心内容：**config** 是一个列表，每个元素是字典，描述一个表的同步规则：
* **source_table**：PG 中的源表（如 **dw_lget_security_code**）。
* **sink_table**：ES 中的目标索引（如 **dm_map_simple_table_1090**）。
* **mapping**：字段映射字典（源字段 -> 目标字段，如 **'security_code': 'code_id_1090'**）。
* **natural_primary_key**：用于生成 ID 的键列表（如 **["code_id_1090", "exchange_1090"]**）。
* **func**：可选，字段转换函数（使用 **functools.partial**，如翻译值、case_when 映射）。
* **join**：可选，join 其他表（如联表查询公司名称）。
* **sql**：可选，自定义 SQL（否则自动生成）。
* 辅助函数：如 **translate**（替换字符串）、**case_when**（条件映射）、**numeric_divide**（数值除以 100000000）。
* 数据主题：配置覆盖股票代码、金融报表（利润、资产、现金流）、公司排名、临床试验、药物审批等。示例：
* 股票表：映射代码、交易所、IPO 信息。
* 金融表：资产负债表、利润表等，包含 join 公司信息。
* 其他：工业园区、投资事件、EMA 药物等。
* 分析：配置非常详细，支持数百个表（文件超长），体现了项目的可扩展性。PG 配置在这里重复定义（readonly 用户，可能为读权限）。

#### 3. 数据流和执行过程

1. 调度：Azkaban 执行 **dw.flow**，调用 **spark.sh** 运行 **sync_script.py** 并传入日期。
2. 提取 (Extract)：脚本循环 **config**，为每个表构建 SQL（增量条件：**update_time >** **昨天**，过滤 **is_delete =** **0**），使用 psycopg2 从 PG 拉取数据。
3. 转换 (Transform)：字符串化值、应用 func（如翻译交易所名称）、生成哈希 ID、添加更新时间。
4. 加载 (Load)：构建 ES actions（update with upsert），批量写入 ES 索引。
5. 增量机制：仅同步更新数据，避免全量重载。
6. ID 生成：基于自然主键的 MD5 哈希，确保唯一性。

#### 4. 优势和潜在问题

* 优势：
* 配置驱动：易扩展新表，无需改代码。
* 高效：批量处理、增量同步。
* 容错：异常跳过，继续执行。
* 潜在问题：
* Spark 未实际使用：可能多余，增加复杂性。
* 硬编码：ES 地址、认证固定，不灵活。
* 配置文件过大：维护困难，可拆分成多个文件。
* 无并行：表同步串行，可优化为多线程。
* 安全：密码明文存储，建议加密。
* 依赖：脚本中 SparkSession 未用，但导入 pyspark，可能为遗留代码。

#### 5. 总结和建议

该项目主要实现了一个可配置的 PG 到 ES 数据同步管道，针对金融数据仓库，适用于搜索索引构建。如果需要扩展，可添加更多 config 项或优化为全分布式 Spark 作业（如使用 Spark SQL 读取 PG）。如果有具体表或功能疑问，我可以进一步分析或使用工具读取更多细节。
