# Oracle导入工具 - 用户操作手册

## 目录
- [基本使用流程](#基本使用流程)
- [表管理策略](#表管理策略)
- [无表头数据处理](#无表头数据处理)
- [表头检测配置](#表头检测配置)
- [文件名和表名映射](#文件名和表名映射)
- [DDL文件格式](#DDL文件格式)
- [配置文件说明](#配置文件说明)
- [命令行选项](#命令行选项)
- [错误处理](#错误处理)
- [最佳实践](#最佳实践)

## 基本使用流程

### 1. 准备工作

**环境要求：**
- Python 3.9+
- Oracle Database 12c+
- 已配置的Oracle客户端

**安装工具：**
```bash
# 克隆项目
git clone <repository-url>
cd oracle_import_tool

# 安装依赖
poetry install

# 或使用pip
pip install -r requirements.txt
```

### 2. 配置数据库连接

```bash
# 生成默认配置文件
poetry run oracle-import config init

# 编辑配置文件
vim config.ini
```

**配置文件示例：**
```ini
[database]
host = localhost
port = 1521
service_name = ORCLPDB1.localdomain
username = your_username
password = your_password
schema = YOUR_SCHEMA

[import_settings]
batch_size = 1000
max_retries = 3
timeout = 30
auto_commit = true
create_table_if_not_exists = false  # 新版本默认为false
```

### 3. 测试数据库连接

```bash
poetry run oracle-import test-db
```

### 4. 扫描数据文件

```bash
# 扫描指定目录下的数据文件
poetry run oracle-import scan ./data
```

### 5. 预览数据文件

```bash
# 预览文件内容
poetry run oracle-import preview ./data/users.csv
```

### 6. 执行数据导入

```bash
# 基本导入
poetry run oracle-import import -df ./data

# 使用DDL文件
poetry run oracle-import import -df ./data --ddl-folder ./ddl

# 干运行模式（验证但不导入）
poetry run oracle-import import -df ./data --dry-run
```

## 表管理策略

### v0.2.0 重要变更

从v0.2.0开始，工具采用更安全的表管理策略：

1. **表必须预先存在** - 工具不再自动创建表
2. **明确错误提示** - 当表不存在时给出清晰的错误信息
3. **DDL文件支持** - 通过DDL文件定义表结构

### 创建表的方法

**方法一：手动创建**
```sql
CREATE TABLE USERS (
    ID NUMBER(10) PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    EMAIL VARCHAR2(200),
    AGE NUMBER(3),
    CREATED_BY VARCHAR2(100) DEFAULT 'SYSTEM',
    CREATE_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UPDATED_BY VARCHAR2(100),
    UPDATE_TIMESTAMP TIMESTAMP
);
```

**方法二：使用DDL文件**
1. 在DDL文件夹中创建对应的.sql文件
2. 手动执行DDL创建表
3. 使用工具导入数据

## 无表头数据处理

### 功能概述

工具支持智能处理没有表头的数据文件：
- 自动检测文件是否包含表头
- 当没有表头时，使用DDL文件中的列定义
- 按照DDL定义的顺序映射数据

### 使用示例

**数据文件（无表头）：**
```csv
1,张三,zhang@example.com,25
2,李四,li@example.com,30
3,王五,wang@example.com,28
```

**对应的DDL文件（users.sql）：**
```sql
CREATE TABLE USERS (
    ID NUMBER(10) PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    EMAIL VARCHAR2(200),
    AGE NUMBER(3),
    CREATED_BY VARCHAR2(100) DEFAULT 'SYSTEM',
    CREATE_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**导入命令：**
```bash
poetry run oracle-import import -df ./data --ddl-folder ./ddl
```

### 工作原理

1. **表头检测**：使用可配置的表头检测器智能识别文件是否包含表头
2. **DDL匹配**：根据文件名匹配对应的DDL文件
3. **列映射**：使用DDL中的列名（排除审计字段）作为数据列
4. **数据处理**：按照定义的列顺序处理数据

## 表头检测配置

### 概述

从v0.2.0开始，工具提供了灵活的表头检测配置功能，支持通过关键词来定义表头检测逻辑。

### 配置参数

在配置文件的 `[header_detection]` 节中设置：

```ini
[header_detection]
# 表头关键词配置，支持AND/OR逻辑
header_keywords = CREATED_BY,CREATE_TIMESTAMP

# 检测模式
header_detection_mode = auto
```

### 关键词配置语法

**AND关系（逗号分隔）：**
```ini
header_keywords = CREATED_BY,CREATE_TIMESTAMP
```
- 表示：同时包含 `CREATED_BY` 和 `CREATE_TIMESTAMP` 两个关键词才认为是表头

**OR关系（竖线分隔）：**
```ini
header_keywords = CREATE_TIMESTAMP|CREATED_BY
```
- 表示：包含 `CREATE_TIMESTAMP` 或 `CREATED_BY` 任一关键词就认为是表头

**混合逻辑：**
```ini
header_keywords = id,name|code,type
```
- 表示：`(id AND name) OR (code AND type)`

### 检测模式

- `auto`: 自动检测（默认）
  - 如果配置了关键词，使用关键词匹配
  - 否则使用默认检测逻辑
- `force_header`: 强制认为第一行是表头
- `force_no_header`: 强制认为第一行是数据

### 使用示例

**场景1：系统表头检测**
```ini
header_keywords = CREATED_BY,CREATE_TIMESTAMP,UPDATED_BY
header_detection_mode = auto
```

**场景2：业务表头检测**
```ini
header_keywords = id,name|编号,姓名
header_detection_mode = auto
```

**场景3：强制无表头处理**
```ini
header_detection_mode = force_no_header
```

## 文件名和表名映射

### 概述

工具会根据文件名自动推断目标表名。从v0.2.0开始，支持智能处理包含日期后缀的文件名。

### 表名推断规则

**基本转换规则：**
1. 提取文件名（去除扩展名）
2. 清理特殊字符，替换为下划线
3. 转换为大写字母
4. 确保以字母开头（否则添加T_前缀）
5. 限制长度不超过30字符（Oracle限制）

**转换示例：**
```
user_data.xlsx     → USER_DATA
order-list.csv     → ORDER_LIST  
product info.xls   → PRODUCT_INFO
123data.csv        → T_123DATA
```

### 日期后缀智能处理

**支持的日期格式：**
- `YYYYMMDD`: `order20250822.xlsx`
- `YYYY-MM-DD`: `data2025-08-22.csv`
- `YYYY_MM_DD`: `file2025_08_22.xls`
- `YYYYMM`: `sales202508.xlsx`
- `timestamp`: `backup1640995200.csv`
- `带序号`: `report20250822_001.xlsx`

### 使用模式

**默认模式：自动移除日期后缀**
```bash
oracle-import import -df ./data
```

转换效果：
```
T_order20250822.xlsx    → T_ORDER
T_order20250823.xlsx    → T_ORDER
user_data_2025-08-22.csv → USER_DATA
sales202508.xlsx        → SALES
```

适用场景：
- 日常数据导入，同类型文件导入到同一张表
- 历史数据合并
- 定期数据更新

**保留日期后缀模式：**
```bash
oracle-import import -df ./data --keep-date-suffix
```

转换效果：
```
T_order20250822.xlsx    → T_ORDER20250822
T_order20250823.xlsx    → T_ORDER20250823
user_data_2025-08-22.csv → USER_DATA_2025_08_22
sales202508.xlsx        → SALES202508
```

适用场景：
- 需要按日期分表存储
- 历史数据归档
- 数据版本控制

**显式指定表名：**
```bash
oracle-import import -df ./data -t T_ORDER,T_USER
```

特点：
- 优先级最高，忽略文件名推断
- 支持多个表名，用逗号分隔
- 文件和表名按顺序对应

### 最佳实践

1. **统一命名规范**：
   - 使用下划线分隔单词
   - 避免特殊字符和中文
   - 保持文件名简洁明了

2. **日期后缀使用建议**：
   - 日常导入：使用默认模式（移除日期）
   - 归档存储：使用`--keep-date-suffix`
   - 复杂场景：使用`-t`参数显式指定

3. **文件组织建议**：
   - 按数据类型分目录存放
   - 使用一致的日期格式
   - 避免文件名冲突

### 故障排除

**问题：表名过长被截断**
- 原因：Oracle表名限制30字符
- 解决：简化文件名或使用`-t`参数指定短表名

**问题：日期识别错误**
- 原因：特殊的数字组合被误认为日期
- 解决：使用`--keep-date-suffix`参数或重命名文件

**问题：表名冲突**
- 原因：多个文件映射到同一表名
- 解决：检查文件名规范，使用`-t`参数明确指定

## DDL文件格式

### 支持的格式

1. **SQL格式**（推荐）
2. **Markdown格式**

### SQL格式示例

```sql
-- users.sql
CREATE TABLE USERS (
    ID NUMBER(10) PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    EMAIL VARCHAR2(200),
    AGE NUMBER(3),
    DEPARTMENT VARCHAR2(50),
    SALARY NUMBER(10,2),
    HIRE_DATE DATE,
    CREATED_BY VARCHAR2(100) DEFAULT 'SYSTEM',
    CREATE_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UPDATED_BY VARCHAR2(100),
    UPDATE_TIMESTAMP TIMESTAMP
);
```

### 审计字段

工具自动添加以下审计字段：
- `CREATED_BY`: 创建用户（默认'SYSTEM'）
- `CREATE_TIMESTAMP`: 创建时间（默认当前时间）
- `UPDATED_BY`: 更新用户
- `UPDATE_TIMESTAMP`: 更新时间

这些字段在数据映射时会自动排除。

## 配置文件说明

### 数据库配置
```ini
[database]
host = localhost          # 数据库主机
port = 1521              # 端口
service_name = ORCLPDB1  # 服务名
username = user          # 用户名
password = pass          # 密码
schema = SCHEMA          # 模式名
```

### 导入设置
```ini
[import_settings]
batch_size = 1000                    # 批量插入大小
max_retries = 3                      # 最大重试次数
timeout = 30                         # 连接超时（秒）
auto_commit = true                   # 自动提交
create_table_if_not_exists = false  # 是否自动创建表（新版本默认false）
```

### 表头检测配置
```ini
[header_detection]
header_keywords =            # 表头关键词（支持AND/OR逻辑）
header_detection_mode = auto # 检测模式：auto/force_header/force_no_header
```

### 数据类型配置
```ini
[data_types]
string_max_length = 4000     # 字符串最大长度
number_precision = 38        # 数字精度
number_scale = 2             # 数字小数位
timestamp_format = YYYY-MM-DD HH24:MI:SS.FF6
```

## 命令行选项

### 主要命令

```bash
# 查看帮助
oracle-import --help

# 查看版本
oracle-import --version

# 配置管理
oracle-import config init          # 初始化配置
oracle-import config validate     # 验证配置

# 文件操作
oracle-import scan <folder>        # 扫描文件
oracle-import preview <file>       # 预览文件

# 数据库操作
oracle-import test-db              # 测试连接

# 数据导入
oracle-import import -df <folder>  # 导入数据
```

### 导入选项

```bash
oracle-import import [OPTIONS]

选项:
  -df, --datafolder PATH      数据文件夹路径（必需）
  -t, --table TEXT           指定导入的表名（逗号分隔）
  --ddl-folder PATH          DDL文件夹路径
  --create-sql               生成SQL文件到output目录
  -c, --config PATH          配置文件路径
  -b, --batch-size INTEGER   批量大小（1-10000）
  --dry-run                  干运行模式
  --max-retries INTEGER      重试次数（0-10）
  -v, --verbose              详细输出（-v: INFO, -vv: DEBUG）
  -q, --quiet                安静模式
  --no-color                 禁用彩色输出
```

### 使用示例

```bash
# 导入指定文件夹的所有数据
oracle-import import -df ./data

# 导入指定表
oracle-import import -df ./data -t users,products

# 使用DDL文件并生成SQL
oracle-import import -df ./data --ddl-folder ./ddl --create-sql

# 干运行验证
oracle-import import -df ./data --dry-run -v

# 自定义配置和批量大小
oracle-import import -df ./data -c my_config.ini -b 500
```

## 错误处理

### 常见错误及解决方案

**1. 表不存在错误**
```
错误: 表 USERS 不存在，请先创建该表或提供相应的DDL文件
```
**解决方案：**
- 手动创建表
- 提供DDL文件并手动执行建表语句
- 检查表名是否正确

**2. 数据库连接失败**
```
错误: 数据库连接失败
```
**解决方案：**
- 检查配置文件中的数据库连接信息
- 确认Oracle客户端已正确安装
- 测试网络连通性

**3. 文件格式不支持**
```
错误: 不支持的文件格式: .txt
```
**解决方案：**
- 使用支持的格式：.csv, .xls, .xlsx
- 转换文件格式

**4. 编码问题**
```
错误: 文件编码无法识别
```
**解决方案：**
- 将文件另存为UTF-8编码
- 使用-e参数指定编码格式

### 调试技巧

1. **使用详细输出**
   ```bash
   oracle-import import -df ./data -v
   ```

2. **干运行模式**
   ```bash
   oracle-import import -df ./data --dry-run
   ```

3. **检查日志文件**
   ```bash
   tail -f logs/import_*.log
   ```

## 最佳实践

### 1. 数据准备

- **文件命名**：使用与表名相同的文件名（如：users.csv → USERS表）
- **编码格式**：建议使用UTF-8编码
- **数据清理**：导入前清理空值和异常数据

### 2. 表结构设计

- **主键设置**：确保每个表都有主键
- **数据类型**：合理选择数据类型和长度
- **审计字段**：预留审计字段

### 3. 导入策略

- **分批导入**：大文件建议分批处理
- **测试优先**：先在测试环境验证
- **备份数据**：导入前备份重要数据

### 4. 性能优化

- **批量大小**：根据数据量调整batch_size
- **并发控制**：避免并发导入同一表
- **索引管理**：大量导入时可临时禁用索引

### 5. 监控和维护

- **日志监控**：定期检查导入日志
- **性能监控**：关注导入速度和数据库性能
- **数据验证**：导入后验证数据完整性

## 版本更新说明

### v0.2.0 主要更新
- 🔒 **安全增强**：默认不自动创建表
- 🧠 **智能处理**：支持无表头数据文件
- 📋 **DDL增强**：改进DDL文件处理逻辑
- 🐛 **问题修复**：解决pandas兼容性问题
- 📝 **文档完善**：更新用户手册和API文档

### 迁移指南

从v0.1.x升级到v0.2.0：

1. **更新配置文件**：`create_table_if_not_exists = false`
2. **预创建表**：确保所有目标表已存在
3. **测试导入**：使用`--dry-run`模式测试
4. **更新脚本**：检查自动化脚本中的表创建逻辑