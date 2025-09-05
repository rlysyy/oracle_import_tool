# Oracle Import Tool - 安装和使用手册

Oracle数据库导入工具，支持多种文件格式（XLS/XLSX/CSV）的批量数据导入。

## ✨ 功能特色

- 🗂️ **多格式支持**: 支持XLS、XLSX、CSV文件格式
- 🔄 **智能表结构推断**: 根据数据自动推断Oracle表结构（需要预先创建表或提供DDL）
- 📋 **DDL文件支持**: 支持使用SQL和Markdown格式的DDL文件定义表结构，支持无表头数据文件的智能处理
- 🎯 **可配置表头检测**: 支持通过关键词配置表头检测逻辑，支持AND/OR逻辑组合
- 🔄 **智能文件名映射**: 支持日期后缀自动处理，灵活的表名映射策略
- 📊 **进度显示**: 多层次进度条，实时显示导入进度和速度
- 🎯 **批量处理**: 高效的批量插入，可配置批次大小
- 🔧 **灵活配置**: 完整的配置文件系统
- 📝 **详细日志**: 完整的导入日志和错误记录
- 🚀 **命令行界面**: 现代化的Click命令行界面
- 🎨 **富文本输出**: 彩色输出和表格显示

## 📋 系统要求

- Python 3.9+
- Oracle Database 12c+
- Oracle客户端或Instant Client

## 🚀 快速开始

### 1. 环境准备

**克隆项目:**
```bash
git clone <repository-url>
cd oracle_import_tool
```

**安装依赖:**
```bash
# 使用Poetry（推荐）
poetry install

# 或使用pip
pip install -r requirements.txt
```

### 2. 配置数据库连接

**生成配置文件:**
```bash
# 使用Poetry
poetry run oracle-import config init

# 或直接运行
python -m oracle_import_tool.main config init
```

**编辑配置文件:**
```bash
# Windows
notepad config.ini

# Linux/Mac
vim config.ini
```

**配置文件示例:**
```ini
[database]
host = localhost
port = 1521
service_name = ORCLPDB1
username = your_username
password = your_password
schema = YOUR_SCHEMA

[import_settings]
batch_size = 1000
create_table_if_not_exists = false

[header_detection]
header_keywords = CREATED_BY,CREATE_TIMESTAMP
header_detection_mode = auto
```

### 3. 测试数据库连接

```bash
poetry run oracle-import test-db
```

### 4. 开始导入数据

**基本导入:**
```bash
# 导入指定文件夹下的所有文件
poetry run oracle-import import -df ./data

# 导入指定表
poetry run oracle-import import -df ./data -t table1,table2

# 使用DDL文件处理无表头数据
poetry run oracle-import import -df ./data --ddl-folder ./ddl
```

**高级选项:**
```bash
# 保留日期后缀（适用于按日期分表）
poetry run oracle-import import -df ./data --keep-date-suffix

# 干运行模式，验证而不实际导入
poetry run oracle-import import -df ./data --dry-run

# 生成SQL文件
poetry run oracle-import import -df ./data --create-sql
```

## 📖 详细使用指南

### 表管理策略

从v0.2.0开始，工具采用更安全的表管理策略：

- **表必须预先存在**: 工具不会自动创建表，确保数据安全性
- **DDL文件支持**: 通过DDL文件定义表结构和数据映射关系
- **错误提示**: 当表不存在时会给出明确的错误提示和建议

### 无表头数据处理

工具支持智能处理没有表头的数据文件：

**工作原理:**
1. 工具首先使用可配置的表头检测器检测数据文件是否包含表头
2. 支持通过关键词配置表头检测逻辑（AND/OR逻辑组合）
3. 如果未检测到表头且提供了DDL文件，使用DDL中的列定义
4. 按照DDL定义的列顺序映射数据

**使用示例:**
```bash
# 使用DDL文件定义列结构，自动处理无表头数据
oracle-import import -df ./data --ddl-folder ./ddl
```

**数据文件（无表头）:**
```csv
1,张三,zhang@example.com,25
2,李四,li@example.com,30
3,王五,wang@example.com,28
```

**对应的DDL文件（users.sql）:**
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

### 表头检测配置

从v0.2.0开始，工具提供了灵活的表头检测配置功能，支持通过关键词来定义表头检测逻辑。

**配置参数:**
```ini
[header_detection]
# 表头关键词配置，支持AND/OR逻辑
header_keywords = CREATED_BY,CREATE_TIMESTAMP

# 检测模式
header_detection_mode = auto
```

**关键词配置语法:**

**AND关系（逗号分隔）:**
```ini
header_keywords = CREATED_BY,CREATE_TIMESTAMP
```
- 表示：同时包含 `CREATED_BY` 和 `CREATE_TIMESTAMP` 两个关键词才认为是表头

**OR关系（竖线分隔）:**
```ini
header_keywords = CREATE_TIMESTAMP|CREATED_BY
```
- 表示：包含 `CREATE_TIMESTAMP` 或 `CREATED_BY` 任一关键词就认为是表头

**混合逻辑:**
```ini
header_keywords = id,name|code,type
```
- 表示：`(id AND name) OR (code AND type)`

**检测模式:**
- `auto`: 自动检测（默认）- 如果配置了关键词，使用关键词匹配；否则使用默认检测逻辑
- `force_header`: 强制认为第一行是表头
- `force_no_header`: 强制认为第一行是数据

**使用示例:**

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

### 文件名和表名映射

工具会根据文件名自动推断目标表名。从v0.2.0开始，支持智能处理包含日期后缀的文件名。

**表名推断规则:**

1. 提取文件名（去除扩展名）
2. 清理特殊字符，替换为下划线
3. 转换为大写字母
4. 确保以字母开头（否则添加T_前缀）
5. 限制长度不超过30字符（Oracle限制）

**转换示例:**
```
user_data.xlsx     → USER_DATA
order-list.csv     → ORDER_LIST  
product info.xls   → PRODUCT_INFO
123data.csv        → T_123DATA
```

**日期后缀智能处理:**

支持的日期格式：
- `YYYYMMDD`: `order20250822.xlsx`
- `YYYY-MM-DD`: `data2025-08-22.csv`
- `YYYY_MM_DD`: `file2025_08_22.xls`
- `YYYYMM`: `sales202508.xlsx`
- `timestamp`: `backup1640995200.csv`
- `带序号`: `report20250822_001.xlsx`

**使用模式:**

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

**保留日期后缀模式:**
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

**显式指定表名:**
```bash
oracle-import import -df ./data -t T_ORDER,T_USER
```

### DDL文件格式

**支持的格式:**

1. **SQL格式**（推荐）
2. **Markdown格式**

**SQL格式示例:**
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

**审计字段:**

工具自动添加以下审计字段：
- `CREATED_BY`: 创建用户（默认'SYSTEM'）
- `CREATE_TIMESTAMP`: 创建时间（默认当前时间）
- `UPDATED_BY`: 更新用户
- `UPDATE_TIMESTAMP`: 更新时间

这些字段在数据映射时会自动排除。

### 配置文件说明

**数据库配置:**
```ini
[database]
host = localhost          # 数据库主机
port = 1521              # 端口
service_name = ORCLPDB1  # 服务名
username = user          # 用户名
password = pass          # 密码
schema = SCHEMA          # 模式名
```

**导入设置:**
```ini
[import_settings]
batch_size = 1000                    # 批量插入大小
max_retries = 3                      # 最大重试次数
timeout = 30                         # 连接超时（秒）
auto_commit = true                   # 自动提交
create_table_if_not_exists = false  # 是否自动创建表（新版本默认false）
```

**表头检测配置:**
```ini
[header_detection]
header_keywords =            # 表头关键词（支持AND/OR逻辑）
header_detection_mode = auto # 检测模式：auto/force_header/force_no_header
```

**数据类型配置:**
```ini
[data_types]
string_max_length = 4000     # 字符串最大长度
number_precision = 38        # 数字精度
number_scale = 2             # 数字小数位
timestamp_format = YYYY-MM-DD HH24:MI:SS.FF6
```

**日志配置:**
```ini
[logging]
log_level = INFO             # 日志级别
log_format = %(asctime)s - %(name)s - %(levelname)s - %(message)s
console_output = true        # 控制台输出
```

### 命令行选项

**查看所有命令:**
```bash
oracle-import --help
```

**导入命令选项:**
```bash
oracle-import import --help
```

**主要参数:**
- `--datafolder, -df`: 数据文件夹路径（必须）
- `--table, -t`: 指定表名，多个用逗号分隔
- `--ddl-folder`: DDL文件夹路径
- `--keep-date-suffix`: 保留文件名中的日期后缀
- `--create-sql`: 生成INSERT SQL文件
- `--dry-run`: 干运行模式，验证但不导入
- `--batch-size, -b`: 批量插入大小
- `--verbose, -v`: 详细输出模式
- `--quiet, -q`: 安静模式

**其他命令:**
- `config init`: 初始化配置文件
- `test-db`: 测试数据库连接
- `scan [folder]`: 扫描文件夹中的数据文件
- `preview [file]`: 预览数据文件内容

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

4. **安全建议**：
   - 生产环境设置`create_table_if_not_exists = false`
   - 使用强密码并妥善保管配置文件
   - 定期备份重要数据

### 错误处理

**常见错误及解决方案:**

1. **数据库连接失败**
   - 检查数据库服务是否启动
   - 验证连接参数是否正确
   - 确认网络连接正常

2. **表不存在错误**
   - 确认目标表已创建
   - 检查表名拼写和大小写
   - 使用DDL文件定义表结构

3. **数据类型错误**
   - 检查数据格式是否符合表结构
   - 调整数据类型配置参数
   - 清理和标准化数据

4. **文件编码问题**
   - 使用正确的编码参数
   - 转换文件编码格式
   - 检查特殊字符处理

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

## 🔧 开发和测试

### 开发环境搭建

```bash
# 安装开发依赖
poetry install --with dev

# 运行测试
poetry run pytest

# 代码格式化
poetry run black .

# 类型检查
poetry run mypy .
```

### 测试文件结构

```
tests/
├── README.md                   # 测试说明
├── run_tests.py               # 统一测试运行入口
├── test_*.py                  # 单元测试（pytest格式）
├── manual/                    # 手动测试脚本
│   ├── test_date_suffix.py    # 日期后缀处理测试
│   ├── final_test.py          # 表头检测综合测试
│   └── ...                    # 其他测试脚本
└── examples/                  # 示例和演示脚本
    └── quick_start.py         # 快速入门示例
```

### 运行测试

```bash
# 运行所有单元测试
pytest tests/

# 运行手动测试脚本
python tests/manual/test_date_suffix.py
python tests/manual/final_test.py

# 运行示例代码
python tests/examples/quick_start.py
```

## 📊 版本历史

### v0.2.0 (当前版本)
- 新增可配置表头检测功能
- 支持智能日期后缀处理
- 增强DDL文件支持
- 改进错误处理和日志记录
- 优化CLI界面和参数

### v0.1.0
- 基础数据导入功能
- 多文件格式支持
- 基本配置管理

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交问题报告和功能建议！

## 📞 技术支持

如需技术支持或有任何问题，请查看项目文档或提交Issue。