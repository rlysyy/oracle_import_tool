# Oracle Import Tool

Oracle数据库导入工具，支持多种文件格式（XLS/XLSX/CSV）的批量数据导入。

## ✨ 功能特色

- 🗂️ **多格式支持**: 支持XLS、XLSX、CSV文件格式
- 🔄 **智能表结构推断**: 根据数据自动推断Oracle表结构（需要预先创建表或提供DDL）
- 📋 **DDL文件支持**: 支持使用SQL和Markdown格式的DDL文件定义表结构，支持无表头数据文件的智能处理
- 🎯 **可配置表头检测**: 支持通过关键词配置表头检测逻辑，支持AND/OR逻辑组合
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

### 1. 克隆项目

```bash
git clone <repository-url>
cd oracle_import_tool
```

### 2. 创建虚拟环境

```bash
# 使用Poetry创建虚拟环境（推荐）
poetry install

# 或使用pip
pip install -r requirements.txt
```

### 3. 配置数据库连接

```bash
# 生成配置文件
poetry run oracle-import config init

# 编辑配置文件
vim config.ini
```

### 4. 测试数据库连接

```bash
poetry run oracle-import test-db
```

### 5. 开始导入数据

```bash
# 导入指定文件夹下的所有文件
poetry run oracle-import import -df ./data

# 导入指定表
poetry run oracle-import import -df ./data -t table1,table2

# 使用DDL文件
poetry run oracle-import import -df ./data --ddl-folder ./ddl
```

## 📖 详细文档

详细的安装和使用说明请查看：[安装和使用手册](INSTALL.md)

## 🔧 开发

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

## 🔧 高级功能

### 表管理策略

从v0.2.0开始，工具采用更安全的表管理策略：

- **表必须预先存在**: 工具不会自动创建表，确保数据安全性
- **DDL文件支持**: 通过DDL文件定义表结构和数据映射关系
- **错误提示**: 当表不存在时会给出明确的错误提示和建议

### 无表头数据处理

支持处理没有表头的数据文件：

```bash
# 使用DDL文件定义列结构，自动处理无表头数据
oracle-import import -df ./data --ddl-folder ./ddl
```

**工作原理:**
1. 工具首先使用可配置的表头检测器检测数据文件是否包含表头
2. 支持通过关键词配置表头检测逻辑（AND/OR逻辑组合）
3. 如果未检测到表头且提供了DDL文件，使用DDL中的列定义
4. 按照DDL定义的列顺序映射数据

**DDL文件示例:**
```sql
-- users.sql
CREATE TABLE USERS (
    ID NUMBER(10) PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    EMAIL VARCHAR2(200),
    AGE NUMBER(3),
    CREATED_BY VARCHAR2(100) DEFAULT 'SYSTEM',
    CREATE_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 配置变更

默认配置已更新：
- `create_table_if_not_exists` 默认为 `false`
- 更严格的表存在性检查
- 增强的错误提示和日志记录
- 新增 `header_detection` 配置节用于表头检测

### 表头检测配置

配置文件中新增表头检测参数：

```ini
[header_detection]
# 表头关键词配置，支持AND/OR逻辑
# 示例：
# CREATED_BY,CREATE_TIMESTAMP  (AND关系：两个关键词都必须存在)
# CREATE_TIMESTAMP|CREATED_BY  (OR关系：任一关键词存在即可)
# id,name|code,type           (混合：(id AND name) OR (code AND type))
header_keywords = 

# 检测模式：auto(自动检测), force_header(强制表头), force_no_header(强制无表头)
header_detection_mode = auto
```

**关键词配置规则：**
- 使用逗号（`,`）表示AND关系
- 使用竖线（`|`）表示OR关系
- 可以组合使用，支持复杂的逻辑表达式
- 关键词不区分大小写

## 📄 许可证

MIT License