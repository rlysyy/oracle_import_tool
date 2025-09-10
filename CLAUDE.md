# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个Oracle数据库导入工具，支持多种文件格式（XLS/XLSX/CSV）的批量数据导入。该项目使用Python开发，采用Poetry进行依赖管理。

## 常用开发命令

### 项目管理
```bash
# 安装项目依赖
poetry install

# 安装包含开发依赖
poetry install --with dev

# 激活虚拟环境
poetry shell
```

### 工具使用
```bash
# 主要命令入口
poetry run oracle-import

# 初始化配置文件
poetry run oracle-import config init

# 测试数据库连接
poetry run oracle-import test-db

# 扫描数据文件
poetry run oracle-import scan ./data

# 预览数据文件
poetry run oracle-import preview <file_path>

# 导入数据（基本用法）
poetry run oracle-import import -df ./data

# 导入数据（带DDL文件支持）
poetry run oracle-import import -df ./data --ddl-folder ./ddl

# 导入指定表
poetry run oracle-import import -df ./data -t table1,table2

# 干运行模式（验证但不导入）
poetry run oracle-import import -df ./data --dry-run

# 生成SQL文件
poetry run oracle-import import -df ./data --create-sql
```

### 开发和测试
```bash
# 运行单元测试
poetry run pytest

# 运行测试并生成覆盖率报告
poetry run pytest --cov

# 代码格式化
poetry run black .

# 代码类型检查
poetry run mypy .

# 代码风格检查
poetry run flake8

# 导入排序
poetry run isort .
```

### Windows特定命令
```bash
# 使用批处理脚本设置环境
setup_env.bat
```

## 项目架构

### 目录结构
```
oracle_import_tool/
├── oracle_import_tool/          # 主要源代码包
│   ├── main.py                 # CLI入口点，Click命令行界面
│   ├── config/                 # 配置管理模块
│   │   └── config_manager.py   # 配置文件处理和验证
│   ├── core/                   # 核心业务逻辑
│   │   ├── importer.py         # 数据导入核心逻辑
│   │   └── table_creator.py    # 表创建和结构管理
│   ├── data/                   # 数据处理模块
│   │   ├── file_reader.py      # 文件读取（支持XLS/XLSX/CSV）
│   │   └── ddl_parser.py       # DDL文件解析
│   ├── database/               # 数据库连接模块
│   │   └── connection.py       # Oracle数据库连接管理
│   └── utils/                  # 工具模块
│       ├── header_detector.py  # 表头检测器（支持可配置关键词）
│       ├── datetime_parser.py  # 日期时间解析
│       └── progress_manager.py # 进度条和状态管理
├── tests/                      # 测试文件
├── ddl/                        # DDL文件存放目录
├── data/                       # 数据文件存放目录
├── output/                     # 生成的输出文件目录
├── logs/                       # 日志文件目录
├── config.ini                  # 主配置文件
└── pyproject.toml             # Poetry配置和项目元数据
```

### 核心模块说明

#### 1. 配置管理 (config/)
- `config_manager.py`: 处理配置文件读取、验证和默认值设置
- 支持数据库连接、导入设置、表头检测等配置

#### 2. 核心逻辑 (core/)
- `importer.py`: 主要导入逻辑，协调各模块完成数据导入
- `table_creator.py`: 表结构推断和创建（v0.2.0后默认不自动创建表）

#### 3. 数据处理 (data/)
- `file_reader.py`: 支持多种文件格式读取，包含编码检测
- `ddl_parser.py`: 解析SQL和Markdown格式的DDL文件

#### 4. 数据库连接 (database/)
- `connection.py`: Oracle数据库连接池管理和操作封装

#### 5. 工具模块 (utils/)
- `header_detector.py`: 智能表头检测，支持关键词配置（AND/OR逻辑）
- `datetime_parser.py`: 日期时间格式解析和转换
- `progress_manager.py`: Rich库实现的多层进度条

## 关键特性和工作流程

### 1. 表头检测机制
- 支持可配置关键词检测（如：`CREATED_BY,CREATE_TIMESTAMP`）
- 支持AND/OR逻辑组合（逗号表示AND，竖线表示OR）
- 三种模式：auto（自动）、force_header（强制表头）、force_no_header（强制无表头）

### 2. 文件名到表名映射
- 自动清理特殊字符，转换为Oracle兼容的表名
- 智能处理日期后缀（如：`data20250822.csv` → `DATA`）
- 支持保留日期后缀模式（`--keep-date-suffix`参数）

### 3. DDL文件支持
- 支持无表头数据文件的智能处理
- 自动排除审计字段（CREATED_BY, CREATE_TIMESTAMP等）
- 按DDL定义的列顺序映射数据

### 4. 安全策略（v0.2.0）
- 默认不自动创建表（`create_table_if_not_exists = false`）
- 要求表必须预先存在或提供DDL文件
- 增强的错误提示和建议

## 配置文件重要参数

### 数据库连接
- `host`, `port`, `service_name`: Oracle连接参数
- `username`, `password`, `schema`: 认证和目标schema

### 导入设置
- `batch_size`: 批量插入大小（默认1000）
- `create_table_if_not_exists`: 是否自动创建表（默认false）
- `auto_commit`: 是否自动提交事务

### 表头检测（v0.2.0新增）
- `header_keywords`: 表头关键词配置
- `header_detection_mode`: 检测模式

## 开发注意事项

1. **依赖管理**: 使用Poetry管理依赖，添加新依赖需更新pyproject.toml
2. **代码风格**: 使用Black进行格式化，遵循PEP 8标准
3. **类型提示**: 项目使用MyPy进行类型检查，新代码需包含类型提示
4. **错误处理**: 重要操作需包含适当的异常处理和日志记录
5. **Windows兼容性**: 项目需要在Windows环境下正常运行
6. **Oracle特性**: 注意Oracle数据类型和约束的特殊处理
7. **界面显示**: 不使用emoji符号，所有界面元素使用ASCII字符以避免编码问题

## 测试和调试

### 测试环境
- 使用pytest作为测试框架
- 测试文件位于`tests/`目录
- 支持覆盖率报告生成

### 调试技巧
- 使用`--dry-run`参数进行测试验证
- 使用`-v`或`-vv`参数获取详细日志
- 检查`logs/`目录中的日志文件

### 常见问题排查
- 表不存在: 检查表名映射和DDL文件
- 编码问题: 检查文件编码格式
- 连接失败: 验证Oracle客户端和网络连接

## 版本信息

当前版本: v0.2.0
- 主要特性: 智能表头检测、DDL文件支持、安全表管理
- Python要求: 3.9+
- Oracle要求: 12c+