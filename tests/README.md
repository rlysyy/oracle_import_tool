# 测试文件结构

本目录包含Oracle导入工具的所有测试相关文件，按功能和类型进行组织。

## 📁 目录结构

```
tests/
├── README.md                   # 本文件，测试说明
├── run_tests.py               # 统一测试运行入口
├── __init__.py                # Python包初始化文件
├── conftest.py                # Pytest配置文件
│
├── test_*.py                  # 单元测试（pytest格式）
├── test_basic.py              # 基础功能测试
├── test_config_manager.py     # 配置管理器测试
├── test_config_only.py        # 纯配置测试
├── test_file_reader.py        # 文件读取器测试
├── test_importer.py           # 导入器核心功能测试
├── test_integration.py        # 集成测试
├── test_table_creator.py      # 表创建器测试
│
├── manual/                    # 手动测试脚本
│   ├── basic_test.py          # 基础功能手动测试
│   ├── debug_no_header.py     # 无表头数据调试脚本
│   ├── final_test.py          # 表头检测综合测试
│   ├── simple_test.py         # 简单功能测试
│   ├── test_cases.py          # 测试用例集合
│   ├── test_date_suffix.py    # 日期后缀处理测试
│   ├── test_import.py         # 导入功能测试
│   └── test_no_header.py      # 无表头数据测试
│
├── integration/               # 集成测试（预留）
│   └── (待添加端到端测试)
│
└── examples/                  # 示例和演示脚本
    └── quick_start.py         # 快速入门示例
```

## 🧪 测试类型说明

### 1. **单元测试** (根目录下的 `test_*.py`)
- 使用 pytest 框架
- 测试单个模块和函数
- 可以独立运行
- 包含断言和异常处理测试

### 2. **手动测试脚本** (`manual/` 目录)
- 用于人工验证和调试
- 包含详细的输出信息
- 适合开发和调试阶段使用
- 可以观察详细的运行过程

### 3. **集成测试** (`integration/` 目录)
- 测试多个组件协同工作
- 端到端功能验证
- 数据库集成测试

### 4. **示例脚本** (`examples/` 目录)
- 演示如何使用工具
- 快速入门代码
- 最佳实践示例

## 🚀 如何运行测试

### 运行所有单元测试
```bash
# 使用 pytest（推荐）
pytest tests/

# 或者使用统一测试脚本
python tests/run_tests.py
```

### 运行特定的单元测试
```bash
pytest tests/test_file_reader.py
pytest tests/test_config_manager.py -v
```

### 运行手动测试脚本
```bash
# 测试表头检测功能
python tests/manual/final_test.py

# 测试日期后缀处理
python tests/manual/test_date_suffix.py

# 调试无表头数据
python tests/manual/debug_no_header.py
```

### 运行示例代码
```bash
python tests/examples/quick_start.py
```

## 📋 测试覆盖的功能

### 核心功能测试
- ✅ 配置文件管理 (`test_config_manager.py`)
- ✅ 文件读取和解析 (`test_file_reader.py`)
- ✅ 数据导入核心逻辑 (`test_importer.py`)
- ✅ 表结构创建 (`test_table_creator.py`)
- ✅ 表头检测逻辑 (`manual/final_test.py`)
- ✅ 日期后缀处理 (`manual/test_date_suffix.py`)

### 数据处理测试
- ✅ 有表头数据处理
- ✅ 无表头数据处理 (`manual/test_no_header.py`)
- ✅ DDL文件解析
- ✅ 各种文件格式支持 (CSV, XLS, XLSX)

### 配置和设置测试
- ✅ 数据库连接配置
- ✅ 导入参数配置
- ✅ 表头检测配置
- ✅ 日志配置

## 🐛 调试和开发

### 开发新测试时的建议
1. **单元测试**: 添加到根目录，使用 `test_` 前缀，遵循 pytest 规范
2. **手动测试**: 添加到 `manual/` 目录，包含详细输出和调试信息
3. **示例代码**: 添加到 `examples/` 目录，注重用户友好性

### 测试数据管理
- 测试数据文件放在各自测试脚本的同目录下
- 使用临时文件进行测试，避免影响真实数据
- 测试完成后清理临时文件

## ⚠️ 注意事项

1. **数据库连接**: 某些测试需要真实的Oracle数据库连接
2. **权限要求**: 确保测试环境有足够的数据库权限
3. **依赖关系**: 运行前确保安装了所有依赖包
4. **配置文件**: 某些测试会创建临时配置文件
5. **日期测试**: 日期后缀测试不需要数据库连接，可以离线运行

## 📊 测试报告

运行测试后会生成相应的报告和日志，帮助识别问题和验证功能正确性。