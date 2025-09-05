#!/bin/bash

echo "Oracle Import Tool - 环境设置脚本"
echo "================================="

# 检查Python
echo "正在检查Python环境..."
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到Python3，请先安装Python 3.8或更高版本"
    exit 1
fi

python3 --version

# 检查Poetry
echo "正在检查Poetry..."
if ! command -v poetry &> /dev/null; then
    echo "正在安装Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    if [ $? -ne 0 ]; then
        echo "错误: Poetry安装失败，请手动安装"
        exit 1
    fi
    
    # 添加Poetry到PATH
    export PATH="$HOME/.local/bin:$PATH"
fi

poetry --version

# 创建虚拟环境并安装依赖
echo "正在创建虚拟环境并安装依赖..."
poetry install

if [ $? -ne 0 ]; then
    echo "错误: 依赖安装失败"
    exit 1
fi

# 创建必要的目录
echo "正在创建必要的目录..."
mkdir -p logs
mkdir -p output

echo "环境设置完成！"
echo
echo "接下来的步骤:"
echo "1. 编辑 config.ini 文件，配置数据库连接信息"
echo "2. 运行 'poetry run oracle-import test-db' 测试数据库连接"
echo "3. 运行 'poetry run oracle-import scan ./doc' 扫描数据文件"
echo "4. 运行 'poetry run oracle-import import -df ./doc' 开始导入"
echo