"""pytest配置文件"""

import pytest
import tempfile
import os
from pathlib import Path


@pytest.fixture
def temp_directory():
    """创建临时目录"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    
    try:
        import shutil
        shutil.rmtree(temp_dir)
    except:
        pass


@pytest.fixture
def sample_csv_data():
    """样例CSV数据"""
    return """id,name,age,email
1,张三,25,zhangsan@example.com
2,李四,30,lisi@example.com
3,王五,28,wangwu@example.com"""


@pytest.fixture
def sample_dataframe():
    """样例DataFrame"""
    try:
        import pandas as pd
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['张三', '李四', '王五', '赵六', '孙七'],
            'age': [25, 30, 28, 35, 22],
            'salary': [5000.50, 6000.75, 5500.25, 7000.00, 4500.80],
            'is_active': [True, False, True, True, False],
            'created_date': pd.to_datetime([
                '2023-01-01', '2023-01-02', '2023-01-03', 
                '2023-01-04', '2023-01-05'
            ])
        })
    except ImportError:
        pytest.skip("pandas not available")


@pytest.fixture
def temp_csv_file(temp_directory, sample_csv_data):
    """创建临时CSV文件"""
    csv_file = os.path.join(temp_directory, 'test_data.csv')
    with open(csv_file, 'w', encoding='utf-8') as f:
        f.write(sample_csv_data)
    return csv_file


@pytest.fixture
def temp_excel_file(temp_directory, sample_dataframe):
    """创建临时Excel文件"""
    try:
        excel_file = os.path.join(temp_directory, 'test_data.xlsx')
        sample_dataframe.to_excel(excel_file, index=False)
        return excel_file
    except ImportError:
        pytest.skip("pandas not available")


@pytest.fixture
def config_content():
    """标准配置文件内容"""
    return """
[database]
host = localhost
port = 1521
service_name = ORCLPDB1.localdomain
username = testuser
password = testpass
schema = TESTSCHEMA

[import_settings]
batch_size = 1000
max_retries = 3
timeout = 30
auto_commit = true
create_table_if_not_exists = true

[data_types]
string_max_length = 4000
number_precision = 38
number_scale = 2
timestamp_format = YYYY-MM-DD HH24:MI:SS.FF6

[logging]
log_level = INFO
log_format = %(asctime)s - %(name)s - %(levelname)s - %(message)s
console_output = true
"""


@pytest.fixture
def temp_config_file(temp_directory, config_content):
    """创建临时配置文件"""
    config_file = os.path.join(temp_directory, 'test_config.ini')
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write(config_content)
    return config_file


@pytest.fixture
def sample_ddl_content():
    """样例DDL内容"""
    return """
-- 用户表结构定义
CREATE TABLE USERS (
    ID NUMBER(10) PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    EMAIL VARCHAR2(200),
    AGE NUMBER(3),
    CREATED_BY VARCHAR2(100) DEFAULT 'SYSTEM',
    CREATE_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@pytest.fixture
def temp_ddl_file(temp_directory, sample_ddl_content):
    """创建临时DDL文件"""
    ddl_file = os.path.join(temp_directory, 'users.sql')
    with open(ddl_file, 'w', encoding='utf-8') as f:
        f.write(sample_ddl_content)
    return ddl_file


# 清理fixture
@pytest.fixture(autouse=True)
def cleanup_logs():
    """自动清理测试日志文件"""
    yield
    
    # 清理测试过程中可能创建的日志文件
    log_dir = Path('logs')
    if log_dir.exists():
        for log_file in log_dir.glob('*.log'):
            try:
                log_file.unlink()
            except:
                pass