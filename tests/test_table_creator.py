"""测试表创建器"""

import pytest
import pandas as pd
import tempfile
from unittest.mock import Mock, patch

from oracle_import_tool.core.table_creator import TableCreator
from oracle_import_tool.config.config_manager import ConfigManager


class TestTableCreator:
    """表创建器测试"""
    
    @pytest.fixture
    def mock_config_manager(self):
        """模拟配置管理器"""
        config = Mock(spec=ConfigManager)
        config.get_data_type_settings.return_value = {
            'string_max_length': 4000,
            'number_precision': 38,
            'number_scale': 2,
            'timestamp_format': 'YYYY-MM-DD HH24:MI:SS.FF6'
        }
        return config
    
    @pytest.fixture
    def table_creator(self, mock_config_manager):
        """创建表创建器实例"""
        return TableCreator(mock_config_manager)
    
    @pytest.fixture
    def sample_dataframe(self):
        """创建样例DataFrame"""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['张三', '李四', '王五'],
            'age': [25, 30, 28],
            'salary': [5000.50, 6000.75, 5500.25],
            'is_active': [True, False, True],
            'created_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
    
    def test_infer_table_structure(self, table_creator, sample_dataframe):
        """测试推断表结构"""
        table_info = table_creator.infer_table_structure(sample_dataframe, 'TEST_TABLE')
        
        assert table_info['table_name'] == 'TEST_TABLE'
        assert 'columns' in table_info
        
        columns = table_info['columns']
        column_names = [col['name'] for col in columns]
        
        # 检查列名
        assert 'ID' in column_names
        assert 'NAME' in column_names
        assert 'AGE' in column_names
        assert 'SALARY' in column_names
        
        # 检查数据类型推断
        id_col = next(col for col in columns if col['name'] == 'ID')
        assert id_col['data_type'] == 'NUMBER'
        
        name_col = next(col for col in columns if col['name'] == 'NAME')
        assert name_col['data_type'] == 'VARCHAR2'
        
        salary_col = next(col for col in columns if col['name'] == 'SALARY')
        assert salary_col['data_type'] == 'NUMBER'
        
    def test_infer_column_type_integer(self, table_creator):
        """测试推断整数列类型"""
        series = pd.Series([1, 2, 3, 4, 5])
        col_info = table_creator._infer_column_type(series, 'TEST_COL')
        
        assert col_info['data_type'] == 'NUMBER'
        assert col_info['precision'] == 38
        assert col_info['scale'] == 0
        
    def test_infer_column_type_float(self, table_creator):
        """测试推断浮点数列类型"""
        series = pd.Series([1.5, 2.7, 3.14, 4.99, 5.0])
        col_info = table_creator._infer_column_type(series, 'TEST_COL')
        
        assert col_info['data_type'] == 'NUMBER'
        assert col_info['precision'] == 38
        assert col_info['scale'] == 2
        
    def test_infer_column_type_string(self, table_creator):
        """测试推断字符串列类型"""
        series = pd.Series(['短文本', '这是一个比较长的文本内容', '另一个文本'])
        col_info = table_creator._infer_column_type(series, 'TEST_COL')
        
        assert col_info['data_type'] == 'VARCHAR2'
        assert col_info['length'] > 0
        
    def test_infer_column_type_datetime(self, table_creator):
        """测试推断日期时间列类型"""
        series = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        col_info = table_creator._infer_column_type(series, 'TEST_COL')
        
        assert col_info['data_type'] == 'TIMESTAMP'
        
    def test_generate_create_table_sql(self, table_creator, sample_dataframe):
        """测试生成建表SQL"""
        table_info = table_creator.infer_table_structure(sample_dataframe, 'TEST_TABLE')
        sql = table_creator.generate_create_table_sql(table_info)
        
        assert 'CREATE TABLE TEST_TABLE' in sql
        assert 'ID NUMBER(38,0)' in sql
        assert 'NAME VARCHAR2' in sql
        assert 'CREATED_BY VARCHAR2(100)' in sql
        assert 'CREATE_TIMESTAMP TIMESTAMP' in sql
        
    def test_validate_table_structure_valid(self, table_creator, sample_dataframe):
        """测试验证有效的表结构"""
        table_info = table_creator.infer_table_structure(sample_dataframe, 'TEST_TABLE')
        errors = table_creator.validate_table_structure(table_info)
        
        assert len(errors) == 0
        
    def test_validate_table_structure_invalid_name(self, table_creator):
        """测试验证无效的表名"""
        table_info = {
            'table_name': '123_INVALID',  # 以数字开头
            'columns': [
                {
                    'name': 'ID',
                    'data_type': 'NUMBER',
                    'precision': 38,
                    'scale': 0,
                    'nullable': False
                }
            ]
        }
        
        errors = table_creator.validate_table_structure(table_info)
        assert len(errors) > 0
        assert any('表名' in error for error in errors)
        
    def test_merge_with_ddl_info(self, table_creator, sample_dataframe):
        """测试合并DDL信息"""
        inferred_info = table_creator.infer_table_structure(sample_dataframe, 'TEST_TABLE')
        
        ddl_info = {
            'table_name': 'TEST_TABLE',
            'columns': [
                {
                    'name': 'ID',
                    'data_type': 'NUMBER',
                    'precision': 10,  # 覆盖推断的精度
                    'scale': 0,
                    'nullable': False
                },
                {
                    'name': 'DESCRIPTION',  # 新增列
                    'data_type': 'VARCHAR2',
                    'length': 500,
                    'nullable': True
                }
            ]
        }
        
        merged_info = table_creator.merge_with_ddl_info(inferred_info, ddl_info)
        
        # 检查ID列精度被覆盖
        id_col = next(col for col in merged_info['columns'] if col['name'] == 'ID')
        assert id_col['precision'] == 10
        
        # 检查新增列
        column_names = [col['name'] for col in merged_info['columns']]
        assert 'DESCRIPTION' in column_names
        
    def test_get_max_string_length(self, table_creator):
        """测试获取最大字符串长度"""
        series = pd.Series(['短', '中等长度文本', '这是一个非常长的文本内容，用来测试最大长度计算'])
        max_length = table_creator._get_max_string_length(series)
        
        assert max_length > 0
        assert max_length >= len('这是一个非常长的文本内容，用来测试最大长度计算')
        
    def test_normalize_column_name(self, table_creator):
        """测试规范化列名"""
        # 测试正常列名
        assert table_creator._normalize_column_name('test_column') == 'TEST_COLUMN'
        
        # 测试包含特殊字符的列名
        assert table_creator._normalize_column_name('test-column name') == 'TEST_COLUMN_NAME'
        
        # 测试中文列名
        assert table_creator._normalize_column_name('用户姓名') == 'COL_1'
        
    def test_add_audit_columns(self, table_creator):
        """测试添加审计列"""
        columns = [
            {
                'name': 'ID',
                'data_type': 'NUMBER',
                'precision': 38,
                'scale': 0,
                'nullable': False
            }
        ]
        
        audit_columns = table_creator._add_audit_columns(columns)
        
        column_names = [col['name'] for col in audit_columns]
        assert 'CREATED_BY' in column_names
        assert 'CREATE_TIMESTAMP' in column_names
        assert 'UPDATED_BY' in column_names
        assert 'UPDATE_TIMESTAMP' in column_names
        
    def test_empty_dataframe(self, table_creator):
        """测试空DataFrame"""
        empty_df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="数据为空"):
            table_creator.infer_table_structure(empty_df, 'TEST_TABLE')