"""测试导入器"""

import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from oracle_import_tool.core.importer import OracleImporter


class TestOracleImporter:
    """导入器测试"""
    
    @pytest.fixture
    def temp_config_file(self):
        """创建临时配置文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""
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

[logging]
log_level = INFO
console_output = false
""")
            temp_file = f.name
        
        yield temp_file
        
        try:
            os.unlink(temp_file)
        except:
            pass
    
    @pytest.fixture
    def temp_data_directory(self):
        """创建临时数据目录"""
        temp_dir = tempfile.mkdtemp()
        
        # 创建测试CSV文件
        csv_file = os.path.join(temp_dir, 'test_table.csv')
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['测试1', '测试2', '测试3'],
            'age': [25, 30, 35]
        })
        df.to_csv(csv_file, index=False)
        
        # 创建测试Excel文件
        excel_file = os.path.join(temp_dir, 'another_table.xlsx')
        df2 = pd.DataFrame({
            'code': ['A001', 'A002', 'A003'],
            'description': ['描述1', '描述2', '描述3'],
            'price': [100.50, 200.75, 150.25]
        })
        df2.to_excel(excel_file, index=False)
        
        yield temp_dir
        
        try:
            import shutil
            shutil.rmtree(temp_dir)
        except:
            pass
    
    @pytest.fixture
    def mock_importer(self, temp_config_file):
        """创建模拟的导入器"""
        with patch.multiple(
            'oracle_import_tool.core.importer',
            DatabaseConnection=MagicMock(),
            FileReader=MagicMock(),
            DDLParser=MagicMock(),
            TableCreator=MagicMock(),
            ProgressBarManager=MagicMock()
        ):
            console_mock = Mock()
            importer = OracleImporter(temp_config_file, console_mock)
            
            # 模拟数据库连接
            importer.db_connection.connect.return_value = True
            importer.db_connection.test_connection.return_value = True
            importer.db_connection.table_exists.return_value = False
            importer.db_connection.execute_ddl.return_value = None
            importer.db_connection.batch_insert.return_value = 3
            importer.db_connection.transaction.return_value.__enter__.return_value = None
            importer.db_connection.transaction.return_value.__exit__.return_value = None
            
            return importer
    
    def test_importer_initialization(self, temp_config_file):
        """测试导入器初始化"""
        console_mock = Mock()
        importer = OracleImporter(temp_config_file, console_mock)
        
        assert importer.config_manager is not None
        assert importer.console == console_mock
        assert 'total_files' in importer.import_results
        
    def test_scan_data_files(self, mock_importer, temp_data_directory):
        """测试扫描数据文件"""
        # 模拟文件扫描结果
        mock_files = [
            {
                'name': 'test_table.csv',
                'path': os.path.join(temp_data_directory, 'test_table.csv'),
                'table_name': 'TEST_TABLE',
                'extension': '.csv',
                'size': 1000
            },
            {
                'name': 'another_table.xlsx',
                'path': os.path.join(temp_data_directory, 'another_table.xlsx'),
                'table_name': 'ANOTHER_TABLE',
                'extension': '.xlsx',
                'size': 2000
            }
        ]
        
        mock_importer.file_reader.scan_directory.return_value = mock_files
        
        files = mock_importer._scan_data_files(temp_data_directory)
        
        assert len(files) == 2
        assert files[0]['table_name'] == 'TEST_TABLE'
        assert files[1]['table_name'] == 'ANOTHER_TABLE'
        
    def test_scan_data_files_with_filter(self, mock_importer, temp_data_directory):
        """测试带过滤器的文件扫描"""
        mock_files = [
            {
                'name': 'test_table.csv',
                'path': os.path.join(temp_data_directory, 'test_table.csv'),
                'table_name': 'TEST_TABLE',
                'extension': '.csv',
                'size': 1000
            },
            {
                'name': 'another_table.xlsx',
                'path': os.path.join(temp_data_directory, 'another_table.xlsx'),
                'table_name': 'ANOTHER_TABLE',
                'extension': '.xlsx',
                'size': 2000
            }
        ]
        
        mock_importer.file_reader.scan_directory.return_value = mock_files
        
        # 只处理TEST_TABLE
        files = mock_importer._scan_data_files(temp_data_directory, ['test_table'])
        
        assert len(files) == 1
        assert files[0]['table_name'] == 'TEST_TABLE'
        
    def test_estimate_total_rows(self, mock_importer):
        """测试估算总行数"""
        files_info = [
            {'path': '/path/to/file1.csv'},
            {'path': '/path/to/file2.csv'},
            {'path': '/path/to/file3.csv'}
        ]
        
        # 模拟读取文件返回不同行数的DataFrame
        mock_dfs = [
            pd.DataFrame({'col1': range(100)}),  # 100行
            pd.DataFrame({'col1': range(150)}),  # 150行
            pd.DataFrame({'col1': range(200)})   # 200行
        ]
        
        mock_importer.file_reader.read_file.side_effect = mock_dfs
        
        total_rows = mock_importer._estimate_total_rows(files_info)
        
        # 预期 (100 + 150 + 200) = 450行
        assert total_rows == 450
        
    def test_prepare_data_for_insert(self, mock_importer):
        """测试准备插入数据"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['张三', '李四', '王五'],
            'age': [25, 30, 28]
        })
        
        table_info = {
            'table_name': 'TEST_TABLE',
            'columns': [
                {'name': 'ID', 'data_type': 'NUMBER'},
                {'name': 'NAME', 'data_type': 'VARCHAR2', 'length': 100},
                {'name': 'AGE', 'data_type': 'NUMBER'},
                {'name': 'CREATED_BY', 'data_type': 'VARCHAR2', 'length': 100},
                {'name': 'CREATE_TIMESTAMP', 'data_type': 'TIMESTAMP'},
                {'name': 'UPDATED_BY', 'data_type': 'VARCHAR2', 'length': 100},
                {'name': 'UPDATE_TIMESTAMP', 'data_type': 'TIMESTAMP'}
            ]
        }
        
        # 修改列名以匹配DataFrame
        df.columns = ['ID', 'NAME', 'AGE']
        
        data_rows = mock_importer._prepare_data_for_insert(df, table_info)
        
        assert len(data_rows) == 3
        assert len(data_rows[0]) == 7  # 3个数据列 + 4个审计列
        assert data_rows[0][0] == 1    # ID
        assert data_rows[0][1] == '张三'  # NAME
        assert data_rows[0][3] == 'SYSTEM'  # CREATED_BY
        
    def test_convert_column_data_string(self, mock_importer):
        """测试字符串列数据转换"""
        series = pd.Series(['测试', None, '', 'normal'])
        column_info = {
            'data_type': 'VARCHAR2',
            'length': 100
        }
        
        result = mock_importer._convert_column_data(series, column_info)
        
        assert result.iloc[0] == '测试'
        assert pd.isna(result.iloc[1]) or result.iloc[1] is None
        assert pd.isna(result.iloc[2]) or result.iloc[2] is None
        assert result.iloc[3] == 'normal'
        
    def test_convert_column_data_number(self, mock_importer):
        """测试数值列数据转换"""
        series = pd.Series([1, 2.5, '3', 'invalid', None])
        column_info = {'data_type': 'NUMBER'}
        
        result = mock_importer._convert_column_data(series, column_info)
        
        assert result.iloc[0] == 1
        assert result.iloc[1] == 2.5
        assert result.iloc[2] == 3
        assert pd.isna(result.iloc[3])  # invalid 转换为 NaN
        assert pd.isna(result.iloc[4])  # None 保持为 NaN
        
    def test_dry_run_mode(self, mock_importer, temp_data_directory):
        """测试干运行模式"""
        # 模拟文件扫描
        mock_files = [{
            'name': 'test.csv',
            'path': os.path.join(temp_data_directory, 'test.csv'),
            'table_name': 'TEST_TABLE',
            'extension': '.csv'
        }]
        mock_importer.file_reader.scan_directory.return_value = mock_files
        
        # 模拟读取文件
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['test1', 'test2', 'test3']
        })
        mock_importer.file_reader.read_file.return_value = df
        
        # 模拟表结构推断
        table_info = {
            'table_name': 'TEST_TABLE',
            'columns': [
                {'name': 'ID', 'data_type': 'NUMBER'},
                {'name': 'NAME', 'data_type': 'VARCHAR2', 'length': 100}
            ]
        }
        mock_importer.table_creator.infer_table_structure.return_value = table_info
        mock_importer.table_creator.validate_table_structure.return_value = []
        
        # 执行干运行
        result = mock_importer.import_data(
            datafolder=temp_data_directory,
            dry_run=True
        )
        
        # 验证数据库连接方法没有被调用
        mock_importer.db_connection.connect.assert_not_called()
        
        # 验证结果
        assert result['total_files'] == 1
        assert result['processed_files'] == 1
        
    @patch('oracle_import_tool.core.importer.Path')
    def test_generate_sql_file(self, mock_path, mock_importer):
        """测试生成SQL文件"""
        # 模拟Path行为
        mock_output_dir = Mock()
        mock_path.return_value = mock_output_dir
        mock_output_dir.mkdir.return_value = None
        
        mock_sql_file = Mock()
        mock_output_dir.__truediv__.return_value = mock_sql_file
        
        # 模拟文件写入
        mock_file_handle = Mock()
        mock_sql_file.open = Mock()
        mock_sql_file.open.return_value.__enter__.return_value = mock_file_handle
        
        data_rows = [
            [1, '测试1', 25, 'SYSTEM', None, None, None],
            [2, '测试2', 30, 'SYSTEM', None, None, None]
        ]
        
        table_info = {
            'table_name': 'TEST_TABLE',
            'columns': [
                {'name': 'ID'},
                {'name': 'NAME'},
                {'name': 'AGE'},
                {'name': 'CREATED_BY'},
                {'name': 'CREATE_TIMESTAMP'},
                {'name': 'UPDATED_BY'},
                {'name': 'UPDATE_TIMESTAMP'}
            ]
        }
        
        mock_importer._generate_sql_file(data_rows, table_info)
        
        # 验证文件操作
        mock_output_dir.mkdir.assert_called_once_with(exist_ok=True)
        mock_file_handle.write.assert_called()
        
    def test_import_data_no_files_found(self, mock_importer, temp_data_directory):
        """测试未找到文件的情况"""
        # 模拟没有找到文件
        mock_importer.file_reader.scan_directory.return_value = []
        
        with pytest.raises(ValueError, match="未找到有效的数据文件"):
            mock_importer.import_data(datafolder=temp_data_directory)
            
    def test_import_results_statistics(self, mock_importer):
        """测试导入结果统计"""
        results = mock_importer.get_import_summary()
        
        assert 'total_files' in results
        assert 'processed_files' in results
        assert 'failed_files' in results
        assert 'total_rows' in results
        assert 'success_rows' in results
        assert 'failed_rows' in results
        assert 'errors' in results