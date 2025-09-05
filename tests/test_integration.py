"""集成测试"""

import pytest
import tempfile
import os
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from oracle_import_tool.core.importer import OracleImporter
from oracle_import_tool.main import cli
from click.testing import CliRunner


class TestIntegration:
    """集成测试"""
    
    @pytest.fixture
    def integration_setup(self, temp_directory, temp_config_file):
        """集成测试环境设置"""
        # 创建测试数据文件
        data_dir = os.path.join(temp_directory, 'data')
        os.makedirs(data_dir, exist_ok=True)
        
        # CSV文件
        csv_file = os.path.join(data_dir, 'users.csv')
        users_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['张三', '李四', '王五'],
            'email': ['zhang@test.com', 'li@test.com', 'wang@test.com'],
            'age': [25, 30, 28]
        })
        users_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        # Excel文件
        excel_file = os.path.join(data_dir, 'products.xlsx')
        products_df = pd.DataFrame({
            'code': ['P001', 'P002', 'P003'],
            'name': ['产品1', '产品2', '产品3'],
            'price': [100.50, 200.75, 150.25],
            'category': ['类别A', '类别B', '类别A']
        })
        products_df.to_excel(excel_file, index=False)
        
        # DDL目录
        ddl_dir = os.path.join(temp_directory, 'ddl')
        os.makedirs(ddl_dir, exist_ok=True)
        
        # 用户表DDL
        users_ddl = os.path.join(ddl_dir, 'users.sql')
        with open(users_ddl, 'w', encoding='utf-8') as f:
            f.write("""
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
""")
        
        return {
            'temp_dir': temp_directory,
            'config_file': temp_config_file,
            'data_dir': data_dir,
            'ddl_dir': ddl_dir,
            'csv_file': csv_file,
            'excel_file': excel_file
        }
    
    def test_cli_version(self):
        """测试CLI版本命令"""
        runner = CliRunner()
        result = runner.invoke(cli, ['--version'])
        
        assert result.exit_code == 0
        assert 'Oracle Import Tool' in result.output
        
    def test_cli_help(self):
        """测试CLI帮助命令"""
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert 'Oracle数据库导入工具' in result.output
        
    def test_cli_config_init(self, temp_directory):
        """测试配置初始化命令"""
        runner = CliRunner()
        config_file = os.path.join(temp_directory, 'new_config.ini')
        
        result = runner.invoke(cli, ['config', 'init', '--output', config_file])
        
        assert result.exit_code == 0
        assert os.path.exists(config_file)
        
    def test_cli_config_validate(self, temp_config_file):
        """测试配置验证命令"""
        runner = CliRunner()
        result = runner.invoke(cli, ['config', 'validate', temp_config_file])
        
        assert result.exit_code == 0
        assert '验证通过' in result.output
        
    def test_cli_scan_files(self, integration_setup):
        """测试文件扫描命令"""
        setup = integration_setup
        runner = CliRunner()
        
        result = runner.invoke(cli, ['scan', setup['data_dir']])
        
        assert result.exit_code == 0
        assert 'users.csv' in result.output or 'USERS' in result.output
        assert 'products.xlsx' in result.output or 'PRODUCTS' in result.output
        
    def test_cli_preview_file(self, integration_setup):
        """测试文件预览命令"""
        setup = integration_setup
        runner = CliRunner()
        
        result = runner.invoke(cli, ['preview', setup['csv_file']])
        
        assert result.exit_code == 0
        assert '张三' in result.output
        
    @patch('oracle_import_tool.database.connection.DatabaseConnection')
    def test_cli_test_database(self, mock_db_class, integration_setup):
        """测试数据库连接测试命令"""
        setup = integration_setup
        
        # 模拟数据库连接成功
        mock_db_instance = Mock()
        mock_db_instance.test_connection.return_value = True
        mock_db_class.return_value = mock_db_instance
        
        runner = CliRunner()
        result = runner.invoke(cli, ['test-db', '--config', setup['config_file']])
        
        assert result.exit_code == 0
        
    @patch('oracle_import_tool.core.importer.OracleImporter')
    def test_cli_import_dry_run(self, mock_importer_class, integration_setup):
        """测试导入命令（干运行模式）"""
        setup = integration_setup
        
        # 模拟导入器
        mock_importer = Mock()
        mock_importer.import_data.return_value = {
            'total_files': 2,
            'processed_files': 2,
            'failed_files': 0,
            'total_rows': 6,
            'success_rows': 6,
            'failed_rows': 0,
            'errors': []
        }
        mock_importer_class.return_value = mock_importer
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', setup['data_dir'],
            '--config', setup['config_file'],
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        mock_importer.import_data.assert_called_once()
        
    @patch('oracle_import_tool.core.importer.OracleImporter')
    def test_cli_import_with_specific_tables(self, mock_importer_class, integration_setup):
        """测试导入指定表"""
        setup = integration_setup
        
        mock_importer = Mock()
        mock_importer.import_data.return_value = {
            'total_files': 1,
            'processed_files': 1,
            'failed_files': 0,
            'total_rows': 3,
            'success_rows': 3,
            'failed_rows': 0,
            'errors': []
        }
        mock_importer_class.return_value = mock_importer
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', setup['data_dir'],
            '--table', 'users',
            '--config', setup['config_file'],
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        # 验证调用参数
        call_args = mock_importer.import_data.call_args
        assert 'users' in call_args[1]['tables']
        
    @patch('oracle_import_tool.core.importer.OracleImporter')
    def test_cli_import_with_ddl_folder(self, mock_importer_class, integration_setup):
        """测试使用DDL文件夹导入"""
        setup = integration_setup
        
        mock_importer = Mock()
        mock_importer.import_data.return_value = {
            'total_files': 1,
            'processed_files': 1,
            'failed_files': 0,
            'total_rows': 3,
            'success_rows': 3,
            'failed_rows': 0,
            'errors': []
        }
        mock_importer_class.return_value = mock_importer
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', setup['data_dir'],
            '--ddl-folder', setup['ddl_dir'],
            '--config', setup['config_file'],
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        call_args = mock_importer.import_data.call_args
        assert call_args[1]['ddl_folder'] == setup['ddl_dir']
        
    @patch('oracle_import_tool.core.importer.OracleImporter')
    def test_cli_import_create_sql(self, mock_importer_class, integration_setup):
        """测试生成SQL文件"""
        setup = integration_setup
        
        mock_importer = Mock()
        mock_importer.import_data.return_value = {
            'total_files': 2,
            'processed_files': 2,
            'failed_files': 0,
            'total_rows': 6,
            'success_rows': 6,
            'failed_rows': 0,
            'errors': []
        }
        mock_importer_class.return_value = mock_importer
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', setup['data_dir'],
            '--create-sql',
            '--config', setup['config_file'],
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        call_args = mock_importer.import_data.call_args
        assert call_args[1]['create_sql'] is True
        
    def test_end_to_end_workflow(self, integration_setup):
        """测试端到端工作流程"""
        setup = integration_setup
        runner = CliRunner()
        
        # 1. 扫描文件
        scan_result = runner.invoke(cli, ['scan', setup['data_dir']])
        assert scan_result.exit_code == 0
        
        # 2. 预览文件
        preview_result = runner.invoke(cli, ['preview', setup['csv_file']])
        assert preview_result.exit_code == 0
        
        # 3. 验证配置
        validate_result = runner.invoke(cli, ['config', 'validate', setup['config_file']])
        assert validate_result.exit_code == 0
        
        # 4. 干运行导入（模拟）
        with patch('oracle_import_tool.core.importer.OracleImporter') as mock_importer_class:
            mock_importer = Mock()
            mock_importer.import_data.return_value = {
                'total_files': 2,
                'processed_files': 2,
                'failed_files': 0,
                'total_rows': 6,
                'success_rows': 6,
                'failed_rows': 0,
                'errors': []
            }
            mock_importer_class.return_value = mock_importer
            
            import_result = runner.invoke(cli, [
                'import',
                '--datafolder', setup['data_dir'],
                '--config', setup['config_file'],
                '--dry-run'
            ])
            
            assert import_result.exit_code == 0
    
    def test_error_handling_missing_data_folder(self, temp_config_file):
        """测试错误处理：缺失数据文件夹"""
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', '/nonexistent/path',
            '--config', temp_config_file
        ])
        
        assert result.exit_code != 0
        
    def test_error_handling_invalid_config(self, integration_setup):
        """测试错误处理：无效配置文件"""
        setup = integration_setup
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', setup['data_dir'],
            '--config', '/nonexistent/config.ini'
        ])
        
        assert result.exit_code != 0
        
    @patch('oracle_import_tool.core.importer.OracleImporter')
    def test_import_failure_handling(self, mock_importer_class, integration_setup):
        """测试导入失败处理"""
        setup = integration_setup
        
        # 模拟导入失败
        mock_importer = Mock()
        mock_importer.import_data.side_effect = Exception("数据库连接失败")
        mock_importer_class.return_value = mock_importer
        
        runner = CliRunner()
        result = runner.invoke(cli, [
            'import',
            '--datafolder', setup['data_dir'],
            '--config', setup['config_file']
        ])
        
        assert result.exit_code != 0
        assert '导入失败' in result.output or 'Error' in result.output or result.output == ""