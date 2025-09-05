"""基础功能测试"""

import pytest
import tempfile
import os
from pathlib import Path


class TestBasicFunctionality:
    """基础功能测试"""
    
    def test_basic_imports(self):
        """测试基本导入"""
        # 测试可以导入主要模块
        try:
            from oracle_import_tool.config.config_manager import ConfigManager
            from oracle_import_tool.main import cli
            assert True
        except ImportError as e:
            pytest.fail(f"Failed to import modules: {e}")
    
    def test_config_manager_basic(self):
        """测试配置管理器基本功能"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        # 创建临时配置文件
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
""")
            config_file = f.name
        
        try:
            config_manager = ConfigManager(config_file)
            db_config = config_manager.get_database_config()
            
            assert db_config['host'] == 'localhost'
            assert db_config['port'] == 1521
            assert db_config['username'] == 'testuser'
            
        finally:
            try:
                os.unlink(config_file)
            except:
                pass
    
    def test_file_operations(self):
        """测试文件操作"""
        # 创建临时目录和文件
        temp_dir = tempfile.mkdtemp()
        
        try:
            # 创建测试CSV文件
            csv_file = os.path.join(temp_dir, 'test.csv')
            with open(csv_file, 'w', encoding='utf-8') as f:
                f.write('id,name,age\n1,张三,25\n2,李四,30\n')
            
            # 测试文件存在
            assert os.path.exists(csv_file)
            
            # 读取文件内容
            with open(csv_file, 'r', encoding='utf-8') as f:
                content = f.read()
                assert '张三' in content
                assert 'id,name,age' in content
                
        finally:
            # 清理
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def test_cli_help_command(self):
        """测试CLI帮助命令"""
        from click.testing import CliRunner
        from oracle_import_tool.main import cli
        
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        
        assert result.exit_code == 0
        assert 'Oracle数据库导入工具' in result.output or 'Oracle' in result.output
    
    def test_version_display(self):
        """测试版本显示"""
        from click.testing import CliRunner
        from oracle_import_tool.main import cli
        
        runner = CliRunner()
        result = runner.invoke(cli, ['--version'])
        
        # 版本命令应该成功执行或显示版本信息
        assert result.exit_code == 0 or 'Oracle Import Tool' in result.output
    
    def test_config_validation_basic(self):
        """测试配置验证基本功能"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        # 创建有效的配置文件（包含所有必需的section）
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

[data_types]
string_max_length = 4000
number_precision = 38
number_scale = 2
timestamp_format = YYYY-MM-DD HH24:MI:SS.FF6

[logging]
log_level = INFO
log_format = %(asctime)s - %(name)s - %(levelname)s - %(message)s
console_output = true
""")
            config_file = f.name
        
        try:
            config_manager = ConfigManager(config_file)
            
            # 验证应该成功（不抛出异常）
            config_manager.validate()
            
            # 获取设置应该成功
            settings = config_manager.get_import_settings()
            assert settings['batch_size'] == 1000
            assert settings['max_retries'] == 3
            
        except Exception as e:
            pytest.fail(f"Configuration validation failed: {e}")
        finally:
            try:
                os.unlink(config_file)
            except:
                pass
    
    def test_directory_creation(self):
        """测试目录创建功能"""
        temp_dir = tempfile.mkdtemp()
        
        try:
            # 测试子目录创建
            sub_dir = os.path.join(temp_dir, 'logs')
            os.makedirs(sub_dir, exist_ok=True)
            
            assert os.path.exists(sub_dir)
            assert os.path.isdir(sub_dir)
            
            # 测试输出目录创建
            output_dir = os.path.join(temp_dir, 'output')
            os.makedirs(output_dir, exist_ok=True)
            
            assert os.path.exists(output_dir)
            assert os.path.isdir(output_dir)
            
        finally:
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def test_pathlib_operations(self):
        """测试Path对象操作"""
        temp_dir = Path(tempfile.mkdtemp())
        
        try:
            # 创建测试文件
            test_file = temp_dir / 'test.txt'
            test_file.write_text('测试内容', encoding='utf-8')
            
            assert test_file.exists()
            assert test_file.is_file()
            
            # 读取内容
            content = test_file.read_text(encoding='utf-8')
            assert content == '测试内容'
            
            # 测试文件扩展名
            assert test_file.suffix == '.txt'
            assert test_file.name == 'test.txt'
            
        finally:
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def test_string_operations(self):
        """测试字符串操作功能"""
        # 测试表名规范化逻辑
        def normalize_table_name(filename):
            """从文件名提取表名"""
            name = Path(filename).stem  # 去除扩展名
            name = name.upper()         # 转大写
            name = name.replace('-', '_').replace(' ', '_')  # 替换特殊字符
            return name
        
        # 测试各种文件名
        assert normalize_table_name('user_data.csv') == 'USER_DATA'
        assert normalize_table_name('product-info.xlsx') == 'PRODUCT_INFO'
        assert normalize_table_name('test table.csv') == 'TEST_TABLE'
        assert normalize_table_name('测试文件.csv') == '测试文件'
    
    def test_error_handling(self):
        """测试错误处理"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        # ConfigManager不存在文件时会创建默认配置，所以测试其他错误情况
        
        # 测试无效路径访问文件
        with pytest.raises((FileNotFoundError, OSError)):
            with open('/invalid/path/file.txt', 'r') as f:
                pass
        
        # 测试配置验证错误
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            f.write("""
[database]
host = localhost
port = invalid_port
""")
            config_file = f.name
        
        try:
            config_manager = ConfigManager(config_file)
            # 这应该抛出验证错误
            with pytest.raises(ValueError):
                config_manager.validate()
        finally:
            try:
                os.unlink(config_file)
            except:
                pass