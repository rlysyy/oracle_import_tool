"""仅测试配置管理器的独立测试"""

import pytest
import tempfile
import os
from pathlib import Path


class TestConfigManagerOnly:
    """仅配置管理器测试"""
    
    def test_config_manager_import(self):
        """测试配置管理器导入"""
        from oracle_import_tool.config.config_manager import ConfigManager
        assert ConfigManager is not None
    
    def test_config_manager_basic_operations(self):
        """测试配置管理器基本操作"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        # 创建完整的配置文件
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
            # 测试加载配置
            config_manager = ConfigManager(config_file)
            
            # 测试获取数据库配置
            db_config = config_manager.get_database_config()
            assert db_config['host'] == 'localhost'
            assert db_config['port'] == 1521
            assert db_config['username'] == 'testuser'
            
            # 测试获取导入设置
            settings = config_manager.get_import_settings()
            assert settings['batch_size'] == 1000
            assert settings['max_retries'] == 3
            assert settings['auto_commit'] is True
            
            # 测试获取数据类型配置
            types_config = config_manager.get_data_types_config()
            assert types_config['string_max_length'] == 4000
            assert types_config['number_precision'] == 38
            
            # 测试获取日志配置
            log_config = config_manager.get_logging_config()
            assert log_config['log_level'] == 'INFO'
            assert log_config['console_output'] is True
            
            # 测试配置验证
            assert config_manager.validate() is True
            
            # 测试连接字符串生成
            conn_str = config_manager.get_connection_string()
            assert 'testuser/testpass@localhost:1521/ORCLPDB1.localdomain' == conn_str
            
        finally:
            try:
                os.unlink(config_file)
            except:
                pass
    
    def test_config_default_creation(self):
        """测试默认配置创建"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        # 使用不存在的文件路径
        temp_dir = tempfile.mkdtemp()
        config_file = os.path.join(temp_dir, 'new_config.ini')
        
        try:
            # ConfigManager应该创建默认配置
            config_manager = ConfigManager(config_file)
            
            # 检查文件是否被创建
            assert os.path.exists(config_file)
            
            # 检查默认配置是否有效
            assert config_manager.validate() is True
            
            # 检查默认值
            db_config = config_manager.get_database_config()
            assert db_config['host'] == 'localhost'
            assert db_config['port'] == 1521
            
            settings = config_manager.get_import_settings()
            assert settings['batch_size'] == 1000
            assert settings['max_retries'] == 3
            
        finally:
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def test_config_validation_errors(self):
        """测试配置验证错误"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        # 创建无效的配置文件（缺少必需字段）
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False, encoding='utf-8') as f:
            f.write("""
[database]
host = localhost
# Missing required fields
""")
            config_file = f.name
        
        try:
            config_manager = ConfigManager(config_file)
            
            # 验证应该失败
            with pytest.raises(ValueError):
                config_manager.validate()
                
        finally:
            try:
                os.unlink(config_file)
            except:
                pass
        
        # 测试无效端口号
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False, encoding='utf-8') as f:
            f.write("""
[database]
host = localhost
port = 99999999
service_name = test
username = user
password = pass

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

[logging]
log_level = INFO
""")
            config_file = f.name
        
        try:
            config_manager = ConfigManager(config_file)
            
            # 验证应该失败（端口号超出范围）
            with pytest.raises(ValueError):
                config_manager.validate()
                
        finally:
            try:
                os.unlink(config_file)
            except:
                pass
    
    def test_config_string_representation(self):
        """测试配置的字符串表示"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        temp_dir = tempfile.mkdtemp()
        config_file = os.path.join(temp_dir, 'test_config.ini')
        
        try:
            config_manager = ConfigManager(config_file)
            
            # 测试字符串表示
            config_str = str(config_manager)
            assert isinstance(config_str, str)
            assert 'database' in config_str
            
        finally:
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass
    
    def test_config_setter_methods(self):
        """测试配置设置方法"""
        from oracle_import_tool.config.config_manager import ConfigManager
        
        temp_dir = tempfile.mkdtemp()
        config_file = os.path.join(temp_dir, 'test_config.ini')
        
        try:
            config_manager = ConfigManager(config_file)
            
            # 测试设置数据库配置
            config_manager.set_database_config(
                host='newhost',
                port=1522,
                username='newuser'
            )
            
            db_config = config_manager.get_database_config()
            assert db_config['host'] == 'newhost'
            assert db_config['port'] == 1522
            assert db_config['username'] == 'newuser'
            
            # 测试设置导入配置
            config_manager.set_import_settings(
                batch_size=2000,
                max_retries=5
            )
            
            settings = config_manager.get_import_settings()
            assert settings['batch_size'] == 2000
            assert settings['max_retries'] == 5
            
        finally:
            try:
                import shutil
                shutil.rmtree(temp_dir)
            except:
                pass