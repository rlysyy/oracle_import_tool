"""测试配置管理器"""

import pytest
import tempfile
import os
from pathlib import Path

from oracle_import_tool.config.config_manager import ConfigManager


class TestConfigManager:
    """配置管理器测试"""
    
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
            temp_file = f.name
        
        yield temp_file
        
        # 清理临时文件
        try:
            os.unlink(temp_file)
        except:
            pass
    
    def test_load_config(self, temp_config_file):
        """测试加载配置"""
        config_manager = ConfigManager(temp_config_file)
        
        db_config = config_manager.get_database_config()
        assert db_config['host'] == 'localhost'
        assert db_config['port'] == 1521
        assert db_config['username'] == 'testuser'
        
    def test_get_database_config(self, temp_config_file):
        """测试获取数据库配置"""
        config_manager = ConfigManager(temp_config_file)
        db_config = config_manager.get_database_config()
        
        assert 'host' in db_config
        assert 'port' in db_config
        assert 'username' in db_config
        assert 'password' in db_config
        
    def test_get_import_settings(self, temp_config_file):
        """测试获取导入设置"""
        config_manager = ConfigManager(temp_config_file)
        settings = config_manager.get_import_settings()
        
        assert settings['batch_size'] == 1000
        assert settings['max_retries'] == 3
        assert settings['timeout'] == 30
        
    def test_validate_config(self, temp_config_file):
        """测试配置验证"""
        config_manager = ConfigManager(temp_config_file)
        
        # 应该不抛出异常
        config_manager.validate()
        
    def test_invalid_config_file(self):
        """测试无效的配置文件"""
        with pytest.raises(FileNotFoundError):
            ConfigManager('nonexistent.ini')
            
    def test_create_default_config(self):
        """测试创建默认配置"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ini', delete=False) as f:
            temp_file = f.name
        
        try:
            os.unlink(temp_file)  # 删除文件，只保留路径
            
            config_manager = ConfigManager(temp_file)
            config_manager.save_config()
            
            # 验证文件是否创建
            assert os.path.exists(temp_file)
            
            # 验证配置是否有效
            config_manager.validate()
            
        finally:
            try:
                os.unlink(temp_file)
            except:
                pass