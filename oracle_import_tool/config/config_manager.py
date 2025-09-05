"""配置管理模块"""

import configparser
from pathlib import Path
from typing import Dict, Any, Optional
import os


class ConfigManager:
    """配置文件管理器"""
    
    def __init__(self, config_file: str = "config.ini"):
        self.config_file = Path(config_file)
        self.config = configparser.ConfigParser(interpolation=None)  # 禁用插值
        self._load_config()
    
    def _load_config(self) -> None:
        """加载配置文件"""
        if self.config_file.exists():
            self.config.read(self.config_file, encoding='utf-8')
        else:
            self._create_default_config()
    
    def _create_default_config(self) -> None:
        """创建默认配置文件"""
        # 数据库连接配置
        self.config['database'] = {
            'host': 'localhost',                    # Oracle数据库服务器地址
            'port': '1521',                         # Oracle数据库端口号，默认1521
            'service_name': 'ORCLPDB1.localdomain', # Oracle服务名或SID
            'username': 'p2dbown',                  # 数据库用户名
            'password': 'p2dbown',                  # 数据库密码
            'schema': 'P2DBOWN'                     # 目标数据库模式名
        }
        
        # 数据导入设置
        self.config['import_settings'] = {
            'batch_size': '1000',               # 批量插入的记录数，建议1000-5000
            'max_retries': '3',                 # 失败重试次数
            'timeout': '30',                    # 数据库连接超时时间（秒）
            'auto_commit': 'true',              # 是否自动提交事务
            'create_table_if_not_exists': 'false'  # 表不存在时是否自动创建（v0.2.0默认为false）
        }
        
        # 数据类型映射配置
        self.config['data_types'] = {
            'string_max_length': '4000',        # VARCHAR2字段最大长度
            'number_precision': '38',           # NUMBER字段精度（总位数）
            'number_scale': '2',                # NUMBER字段小数位数
            'timestamp_format': 'YYYY-MM-DD HH24:MI:SS.FF6'  # 时间戳格式
        }
        
        # 日志配置
        self.config['logging'] = {
            'log_level': 'INFO',                # 日志级别：DEBUG, INFO, WARNING, ERROR, CRITICAL
            'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # 日志格式
            'console_output': 'true'            # 是否在控制台输出日志
        }
        
        # 表头检测配置（v0.2.0新增）
        self.config['header_detection'] = {
            'header_keywords': '',              # 表头关键词配置，支持AND/OR逻辑
                                               # 格式：ID,NAME (AND关系) 或 ID|NAME (OR关系)
                                               # 示例：CREATED_BY,CREATE_TIMESTAMP 或 CREATE_TIMESTAMP|CREATED_BY
            'header_detection_mode': 'auto'    # 检测模式：
                                               # auto - 自动检测（优先使用关键词，否则使用默认逻辑）
                                               # force_header - 强制认为第一行是表头
                                               # force_no_header - 强制认为第一行是数据
        }
        
        self.save_config()
    
    def save_config(self) -> None:
        """保存配置文件"""
        # 确保目录存在
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入配置文件，包含详细注释
        with open(self.config_file, 'w', encoding='utf-8') as f:
            # 写入文件头注释
            f.write("# Oracle数据导入工具配置文件\n")
            f.write("# 版本：v0.2.0\n")
            f.write("# 最后更新：2024年\n")
            f.write("\n")
            
            # 数据库配置节
            f.write("# ===========================================\n")
            f.write("# 数据库连接配置\n")
            f.write("# ===========================================\n")
            f.write("[database]\n")
            f.write("# Oracle数据库服务器地址\n")
            f.write("# 可以是IP地址或主机名\n")
            f.write("# 示例：192.168.1.100, localhost, db.company.com\n")
            f.write(f"host = {self.config['database']['host']}\n")
            f.write("\n")
            f.write("# Oracle数据库端口号\n")
            f.write("# 默认值：1521\n")
            f.write("# 范围：1-65535\n")
            f.write(f"port = {self.config['database']['port']}\n")
            f.write("\n")
            f.write("# Oracle服务名（Service Name）或SID\n")
            f.write("# 服务名示例：ORCLPDB1.localdomain, ORCL\n")
            f.write("# SID示例：ORCL, XE\n")
            f.write(f"service_name = {self.config['database']['service_name']}\n")
            f.write("\n")
            f.write("# 数据库用户名\n")
            f.write("# 需要有目标schema的INSERT权限\n")
            f.write(f"username = {self.config['database']['username']}\n")
            f.write("\n")
            f.write("# 数据库密码\n")
            f.write("# 建议使用强密码\n")
            f.write(f"password = {self.config['database']['password']}\n")
            f.write("\n")
            f.write("# 目标数据库模式名（Schema）\n")
            f.write("# 数据将导入到此模式下的表中\n")
            f.write(f"schema = {self.config['database']['schema']}\n")
            f.write("\n")
            
            # 导入设置节
            f.write("# ===========================================\n")
            f.write("# 数据导入设置\n")
            f.write("# ===========================================\n")
            f.write("[import_settings]\n")
            f.write("# 批量插入的记录数\n")
            f.write("# 建议范围：500-5000，过大可能导致内存问题，过小影响性能\n")
            f.write("# 默认值：1000\n")
            f.write(f"batch_size = {self.config['import_settings']['batch_size']}\n")
            f.write("\n")
            f.write("# 操作失败时的最大重试次数\n")
            f.write("# 范围：0-10\n")
            f.write("# 默认值：3\n")
            f.write(f"max_retries = {self.config['import_settings']['max_retries']}\n")
            f.write("\n")
            f.write("# 数据库连接超时时间（秒）\n")
            f.write("# 范围：10-300\n")
            f.write("# 默认值：30\n")
            f.write(f"timeout = {self.config['import_settings']['timeout']}\n")
            f.write("\n")
            f.write("# 是否自动提交事务\n")
            f.write("# 可选值：true, false\n")
            f.write("# true - 每个批次后自动提交\n")
            f.write("# false - 需要手动提交，出错时可以回滚\n")
            f.write("# 默认值：true\n")
            f.write(f"auto_commit = {self.config['import_settings']['auto_commit']}\n")
            f.write("\n")
            f.write("# 表不存在时是否自动创建\n")
            f.write("# 可选值：true, false\n")
            f.write("# true - 根据数据结构自动创建表\n")
            f.write("# false - 表不存在时报错（v0.2.0默认值，更安全）\n")
            f.write("# 建议：生产环境设置为false，开发环境可设置为true\n")
            f.write(f"create_table_if_not_exists = {self.config['import_settings']['create_table_if_not_exists']}\n")
            f.write("\n")
            
            # 数据类型配置节
            f.write("# ===========================================\n")
            f.write("# 数据类型映射配置\n")
            f.write("# ===========================================\n")
            f.write("[data_types]\n")
            f.write("# VARCHAR2字段的最大长度\n")
            f.write("# 范围：1-4000（Oracle标准限制）\n")
            f.write("# 默认值：4000\n")
            f.write("# 注意：超过此长度的字符串数据将被截断\n")
            f.write(f"string_max_length = {self.config['data_types']['string_max_length']}\n")
            f.write("\n")
            f.write("# NUMBER字段的总精度（总位数）\n")
            f.write("# 范围：1-38（Oracle标准限制）\n")
            f.write("# 默认值：38（最大精度）\n")
            f.write(f"number_precision = {self.config['data_types']['number_precision']}\n")
            f.write("\n")
            f.write("# NUMBER字段的小数位数\n")
            f.write("# 范围：0到number_precision的值\n")
            f.write("# 默认值：2\n")
            f.write("# 示例：precision=10, scale=2 表示最大8位整数+2位小数\n")
            f.write(f"number_scale = {self.config['data_types']['number_scale']}\n")
            f.write("\n")
            f.write("# 时间戳字段的格式\n")
            f.write("# Oracle日期时间格式掩码\n")
            f.write("# 常用格式：\n")
            f.write("#   YYYY-MM-DD HH24:MI:SS.FF6 （年-月-日 时:分:秒.微秒）\n")
            f.write("#   YYYY-MM-DD HH24:MI:SS （年-月-日 时:分:秒）\n")
            f.write("#   DD/MM/YYYY HH12:MI:SS AM （日/月/年 12小时制）\n")
            f.write(f"timestamp_format = {self.config['data_types']['timestamp_format']}\n")
            f.write("\n")
            
            # 日志配置节
            f.write("# ===========================================\n")
            f.write("# 日志配置\n")
            f.write("# ===========================================\n")
            f.write("[logging]\n")
            f.write("# 日志记录级别\n")
            f.write("# 可选值（从低到高）：DEBUG, INFO, WARNING, ERROR, CRITICAL\n")
            f.write("# DEBUG - 详细调试信息（包含所有SQL语句）\n")
            f.write("# INFO - 一般信息（推荐，默认值）\n")
            f.write("# WARNING - 警告信息\n")
            f.write("# ERROR - 错误信息\n")
            f.write("# CRITICAL - 严重错误\n")
            f.write(f"log_level = {self.config['logging']['log_level']}\n")
            f.write("\n")
            f.write("# 日志消息格式\n")
            f.write("# 支持的格式化字段：\n")
            f.write("#   %(asctime)s - 时间戳\n")
            f.write("#   %(name)s - 记录器名称\n")
            f.write("#   %(levelname)s - 日志级别\n")
            f.write("#   %(message)s - 日志消息\n")
            f.write("#   %(filename)s - 文件名\n")
            f.write("#   %(lineno)d - 行号\n")
            f.write(f"log_format = {self.config['logging']['log_format']}\n")
            f.write("\n")
            f.write("# 是否在控制台输出日志\n")
            f.write("# 可选值：true, false\n")
            f.write("# true - 同时输出到控制台和日志文件\n")
            f.write("# false - 仅输出到日志文件\n")
            f.write("# 默认值：true\n")
            f.write(f"console_output = {self.config['logging']['console_output']}\n")
            f.write("\n")
            
            # 表头检测配置节
            f.write("# ===========================================\n")
            f.write("# 表头检测配置（v0.2.0新增功能）\n")
            f.write("# 用于智能识别数据文件是否包含表头\n")
            f.write("# ===========================================\n")
            f.write("[header_detection]\n")
            f.write("# 表头关键词配置，支持复杂的逻辑表达式\n")
            f.write("# 语法规则：\n")
            f.write("#   逗号(,) 表示 AND关系：所有关键词都必须存在\n")
            f.write("#   竖线(|) 表示 OR关系：任一关键词存在即可\n")
            f.write("#   可以混合使用，支持复杂逻辑\n")
            f.write("# \n")
            f.write("# 配置示例：\n")
            f.write("#   CREATED_BY,CREATE_TIMESTAMP  （AND关系：两个都必须存在）\n")
            f.write("#   CREATE_TIMESTAMP|CREATED_BY  （OR关系：任一个存在即可）\n")
            f.write("#   id,name|code,type           （混合：(id AND name) OR (code AND type)）\n")
            f.write("#   ID|编号|序号                  （多个OR：支持中英文混合）\n")
            f.write("# \n")
            f.write("# 空值表示不使用关键词匹配，使用默认检测逻辑\n")
            f.write("# 关键词匹配不区分大小写\n")
            f.write(f"header_keywords = {self.config['header_detection']['header_keywords']}\n")
            f.write("\n")
            f.write("# 表头检测模式\n")
            f.write("# 可选值：auto, force_header, force_no_header\n")
            f.write("# \n")
            f.write("# auto（默认值）- 自动检测模式：\n")
            f.write("#   如果配置了header_keywords，优先使用关键词匹配\n")
            f.write("#   否则使用内置的智能检测算法\n")
            f.write("# \n")
            f.write("# force_header - 强制表头模式：\n")
            f.write("#   始终认为第一行是表头，忽略所有检测逻辑\n")
            f.write("#   适用于确定有表头但检测不准确的情况\n")
            f.write("# \n")
            f.write("# force_no_header - 强制数据模式：\n")
            f.write("#   始终认为第一行是数据，需要配合DDL文件使用\n")
            f.write("#   适用于确定无表头的数据文件\n")
            f.write(f"header_detection_mode = {self.config['header_detection']['header_detection_mode']}\n")
            f.write("\n")
            
            # 文件末尾说明
            f.write("# ===========================================\n")
            f.write("# 配置文件说明\n")
            f.write("# ===========================================\n")
            f.write("# 1. 修改配置后需要重启工具才能生效\n")
            f.write("# 2. 密码等敏感信息请妥善保管此配置文件\n")
            f.write("# 3. 如果配置文件损坏，删除后工具会自动重新生成\n")
            f.write("# 4. 更多帮助信息请参考 USER_GUIDE.md\n")
            f.write("# 5. 技术支持：请查看项目README.md\n")
            f.write("\n")
    
    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        db_config = dict(self.config['database'])
        
        # 转换端口为整数
        db_config['port'] = int(db_config.get('port', 1521))
        
        # 构建连接字符串
        db_config['dsn'] = f"{db_config['host']}:{db_config['port']}/{db_config['service_name']}"
        
        return db_config
    
    def get_import_settings(self) -> Dict[str, Any]:
        """获取导入设置"""
        settings = dict(self.config['import_settings'])
        
        # 类型转换
        settings['batch_size'] = int(settings.get('batch_size', 1000))
        settings['max_retries'] = int(settings.get('max_retries', 3))
        settings['timeout'] = int(settings.get('timeout', 30))
        settings['auto_commit'] = settings.get('auto_commit', 'true').lower() == 'true'
        settings['create_table_if_not_exists'] = settings.get('create_table_if_not_exists', 'true').lower() == 'true'
        
        return settings
    
    def get_data_types_config(self) -> Dict[str, Any]:
        """获取数据类型配置"""
        types_config = dict(self.config['data_types'])
        
        # 类型转换
        types_config['string_max_length'] = int(types_config.get('string_max_length', 4000))
        types_config['number_precision'] = int(types_config.get('number_precision', 38))
        types_config['number_scale'] = int(types_config.get('number_scale', 2))
        
        return types_config
    
    def get_logging_config(self) -> Dict[str, Any]:
        """获取日志配置"""
        logging_config = dict(self.config['logging'])
        
        # 类型转换
        logging_config['console_output'] = logging_config.get('console_output', 'true').lower() == 'true'
        
        return logging_config
    
    def get_header_detection_config(self) -> Dict[str, Any]:
        """获取表头检测配置"""
        if 'header_detection' not in self.config:
            # 如果配置中没有表头检测配置，返回默认值
            return {
                'header_keywords': '',
                'header_detection_mode': 'auto'
            }
        
        header_config = dict(self.config['header_detection'])
        return header_config
    
    def validate(self) -> bool:
        """验证配置文件"""
        required_sections = ['database', 'import_settings', 'data_types', 'logging']
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required section: {section}")
        
        # 验证数据库配置
        db_config = self.config['database']
        required_db_keys = ['host', 'port', 'service_name', 'username', 'password']
        
        for key in required_db_keys:
            if key not in db_config:
                raise ValueError(f"Missing required database config: {key}")
        
        # 验证端口号
        try:
            port = int(db_config['port'])
            if port < 1 or port > 65535:
                raise ValueError("Port must be between 1 and 65535")
        except ValueError as e:
            raise ValueError(f"Invalid port number: {e}")
        
        # 验证批量大小
        try:
            batch_size = int(self.config['import_settings']['batch_size'])
            if batch_size < 1 or batch_size > 10000:
                raise ValueError("Batch size must be between 1 and 10000")
        except (ValueError, KeyError) as e:
            raise ValueError(f"Invalid batch size: {e}")
        
        return True
    
    def set_database_config(self, **kwargs) -> None:
        """设置数据库配置"""
        if 'database' not in self.config:
            self.config.add_section('database')
        
        for key, value in kwargs.items():
            self.config.set('database', key, str(value))
    
    def set_import_settings(self, **kwargs) -> None:
        """设置导入配置"""
        if 'import_settings' not in self.config:
            self.config.add_section('import_settings')
        
        for key, value in kwargs.items():
            self.config.set('import_settings', key, str(value))
    
    def get_connection_string(self) -> str:
        """获取Oracle连接字符串"""
        db_config = self.get_database_config()
        return f"{db_config['username']}/{db_config['password']}@{db_config['dsn']}"
    
    def __str__(self) -> str:
        """字符串表示"""
        config_dict = {}
        for section in self.config.sections():
            config_dict[section] = dict(self.config[section])
        return str(config_dict)