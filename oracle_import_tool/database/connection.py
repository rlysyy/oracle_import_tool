"""数据库连接模块"""

import cx_Oracle
from typing import Optional, Dict, Any, List, Tuple
import logging
from contextlib import contextmanager
from datetime import datetime
import pandas as pd
import numpy as np
from ..config.config_manager import ConfigManager


class DatabaseConnection:
    """Oracle数据库连接管理器"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.connection: Optional[cx_Oracle.Connection] = None
        self.cursor: Optional[cx_Oracle.Cursor] = None
        self.logger = logging.getLogger(__name__)
        
        # 获取数据库配置
        self.db_config = config_manager.get_database_config()
        self.import_settings = config_manager.get_import_settings()
    
    def connect(self) -> cx_Oracle.Connection:
        """建立数据库连接"""
        try:
            # 构建连接字符串
            dsn = cx_Oracle.makedsn(
                host=self.db_config['host'],
                port=self.db_config['port'],
                service_name=self.db_config['service_name']
            )
            
            self.connection = cx_Oracle.connect(
                user=self.db_config['username'],
                password=self.db_config['password'],
                dsn=dsn,
                encoding='UTF-8'
            )
            
            self.cursor = self.connection.cursor()
            self.logger.info(f"成功连接到Oracle数据库: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['service_name']}")
            
            return self.connection
            
        except cx_Oracle.Error as e:
            error_msg = f"数据库连接失败: {str(e)}"
            self.logger.error(error_msg)
            raise ConnectionError(error_msg) from e
    
    def disconnect(self) -> None:
        """断开数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            
            if self.connection:
                self.connection.close()
                self.connection = None
                
            self.logger.info("数据库连接已断开")
            
        except cx_Oracle.Error as e:
            self.logger.error(f"断开数据库连接时出错: {str(e)}")
    
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            if not self.connection:
                self.connect()
            
            # 测试查询
            result = self.execute_query("SELECT 1 FROM DUAL")
            if result and len(result) == 1:
                self.logger.info("数据库连接测试成功")
                return True
            else:
                raise Exception("测试查询返回异常结果")
                
        except Exception as e:
            self.logger.error(f"数据库连接测试失败: {str(e)}")
            return False
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> List[Tuple]:
        """执行查询语句"""
        if not self.connection or not self.cursor:
            raise ConnectionError("数据库未连接")
        
        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            return self.cursor.fetchall()
            
        except cx_Oracle.Error as e:
            self.logger.error(f"执行查询失败: {sql}, 错误: {str(e)}")
            raise
    
    def execute_ddl(self, sql: str) -> None:
        """执行DDL语句（CREATE, DROP, ALTER等）"""
        if not self.connection or not self.cursor:
            raise ConnectionError("数据库未连接")
        
        try:
            self.cursor.execute(sql)
            if self.import_settings.get('auto_commit', True):
                self.connection.commit()
            
            self.logger.info(f"DDL语句执行成功: {sql[:50]}...")
            
        except cx_Oracle.Error as e:
            self.logger.error(f"执行DDL失败: {sql[:50]}..., 错误: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def execute_dml(self, sql: str, params: Optional[List] = None) -> int:
        """执行DML语句（INSERT, UPDATE, DELETE）"""
        if not self.connection or not self.cursor:
            raise ConnectionError("数据库未连接")
        
        try:
            # Preprocess parameters to ensure datetime values are properly formatted
            if params:
                processed_params = self._preprocess_batch_data([params])[0]
                self.cursor.execute(sql, processed_params)
            else:
                self.cursor.execute(sql)
            
            affected_rows = self.cursor.rowcount
            
            if self.import_settings.get('auto_commit', True):
                self.connection.commit()
            
            return affected_rows
            
        except cx_Oracle.Error as e:
            self.logger.error(f"执行DML失败: {sql[:50]}..., 错误: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def batch_insert(self, sql: str, data_batch: List[List]) -> int:
        """批量插入数据"""
        if not self.connection or not self.cursor:
            raise ConnectionError("数据库未连接")
        
        if not data_batch:
            return 0
        
        try:
            # Preprocess data to ensure datetime values are properly formatted
            processed_batch = self._preprocess_batch_data(data_batch)
            
            self.cursor.executemany(sql, processed_batch)
            affected_rows = self.cursor.rowcount
            
            if self.import_settings.get('auto_commit', True):
                self.connection.commit()
            
            self.logger.debug(f"批量插入成功，插入 {affected_rows} 行数据")
            return affected_rows
            
        except cx_Oracle.Error as e:
            self.logger.error(f"批量插入失败: {sql[:50]}..., 错误: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def _preprocess_batch_data(self, data_batch: List[List]) -> List[List]:
        """
        预处理批量数据，确保datetime值被正确格式化并且numpy类型被转换为Python原生类型
        
        Args:
            data_batch: 包含待插入数据的列表
            
        Returns:
            预处理后的数据列表
        """
        processed_batch = []
        
        for row in data_batch:
            processed_row = []
            for value in row:
                # Handle datetime values
                if isinstance(value, (datetime, pd.Timestamp)):
                    # Convert to Python datetime if it's a pandas Timestamp
                    if isinstance(value, pd.Timestamp):
                        dt_value = value.to_pydatetime()
                    else:
                        dt_value = value
                    
                    # For Oracle TIMESTAMP, we can send the datetime object directly
                    # cx_Oracle will handle the conversion properly
                    processed_row.append(dt_value)
                # Handle numpy types
                elif isinstance(value, np.integer):
                    # Convert numpy integers to Python int
                    processed_row.append(int(value))
                elif isinstance(value, np.floating):
                    # Convert numpy floats to Python float or None for NaN
                    if np.isnan(value):
                        processed_row.append(None)
                    else:
                        processed_row.append(float(value))
                elif isinstance(value, np.bool_):
                    # Convert numpy booleans to Python bool
                    processed_row.append(bool(value))
                elif pd.isna(value):
                    # Handle pandas NA values
                    processed_row.append(None)
                else:
                    processed_row.append(value)
            
            processed_batch.append(processed_row)
        
        return processed_batch
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        sql = """
        SELECT COUNT(*) FROM ALL_TABLES 
        WHERE TABLE_NAME = UPPER(:table_name)
        """
        
        try:
            result = self.execute_query(sql, [table_name])
            return result[0][0] > 0 if result else False
            
        except Exception as e:
            self.logger.error(f"检查表是否存在失败: {table_name}, 错误: {str(e)}")
            return False
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表的列信息"""
        sql = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            DATA_LENGTH,
            DATA_PRECISION,
            DATA_SCALE,
            NULLABLE
        FROM ALL_TAB_COLUMNS 
        WHERE TABLE_NAME = UPPER(:table_name)
        ORDER BY COLUMN_ID
        """
        
        try:
            rows = self.execute_query(sql, [table_name])
            columns = []
            
            for row in rows:
                column_info = {
                    'name': row[0],
                    'data_type': row[1],
                    'length': row[2],
                    'precision': row[3],
                    'scale': row[4],
                    'nullable': row[5] == 'Y'
                }
                columns.append(column_info)
            
            return columns
            
        except Exception as e:
            self.logger.error(f"获取表列信息失败: {table_name}, 错误: {str(e)}")
            return []
    
    def get_table_count(self, table_name: str) -> int:
        """获取表的记录数"""
        sql = f"SELECT COUNT(*) FROM {table_name}"
        
        try:
            result = self.execute_query(sql)
            return result[0][0] if result else 0
            
        except Exception as e:
            self.logger.error(f"获取表记录数失败: {table_name}, 错误: {str(e)}")
            return 0
    
    def commit(self) -> None:
        """提交事务"""
        if self.connection:
            self.connection.commit()
            self.logger.debug("事务已提交")
    
    def rollback(self) -> None:
        """回滚事务"""
        if self.connection:
            self.connection.rollback()
            self.logger.debug("事务已回滚")
    
    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        if not self.connection:
            self.connect()
        
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            self.logger.error(f"事务执行失败，已回滚: {str(e)}")
            raise
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if exc_type:
            self.rollback()
        self.disconnect()
    
    def __del__(self):
        """析构函数"""
        self.disconnect()