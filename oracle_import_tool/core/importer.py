"""核心导入器"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import logging
import time
import os
from datetime import datetime, date
from dataclasses import dataclass

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from ..data.file_reader import FileReader
from ..data.ddl_parser import DDLParser
from ..core.table_creator import TableCreator
from ..utils.progress_manager import ProgressBarManager
from rich.console import Console


class DuplicateImportError(Exception):
    """重复导入异常"""
    pass


class OracleImporter:
    """Oracle数据导入核心类"""
    
    def __init__(self, config_file: str = "config.ini", console: Optional[Console] = None, keep_date_suffix: bool = False):
        self.config_manager = ConfigManager(config_file)
        self.console = console or Console()
        
        # 初始化各组件
        self.db_connection = DatabaseConnection(self.config_manager)
        
        # 获取表头检测配置
        header_config = self.config_manager.get_header_detection_config()
        
        # 初始化FileReader，传递日期后缀处理参数
        self.file_reader = FileReader(
            header_config=header_config,
            remove_date_suffix=not keep_date_suffix  # CLI参数是keep，内部逻辑是remove
        )
        
        self.ddl_parser = DDLParser()
        self.table_creator = TableCreator(self.config_manager)
        self.progress_manager = ProgressBarManager(self.console)
        
        # 设置日志
        self._setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # 获取配置
        self.import_settings = self.config_manager.get_import_settings()
        
        # 统计信息
        self.import_results = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,  # 新增：跳过的文件数（重复导入）
            'total_rows': 0,
            'success_rows': 0,
            'failed_rows': 0,
            'start_time': None,
            'end_time': None,
            'errors': [],
            'skipped_files_list': []  # 新增：跳过的文件列表
        }
        
        # 重复导入检测相关配置
        self.duplicate_detection_config = {
            'min_batch_size': 10,  # 最小批量大小，小于此值不进行重复检测
            'duplicate_error_keywords': [  # 重复导入错误关键词
                'unique constraint',
                'unique key',
                'duplicate key',
                'ORA-00001',  # Oracle唯一约束违反
                'violates unique constraint'
            ]
        }
    
    def _setup_logging(self):
        """设置日志配置"""
        logging_config = self.config_manager.get_logging_config()
        
        # 创建logs目录
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # 配置日志
        log_level = getattr(logging, logging_config.get('log_level', 'INFO').upper())
        log_format = logging_config.get('log_format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # 配置根日志记录器
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(
                    log_dir / f"import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                    encoding='utf-8'
                ),
                logging.StreamHandler() if logging_config.get('console_output', True) else logging.NullHandler()
            ]
        )
    
    def import_data(
        self, 
        datafolder: str, 
        tables: Optional[List[str]] = None, 
        ddl_folder: Optional[str] = None, 
        create_sql: bool = False,
        dry_run: bool = False,
        **kwargs
    ) -> Dict[str, Any]:
        """主导入流程"""
        
        self.import_results['start_time'] = time.time()
        self.logger.info("开始数据导入任务")
        
        try:
            # 1. 扫描数据文件
            files_info = self._scan_data_files(datafolder, tables)
            if not files_info:
                raise ValueError(f"在目录 {datafolder} 中未找到有效的数据文件")
            
            self.import_results['total_files'] = len(files_info)
            
            # 2. 解析DDL文件（如果提供）
            ddl_info = {}
            if ddl_folder:
                ddl_info = self.ddl_parser.scan_ddl_directory(ddl_folder)
                self.logger.info(f"加载了 {len(ddl_info)} 个表的DDL信息")
            
            # 3. 连接数据库
            if not dry_run:
                self.db_connection.connect()
                if not self.db_connection.test_connection():
                    raise ConnectionError("数据库连接测试失败")
            
            # 4. 开始进度显示
            estimated_rows = self._estimate_total_rows(files_info)
            self.progress_manager.start_import_progress(len(files_info), estimated_rows)
            
            # 5. 处理每个文件
            for file_info in files_info:
                try:
                    self._process_single_file(
                        file_info=file_info,
                        ddl_info=ddl_info.get(file_info['table_name']),
                        create_sql=create_sql,
                        dry_run=dry_run
                    )
                    self.import_results['processed_files'] += 1
                    self.progress_manager.complete_file_progress(success=True)
                    
                except DuplicateImportError as e:
                    # 重复导入错误已经在_process_single_file中处理了
                    # 这里使用success=False但记录为警告
                    self.progress_manager.complete_file_progress(success=False, error_msg=f"WARNING: {str(e)}")
                    
                except Exception as e:
                    self.import_results['failed_files'] += 1
                    error_msg = f"处理文件失败: {file_info['name']}, 错误: {str(e)}"
                    self.logger.error(error_msg)
                    self.import_results['errors'].append(error_msg)
                    self.progress_manager.complete_file_progress(success=False, error_msg=str(e))
            
            # 6. 完成导入
            self.progress_manager.finish_import_progress()
            
        except Exception as e:
            self.logger.error(f"导入任务失败: {str(e)}")
            self.import_results['errors'].append(str(e))
            if hasattr(self, 'progress_manager'):
                self.progress_manager.finish_import_progress()
            raise
        
        finally:
            # 清理资源
            if hasattr(self, 'db_connection'):
                self.db_connection.disconnect()
            
            self.import_results['end_time'] = time.time()
            
            # 显示总结
            self.progress_manager.print_summary()
        
        return self.import_results
    
    def _scan_data_files(self, datafolder: str, tables: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """扫描数据文件"""
        files_info = self.file_reader.scan_directory(datafolder)
        
        # 过滤指定的表
        if tables:
            table_set = {table.upper() for table in tables}
            files_info = [
                file_info for file_info in files_info 
                if file_info['table_name'] in table_set
            ]
        
        self.logger.info(f"找到 {len(files_info)} 个数据文件待处理")
        return files_info
    
    def _estimate_total_rows(self, files_info: List[Dict[str, Any]]) -> int:
        """估算总行数"""
        total_rows = 0
        for file_info in files_info[:5]:  # 只检查前5个文件来估算
            try:
                df = self.file_reader.read_file(file_info['path'])  # 估算时不需要DDL信息
                total_rows += len(df)
            except:
                pass
        
        # 基于抽样估算总数
        if len(files_info) > 5:
            avg_rows = total_rows / min(5, len(files_info))
            total_rows = int(avg_rows * len(files_info))
        
        return total_rows
    
    def _process_single_file(
        self, 
        file_info: Dict[str, Any], 
        ddl_info: Optional[Dict[str, Any]] = None, 
        create_sql: bool = False,
        dry_run: bool = False
    ) -> None:
        """处理单个文件的导入"""
        
        file_path = file_info['path']
        table_name = file_info['table_name']
        
        self.logger.info(f"开始处理文件: {file_info['name']} -> {table_name}")
        
        try:
            # 1. 读取数据
            # 如果有DDL信息，提取列名用于无表头数据处理
            ddl_columns = None
            if ddl_info and 'columns' in ddl_info:
                ddl_columns = [col['name'] for col in ddl_info['columns'] 
                              if not col['name'].upper().endswith('_BY') and not col['name'].upper().endswith('_TIMESTAMP')]
            
            df = self.file_reader.read_file(file_path, ddl_columns)
            if df.empty:
                self.logger.warning(f"文件 {file_info['name']} 为空，跳过处理")
                return
            
            # 更新进度
            self.progress_manager.update_file_progress(file_info['name'], table_name, len(df))
            
            # 2. 推断表结构
            inferred_table_info = self.table_creator.infer_table_structure(df, table_name)
            
            # 3. 合并DDL信息
            if ddl_info:
                table_info = self.table_creator.merge_with_ddl_info(inferred_table_info, ddl_info)
            else:
                table_info = inferred_table_info
            
            # 4. 验证表结构
            validation_errors = self.table_creator.validate_table_structure(table_info)
            if validation_errors:
                raise ValueError(f"表结构验证失败: {'; '.join(validation_errors)}")
            
            # 5. 创建表（如果不存在）
            if not dry_run:
                self._ensure_table_exists(table_info)
            
            # 6. 准备数据
            prepared_data = self._prepare_data_for_insert(df, table_info)
            
            # 7. 插入数据
            if not dry_run:
                self._insert_data_in_batches(prepared_data, table_info, file_path)
            
            # 8. 生成SQL文件（如果需要）
            if create_sql:
                self._generate_sql_file(prepared_data, table_info)
            
            self.logger.info(f"文件处理完成: {file_info['name']}, 处理行数: {len(df)}")
            
        except DuplicateImportError as e:
            # 重复导入错误，已经在_handle_duplicate_import中处理了
            self.logger.info(f"跳过重复导入的文件: {file_info['name']}")
            # 不重新抛出异常，让程序继续处理其他文件
            return
        except Exception as e:
            # 其他错误正常抛出
            raise e
    
    def _ensure_table_exists(self, table_info: Dict[str, Any]) -> None:
        """确保表存在，如果不存在则报错"""
        table_name = table_info['table_name']
        
        if self.db_connection.table_exists(table_name):
            self.logger.info(f"表 {table_name} 已存在")
            
            # 检查表结构是否匹配
            existing_columns = self.db_connection.get_table_columns(table_name)
            self._validate_table_compatibility(table_info, existing_columns)
            
        else:
            # 表不存在时直接报错，不再自动创建
            raise ValueError(f"表 {table_name} 不存在，请先创建该表或提供相应的DDL文件")
    
    def _validate_table_compatibility(self, table_info: Dict[str, Any], existing_columns: List[Dict[str, Any]]) -> None:
        """验证表结构兼容性"""
        existing_column_names = {col['name'].upper() for col in existing_columns}
        required_columns = {col['name'].upper() for col in table_info['columns'] if not col['name'].upper().endswith('_BY') and not col['name'].upper().endswith('_TIMESTAMP')}
        
        missing_columns = required_columns - existing_column_names
        if missing_columns:
            self.logger.warning(f"表中缺少列: {', '.join(missing_columns)}")
    
    def _prepare_data_for_insert(self, df: pd.DataFrame, table_info: Dict[str, Any]) -> List[List[Any]]:
        """准备插入数据"""
        # 获取表列信息
        # 如果Excel文件包含审计字段，则使用它们；否则自动添加
        data_columns = []
        audit_columns = ['CREATED_BY', 'CREATE_TIMESTAMP', 'UPDATED_BY', 'UPDATE_TIMESTAMP']
        excel_has_audit_fields = {}
        
        for col in table_info['columns']:
            col_name = col['name'].upper()
            if col_name in audit_columns:
                # 检查Excel是否包含此审计字段
                has_field = col['name'] in df.columns or col_name in df.columns
                excel_has_audit_fields[col_name] = has_field
                if has_field:
                    data_columns.append(col)  # Excel包含此字段，作为数据列处理
            else:
                data_columns.append(col)  # 非审计字段，直接添加
        
        # 确保DataFrame列顺序与表结构一致
        ordered_columns = []
        for col in data_columns:
            if col['name'] in df.columns:
                ordered_columns.append(col['name'])
            else:
                self.logger.warning(f"列 {col['name']} 在数据中不存在")
        
        # 重新排序DataFrame
        df_ordered = df[ordered_columns].copy()
        
        # 数据类型转换和清理
        for i, col in enumerate(data_columns):
            if col['name'] in df_ordered.columns:
                df_ordered[col['name']] = self._convert_column_data(df_ordered[col['name']], col)
        
        # 转换为列表格式
        data_rows = []
        from datetime import datetime
        current_time = datetime.now()
        
        for _, row in df_ordered.iterrows():
            row_data = row.tolist()
            
            # 只为缺失的审计字段添加默认值
            all_columns = [col['name'].upper() for col in table_info['columns']]
            data_column_names = [col['name'].upper() for col in data_columns]
            
            # 按照表结构的完整顺序构建最终数据
            final_row_data = []
            data_col_index = 0
            
            for col in table_info['columns']:
                col_name = col['name'].upper()
                
                if col_name in data_column_names:
                    # 使用Excel中的数据
                    final_row_data.append(row_data[data_col_index])
                    data_col_index += 1
                elif col_name == 'CREATED_BY':
                    final_row_data.append('SYSTEM')
                elif col_name == 'CREATE_TIMESTAMP':
                    final_row_data.append(current_time)
                elif col_name in ['UPDATED_BY', 'UPDATE_TIMESTAMP']:
                    final_row_data.append(None)
                else:
                    final_row_data.append(None)
            
            data_rows.append(final_row_data)
        
        return data_rows
    
    def _convert_column_data(self, series: pd.Series, column_info: Dict[str, Any]) -> pd.Series:
        """转换列数据类型"""
        data_type = column_info['data_type']
        nullable = column_info.get('nullable', True)
        
        if data_type == 'VARCHAR2' or data_type == 'CHAR':
            # 转换为字符串，处理空值
            series = series.astype(str)
            
            # 处理空值：如果列允许为空，则将'nan'转换为None，否则保持原值
            if nullable:
                series = series.replace('nan', None)
                series = series.replace('NaN', None)
                series = series.replace('', None)
            
            # 截断过长的字符串
            max_length = column_info.get('length', 4000)
            series = series.apply(lambda x: x[:max_length] if x and len(str(x)) > max_length else x)
            
        elif data_type == 'NUMBER':
            # 处理数值类型
            series = pd.to_numeric(series, errors='coerce')
            
            # 根据精度和标度进行范围检查
            precision = column_info.get('precision')
            scale = column_info.get('scale')
            
            if precision is not None:
                # 计算最大值和最小值
                if scale is not None and scale > 0:
                    # 有小数位的情况，例如 NUMBER(6,2) 范围是 -9999.99 到 9999.99
                    max_value = (10 ** (precision - scale)) - (10 ** (-scale))
                    min_value = -max_value
                else:
                    # 整数情况，例如 NUMBER(6) 范围是 -999999 到 999999
                    max_value = (10 ** precision) - 1
                    min_value = -max_value
                
                # 限制数值范围
                series = series.clip(min_value, max_value)
            
            # 处理空值：如果列允许为空，则将NaN转换为None，否则设为0
            if nullable:
                # 将NaN转换为None，使用replace方法
                series = series.replace({np.nan: None, pd.NA: None})
            else:
                series = series.fillna(0)
                # 确保是整数类型（如果没有小数位）
                if scale is None or scale == 0:
                    series = series.astype(int)
            
        elif data_type == 'TIMESTAMP':
            # 处理时间戳 - 使用增强的日期时间解析器
            from ..utils.datetime_parser import datetime_parser
            series = datetime_parser.parse_series(series)
            
            # 处理空值：如果列允许为空，则将NaT转换为None
            if nullable:
                series = series.replace({pd.NaT: None})
            
            # Add additional validation for TIMESTAMP values
            # Ensure all non-null values are proper datetime objects
            def validate_timestamp(val):
                if val is None or pd.isna(val):
                    return None
                elif isinstance(val, (datetime, pd.Timestamp)):
                    # Convert pandas Timestamp to Python datetime if needed
                    if isinstance(val, pd.Timestamp):
                        return val.to_pydatetime()
                    return val
                else:
                    # Try to convert string values to datetime
                    try:
                        parsed = datetime_parser.parse_datetime(val)
                        return parsed
                    except:
                        self.logger.warning(f"无法将值转换为TIMESTAMP: {val} (类型: {type(val)})")
                        return None if nullable else datetime.now()
            
            # Apply validation to the series
            series = series.apply(validate_timestamp)
            
        elif data_type == 'DATE':
            # 处理日期类型
            series = pd.to_datetime(series, errors='coerce')
            
            # 处理空值：如果列允许为空，则将NaT转换为None
            if nullable:
                series = series.replace({pd.NaT: None})
            
            # Add additional validation for DATE values
            # Ensure all non-null values are proper datetime objects
            def validate_date(val):
                if val is None or pd.isna(val):
                    return None
                elif isinstance(val, (datetime, date, pd.Timestamp)):
                    # Convert to datetime if it's a date object
                    if isinstance(val, date) and not isinstance(val, datetime):
                        return datetime.combine(val, datetime.min.time())
                    # Convert pandas Timestamp to Python datetime if needed
                    elif isinstance(val, pd.Timestamp):
                        return val.to_pydatetime()
                    return val
                else:
                    # Try to convert string values to datetime
                    try:
                        from ..utils.datetime_parser import datetime_parser
                        parsed = datetime_parser.parse_datetime(val)
                        return parsed
                    except:
                        self.logger.warning(f"无法将值转换为DATE: {val} (类型: {type(val)})")
                        return None if nullable else datetime.now()
            
            # Apply validation to the series
            series = series.apply(validate_date)
            
        return series
    
    def _insert_data_in_batches(self, data_rows: List[List[Any]], table_info: Dict[str, Any], file_path: str) -> None:
        """批量插入数据"""
        table_name = table_info['table_name']
        columns = table_info['columns']
        
        # 构建INSERT语句
        column_names = [col['name'] for col in columns]
        placeholders = ', '.join([':' + str(i+1) for i in range(len(column_names))])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
        
        batch_size = self.import_settings.get('batch_size', 1000)
        total_batches = (len(data_rows) + batch_size - 1) // batch_size
        
        success_count = 0
        failed_count = 0
        duplicate_detected = False
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(data_rows))
            batch_data = data_rows[start_idx:end_idx]
            
            try:
                with self.db_connection.transaction():
                    affected_rows = self.db_connection.batch_insert(insert_sql, batch_data)
                    success_count += affected_rows
                    self.progress_manager.update_row_progress(len(batch_data))
                
            except Exception as e:
                error_msg = f"批量插入失败 (批次 {batch_num + 1}): {str(e)}"
                self.logger.error(error_msg)
                
                # 检测是否为重复导入
                if self._detect_duplicate_import(batch_data, str(e), batch_size):
                    # 如果是重复导入，处理并跳出循环
                    self._handle_duplicate_import(file_path, table_name)
                    duplicate_detected = True
                    break
                
                # 批量插入失败的行数暂时计入失败
                batch_failed_count = len(batch_data)
                
                # 尝试单行插入，看看能否挽救部分数据
                retry_success, retry_failed = self._retry_single_row_insert(batch_data, insert_sql, batch_num + 1)
                
                # 修正统计：重试成功的行数从失败转为成功
                success_count += retry_success
                failed_count += retry_failed
                
                # 即使失败也要更新已处理的行数
                self.progress_manager.update_row_progress(len(batch_data))
                
                self.logger.info(f"批次 {batch_num + 1} 重试结果: 成功 {retry_success} 行, 失败 {retry_failed} 行")
            
            # 如果检测到重复导入，跳出循环
            if duplicate_detected:
                break
            
            # 更新批次进度
            self.progress_manager.update_batch_progress(
                batch_num + 1, total_batches, len(batch_data), 
                success_count, failed_count
            )
        
        # 如果不是重复导入，更新最终统计
        if not duplicate_detected:
            # 最终统计：确保总行数 = 成功行数 + 失败行数
            total_processed = success_count + failed_count
            self.import_results['success_rows'] += success_count
            self.import_results['failed_rows'] += failed_count
            self.import_results['total_rows'] += total_processed
            
            self.logger.info(f"数据插入完成: 成功 {success_count} 行, 失败 {failed_count} 行, 总计 {total_processed} 行")
        else:
            # 如果是重复导入，不更新行数统计（因为数据没有实际处理）
            self.logger.info(f"检测到重复导入，跳过文件 {file_path} 的剩余数据处理")
    
    def _retry_single_row_insert(self, batch_data: List[List[Any]], insert_sql: str, batch_num: int) -> Tuple[int, int]:
        """单行重试插入"""
        retry_success_count = 0
        retry_failed_count = 0
        
        for row_idx, row_data in enumerate(batch_data):
            try:
                affected_rows = self.db_connection.execute_dml(insert_sql, row_data)
                retry_success_count += affected_rows
            except Exception as e:
                retry_failed_count += 1
                error_msg = f"单行插入失败 (批次 {batch_num}, 行 {row_idx + 1}): {str(e)}"
                self.logger.error(error_msg)
                self.progress_manager.add_error(error_msg)
        
        return retry_success_count, retry_failed_count
    
    def _is_duplicate_import_error(self, error_message: str) -> bool:
        """判断是否为重复导入错误"""
        if not error_message:
            return False
        
        error_lower = error_message.lower()
        return any(keyword.lower() in error_lower for keyword in self.duplicate_detection_config['duplicate_error_keywords'])
    
    def _detect_duplicate_import(self, batch_data: List[List[Any]], error_message: str, batch_size: int) -> bool:
        """检测是否为重复导入
        
        Args:
            batch_data: 批量数据
            error_message: 错误信息
            batch_size: 批量大小
            
        Returns:
            bool: 如果是重复导入返回True，否则返回False
        """
        # 1. 检查批量大小是否足够大
        if len(batch_data) < self.duplicate_detection_config['min_batch_size']:
            self.logger.debug(f"批量大小 {len(batch_data)} 小于最小检测阈值 {self.duplicate_detection_config['min_batch_size']}，跳过重复检测")
            return False
        
        # 2. 检查错误信息是否包含重复导入关键词
        if not self._is_duplicate_import_error(error_message):
            self.logger.debug("错误信息不包含重复导入关键词，跳过重复检测")
            return False
        
        # 3. 检查是否所有行都因为唯一约束冲突而失败
        # 这里我们假设如果批量插入失败且错误信息包含唯一约束冲突，则很可能是重复导入
        self.logger.info(f"检测到可能的重复导入: 批量大小={len(batch_data)}, 错误类型=唯一约束冲突")
        
        return True
    
    def _handle_duplicate_import(self, file_path: str, table_name: str) -> None:
        """处理重复导入的情况"""
        error_msg = f"文件 {file_path} 已导入过，检测到重复导入，跳过处理"
        self.logger.warning(error_msg)
        
        # 更新统计信息
        self.import_results['skipped_files'] += 1
        self.import_results['skipped_files_list'].append({
            'file_path': file_path,
            'table_name': table_name,
            'reason': 'duplicate_import',
            'timestamp': datetime.now()
        })
        
        # 记录错误信息（作为警告而不是错误）
        self.import_results['errors'].append(f"WARNING: {error_msg}")
        
        # 抛出特定异常以跳过当前文件的处理
        raise DuplicateImportError(error_msg)
    
    def _generate_sql_file(self, data_rows: List[List[Any]], table_info: Dict[str, Any]) -> None:
        """生成SQL文件"""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        table_name = table_info['table_name']
        sql_file = output_dir / f"{table_name}_insert.sql"
        
        column_names = [col['name'] for col in table_info['columns']]
        
        with open(sql_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Generated INSERT statements for {table_name}\\n")
            f.write(f"-- Generated at: {datetime.now()}\\n\\n")
            
            for row_data in data_rows:
                values = []
                for value in row_data:
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, str):
                        # 转义单引号
                        escaped_value = str(value).replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    else:
                        values.append(str(value))
                
                values_str = ', '.join(values)
                f.write(f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({values_str});\\n")
        
        self.logger.info(f"SQL文件已生成: {sql_file}")
    
    def get_import_summary(self) -> Dict[str, Any]:
        """获取导入总结"""
        return self.import_results.copy()