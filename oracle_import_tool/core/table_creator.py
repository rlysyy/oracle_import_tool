"""表结构创建器"""

import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
import logging
import re
from datetime import datetime
from ..config.config_manager import ConfigManager


class TableCreator:
    """表结构推断和创建"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.data_types_config = config_manager.get_data_types_config()
        self.logger = logging.getLogger(__name__)
    
    def infer_table_structure(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """基于DataFrame推断表结构"""
        columns = []
        
        for column_name in df.columns:
            column_info = self._analyze_column(df[column_name], column_name)
            columns.append(column_info)
        
        table_info = {
            'table_name': table_name.upper(),
            'columns': columns,
            'primary_key': [],
            'indexes': [],
            'constraints': []
        }
        
        # 添加审计字段
        table_info = self.add_audit_columns(table_info)
        
        return table_info
    
    def _analyze_column(self, series: pd.Series, column_name: str) -> Dict[str, Any]:
        """分析单个列的数据类型和属性"""
        column_info = {
            'name': column_name.upper(),
            'nullable': series.isnull().any(),
            'default_value': None
        }
        
        # 移除空值来分析数据类型
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            # 全为空值，默认为VARCHAR2
            column_info.update({
                'data_type': 'VARCHAR2',
                'length': 4000,
                'precision': None,
                'scale': None
            })
            return column_info
        
        # 获取数据类型信息
        oracle_type_info = self.map_pandas_to_oracle_type(non_null_series)
        column_info.update(oracle_type_info)
        
        return column_info
    
    def map_pandas_to_oracle_type(self, series: pd.Series) -> Dict[str, Any]:
        """将Pandas数据类型映射到Oracle数据类型"""
        dtype = series.dtype
        
        if pd.api.types.is_integer_dtype(dtype):
            return self._handle_integer_type(series)
        elif pd.api.types.is_float_dtype(dtype):
            return self._handle_float_type(series)
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return {
                'data_type': 'TIMESTAMP',
                'length': None,
                'precision': 6,
                'scale': None
            }
        elif pd.api.types.is_bool_dtype(dtype):
            return {
                'data_type': 'CHAR',
                'length': 1,
                'precision': None,
                'scale': None
            }
        else:
            return self._handle_string_type(series)
    
    def _handle_integer_type(self, series: pd.Series) -> Dict[str, Any]:
        """处理整数类型"""
        max_val = series.max()
        min_val = series.min()
        
        # 根据数值范围决定精度
        max_abs = max(abs(max_val), abs(min_val))
        
        if max_abs < 10**9:
            precision = 10
        elif max_abs < 10**18:
            precision = 19
        else:
            precision = self.data_types_config.get('number_precision', 38)
        
        return {
            'data_type': 'NUMBER',
            'length': None,
            'precision': precision,
            'scale': 0
        }
    
    def _handle_float_type(self, series: pd.Series) -> Dict[str, Any]:
        """处理浮点数类型"""
        # 分析小数位数
        max_decimal_places = 0
        
        for value in series.dropna():
            if pd.notna(value):
                decimal_str = str(float(value))
                if '.' in decimal_str:
                    decimal_places = len(decimal_str.split('.')[1].rstrip('0'))
                    max_decimal_places = max(max_decimal_places, decimal_places)
        
        scale = min(max_decimal_places, self.data_types_config.get('number_scale', 2))
        precision = self.data_types_config.get('number_precision', 38)
        
        return {
            'data_type': 'NUMBER',
            'length': None,
            'precision': precision,
            'scale': scale
        }
    
    def _handle_string_type(self, series: pd.Series) -> Dict[str, Any]:
        """处理字符串类型"""
        # 首先检查是否可能是日期时间类型
        datetime_result = self._check_if_datetime_string(series)
        if datetime_result:
            return datetime_result
        
        # 计算最大长度
        max_length = 0
        
        for value in series.dropna():
            if pd.notna(value):
                length = len(str(value))
                max_length = max(max_length, length)
        
        # 确定VARCHAR2长度，至少50，最大4000
        if max_length == 0:
            length = 100
        elif max_length < 50:
            length = 50
        elif max_length > self.data_types_config.get('string_max_length', 4000):
            length = self.data_types_config.get('string_max_length', 4000)
        else:
            # 增加一些余量
            length = min(max_length * 2, self.data_types_config.get('string_max_length', 4000))
        
        return {
            'data_type': 'VARCHAR2',
            'length': length,
            'precision': None,
            'scale': None
        }
    
    def _check_if_datetime_string(self, series: pd.Series) -> Optional[Dict[str, Any]]:
        """检查字符串列是否包含日期时间数据"""
        from ..utils.datetime_parser import datetime_parser
        
        # 如果列为空，不进行检查
        if series.empty:
            return None
            
        non_null_series = series.dropna()
        if len(non_null_series) == 0:
            return None
        
        # 首先通过列名快速判断
        col_name_upper = series.name.upper()
        if any(keyword in col_name_upper for keyword in ['TIMESTAMP', 'DATE', 'TIME', 'CREATED', 'UPDATED']):
            # 列名包含时间关键词，更宽松的检测标准
            return self._check_datetime_by_content(series, threshold=0.5)
        
        # 对于其他列，使用更严格的标准
        return self._check_datetime_by_content(series, threshold=0.8)
    
    def _check_datetime_by_content(self, series: pd.Series, threshold: float = 0.7) -> Optional[Dict[str, Any]]:
        """通过内容检查是否为日期时间列"""
        from ..utils.datetime_parser import datetime_parser
        
        # 采样检查前几个值是否像日期时间
        sample_size = min(10, len(series))
        sample_values = series.head(sample_size)
        
        datetime_like_count = 0
        total_checked = 0
        
        for val in sample_values:
            if isinstance(val, str) and pd.notna(val):
                total_checked += 1
                str_val = str(val).strip()
                
                # 基本格式检查：必须包含日期分隔符
                if not any(indicator in str_val for indicator in ['/', '-', '.']):
                    continue
                
                # 检查是否包含数字和分隔符的合理组合
                # 排除明显不是日期的格式，如纯数字ID
                if re.match(r'^[0-9]+-[0-9]+$', str_val):
                    continue  # 类似 "123-456" 的格式不算日期
                
                datetime_like_count += 1
                
                # 尝试解析
                try:
                    parsed = datetime_parser.parse_datetime(str_val)
                    if parsed and datetime_parser._is_valid_date(parsed):
                        # 额外检查：年份是否合理
                        if 1900 <= parsed.year <= 2100:
                            continue  # 解析成功且年份合理
                        else:
                            datetime_like_count -= 1  # 年份不合理
                    else:
                        datetime_like_count -= 1  # 解析失败
                except:
                    datetime_like_count -= 1  # 解析失败
                    
            elif isinstance(val, (pd.Timestamp, datetime)):
                datetime_like_count += 1
                total_checked += 1
        
        # 如果达到阈值，则认为是日期时间列
        if total_checked > 0 and (datetime_like_count / total_checked) >= threshold:
            self.logger.info(f"检测到日期时间字符串列: {series.name}, {datetime_like_count}/{total_checked} 个值符合日期时间格式 (阈值: {threshold})")
            return {
                'data_type': 'TIMESTAMP',
                'length': None,
                'precision': 6,
                'scale': None
            }
        
        return None
    
    def add_audit_columns(self, table_info: Dict[str, Any]) -> Dict[str, Any]:
        """添加审计字段"""
        audit_columns = [
            {
                'name': 'CREATED_BY',
                'data_type': 'VARCHAR2',
                'length': 50,
                'precision': None,
                'scale': None,
                'nullable': True,
                'default_value': None
            },
            {
                'name': 'CREATE_TIMESTAMP',
                'data_type': 'TIMESTAMP',
                'length': None,
                'precision': 6,
                'scale': None,
                'nullable': True,
                'default_value': 'SYSTIMESTAMP'
            },
            {
                'name': 'UPDATED_BY',
                'data_type': 'VARCHAR2',
                'length': 50,
                'precision': None,
                'scale': None,
                'nullable': True,
                'default_value': None
            },
            {
                'name': 'UPDATE_TIMESTAMP',
                'data_type': 'TIMESTAMP',
                'length': None,
                'precision': 6,
                'scale': None,
                'nullable': True,
                'default_value': None
            }
        ]
        
        # 检查是否已经存在审计字段
        existing_columns = {col['name'].upper() for col in table_info['columns']}
        
        for audit_col in audit_columns:
            if audit_col['name'] not in existing_columns:
                table_info['columns'].append(audit_col)
        
        return table_info
    
    def generate_create_table_sql(self, table_info: Dict[str, Any]) -> str:
        """生成CREATE TABLE语句"""
        table_name = table_info['table_name']
        columns = table_info['columns']
        
        sql_lines = [f"CREATE TABLE {table_name} ("]
        
        column_definitions = []
        
        for column in columns:
            col_def = self._generate_column_definition(column)
            column_definitions.append(f"    {col_def}")
        
        # 添加主键约束（如果有）
        if table_info.get('primary_key'):
            pk_columns = ', '.join(table_info['primary_key'])
            column_definitions.append(f"    CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_columns})")
        
        sql_lines.append(',\\n'.join(column_definitions))
        sql_lines.append(")")
        
        return '\\n'.join(sql_lines)
    
    def _generate_column_definition(self, column: Dict[str, Any]) -> str:
        """生成单个列定义"""
        name = column['name']
        data_type = column['data_type']
        
        # 构建数据类型字符串
        if data_type == 'VARCHAR2':
            type_str = f"VARCHAR2({column['length']})"
        elif data_type == 'NUMBER':
            if column.get('scale') is not None:
                type_str = f"NUMBER({column['precision']}, {column['scale']})"
            else:
                type_str = f"NUMBER({column['precision']})"
        elif data_type == 'TIMESTAMP':
            precision = column.get('precision', 6)
            type_str = f"TIMESTAMP({precision})"
        elif data_type == 'CHAR':
            type_str = f"CHAR({column['length']})"
        else:
            type_str = data_type
        
        # 添加约束
        constraints = []
        
        if not column.get('nullable', True):
            constraints.append("NOT NULL")
        
        if column.get('default_value'):
            default_val = column['default_value']
            if default_val == 'SYSTIMESTAMP':
                constraints.append("DEFAULT SYSTIMESTAMP")
            else:
                constraints.append(f"DEFAULT '{default_val}'")
        
        constraint_str = ' '.join(constraints)
        
        return f"{name} {type_str} {constraint_str}".strip()
    
    def merge_with_ddl_info(self, inferred_info: Dict[str, Any], ddl_info: Dict[str, Any]) -> Dict[str, Any]:
        """合并推断的结构和DDL信息"""
        if not ddl_info:
            return inferred_info
        
        # 使用DDL信息作为基础
        merged_info = ddl_info.copy()
        
        # 从推断信息中获取不在DDL中的列
        ddl_column_names = {col['name'].upper() for col in ddl_info.get('columns', [])}
        inferred_columns = inferred_info.get('columns', [])
        
        for inferred_col in inferred_columns:
            if inferred_col['name'].upper() not in ddl_column_names:
                merged_info['columns'].append(inferred_col)
                self.logger.info(f"添加DDL中缺失的列: {inferred_col['name']}")
        
        return merged_info
    
    def create_table_info_from_ddl(self, ddl_info: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """Create table info structure directly from DDL information"""
        if not ddl_info:
            raise ValueError(f"No DDL information provided for table {table_name}")
        
        # Use DDL info as the primary source
        table_info = ddl_info.copy()
        
        # Ensure table name is set correctly
        table_info['table_name'] = table_name.upper()
        
        # Add audit columns if not already present
        table_info = self.add_audit_columns(table_info)
        
        self.logger.info(f"Created table structure from DDL for {table_name}: {len(table_info.get('columns', []))} columns")
        return table_info
    
    def validate_data_against_ddl(self, df: pd.DataFrame, ddl_info: Dict[str, Any]) -> Dict[str, Any]:
        """Validate DataFrame structure against DDL definition"""
        validation_result = {
            'has_errors': False,
            'errors': [],
            'warnings': []
        }
        
        if not ddl_info or 'columns' not in ddl_info:
            validation_result['has_errors'] = True
            validation_result['errors'].append("DDL information is incomplete")
            return validation_result
        
        # Get DDL column names (excluding audit columns)
        ddl_columns = [col['name'].upper() for col in ddl_info['columns'] 
                      if not col['name'].upper().endswith('_BY') and 
                         not col['name'].upper().endswith('_TIMESTAMP')]
        
        df_columns = [col.upper() for col in df.columns]
        
        # Check if data has more columns than DDL expects
        extra_columns = set(df_columns) - set(ddl_columns)
        if extra_columns:
            validation_result['warnings'].append(f"Data has extra columns not in DDL: {extra_columns}")
        
        # Check if DDL has required columns that data doesn't have
        missing_columns = set(ddl_columns) - set(df_columns)
        if missing_columns:
            # Check if missing columns have defaults or are nullable
            required_missing = []
            for col_name in missing_columns:
                ddl_col = next((col for col in ddl_info['columns'] if col['name'].upper() == col_name), None)
                if ddl_col and not ddl_col.get('nullable', True) and not ddl_col.get('default_value'):
                    required_missing.append(col_name)
            
            if required_missing:
                validation_result['has_errors'] = True
                validation_result['errors'].append(f"Data missing required columns: {required_missing}")
            elif missing_columns:
                validation_result['warnings'].append(f"Data missing optional columns: {missing_columns}")
        
        return validation_result
    
    def validate_table_structure(self, table_info: Dict[str, Any]) -> List[str]:
        """验证表结构"""
        errors = []
        
        table_name = table_info.get('table_name', '')
        if not table_name:
            errors.append("表名不能为空")
        elif len(table_name) > 30:
            errors.append(f"表名过长（{len(table_name)} > 30）: {table_name}")
        
        columns = table_info.get('columns', [])
        if not columns:
            errors.append("表必须至少包含一列")
        
        column_names = set()
        for i, column in enumerate(columns):
            col_name = column.get('name', '')
            
            # 检查列名
            if not col_name:
                errors.append(f"第{i+1}列的列名不能为空")
                continue
            
            if col_name in column_names:
                errors.append(f"重复的列名: {col_name}")
            column_names.add(col_name)
            
            if len(col_name) > 30:
                errors.append(f"列名过长（{len(col_name)} > 30）: {col_name}")
            
            # 检查数据类型
            data_type = column.get('data_type', '')
            if not data_type:
                errors.append(f"列 {col_name} 缺少数据类型")
            
            # 检查VARCHAR2长度
            if data_type == 'VARCHAR2':
                length = column.get('length')
                if not length or length <= 0:
                    errors.append(f"列 {col_name} 的VARCHAR2长度无效: {length}")
                elif length > 4000:
                    errors.append(f"列 {col_name} 的VARCHAR2长度超出限制: {length} > 4000")
            
            # 检查NUMBER精度
            elif data_type == 'NUMBER':
                precision = column.get('precision')
                scale = column.get('scale')
                
                if precision and (precision < 1 or precision > 38):
                    errors.append(f"列 {col_name} 的NUMBER精度超出范围: {precision} (1-38)")
                
                if scale is not None and precision and scale > precision:
                    errors.append(f"列 {col_name} 的NUMBER标度不能大于精度: scale={scale}, precision={precision}")
        
        return errors