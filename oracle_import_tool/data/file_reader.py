"""文件读取器模块"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging
import chardet
import re
from ..utils.header_detector import HeaderDetector


class FileReader:
    """多格式文件读取器"""
    
    def __init__(self, encoding: str = 'utf-8-sig', header_config: Optional[Dict[str, Any]] = None, remove_date_suffix: bool = True):
        self.encoding = encoding
        self.remove_date_suffix = remove_date_suffix
        self.logger = logging.getLogger(__name__)
        
        # 初始化表头检测器
        if header_config is None:
            header_config = {
                'header_keywords': '',
                'header_detection_mode': 'auto'
            }
        self.header_detector = HeaderDetector(header_config)
    
    def read_file(self, file_path: str, ddl_columns: Optional[List[str]] = None, ddl_info: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """根据文件扩展名选择相应的读取方法
        
        Args:
            file_path: 文件路径
            ddl_columns: DDL中定义的列名列表，用于处理无表头的数据
            ddl_info: DDL信息，包含列类型定义
        """
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        extension = path.suffix.lower()
        
        if extension in ['.xls', '.xlsx']:
            return self.read_excel(file_path, ddl_columns, ddl_info)
        elif extension == '.csv':
            return self.read_csv(file_path, ddl_columns, ddl_info)
        else:
            raise ValueError(f"不支持的文件格式: {extension}")
    
    def read_excel(self, file_path: str, ddl_columns: Optional[List[str]] = None, ddl_info: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """读取Excel文件 (XLS/XLSX)"""
        try:
            self.logger.info(f"读取Excel文件: {file_path}")
            
            # 构建数据类型字典
            dtype_dict = self._build_dtype_dict_from_ddl(ddl_info) if ddl_info else None
            
            # 读取Excel文件
            read_params = {
                'engine': 'openpyxl' if file_path.endswith('.xlsx') else 'xlrd'
            }
            
            if dtype_dict:
                read_params['dtype'] = dtype_dict
                self.logger.info(f"使用DDL指定的数据类型读取Excel: {dtype_dict}")
            
            df = pd.read_excel(file_path, **read_params)
            
            # 数据清理（跳过数据类型优化如果有DDL）
            df = self._clean_dataframe(df, skip_dtype_optimization=bool(ddl_info))
            
            self.logger.info(f"成功读取Excel文件，共 {len(df)} 行数据")
            return df
            
        except Exception as e:
            error_msg = f"读取Excel文件失败: {file_path}, 错误: {str(e)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def read_csv(self, file_path: str, ddl_columns: Optional[List[str]] = None, ddl_info: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """读取CSV文件，处理BOM和编码问题
        
        Args:
            file_path: 文件路径
            ddl_columns: DDL中定义的列名列表，用于处理无表头的数据
            ddl_info: DDL信息，包含列类型定义
        """
        try:
            self.logger.info(f"读取CSV文件: {file_path}")
            
            # 检测文件编码
            encoding = self._detect_encoding(file_path)
            
            # 构建数据类型字典
            dtype_dict = self._build_dtype_dict_from_ddl(ddl_info) if ddl_info else None
            
            # 尝试不同的分隔符
            separators = [',', ';', '\t', '|']
            df = None
            
            # 首先尝试有表头的读取
            for sep in separators:
                try:
                    read_params = {
                        'encoding': encoding,
                        'sep': sep,
                        'na_values': ['', 'NaN', 'NULL', 'null', 'None']
                    }
                    if dtype_dict:
                        read_params['dtype'] = dtype_dict
                    
                    df_with_header = pd.read_csv(file_path, **read_params)
                    
                    # 检查是否成功解析且有合理的表头
                    if (len(df_with_header.columns) > 1 or (len(df_with_header.columns) == 1 and len(df_with_header) > 0)):
                        # 使用HeaderDetector进行表头检测
                        first_row_looks_like_header = self.header_detector.detect_header(df_with_header.columns.tolist())
                        
                        if first_row_looks_like_header:
                            df = df_with_header
                            break
                except:
                    continue
            
            # 如果没有找到合适的表头，且提供了DDL列信息，尝试无表头读取
            if df is None and ddl_columns:
                self.logger.info("未检测到表头，尝试使用DDL列定义进行无表头读取")
                for sep in separators:
                    try:
                        df_no_header = pd.read_csv(
                            file_path,
                            encoding=encoding,
                            sep=sep,
                            header=None,  # 无表头
                            names=ddl_columns,  # 使用DDL中的列名
                            na_values=['', 'NaN', 'NULL', 'null', 'None']
                        )
                        
                        # 检查列数是否匹配
                        if len(df_no_header.columns) == len(ddl_columns):
                            df = df_no_header
                            self.logger.info(f"成功使用DDL列定义读取数据，列数: {len(ddl_columns)}")
                            break
                    except:
                        continue
            
            # 如果仍然没有成功读取，尝试标准读取
            if df is None:
                for sep in separators:
                    try:
                        df = pd.read_csv(
                            file_path,
                            encoding=encoding,
                            sep=sep,
                            na_values=['', 'NaN', 'NULL', 'null', 'None']
                        )
                        
                        if len(df.columns) > 0 and len(df) > 0:
                            break
                    except:
                        continue
            
            if df is None or df.empty:
                raise ValueError("无法解析CSV文件或文件为空")
            
            # 数据清理（跳过数据类型优化如果有DDL）
            df = self._clean_dataframe(df, skip_dtype_optimization=bool(ddl_info))
            
            self.logger.info(f"成功读取CSV文件，共 {len(df)} 行数据")
            return df
            
        except Exception as e:
            error_msg = f"读取CSV文件失败: {file_path}, 错误: {str(e)}"
            self.logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def _detect_encoding(self, file_path: str) -> str:
        """检测文件编码"""
        try:
            with open(file_path, 'rb') as f:
                sample = f.read(10000)  # 读取前10KB来检测编码
                
            result = chardet.detect(sample)
            detected_encoding = result.get('encoding', self.encoding)
            
            # 优先使用配置的编码，如果检测失败则使用检测到的编码
            if detected_encoding and result.get('confidence', 0) > 0.7:
                return detected_encoding
            else:
                return self.encoding
                
        except Exception as e:
            self.logger.warning(f"编码检测失败，使用默认编码 {self.encoding}: {str(e)}")
            return self.encoding
    
    def _build_dtype_dict_from_ddl(self, ddl_info: Dict[str, Any]) -> Dict[str, str]:
        """根据DDL信息构建pandas数据类型字典"""
        dtype_dict = {}
        
        if 'columns' in ddl_info:
            for col in ddl_info['columns']:
                col_name = col['name']
                data_type = col['data_type']
                
                # 审计字段通常不在Excel中，跳过
                if col_name.upper() in ['CREATED_BY', 'CREATE_TIMESTAMP', 'UPDATED_BY', 'UPDATE_TIMESTAMP']:
                    continue
                
                # 将Oracle类型映射为pandas类型
                if data_type in ['VARCHAR2', 'CHAR', 'CLOB']:
                    dtype_dict[col_name] = 'str'
                elif data_type == 'NUMBER':
                    # NUMBER类型使用object，避免精度丢失
                    dtype_dict[col_name] = 'object'
                elif data_type in ['DATE', 'TIMESTAMP']:
                    dtype_dict[col_name] = 'str'  # 先读为字符串，后续再转换
                else:
                    dtype_dict[col_name] = 'object'
        
        return dtype_dict
    
    def _clean_dataframe(self, df: pd.DataFrame, skip_dtype_optimization: bool = False) -> pd.DataFrame:
        """清理DataFrame数据"""
        # 删除所有列都为空的行
        df = df.dropna(how='all')
        
        # 清理列名
        df.columns = [self._clean_column_name(col) for col in df.columns]
        
        # 删除重复的列名（保留第一个）
        df = df.loc[:, ~df.columns.duplicated()]
        
        # 重置索引
        df = df.reset_index(drop=True)
        
        # 处理数据类型（如果没有DDL信息才进行优化）
        if not skip_dtype_optimization:
            df = self._optimize_dtypes(df)
        else:
            self.logger.info("跳过数据类型优化，使用DDL定义的类型")
        
        return df
    
    def _looks_like_header_row(self, columns) -> bool:
        """检查是否看起来像表头行（保留作为备用方法）"""
        # 使用HeaderDetector的默认检测逻辑
        return self.header_detector._default_header_detection(columns)
    
    def _clean_column_name(self, column_name: str) -> str:
        """清理列名"""
        if pd.isna(column_name):
            return "UNKNOWN_COLUMN"
        
        # 转换为字符串
        column_name = str(column_name).strip()
        
        # 移除特殊字符，保留字母、数字和下划线
        column_name = re.sub(r'[^\w\s]', '_', column_name)
        
        # 替换空格为下划线
        column_name = re.sub(r'\s+', '_', column_name)
        
        # 移除连续的下划线
        column_name = re.sub(r'_+', '_', column_name)
        
        # 移除开头和结尾的下划线
        column_name = column_name.strip('_')
        
        # 确保不为空
        if not column_name:
            column_name = "UNKNOWN_COLUMN"
        
        # 转为大写（Oracle列名通常为大写）
        return column_name.upper()
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化数据类型（保守模式，保留前导零）"""
        for column in df.columns:
            # 仅转换明显的数值类型
            if df[column].dtype == 'object':
                # 检查是否包含前导零的字符串
                has_leading_zeros = self._has_leading_zeros(df[column])
                
                if not has_leading_zeros:
                    # 只有在没有前导零的情况下才尝试数值转换
                    try:
                        # 检查所有非空值是否都能转换为数字
                        non_null_series = df[column].dropna()
                        if len(non_null_series) > 0:
                            # 如果所有值都能转换为数字，才进行转换
                            pd.to_numeric(non_null_series, errors='raise')
                            df[column] = pd.to_numeric(df[column], errors='coerce')
                    except (ValueError, TypeError):
                        # 保持为字符串类型，不尝试日期转换
                        pass
                else:
                    self.logger.info(f"Column '{column}' contains leading zeros, preserving as string type")
        
        return df
    
    def _has_leading_zeros(self, series: pd.Series) -> bool:
        """检查列是否包含前导零的字符串"""
        non_null_series = series.dropna()
        if len(non_null_series) == 0:
            return False
        
        for value in non_null_series:
            str_value = str(value).strip()
            # 检查字符串是否以0开头且长度大于1，并且是纯数字
            if (str_value.startswith('0') and 
                len(str_value) > 1 and 
                str_value.isdigit()):
                return True
        
        return False
    
    def get_table_name_from_filename(self, file_path: str) -> str:
        """从文件名提取表名"""
        path = Path(file_path)
        table_name = path.stem
        
        # 移除常见的日期后缀（如果启用）
        if self.remove_date_suffix:
            table_name = self._remove_date_suffix(table_name)
        
        # 清理表名
        table_name = re.sub(r'[^\w]', '_', table_name)
        table_name = re.sub(r'_+', '_', table_name)
        table_name = table_name.strip('_').upper()
        
        # 确保表名符合Oracle命名规范
        if not table_name or not table_name[0].isalpha():
            table_name = f"T_{table_name}"
        
        # 限制长度（Oracle表名最大30字符）
        if len(table_name) > 30:
            table_name = table_name[:30]
        
        return table_name
    
    def _remove_date_suffix(self, filename: str) -> str:
        """移除文件名中的日期后缀
        
        支持的日期格式：
        - YYYYMMDD: 20250822
        - YYYY-MM-DD: 2025-08-22
        - YYYY_MM_DD: 2025_08_22
        - YYYYMM: 202508
        - timestamp: 1640995200 (10位数字)
        """
        # 移除常见的日期格式后缀
        patterns = [
            r'_?\d{8}$',           # _20250822 或 20250822
            r'_?\d{4}-\d{2}-\d{2}$',  # _2025-08-22 或 2025-08-22
            r'_?\d{4}_\d{2}_\d{2}$',  # _2025_08_22 或 2025_08_22
            r'_?\d{6}$',           # _202508 或 202508
            r'_?\d{10}$',          # _1640995200 或 1640995200 (timestamp)
            r'_?\d{4}\d{2}\d{2}_\d+$', # _20250822_001 (带序号)
        ]
        
        original_filename = filename
        for pattern in patterns:
            filename = re.sub(pattern, '', filename, flags=re.IGNORECASE)
            # 如果移除后缀后文件名变得太短或为空，恢复原文件名
            if len(filename.strip('_')) < 2:
                filename = original_filename
                break
        
        return filename
    
    def scan_directory(self, directory: str, extensions: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """扫描目录中的数据文件"""
        if extensions is None:
            extensions = ['xls', 'xlsx', 'csv']
        
        directory_path = Path(directory)
        if not directory_path.exists():
            raise FileNotFoundError(f"目录不存在: {directory}")
        
        files_info = []
        
        for ext in extensions:
            for file_path in directory_path.rglob(f"*.{ext}"):
                # 跳过临时文件（以~$开头的文件）
                if file_path.name.startswith('~$'):
                    continue
                    
                try:
                    file_info = {
                        'path': str(file_path),
                        'name': file_path.name,
                        'extension': file_path.suffix.lower(),
                        'size': file_path.stat().st_size,
                        'table_name': self.get_table_name_from_filename(str(file_path)),
                        'modified_time': file_path.stat().st_mtime
                    }
                    files_info.append(file_info)
                except Exception as e:
                    self.logger.warning(f"无法获取文件信息: {file_path}, 错误: {str(e)}")
        
        # 按修改时间排序
        files_info.sort(key=lambda x: x['modified_time'])
        
        self.logger.info(f"扫描目录 {directory}，发现 {len(files_info)} 个数据文件")
        return files_info
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """获取文件基本信息"""
        path = Path(file_path)
        
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        return {
            'path': str(path),
            'name': path.name,
            'extension': path.suffix.lower(),
            'size': path.stat().st_size,
            'table_name': self.get_table_name_from_filename(str(path)),
            'modified_time': path.stat().st_mtime
        }
    
    def preview_file(self, file_path: str, rows: int = 5, ddl_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """预览文件前几行"""
        df = self.read_file(file_path, ddl_columns)
        return df.head(rows)
    
    def get_file_stats(self, file_path: str, ddl_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """获取文件统计信息"""
        df = self.read_file(file_path, ddl_columns)
        
        return {
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': df.columns.tolist(),
            'null_counts': df.isnull().sum().to_dict(),
            'dtypes': df.dtypes.to_dict()
        }