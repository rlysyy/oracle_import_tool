"""
灵活的日期时间解析器，支持多种时间格式
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Any, Optional, Union
import re
import logging


class FlexibleDateTimeParser:
    """灵活的日期时间解析器"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # 常见的日期时间格式模式
        self.datetime_patterns = [
            # 标准 ISO 格式
            '%Y-%m-%d %H:%M:%S.%f',      # 2025-08-27 08:22:10.422682
            '%Y-%m-%d %H:%M:%S',         # 2025-08-27 08:22:10
            '%Y-%m-%dT%H:%M:%S.%f',      # 2025-08-27T08:22:10.422682
            '%Y-%m-%dT%H:%M:%S',         # 2025-08-27T08:22:10
            '%Y-%m-%d',                  # 2025-08-27
            
            # 常见分隔符格式
            '%Y/%m/%d %H:%M:%S.%f',      # 2025/08/27 08:22:10.422682
            '%Y/%m/%d %H:%M:%S',         # 2025/08/27 08:22:10
            '%Y/%m/%d',                  # 2025/08/27
            
            # 数字格式（月日可能没有前导0）
            '%Y/%m/%d %H:%M:%S.%f',      # 2025/8/27 8:22:10.422682
            '%Y/%m/%d %H:%M:%S',         # 2025/8/27 8:22:10
            '%Y/%m/%d',                  # 2025/8/27
            
            # 中文格式
            '%Y年%m月%d日 %H:%M:%S',       # 2025年08月27日 08:22:10
            '%Y年%m月%d日',              # 2025年08月27日
            
            # 其他常见格式
            '%d/%m/%Y %H:%M:%S',         # 27/08/2025 08:22:10
            '%d/%m/%Y',                  # 27/08/2025
            '%m/%d/%Y %H:%M:%S',         # 08/27/2025 08:22:10
            '%m/%d/%Y',                  # 08/27/2025
            
            # 点分隔格式
            '%Y.%m.%d %H:%M:%S',         # 2025.08.27 08:22:10
            '%Y.%m.%d',                  # 2025.08.27
            
            # 紧凑格式
            '%Y%m%d %H%M%S',             # 20250827 082210
            '%Y%m%d',                    # 20250827
            
            # Excel 序列号格式会由 pandas 自动处理
        ]
        
        # 预编译正则表达式用于预处理
        self.regex_patterns = [
            # 匹配类似 "2025/8/27 8:22:10.422682" 的格式，补齐前导0
            (re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2}) (\d{1,2}):(\d{1,2}):(\d{1,2})\.?(\d*)'), 
             self._normalize_datetime_parts),
            
            # 匹配日期部分，补齐前导0  
            (re.compile(r'(\d{4})/(\d{1,2})/(\d{1,2})$'), 
             self._normalize_date_parts),
        ]
    
    def _normalize_datetime_parts(self, match):
        """标准化日期时间各部分，补齐前导零"""
        year, month, day, hour, minute, second, microsecond = match.groups()
        
        # 补齐前导零
        month = month.zfill(2)
        day = day.zfill(2) 
        hour = hour.zfill(2)
        minute = minute.zfill(2)
        second = second.zfill(2)
        
        if microsecond:
            # 确保微秒部分是6位
            microsecond = microsecond.ljust(6, '0')[:6]
            return f"{year}-{month}-{day} {hour}:{minute}:{second}.{microsecond}"
        else:
            return f"{year}-{month}-{day} {hour}:{minute}:{second}"
    
    def _normalize_date_parts(self, match):
        """标准化日期各部分"""
        year, month, day = match.groups()
        month = month.zfill(2)
        day = day.zfill(2)
        return f"{year}-{month}-{day}"
    
    def _is_valid_date(self, dt: datetime) -> bool:
        """
        验证解析的日期是否有效
        
        Args:
            dt: 要验证的 datetime 对象
            
        Returns:
            如果日期有效返回 True，否则返回 False
        """
        if not isinstance(dt, (datetime, date)):
            return False
            
        # 检查年份是否合理 (1900-2100)
        if dt.year < 1900 or dt.year > 2100:
            self.logger.warning(f"日期年份超出合理范围: {dt.year}")
            return False
            
        # 检查月份是否有效 (1-12)
        if dt.month < 1 or dt.month > 12:
            self.logger.warning(f"日期月份无效: {dt.month}")
            return False
            
        # 检查日期是否有效
        try:
            # This will raise ValueError if the date is invalid (e.g., Feb 30)
            datetime(dt.year, dt.month, dt.day)
        except ValueError as e:
            self.logger.warning(f"日期无效: {dt} - {e}")
            return False
            
        return True
    
    def _preprocess_datetime_string(self, dt_str: str) -> str:
        """预处理日期时间字符串"""
        if not isinstance(dt_str, str):
            return dt_str
            
        dt_str = dt_str.strip()
        
        # 应用正则表达式预处理
        for pattern, normalizer in self.regex_patterns:
            match = pattern.match(dt_str)
            if match:
                return normalizer(match)
        
        return dt_str
    
    def parse_datetime(self, value: Any) -> Optional[datetime]:
        """
        解析单个日期时间值
        
        Args:
            value: 要解析的值，可以是字符串、数字、datetime对象等
            
        Returns:
            解析后的 datetime 对象，解析失败返回 None
        """
        if pd.isna(value) or value is None:
            return None
            
        # 如果已经是 datetime 对象
        if isinstance(value, (datetime, pd.Timestamp)):
            return value if isinstance(value, datetime) else value.to_pydatetime()
            
        # 如果是日期对象
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        
        # 如果是数字，可能是 Excel 序列号
        if isinstance(value, (int, float)):
            try:
                # Excel 日期序列号从 1900-01-01 开始计算
                # pandas 能够自动处理这种情况
                return pd.to_datetime(value, origin='1899-12-30', unit='D').to_pydatetime()
            except:
                pass
        
        # 字符串格式解析
        if isinstance(value, str):
            # 预处理字符串
            processed_str = self._preprocess_datetime_string(value)
            
            # 首先尝试 pandas 的自动解析
            try:
                result = pd.to_datetime(processed_str)
                if not pd.isna(result):
                    parsed_dt = result.to_pydatetime() if hasattr(result, 'to_pydatetime') else result
                    # Validate that the parsed date is reasonable
                    if self._is_valid_date(parsed_dt):
                        return parsed_dt
                    else:
                        self.logger.warning(f"解析的日期无效: {parsed_dt} (来自原始值: {value})")
            except Exception as e:
                self.logger.debug(f"Pandas自动解析失败: {e} (值: {value})")
                pass
            
            # 尝试各种格式模式
            for pattern in self.datetime_patterns:
                try:
                    result = datetime.strptime(processed_str, pattern)
                    # Validate that the parsed date is reasonable
                    if self._is_valid_date(result):
                        return result
                    else:
                        self.logger.warning(f"解析的日期无效: {result} (来自原始值: {value})")
                except ValueError:
                    continue
            
            # 最后尝试一些常见的变体
            try:
                # 尝试去除多余的空格
                cleaned = re.sub(r'\s+', ' ', processed_str.strip())
                result = pd.to_datetime(cleaned)
                if not pd.isna(result):
                    parsed_dt = result.to_pydatetime() if hasattr(result, 'to_pydatetime') else result
                    # Validate that the parsed date is reasonable
                    if self._is_valid_date(parsed_dt):
                        return parsed_dt
                    else:
                        self.logger.warning(f"解析的日期无效: {parsed_dt} (来自原始值: {value})")
            except Exception as e:
                self.logger.debug(f"变体解析失败: {e} (值: {value})")
                pass
        
        self.logger.warning(f"无法解析日期时间格式: {value} (类型: {type(value)})")
        return None
    
    def parse_series(self, series: pd.Series) -> pd.Series:
        """
        解析 pandas Series 中的日期时间数据
        
        Args:
            series: 包含日期时间数据的 pandas Series
            
        Returns:
            解析后的 pandas Series，失败的值为 NaT
        """
        # First, try to validate that this is actually a datetime series
        if series.empty:
            return series
            
        # Check if series likely contains datetime data
        non_null_series = series.dropna()
        if len(non_null_series) == 0:
            return series
            
        # Sample some values to see if they look like dates
        sample_size = min(5, len(non_null_series))
        sample_values = non_null_series.head(sample_size)
        
        date_like_count = 0
        for val in sample_values:
            if isinstance(val, (str, int, float)) and pd.notna(val):
                str_val = str(val)
                # Check if it contains common date indicators
                if any(indicator in str_val for indicator in ['/', '-', ':', '年', '月', '日']):
                    date_like_count += 1
            elif isinstance(val, (datetime, date, pd.Timestamp)):
                date_like_count += 1
                
        # If less than 30% look like dates, don't try to parse as datetime
        if date_like_count < sample_size * 0.3:
            self.logger.debug(f"Series 不太像日期时间数据，跳过解析: {series.name}")
            return series
            
        self.logger.debug(f"解析 Series '{series.name}' 中的日期时间数据，共 {len(series)} 个值")
        
        # 首先尝试 pandas 的批量转换
        try:
            result = pd.to_datetime(series, errors='coerce')
            # 检查是否有解析失败的值
            failed_mask = result.isna() & series.notna()
            
            if not failed_mask.any():
                self.logger.debug(f"Series 批量转换成功，共 {len(result)} 个值")
                return result
                
            # 对失败的值进行单独处理
            self.logger.info(f"批量转换失败 {failed_mask.sum()} 个值，进行单独解析")
            
        except Exception as e:
            self.logger.warning(f"批量日期时间转换失败: {e}，转为逐个解析")
            result = pd.Series([pd.NaT] * len(series), index=series.index)
            failed_mask = pd.Series([True] * len(series), index=series.index)
        
        # 对解析失败的值逐个处理
        if failed_mask.any():
            failed_indices = series[failed_mask].index
            self.logger.debug(f"单独解析 {len(failed_indices)} 个失败的值")
            
            for idx in failed_indices:
                try:
                    parsed_value = self.parse_datetime(series.loc[idx])
                    if parsed_value is not None:
                        result.loc[idx] = parsed_value
                        self.logger.debug(f"成功解析索引 {idx} 的值: {series.loc[idx]} -> {parsed_value}")
                except Exception as e:
                    self.logger.warning(f"解析索引 {idx} 的值失败: {series.loc[idx]} - {e}")
        
        return result
    
    def get_supported_formats(self) -> list:
        """获取支持的日期时间格式列表"""
        return self.datetime_patterns.copy()


# 创建全局实例
datetime_parser = FlexibleDateTimeParser()