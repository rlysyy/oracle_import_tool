"""表头检测工具"""

import re
from typing import List, Dict, Any
from enum import Enum


class HeaderDetectionMode(Enum):
    """表头检测模式"""
    AUTO = "auto"              # 自动检测
    FORCE_HEADER = "force_header"        # 强制认为有表头
    FORCE_NO_HEADER = "force_no_header"  # 强制认为无表头


class HeaderDetector:
    """表头检测器"""
    
    def __init__(self, header_config: Dict[str, Any]):
        """
        初始化表头检测器
        
        Args:
            header_config: 表头检测配置
                - header_keywords: 关键词字符串，支持AND/OR逻辑
                - header_detection_mode: 检测模式 (auto/force_header/force_no_header)
        """
        self.header_keywords = header_config.get('header_keywords', '').strip()
        self.detection_mode = HeaderDetectionMode(
            header_config.get('header_detection_mode', 'auto')
        )
        
        # 解析关键词表达式
        self.keyword_groups = self._parse_header_keywords(self.header_keywords)
        
    def _parse_header_keywords(self, keywords_str: str) -> List[Dict[str, Any]]:
        """
        解析表头关键词表达式
        
        支持的格式:
        - CREATED_BY,CREATE_TIMESTAMP  (AND关系，都必须包含)
        - CREATE_TIMESTAMP|CREATED_BY  (OR关系，包含任意一个)
        - id,name|code,type            (混合关系：(id AND name) OR (code AND type))
        
        Args:
            keywords_str: 关键词表达式字符串
            
        Returns:
            解析后的关键词组列表，每组包含type(and/or)和keywords列表
        """
        if not keywords_str:
            return []
        
        groups = []
        
        # 按|分割得到OR组
        or_groups = keywords_str.split('|')
        
        for group in or_groups:
            group = group.strip()
            if not group:
                continue
                
            # 按逗号分割得到AND关键词
            and_keywords = [kw.strip().upper() for kw in group.split(',') if kw.strip()]
            
            if and_keywords:
                groups.append({
                    'type': 'and',
                    'keywords': and_keywords
                })
        
        # 如果只有一个组，根据原始字符串判断是AND还是OR
        if len(groups) == 1 and '|' not in keywords_str:
            if ',' in keywords_str:
                groups[0]['type'] = 'and'  # 逗号分隔表示AND关系
            else:
                groups[0]['type'] = 'or'   # 单个关键词
                
        return groups
    
    def detect_header(self, columns: List[str]) -> bool:
        """
        检测是否包含表头
        
        Args:
            columns: 列名列表
            
        Returns:
            True表示包含表头，False表示不包含表头
        """
        # 强制模式
        if self.detection_mode == HeaderDetectionMode.FORCE_HEADER:
            return True
        elif self.detection_mode == HeaderDetectionMode.FORCE_NO_HEADER:
            return False
        
        # 自动检测模式
        if self.keyword_groups:
            # 使用配置的关键词进行检测
            return self._check_keywords_match(columns)
        else:
            # 使用默认检测逻辑
            return self._default_header_detection(columns)
    
    def _check_keywords_match(self, columns: List[str]) -> bool:
        """
        检查关键词是否匹配
        
        Args:
            columns: 列名列表
            
        Returns:
            是否匹配关键词规则
        """
        # 将列名转为大写便于比较
        upper_columns = [col.upper() for col in columns]
        
        # 检查每个关键词组（OR关系）
        for group in self.keyword_groups:
            if group['type'] == 'and':
                # AND关系：所有关键词都必须存在
                if all(keyword in upper_columns for keyword in group['keywords']):
                    return True
            else:  # or
                # OR关系：任意关键词存在即可
                if any(keyword in upper_columns for keyword in group['keywords']):
                    return True
        
        return False
    
    def _default_header_detection(self, columns: List[str]) -> bool:
        """
        默认表头检测逻辑
        
        Args:
            columns: 列名列表
            
        Returns:
            是否看起来像表头
        """
        # 常见的表头关键词
        common_header_keywords = [
            'ID', 'NAME', 'CODE', 'TYPE', 'STATUS', 'DATE', 'TIME',
            'EMAIL', 'PHONE', 'ADDRESS', 'TITLE', 'DESCRIPTION', 'VALUE',
            'CREATED_BY', 'CREATE_TIMESTAMP', 'UPDATED_BY', 'UPDATE_TIMESTAMP',
            '编号', '姓名', '名称', '代码', '类型', '状态', '日期', '时间',
            '邮箱', '电话', '地址', '标题', '描述', '数值'
        ]
        
        # 检查列名
        for col in columns:
            col_str = str(col).strip().upper()
            
            # 如果列名包含常见的表头关键词
            for keyword in common_header_keywords:
                if keyword in col_str:
                    return True
            
            # 如果列名看起来像字段名（包含下划线）
            if '_' in col_str:
                return True
            
            # 如果列名是纯英文字母且长度合理
            if col_str.isalpha() and 2 < len(col_str) < 20:
                return True
        
        # 检查是否看起来像数据行
        looks_like_data = self._looks_like_data_row(columns)
        return not looks_like_data
    
    def _looks_like_data_row(self, columns: List[str]) -> bool:
        """
        检查是否看起来像数据行
        
        Args:
            columns: 列名列表
            
        Returns:
            是否看起来像数据行
        """
        data_patterns = 0
        
        for col in columns:
            col_str = str(col).strip()
            
            # 数字
            if col_str.isdigit():
                data_patterns += 1
            # 小数
            elif re.match(r'^\d+\.\d+$', col_str):
                data_patterns += 1
            # 邮箱格式
            elif '@' in col_str and '.' in col_str:
                data_patterns += 1
            # 日期格式 (YYYY-MM-DD, MM/DD/YYYY等)
            elif re.match(r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$', col_str):
                data_patterns += 1
            # 中文姓名 (1-4个中文字符)
            elif re.match(r'^[\u4e00-\u9fff]{1,4}$', col_str):
                data_patterns += 1
            # 电话号码格式
            elif re.match(r'^[\d\-\+\(\)\s]{7,}$', col_str):
                data_patterns += 1
        
        # 如果超过一半的列看起来像数据，则认为是数据行
        return data_patterns > len(columns) / 2
    
    def get_detection_summary(self) -> str:
        """
        获取检测配置摘要
        
        Returns:
            检测配置的文字描述
        """
        if self.detection_mode == HeaderDetectionMode.FORCE_HEADER:
            return "强制表头模式：总是认为第一行是表头"
        elif self.detection_mode == HeaderDetectionMode.FORCE_NO_HEADER:
            return "强制无表头模式：总是认为第一行是数据"
        
        if self.keyword_groups:
            descriptions = []
            for group in self.keyword_groups:
                keywords_str = ", ".join(group['keywords'])
                if group['type'] == 'and':
                    descriptions.append(f"同时包含: {keywords_str}")
                else:
                    descriptions.append(f"包含任一: {keywords_str}")
            
            return f"关键词检测模式：{' OR '.join(descriptions)}"
        else:
            return "自动检测模式：使用默认检测逻辑"