"""测试文件读取器"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

from oracle_import_tool.data.file_reader import FileReader


class TestFileReader:
    """文件读取器测试"""
    
    @pytest.fixture
    def temp_csv_file(self):
        """创建临时CSV文件"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("id,name,age,email\n")
            f.write("1,张三,25,zhangsan@example.com\n")
            f.write("2,李四,30,lisi@example.com\n")
            f.write("3,王五,28,wangwu@example.com\n")
            temp_file = f.name
        
        yield temp_file
        
        try:
            os.unlink(temp_file)
        except:
            pass
    
    @pytest.fixture
    def temp_excel_file(self):
        """创建临时Excel文件"""
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            temp_file = f.name
        
        # 创建测试数据
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['张三', '李四', '王五'],
            'age': [25, 30, 28],
            'email': ['zhangsan@example.com', 'lisi@example.com', 'wangwu@example.com']
        })
        
        df.to_excel(temp_file, index=False)
        
        yield temp_file
        
        try:
            os.unlink(temp_file)
        except:
            pass
    
    @pytest.fixture
    def temp_directory(self, temp_csv_file, temp_excel_file):
        """创建临时目录包含测试文件"""
        temp_dir = tempfile.mkdtemp()
        
        # 复制文件到临时目录
        csv_dest = os.path.join(temp_dir, 'test_table.csv')
        excel_dest = os.path.join(temp_dir, 'another_table.xlsx')
        
        import shutil
        shutil.copy2(temp_csv_file, csv_dest)
        shutil.copy2(temp_excel_file, excel_dest)
        
        yield temp_dir
        
        try:
            shutil.rmtree(temp_dir)
        except:
            pass
    
    def test_read_csv_file(self, temp_csv_file):
        """测试读取CSV文件"""
        file_reader = FileReader()
        df = file_reader.read_file(temp_csv_file)
        
        assert not df.empty
        assert len(df) == 3
        assert 'ID' in df.columns
        assert 'NAME' in df.columns
        assert df.iloc[0]['NAME'] == '张三'
        
    def test_read_excel_file(self, temp_excel_file):
        """测试读取Excel文件"""
        file_reader = FileReader()
        df = file_reader.read_file(temp_excel_file)
        
        assert not df.empty
        assert len(df) == 3
        assert 'ID' in df.columns
        assert 'NAME' in df.columns
        
    def test_scan_directory(self, temp_directory):
        """测试扫描目录"""
        file_reader = FileReader()
        files = file_reader.scan_directory(temp_directory)
        
        assert len(files) == 2
        
        # 检查文件信息
        file_names = [f['name'] for f in files]
        assert 'test_table.csv' in file_names
        assert 'another_table.xlsx' in file_names
        
        # 检查表名推断
        table_names = [f['table_name'] for f in files]
        assert 'TEST_TABLE' in table_names
        assert 'ANOTHER_TABLE' in table_names
        
    def test_get_file_info(self, temp_csv_file):
        """测试获取文件信息"""
        file_reader = FileReader()
        info = file_reader.get_file_info(temp_csv_file)
        
        assert 'name' in info
        assert 'path' in info
        assert 'extension' in info
        assert 'table_name' in info
        assert 'size' in info
        
        assert info['extension'] == '.csv'
        
    def test_get_file_stats(self, temp_csv_file):
        """测试获取文件统计"""
        file_reader = FileReader()
        stats = file_reader.get_file_stats(temp_csv_file)
        
        assert 'rows' in stats
        assert 'columns' in stats
        assert stats['rows'] == 3
        assert stats['columns'] == 4
        
    def test_preview_file(self, temp_csv_file):
        """测试预览文件"""
        file_reader = FileReader()
        df = file_reader.preview_file(temp_csv_file, rows=2)
        
        assert len(df) == 2
        assert list(df.columns) == ['ID', 'NAME', 'AGE', 'EMAIL']
        
    def test_unsupported_file_format(self):
        """测试不支持的文件格式"""
        with tempfile.NamedTemporaryFile(suffix='.txt') as f:
            f.write(b"test content")
            f.flush()
            
            file_reader = FileReader()
            with pytest.raises(ValueError, match="不支持的文件格式"):
                file_reader.read_file(f.name)
                
    def test_nonexistent_file(self):
        """测试不存在的文件"""
        file_reader = FileReader()
        with pytest.raises(FileNotFoundError):
            file_reader.read_file("nonexistent.csv")
            
    def test_empty_directory(self):
        """测试空目录"""
        temp_dir = tempfile.mkdtemp()
        
        try:
            file_reader = FileReader()
            files = file_reader.scan_directory(temp_dir)
            assert len(files) == 0
            
        finally:
            os.rmdir(temp_dir)