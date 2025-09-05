"""进度条管理器"""

from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    MofNCompleteColumn,
    TransferSpeedColumn
)
from rich.table import Table
from rich.panel import Panel
import time
from typing import Optional, Dict, List
from dataclasses import dataclass, field
import logging


@dataclass
class ImportStats:
    """导入统计信息"""
    total_files: int = 0
    processed_files: int = 0
    total_rows: int = 0
    processed_rows: int = 0
    success_rows: int = 0
    failed_rows: int = 0
    current_file: str = ""
    current_table: str = ""
    start_time: float = field(default_factory=time.time)
    failed_files: List[str] = field(default_factory=list)
    error_details: List[Dict] = field(default_factory=list)


class ProgressBarManager:
    """进度条管理器 - 管理多层次进度显示"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
        self.stats = ImportStats()
        self.progress = None
        self.main_task = None
        self.file_task = None
        self.batch_task = None
        self.logger = logging.getLogger(__name__)
    
    def create_progress_display(self) -> Progress:
        """创建多层进度条显示"""
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            TransferSpeedColumn(),
            console=self.console,
            refresh_per_second=4
        )
    
    def start_import_progress(self, total_files: int, estimated_total_rows: int = 0):
        """开始导入进度显示"""
        self.stats = ImportStats()
        self.stats.total_files = total_files
        self.stats.total_rows = estimated_total_rows
        self.stats.start_time = time.time()
        
        self.progress = self.create_progress_display()
        self.progress.start()
        
        # 主进度条 - 文件级别
        self.main_task = self.progress.add_task(
            "[cyan]总体进度",
            total=total_files
        )
        
        # 当前文件进度条
        self.file_task = self.progress.add_task(
            "[green]当前文件",
            total=100,
            visible=False
        )
        
        # 批量插入进度条
        self.batch_task = self.progress.add_task(
            "[yellow]批量插入",
            total=100,
            visible=False
        )
    
    def update_file_progress(self, filename: str, table_name: str, total_rows: int):
        """更新文件级别进度"""
        self.stats.current_file = filename
        self.stats.current_table = table_name
        
        # 更新文件进度条
        self.progress.update(
            self.file_task,
            description=f"[green]处理文件: {filename} -> {table_name}",
            total=total_rows,
            completed=0,
            visible=True
        )
        
        self.logger.info(f"开始处理文件: {filename} -> {table_name}, 总行数: {total_rows}")
    
    def update_row_progress(self, processed_rows: int):
        """更新行级别进度"""
        # 更新文件进度
        self.progress.update(
            self.file_task,
            advance=processed_rows
        )
        
        self.stats.processed_rows += processed_rows
    
    def update_batch_progress(self, batch_number: int, total_batches: int, batch_size: int, success_count: int, failed_count: int):
        """更新批量插入进度"""
        progress_percent = (batch_number / total_batches) * 100
        
        self.progress.update(
            self.batch_task,
            description=f"[yellow]批量插入: 第{batch_number}/{total_batches}批 ({batch_size}行) 成功:{success_count} 失败:{failed_count}",
            completed=progress_percent,
            visible=True
        )
        
        # 更新统计
        self.stats.success_rows += success_count
        self.stats.failed_rows += failed_count
    
    def complete_file_progress(self, success: bool = True, error_msg: str = None):
        """完成文件处理"""
        self.stats.processed_files += 1
        
        if not success:
            self.stats.failed_files.append(self.stats.current_file)
            if error_msg:
                self.stats.error_details.append({
                    'file': self.stats.current_file,
                    'table': self.stats.current_table,
                    'error': error_msg,
                    'time': time.time()
                })
        
        # 更新主进度
        self.progress.update(
            self.main_task,
            advance=1
        )
        
        # 隐藏文件和批量进度条
        self.progress.update(self.file_task, visible=False)
        self.progress.update(self.batch_task, visible=False)
        
        status = "成功" if success else "失败"
        self.logger.info(f"文件处理{status}: {self.stats.current_file}")
    
    def add_error(self, error_msg: str, file_name: str = None, table_name: str = None):
        """添加错误信息"""
        self.stats.error_details.append({
            'file': file_name or self.stats.current_file,
            'table': table_name or self.stats.current_table,
            'error': error_msg,
            'time': time.time()
        })
    
    def finish_import_progress(self):
        """完成导入进度显示"""
        if self.progress:
            self.progress.stop()
    
    def create_summary_table(self) -> Table:
        """创建导入总结表格"""
        table = Table(title="导入完成总结", show_header=True, header_style="bold blue")
        table.add_column("项目", style="cyan", no_wrap=True)
        table.add_column("数量", style="magenta", justify="right")
        table.add_column("状态", style="green")
        
        # 计算用时
        elapsed_time = time.time() - self.stats.start_time
        success_files = self.stats.processed_files - len(self.stats.failed_files)
        
        table.add_row("处理文件数", str(self.stats.processed_files), "📁")
        table.add_row("成功文件数", str(success_files), "✅")
        table.add_row("失败文件数", str(len(self.stats.failed_files)), "❌" if len(self.stats.failed_files) > 0 else "✅")
        table.add_row("总行数", f"{self.stats.processed_rows:,}", "📝")
        table.add_row("成功行数", f"{self.stats.success_rows:,}", "✅")
        table.add_row("失败行数", f"{self.stats.failed_rows:,}", "❌" if self.stats.failed_rows > 0 else "✅")
        table.add_row("用时", f"{elapsed_time:.2f}秒", "⏱️")
        
        if elapsed_time > 0 and self.stats.processed_rows > 0:
            table.add_row("平均速度", f"{self.stats.processed_rows/elapsed_time:.0f} 行/秒", "🚀")
        
        return table
    
    def create_error_summary(self) -> Optional[Table]:
        """创建错误总结表格"""
        if not self.stats.error_details:
            return None
        
        table = Table(title="错误详情", show_header=True, header_style="bold red")
        table.add_column("文件", style="cyan", no_wrap=True, max_width=30)
        table.add_column("表名", style="yellow", no_wrap=True, max_width=20)
        table.add_column("错误信息", style="red", max_width=50)
        
        # 只显示最近的10个错误
        recent_errors = self.stats.error_details[-10:]
        
        for error in recent_errors:
            table.add_row(
                error.get('file', 'Unknown'),
                error.get('table', 'Unknown'),
                str(error.get('error', ''))[:50] + "..." if len(str(error.get('error', ''))) > 50 else str(error.get('error', ''))
            )
        
        if len(self.stats.error_details) > 10:
            table.add_row("...", "...", f"还有 {len(self.stats.error_details) - 10} 个错误未显示")
        
        return table
    
    def print_summary(self):
        """打印完整的总结信息"""
        self.console.print("\n")
        
        # 主要统计信息
        summary_table = self.create_summary_table()
        self.console.print(Panel(summary_table, title="导入结果", border_style="green"))
        
        # 错误信息（如果有）
        if self.stats.error_details:
            self.console.print("\n")
            error_table = self.create_error_summary()
            if error_table:
                self.console.print(Panel(error_table, title="错误详情", border_style="red"))
        
        # 失败的文件列表
        if self.stats.failed_files:
            self.console.print("\n[red]失败的文件:[/red]")
            for failed_file in self.stats.failed_files:
                self.console.print(f"  • {failed_file}")
    
    def get_stats(self) -> ImportStats:
        """获取当前统计信息"""
        return self.stats


class ScanProgressDisplay:
    """文件扫描进度显示"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
    
    def scan_with_progress(self, scan_function, *args, **kwargs):
        """带进度显示的扫描操作"""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            task = progress.add_task("[cyan]扫描文件中...", total=None)
            
            try:
                result = scan_function(*args, **kwargs)
                progress.update(task, description="[green]扫描完成!")
                return result
            except Exception as e:
                progress.update(task, description=f"[red]扫描失败: {str(e)}")
                raise


class DatabaseTestProgress:
    """数据库连接测试进度显示"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
    
    def test_connection_with_progress(self, test_function, *args, **kwargs):
        """带进度显示的数据库连接测试"""
        steps = [
            "读取配置文件",
            "建立数据库连接", 
            "验证连接状态",
            "测试查询权限",
            "连接测试完成"
        ]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            
            task = progress.add_task("[cyan]数据库连接测试", total=len(steps))
            
            for i, step in enumerate(steps):
                progress.update(task, description=f"[cyan]{step}...")
                
                if i == 1:  # 在建立连接步骤执行实际测试
                    try:
                        result = test_function(*args, **kwargs)
                        if not result:
                            raise Exception("连接测试失败")
                    except Exception as e:
                        progress.update(task, description=f"[red]连接失败: {str(e)}")
                        raise
                
                time.sleep(0.5)  # 模拟处理时间
                progress.advance(task)
            
            progress.update(task, description="[green]数据库连接正常!")
            return True