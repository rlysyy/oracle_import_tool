"""
Oracle导入工具主命令行接口
"""

import click
import sys
from pathlib import Path
from typing import Optional, List
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .core.importer import OracleImporter
from .config.config_manager import ConfigManager
from .database.connection import DatabaseConnection
from .data.file_reader import FileReader
from .utils.progress_manager import ScanProgressDisplay, DatabaseTestProgress

# 初始化富文本控制台
console = Console()


class PathType(click.Path):
    """自定义路径类型，确保路径存在且可访问"""
    def convert(self, value, param, ctx):
        path = super().convert(value, param, ctx)
        if not Path(path).exists():
            self.fail(f"路径 '{path}' 不存在", param, ctx)
        return path


class TableListType(click.ParamType):
    """自定义表名列表类型，支持逗号分隔的表名"""
    name = "table_list"
    
    def convert(self, value, param, ctx):
        if not value:
            return []
        return [table.strip() for table in value.split(',') if table.strip()]


# 全局选项组
@click.group(invoke_without_command=True)
@click.option('--version', is_flag=True, help='显示版本信息')
@click.pass_context
def cli(ctx, version):
    """
    Oracle数据库导入工具
    
    支持从XLS/XLSX/CSV文件批量导入数据到Oracle数据库
    """
    if version:
        from . import __version__
        console.print(f"[bold blue]Oracle Import Tool v{__version__}[/bold blue]")
        return
    
    if ctx.invoked_subcommand is None:
        console.print("[bold blue]Oracle数据库导入工具[/bold blue]")
        console.print("\n使用 --help 查看可用命令")
        console.print("使用 oracle-import [COMMAND] --help 查看具体命令帮助")


# 主导入命令
@cli.command(name='import')
@click.option(
    '--datafolder', '-df',
    type=PathType(exists=True, file_okay=False, dir_okay=True),
    required=True,
    help='数据文件夹路径 (必须)'
)
@click.option(
    '--table', '-t',
    type=TableListType(),
    help='指定要导入的表名，多个表名用逗号分隔。不指定则处理文件夹中所有文件'
)
@click.option(
    '--ddl-folder', '--ddlf',
    type=PathType(exists=True, file_okay=False, dir_okay=True),
    help='DDL文件夹路径，包含表结构定义文件(SQL/MD格式)'
)
@click.option(
    '--create-sql', '--cs',
    is_flag=True,
    default=False,
    help='生成INSERT SQL文件到output目录'
)
@click.option(
    '--config', '-c',
    type=click.Path(),
    default='config.ini',
    help='配置文件路径 (默认: config.ini)'
)
@click.option(
    '--log-file', '-l',
    type=click.Path(),
    help='日志文件路径 (默认: logs/import_<timestamp>.log)'
)
@click.option(
    '--encoding', '-e',
    default='utf-8-sig',
    help='文件编码格式 (默认: utf-8-sig)'
)
@click.option(
    '--batch-size', '-b',
    type=click.IntRange(min=1, max=10000),
    default=1000,
    help='批量插入大小 (默认: 1000)'
)
@click.option(
    '--dry-run', '--dr',
    is_flag=True,
    default=False,
    help='干运行模式，只验证不实际导入'
)
@click.option(
    '--max-retries',
    type=click.IntRange(min=0, max=10),
    default=3,
    help='失败重试次数 (默认: 3)'
)
@click.option(
    '--timeout',
    type=click.IntRange(min=5, max=300),
    default=30,
    help='数据库连接超时时间(秒) (默认: 30)'
)
@click.option(
    '--verbose', '-v',
    count=True,
    help='详细输出模式 (-v: INFO, -vv: DEBUG)'
)
@click.option(
    '--quiet', '-q',
    is_flag=True,
    default=False,
    help='安静模式，只输出错误信息'
)
@click.option(
    '--no-color',
    is_flag=True,
    default=False,
    help='禁用彩色输出'
)
@click.option(
    '--keep-date-suffix',
    is_flag=True,
    default=False,
    help='保留文件名中的日期后缀作为表名的一部分'
)
def import_data(
    datafolder: str,
    table: List[str],
    ddl_folder: Optional[str],
    create_sql: bool,
    config: str,
    log_file: Optional[str],
    encoding: str,
    batch_size: int,
    dry_run: bool,
    max_retries: int,
    timeout: int,
    verbose: int,
    quiet: bool,
    no_color: bool,
    keep_date_suffix: bool
):
    """
    执行数据导入操作
    
    示例:
    \b
    # 导入指定文件夹下所有数据文件
    oracle-import import -df ./data
    
    # 导入指定表并生成SQL文件
    oracle-import import -df ./data -t table1,table2 --create-sql
    
    # 使用DDL文件定义表结构
    oracle-import import -df ./data --ddl-folder ./ddl
    
    # 干运行模式，只验证不导入
    oracle-import import -df ./data --dry-run
    
    # 保留日期后缀（适用于T_order20250822.xlsx -> T_ORDER20250822）
    oracle-import import -df ./data --keep-date-suffix
    """
    
    # 设置控制台颜色
    if no_color:
        console._color_system = None
    
    # 显示配置信息
    if not quiet:
        console.print("\\n[bold blue]Oracle数据导入工具[/bold blue]")
        console.print("=" * 50)
        
        config_table = Table(title="配置信息", show_header=False)
        config_table.add_column("参数", style="cyan", no_wrap=True)
        config_table.add_column("值", style="magenta")
        
        config_table.add_row("数据文件夹", datafolder)
        config_table.add_row("指定表名", ', '.join(table) if table else "全部文件")
        config_table.add_row("DDL文件夹", ddl_folder or "无")
        config_table.add_row("生成SQL", "是" if create_sql else "否")
        config_table.add_row("批量大小", str(batch_size))
        config_table.add_row("干运行模式", "是" if dry_run else "否")
        config_table.add_row("编码格式", encoding)
        
        console.print(config_table)
        console.print()
    
    # 执行导入
    try:
        importer = OracleImporter(config_file=config, console=console, keep_date_suffix=keep_date_suffix)
        
        # 更新配置
        importer.file_reader.encoding = encoding
        importer.import_settings['batch_size'] = batch_size
        importer.import_settings['max_retries'] = max_retries
        importer.import_settings['timeout'] = timeout
        
        # 执行导入
        result = importer.import_data(
            datafolder=datafolder,
            tables=table,
            ddl_folder=ddl_folder,
            create_sql=create_sql,
            dry_run=dry_run
        )
        
        # 显示结果
        if not quiet:
            if dry_run:
                console.print("\\n[green]干运行完成！数据验证通过[/green]")
            else:
                console.print("\\n[green]导入完成！[/green]")
        
        # 根据结果设置退出码
        if result['failed_files'] > 0 or result['failed_rows'] > 0:
            sys.exit(1)
        
    except Exception as e:
        console.print(f"\\n[red]导入失败: {e}[/red]")
        sys.exit(1)


# 配置管理命令组
@cli.group()
def config():
    """配置文件管理"""
    pass


@config.command(name='init')
@click.option(
    '--output', '-o',
    default='config.ini',
    help='配置文件输出路径 (默认: config.ini)'
)
def init_config(output: str):
    """初始化配置文件"""
    try:
        config_manager = ConfigManager(output)
        config_manager.save_config()
        
        console.print(f"[green]配置文件已创建: {output}[/green]")
        console.print("[yellow]请根据实际环境修改数据库连接信息！[/yellow]")
        
        # 显示配置内容
        config_table = Table(title="默认配置", show_header=True)
        config_table.add_column("配置项", style="cyan")
        config_table.add_column("默认值", style="magenta")
        
        db_config = config_manager.get_database_config()
        config_table.add_row("数据库主机", db_config['host'])
        config_table.add_row("端口", str(db_config['port']))
        config_table.add_row("服务名", db_config['service_name'])
        config_table.add_row("用户名", db_config['username'])
        config_table.add_row("密码", "***")
        
        console.print(config_table)
        
    except Exception as e:
        console.print(f"[red]创建配置文件失败: {e}[/red]")
        sys.exit(1)


@config.command(name='validate')
@click.argument('config_file', type=click.Path(exists=True))
def validate_config(config_file: str):
    """验证配置文件"""
    try:
        config_manager = ConfigManager(config_file)
        config_manager.validate()
        console.print(f"[green]配置文件 {config_file} 验证通过[/green]")
    except Exception as e:
        console.print(f"[red]配置文件验证失败: {e}[/red]")
        sys.exit(1)


# 数据库连接测试命令
@cli.command(name='test-db')
@click.option(
    '--config', '-c',
    type=click.Path(exists=True),
    default='config.ini',
    help='配置文件路径 (默认: config.ini)'
)
def test_database(config: str):
    """测试数据库连接"""
    def test_connection():
        config_manager = ConfigManager(config)
        db_connection = DatabaseConnection(config_manager)
        return db_connection.test_connection()
    
    try:
        progress_display = DatabaseTestProgress(console)
        result = progress_display.test_connection_with_progress(test_connection)
        
        if result:
            console.print("\\n[green]数据库连接测试成功！[/green]")
        else:
            console.print("\\n[red]数据库连接测试失败！[/red]")
            sys.exit(1)
            
    except Exception as e:
        console.print(f"\\n[red]数据库连接失败: {e}[/red]")
        sys.exit(1)


# 文件扫描命令
@cli.command(name='scan')
@click.argument(
    'folder',
    type=PathType(exists=True, file_okay=False, dir_okay=True)
)
@click.option(
    '--format', '-f',
    type=click.Choice(['table', 'simple']),
    default='table',
    help='输出格式 (默认: table)'
)
def scan_files(folder: str, format: str):
    """扫描文件夹中的数据文件"""
    def scan_directory():
        file_reader = FileReader()
        return file_reader.scan_directory(folder)
    
    try:
        progress_display = ScanProgressDisplay(console)
        files = progress_display.scan_with_progress(scan_directory)
        
        if not files:
            console.print(f"[yellow]在目录 {folder} 中未找到数据文件[/yellow]")
            return
        
        if format == 'table':
            table = Table(title=f"扫描结果: {folder}")
            table.add_column("文件名", style="cyan", max_width=30)
            table.add_column("表名", style="magenta", max_width=25)
            table.add_column("格式", style="green")
            table.add_column("大小", style="yellow", justify="right")
            
            for file_info in files:
                size_str = f"{file_info['size']:,} B"
                if file_info['size'] > 1024:
                    size_str = f"{file_info['size']/1024:.1f} KB"
                if file_info['size'] > 1024*1024:
                    size_str = f"{file_info['size']/(1024*1024):.1f} MB"
                
                table.add_row(
                    file_info['name'],
                    file_info['table_name'],
                    file_info['extension'].upper(),
                    size_str
                )
            
            console.print("\\n")
            console.print(table)
            
        else:
            console.print(f"\\n发现 {len(files)} 个文件:")
            for file_info in files:
                console.print(f"  • {file_info['name']} -> {file_info['table_name']}")
        
        console.print(f"\\n[green]总计: {len(files)} 个数据文件[/green]")
            
    except Exception as e:
        console.print(f"[red]扫描文件失败: {e}[/red]")
        sys.exit(1)


# 预览文件命令
@cli.command(name='preview')
@click.argument('file_path', type=click.Path(exists=True))
@click.option(
    '--rows', '-r',
    type=click.IntRange(min=1, max=100),
    default=5,
    help='预览行数 (默认: 5)'
)
def preview_file(file_path: str, rows: int):
    """预览数据文件内容"""
    try:
        file_reader = FileReader()
        df = file_reader.preview_file(file_path, rows)
        
        # 获取文件信息
        file_info = file_reader.get_file_info(file_path)
        stats = file_reader.get_file_stats(file_path)
        
        # 显示文件信息
        info_table = Table(title="文件信息", show_header=False)
        info_table.add_column("属性", style="cyan")
        info_table.add_column("值", style="magenta")
        
        info_table.add_row("文件名", file_info['name'])
        info_table.add_row("表名", file_info['table_name'])
        info_table.add_row("格式", file_info['extension'].upper())
        info_table.add_row("总行数", str(stats['rows']))
        info_table.add_row("总列数", str(stats['columns']))
        
        console.print(info_table)
        console.print()
        
        # 显示数据预览
        console.print(f"[bold]数据预览 (前{rows}行):[/bold]")
        
        # 转换DataFrame为Rich表格
        data_table = Table(show_header=True, header_style="bold blue")
        
        for column in df.columns:
            data_table.add_column(column, max_width=15)
        
        for _, row in df.iterrows():
            data_table.add_row(*[str(value)[:50] + "..." if len(str(value)) > 50 else str(value) for value in row])
        
        console.print(data_table)
        
    except Exception as e:
        console.print(f"[red]预览文件失败: {e}[/red]")
        sys.exit(1)


# 版本信息命令
@cli.command(name='version')
def show_version():
    """显示详细版本信息"""
    from . import __version__, __author__, __description__
    
    console.print(Panel(f"""
[bold blue]Oracle Import Tool[/bold blue]

版本: {__version__}
作者: {__author__}
描述: {__description__}

支持的Python版本: 3.8+
支持的文件格式: XLS, XLSX, CSV
支持的数据库: Oracle 12c+
    """.strip(), title="版本信息", border_style="blue"))


# 自定义异常处理
def handle_exception(exc_type, exc_value, exc_traceback):
    """全局异常处理"""
    if issubclass(exc_type, KeyboardInterrupt):
        console.print("\\n[yellow]用户中断操作[/yellow]")
        sys.exit(1)
    else:
        import traceback
        console.print(f"\\n[red]程序异常: {exc_value}[/red]")
        if console._color_system:  # 只在支持颜色时显示详细错误
            console.print("[dim]详细错误信息:[/dim]")
            console.print(traceback.format_exception(exc_type, exc_value, exc_traceback))
        sys.exit(1)


# 设置全局异常处理
sys.excepthook = handle_exception


if __name__ == '__main__':
    cli()