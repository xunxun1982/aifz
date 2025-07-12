#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AI病历分析系统 - 统一日志模块 (aifz_logger.py)

本模块提供统一的日志管理功能，支持多模块日志记录和灵活的配置管理。

主要功能：
1. 根据配置文件自动设置日志级别（DEBUG/RELEASE模式）
2. 支持按日期自动轮换的日志文件
3. 为不同模块创建独立的日志文件
4. 同时输出到控制台和文件
5. 统一的日志格式和编码
6. API调用统计功能（记录每日成功/失败次数）

技术特性：
- 自定义日志处理器，支持日期切换
- 配置驱动的日志级别管理
- UTF-8编码确保中文日志正常显示
- 防止重复添加处理器的机制
- 线程安全的API统计计数器

文件命名规则：
logs/YYYY-MM-DD_模块名.log
logs/YYYY-MM-DD_analysis.log (API统计)


"""

import logging
import os
from datetime import datetime
import configparser
import sys
import threading
from collections import defaultdict

# 全局变量
CONFIG = None
LOG_DIR = "logs"  # 日志文件存放目录

# API调用统计相关全局变量
API_STATS_LOCK = threading.Lock()
API_STATS = defaultdict(lambda: {'success': 0, 'fail': 0})  # 按日期统计
LAST_STATS_DATE = None

def _load_config():
    """
    加载系统配置文件
    
    返回：配置对象
    异常：配置文件不存在或读取失败时退出程序
    """
    global CONFIG
    if CONFIG is None:
        try:
            config = configparser.ConfigParser()
            config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
            
            if not os.path.exists(config_path):
                logging.basicConfig(level=logging.ERROR)
                logging.error(f"配置文件不存在: {config_path}")
                raise FileNotFoundError(f"配置文件不存在: {config_path}")
            
            config.read(config_path, encoding='utf-8')
            CONFIG = config
        except Exception as e:
            logging.basicConfig(level=logging.ERROR)
            logging.error(f"配置文件加载失败: {e}")
            sys.exit(1)
    return CONFIG

class DailyRotatingFileHandler(logging.Handler):
    """
    按日期自动轮换的文件日志处理器
    
    功能说明：
    1. 每天自动创建新的日志文件
    2. 文件名包含日期信息，便于归档管理
    3. 支持UTF-8编码，确保中文日志正常显示
    4. 自动创建日志目录
    
    文件格式：logs/YYYY-MM-DD_模块名.log
    例如：logs/2025-01-15_main.log
    """
    
    def __init__(self, log_dir, log_name):
        """
        初始化日志处理器
        
        参数：
        log_dir: 日志文件存放目录
        log_name: 日志文件名标识（通常是模块名）
        """
        super().__init__()
        self.log_dir = log_dir
        self.log_name = log_name
        
        # 确保日志目录存在
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            
        self.current_date = None
        self.file_handler = None
        self._update_handler()

    def _update_handler(self):
        """
        检查并更新文件处理器
        
        当日期发生变化时，自动创建新的日志文件
        关闭旧的文件处理器，防止资源泄漏
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        if self.current_date != today:
            # 关闭旧的文件处理器
            if self.file_handler:
                self.file_handler.close()
            
            # 创建新的日志文件
            log_filename = os.path.join(self.log_dir, f"{today}_{self.log_name}.log")
            self.file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
            
            # 应用格式化器
            if self.formatter:
                self.file_handler.setFormatter(self.formatter)
            
            self.current_date = today

    def emit(self, record):
        """发出日志记录"""
        self._update_handler()
        if self.file_handler:
            self.file_handler.emit(record)

    def setFormatter(self, fmt):
        """设置日志格式化器"""
        super().setFormatter(fmt)
        if self.file_handler:
            self.file_handler.setFormatter(fmt)

    def close(self):
        """关闭处理器并释放资源"""
        if self.file_handler:
            self.file_handler.close()
        super().close()

def setup_logging(log_name='main'):
    """
    配置系统日志记录器
    
    配置内容：
    1. 根据系统模式（DEBUG/RELEASE）设置日志级别
    2. 创建文件和控制台两个输出通道
    3. 设置统一的日志格式
    4. 为不同模块创建独立的日志文件
    
    参数：
    log_name: 日志文件名标识，用于区分不同模块（如'main', 'zdss'）
    
    返回：配置好的日志记录器
    """
    # 1. 加载系统配置
    config = _load_config()
    try:
        run_mode = config.get('system', 'mode', fallback='RELEASE').upper()
    except (configparser.NoSectionError, configparser.NoOptionError):
        run_mode = 'RELEASE'  # 默认为发布模式

    # 2. 根据运行模式设置日志级别
    # DEBUG模式：输出所有级别的日志，便于调试
    # RELEASE模式：只输出INFO及以上级别，减少日志量
    console_level = logging.DEBUG if run_mode == 'DEBUG' else logging.INFO
    file_level = logging.DEBUG if run_mode == 'DEBUG' else logging.INFO

    # 3. 配置根日志记录器
    logger = logging.getLogger()
    
    # 清除现有处理器，防止重复配置
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.setLevel(logging.DEBUG)  # 设置为最低级别，由处理器控制输出

    # 4. 设置统一的日志格式
    # 包含：时间戳 - 日志级别 - [模块名.函数名] - 日志消息
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s] - %(message)s')

    # 5. 创建文件日志处理器（按日期轮换）
    file_handler = DailyRotatingFileHandler(LOG_DIR, log_name)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    logger.addHandler(file_handler)

    # 6. 创建控制台日志处理器
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(console_level)
    logger.addHandler(stream_handler)

    # 7. 记录日志系统初始化信息
    logger.info(f"日志系统已初始化，模块: {log_name}，运行模式: {run_mode}")
    return logger


# =============================================================================
# API调用统计功能
# =============================================================================

def log_api_call_success():
    """记录API调用成功"""
    global API_STATS_LOCK, API_STATS, LAST_STATS_DATE
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    with API_STATS_LOCK:
        API_STATS[today]['success'] += 1
        
        # 检查是否需要写入统计日志
        if LAST_STATS_DATE != today:
            _write_api_stats_to_file()
            LAST_STATS_DATE = today

def log_api_call_failure():
    """记录API调用失败"""
    global API_STATS_LOCK, API_STATS, LAST_STATS_DATE
    
    today = datetime.now().strftime('%Y-%m-%d')
    
    with API_STATS_LOCK:
        API_STATS[today]['fail'] += 1
        
        # 检查是否需要写入统计日志
        if LAST_STATS_DATE != today:
            _write_api_stats_to_file()
            LAST_STATS_DATE = today

def force_write_api_stats():
    """强制写入API统计信息（程序退出时调用）"""
    global API_STATS_LOCK
    
    with API_STATS_LOCK:
        _write_api_stats_to_file()

def _write_api_stats_to_file():
    """内部函数：将API统计信息写入日志文件"""
    try:
        # 确保日志目录存在
        os.makedirs(LOG_DIR, exist_ok=True)
        
        # 为每个日期写入统计信息
        for date, stats in API_STATS.items():
            if stats['success'] > 0 or stats['fail'] > 0:
                log_filename = f"{date}_analysis.log"
                log_path = os.path.join(LOG_DIR, log_filename)
                
                # 读取现有统计（如果存在）
                existing_success = 0
                existing_fail = 0
                
                if os.path.exists(log_path):
                    try:
                        with open(log_path, 'r', encoding='utf-8') as f:
                            content = f.read()
                            # 解析现有统计
                            import re
                            success_match = re.search(r'成功: (\d+)', content)
                            fail_match = re.search(r'失败: (\d+)', content)
                            
                            if success_match:
                                existing_success = int(success_match.group(1))
                            if fail_match:
                                existing_fail = int(fail_match.group(1))
                    except Exception:
                        pass  # 如果读取失败，使用默认值0
                
                # 累加统计
                total_success = existing_success + stats['success']
                total_fail = existing_fail + stats['fail']
                total_calls = total_success + total_fail
                
                # 写入统计信息
                with open(log_path, 'w', encoding='utf-8') as f:
                    f.write(f"=== API调用统计 - {date} ===\n")
                    f.write(f"总调用次数: {total_calls}\n")
                    f.write(f"成功: {total_success}\n")
                    f.write(f"失败: {total_fail}\n")
                    f.write(f"成功率: {(total_success/total_calls*100):.2f}%\n")
                    f.write(f"最后更新: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                
                # 清空已写入的统计
                stats['success'] = 0
                stats['fail'] = 0
                
    except Exception as e:
        # 统计日志写入失败不应该影响主程序
        print(f"写入API统计日志时出错: {e}")


def get_api_stats(date_str=None):
    """
    获取API调用统计信息
    
    参数：
    date_str: 日期字符串（YYYY-MM-DD格式），如果为None则返回今天的统计
    
    返回：包含success和fail计数的字典
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    with API_STATS_LOCK:
        return dict(API_STATS[date_str]) 