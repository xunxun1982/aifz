#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AI病历智能分析系统 - 主程序 (aifz_main.py)

本程序是一个基于AI大模型的医疗病历自动化分析系统，主要功能包括：

核心功能：
1. 从数据库自动获取待处理的病历数据
2. 调用AI大模型接口对病历内容进行智能分析
3. 将AI分析结果结构化存储到数据库
4. 提取分析结果中的诊断和手术信息并标准化保存
5. 支持多线程并发处理，提高处理效率
6. 支持定时循环执行和单次执行模式

技术特性：
- 多API密钥轮换和负载均衡
- 智能重试和错误处理机制
- 数据库事务管理和死锁预防
- 线程池并发处理
- 实时日志记录和监控
- Docker容器化部署支持

运行模式：
- 定时模式：每5分钟自动检查并处理新的病历数据
- 单次模式：通过命令行参数指定特定病历进行处理
- 立即模式：立即执行一轮完整的处理流程


"""

import pymssql
import requests
import logging
from datetime import datetime, timedelta
import time
import os
import re
import argparse
import configparser
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import signal
import random
from collections import deque
import threading

# 导入自定义模块
from aifz_logger import setup_logging, log_api_call_success, log_api_call_failure  # 统一日志配置模块
from aifz_zdss_extract import reprocess_and_save_syxh_list  # 诊断手术提取模块

# 初始化日志系统
# 必须在所有其他操作之前配置日志，确保从程序启动开始就能记录所有日志信息
setup_logging('main')

# =============================================================================
# 全局配置常量
# =============================================================================

RETRY_DELAY = 5  # API调用失败时的默认重试延迟（秒）
SHUTDOWN_FLAG = threading.Event()  # 全局优雅关闭标志
last_maintenance_time = datetime.min  # 上次维护任务执行时间

# 全局配置容器
API_MANAGERS = {}  # API密钥管理器字典，键为组名，值为ApiKeyManager实例
ENABLED_API_GROUPS = []  # 启用的API组配置列表

class ApiLogicError(Exception):
    """API逻辑错误异常类，用于处理API返回成功状态但包含错误信息的情况"""
    pass

# =============================================================================
# API密钥管理器 - 核心组件
# =============================================================================

class ApiKeyManager:
    """
    线程安全的API密钥管理器
    
    功能特性：
    1. 支持多密钥轮换使用，避免单个密钥过载
    2. 智能限速：控制每个密钥的最小使用间隔
    3. 错误处理：自动暂停出现过多429错误的密钥
    4. 线程安全：支持多线程并发访问
    5. 随机选择：从可用密钥中随机选择，实现负载均衡
    
    使用场景：
    - 防止API限流（429错误）
    - 提高API调用成功率
    - 实现多密钥负载均衡
    """
    
    def __init__(self, api_keys, min_interval_seconds=10, error_threshold=3, 
                 error_window_minutes=30, pause_duration_minutes=60):
        """
        初始化API密钥管理器
        
        参数：
        api_keys: API密钥列表
        min_interval_seconds: 同一密钥两次使用的最小间隔（秒）
        error_threshold: 暂停密钥的错误次数阈值
        error_window_minutes: 错误统计时间窗口（分钟）
        pause_duration_minutes: 密钥暂停时长（分钟）
        """
        self._keys = api_keys
        self._lock = threading.Lock()  # 线程锁，确保线程安全
        
        # 为每个密钥初始化状态跟踪信息
        self._key_states = {
            key: {
                'in_use': False,  # 是否正在使用
                'last_used': datetime.min,  # 最后使用时间
                'paused_until': None,  # 暂停到的时间点
                'error_timestamps': deque()  # 错误时间戳队列
            } 
            for key in self._keys
        }
        
        # 配置参数
        self._min_interval = timedelta(seconds=min_interval_seconds)
        self._error_threshold = error_threshold
        self._error_window = timedelta(minutes=error_window_minutes)
        self._pause_duration = timedelta(minutes=pause_duration_minutes)

    def get_key(self):
        """
        获取一个可用的API密钥
        
        算法逻辑：
        1. 检查所有密钥的状态（使用中、冷却期、暂停状态）
        2. 筛选出当前可用的密钥列表
        3. 从可用列表中随机选择一个密钥
        4. 标记该密钥为使用中状态
        5. 如果没有可用密钥，等待1秒后重试
        
        返回：可用的API密钥字符串
        """
        while True:
            with self._lock:
                now = datetime.now()
                available_keys = []
                
                # 遍历所有密钥，筛选可用的
                for key, state in self._key_states.items():
                    # 检查密钥是否被暂停
                    is_paused = state['paused_until'] and now < state['paused_until']
                    
                    # 检查密钥可用性：未使用 + 过了冷却期 + 未被暂停
                    if (not state['in_use'] and 
                        (now - state['last_used']) >= self._min_interval and 
                        not is_paused):
                        available_keys.append(key)
                
                if available_keys:
                    # 随机选择一个可用密钥（负载均衡）
                    key_to_use = random.choice(available_keys)
                    self._key_states[key_to_use]['in_use'] = True
                    logging.debug(f"线程 {threading.get_ident()} 获取API密钥: ...{key_to_use[-4:]}")
                    return key_to_use

            # 没有可用密钥时，短暂等待避免CPU空转
            time.sleep(1)

    def release_key(self, key):
        """
        释放正在使用的密钥
        
        参数：
        key: 要释放的API密钥
        """
        with self._lock:
            if key in self._key_states:
                self._key_states[key]['in_use'] = False
                self._key_states[key]['last_used'] = datetime.now()
                logging.debug(f"线程 {threading.get_ident()} 释放API密钥: ...{key[-4:]}")

    def handle_429_error(self, key):
        """
        处理API密钥的429（请求过多）错误
        
        错误处理逻辑：
        1. 记录错误发生时间
        2. 清理时间窗口外的旧错误记录
        3. 检查窗口内错误次数是否超过阈值
        4. 如果超过阈值，暂停该密钥一段时间
        
        参数：
        key: 出现429错误的API密钥
        """
        with self._lock:
            if key not in self._key_states:
                return

            now = datetime.now()
            state = self._key_states[key]
            
            # 记录错误时间戳
            state['error_timestamps'].append(now)
            
            # 移除时间窗口外的旧错误记录
            window_start_time = now - self._error_window
            while state['error_timestamps'] and state['error_timestamps'][0] < window_start_time:
                state['error_timestamps'].popleft()
            
            # 检查错误频率，决定是否暂停密钥
            if len(state['error_timestamps']) >= self._error_threshold:
                pause_until = now + self._pause_duration
                state['paused_until'] = pause_until
                state['error_timestamps'].clear()  # 清空错误记录，重新计数
                
                logging.critical(
                    f"API密钥 ...{key[-4:]} 在 {self._error_window.total_seconds()/60:.0f} 分钟内"
                    f"收到 {self._error_threshold} 次429错误，暂停 {self._pause_duration.total_seconds()/60:.0f} 分钟"
                    f"至 {pause_until.strftime('%Y-%m-%d %H:%M:%S')}"
                )

# =============================================================================
# 系统初始化和配置加载
# =============================================================================

def check_and_install_packages():
    """
    自动检查并安装Python依赖包
    
    功能说明：
    1. 读取requirements.txt文件
    2. 检查当前环境中已安装的包
    3. 找出缺失的依赖包
    4. 使用多个镜像源尝试安装缺失的包
    
    镜像源优先级：清华源 > 阿里源 > 官方源
    """
    try:
        # 导入包元数据模块，用于检查已安装的包
        import importlib.metadata as importlib_metadata
    except ImportError:
        # 兼容低版本Python
        logging.warning("当前Python版本较低，正在安装 'importlib-metadata'...")
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'importlib-metadata'])
            import importlib.metadata as importlib_metadata
            logging.info("已成功安装 'importlib-metadata'")
        except Exception as e:
            logging.error(f"无法安装 'importlib-metadata'：{e}")
            sys.exit(1)

    try:
        # 读取依赖文件
        with open('requirements.txt', 'r', encoding='utf-8') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        # 获取已安装包的列表
        installed_packages = {dist.metadata['Name'].lower() for dist in importlib_metadata.distributions()}
        
        # 找出缺失的包
        missing = []
        for req in requirements:
            req_name = re.split(r'[=<>~]', req)[0].strip().lower()
            if req_name not in installed_packages:
                missing.append(req)

        if missing:
            logging.warning(f"检测到缺失依赖包: {', '.join(missing)}，开始自动安装...")
            python_executable = sys.executable
            
            # 配置镜像源
            mirrors = [
                '-i', 'https://pypi.tuna.tsinghua.edu.cn/simple',
                '--extra-index-url', 'http://mirrors.aliyun.com/pypi/simple/'
            ]
            
            # 优先使用国内镜像源
            try:
                logging.info("正在从清华源和阿里源安装依赖...")
                subprocess.check_call([python_executable, '-m', 'pip', 'install', *missing, *mirrors])
                logging.info("依赖包安装完成")
            except subprocess.CalledProcessError:
                # 镜像源失败时使用官方源
                logging.warning("镜像源安装失败，尝试官方源...")
                try:
                    subprocess.check_call([python_executable, '-m', 'pip', 'install', *missing])
                    logging.info("依赖包安装完成")
                except subprocess.CalledProcessError as e:
                    logging.error(f"依赖包安装失败，请手动执行: pip install -r requirements.txt")
                    sys.exit(1)
        else:
            logging.info("所有依赖包均已安装")
            
    except FileNotFoundError:
        logging.error("requirements.txt 文件未找到")
        sys.exit(1)
    except Exception as e:
        logging.error(f"检查依赖时发生错误: {e}", exc_info=True)
        sys.exit(1)

def load_config():
    """
    从config.ini文件加载系统配置
    
    配置项包括：
    - 数据库连接信息
    - API组配置（支持多个API服务商）
    - 代理设置
    - 线程配置
    - 系统运行模式
    
    返回：包含所有配置的字典
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    
    if not os.path.exists(config_path):
        logging.error(f"配置文件不存在: {config_path}")
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    config.read(config_path, encoding='utf-8')
    
    try:
        # 加载数据库配置，进行类型转换
        db_config = {}
        db_section = config['database']
        
        # 字符串参数直接复制
        for key in ['server', 'user', 'password', 'database', 'charset']:
            if key in db_section:
                db_config[key] = db_section[key]
        
        # 整数参数转换
        for key in ['timeout', 'login_timeout', 'arraysize']:
            if key in db_section:
                try:
                    db_config[key] = int(db_section[key])
                except ValueError:
                    logging.warning(f"数据库配置 '{key}' 值无法转换为整数，使用默认值")
        
        # 布尔参数转换
        for key in ['as_dict', 'autocommit']:
            if key in db_section:
                db_config[key] = db_section[key].lower() in ('true', '1', 'yes', 'on')
        
        # 加载API组配置（支持多API服务商）
        api_groups = []
        for section in config.sections():
            if section.startswith('api_group_') and config.getboolean(section, 'enabled', fallback=False):
                group_config = dict(config[section])
                group_config['name'] = section
                
                # 验证API密钥配置
                if not group_config.get('api_keys', '').strip():
                    logging.warning(f"API组 '{section}' 已启用但未配置密钥，将被忽略")
                    continue
                
                api_groups.append(group_config)
        
        if not api_groups:
            logging.critical("未找到任何启用的API组，请检查config.ini配置")
            sys.exit(2)

        # 加载代理配置
        proxies = {}
        proxy_enabled = True
        if 'proxy' in config:
            proxy_section = config['proxy']
            proxy_enabled = proxy_section.get('enabled', 'true').lower() == 'true'
            http_proxy = proxy_section.get('http_proxy')
            https_proxy = proxy_section.get('https_proxy')
            if proxy_enabled:
                if http_proxy:
                    proxies['http'] = http_proxy
                if https_proxy:
                    proxies['https'] = https_proxy
                    
        # 将代理配置应用到所有API组
        for group in api_groups:
            group['proxies'] = proxies if proxy_enabled else {}

        # 组装最终配置
        result_config = {
            'database': db_config, 
            'api_groups': api_groups
        }
        
        # 加载线程配置（包含类型转换）
        if 'thread' in config:
            thread_config = {}
            thread_section = config['thread']
            
            # 整数参数转换
            for key in ['max_workers', 'min_delay', 'max_delay']:
                if key in thread_section:
                    try:
                        thread_config[key] = int(thread_section[key])
                    except ValueError:
                        logging.warning(f"线程配置 '{key}' 值无法转换为整数，使用默认值")
                        # 设置默认值
                        defaults = {'max_workers': 10, 'min_delay': 0, 'max_delay': 20}
                        thread_config[key] = defaults[key]
            
            result_config['thread'] = thread_config

        # 加载系统配置
        if 'system' in config:
            result_config['system'] = dict(config['system'])
        else:
            result_config['system'] = {'mode': 'RELEASE'}  # 默认发布模式

        return result_config
        
    except KeyError as e:
        logging.error(f"配置文件中缺少必要配置: {e}")
        raise

# 全局配置初始化
try:
    APP_CONFIG = load_config()
    DB_CONFIG = APP_CONFIG['database']
    ENABLED_API_GROUPS = APP_CONFIG['api_groups']
    SYSTEM_CONFIG = APP_CONFIG['system']
    
    # 为每个启用的API组创建密钥管理器
    for group in ENABLED_API_GROUPS:
        group_name = group['name']
        api_keys_list = [key.strip() for key in group.get('api_keys', '').split(',') if key.strip()]
        API_MANAGERS[group_name] = ApiKeyManager(api_keys_list)
        logging.info(f"API组 '{group_name}' 已加载，包含 {len(api_keys_list)} 个密钥")

    # 加载线程配置
    if 'thread' in APP_CONFIG:
        THREAD_CONFIG = APP_CONFIG['thread']
    else:
        # 默认线程配置
        THREAD_CONFIG = {
            'max_workers': 10,
            'min_delay': 0,
            'max_delay': 30
        }
        
except (FileNotFoundError, KeyError) as e:
    logging.critical(f"配置加载失败，程序终止: {e}")
    sys.exit(1)

# =============================================================================
# 数据库操作模块
# =============================================================================

def get_db_connection():
    """建立数据库连接"""
    try:
        db_cfg = DB_CONFIG.copy()
        # 配置文件中已经正确处理了类型转换，直接使用
        conn = pymssql.connect(**db_cfg)
        logging.info("数据库连接成功")
        return conn
    except Exception as e:
        logging.error(f"数据库连接失败: {e}")
        raise

def close_db_connection(conn, conn_name=""):
    """安全关闭数据库连接"""
    try:
        if conn:
            conn.close()
            logging.debug(f"数据库连接 {conn_name} 已关闭")
    except Exception as e:
        logging.error(f"关闭数据库连接时出错: {e}")
    finally:
        conn = None

def execute_sp(cursor, sp_name, *params):
    """执行存储过程"""
    try:
        sql_command = f"EXEC {sp_name}"
        if params:
            param_placeholders = ', '.join(['%s'] * len(params))
            sql_command += f" {param_placeholders}"
        
        cursor.execute(sql_command, params)
        return cursor.fetchall()
    except Exception as e:
        logging.error(f"执行存储过程 {sp_name} 失败: {e}")
        return None

# =============================================================================
# AI接口调用模块
# =============================================================================

def call_ai_api(content):
    """
    调用AI大模型API进行病历分析
    
    功能特性：
    1. 自动重试机制：最多重试3次
    2. 智能API组选择：随机选择可用的API组
    3. 密钥管理：自动获取和释放API密钥
    4. 错误处理：特殊处理429限流错误
    5. 响应验证：检查API返回格式的有效性
    
    参数：
    content: 要发送给AI模型的病历内容
    
    返回：
    AI分析结果文本，失败时返回None
    """
    max_retries = 3
    
    for attempt in range(max_retries):
        api_key = None
        api_group = None
        
        try:
            # 1. 选择API组和获取密钥
            if not ENABLED_API_GROUPS:
                logging.error("没有可用的API组")
                log_api_call_failure()  # 记录API调用失败
                return None
                
            api_group = random.choice(ENABLED_API_GROUPS)  # 随机选择API组
            group_name = api_group['name']
            api_manager = API_MANAGERS[group_name]
            logging.debug(f"线程 {threading.get_ident()} 选择API组: '{group_name}'")

            api_key = api_manager.get_key()  # 获取可用密钥

            # 2. 构建API请求
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f"Bearer {api_key}"
            }
            payload = {
                "model": api_group['model'],
                "messages": [{"role": "user", "content": content}],
                "stream": False,
                "temperature": 0.1,
                "transforms": ["middle-out"]
            }
            proxies = api_group.get('proxies', {})

            # 3. 发送API请求
            with requests.Session() as session:
                logging.info(f"调用API组 '{group_name}' (尝试 {attempt + 1}/{max_retries})")
                
                response = session.post(
                    api_group['url'], 
                    json=payload, 
                    headers=headers, 
                    timeout=int(api_group.get('timeout', '1800')),
                    proxies=proxies
                )
                response.raise_for_status()

                # 4. 解析API响应
                try:
                    response_data = response.json()
                except ValueError as json_error:
                    # JSON解析失败，通常是响应内容不是有效的JSON
                    raise ApiLogicError(f"API返回的响应无法解析为JSON: {json_error}")

                # 检查API逻辑错误
                if 'error' in response_data:
                    raise ApiLogicError(f"API返回错误: {response_data['error']}")

                # 提取AI分析结果
                if (response_data and 'choices' in response_data and 
                    len(response_data['choices']) > 0):
                    message = response_data['choices'][0].get('message', {})
                    ai_return = message.get('content')
                    
                    if ai_return is not None:
                        logging.info("AI分析完成")
                        log_api_call_success()  # 记录API调用成功
                        return ai_return
                
                # 响应格式不符合预期
                logging.warning(f"API返回格式异常: {response_data}")
                log_api_call_failure()  # 记录API调用失败
                return None

        except (requests.exceptions.RequestException, ApiLogicError, ValueError) as e:
            # 记录详细错误信息
            response_info = ""
            if 'response' in locals() and response is not None:
                # 获取响应状态码
                status_code = response.status_code
                
                # 根据运行模式决定是否显示响应体
                if SYSTEM_CONFIG.get('mode', 'RELEASE') == 'DEBUG':
                    # DEBUG模式：显示响应体内容（清理空白行）
                    response_body = response.text.strip()
                    # 压缩连续的空白行为单个空行
                    import re
                    response_body = re.sub(r'\n\s*\n+', '\n\n', response_body)
                    response_info = f" Status: {status_code}, Body: {response_body[:500]}"
                else:
                    # RELEASE模式：只显示状态码和基本信息
                    if status_code == 200:
                        # 200状态码但解析失败，通常是格式问题
                        response_info = f" Status: {status_code} (响应格式异常)"
                    else:
                        response_info = f" Status: {status_code}"
            
            retry_delay = RETRY_DELAY
            
            # 特殊处理429限流错误
            is_429_error = (isinstance(e, requests.exceptions.HTTPError) and 
                           e.response is not None and e.response.status_code == 429)

            if is_429_error and api_key and api_group:
                group_name = api_group['name']
                api_manager = API_MANAGERS[group_name]
                logging.warning(f"API密钥 ...{api_key[-4:]} (组 '{group_name}') 收到429错误")
                retry_delay += 60  # 429错误时增加重试延迟
                api_manager.handle_429_error(api_key)  # 通知密钥管理器

            logging.warning(f"API调用失败 (尝试 {attempt + 1}/{max_retries}): {e}{response_info}")
            
            if attempt < max_retries - 1:
                logging.info(f"将在 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
            else:
                logging.error(f"API调用在 {max_retries} 次尝试后仍然失败")
                log_api_call_failure()  # 记录API调用失败
                return None
                
        finally:
            # 确保释放API密钥
            if api_key and api_group:
                group_name = api_group['name']
                api_manager = API_MANAGERS[group_name]
                api_manager.release_key(api_key)
    
    log_api_call_failure()  # 记录API调用失败
    return None


# =============================================================================
# 系统维护模块
# =============================================================================

def perform_hourly_maintenance():
    """
    执行每小时一次的系统维护任务
    
    维护内容：
    1. 清理数据库中的孤立记录
    2. 查找并重新处理未完成的任务
    3. 数据一致性检查和修复
    
    触发条件：
    - 距离上次维护超过1小时（3600秒）
    - 通常在长时间等待期间自动触发
    """
    global last_maintenance_time
    now = datetime.now()

    # 检查维护间隔，避免频繁执行维护任务
    if (now - last_maintenance_time).total_seconds() < 3600:
        return

    logging.info("开始执行每小时系统维护任务...")
    
    conn = None
    try:
        # 使用独立的数据库连接进行维护操作
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 步骤1：清理孤立的AI分析结果记录
            logging.info("维护步骤1：清理数据库孤立记录...")
            
            # 删除在诊断手术表中完全没有对应记录的AI结果
            sql_delete_1 = """
            DELETE a FROM XX_AIFZ_RETURN a WITH (NOLOCK) 
            LEFT JOIN XX_AIFZ_ZDSS b WITH (NOLOCK) ON a.syxh = b.syxh 
            WHERE ISNULL(b.syxh, '') = ''
            """
            cursor.execute(sql_delete_1)
            deleted_1 = cursor.rowcount
            logging.info(f"清理了 {deleted_1} 条完全孤立的AI分析记录")
            
            # 删除在诊断表中没有诊断类型记录的AI结果
            sql_delete_2 = """
            DELETE a FROM XX_AIFZ_RETURN a WITH (NOLOCK) 
            LEFT JOIN XX_AIFZ_ZDSS b WITH (NOLOCK) ON a.syxh = b.syxh AND b.type = 'zd' 
            WHERE ISNULL(b.syxh, '') = ''
            """
            cursor.execute(sql_delete_2)
            deleted_2 = cursor.rowcount
            logging.info(f"清理了 {deleted_2} 条缺少诊断记录的AI分析记录")
            
            conn.commit()
            logging.info("数据库清理操作已提交")

            # 步骤2：查找需要重新处理的记录
            logging.info("维护步骤2：查找需要重新处理的记录...")
            sql_select_reprocess = """
            SELECT syxh FROM XX_AIFZ_RETURN WITH (NOLOCK) 
            WHERE ISNULL(zdssextime, '') = '' OR zdssextime < aisavetime
            """
            cursor.execute(sql_select_reprocess)
            results = cursor.fetchall()
            
            # 提取需要重新处理的syxh列表
            syxh_to_reprocess: list[str] = []
            if results:
                for row in results:
                    if isinstance(row, dict):
                        val = row.get('syxh')
                        if isinstance(val, str):
                            syxh_to_reprocess.append(val)
                        elif val is not None:
                            syxh_to_reprocess.append(str(val))

        # 步骤3：重新处理未完成的记录
        if syxh_to_reprocess:
            logging.info(f"发现 {len(syxh_to_reprocess)} 条记录需要重新处理，开始调用诊断提取模块...")
            reprocess_and_save_syxh_list(syxh_to_reprocess)
        else:
            logging.info("没有发现需要重新处理的记录")

        # 更新维护时间戳
        last_maintenance_time = now
        logging.info("系统维护任务完成")

    except Exception as e:
        logging.error(f"执行系统维护任务时发生错误: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except Exception as rb_e:
                logging.error(f"维护任务事务回滚失败: {rb_e}")
    finally:
        if conn:
            close_db_connection(conn, "maintenance_conn")


# =============================================================================
# 病历AI分析处理模块
# =============================================================================

def save_aireturn_to_db(cursor, syxh, ai_return, process_type):
    """
    将AI分析结果保存到数据库
    
    处理逻辑：
    1. 使用行级锁检查记录是否存在（防止并发冲突）
    2. 如果存在则更新，不存在则插入
    3. 根据处理类型设置相应的标志位
    4. 重置诊断提取时间，触发后续处理
    5. 包含死锁重试机制，最多重试3次
    
    参数：
    cursor: 数据库游标
    syxh: 病历首页序号
    ai_return: AI分析结果内容
    process_type: 处理类型（'brgd'=病人挂单, 'brcq'=病人出院, 'brzy'=病人在院）
    
    返回：成功返回True，失败返回False
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 设置事务隔离级别和锁超时时间
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET LOCK_TIMEOUT 30000")  # 30秒锁超时
            
            # 使用行级更新锁检查记录是否存在（防止死锁）
            cursor.execute("SELECT 1 AS record_exists FROM XX_AIFZ_RETURN WITH (UPDLOCK, ROWLOCK) WHERE syxh = %s", (syxh,))
            exists = cursor.fetchone()
            
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if exists:
                # 记录存在，执行更新操作
                set_clauses = [
                    "aireturn = %s",
                    "aisavetime = %s",
                    "zdssextime = NULL"  # 重置诊断提取时间，触发重新提取
                ]
                params = [ai_return, current_time]

                # 根据处理类型设置标志位
                if process_type == 'brgd':
                    set_clauses.append("isgdsave = 1")  # 标记挂单已保存
                elif process_type == 'brcq':
                    set_clauses.append("iscqsave = 1")  # 标记出院已保存
                # brzy类型只更新内容，不修改标志位

                update_query = f"UPDATE XX_AIFZ_RETURN WITH (ROWLOCK) SET {', '.join(set_clauses)} WHERE syxh = %s"
                params.append(syxh)
                
                cursor.execute(update_query, tuple(params))
                logging.info(f"[更新] syxh: {syxh} 的AI分析结果已更新（类型: {process_type}）")
            else:
                # 记录不存在，执行插入操作
                isgdsave = 1 if process_type == 'brgd' else 0
                iscqsave = 1 if process_type == 'brcq' else 0

                insert_query = "INSERT INTO XX_AIFZ_RETURN (syxh, aireturn, aisavetime, zdssextime, iscqsave, isgdsave) VALUES (%s, %s, %s, NULL, %s, %s)"
                cursor.execute(insert_query, (syxh, ai_return, current_time, iscqsave, isgdsave))
                logging.info(f"[插入] syxh: {syxh} 的AI分析结果已插入（类型: {process_type}）")
            return True
            
        except Exception as e:
            # 检查是否是死锁错误 (1205)
            is_deadlock = False
            if hasattr(e, 'args') and len(e.args) > 0:
                if isinstance(e.args[0], tuple) and len(e.args[0]) > 0:
                    error_code = e.args[0][0]
                    is_deadlock = (error_code == 1205)
            
            if is_deadlock and attempt < max_retries - 1:
                # 死锁错误，增加随机延迟后重试
                retry_delay = random.uniform(1, 3) * (attempt + 1)
                logging.warning(f"syxh: {syxh} 遇到死锁错误，{retry_delay:.2f}秒后重试 (尝试 {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                continue
            else:
                # 非死锁错误或重试次数用完，记录错误并返回失败
                logging.error(f"保存AI分析结果失败 (syxh: {syxh}): {e}")
                return False
    
    # 所有重试都失败了
    logging.error(f"syxh: {syxh} 在 {max_retries} 次重试后仍然失败")
    return False

# 导入诊断手术提取模块
from aifz_zdss_extract import process_single_syxh as process_zdss_for_syxh

def process_single_syxh(cursor, syxh_row, process_type):
    """
    处理单个病历的完整AI分析流程
    
    处理步骤：
    1. 从数据库获取病历的各部分内容（提示词、病程记录、检查结果等）
    2. 将内容拼接后发送给AI模型进行分析
    3. 将AI分析结果保存到数据库
    4. 调用诊断手术提取模块进行结构化处理
    
    参数：
    cursor: 数据库游标
    syxh_row: 包含病历序号的字典
    process_type: 处理类型（brgd/brcq/brzy）
    
    返回：(结果描述, 是否成功)
    """
    syxh = syxh_row['syxh']
    logging.info(f"开始处理病历 syxh: {syxh} (类型: {process_type})")
    
    try:
        # 设置数据库事务参数
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        cursor.execute("SET LOCK_TIMEOUT 60000")  # 60秒锁超时
        
        # 按顺序获取病历的各部分内容
        content_parts = []
        content_types = ['prompt', 'bcjl', 'jcjg', 'jyjg', 'fymx', 'kb']  # 提示词、病程记录、检查结果、检验结果、费用明细、知识库
        
        for content_type in content_types:
            logging.debug(f"获取 syxh: {syxh} 的 '{content_type}' 内容")
            result = execute_sp(cursor, 'usp_xx_hz_zlxx', syxh, content_type)
            
            # 提取有效内容
            if result and result[0] and result[0].get('Content'):
                content_parts.append(result[0]['Content'])
            else:
                content_parts.append('')

        # 拼接所有内容作为AI输入
        final_content = "".join(content_parts)

        if not final_content.strip():
            logging.warning(f"syxh: {syxh} 的病历内容为空，跳过处理")
            return f"syxh: {syxh} - 病历内容为空", False
            
        # 调用AI接口进行分析
        ai_return = call_ai_api(final_content)

        if ai_return:
            # 添加随机延迟，降低数据库并发冲突概率
            save_delay = random.uniform(0, 10)
            logging.info(f"syxh: {syxh} - AI分析完成，延迟 {save_delay:.2f} 秒后保存结果")
            time.sleep(save_delay)

            # 步骤1：保存AI分析结果
            if save_aireturn_to_db(cursor, syxh, ai_return, process_type):
                logging.info(f"syxh: {syxh} 的AI分析结果已保存")
                
                # 步骤2：立即进行诊断手术信息提取
                conn = cursor.connection
                if process_zdss_for_syxh(conn, syxh):
                    logging.info(f"syxh: {syxh} 处理完成 - AI分析和诊断提取均成功")
                    return f"syxh: {syxh} - 处理完成", True
                else:
                    logging.error(f"syxh: {syxh} 诊断提取失败（AI结果已保存）")
                    return f"syxh: {syxh} - 诊断提取失败", False
            else:
                logging.error(f"syxh: {syxh} AI结果保存失败")
                return f"syxh: {syxh} - AI结果保存失败", False
        else:
            logging.warning(f"syxh: {syxh} AI分析失败，无有效返回")
            return f"syxh: {syxh} - AI分析失败", False

    except Exception as e:
        logging.error(f"处理 syxh: {syxh} 时发生异常: {e}", exc_info=True)
        return f"syxh: {syxh} - 系统异常", False

def process_syxh_threaded(syxh_row, process_type):
    """
    为线程池执行设计的包装函数。
    它负责处理单个syxh的整个生命周期，包括独立的数据库连接和事务管理。
    """
    syxh = syxh_row['syxh']
    
    # 添加随机延迟，避免所有线程同时发起请求
    min_delay = int(THREAD_CONFIG.get('min_delay', 0))
    max_delay = int(THREAD_CONFIG.get('max_delay', 30))
    if max_delay > 0:
        delay_seconds = random.uniform(min_delay, max_delay)
        if SHUTDOWN_FLAG.is_set():
            logging.info(f"syxh: {syxh} 检测到关闭标志，线程提前退出（延迟前）")
            return
        logging.debug(f"syxh: {syxh} 的处理将随机延迟 {delay_seconds:.2f} 秒")
        time.sleep(delay_seconds)
        if SHUTDOWN_FLAG.is_set():
            logging.info(f"syxh: {syxh} 检测到关闭标志，线程提前退出（延迟后）")
            return
    
    conn = None
    try:
        # 每个线程都创建自己的数据库连接
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 先设置事务隔离级别，然后再设置锁超时 - 顺序很重要
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET LOCK_TIMEOUT 60000")  # 60秒锁超时
            
            # 调用原始的单一处理逻辑
            if SHUTDOWN_FLAG.is_set():
                logging.info(f"syxh: {syxh} 检测到关闭标志，线程提前退出（处理前）")
                return
            result_msg, success = process_single_syxh(cursor, syxh_row, process_type)
            if success:
                try:
                    conn.commit()
                    logging.info(f"syxh: {syxh} 的数据库更改已提交。结果: {result_msg}")
                except Exception as commit_err:
                    logging.error(f"提交 syxh: {syxh} 的事务时发生错误: {commit_err}")
            else:
                try:
                    conn.rollback()
                    logging.error(f"syxh: {syxh} 处理失败，数据库更改已回滚。结果: {result_msg}")
                except Exception as rb_e:
                    logging.error(f"回滚 syxh: {syxh} 的事务时发生错误: {rb_e}")
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception as rb_e:
                logging.error(f"对 syxh: {syxh} 进行回滚时发生错误: {rb_e}")
        logging.error(f"处理 syxh: {syxh} 的线程中发生严重错误: {e}", exc_info=True)
    finally:
        close_db_connection(conn, f"syxh: {syxh} 的线程")

def run_main_process(process_type, specific_syxh=None, max_workers=None):
    """
    运行主流程，获取syxh列表并使用线程池并发处理。
    :param process_type: 处理类型 ('brgd', 'brcq', 'brzy')，如果指定了specific_syxh则此项无效
    :param specific_syxh: 如果提供，则只处理这一个syxh
    :param max_workers: 线程池的最大线程数，如果未指定则使用配置文件中的值
    """
    if max_workers is None:
        max_workers = int(THREAD_CONFIG.get('max_workers', 10))
    
    task_name = f"syxh: {specific_syxh}" if specific_syxh else f"'{process_type}' 类型"
    logging.info(f"====== 开始执行 {task_name} 任务 (并发数: {max_workers}) ======")
    start_time = time.time()
    
    conn = None
    try:
        conn = get_db_connection()
        
        syxh_list = []
        with conn.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute("SET LOCK_TIMEOUT 30000")  # 30秒锁超时
            
            if specific_syxh:
                syxh_list.append({'syxh': specific_syxh})
                process_type = 'brgd'
            else:
                logging.info(f"正在获取 '{process_type}' 类型的待处理 syxh 列表...")
                syxh_list = execute_sp(cursor, 'usp_xx_aifz_auto', process_type)
        close_db_connection(conn, "获取任务列表")
        conn = None
        
        if not syxh_list:
            logging.info(f"未获取到 {task_name} 的待处理记录，任务结束。")
            return

        logging.info(f"获取到 {len(syxh_list)} 个 syxh，准备使用 {max_workers} 个线程进行处理...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for i, syxh_row in enumerate(syxh_list):
                if max_workers > 1 and i > 0:
                    stagger_delay = min(2.0, 10.0 / max_workers) * (i % max_workers)
                    if stagger_delay > 0:
                        if SHUTDOWN_FLAG.is_set():
                            logging.warning("检测到关闭标志，线程池任务提交提前终止！")
                            break
                        time.sleep(stagger_delay)
                future = executor.submit(process_syxh_threaded, syxh_row, process_type)
                # 兼容syxh_row为dict或str
                if isinstance(syxh_row, dict):
                    futures[future] = syxh_row.get('syxh', str(syxh_row))
                else:
                    futures[future] = str(syxh_row)
            # --- 优化主线程等待方式 ---
            pending_futures = set(futures.keys())
            total = len(pending_futures)
            finished = 0
            while pending_futures:
                done, not_done = wait(pending_futures, timeout=2, return_when=FIRST_COMPLETED)
                for future in done:
                    syxh = futures[future]
                    finished += 1
                    logging.info(f"--- 进度: {finished}/{total} (syxh: {syxh} 已处理完毕) ---")
                    try:
                        future.result()
                    except Exception as exc:
                        logging.error(f'syxh {syxh} 在其工作线程中产生了一个未处理的异常: {exc}')
                    pending_futures.remove(future)
                if SHUTDOWN_FLAG.is_set():
                    logging.warning("检测到关闭标志，正在取消剩余的未开始任务...")
                    for f in not_done:
                        if not f.running() and not f.done():
                            f.cancel()
                    break
    except Exception as e:
        logging.error(f"执行 {task_name} 任务主流程时发生严重错误: {e}", exc_info=True)
    finally:
        if conn:
            close_db_connection(conn, "主流程")
    end_time = time.time()
    logging.info(f"====== {task_name} 任务执行完毕，总耗时: {end_time - start_time:.2f} 秒 ======")


def run_scheduled_tasks(max_workers=1):
    """封装需要定时执行的所有任务"""
    logging.info(f"##### 开始执行一轮预定任务 @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} #####")
    # 依次执行所有类型的任务
    for process_type in ['brgd', 'brcq', 'brzy']:
        if SHUTDOWN_FLAG.is_set():
            logging.warning("检测到关闭标志，本轮预定任务提前中止。")
            break
        run_main_process(process_type, max_workers=max_workers)
    logging.info(f"##### 本轮所有预定任务执行完毕 @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} #####")

def get_next_run_time():
    """
    计算下一个预定的运行时间点。
    任务每5分钟在分钟数为 0, 5, 10, ... 55 时启动。
    """
    now = datetime.now()
    next_minute_slot = (now.minute // 5 + 1) * 5
    if next_minute_slot < 60:
        next_run = now.replace(minute=next_minute_slot, second=0, microsecond=0)
    else:
        next_run = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return next_run

def main():
    """主函数，负责初始化、解析命令行参数并启动程序核心逻辑"""
    
    # --- Ctrl+C 优雅退出处理 ---
    def shutdown_handler(signum, frame):
        if not SHUTDOWN_FLAG.is_set():
            logging.warning("\n捕获到中断信号 (Ctrl+C)，正在准备优雅退出... 按第二次 Ctrl+C 可强制退出。")
            SHUTDOWN_FLAG.set()
        else:
            logging.warning("已接收到第二次关闭信号，将强制退出。")
            os._exit(1) # 强制退出

    signal.signal(signal.SIGINT, shutdown_handler)

    parser = argparse.ArgumentParser(description="自动化AI病历辅助处理程序", formatter_class=argparse.RawTextHelpFormatter)
    
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--run-now', action='store_true', help="立即执行一次所有定时任务，然后退出。")
    group.add_argument('--syxh', type=str, help="只处理指定的单个syxh，然后退出。")
    
    # 移除 --debug 参数，现在由 config.ini 控制
    # parser.add_argument('--debug', action='store_true', help="启用调试模式，输出详细日志。")
    parser.add_argument('--threads', type=int, help=f"并发处理的线程数 (1-20)。默认为配置文件中的设置 ({THREAD_CONFIG.get('max_workers', 10)})。")
    
    args = parser.parse_args()

    if args.threads is not None and not (1 <= args.threads <= 20):
        parser.error("--threads 参数值必须在 1 到 20 之间。")

    # 1. 初始化日志 (已在模块顶部完成)
    
    # 从命令行或配置文件获取线程数
    max_workers = args.threads if args.threads is not None else int(THREAD_CONFIG.get('max_workers', 10))
    
    global RETRY_DELAY
    # 根据线程数设置重试延迟并记录
    if max_workers > 1:
        RETRY_DELAY = 30
        logging.info(f"多线程模式启用 ({max_workers} 线程)，重试延迟设置为 {RETRY_DELAY} 秒。")
    else:
        # 默认值是5，这里只记录信息
        RETRY_DELAY = 5
        logging.info(f"单线程模式，重试延迟为 {RETRY_DELAY} 秒。")

    logging.info("="*20 + " 程序启动 " + "="*20)
    # 运行模式的日志记录已在 setup_logging 中处理

    # 2. 检查并安装依赖
    check_and_install_packages()

    try:
        # 3. 根据命令行参数执行任务
        if args.syxh:
            logging.info(f"接收到 --syxh 参数，将只处理 syxh: {args.syxh}")
            run_main_process(process_type=None, specific_syxh=args.syxh, max_workers=1)
        elif args.run_now:
            logging.info(f"接收到 --run-now 参数，立即执行一次所有任务... (并发数: {max_workers})")
            run_scheduled_tasks(max_workers=max_workers)
        else:
            # 默认行为：进入定时任务模式
            logging.info(f"##### 进入计划任务模式 (并发数: {max_workers}) #####")
            logging.info("按 Ctrl+C 可以安全地终止定时循环。")
            has_logged_wait_message = False
            while not SHUTDOWN_FLAG.is_set():
                try:
                    next_run = get_next_run_time()
                    wait_seconds = (next_run - datetime.now()).total_seconds()

                    if wait_seconds > 0:
                        if not has_logged_wait_message:
                            logging.info(f"下一次任务将在 {next_run.strftime('%Y-%m-%d %H:%M:%S')} 执行，等待 {wait_seconds:.0f} 秒...")
                            has_logged_wait_message = True

                        # 新增：在长时间等待期间执行维护任务
                        if wait_seconds > 200:
                            if SHUTDOWN_FLAG.is_set():
                                break
                            perform_hourly_maintenance()

                        # 使用 next_run 作为最终的等待时间点，确保任务准时执行
                        while datetime.now() < next_run:
                            if SHUTDOWN_FLAG.is_set():
                                break
                            remaining_wait = (next_run - datetime.now()).total_seconds()
                            SHUTDOWN_FLAG.wait(min(1, max(0, remaining_wait)))
                        
                        if SHUTDOWN_FLAG.is_set():
                            break

                    if not SHUTDOWN_FLAG.is_set():
                        run_scheduled_tasks(max_workers=max_workers)
                        has_logged_wait_message = False

                except Exception as e:
                    logging.error(f"计划任务主循环中发生未捕获的异常: {e}", exc_info=True)
                    logging.info("将等待 60 秒后重试...")
                    SHUTDOWN_FLAG.wait(60)
                    has_logged_wait_message = False
            
            logging.info("定时任务循环已终止。程序退出。")
    except KeyboardInterrupt:
        logging.warning("主线程捕获到 KeyboardInterrupt，确保关闭标志已设置。")
        SHUTDOWN_FLAG.set()
    finally:
        # 程序退出时强制写入API统计信息
        try:
            from aifz_logger import force_write_api_stats
            force_write_api_stats()
        except Exception as e:
            logging.warning(f"写入API统计信息时出错: {e}")


if __name__ == '__main__':
    main()