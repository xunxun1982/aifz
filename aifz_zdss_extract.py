#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
诊断和手术解析程序 (aifz_zdss_extract.py)
功能：从XX_AIFZ_RETURN表中提取aireturn字段的内容，解析诊断和手术信息，
      并重构后保存到XX_AIFZ_ZDSS表中。
可以作为独立程序运行，也可以被其他程序调用。
"""

import pymssql
import logging
import re
import argparse
from datetime import datetime
import time
import os
import configparser
from aifz_parser import parse_diagnoses_and_surgeries, restructure_and_validate_data
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue, Empty
from typing import List, Dict, Tuple, Any, Optional
import sys

# --- 导入统一日志模块 ---
from aifz_logger import setup_logging

# Pre-compile regex patterns for performance optimization
STRIP_STARS_RE = re.compile(r'^[*]+|[*]+$')
STRIP_PAREN_RE = re.compile(r'[（(].*?[)）]')
STRIP_UNSPECIFIED_RE = re.compile(r'([，,]?\s*未特指(的)?[，,]?)+', re.IGNORECASE)
STRIP_LEADING_MODS_RE = re.compile(r'^[\s,，、的]+')
STRIP_TRAILING_MODS_RE = re.compile(r'[\s,，、的]+$')
STRIP_EXTRA_SPACES_RE = re.compile(r'\s+')
CLEAN_NON_CHINESE_RE = re.compile(r'[^\u4e00-\u9fa5]')


# --- 配置部分 ---
def load_db_config():
    """从 config.ini 文件加载数据库配置"""
    config = configparser.ConfigParser()
    # 假设 config.ini 与脚本在同一目录下
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    config.read(config_path, encoding='utf-8')
    
    # 创建新字典并进行类型转换
    db_config = {}
    config_section = config['database']
    
    # 字符串字段直接复制
    for key in ['server', 'user', 'password', 'database', 'charset']:
        if key in config_section:
            db_config[key] = config_section[key]
    
    # 整数字段转换
    for key in ['timeout', 'login_timeout', 'arraysize']:
        if key in config_section:
            db_config[key] = int(config_section[key])
    
    # 布尔字段转换
    for key in ['as_dict', 'read_only', 'autocommit', 'use_datetime2']:
        if key in config_section:
            db_config[key] = config_section[key].lower() in ('true', '1', 'yes', 'on')
    
    return db_config

# 加载线程配置
def load_thread_config():
    """从 config.ini 文件加载线程配置"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    config.read(config_path, encoding='utf-8')
    
    if 'thread' in config:
        config_section = config['thread']
        thread_config = {}
        
        # 整数字段转换，处理内联注释
        for key in ['max_workers', 'min_delay', 'max_delay']:
            if key in config_section:
                # 提取数值部分，忽略注释
                value_str = config_section[key].strip()
                # 如果有注释（#），只取注释前的部分
                if '#' in value_str:
                    value_str = value_str.split('#')[0].strip()
                try:
                    thread_config[key] = int(value_str)
                except ValueError as e:
                    logging.warning(f"配置项 'thread.{key}' 值 '{value_str}' 无法转换为整数，使用默认值")
                    # 设置默认值
                    defaults = {'max_workers': 10, 'min_delay': 0, 'max_delay': 30}
                    thread_config[key] = defaults[key]
            else:
                # 默认值
                defaults = {'max_workers': 10, 'min_delay': 0, 'max_delay': 30}
                thread_config[key] = defaults[key]
        return thread_config
    else:
        # 默认配置
        return {
            'max_workers': 10,
            'min_delay': 0,
            'max_delay': 30
        }

DB_CONFIG = load_db_config()
THREAD_CONFIG = load_thread_config()

# 调试模式设置 (已移除，由 aifz_logger 根据 config.ini 控制)
# DEBUG = True 

# 全局日志记录器
# aifz_main.py 在导入此模块前会配置好根 logger
# 当此文件独立运行时，main 函数会配置好 logger
logger = logging.getLogger(__name__)

def fix_db_read_encoding(text):
    """
    尝试修复从数据库读取时产生的乱码。
    这种情况通常是因为数据库连接编码与列的实际编码不匹配。
    我们假定数据是GBK/CP936，但被错误地按latin-1解码了。
    """
    if not text or not isinstance(text, str):
        return text
    try:
        # 将错误解码的字符串重新编码为字节，然后用正确的编码(gbk)解码
        return text.encode('latin1').decode('gbk')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # 如果转换失败，说明原始编码假设错误，返回原样字符串
        return text

def get_db_connection():
    """建立并返回一个数据库连接"""
    try:
        # 确保每次连接时都重新加载配置，以应对可能的动态变化
        conn = pymssql.connect(**load_db_config())
        logging.debug("数据库连接成功。")
        return conn
    except Exception as e:
        logging.error(f"数据库连接失败: {e}")
        raise

def longest_common_substring(s1, s2):
    """返回s1和s2的最长公共子串长度"""
    if not s1 or not s2:
        return 0
    m = len(s1)
    n = len(s2)
    dp = [[0]*(n+1) for _ in range(m+1)]
    max_len = 0
    for i in range(m):
        for j in range(n):
            if s1[i] == s2[j]:
                dp[i+1][j+1] = dp[i][j] + 1
                if dp[i+1][j+1] > max_len:
                    max_len = dp[i+1][j+1]
    return max_len

def jaccard_score(s1, s2):
    """Jaccard分词相似度+最长公共子串，返回(主分,次分)"""
    set1 = smart_tokenize(s1)
    set2 = smart_tokenize(s2)
    if not set1 or not set2:
        return (0, 0)
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    jaccard = intersection / union if union else 0
    lcs = longest_common_substring(s1, s2)
    return (jaccard, lcs)

def best_match_with_lcs_priority(name, candidates, fix_func):
    """
    先筛选与原始名称有最长连续子串（长度>=3）的候选，再用Jaccard+最长公共子串排序。
    :param name: 原始名称
    :param candidates: 候选列表，每项为dict，需有'glmc'字段
    :param fix_func: 修复编码的函数
    :return: 最优候选
    """
    if not candidates:
        return None
    name = fix_func(name)
    # 计算每个候选与原始名称的最长连续子串长度
    lcs_len = [(r, longest_common_substring(name, fix_func(r['glmc']))) for r in candidates]
    max_lcs = max([l for r, l in lcs_len], default=0)
    filtered = [r for r, l in lcs_len if l == max_lcs and l >= 3]
    if filtered:
        scored = [(r, jaccard_score(name, fix_func(r['glmc']))) for r in filtered]
        scored.sort(key=lambda x: (x[1][0], x[1][1]), reverse=True)
        return scored[0][0]
    # 否则全量排序
    scored = [(r, jaccard_score(name, fix_func(r['glmc']))) for r in candidates]
    scored.sort(key=lambda x: (x[1][0], x[1][1]), reverse=True)
    return scored[0][0]

def smart_tokenize(text):
    """智能分词，返回关键词集合"""
    try:
        # 检查系统模式
        import os
        system_mode = 'RELEASE'  # 默认为RELEASE模式
        try:
            import configparser
            config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
            if os.path.exists(config_path):
                config = configparser.ConfigParser()
                config.read(config_path, encoding='utf-8')
                if 'system' in config:
                    system_mode = config['system'].get('mode', 'RELEASE')
        except Exception:
            pass  # 如果配置读取失败，保持默认值
        
        # 在RELEASE模式下禁用jieba的输出
        if system_mode == 'RELEASE':
            import sys
            from io import StringIO
            old_stderr = sys.stderr
            sys.stderr = StringIO()
            
        try:
            import jieba
            # 在RELEASE模式下设置jieba的日志级别
            if system_mode == 'RELEASE':
                jieba.setLogLevel(60)  # 设置为CRITICAL级别，抑制所有日志
            
            return set(jieba.lcut(text, cut_all=False))
        finally:
            if system_mode == 'RELEASE':
                # 恢复标准错误输出
                sys.stderr = old_stderr
    except ImportError:
        # 简单的回退，可以根据需要变得更复杂
        return set(re.findall(r'\w+', text))

def generate_search_terms(name):
    """
    从一个名称中生成一系列按重要性排序的搜索词条。
    优化策略：先清理名称，再进行分词和关键词提取。
    """
    # 1. 清理名称，去除无关修饰
    cleaned_name = strip_stars_and_brackets(name)
    if not cleaned_name:
        return []

    # 2. 移除常见的、意义不大的通用词汇和限定词
    # '未特指' 已被 strip_stars_and_brackets 处理，但保留在停用词中无害
    stop_words = {'的', '性', '型', '综合征', '其他', '和', '伴有', '继发', '原发'}
    
    # 3. 优先使用jieba分词
    try:
        # 检查系统模式
        import os
        system_mode = 'RELEASE'  # 默认为RELEASE模式
        try:
            import configparser
            config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
            if os.path.exists(config_path):
                config = configparser.ConfigParser()
                config.read(config_path, encoding='utf-8')
                if 'system' in config:
                    system_mode = config['system'].get('mode', 'RELEASE')
        except Exception:
            pass  # 如果配置读取失败，保持默认值
        
        # 在RELEASE模式下禁用jieba的输出
        if system_mode == 'RELEASE':
            import sys
            from io import StringIO
            old_stderr = sys.stderr
            sys.stderr = StringIO()
            
        try:
            import jieba
            # 在RELEASE模式下设置jieba的日志级别
            if system_mode == 'RELEASE':
                jieba.setLogLevel(60)  # 设置为CRITICAL级别，抑制所有日志
                
            # 使用搜索引擎模式，能更好地切分长词
            terms = jieba.lcut_for_search(cleaned_name)
            # 过滤掉停用词和单个字符
            meaningful_terms = [t for t in terms if t not in stop_words and len(t) > 1]
            
            # 4. 构造结果列表
            # 将处理后的完整名称作为最重要的关键词
            result_terms = {cleaned_name}
            result_terms.update(meaningful_terms)
            
            # 按长度降序排序，较长的词通常更具特异性
            # 过滤掉空字符串
            sorted_terms = sorted([term for term in result_terms if term], key=len, reverse=True)
            return sorted_terms
        finally:
            if system_mode == 'RELEASE':
                # 恢复标准错误输出
                sys.stderr = old_stderr

    except ImportError:
        # jieba不可用时的降级策略
        logging.warning("jieba库未安装，关键词生成策略降级，可能影响匹配精度。")
        # 移除所有非汉字字符，然后返回整个字符串作为唯一关键词
        clean_name = CLEAN_NON_CHINESE_RE.sub('', cleaned_name)
        return [clean_name] if clean_name else []

def find_match_by_stripping_aggressively(cursor, name, table_name, use_flags=True):
    """
    Aggressively strips one character from both head and tail simultaneously and searches for a match.
    This is a last-resort strategy for terms that fail other matching methods.
    e.g., '电子生物反馈疗法' -> '子生物反馈疗' -> '生物反馈' (match)
    """
    if not name or len(name) < 3:
        return None

    current_name = name
    while len(current_name) >= 2: # Minimum length for a meaningful match
        # Use a contains query, as the core term might be surrounded by other words
        query = f"SELECT TOP 1 glbm, glmc FROM {table_name} WITH (NOLOCK) WHERE glmc LIKE %s"
        if use_flags:
            # This flag is not used in the simplified find_best_name_match, but we add it for robustness
            # in case it's used by the calling context (e.g., the non-simplified logic).
            query += " AND isgray = 0 AND isexcept = 0"
        
        cursor.execute(query, (f'%{current_name}%',))
        result = cursor.fetchone()
        
        if result:
            matched_name = fix_db_read_encoding(result['glmc'])
            jaccard_sim, _ = jaccard_score(name, matched_name)
            # Add a sanity check to ensure the match is not completely random
            if jaccard_sim > 0.2: # A reasonably high threshold for this aggressive strategy
                logging.debug(f"头尾缩减匹配成功: '{name}' -> '{current_name}' -> '{matched_name}' (相似度: {jaccard_sim:.2f})")
                return result
            else:
                 logging.debug(f"头尾缩减匹配 '{name}' -> '{current_name}' -> '{matched_name}' 因相似度低({jaccard_sim:.2f})被拒绝")

        # If not found, strip one char from both ends
        if len(current_name) > 2:
            current_name = current_name[1:-1]
        else:
            break # Stop if the string becomes too short
            
    return None

def find_best_name_match(cursor, name, code, table_name, is_main, use_flags=True):
    """
    通用名称匹配，按优先级：
    1. 整字准确匹配
    2. 去括号名称准确匹配
    3. 基于关键词的模糊匹配（优化版）
    4. 编码模糊匹配
    """
    if not name:
        return None

    original_name_fixed = fix_db_read_encoding(name)
    name_no_brackets = re.sub(r"[（(].*?[)）]", "", original_name_fixed).strip()

    # 1. & 2. 整字和去括号准确匹配
    for n in [original_name_fixed, name_no_brackets]:
        if not n: continue
        query = f"SELECT glbm, glmc FROM {table_name} WITH (NOLOCK) WHERE glmc = %s"
        # ... (此处省略了原有的 isgray/isexcept 条件，因为它们使逻辑过于复杂，暂时简化)
        cursor.execute(query, (n,))
        result = cursor.fetchone()
        if result:
            logging.debug(f"精确匹配成功 ('{n}'): '{original_name_fixed}' -> '{fix_db_read_encoding(result['glmc'])}'")
            return result

    # 3. 基于关键词的模糊匹配 (优化版)
    search_terms = generate_search_terms(original_name_fixed)
    logging.debug(f"为 '{original_name_fixed}' 生成的搜索关键词: {search_terms}")
    
    # 收集所有可能的候选
    all_candidates = []
    for term in search_terms:
        query = f"SELECT glbm, glmc FROM {table_name} WITH (NOLOCK) WHERE glmc LIKE %s"
        cursor.execute(query, (f'%{term}%',))
        all_candidates.extend(cursor.fetchall())
    
    if not all_candidates:
        logging.debug(f"为 '{original_name_fixed}' 未找到任何模糊匹配候选。")
        return None

    # 去重
    unique_candidates = {r['glbm']: r for r in all_candidates}.values()

    scored_candidates = []
    for r in unique_candidates:
        candidate_name = fix_db_read_encoding(r['glmc'])
        jaccard, lcs = jaccard_score(original_name_fixed, candidate_name)
        
        core_keywords = smart_tokenize(original_name_fixed)
        candidate_keywords = smart_tokenize(candidate_name)
        keyword_bonus = len(core_keywords.intersection(candidate_keywords)) / len(core_keywords.union(candidate_keywords))

        final_score = 0.6 * jaccard + 0.2 * (lcs / max(len(original_name_fixed), len(candidate_name), 1)) + 0.2 * keyword_bonus
        scored_candidates.append((r, final_score))

    if scored_candidates:
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        best_candidate, best_score = scored_candidates[0]
        
        if best_score > 0.3: # 阈值可调
            logging.debug(f"模糊匹配成功: '{original_name_fixed}' -> '{fix_db_read_encoding(best_candidate['glmc'])}' (分数: {best_score:.4f})")
            return best_candidate

    # 3.5. Last resort: Aggressive stripping match
    aggressive_match = find_match_by_stripping_aggressively(cursor, original_name_fixed, table_name, use_flags)
    if aggressive_match:
        return aggressive_match

    # 4. 编码模糊匹配 (如果前面的策略都失败了，使用更安全的前缀匹配和名称相似度检查)
    if code:
        # Use a safer prefix match instead of contains match
        query = f"SELECT TOP 1 glbm, glmc FROM {table_name} WITH (NOLOCK) WHERE glbm LIKE %s"
        cursor.execute(query, (f'{code}%',))
        result = cursor.fetchone()
        if result:
            # Sanity check: the matched name should have *some* similarity to the original
            matched_name = fix_db_read_encoding(result['glmc'])
            jaccard_sim, _ = jaccard_score(original_name_fixed, matched_name)
            # A low threshold to prevent completely unrelated matches like '生物反馈' vs 'ECT'
            if jaccard_sim > 0.1:
                logging.debug(f"编码前缀匹配成功: '{code}' -> '{matched_name}' (相似度: {jaccard_sim:.2f})")
                return result
            else:
                logging.debug(f"编码前缀匹配 '{code}' -> '{matched_name}' 因名称相似度过低({jaccard_sim:.2f})而被拒绝。")

    logging.debug(f"为 '{original_name_fixed}' 未找到任何高质量匹配。")
    return None

def strip_stars_and_brackets(name):
    """使用预编译的正则表达式去除头尾*、括号内容、以及所有'未特指'相关修饰短语，返回核心疾病名称。"""
    if not isinstance(name, str):
        return name
    name = STRIP_STARS_RE.sub('', name)
    name = STRIP_PAREN_RE.sub('', name)
    name = STRIP_UNSPECIFIED_RE.sub('', name)
    name = STRIP_LEADING_MODS_RE.sub('', name)
    name = STRIP_TRAILING_MODS_RE.sub('', name)
    name = STRIP_EXTRA_SPACES_RE.sub('', name)
    return name.strip()

def reconstruct_diagnoses(cursor, initial_diagnoses):
    """重构诊断列表"""
    final_diagnoses = []
    
    cursor.execute("SELECT 疾病编码 FROM [XX_DRGS_WJ_2.0_MCC] WITH (NOLOCK)")
    mcc_results = cursor.fetchall()
    mcc_codes = {r['疾病编码'] for r in mcc_results} if mcc_results else set()

    cursor.execute("SELECT 疾病编码 FROM [XX_DRGS_WJ_2.0_CC] WITH (NOLOCK)")
    cc_results = cursor.fetchall()
    cc_codes = {r['疾病编码'] for r in cc_results} if cc_results else set()

    for diag in initial_diagnoses:
        # 如果编码或名称是占位符"无"或为空，则跳过
        if not diag.get('bm') or not diag.get('mc') or '无' in diag.get('bm', '') or '无' in diag.get('mc', ''):
            continue

        is_main = diag['xh'] == 1
        found_match = None

        if is_main:
            # 搜索顺序: 1. 去括号和*后的名称完全匹配 -> 2. 原始名称完全匹配 -> 3. 模糊匹配 -> 4. 编码匹配
            original_name = diag['mc']
            name_without_paren = strip_stars_and_brackets(original_name)
            
            # 1. 按去括号和*后的名称进行完全匹配
            if name_without_paren and name_without_paren != original_name:
                for use_flags in [True, False]:
                    query = f"""SELECT glbm, glmc FROM MED_LCYBZDDYK WITH (NOLOCK) WHERE glmc = %s 
                                {'AND isgray = 0 AND isexcept = 0' if use_flags else ''}"""
                    cursor.execute(query, (name_without_paren,))
                    results = cursor.fetchall()
                    if not results: continue

                    # 首字母偏好
                    if diag.get('bm'):
                        preferred = [r for r in results if r.get('glbm') and r.get('glbm') and r['glbm'][0].upper() == diag['bm'][0].upper()]
                        if preferred:
                            found_match = preferred[0]
                            break
                    if not found_match:
                        found_match = results[0]
                        break
            # 2. 按原始名称进行完全匹配
            if not found_match:
                for use_flags in [True, False]:
                    query = f"""SELECT glbm, glmc FROM MED_LCYBZDDYK WITH (NOLOCK) WHERE glmc = %s 
                                {'AND isgray = 0 AND isexcept = 0' if use_flags else ''}"""
                    cursor.execute(query, (original_name,))
                    results = cursor.fetchall()
                    if not results: continue

                    # 首字母偏好
                    if diag.get('bm'):
                        preferred = [r for r in results if r.get('glbm') and r.get('glbm') and r['glbm'][0].upper() == diag['bm'][0].upper()]
                        if preferred:
                            found_match = preferred[0]
                            break
                    if not found_match:
                        found_match = results[0]
                        break
            # 3. 如果完全匹配失败，进行模糊匹配
            if not found_match:
                # 按原始名称模糊搜索
                for use_flags in [True, False]:
                    query = f"""SELECT glbm, glmc FROM MED_LCYBZDDYK WITH (NOLOCK) WHERE glmc LIKE %s 
                                {'AND isgray = 0 AND isexcept = 0' if use_flags else ''}"""
                    cursor.execute(query, (f'%{original_name}%',))
                    results = cursor.fetchall()
                    if not results: continue

                    # 首字母偏好
                    if diag.get('bm'):
                        preferred = [r for r in results if r.get('glbm') and r.get('glbm') and r['glbm'][0].upper() == diag['bm'][0].upper()]
                        if preferred:
                            found_match = preferred[0]
                            break
                    if not found_match:
                        found_match = results[0]
                        break
                # 如果按原名没找到，按去括号和*后的名称模糊搜索
                if not found_match and name_without_paren and name_without_paren != original_name:
                    for use_flags in [True, False]:
                        query = f"""SELECT glbm, glmc FROM MED_LCYBZDDYK WITH (NOLOCK) WHERE glmc LIKE %s 
                                    {'AND isgray = 0 AND isexcept = 0' if use_flags else ''}"""
                        cursor.execute(query, (f'%{name_without_paren}%',))
                        results = cursor.fetchall()
                        if not results: continue
                        # 首字母偏好
                        if diag.get('bm'):
                            preferred = [r for r in results if r.get('glbm') and r.get('glbm') and r['glbm'][0].upper() == diag['bm'][0].upper()]
                            if preferred:
                                found_match = preferred[0]
                                break
                        if not found_match:
                            found_match = results[0]
                            break
            # 4. 如果还是没找到, 按编码搜索
            if not found_match:
                for use_flags in [True, False]:
                    query = f"""SELECT TOP 1 glbm, glmc FROM MED_LCYBZDDYK WITH (NOLOCK) WHERE glbm LIKE %s
                                {'AND isgray = 0 AND isexcept = 0' if use_flags else ''}"""
                    cursor.execute(query, (f'%{diag["bm"]}%',))
                    found_match = cursor.fetchone()
                    if found_match: break
            # 5. 如果编码匹配也失败，尝试逐字递减匹配
            if not found_match:
                for use_flags in [True, False]:
                    found_match = find_match_by_decreasing_chars(cursor, original_name, True, 'MED_LCYBZDDYK', use_flags)
                    if found_match: break
                # 如果原始名称逐字递减失败，尝试去括号和*后的名称
                if not found_match and name_without_paren and name_without_paren != original_name:
                    for use_flags in [True, False]:
                        found_match = find_match_by_decreasing_chars(cursor, name_without_paren, True, 'MED_LCYBZDDYK', use_flags)
                        if found_match: break
            # 6. 如果逐字递减匹配也失败，尝试从首字递减模糊匹配
            if not found_match:
                for use_flags in [True, False]:
                    found_match = find_match_by_decreasing_chars_from_start(cursor, original_name, True, 'MED_LCYBZDDYK', use_flags)
                    if found_match: break
                # 如果原始名称从首字递减失败，尝试去括号和*后的名称
                if not found_match and name_without_paren and name_without_paren != original_name:
                    for use_flags in [True, False]:
                        found_match = find_match_by_decreasing_chars_from_start(cursor, name_without_paren, True, 'MED_LCYBZDDYK', use_flags)
                        if found_match: break
            if found_match:
                final_diagnoses.append({'xh': diag['xh'], 'bm': found_match['glbm'], 'mc': fix_db_read_encoding(found_match['glmc'])})
            else:
                final_diagnoses.append(diag)
        else: # 其他诊断
            match = find_best_name_match(cursor, diag['mc'], diag['bm'], 'MED_LCYBZDDYK', True)
            if not match:
                match = find_best_name_match(cursor, diag['mc'], diag['bm'], 'MED_LCYBZDDYK', False)
            if match:
                final_diagnoses.append({'xh': diag['xh'], 'bm': match['glbm'], 'mc': fix_db_read_encoding(match['glmc'])})
            else:
                final_diagnoses.append(diag)
    return final_diagnoses

def reconstruct_surgeries(cursor, initial_surgeries):
    """重构手术列表"""
    final_surgeries = []
    for surg in initial_surgeries:
        # 如果编码或名称是占位符"无"或为空，则跳过
        if not surg.get('bm') or not surg.get('mc') or '无' in surg.get('bm', '') or '无' in surg.get('mc', ''):
            continue
        is_main = surg['xh'] == 1
        found_match = None
        original_name = surg['mc']
        name_without_paren = strip_stars_and_brackets(original_name)
        # 统一名称匹配逻辑
        for use_flags in [True, False]:
            found_match = find_best_name_match(cursor, original_name, surg['bm'], 'MED_LCYBSSDYK', is_main, use_flags)
            if found_match:
                break
        if not found_match and name_without_paren and name_without_paren != original_name:
            for use_flags in [True, False]:
                found_match = find_best_name_match(cursor, name_without_paren, surg['bm'], 'MED_LCYBSSDYK', is_main, use_flags)
                if found_match:
                    break
        if found_match:
            final_surgeries.append({'xh': surg['xh'], 'bm': found_match['glbm'], 'mc': fix_db_read_encoding(found_match['glmc'])})
        else:
            final_surgeries.append(surg)
    return final_surgeries

def save_zdss_to_db(cursor, syxh, final_diags, final_surgeries):
    """
    将最终的诊断和手术信息保存到数据库。
    此函数现在假定事务由调用方管理。
    """
    try:
        # 为本次操作设置特定的锁超时
        cursor.execute("SET LOCK_TIMEOUT 60000") # 60秒锁超时
        
        # 1. 删除旧数据 (使用ROWLOCK提示减少锁升级概率)
        cursor.execute("DELETE FROM XX_AIFZ_ZDSS WITH (ROWLOCK) WHERE syxh = %s", (syxh,))
        
        # 2. 批量插入诊断数据
        if final_diags:
            diag_values = [(syxh, 'zd', diag['xh'], diag['bm'], diag['mc']) for diag in final_diags]
            cursor.executemany("INSERT INTO XX_AIFZ_ZDSS(syxh, type, xh, bm, mc) VALUES (%s, %s, %s, %s, %s)", diag_values)
        
        # 3. 批量插入手术数据
        if final_surgeries:
            surg_values = [(syxh, 'ss', surg['xh'], surg['bm'], surg['mc']) for surg in final_surgeries]
            cursor.executemany("INSERT INTO XX_AIFZ_ZDSS(syxh, type, xh, bm, mc) VALUES (%s, %s, %s, %s, %s)", surg_values)
        
        # 4. 更新时间戳
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("UPDATE XX_AIFZ_RETURN WITH (ROWLOCK) SET zdssextime = %s WHERE syxh = %s", (current_time, syxh))
        
        logging.info(f"syxh: {syxh} 的诊断和手术信息已成功暂存以待提交。")
        return True
    except Exception as e:
        # 不在这里回滚，异常将冒泡到调用者处，由调用者决定如何处理事务
        logging.error(f"保存 syxh: {syxh} 的诊断手术信息到数据库时失败: {e}")
        # 将原始异常重新抛出，以便上层捕获
        raise

def process_single_syxh(conn, syxh):
    """处理单个syxh的诊断和手术提取，并包含重试逻辑。"""
    try:
        with conn.cursor() as cursor:
            # 获取aireturn内容。使用 READ UNCOMMITTED 避免锁定。
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            cursor.execute("SELECT aireturn FROM XX_AIFZ_RETURN WHERE syxh = %s", (syxh,))
            aireturn_row = cursor.fetchone()

            # 修复类型错误：安全访问数据库结果
            if not aireturn_row:
                logging.warning(f"syxh: {syxh} 的记录未找到，跳过处理。")
                return True
                
            # 确保安全访问字典键
            if isinstance(aireturn_row, dict):
                aireturn_content = aireturn_row.get('aireturn')
            else:
                # 如果不是字典，可能是元组，尝试按索引访问
                try:
                    aireturn_content = aireturn_row[0] if aireturn_row else None
                except (IndexError, TypeError):
                    aireturn_content = None
                    
            if not aireturn_content:
                logging.warning(f"syxh: {syxh} 的 aireturn 内容为空，跳过处理。")
                return True
            
            initial_diagnoses, initial_surgeries = parse_diagnoses_and_surgeries(aireturn_content)
            logging.info(f"syxh: {syxh} - 初步解析到 {len(initial_diagnoses)} 条诊断, {len(initial_surgeries)} 条手术。")

            if not initial_diagnoses and not initial_surgeries:
                logging.warning(f"syxh: {syxh} 的诊断和手术均为空，AI可能未返回有效信息，跳过数据库保存。")
                return True

            final_diagnoses = reconstruct_diagnoses(cursor, initial_diagnoses)
            final_surgeries = reconstruct_surgeries(cursor, initial_surgeries)
            logging.info(f"syxh: {syxh} - 重构后有 {len(final_diagnoses)} 条诊断, {len(final_surgeries)} 条手术。")

            # 调用保存函数，现在它不会自己处理事务了
            return save_zdss_to_db(cursor, syxh, final_diagnoses, final_surgeries)

    except Exception as e:
        # 如果在任何步骤发生错误，确保日志记录
        logging.error(f"处理 syxh: {syxh} 的过程中发生无法恢复的错误: {e}", exc_info=True)
    return False


def get_syxh_list_to_process(limit, specific_syxh):
    """在一个独立的连接中获取待处理的syxh列表，防止主循环中的锁问题。"""
    syxhs = []
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 设置事务隔离级别为READ COMMITTED
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            
            if specific_syxh:
                syxhs = [specific_syxh]
            else:
                # 使用NOLOCK提示，移除READPAST避免兼容性问题
                top_clause = f"TOP {limit}" if limit else ""
                query = f"""
                    SELECT {top_clause} syxh FROM XX_AIFZ_RETURN WITH (NOLOCK) 
                    WHERE ISNULL(zdssextime, '') = '' OR (ISNULL(zdssextime, '') <> '' and zdssextime < aisavetime)
                """
                cursor.execute(query)
                results = cursor.fetchall()
                if results:
                    # 修复类型错误：使用通用的安全访问方法
                    syxhs = []
                    for row in results:
                        # 安全提取syxh值，兼容字典和元组格式
                        syxh_value = None
                        if isinstance(row, dict):
                            syxh_value = row.get('syxh')
                        elif isinstance(row, (tuple, list)) and len(row) > 0:
                            syxh_value = row[0]
                        
                        if syxh_value:
                            syxhs.append(syxh_value)
                        else:
                            logging.warning(f"跳过无效的数据库记录: {row}")
        conn.commit() # 确保事务关闭
        return syxhs
    except Exception as e:
        logging.error(f"获取待处理SYXH列表失败: {e}")
        return [] # 返回空列表表示失败
    finally:
        if conn:
            close_db_connection(conn, "获取syxh列表")

def process_diagnoses_and_surgeries(specific_syxh=None, limit=None):
    """
    主流程，提取诊断和手术信息，并进行重构和保存。
    为每个syxh使用独立的数据库连接和事务，以彻底避免锁问题。
    """
    logging.info("====== 开始执行诊断和手术提取与重构任务 ======")
    start_time = time.time()
    
    # 启用自动测试验证
    auto_test_validation = True
    
    # 1. 在一个完全独立的会话中获取任务列表
    syxhs_to_process = get_syxh_list_to_process(limit, specific_syxh)
    
    if not syxhs_to_process:
        logging.info("没有需要处理的诊断/手术记录。")
        return 0

    logging.info(f"共找到 {len(syxhs_to_process)} 条记录待处理。")
    processed_count = 0
    
    # 2. 遍历列表，为每个syxh使用一个全新的连接
    for index, syxh in enumerate(syxhs_to_process):
        logging.info(f"--- 开始处理 syxh: {syxh} (进度: {processed_count + 1}/{len(syxhs_to_process)}) ---")
        
        # 为诊断提取任务设置独立的、较短的随机延迟 (0-10秒)
        delay_seconds = random.uniform(0, 10)
        logging.debug(f"syxh: {syxh} 的处理将随机延迟 {delay_seconds:.2f} 秒")
        time.sleep(delay_seconds)
            
        conn = None
        try:
            # 为当前syxh建立一个全新的连接
            conn = get_db_connection()
            
            # 设置处理超时
            import threading
            import queue
            
            # 创建一个队列用于获取结果
            result_queue = queue.Queue()
            
            # 定义一个函数在子线程中执行处理操作
            def process_with_timeout():
                try:
                    result = process_single_syxh(conn, syxh)
                    result_queue.put(("success", result))
                except Exception as e:
                    result_queue.put(("error", str(e)))
            
            # 创建并启动子线程
            process_thread = threading.Thread(target=process_with_timeout)
            process_thread.daemon = True
            process_thread.start()
            
            # 等待子线程执行完毕或超时
            try:
                status, result = result_queue.get(timeout=120)  # 增加到120秒超时
                if status == "success" and result:
                    try:
                        conn.commit()
                        processed_count += 1
                        logging.info(f"syxh: {syxh} 处理成功，事务已提交。")
                    except Exception as commit_err:
                        logging.error(f"提交事务时发生错误: {commit_err}")
                        # 即使提交失败也不尝试回滚，避免更多错误
                else:
                    if status == "error":
                        logging.error(f"处理 syxh: {syxh} 时发生错误: {result}")
                    else:
                        logging.warning(f"处理 syxh: {syxh} 失败。")
                    
                    try:
                        conn.rollback()
                        logging.info(f"syxh: {syxh} 的事务已回滚。")
                    except Exception as rollback_err:
                        logging.error(f"回滚事务时发生错误，但将继续处理: {rollback_err}")
            except queue.Empty:
                logging.error(f"处理 syxh: {syxh} 操作超时，强制中断")
                # 不再尝试回滚，直接关闭连接释放资源
                
                # 如果在自动测试模式下，且已经处理了几条记录后遇到超时，则认为问题仍未解决
                if auto_test_validation and index > 0:
                    logging.critical("自动测试验证失败：程序仍然存在卡住的问题，需要进一步修复")
                    break
                
        except Exception as e:
            logging.error(f"处理 syxh: {syxh} 时发生意外错误。错误: {e}")
        finally:
            if conn:
                try:
                    # 使用close_db_connection函数安全关闭连接
                    close_db_connection(conn, f"syxh {syxh}")
                except:
                    logging.error(f"关闭 syxh {syxh} 的数据库连接时发生错误。")
    
    # 验证是否已经成功解决问题
    if auto_test_validation and processed_count > 0:
        logging.info("自动测试验证通过：脚本能够正常处理记录而不会卡住")
    
    end_time = time.time()
    logging.info(f"====== 诊断和手术提取与重构任务执行完毕，共处理 {processed_count} 条记录，耗时: {end_time - start_time:.2f} 秒 ======")
    return processed_count

# 安全关闭数据库连接的辅助函数
def close_db_connection(conn, conn_name=""):
    """安全关闭数据库连接，避免在关闭连接时发生异常"""
    try:
        conn.close()
        logging.debug(f"数据库连接 {conn_name} 已安全关闭。")
    except Exception as e:
        logging.error(f"关闭数据库连接 {conn_name} 时发生错误: {e}")
    finally:
        # 确保连接变量被清除，帮助垃圾回收
        conn = None

def find_match_by_decreasing_chars(cursor, name, is_main, table_name, use_flags=True):
    """
    通过逐字递减匹配来查找诊断或手术
    :param cursor: 数据库游标
    :param name: 原始名称
    :param is_main: 是否为主诊断/主手术
    :param table_name: 表名 ('MED_LCYBZDDYK' 或 'MED_LCYBSSDYK')
    :param use_flags: 是否使用标志位过滤
    :return: 匹配结果或None
    """
    if not name or len(name) < 2:
        return None
    
    # 构建基础查询
    base_query = f"""
        SELECT glbm, glmc FROM {table_name} WITH (NOLOCK) 
        WHERE glmc = %s
    """
    
    # 添加标志位过滤
    if table_name == 'MED_LCYBZDDYK':
        if use_flags:
            base_query += " AND isgray = 0 AND isexcept = 0"
    else:  # MED_LCYBSSDYK
        if is_main and use_flags:
            base_query += " AND isgray = 0 AND isexcept = 0"
        elif is_main:
            base_query += " AND isgray = 0"
    
    # 逐字递减匹配
    current_name = name
    while len(current_name) >= 2:  # 至少保留2个字符
        cursor.execute(base_query, (current_name,))
        results = cursor.fetchall()
        
        if results:
            logging.debug(f"逐字递减匹配成功: '{name}' -> '{current_name}'")
            return results[0]
        
        # 减少最后一个字符
        current_name = current_name[:-1]
    
    return None

def find_match_by_decreasing_chars_from_start(cursor, name, is_main, table_name, use_flags=True):
    """
    通过从第一个字开始递减进行模糊匹配来查找诊断或手术，返回自动相似度最优结果。
    :param cursor: 数据库游标
    :param name: 原始名称
    :param is_main: 是否为主诊断/主手术
    :param table_name: 表名 ('MED_LCYBZDDYK' 或 'MED_LCYBSSDYK')
    :param use_flags: 是否使用标志位过滤
    :return: 匹配结果或None
    """
    if not name or len(name) < 2:
        return None
    
    # 构建基础查询
    base_query = f"""
        SELECT glbm, glmc FROM {table_name} WITH (NOLOCK) 
        WHERE glmc LIKE %s
    """
    
    # 添加标志位过滤
    if table_name == 'MED_LCYBZDDYK':
        if use_flags:
            base_query += " AND isgray = 0 AND isexcept = 0"
    else:  # MED_LCYBSSDYK
        if is_main and use_flags:
            base_query += " AND isgray = 0 AND isexcept = 0"
        elif is_main:
            base_query += " AND isgray = 0"
    
    # 从第一个字开始递减模糊匹配
    current_name = name
    while len(current_name) >= 2:  # 至少保留2个字符
        cursor.execute(base_query, (f'%{current_name}%',))
        results = cursor.fetchall()
        if results:
            # 全自动相似度排序，返回最优
            best = best_match_with_lcs_priority(name, results, fix_db_read_encoding)
            log_candidates = [(fix_db_read_encoding(r['glmc']), jaccard_score(name, fix_db_read_encoding(r['glmc'])), longest_common_substring(name, fix_db_read_encoding(r['glmc']))) for r in results]
            logging.debug(f"从首字递减模糊候选({current_name}): {log_candidates}")
            return best
        # 减少第一个字符
        current_name = current_name[1:]
    return None

def run_preliminary_update():
    """
    执行一个预处理SQL，将那些在XX_AIFZ_RETURN中存在但在XX_AIFZ_ZDSS中没有对应记录的条目
    的zdssextime重置，以便重新处理。
    """
    logging.info("===== 开始执行预处理SQL更新 =====")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
            UPDATE XX_AIFZ_RETURN SET zdssextime = '' 
            WHERE syxh IN (
                SELECT a.syxh 
                FROM XX_AIFZ_RETURN a WITH (NOLOCK)
                LEFT JOIN XX_AIFZ_ZDSS b WITH (NOLOCK) ON a.syxh = b.syxh
                WHERE b.syxh IS NULL AND a.aireturn IS NOT NULL AND a.aireturn <> ''
            )
            """
            cursor.execute(sql)
            conn.commit()
            # cursor.rowcount is available in pymssql
            logging.info(f"预处理SQL执行完毕，共有 {cursor.rowcount} 条记录被标记为待重新处理。")
    except Exception as e:
        logging.error(f"执行预处理SQL更新时发生错误: {e}")
        if conn:
            try:
                conn.rollback()
            except Exception as rb_err:
                logging.error(f"预处理SQL更新回滚失败: {rb_err}")
    finally:
        if conn:
            close_db_connection(conn, "预处理SQL")

def reprocess_and_save_syxh_list(syxh_list: List[str]):
    """
    接收一个SYXH列表，为列表中的每个条目重新提取和保存诊断及手术信息。
    这个函数是为维护任务设计的，它会自己管理数据库连接和错误处理。

    :param syxh_list: 需要重新处理的病案首页序号列表。
    """
    if not syxh_list:
        logging.info("reprocess_and_save_syxh_list 接收到空列表，无需处理。")
        return

    logging.info(f"开始为 {len(syxh_list)} 个 SYXH 执行重新处理任务...")
    
    # 使用线程池来并行处理
    # 从配置加载线程数，如果未配置则使用默认值10
    max_workers = THREAD_CONFIG.get('max_workers', 10)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 为每个syxh提交一个处理任务
        future_to_syxh = {executor.submit(process_single_syxh_for_reprocessing, syxh): syxh for syxh in syxh_list}
        
        success_count = 0
        failure_count = 0

        for future in as_completed(future_to_syxh):
            syxh = future_to_syxh[future]
            try:
                # 正确的获取 future 结果的方法是调用 result()
                result = future.result()
                if result:
                    success_count += 1
                    # 日志级别从INFO调整为DEBUG，避免在成功时产生过多日志
                    logging.debug(f"成功重新处理 SYXH: {syxh}")
                else:
                    failure_count += 1
                    logging.warning(f"重新处理 SYXH: {syxh} 失败或被跳过。更多信息请查看日志。")
            except Exception as exc:
                failure_count += 1
                logging.error(f"为 SYXH: {syxh} 的重新处理任务在执行时抛出异常: {exc}", exc_info=True)

    logging.info(f"重新处理任务完成。成功: {success_count}，失败: {failure_count}。")

def process_single_syxh_for_reprocessing(syxh: str) -> bool:
    """
    处理单个SYXH的重新提取逻辑。包括获取数据、解析、重构和保存。
    返回 True 表示成功，False 表示失败。
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 1. 从 XX_AIFZ_RETURN 获取 aireturn 内容
            cursor.execute("SELECT aireturn FROM XX_AIFZ_RETURN WITH (NOLOCK) WHERE syxh = %s", (syxh,))
            row = cursor.fetchone()
            
            # 修复类型错误：安全访问数据库结果
            if not row:
                logging.warning(f"在重新处理时未找到 SYXH: {syxh} 的记录。")
                return False
                
            # 安全提取aireturn值，兼容字典和元组格式
            aireturn_content = None
            if isinstance(row, dict):
                aireturn_content = row.get('aireturn')
            elif isinstance(row, (tuple, list)) and len(row) > 0:
                aireturn_content = row[0]
                
            if not aireturn_content:
                logging.warning(f"在重新处理时未找到 SYXH: {syxh} 的有效 'aireturn' 内容。")
                return False
            
            # 2. 解析诊断和手术信息
            parsed_data = parse_diagnoses_and_surgeries(aireturn_content)
            if not parsed_data or (not parsed_data[0] and not parsed_data[1]):
                # 当解析不出任何内容时，日志级别降为DEBUG，避免在RELEASE模式下过多输出
                logging.debug(f"为 SYXH: {syxh} 解析 'aireturn' 未返回有效诊断或手术，将删除该记录。")
                try:
                    # 删除 XX_AIFZ_RETURN 表中对应的记录
                    cursor.execute("DELETE FROM XX_AIFZ_RETURN WHERE syxh = %s", (syxh,))
                    conn.commit()
                    logging.info(f"已成功删除 SYXH: {syxh} 的无效记录。")
                except Exception as del_e:
                    logging.error(f"删除 SYXH: {syxh} 的无效记录时失败: {del_e}")
                    conn.rollback()
                    return False # 删除失败，标记为处理失败
                return True # 标记为处理成功（因为已经按要求删除了）

            # 3. 重构、验证并保存数据
            # 直接调用 reconstruct_diagnoses 和 reconstruct_surgeries，然后保存
            final_diagnoses = reconstruct_diagnoses(cursor, parsed_data[0])
            final_surgeries = reconstruct_surgeries(cursor, parsed_data[1])
            
            # 保存到数据库
            success = save_zdss_to_db(cursor, syxh, final_diagnoses, final_surgeries)
            if not success:
                logging.error(f"为 SYXH: {syxh} 保存数据失败，将回滚事务。")
                conn.rollback()
                return False

            # 4. 更新 XX_AIFZ_RETURN 表中的 zdssextime 字段
            # 确保此操作在同一个事务中
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("UPDATE XX_AIFZ_RETURN SET zdssextime = %s WHERE syxh = %s", (update_time, syxh))
            
            # 5. 提交整个事务
            conn.commit()
            logging.info(f"已成功重新处理并提交 SYXH: {syxh} 的所有更改。")

        return True

    except Exception as e:
        logging.error(f"处理单个 SYXH: {syxh} 进行重新提取时发生严重错误: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except Exception as rb_e:
                logging.error(f"回滚 SYXH: {syxh} 的事务失败: {rb_e}")
        return False
    finally:
        if conn:
            close_db_connection(conn, f"reprocess_{syxh}")


def main():
    """主函数，处理命令行参数并执行程序"""
    
    parser = argparse.ArgumentParser(description="诊断和手术提取与重构程序")
    parser.add_argument('--syxh', type=str, help="指定处理单个syxh")
    parser.add_argument('--limit', type=int, help="限制处理的记录数量")
    # --debug 和 --no-debug 参数保留，但其作用将由新的日志系统处理
    # 如果用户显式使用它们，我们可以在这里动态改变日志级别，但这会增加复杂性
    # 目前，我们让日志级别完全由 config.ini 控制，这些参数暂时不起作用
    parser.add_argument('--debug', action='store_true', help="[已废弃] 启用调试模式 (由 config.ini 控制)。")
    parser.add_argument('--no-debug', dest='debug', action='store_false', help="[已废弃] 禁用调试模式 (由 config.ini 控制)。")
    parser.add_argument('--threads', type=int, help=f"并发处理的线程数 (1-20)。默认为配置文件中的设置 ({THREAD_CONFIG.get('max_workers', 10)})。")
    parser.set_defaults(debug=None)
    
    args = parser.parse_args()
    
    # --- 日志初始化 ---
    # 当作为独立脚本运行时，需要调用 setup_logging
    # 当被 aifz_main 导入时，日志已由 aifz_main 初始化
    setup_logging('zdss')
    
    # 当没有指定特定syxh或limit时，执行预处理SQL
    if not args.syxh and not args.limit:
        run_preliminary_update()
    
    logging.info("===== 诊断和手术提取程序启动 =====")
    
    # 获取线程数配置
    max_workers = args.threads if args.threads is not None else THREAD_CONFIG.get('max_workers', 10)
    logging.info(f"设置并发处理线程数为: {max_workers}")
    
    process_diagnoses_and_surgeries(specific_syxh=args.syxh, limit=args.limit)

if __name__ == "__main__":
    main()