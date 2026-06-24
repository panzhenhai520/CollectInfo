#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
配置文件
包含应用的各种配置参数
所有可配置的值都应该通过环境变量或配置文件设置
"""

import os
import sys


def _load_dotenv_file(path='.env'):
    if not os.path.exists(path):
        return

    try:
        with open(path, 'r', encoding='utf-8') as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                if line.startswith('export '):
                    line = line[7:].strip()
                key, value = line.split('=', 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    except Exception as exc:
        print(f"Warning: failed to load .env: {exc}")


_load_dotenv_file()


def _configure_stdio():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            try:
                stream.reconfigure(encoding='utf-8', errors='replace')
            except Exception:
                pass


_configure_stdio()


def _env_bool(name, default=False):
    """Parse a boolean environment variable."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ('1', 'true', 'yes', 'on')


def _env_int(name, default, min_value=None, max_value=None):
    """Parse a bounded integer environment variable."""
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _env_str(name, default=''):
    value = os.getenv(name, default)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


# ==================== Ragflow API配置 ====================
# 默认指向内部 Ragflow 服务器，若部署到其他环境可通过环境变量覆盖
RAGFLOW_BASE_URL = (_env_str('RAGFLOW_BASE_URL', '') or '').rstrip('/')
RAGFLOW_API_KEY = _env_str('RAGFLOW_API_KEY', '')
RAGFLOW_UPLOAD_ENABLED = _env_bool('RAGFLOW_UPLOAD_ENABLED', bool(RAGFLOW_BASE_URL and RAGFLOW_API_KEY))
RAGFLOW_AUTO_PARSE = _env_bool('RAGFLOW_AUTO_PARSE', True)
RAGFLOW_REUPLOAD_EXISTING = _env_bool('RAGFLOW_REUPLOAD_EXISTING', True)
RAGFLOW_TIMEOUT = _env_int('RAGFLOW_TIMEOUT', 45, 5, 600)
RAGFLOW_UPLOAD_RETRIES = _env_int('RAGFLOW_UPLOAD_RETRIES', 1, 0, 5)
RAGFLOW_PROXY_ENABLED = _env_bool('RAGFLOW_PROXY_ENABLED', False)
RAGFLOW_PROXY_HTTP = _env_str('RAGFLOW_PROXY_HTTP')
RAGFLOW_PROXY_HTTPS = _env_str('RAGFLOW_PROXY_HTTPS')
RAGFLOW_PROXY_SOCKS5 = _env_str('RAGFLOW_PROXY_SOCKS5')


def is_ragflow_configured():
    return bool(RAGFLOW_BASE_URL and RAGFLOW_API_KEY)

# ==================== Redis配置 ====================
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 1))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

# ==================== 数据库配置 ====================
DATABASE_PATH = os.getenv('DATABASE_PATH', 'crawler_articles.db')

# ==================== 存储目录配置 ====================
CRAWL_RESULTS_DIR = os.getenv('CRAWL_RESULTS_DIR', 'crawl_results')
AUTH_STORAGE_DIR = os.getenv('AUTH_STORAGE_DIR', 'auth_storage')
SCREENSHOT_DIR = os.path.join(AUTH_STORAGE_DIR, 'screenshots')

# ==================== Flask应用配置 ====================
FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
FLASK_PORT = int(os.getenv('FLASK_PORT', 8003))
FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
SECRET_KEY = os.getenv('SECRET_KEY', None)  # 生产环境必须设置
PERMANENT_SESSION_LIFETIME = int(os.getenv('SESSION_LIFETIME', 86400))  # 24小时

# ==================== 认证监控配置 ====================
AUTH_CHECK_INTERVAL = int(os.getenv('AUTH_CHECK_INTERVAL', 3600))  # 1小时

# ==================== 爬虫配置 ====================
DEFAULT_USER_AGENT = os.getenv('USER_AGENT', 
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
DEFAULT_TIMEOUT = int(os.getenv('CRAWL_TIMEOUT', 30))  # 30秒
DEFAULT_WAIT_TIME = int(os.getenv('CRAWL_WAIT_TIME', 3))  # 3秒

# ==================== 代理配置 ====================
# 是否启用代理（设置为 'true' 启用）。生产默认直连，避免客户环境误连内网代理。
PROXY_ENABLED = _env_bool('PROXY_ENABLED', False)

# HTTP代理地址，例如 http://127.0.0.1:7890
PROXY_HTTP = _env_str('PROXY_HTTP')

# HTTPS代理地址
PROXY_HTTPS = _env_str('PROXY_HTTPS')

# SOCKS5代理地址（例如：socks5://127.0.0.1:7891）
PROXY_SOCKS5 = _env_str('PROXY_SOCKS5')

# Playwright代理地址（用于浏览器），例如 http://127.0.0.1:7890
PLAYWRIGHT_PROXY = _env_str('PLAYWRIGHT_PROXY')

def has_proxy_configured():
    """Return whether any public-site proxy endpoint is configured."""
    return bool(PROXY_HTTP or PROXY_HTTPS or PROXY_SOCKS5 or PLAYWRIGHT_PROXY)


def get_proxies(enabled=None):
    """
    获取代理配置（用于requests库）
    
    Returns:
        dict: 代理配置字典，如果未启用则返回None
    """
    if enabled is None:
        enabled = PROXY_ENABLED

    if not enabled:
        return None
    
    proxies = {}
    
    # 优先使用单独配置的HTTP/HTTPS代理
    if PROXY_HTTP:
        proxies['http'] = PROXY_HTTP
    if PROXY_HTTPS:
        proxies['https'] = PROXY_HTTPS
    
    # 如果配置了SOCKS5代理，同时用于HTTP和HTTPS
    if PROXY_SOCKS5 and not proxies:
        proxies['http'] = PROXY_SOCKS5
        proxies['https'] = PROXY_SOCKS5
    
    return proxies if proxies else None


def get_requests_proxy(enabled=None):
    """
    获取单个代理地址（兼容旧代码）。

    新代码优先使用 get_proxies()，旧代码如果只接受字符串则使用该函数。
    """
    proxies = get_proxies(enabled=enabled)
    if not proxies:
        return None
    return proxies.get('https') or proxies.get('http')


def get_playwright_proxy(enabled=None):
    """
    获取Playwright代理配置
    
    Returns:
        dict: Playwright代理配置字典，如果未启用则返回None
        格式: {"server": "http://127.0.0.1:7890"}
    """
    if enabled is None:
        enabled = PROXY_ENABLED

    if not enabled:
        return None
    
    if PLAYWRIGHT_PROXY:
        return {"server": PLAYWRIGHT_PROXY}
    
    # 回退到HTTP代理
    if PROXY_HTTP:
        return {"server": PROXY_HTTP}
    
    return None


def get_ragflow_proxies():
    """
    获取RAGFlow专用代理配置。

    默认不对RAGFlow使用代理，因为RAGFlow常部署在内网；如需代理，显式设置
    RAGFLOW_PROXY_ENABLED=true，并可单独配置 RAGFLOW_PROXY_HTTP/HTTPS/SOCKS5。
    """
    if not RAGFLOW_PROXY_ENABLED:
        return None

    proxies = {}
    http_proxy = RAGFLOW_PROXY_HTTP or PROXY_HTTP
    https_proxy = RAGFLOW_PROXY_HTTPS or PROXY_HTTPS
    socks_proxy = RAGFLOW_PROXY_SOCKS5 or PROXY_SOCKS5

    if http_proxy:
        proxies['http'] = http_proxy
    if https_proxy:
        proxies['https'] = https_proxy
    if socks_proxy and not proxies:
        proxies['http'] = socks_proxy
        proxies['https'] = socks_proxy

    return proxies if proxies else None

# ==================== 日志配置 ====================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'app.log')

# ==================== 创建必要的目录 ====================
def ensure_directories():
    """确保所有必要的目录存在"""
    directories = [
        CRAWL_RESULTS_DIR,
        AUTH_STORAGE_DIR,
        SCREENSHOT_DIR,
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            print(f"[OK] created directory: {directory}")

# 初始化时创建目录
ensure_directories()

