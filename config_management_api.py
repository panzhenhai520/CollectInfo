#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Configuration management API for customer-facing deployment settings."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable

from flask import Blueprint, current_app, jsonify, request

import config
from logger_utils import log_error, log_info


config_management_bp = Blueprint(
    'config_management',
    __name__,
    url_prefix='/api/config-management',
)

ENV_PATH = Path('.env')

TRUE_VALUES = {'1', 'true', 'yes', 'on'}

BOOL_KEYS = {
    'FLASK_DEBUG',
    'PROXY_ENABLED',
    'RAGFLOW_UPLOAD_ENABLED',
    'RAGFLOW_AUTO_PARSE',
    'RAGFLOW_REUPLOAD_EXISTING',
    'RAGFLOW_PROXY_ENABLED',
    'CRAWL_DATE_RANGE_PRIORITY',
    'CRAWL_PREFILTER_CANDIDATE_DATES',
    'CRAWL_NETWORK_JSON_ENABLED',
    'CRAWL_SUPPLEMENTAL_ENABLED',
    'CRAWL_SUPPLEMENTAL_HTML_ENABLED',
    'CRAWL_SUPPLEMENTAL_ATTRIBUTES_ENABLED',
    'CRAWL_SUPPLEMENTAL_STRUCTURED_ENABLED',
    'CRAWL_SUPPLEMENTAL_SCRIPTS_ENABLED',
    'CRAWL_SUPPLEMENTAL_STATIC_PAGINATION_ENABLED',
    'CRAWL_SUPPLEMENTAL_FEEDS_ENABLED',
    'CRAWL_SUPPLEMENTAL_SITEMAPS_ENABLED',
    'CRAWL_USE_PROXY_DEFAULT',
}

INT_LIMITS = {
    'FLASK_PORT': (1, 65535),
    'SESSION_LIFETIME': (300, 2_592_000),
    'AUTH_CHECK_INTERVAL': (60, 86_400),
    'REDIS_PORT': (1, 65535),
    'REDIS_DB': (0, 15),
    'CRAWL_TIMEOUT': (5, 600),
    'CRAWL_WAIT_TIME': (0, 120),
    'CRAWL_RENDER_WAIT_MS': (1000, 60000),
    'CRAWL_PLAYWRIGHT_CLICK_TIMEOUT_MS': (250, 5000),
    'CRAWL_LINK_DISCOVERY_MAX_PAGES': (1, 1000),
    'CRAWL_LINK_DISCOVERY_MAX_PAGES_WITH_LIMIT': (1, 1000),
    'CRAWL_PLAYWRIGHT_MAX_EMPTY_PAGES': (1, 50),
    'CRAWL_DETAIL_MAX_RETRIES': (1, 5),
    'CRAWL_SUPPLEMENTAL_MAX_PER_SOURCE': (100, 5000),
    'CRAWL_SUPPLEMENTAL_MAX_SITEMAPS': (1, 200),
    'CRAWL_SUPPLEMENTAL_MAX_STATIC_PAGES': (1, 100),
    'CRAWL_SCHEDULER_MAX_CONCURRENT': (1, 32),
    'CRAWL_SCHEDULER_MAX_PER_DOMAIN': (1, 8),
    'CRAWL_SCHEDULER_RETRIES': (0, 5),
    'CRAWL_SCHEDULER_RETRY_BACKOFF': (0, 600),
    'CRAWL_SCHEDULER_DOMAIN_COOLDOWN': (0, 300),
    'CRAWL_SCHEDULER_TASK_TIMEOUT': (300, 86400),
    'CRAWL_SCHEDULER_COMPLETED_RETENTION': (60, 86400),
    'RAGFLOW_TIMEOUT': (5, 600),
    'RAGFLOW_UPLOAD_RETRIES': (0, 5),
}

SECRET_KEYS = {
    'SECRET_KEY',
    'DEFAULT_ADMIN_PASSWORD',
    'REDIS_PASSWORD',
    'RAGFLOW_API_KEY',
}

RESTART_KEYS = {
    'FLASK_HOST',
    'FLASK_PORT',
    'FLASK_DEBUG',
    'SECRET_KEY',
    'DATABASE_PATH',
    'CRAWL_RESULTS_DIR',
    'AUTH_STORAGE_DIR',
    'REDIS_HOST',
    'REDIS_PORT',
    'REDIS_DB',
    'REDIS_PASSWORD',
    'CRAWL_SCHEDULER_MAX_CONCURRENT',
}

MANAGED_KEYS = [
    # System
    'FLASK_HOST',
    'FLASK_PORT',
    'FLASK_DEBUG',
    'SECRET_KEY',
    'SESSION_LIFETIME',
    'AUTH_CHECK_INTERVAL',
    'LOG_LEVEL',
    'LOG_FILE',
    # Login bootstrap
    'DEFAULT_ADMIN_USERNAME',
    'DEFAULT_ADMIN_PASSWORD',
    'DEFAULT_ADMIN_EMAIL',
    'DEFAULT_ADMIN_FULL_NAME',
    # Storage
    'DATABASE_PATH',
    'CRAWL_RESULTS_DIR',
    'AUTH_STORAGE_DIR',
    # Redis
    'REDIS_HOST',
    'REDIS_PORT',
    'REDIS_DB',
    'REDIS_PASSWORD',
    # Scheduler
    'CRAWL_SCHEDULER_MAX_CONCURRENT',
    'CRAWL_SCHEDULER_MAX_PER_DOMAIN',
    'CRAWL_SCHEDULER_RETRIES',
    'CRAWL_SCHEDULER_RETRY_BACKOFF',
    'CRAWL_SCHEDULER_DOMAIN_COOLDOWN',
    'CRAWL_SCHEDULER_TASK_TIMEOUT',
    'CRAWL_SCHEDULER_COMPLETED_RETENTION',
    # Crawl behavior
    'USER_AGENT',
    'CRAWL_TIMEOUT',
    'CRAWL_WAIT_TIME',
    'CRAWL_RENDER_WAIT_MS',
    'CRAWL_PLAYWRIGHT_CLICK_TIMEOUT_MS',
    'CRAWL_LINK_DISCOVERY_MAX_PAGES',
    'CRAWL_LINK_DISCOVERY_MAX_PAGES_WITH_LIMIT',
    'CRAWL_PLAYWRIGHT_MAX_EMPTY_PAGES',
    'CRAWL_DETAIL_MAX_RETRIES',
    'CRAWL_DATE_RANGE_PRIORITY',
    'CRAWL_PREFILTER_CANDIDATE_DATES',
    'CRAWL_NETWORK_JSON_ENABLED',
    'CRAWL_SUPPLEMENTAL_ENABLED',
    'CRAWL_SUPPLEMENTAL_HTML_ENABLED',
    'CRAWL_SUPPLEMENTAL_ATTRIBUTES_ENABLED',
    'CRAWL_SUPPLEMENTAL_STRUCTURED_ENABLED',
    'CRAWL_SUPPLEMENTAL_SCRIPTS_ENABLED',
    'CRAWL_SUPPLEMENTAL_STATIC_PAGINATION_ENABLED',
    'CRAWL_SUPPLEMENTAL_FEEDS_ENABLED',
    'CRAWL_SUPPLEMENTAL_SITEMAPS_ENABLED',
    'CRAWL_SUPPLEMENTAL_MAX_PER_SOURCE',
    'CRAWL_SUPPLEMENTAL_MAX_SITEMAPS',
    'CRAWL_SUPPLEMENTAL_MAX_STATIC_PAGES',
    'CRAWL_USE_PROXY_DEFAULT',
    # Proxy
    'PROXY_ENABLED',
    'PROXY_HTTP',
    'PROXY_HTTPS',
    'PROXY_SOCKS5',
    'PLAYWRIGHT_PROXY',
    # RAGFlow
    'RAGFLOW_BASE_URL',
    'RAGFLOW_API_KEY',
    'RAGFLOW_UPLOAD_ENABLED',
    'RAGFLOW_AUTO_PARSE',
    'RAGFLOW_REUPLOAD_EXISTING',
    'RAGFLOW_TIMEOUT',
    'RAGFLOW_UPLOAD_RETRIES',
    'RAGFLOW_PROXY_ENABLED',
    'RAGFLOW_PROXY_HTTP',
    'RAGFLOW_PROXY_HTTPS',
    'RAGFLOW_PROXY_SOCKS5',
]

DEFAULTS = {
    'FLASK_HOST': '0.0.0.0',
    'FLASK_PORT': '8003',
    'FLASK_DEBUG': 'false',
    'SESSION_LIFETIME': '86400',
    'AUTH_CHECK_INTERVAL': '3600',
    'LOG_LEVEL': 'INFO',
    'LOG_FILE': 'app.log',
    'DEFAULT_ADMIN_USERNAME': 'admin',
    'DEFAULT_ADMIN_EMAIL': 'admin@example.com',
    'DEFAULT_ADMIN_FULL_NAME': '系统管理员',
    'DATABASE_PATH': 'crawler_articles.db',
    'CRAWL_RESULTS_DIR': 'crawl_results',
    'AUTH_STORAGE_DIR': 'auth_storage',
    'REDIS_HOST': 'localhost',
    'REDIS_PORT': '6379',
    'REDIS_DB': '1',
    'USER_AGENT': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'CRAWL_TIMEOUT': '30',
    'CRAWL_WAIT_TIME': '3',
    'CRAWL_RENDER_WAIT_MS': '8000',
    'CRAWL_PLAYWRIGHT_CLICK_TIMEOUT_MS': '1200',
    'CRAWL_LINK_DISCOVERY_MAX_PAGES': '30',
    'CRAWL_LINK_DISCOVERY_MAX_PAGES_WITH_LIMIT': '10',
    'CRAWL_PLAYWRIGHT_MAX_EMPTY_PAGES': '5',
    'CRAWL_DETAIL_MAX_RETRIES': '2',
    'CRAWL_DATE_RANGE_PRIORITY': 'true',
    'CRAWL_PREFILTER_CANDIDATE_DATES': 'true',
    'CRAWL_NETWORK_JSON_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_HTML_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_ATTRIBUTES_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_STRUCTURED_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_SCRIPTS_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_STATIC_PAGINATION_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_FEEDS_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_SITEMAPS_ENABLED': 'true',
    'CRAWL_SUPPLEMENTAL_MAX_PER_SOURCE': '500',
    'CRAWL_SUPPLEMENTAL_MAX_SITEMAPS': '25',
    'CRAWL_SUPPLEMENTAL_MAX_STATIC_PAGES': '8',
    'CRAWL_USE_PROXY_DEFAULT': '',
    'CRAWL_SCHEDULER_MAX_CONCURRENT': '4',
    'CRAWL_SCHEDULER_MAX_PER_DOMAIN': '1',
    'CRAWL_SCHEDULER_RETRIES': '2',
    'CRAWL_SCHEDULER_RETRY_BACKOFF': '20',
    'CRAWL_SCHEDULER_DOMAIN_COOLDOWN': '5',
    'CRAWL_SCHEDULER_TASK_TIMEOUT': '7200',
    'CRAWL_SCHEDULER_COMPLETED_RETENTION': '3600',
    'PROXY_ENABLED': 'false',
    'RAGFLOW_UPLOAD_ENABLED': 'false',
    'RAGFLOW_AUTO_PARSE': 'true',
    'RAGFLOW_REUPLOAD_EXISTING': 'true',
    'RAGFLOW_TIMEOUT': '45',
    'RAGFLOW_UPLOAD_RETRIES': '1',
    'RAGFLOW_PROXY_ENABLED': 'false',
}


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text == '':
        return default
    return text in TRUE_VALUES


def _clean_value(value: Any) -> str:
    value = '' if value is None else str(value).strip()
    if '\n' in value or '\r' in value:
        raise ValueError('配置值不能包含换行')
    return value


def _to_int_string(key: str, value: Any) -> str:
    min_value, max_value = INT_LIMITS[key]
    try:
        parsed = int(float(_clean_value(value)))
    except (TypeError, ValueError):
        parsed = int(DEFAULTS.get(key, min_value))
    return str(max(min_value, min(max_value, parsed)))


def _normalize_env_value(key: str, value: Any) -> str:
    if key in BOOL_KEYS:
        return 'true' if _to_bool(value, _to_bool(DEFAULTS.get(key), False)) else 'false'
    if key in INT_LIMITS:
        return _to_int_string(key, value)
    value = _clean_value(value)
    if key in {'RAGFLOW_BASE_URL'}:
        return value.rstrip('/')
    return value


def _read_env_values() -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    with ENV_PATH.open('r', encoding='utf-8') as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            if line.startswith('export '):
                line = line[7:].strip()
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _value(values: Dict[str, str], key: str) -> str:
    if key in values:
        return values.get(key) or ''
    if key in DEFAULTS:
        return DEFAULTS[key]
    runtime = getattr(config, key, '')
    return '' if runtime is None else str(runtime)


def _int_value(values: Dict[str, str], key: str) -> int:
    return int(_normalize_env_value(key, _value(values, key)))


def _bool_value(values: Dict[str, str], key: str, default: bool = False) -> bool:
    return _to_bool(_value(values, key), default)


def _format_env_line(key: str, value: str) -> str:
    return f'{key}={value}'


def _write_env_values(updates: Dict[str, str]) -> None:
    existing_lines = []
    if ENV_PATH.exists():
        existing_lines = ENV_PATH.read_text(encoding='utf-8').splitlines()

    pending = dict(updates)
    written_keys = set()
    output = []
    for raw_line in existing_lines:
        stripped = raw_line.strip()
        candidate = stripped[7:].strip() if stripped.startswith('export ') else stripped
        if candidate and not candidate.startswith('#') and '=' in candidate:
            key = candidate.split('=', 1)[0].strip()
            if key in updates and key in written_keys:
                continue
            if key in pending:
                output.append(_format_env_line(key, pending.pop(key)))
                written_keys.add(key)
                continue
        output.append(raw_line)

    if pending:
        if output and output[-1].strip():
            output.append('')
        output.append('# ==================== 配置管理页面维护 ====================')
        for key in MANAGED_KEYS:
            if key in pending:
                output.append(_format_env_line(key, pending.pop(key)))
        for key, value in pending.items():
            output.append(_format_env_line(key, value))

    ENV_PATH.write_text('\n'.join(output).rstrip() + '\n', encoding='utf-8')


def _changed_keys(current_values: Dict[str, str], updates: Dict[str, str]) -> set:
    changed = set()
    for key, new_value in updates.items():
        if (current_values.get(key) or '') != (new_value or ''):
            changed.add(key)
    return changed


def _apply_runtime_values(updates: Dict[str, str]) -> None:
    for key, value in updates.items():
        os.environ[key] = value

    config.FLASK_HOST = os.environ.get('FLASK_HOST', '0.0.0.0')
    config.FLASK_PORT = int(_normalize_env_value('FLASK_PORT', os.environ.get('FLASK_PORT') or 8003))
    config.FLASK_DEBUG = _to_bool(os.environ.get('FLASK_DEBUG'), False)
    config.SECRET_KEY = os.environ.get('SECRET_KEY') or None
    config.PERMANENT_SESSION_LIFETIME = int(
        _normalize_env_value('SESSION_LIFETIME', os.environ.get('SESSION_LIFETIME') or 86400)
    )
    config.AUTH_CHECK_INTERVAL = int(
        _normalize_env_value('AUTH_CHECK_INTERVAL', os.environ.get('AUTH_CHECK_INTERVAL') or 3600)
    )
    current_app.config['PERMANENT_SESSION_LIFETIME'] = config.PERMANENT_SESSION_LIFETIME

    config.REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    config.REDIS_PORT = int(_normalize_env_value('REDIS_PORT', os.environ.get('REDIS_PORT') or 6379))
    config.REDIS_DB = int(_normalize_env_value('REDIS_DB', os.environ.get('REDIS_DB') or 1))
    config.REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD') or None

    config.DATABASE_PATH = os.environ.get('DATABASE_PATH', 'crawler_articles.db')
    config.CRAWL_RESULTS_DIR = os.environ.get('CRAWL_RESULTS_DIR', 'crawl_results')
    config.AUTH_STORAGE_DIR = os.environ.get('AUTH_STORAGE_DIR', 'auth_storage')
    config.SCREENSHOT_DIR = os.path.join(config.AUTH_STORAGE_DIR, 'screenshots')
    config.DEFAULT_USER_AGENT = os.environ.get('USER_AGENT') or DEFAULTS['USER_AGENT']
    config.DEFAULT_TIMEOUT = int(_normalize_env_value('CRAWL_TIMEOUT', os.environ.get('CRAWL_TIMEOUT') or 30))
    config.DEFAULT_WAIT_TIME = int(_normalize_env_value('CRAWL_WAIT_TIME', os.environ.get('CRAWL_WAIT_TIME') or 3))

    config.PROXY_ENABLED = _to_bool(os.environ.get('PROXY_ENABLED'), False)
    config.PROXY_HTTP = os.environ.get('PROXY_HTTP') or None
    config.PROXY_HTTPS = os.environ.get('PROXY_HTTPS') or None
    config.PROXY_SOCKS5 = os.environ.get('PROXY_SOCKS5') or None
    config.PLAYWRIGHT_PROXY = os.environ.get('PLAYWRIGHT_PROXY') or None

    config.RAGFLOW_BASE_URL = (os.environ.get('RAGFLOW_BASE_URL') or '').rstrip('/')
    config.RAGFLOW_API_KEY = os.environ.get('RAGFLOW_API_KEY') or ''
    config.RAGFLOW_UPLOAD_ENABLED = _to_bool(
        os.environ.get('RAGFLOW_UPLOAD_ENABLED'),
        bool(config.RAGFLOW_BASE_URL and config.RAGFLOW_API_KEY),
    )
    config.RAGFLOW_AUTO_PARSE = _to_bool(os.environ.get('RAGFLOW_AUTO_PARSE'), True)
    config.RAGFLOW_REUPLOAD_EXISTING = _to_bool(os.environ.get('RAGFLOW_REUPLOAD_EXISTING'), True)
    config.RAGFLOW_TIMEOUT = int(_normalize_env_value('RAGFLOW_TIMEOUT', os.environ.get('RAGFLOW_TIMEOUT') or 45))
    config.RAGFLOW_UPLOAD_RETRIES = int(
        _normalize_env_value('RAGFLOW_UPLOAD_RETRIES', os.environ.get('RAGFLOW_UPLOAD_RETRIES') or 1)
    )
    config.RAGFLOW_PROXY_ENABLED = _to_bool(os.environ.get('RAGFLOW_PROXY_ENABLED'), False)
    config.RAGFLOW_PROXY_HTTP = os.environ.get('RAGFLOW_PROXY_HTTP') or None
    config.RAGFLOW_PROXY_HTTPS = os.environ.get('RAGFLOW_PROXY_HTTPS') or None
    config.RAGFLOW_PROXY_SOCKS5 = os.environ.get('RAGFLOW_PROXY_SOCKS5') or None
    config.LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    config.LOG_FILE = os.environ.get('LOG_FILE', 'app.log')

    config.ensure_directories()

    try:
        import ragflow_client

        ragflow_client._client_instance = None
    except Exception:
        pass

    try:
        from scheduler import scheduler

        scheduler.max_concurrent_tasks = int(os.environ.get('CRAWL_SCHEDULER_MAX_CONCURRENT', '4'))
        scheduler.max_tasks_per_domain = int(os.environ.get('CRAWL_SCHEDULER_MAX_PER_DOMAIN', '1'))
        scheduler.retry_attempts = int(os.environ.get('CRAWL_SCHEDULER_RETRIES', '2'))
        scheduler.retry_backoff_seconds = int(os.environ.get('CRAWL_SCHEDULER_RETRY_BACKOFF', '20'))
        scheduler.domain_cooldown_seconds = int(os.environ.get('CRAWL_SCHEDULER_DOMAIN_COOLDOWN', '5'))
        scheduler.task_timeout_seconds = int(os.environ.get('CRAWL_SCHEDULER_TASK_TIMEOUT', '7200'))
        scheduler.completed_task_retention_seconds = int(os.environ.get('CRAWL_SCHEDULER_COMPLETED_RETENTION', '3600'))
    except Exception:
        pass


def _secret_status(values: Dict[str, str], key: str) -> bool:
    return bool(values.get(key) or os.environ.get(key) or getattr(config, key, ''))


def _public_config(values: Dict[str, str]) -> Dict[str, Any]:
    return {
        'success': True,
        'env_path': str(ENV_PATH.resolve()),
        'restart_keys': sorted(RESTART_KEYS),
        'system': {
            'flask_host': _value(values, 'FLASK_HOST'),
            'flask_port': _int_value(values, 'FLASK_PORT'),
            'flask_debug': _bool_value(values, 'FLASK_DEBUG', False),
            'secret_key_configured': _secret_status(values, 'SECRET_KEY'),
            'session_lifetime': _int_value(values, 'SESSION_LIFETIME'),
            'auth_check_interval': _int_value(values, 'AUTH_CHECK_INTERVAL'),
            'log_level': _value(values, 'LOG_LEVEL'),
            'log_file': _value(values, 'LOG_FILE'),
        },
        'admin': {
            'username': _value(values, 'DEFAULT_ADMIN_USERNAME'),
            'password_configured': _secret_status(values, 'DEFAULT_ADMIN_PASSWORD'),
            'email': _value(values, 'DEFAULT_ADMIN_EMAIL'),
            'full_name': _value(values, 'DEFAULT_ADMIN_FULL_NAME'),
        },
        'storage': {
            'database_path': _value(values, 'DATABASE_PATH'),
            'crawl_results_dir': _value(values, 'CRAWL_RESULTS_DIR'),
            'auth_storage_dir': _value(values, 'AUTH_STORAGE_DIR'),
        },
        'redis': {
            'host': _value(values, 'REDIS_HOST'),
            'port': _int_value(values, 'REDIS_PORT'),
            'db': _int_value(values, 'REDIS_DB'),
            'password_configured': _secret_status(values, 'REDIS_PASSWORD'),
        },
        'scheduler': {
            'max_concurrent': _int_value(values, 'CRAWL_SCHEDULER_MAX_CONCURRENT'),
            'max_per_domain': _int_value(values, 'CRAWL_SCHEDULER_MAX_PER_DOMAIN'),
            'retries': _int_value(values, 'CRAWL_SCHEDULER_RETRIES'),
            'retry_backoff': _int_value(values, 'CRAWL_SCHEDULER_RETRY_BACKOFF'),
            'domain_cooldown': _int_value(values, 'CRAWL_SCHEDULER_DOMAIN_COOLDOWN'),
            'task_timeout': _int_value(values, 'CRAWL_SCHEDULER_TASK_TIMEOUT'),
            'completed_retention': _int_value(values, 'CRAWL_SCHEDULER_COMPLETED_RETENTION'),
        },
        'crawl': {
            'user_agent': _value(values, 'USER_AGENT'),
            'timeout': _int_value(values, 'CRAWL_TIMEOUT'),
            'wait_time': _int_value(values, 'CRAWL_WAIT_TIME'),
            'render_wait_ms': _int_value(values, 'CRAWL_RENDER_WAIT_MS'),
            'click_timeout_ms': _int_value(values, 'CRAWL_PLAYWRIGHT_CLICK_TIMEOUT_MS'),
            'max_pages': _int_value(values, 'CRAWL_LINK_DISCOVERY_MAX_PAGES'),
            'max_pages_with_limit': _int_value(values, 'CRAWL_LINK_DISCOVERY_MAX_PAGES_WITH_LIMIT'),
            'max_empty_pages': _int_value(values, 'CRAWL_PLAYWRIGHT_MAX_EMPTY_PAGES'),
            'detail_max_retries': _int_value(values, 'CRAWL_DETAIL_MAX_RETRIES'),
            'date_range_priority': _bool_value(values, 'CRAWL_DATE_RANGE_PRIORITY', True),
            'candidate_date_prefilter': _bool_value(values, 'CRAWL_PREFILTER_CANDIDATE_DATES', True),
            'network_json_enabled': _bool_value(values, 'CRAWL_NETWORK_JSON_ENABLED', True),
            'supplemental_enabled': _bool_value(values, 'CRAWL_SUPPLEMENTAL_ENABLED', True),
            'supplemental_html': _bool_value(values, 'CRAWL_SUPPLEMENTAL_HTML_ENABLED', True),
            'supplemental_attributes': _bool_value(values, 'CRAWL_SUPPLEMENTAL_ATTRIBUTES_ENABLED', True),
            'supplemental_structured': _bool_value(values, 'CRAWL_SUPPLEMENTAL_STRUCTURED_ENABLED', True),
            'supplemental_scripts': _bool_value(values, 'CRAWL_SUPPLEMENTAL_SCRIPTS_ENABLED', True),
            'supplemental_static_pagination': _bool_value(values, 'CRAWL_SUPPLEMENTAL_STATIC_PAGINATION_ENABLED', True),
            'supplemental_feeds': _bool_value(values, 'CRAWL_SUPPLEMENTAL_FEEDS_ENABLED', True),
            'supplemental_sitemaps': _bool_value(values, 'CRAWL_SUPPLEMENTAL_SITEMAPS_ENABLED', True),
            'supplemental_max_per_source': _int_value(values, 'CRAWL_SUPPLEMENTAL_MAX_PER_SOURCE'),
            'supplemental_max_sitemaps': _int_value(values, 'CRAWL_SUPPLEMENTAL_MAX_SITEMAPS'),
            'supplemental_max_static_pages': _int_value(values, 'CRAWL_SUPPLEMENTAL_MAX_STATIC_PAGES'),
            'use_proxy_default': _bool_value(values, 'CRAWL_USE_PROXY_DEFAULT', False),
        },
        'proxy': {
            'enabled': _bool_value(values, 'PROXY_ENABLED', getattr(config, 'PROXY_ENABLED', False)),
            'http': _value(values, 'PROXY_HTTP'),
            'https': _value(values, 'PROXY_HTTPS'),
            'socks5': _value(values, 'PROXY_SOCKS5'),
            'playwright': _value(values, 'PLAYWRIGHT_PROXY'),
        },
        'ragflow': {
            'base_url': _value(values, 'RAGFLOW_BASE_URL'),
            'api_key_configured': _secret_status(values, 'RAGFLOW_API_KEY'),
            'upload_enabled': _bool_value(values, 'RAGFLOW_UPLOAD_ENABLED', False),
            'auto_parse': _bool_value(values, 'RAGFLOW_AUTO_PARSE', True),
            'reupload_existing': _bool_value(values, 'RAGFLOW_REUPLOAD_EXISTING', True),
            'timeout': _int_value(values, 'RAGFLOW_TIMEOUT'),
            'upload_retries': _int_value(values, 'RAGFLOW_UPLOAD_RETRIES'),
            'proxy_enabled': _bool_value(values, 'RAGFLOW_PROXY_ENABLED', False),
            'proxy_http': _value(values, 'RAGFLOW_PROXY_HTTP'),
            'proxy_https': _value(values, 'RAGFLOW_PROXY_HTTPS'),
            'proxy_socks5': _value(values, 'RAGFLOW_PROXY_SOCKS5'),
        },
    }


def _secret_update(section: Dict[str, Any], key: str, clear_key: str, value_key: str, current_values: Dict[str, str]) -> Dict[str, str]:
    if section.get(clear_key):
        return {key: ''}
    value = _clean_value(section.get(value_key))
    if value:
        return {key: value}
    if key in current_values:
        return {key: current_values.get(key, '')}
    return {key: ''}


def _collect(updates: Dict[str, str], mapping: Iterable[tuple], source: Dict[str, Any]) -> None:
    for env_key, input_key in mapping:
        updates[env_key] = _normalize_env_value(env_key, source.get(input_key))


def _build_updates(data: Dict[str, Any], current_values: Dict[str, str]) -> Dict[str, str]:
    system = data.get('system') if isinstance(data.get('system'), dict) else {}
    admin = data.get('admin') if isinstance(data.get('admin'), dict) else {}
    storage = data.get('storage') if isinstance(data.get('storage'), dict) else {}
    redis_cfg = data.get('redis') if isinstance(data.get('redis'), dict) else {}
    scheduler_cfg = data.get('scheduler') if isinstance(data.get('scheduler'), dict) else {}
    crawl = data.get('crawl') if isinstance(data.get('crawl'), dict) else {}
    proxy = data.get('proxy') if isinstance(data.get('proxy'), dict) else {}
    ragflow = data.get('ragflow') if isinstance(data.get('ragflow'), dict) else {}

    updates: Dict[str, str] = {}
    _collect(updates, [
        ('FLASK_HOST', 'flask_host'),
        ('FLASK_PORT', 'flask_port'),
        ('FLASK_DEBUG', 'flask_debug'),
        ('SESSION_LIFETIME', 'session_lifetime'),
        ('AUTH_CHECK_INTERVAL', 'auth_check_interval'),
        ('LOG_LEVEL', 'log_level'),
        ('LOG_FILE', 'log_file'),
    ], system)
    updates.update(_secret_update(system, 'SECRET_KEY', 'clear_secret_key', 'secret_key', current_values))

    _collect(updates, [
        ('DEFAULT_ADMIN_USERNAME', 'username'),
        ('DEFAULT_ADMIN_EMAIL', 'email'),
        ('DEFAULT_ADMIN_FULL_NAME', 'full_name'),
    ], admin)
    updates.update(_secret_update(admin, 'DEFAULT_ADMIN_PASSWORD', 'clear_password', 'password', current_values))

    _collect(updates, [
        ('DATABASE_PATH', 'database_path'),
        ('CRAWL_RESULTS_DIR', 'crawl_results_dir'),
        ('AUTH_STORAGE_DIR', 'auth_storage_dir'),
    ], storage)

    _collect(updates, [
        ('REDIS_HOST', 'host'),
        ('REDIS_PORT', 'port'),
        ('REDIS_DB', 'db'),
    ], redis_cfg)
    updates.update(_secret_update(redis_cfg, 'REDIS_PASSWORD', 'clear_password', 'password', current_values))

    _collect(updates, [
        ('CRAWL_SCHEDULER_MAX_CONCURRENT', 'max_concurrent'),
        ('CRAWL_SCHEDULER_MAX_PER_DOMAIN', 'max_per_domain'),
        ('CRAWL_SCHEDULER_RETRIES', 'retries'),
        ('CRAWL_SCHEDULER_RETRY_BACKOFF', 'retry_backoff'),
        ('CRAWL_SCHEDULER_DOMAIN_COOLDOWN', 'domain_cooldown'),
        ('CRAWL_SCHEDULER_TASK_TIMEOUT', 'task_timeout'),
        ('CRAWL_SCHEDULER_COMPLETED_RETENTION', 'completed_retention'),
    ], scheduler_cfg)

    _collect(updates, [
        ('USER_AGENT', 'user_agent'),
        ('CRAWL_TIMEOUT', 'timeout'),
        ('CRAWL_WAIT_TIME', 'wait_time'),
        ('CRAWL_RENDER_WAIT_MS', 'render_wait_ms'),
        ('CRAWL_PLAYWRIGHT_CLICK_TIMEOUT_MS', 'click_timeout_ms'),
        ('CRAWL_LINK_DISCOVERY_MAX_PAGES', 'max_pages'),
        ('CRAWL_LINK_DISCOVERY_MAX_PAGES_WITH_LIMIT', 'max_pages_with_limit'),
        ('CRAWL_PLAYWRIGHT_MAX_EMPTY_PAGES', 'max_empty_pages'),
        ('CRAWL_DETAIL_MAX_RETRIES', 'detail_max_retries'),
        ('CRAWL_DATE_RANGE_PRIORITY', 'date_range_priority'),
        ('CRAWL_PREFILTER_CANDIDATE_DATES', 'candidate_date_prefilter'),
        ('CRAWL_NETWORK_JSON_ENABLED', 'network_json_enabled'),
        ('CRAWL_SUPPLEMENTAL_ENABLED', 'supplemental_enabled'),
        ('CRAWL_SUPPLEMENTAL_HTML_ENABLED', 'supplemental_html'),
        ('CRAWL_SUPPLEMENTAL_ATTRIBUTES_ENABLED', 'supplemental_attributes'),
        ('CRAWL_SUPPLEMENTAL_STRUCTURED_ENABLED', 'supplemental_structured'),
        ('CRAWL_SUPPLEMENTAL_SCRIPTS_ENABLED', 'supplemental_scripts'),
        ('CRAWL_SUPPLEMENTAL_STATIC_PAGINATION_ENABLED', 'supplemental_static_pagination'),
        ('CRAWL_SUPPLEMENTAL_FEEDS_ENABLED', 'supplemental_feeds'),
        ('CRAWL_SUPPLEMENTAL_SITEMAPS_ENABLED', 'supplemental_sitemaps'),
        ('CRAWL_SUPPLEMENTAL_MAX_PER_SOURCE', 'supplemental_max_per_source'),
        ('CRAWL_SUPPLEMENTAL_MAX_SITEMAPS', 'supplemental_max_sitemaps'),
        ('CRAWL_SUPPLEMENTAL_MAX_STATIC_PAGES', 'supplemental_max_static_pages'),
        ('CRAWL_USE_PROXY_DEFAULT', 'use_proxy_default'),
    ], crawl)

    _collect(updates, [
        ('PROXY_ENABLED', 'enabled'),
        ('PROXY_HTTP', 'http'),
        ('PROXY_HTTPS', 'https'),
        ('PROXY_SOCKS5', 'socks5'),
        ('PLAYWRIGHT_PROXY', 'playwright'),
    ], proxy)

    _collect(updates, [
        ('RAGFLOW_BASE_URL', 'base_url'),
        ('RAGFLOW_UPLOAD_ENABLED', 'upload_enabled'),
        ('RAGFLOW_AUTO_PARSE', 'auto_parse'),
        ('RAGFLOW_REUPLOAD_EXISTING', 'reupload_existing'),
        ('RAGFLOW_TIMEOUT', 'timeout'),
        ('RAGFLOW_UPLOAD_RETRIES', 'upload_retries'),
        ('RAGFLOW_PROXY_ENABLED', 'proxy_enabled'),
        ('RAGFLOW_PROXY_HTTP', 'proxy_http'),
        ('RAGFLOW_PROXY_HTTPS', 'proxy_https'),
        ('RAGFLOW_PROXY_SOCKS5', 'proxy_socks5'),
    ], ragflow)
    updates.update(_secret_update(ragflow, 'RAGFLOW_API_KEY', 'clear_api_key', 'api_key', current_values))

    return updates


def _restart_messages(changed_keys: set) -> list:
    reasons = []
    if changed_keys & {'FLASK_HOST', 'FLASK_PORT', 'FLASK_DEBUG'}:
        reasons.append('Web监听地址/端口需要重启服务后生效')
    if 'SECRET_KEY' in changed_keys:
        reasons.append('登录密钥需要重启后完全生效，已有登录会话可能失效')
    if changed_keys & {'DATABASE_PATH', 'CRAWL_RESULTS_DIR', 'AUTH_STORAGE_DIR'}:
        reasons.append('数据库或存储目录需要重启后让所有模块使用新路径')
    if changed_keys & {'REDIS_HOST', 'REDIS_PORT', 'REDIS_DB', 'REDIS_PASSWORD'}:
        reasons.append('Redis连接参数需要重启后重新连接')
    if 'CRAWL_SCHEDULER_MAX_CONCURRENT' in changed_keys:
        reasons.append('线程池最大并发需要重启调度器后完全生效')
    return reasons


@config_management_bp.route('/config', methods=['GET'])
def get_config():
    try:
        return jsonify(_public_config(_read_env_values()))
    except Exception as exc:
        log_error(exc, '读取配置管理')
        return jsonify({'success': False, 'error': f'读取配置失败: {exc}'}), 500


@config_management_bp.route('/config', methods=['PUT'])
def update_config():
    try:
        data = request.get_json(silent=True) or {}
        current_values = _read_env_values()
        updates = _build_updates(data, current_values)
        changed = _changed_keys(current_values, updates)
        _write_env_values(updates)
        _apply_runtime_values(updates)
        restart_reasons = _restart_messages(changed)
        message = '配置已保存'
        if restart_reasons:
            message += '；部分配置需要重启服务后完全生效'
        else:
            message += '并应用到当前进程'
        log_info(f'配置管理已更新: {", ".join(sorted(changed)) or "无变化"}', '配置管理')
        return jsonify({
            **_public_config(_read_env_values()),
            'message': message,
            'restart_required': bool(restart_reasons),
            'restart_reasons': restart_reasons,
        })
    except Exception as exc:
        log_error(exc, '更新配置管理')
        return jsonify({'success': False, 'error': f'保存配置失败: {exc}'}), 500
