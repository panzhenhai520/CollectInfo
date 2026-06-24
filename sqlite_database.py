#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SQLite数据库管理模块
用于管理文章数据的本地存储和查询
"""

import sqlite3
import json
import hashlib
import os
import re
import threading
from datetime import datetime
from utils import coerce_int, get_china_time
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse
from url_validation_helper import normalize_task_url, validate_http_url

_SOURCE_TASK_PLACEHOLDERS = {'source task', 'sourcetask', '来源任务'}


def _is_placeholder_source_task(value) -> bool:
    normalized = re.sub(r'\s+', ' ', str(value or '').strip()).lower()
    compact = normalized.replace(' ', '')
    return normalized in _SOURCE_TASK_PLACEHOLDERS or compact in _SOURCE_TASK_PLACEHOLDERS


def _split_keyword_value(value) -> List[str]:
    return [item.strip() for item in re.split(r'[,，;；、\s]+', str(value or '')) if item.strip()]


def _add_parsed_keyword(target: set, keyword) -> None:
    clean = str(keyword or '').strip()
    if not clean:
        return
    if clean.startswith('[标]') or clean.startswith('[標]'):
        clean = clean[3:].strip()
    elif clean.startswith('[文]'):
        clean = clean[3:].strip()
    if clean and not re.fullmatch(r'\d+', clean):
        target.add(clean)


def _parse_matched_keyword_text(value) -> List[str]:
    """Parse legacy and structured matched_keywords values into unique keyword names."""
    if not value:
        return []

    parsed = set()
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _add_parsed_keyword(parsed, item)
        return sorted(parsed)

    if isinstance(value, dict):
        for key in ('title_keywords', 'content_keywords', 'all_keywords'):
            for item in value.get(key) or []:
                _add_parsed_keyword(parsed, item)
        return sorted(parsed)

    raw = str(value).strip()
    if not raw:
        return []

    if raw.startswith('{') or raw.startswith('['):
        try:
            return _parse_matched_keyword_text(json.loads(raw))
        except Exception:
            pass

    location_pattern = re.compile(r'(标题|標題|title|内容|內容|正文|content)\s*[\(:：]\s*([^）)]+)', re.I)
    for match in location_pattern.finditer(raw):
        for item in _split_keyword_value(match.group(2)):
            _add_parsed_keyword(parsed, item)

    for token in [item.strip() for item in re.split(r'[,，;；]+', raw) if item.strip()]:
        _add_parsed_keyword(parsed, token)

    return sorted(parsed)


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
                os.environ[key] = value.strip().strip('"').strip("'")
    except Exception as exc:
        print(f"Warning: failed to load .env: {exc}")


_load_dotenv_file()


def _normalize_schedule_list_value(value, min_value, max_value):
    result = []
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = str(value or '').split(',')
    for item in raw_items:
        parsed = coerce_int(item, None)
        if parsed is not None and min_value <= parsed <= max_value and parsed not in result:
            result.append(parsed)
    return ','.join(str(item) for item in sorted(result))


def _normalize_schedule_time_value(value):
    parts = str(value or '00:00:00').split(':')
    hour = coerce_int(parts[0] if len(parts) > 0 else 0, 0, 0, 23)
    minute = coerce_int(parts[1] if len(parts) > 1 else 0, 0, 0, 59)
    second = coerce_int(parts[2] if len(parts) > 2 else 0, 0, 0, 59)
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def _normalize_schedule_fields(task_data):
    normalized = dict(task_data or {})
    schedule_type = normalized.get('schedule_type', 'daily')
    if schedule_type == 'weekly':
        weekdays = _normalize_schedule_list_value(normalized.get('schedule_weekdays', ''), 0, 6)
        normalized['schedule_weekdays'] = weekdays or str(get_china_time().weekday())
        normalized['schedule_monthdays'] = ''
    elif schedule_type == 'monthly':
        monthdays = _normalize_schedule_list_value(normalized.get('schedule_monthdays', ''), 1, 31)
        normalized['schedule_monthdays'] = monthdays or str(get_china_time().day)
        normalized['schedule_weekdays'] = ''
    else:
        normalized['schedule_weekdays'] = ''
        normalized['schedule_monthdays'] = ''
    return normalized


class SQLiteDatabase:
    """SQLite数据库管理类"""
    
    def __init__(self, db_path: str = None):
        """
        初始化数据库连接
        
        Args:
            db_path: 数据库文件路径
        """
        self.db_path = db_path or os.getenv('DATABASE_PATH') or os.path.join(os.getcwd(), 'crawler_articles.db')
        self.lock = threading.RLock()  # 使用可重入锁，避免死锁
        self.local = threading.local()  # 线程本地存储
        self.connection = None
        
    def _ensure_connection(self):
        """确保数据库连接已建立且可用"""
        need_reconnect = False
        
        # 检查连接是否存在
        if not self.connection:
            need_reconnect = True
        else:
            # 检查连接是否仍然有效
            try:
                # 尝试执行一个简单的查询来测试连接
                cursor = self.connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
            except (sqlite3.ProgrammingError, sqlite3.OperationalError, AttributeError) as e:
                print(f"🔄 数据库连接无效，需要重连: {e}")
                need_reconnect = True
        
        if need_reconnect:
            # 先清理旧连接
            try:
                if hasattr(self, 'connection') and self.connection:
                    self.connection.close()
            except:
                pass
            
            # 重新连接
            if not self.connect():
                raise Exception("无法连接到数据库")
    
    def connect(self) -> bool:
        """
        连接数据库
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # 确保目录存在
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)
            
            self.connection = sqlite3.connect(
                self.db_path, 
                check_same_thread=False,
                timeout=30.0,
                isolation_level=None  # 自动提交模式
            )
            self.connection.row_factory = sqlite3.Row  # 使结果可以按列名访问
            
            # 启用外键约束和优化设置
            cursor = self.connection.cursor()
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.execute("PRAGMA journal_mode = WAL")  # 使用WAL模式，提高并发性能
            cursor.execute("PRAGMA synchronous = NORMAL")  # 平衡性能和安全
            cursor.execute("PRAGMA busy_timeout = 30000")  # 30秒超时
            self._migrate_existing_schema(cursor)
            cursor.close()
            
            print(f"✅ SQLite数据库连接成功: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ SQLite数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开数据库连接"""
        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()
            print("🔌 SQLite数据库连接已断开")
        except Exception as e:
            print(f"❌ 断开数据库连接失败: {e}")
    
    def create_tables(self) -> bool:
        """
        创建数据库表
        
        Returns:
            bool: 创建是否成功
        """
        try:
            with self.lock:
                # 创建文章表
                create_articles_table = """
                CREATE TABLE IF NOT EXISTS articles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    content TEXT,
                    domain TEXT,
                    category_id INTEGER,
                    source_url_id INTEGER,
                    publish_date DATE,
                    content_hash TEXT,
                    source_task_id TEXT,
                    source_task_name TEXT,
                    first_crawled TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    last_crawled TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    crawl_count INTEGER DEFAULT 1,
                    content_length INTEGER DEFAULT 0,
                    extraction_method TEXT,
                    quality_score REAL DEFAULT 0,
                    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'deleted', 'archived')),
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
                    FOREIGN KEY (source_url_id) REFERENCES managed_urls(id) ON DELETE SET NULL
                )
                """
                
                # 创建爬取任务表
                create_tasks_table = """
                CREATE TABLE IF NOT EXISTS crawl_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL UNIQUE,
                    target_url TEXT NOT NULL,
                    task_name TEXT,
                    crawl_depth INTEGER DEFAULT 1,
                    crawl_mode TEXT DEFAULT 'standard',
                    page_limit INTEGER DEFAULT 50,
                    incremental_mode BOOLEAN DEFAULT FALSE,
                    keywords TEXT,
                    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
                    progress INTEGER DEFAULT 0,
                    articles_found INTEGER DEFAULT 0,
                    articles_processed INTEGER DEFAULT 0,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime'))
                )
                """
                
                # 创建文章-任务关联表
                create_article_tasks_table = """
                CREATE TABLE IF NOT EXISTS article_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_id INTEGER NOT NULL,
                    task_id TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
                    FOREIGN KEY (task_id) REFERENCES crawl_tasks(task_id) ON DELETE CASCADE,
                    UNIQUE(article_id, task_id)
                )
                """
                
                # 创建统计表
                create_stats_table = """
                CREATE TABLE IF NOT EXISTS crawl_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date DATE NOT NULL UNIQUE,
                    total_articles INTEGER DEFAULT 0,
                    new_articles INTEGER DEFAULT 0,
                    total_domains INTEGER DEFAULT 0,
                    total_tasks INTEGER DEFAULT 0,
                    completed_tasks INTEGER DEFAULT 0,
                    failed_tasks INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime'))
                )
                """
                
                # 创建分类表
                create_categories_table = """
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    display_order INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime'))
                )
                """
                
                # 创建URL管理表
                create_urls_table = """
                CREATE TABLE IF NOT EXISTS managed_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    name TEXT,
                    description TEXT,
                    category_id INTEGER,
                    parent_url_id INTEGER,
                    domain TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    auto_crawl BOOLEAN DEFAULT FALSE,
                    crawl_frequency TEXT,
                    auth_config TEXT,
                    keywords TEXT,
                    access_status TEXT,
                    access_checked_at TIMESTAMP,
                    access_status_code INTEGER,
                    access_error TEXT,
                    last_crawled TIMESTAMP,
                    next_crawl TIMESTAMP,
                    total_crawls INTEGER DEFAULT 0,
                    success_crawls INTEGER DEFAULT 0,
                    failed_crawls INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
                    FOREIGN KEY (parent_url_id) REFERENCES managed_urls(id) ON DELETE CASCADE
                )
                """
                
                # 创建定时任务表
                create_schedules_table = """
                CREATE TABLE IF NOT EXISTS scheduled_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL,
                    task_type TEXT NOT NULL CHECK (task_type IN ('crawl', 'extract', 'export')),
                    target_url TEXT,
                    url_id INTEGER,
                    schedule_type TEXT NOT NULL CHECK (schedule_type IN ('once', 'daily', 'weekly', 'monthly', 'cron')),
                    schedule_time TIME,
                    schedule_day INTEGER,
                    cron_expression TEXT,
                    keywords TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_run TIMESTAMP,
                    next_run TIMESTAMP,
                    total_runs INTEGER DEFAULT 0,
                    success_runs INTEGER DEFAULT 0,
                    failed_runs INTEGER DEFAULT 0,
                    ragflow_kb_id TEXT,
                    days_limit INTEGER DEFAULT 7,
                    running_lock_id TEXT,
                    running_started_at TIMESTAMP,
                    config JSON,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    updated_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (url_id) REFERENCES managed_urls(id) ON DELETE CASCADE
                )
                """
                
                # 创建任务执行历史表
                create_task_history_table = """
                CREATE TABLE IF NOT EXISTS task_execution_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    schedule_id INTEGER,
                    task_id TEXT,
                    status TEXT CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'timeout', 'skipped')),
                    run_key TEXT,
                    scheduled_for TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    duration_seconds INTEGER,
                    articles_found INTEGER DEFAULT 0,
                    error_message TEXT,
                    result_summary JSON,
                    created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                    FOREIGN KEY (schedule_id) REFERENCES scheduled_tasks(id) ON DELETE CASCADE,
                    FOREIGN KEY (task_id) REFERENCES crawl_tasks(task_id) ON DELETE SET NULL
                )
                """
                
                # 执行创建表的SQL
                tables = [
                    create_categories_table,
                    create_articles_table,
                    create_tasks_table,
                    create_article_tasks_table,
                    create_stats_table,
                    create_urls_table,
                    create_schedules_table,
                    create_task_history_table
                ]
                
                cursor = self.connection.cursor()
                try:
                    for table_sql in tables:
                        cursor.execute(table_sql)
                    
                    self._run_schema_migrations(cursor)
                    # 创建索引
                    self._create_indexes(cursor)
                    
                    self.connection.commit()
                finally:
                    cursor.close()
                print("✅ SQLite数据库表创建成功")
                return True
                
        except Exception as e:
            print(f"❌ 创建数据库表失败: {e}")
            return False
    
    def _create_indexes(self, cursor=None):
        """创建索引"""
        indexes = [
            # 分类表索引
            "CREATE INDEX IF NOT EXISTS idx_categories_name ON categories(name)",
            "CREATE INDEX IF NOT EXISTS idx_categories_is_active ON categories(is_active)",
            # 文章表索引
            "CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url)",
            "CREATE INDEX IF NOT EXISTS idx_articles_domain ON articles(domain)",
            "CREATE INDEX IF NOT EXISTS idx_articles_category_id ON articles(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_articles_source_url_id ON articles(source_url_id)",
            "CREATE INDEX IF NOT EXISTS idx_articles_source_task_id ON articles(source_task_id)",
            "CREATE INDEX IF NOT EXISTS idx_articles_publish_date ON articles(publish_date)",
            "CREATE INDEX IF NOT EXISTS idx_articles_last_crawled ON articles(last_crawled)",
            "CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status)",
            # 爬取任务表索引
            "CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON crawl_tasks(task_id)",
            "CREATE INDEX IF NOT EXISTS idx_tasks_status ON crawl_tasks(status)",
            "CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON crawl_tasks(created_at)",
            # 文章-任务关联表索引
            "CREATE INDEX IF NOT EXISTS idx_article_tasks_article_id ON article_tasks(article_id)",
            "CREATE INDEX IF NOT EXISTS idx_article_tasks_task_id ON article_tasks(task_id)",
            # 统计表索引
            "CREATE INDEX IF NOT EXISTS idx_stats_date ON crawl_stats(date)",
            # URL管理表索引
            "CREATE INDEX IF NOT EXISTS idx_urls_url ON managed_urls(url)",
            "CREATE INDEX IF NOT EXISTS idx_urls_domain ON managed_urls(domain)",
            "CREATE INDEX IF NOT EXISTS idx_urls_category_id ON managed_urls(category_id)",
            "CREATE INDEX IF NOT EXISTS idx_urls_parent_url_id ON managed_urls(parent_url_id)",
            "CREATE INDEX IF NOT EXISTS idx_urls_is_active ON managed_urls(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_urls_auto_crawl ON managed_urls(auto_crawl)",
            "CREATE INDEX IF NOT EXISTS idx_urls_access_status ON managed_urls(access_status)",
            # 定时任务表索引
            "CREATE INDEX IF NOT EXISTS idx_schedules_is_active ON scheduled_tasks(is_active)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON scheduled_tasks(next_run)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_url_id ON scheduled_tasks(url_id)",
            "CREATE INDEX IF NOT EXISTS idx_schedules_running_lock ON scheduled_tasks(running_lock_id)",
            # 任务执行历史表索引
            "CREATE INDEX IF NOT EXISTS idx_history_schedule_id ON task_execution_history(schedule_id)",
            "CREATE INDEX IF NOT EXISTS idx_history_task_id ON task_execution_history(task_id)",
            "CREATE INDEX IF NOT EXISTS idx_history_status ON task_execution_history(status)",
            "CREATE INDEX IF NOT EXISTS idx_history_started_at ON task_execution_history(started_at)",
            "CREATE INDEX IF NOT EXISTS idx_history_scheduled_for ON task_execution_history(scheduled_for)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_history_run_key_unique ON task_execution_history(run_key) WHERE run_key IS NOT NULL AND TRIM(run_key) != ''"
        ]
        
        if cursor is None:
            cursor = self.connection.cursor()
            cursor_created = True
        else:
            cursor_created = False
            
        try:
            for index_sql in indexes:
                cursor.execute(index_sql)
        finally:
            if cursor_created:
                cursor.close()

    def _run_schema_migrations(self, cursor=None):
        """运行增量表结构迁移"""
        cursor_created = False
        if cursor is None:
            cursor = self.connection.cursor()
            cursor_created = True
        try:
            self._ensure_column_exists(cursor, 'scheduled_tasks', 'ragflow_kb_id', 'TEXT')
            self._ensure_column_exists(cursor, 'scheduled_tasks', 'days_limit', 'INTEGER DEFAULT 7')  # 🔥 添加日期限制字段
            self._ensure_column_exists(cursor, 'scheduled_tasks', 'schedule_weekdays', 'TEXT')  # 每周哪几天执行
            self._ensure_column_exists(cursor, 'scheduled_tasks', 'schedule_monthdays', 'TEXT')  # 每月哪几天执行
            self._ensure_column_exists(cursor, 'scheduled_tasks', 'running_lock_id', 'TEXT')
            self._ensure_column_exists(cursor, 'scheduled_tasks', 'running_started_at', 'TIMESTAMP')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_status', 'TEXT')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_checked_at', 'TIMESTAMP')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_status_code', 'INTEGER')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_error', 'TEXT')
            self._ensure_column_exists(cursor, 'articles', 'matched_keywords', 'TEXT')  # 🔥 文章匹配的关键词
            self._ensure_column_exists(cursor, 'articles', 'source_task_id', 'TEXT')
            self._ensure_column_exists(cursor, 'articles', 'source_task_name', 'TEXT')
            self._ensure_column_exists(cursor, 'task_execution_history', 'run_key', 'TEXT')
            self._ensure_column_exists(cursor, 'task_execution_history', 'scheduled_for', 'TIMESTAMP')
            self._backfill_article_source_tasks(cursor)
            self._migrate_task_execution_history_statuses(cursor)
        finally:
            if cursor_created:
                cursor.close()

    def _resolve_task_source(self, cursor, task_id: str) -> Tuple[str, Optional[str]]:
        """Return a user-facing task name and schedule id for an exact task id."""
        if not task_id:
            return '', None

        task_name = ''
        schedule_id = None
        schedule_match = re.match(r'^schedule_(\d+)_', str(task_id))
        if schedule_match:
            schedule_id = schedule_match.group(1)
            cursor.execute(
                "SELECT task_name FROM scheduled_tasks WHERE id = ?",
                (schedule_id,)
            )
            schedule_row = cursor.fetchone()
            if schedule_row and schedule_row['task_name'] and not _is_placeholder_source_task(schedule_row['task_name']):
                task_name = schedule_row['task_name']

        if not task_name:
            cursor.execute(
                "SELECT task_name FROM crawl_tasks WHERE task_id = ?",
                (task_id,)
            )
            task_row = cursor.fetchone()
            if task_row and task_row['task_name'] and not _is_placeholder_source_task(task_row['task_name']):
                task_name = task_row['task_name']

        return task_name or str(task_id), schedule_id

    def _backfill_article_source_tasks(self, cursor):
        """Populate article source task fields from exact article_tasks links."""
        cursor.execute(
            """
            SELECT a.id, latest.task_id
            FROM articles a
            JOIN (
                SELECT at1.article_id, at1.task_id
                FROM article_tasks at1
                JOIN (
                    SELECT article_id, MAX(id) AS latest_id
                    FROM article_tasks
                    GROUP BY article_id
                ) latest_link ON latest_link.latest_id = at1.id
            ) latest ON latest.article_id = a.id
            WHERE (a.source_task_id IS NULL OR TRIM(a.source_task_id) = '')
               OR (a.source_task_name IS NULL OR TRIM(a.source_task_name) = '')
               OR LOWER(TRIM(a.source_task_name)) = 'source task'
               OR REPLACE(LOWER(TRIM(a.source_task_name)), ' ', '') = 'sourcetask'
               OR TRIM(a.source_task_name) = '来源任务'
            """
        )
        rows = cursor.fetchall()
        for row in rows:
            task_id = row['task_id']
            task_name, _schedule_id = self._resolve_task_source(cursor, task_id)
            cursor.execute(
                """
                UPDATE articles
                SET source_task_id = ?,
                    source_task_name = ?
                WHERE id = ?
                """,
                (task_id, task_name, row['id'])
            )

    def _migrate_task_execution_history_statuses(self, cursor):
        """Rebuild old history tables whose CHECK constraint lacks current statuses."""
        cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'task_execution_history'"
        )
        row = cursor.fetchone()
        table_sql = ''
        if row:
            try:
                table_sql = row['sql'] if isinstance(row, sqlite3.Row) else row[0]
            except Exception:
                table_sql = ''
        if not table_sql or "'completed'" in table_sql:
            return

        backup_table = 'task_execution_history_migration_backup'
        cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
        cursor.execute(f"ALTER TABLE task_execution_history RENAME TO {backup_table}")
        cursor.execute(
            """
            CREATE TABLE task_execution_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER,
                task_id TEXT,
                status TEXT CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'timeout', 'skipped')),
                run_key TEXT,
                scheduled_for TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                duration_seconds INTEGER,
                articles_found INTEGER DEFAULT 0,
                error_message TEXT,
                result_summary JSON,
                created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                FOREIGN KEY (schedule_id) REFERENCES scheduled_tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (task_id) REFERENCES crawl_tasks(task_id) ON DELETE SET NULL
            )
            """
        )
        cursor.execute(
            f"""
            INSERT INTO task_execution_history (
                id, schedule_id, task_id, status, run_key, scheduled_for, started_at, completed_at,
                duration_seconds, articles_found, error_message, result_summary, created_at
            )
            SELECT
                id,
                schedule_id,
                task_id,
                CASE WHEN status = 'success' THEN 'completed' ELSE status END,
                NULL,
                NULL,
                started_at,
                completed_at,
                duration_seconds,
                articles_found,
                error_message,
                result_summary,
                created_at
            FROM {backup_table}
            """
        )
        cursor.execute(f"DROP TABLE {backup_table}")

    def _ensure_column_exists(self, cursor, table_name: str, column_name: str, column_definition: str):
        """如果缺少列则自动添加"""
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row['name'] if isinstance(row, sqlite3.Row) else row[1] for row in cursor.fetchall()]
        if column_name not in columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")
    
    def insert_article(self, article_data: Dict) -> Optional[int]:
        """
        插入文章数据
        
        Args:
            article_data: 文章数据字典
            
        Returns:
            Optional[int]: 插入的文章ID，失败返回None
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 获取原始URL
                    url = article_data.get('url', '')
                    
                    # 🔥 过滤图片URL
                    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico')
                    if url.lower().endswith(image_extensions) or '/upload/editor-images/' in url.lower():
                        print(f"⏭️ 跳过图片URL: {url[:80]}...")
                        return None
                    
                    # 🔧 URL标准化：已禁用
                    # 原因：我们使用Playwright的page.url，这已经是正确的URL了
                    # 不需要再次转换，否则会把正确的URL转换成错误的
                    # try:
                    #     from url_transformation_rules import transform_url
                    #     normalized_url = transform_url(url, verbose=False, verify=False)
                    #     if normalized_url != url:
                    #         print(f"🔄 URL标准化: {url[:60]}... → {normalized_url[:60]}...")
                    #         url = normalized_url
                    #         article_data['url'] = url
                    # except Exception as e:
                    #     print(f"⚠️  URL标准化失败: {e}，使用原始URL")
                    
                    # 生成内容哈希
                    content = article_data.get('content', '')
                    content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
                    
                    # 提取域名
                    domain = self._extract_domain(url)
                    
                    # 获取标题
                    title = article_data.get('title', '无标题')
                    
                    # 处理发布日期
                    publish_date = article_data.get('publish_date')
                    if publish_date and isinstance(publish_date, str):
                        try:
                            publish_date = datetime.strptime(publish_date, '%Y-%m-%d').date()
                        except:
                            publish_date = None
                    
                    # ========== 🔥 多重去重检查 ==========
                    existing_id = None
                    
                    # 检查1: 通过URL去重
                    existing_id = self.get_article_id_by_url(url)
                    if existing_id:
                        print(f"🔁 检测到重复文章(URL相同): {title[:30]}... (ID: {existing_id})")
                        return self.update_article(existing_id, article_data)
                    
                    # 检查2: 通过标题+域名去重
                    check_sql = """
                    SELECT id FROM articles 
                    WHERE title = ? AND domain = ? AND status = 'active'
                    LIMIT 1
                    """
                    cursor.execute(check_sql, (title, domain))
                    result = cursor.fetchone()
                    if result:
                        existing_id = result[0]
                        print(f"🔁 检测到重复文章(标题+域名相同): {title[:30]}... (ID: {existing_id})")
                        print(f"   已存在URL与新URL不同，更新为新URL: {url[:60]}...")
                        return self.update_article(existing_id, article_data)
                    
                    # 检查3: 通过内容哈希+域名去重（防止标题略有不同但内容相同）
                    # 🔥 只有当内容足够长（>=300字）时才进行内容哈希去重，避免提取失败导致误判
                    if content and len(content) >= 300:
                        check_sql = """
                        SELECT id, title, content_length FROM articles 
                        WHERE content_hash = ? AND domain = ? AND status = 'active'
                        LIMIT 1
                        """
                        cursor.execute(check_sql, (content_hash, domain))
                        result = cursor.fetchone()
                        if result:
                            existing_id = result[0]
                            existing_title = result[1]
                            existing_length = result[2] or 0
                            # 🔥 只有当已存在文章也足够长时才认为是重复
                            if existing_length >= 300:
                                print(f"🔁 检测到重复文章(内容哈希+域名相同): {title[:30]}...")
                                print(f"   已存在文章: {existing_title[:30]}... (ID: {existing_id})")
                                print(f"   标题或URL略有不同，更新为新数据")
                                return self.update_article(existing_id, article_data)
                    
                    # 插入新文章
                    # 手动设置中国时间，确保时区正确
                    china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 获取匹配的关键词（如果有的话）
                    matched_keywords = article_data.get('matched_keywords', '')
                    if isinstance(matched_keywords, list):
                        matched_keywords = ','.join(matched_keywords)
                    
                    insert_sql = """
                    INSERT INTO articles (
                        url, title, content, domain, category_id, source_url_id, 
                        publish_date, content_hash, content_length, extraction_method, quality_score,
                        matched_keywords, source_task_id, source_task_name, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        url,
                        title,
                        content,
                        domain,
                        article_data.get('category_id'),
                        article_data.get('source_url_id'),
                        publish_date,
                        content_hash,
                        len(content),
                        article_data.get('extraction_method', 'unknown'),
                        article_data.get('quality_score', 0),
                        matched_keywords,
                        article_data.get('source_task_id') or '',
                        article_data.get('source_task_name') or '',
                        china_time,
                        china_time
                    )
                    
                    cursor.execute(insert_sql, values)
                    article_id = cursor.lastrowid
                    self.connection.commit()
                    
                    print(f"✅ 文章入库成功: {title[:30]}... (ID: {article_id})")
                    return article_id
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 插入文章失败: {e}")
            return None
    
    def link_article_to_task(self, article_id: int, task_id: str) -> bool:
        """
        创建文章与任务的关联
        
        Args:
            article_id: 文章ID
            task_id: 任务ID
            
        Returns:
            bool: 是否成功
        """
        if not article_id or not task_id:
            return False
            
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 使用INSERT OR IGNORE避免重复插入
                    cursor.execute("""
                        INSERT OR IGNORE INTO article_tasks (article_id, task_id)
                        VALUES (?, ?)
                    """, (article_id, task_id))
                    source_task_name, _schedule_id = self._resolve_task_source(cursor, task_id)
                    cursor.execute(
                        """
                        UPDATE articles
                        SET source_task_id = ?,
                            source_task_name = ?,
                            updated_at = datetime('now', 'localtime')
                        WHERE id = ?
                        """,
                        (task_id, source_task_name, article_id)
                    )
                    self.connection.commit()
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"⚠️ 创建文章-任务关联失败: {e}")
            return False
    
    def update_article(self, article_id: int, article_data: Dict) -> Optional[int]:
        """
        更新文章数据
        
        Args:
            article_id: 文章ID
            article_data: 文章数据字典
            
        Returns:
            Optional[int]: 成功返回文章ID，失败返回None
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 生成内容哈希
                    content = article_data.get('content', '')
                    content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
                    
                    # 处理发布日期
                    publish_date = article_data.get('publish_date')
                    if publish_date and isinstance(publish_date, str):
                        try:
                            publish_date = datetime.strptime(publish_date, '%Y-%m-%d').date()
                        except:
                            publish_date = None

                    matched_keywords = article_data.get('matched_keywords')
                    if isinstance(matched_keywords, list):
                        matched_keywords = ','.join(matched_keywords)
                    
                    update_sql = """
                    UPDATE articles SET
                        title = ?,
                        content = ?,
                        category_id = ?,
                        source_url_id = ?,
                        publish_date = ?,
                        content_hash = ?,
                        content_length = ?,
                        extraction_method = ?,
                        quality_score = ?,
                        source_task_id = CASE
                            WHEN ? IS NULL OR ? = '' THEN source_task_id
                            ELSE ?
                        END,
                        source_task_name = CASE
                            WHEN ? IS NULL OR ? = '' THEN source_task_name
                            ELSE ?
                        END,
                        matched_keywords = CASE
                            WHEN ? IS NULL OR ? = '' THEN matched_keywords
                            ELSE ?
                        END,
                        crawl_count = crawl_count + 1,
                        last_crawled = datetime('now', 'localtime'),
                        updated_at = datetime('now', 'localtime')
                    WHERE id = ?
                    """
                    
                    values = (
                        article_data.get('title', '无标题'),
                        content,
                        article_data.get('category_id'),
                        article_data.get('source_url_id'),
                        publish_date,
                        content_hash,
                        len(content),
                        article_data.get('extraction_method', 'unknown'),
                        article_data.get('quality_score', 0),
                        article_data.get('source_task_id'),
                        article_data.get('source_task_id'),
                        article_data.get('source_task_id'),
                        article_data.get('source_task_name'),
                        article_data.get('source_task_name'),
                        article_data.get('source_task_name'),
                        matched_keywords,
                        matched_keywords,
                        matched_keywords,
                        article_id
                    )
                    
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    print(f"✅ 文章更新成功: ID {article_id}")
                    return article_id
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新文章失败: {e}")
            return None
    
    def get_article_id_by_url(self, url: str) -> Optional[int]:
        """
        根据URL获取文章ID
        
        Args:
            url: 文章URL
            
        Returns:
            Optional[int]: 文章ID，不存在返回None
        """
        try:
            # 🔧 URL标准化：已禁用
            # 原因：我们使用Playwright的page.url，这已经是正确的URL了
            # 不需要再次转换
            # try:
            #     from url_transformation_rules import transform_url
            #     normalized_url = transform_url(url, verbose=False, verify=False)
            #     if normalized_url != url:
            #         url = normalized_url
            # except:
            #     pass
            
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = "SELECT id FROM articles WHERE url = ? AND status = 'active'"
                    cursor.execute(select_sql, (url,))
                    result = cursor.fetchone()
                    return result['id'] if result else None
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 查询文章ID失败: {e}")
            return None
    
    def check_article_exists(self, url: str) -> bool:
        """
        检查文章是否已存在
        
        Args:
            url: 文章URL
            
        Returns:
            bool: 文章是否存在
        """
        try:
            article_id = self.get_article_id_by_url(url)
            return article_id is not None
        except Exception as e:
            print(f"❌ 检查文章是否存在失败: {e}")
            return False
    
    def is_article_exists(self, url: str) -> bool:
        """
        检查文章是否存在
        
        Args:
            url: 文章URL
            
        Returns:
            bool: 文章是否存在
        """
        return self.get_article_id_by_url(url) is not None

    def _lookup_keywords_for_article(self, cursor, article: Dict) -> str:
        """Best-effort keyword lookup for old articles without matched_keywords."""
        try:
            source_url_id = article.get('source_url_id')
            if source_url_id:
                cursor.execute("SELECT keywords FROM managed_urls WHERE id = ?", (source_url_id,))
                row = cursor.fetchone()
                if row and row['keywords']:
                    return row['keywords']

                cursor.execute("SELECT keywords FROM scheduled_tasks WHERE url_id = ? ORDER BY id DESC LIMIT 1", (source_url_id,))
                row = cursor.fetchone()
                if row and row['keywords']:
                    return row['keywords']

            article_id = article.get('id')
            if article_id:
                cursor.execute(
                    """
                    SELECT ct.keywords
                    FROM article_tasks at
                    JOIN crawl_tasks ct ON at.task_id = ct.task_id
                    WHERE at.article_id = ? AND ct.keywords IS NOT NULL AND TRIM(ct.keywords) != ''
                    ORDER BY ct.id DESC
                    LIMIT 1
                    """,
                    (article_id,)
                )
                row = cursor.fetchone()
                if row and row['keywords']:
                    return row['keywords']

            domain = article.get('domain')
            if domain:
                cursor.execute(
                    """
                    SELECT keywords
                    FROM managed_urls
                    WHERE domain = ? AND keywords IS NOT NULL AND TRIM(keywords) != ''
                    ORDER BY parent_url_id IS NOT NULL, id DESC
                    LIMIT 1
                    """,
                    (domain,)
                )
                row = cursor.fetchone()
                if row and row['keywords']:
                    return row['keywords']
        except Exception as e:
            print(f"⚠️ 推断文章关键词配置失败: {e}")

        return ''

    def _hydrate_matched_keywords_for_display(self, cursor, article: Dict) -> None:
        """Populate matched_keywords for display when old rows did not store it."""
        if not article or article.get('matched_keywords'):
            return

        keywords = self._lookup_keywords_for_article(cursor, article)
        if not keywords:
            return

        try:
            from keyword_filter import KeywordFilter
            keyword_filter = KeywordFilter(keywords)
            if not keyword_filter.is_enabled():
                return

            match_result = keyword_filter.get_matched_keywords_by_location(
                article.get('title', ''),
                article.get('content', '')
            )
            article['matched_keywords'] = match_result.get('matched_keywords_str', '')
        except Exception as e:
            print(f"⚠️ 计算文章匹配关键词失败: {e}")
    
    def get_articles(self, page: int = 1, per_page: int = 20, 
                    domain: str = None, category_id: int = None, source_url_id: int = None, search: str = None,
                    keyword: str = None) -> Tuple[List[Dict], int]:
        """
        获取文章列表
        
        Args:
            page: 页码
            per_page: 每页数量
            domain: 域名过滤
            category_id: 分类过滤
            source_url_id: 来源URL过滤
            search: 搜索关键词
            keyword: 匹配关键词过滤
            
        Returns:
            Tuple[List[Dict], int]: (文章列表, 总数)
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 构建查询条件
                    where_conditions = ["a.status = 'active'"]
                    params = []
                    
                    if domain:
                        where_conditions.append("a.domain = ?")
                        params.append(domain)
                    
                    if category_id:
                        where_conditions.append("a.category_id = ?")
                        params.append(category_id)
                    
                    if source_url_id:
                        where_conditions.append("a.source_url_id = ?")
                        params.append(source_url_id)
                    
                    if search:
                        where_conditions.append("(a.title LIKE ? OR a.content LIKE ?)")
                        search_param = f"%{search}%"
                        params.extend([search_param, search_param])

                    if keyword:
                        if re.fullmatch(r'\d+', str(keyword).strip()):
                            return [], 0
                        keyword_param = f"%{keyword}%"
                        where_conditions.append("a.matched_keywords LIKE ?")
                        params.append(keyword_param)
                    
                    where_clause = " AND ".join(where_conditions)
                    
                    # 获取总数
                    count_sql = f"SELECT COUNT(*) as total FROM articles a WHERE {where_clause}"
                    cursor.execute(count_sql, params)
                    total = cursor.fetchone()['total']
                    
                    # 获取文章列表（关联分类和来源URL信息）
                    offset = (page - 1) * per_page
                    select_sql = f"""
                    SELECT
                        a.*,
                        c.name as category_name,
                        COALESCE(mu.name,
                            (SELECT mu2.name FROM managed_urls mu2
                             WHERE REPLACE(REPLACE(a.url,'https://',''),'http://','')
                                   LIKE REPLACE(REPLACE(mu2.url,'https://',''),'http://','') || '%'
                             ORDER BY LENGTH(mu2.url) DESC LIMIT 1)
                        ) as source_url_name,
                        (
                            SELECT at.task_id
                            FROM article_tasks at
                            WHERE at.article_id = a.id
                            ORDER BY at.created_at DESC, at.id DESC
                            LIMIT 1
                        ) AS latest_task_id,
                        (
                            SELECT ct.task_name
                            FROM article_tasks at
                            LEFT JOIN crawl_tasks ct ON at.task_id = ct.task_id
                            WHERE at.article_id = a.id
                            ORDER BY at.created_at DESC, at.id DESC
                            LIMIT 1
                        ) AS latest_task_name,
                        (
                            SELECT ct.target_url
                            FROM article_tasks at
                            LEFT JOIN crawl_tasks ct ON at.task_id = ct.task_id
                            WHERE at.article_id = a.id
                            ORDER BY at.created_at DESC, at.id DESC
                            LIMIT 1
                        ) AS latest_task_target_url
                    FROM articles a
                    LEFT JOIN categories c ON a.category_id = c.id
                    LEFT JOIN managed_urls mu ON a.source_url_id = mu.id
                    WHERE {where_clause}
                    ORDER BY a.last_crawled DESC
                    LIMIT ? OFFSET ?
                    """
                    params.extend([per_page, offset])
                    
                    cursor.execute(select_sql, params)
                    articles = [dict(row) for row in cursor.fetchall()]
                    
                    # 转换日期格式
                    for article in articles:
                        if article['publish_date']:
                            article['publish_date'] = str(article['publish_date'])
                        if article['first_crawled']:
                            article['first_crawled'] = article['first_crawled']
                        if article['last_crawled']:
                            article['last_crawled'] = article['last_crawled']
                        self._hydrate_matched_keywords_for_display(cursor, article)
                        task_id = article.get('source_task_id') or article.get('latest_task_id') or ''
                        schedule_match = re.match(r'^schedule_(\d+)_', task_id)
                        article['latest_schedule_id'] = schedule_match.group(1) if schedule_match else None
                        scheduled_task_name = ''
                        if article['latest_schedule_id']:
                            cursor.execute(
                                "SELECT task_name FROM scheduled_tasks WHERE id = ?",
                                (article['latest_schedule_id'],)
                            )
                            schedule_row = cursor.fetchone()
                            scheduled_task_name = schedule_row['task_name'] if schedule_row and schedule_row['task_name'] and not _is_placeholder_source_task(schedule_row['task_name']) else ''
                        article['latest_task_id'] = task_id
                        source_task_name = article.get('source_task_name')
                        if _is_placeholder_source_task(source_task_name):
                            source_task_name = ''
                        latest_task_name = article.get('latest_task_name')
                        if _is_placeholder_source_task(latest_task_name):
                            latest_task_name = ''
                        article['latest_task_display_name'] = (
                            source_task_name
                            or scheduled_task_name
                            or latest_task_name
                            or task_id
                            or ''
                        )
                    
                    return articles, total
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 获取文章列表失败: {e}")
            return [], 0

    def get_keyword_map(self, limit: int = 500) -> List[Dict]:
        """Build a keyword information map from active articles."""
        try:
            self._ensure_connection()
            limit = coerce_int(limit, 500, 1, 5000)
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute("PRAGMA table_info(articles)")
                    article_columns = {row['name'] for row in cursor.fetchall()}
                    source_task_expr = 'a.source_task_id' if 'source_task_id' in article_columns else "''"
                    cursor.execute("""
                        SELECT
                            a.id,
                            a.title,
                            a.content,
                            a.matched_keywords,
                            a.last_crawled,
                            {source_task_expr} AS source_task_id,
                            (
                                SELECT at.task_id
                                FROM article_tasks at
                                WHERE at.article_id = a.id
                                ORDER BY at.created_at DESC, at.id DESC
                                LIMIT 1
                            ) AS latest_task_id
                        FROM articles a
                        WHERE a.status = 'active'
                    """.format(source_task_expr=source_task_expr))
                    keyword_stats = {}
                    for row in cursor.fetchall():
                        article = dict(row)
                        self._hydrate_matched_keywords_for_display(cursor, article)
                        keywords = _parse_matched_keyword_text(article.get('matched_keywords'))
                        task_id = article.get('source_task_id') or article.get('latest_task_id') or ''
                        for keyword in keywords:
                            item = keyword_stats.setdefault(keyword, {
                                'keyword': keyword,
                                'article_ids': set(),
                                'task_ids': set(),
                                'latest_crawled': ''
                            })
                            item['article_ids'].add(article['id'])
                            if task_id:
                                item['task_ids'].add(task_id)
                            last_crawled = str(article.get('last_crawled') or '')
                            if last_crawled and last_crawled > item['latest_crawled']:
                                item['latest_crawled'] = last_crawled

                    result = []
                    for item in keyword_stats.values():
                        result.append({
                            'keyword': item['keyword'],
                            'article_count': len(item['article_ids']),
                            'task_count': len(item['task_ids']),
                            'latest_crawled': item['latest_crawled']
                        })

                    result.sort(key=lambda item: (-item['article_count'], item['keyword'].lower()))
                    return result[:limit]
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取关键词信息图谱失败: {e}")
            return []

    def get_articles_by_task_id(self, task_id: str, page: int = 1, per_page: int = 100) -> Tuple[List[Dict], int]:
        """获取指定爬取任务关联的文章列表。"""
        if not task_id:
            return [], 0

        try:
            self._ensure_connection()
            page = coerce_int(page, 1, 1)
            per_page = coerce_int(per_page, 100, 1, 500)
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    direct_count_sql = """
                        SELECT COUNT(*) AS total
                        FROM articles a
                        INNER JOIN article_tasks at ON a.id = at.article_id
                        WHERE at.task_id = ? AND a.status = 'active'
                    """
                    direct_select_sql = """
                        SELECT
                            a.*,
                            at.created_at AS task_linked_at,
                            ct.keywords AS task_keywords,
                            ct.task_name,
                            ct.target_url AS task_target_url
                        FROM articles a
                        INNER JOIN article_tasks at ON a.id = at.article_id
                        LEFT JOIN crawl_tasks ct ON at.task_id = ct.task_id
                        WHERE at.task_id = ? AND a.status = 'active'
                        ORDER BY at.created_at DESC, a.created_at DESC, a.id DESC
                        LIMIT ? OFFSET ?
                        """

                    cursor.execute(direct_count_sql, (task_id,))
                    total = cursor.fetchone()['total']
                    if total == 0:
                        repaired = self._repair_article_task_links_from_audit(cursor, task_id)
                        if not repaired:
                            repaired = self._repair_article_task_links_for_task(cursor, task_id)
                        if repaired:
                            self.connection.commit()
                            cursor.execute(direct_count_sql, (task_id,))
                            total = cursor.fetchone()['total']

                    offset = (page - 1) * per_page
                    cursor.execute(direct_select_sql, (task_id, per_page, offset))
                    articles = [dict(row) for row in cursor.fetchall()]

                    for article in articles:
                        if article.get('publish_date'):
                            article['publish_date'] = str(article['publish_date'])
                        self._hydrate_matched_keywords_for_display(cursor, article)

                    return articles, total
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取任务文章列表失败: {e}")
            return [], 0

    def _repair_article_task_links_from_audit(self, cursor, task_id: str) -> int:
        """Repair exact article-task links from this task's audit JSON."""
        if not task_id:
            return 0

        results_dir = os.getenv('CRAWL_RESULTS_DIR') or os.path.join(os.getcwd(), 'crawl_results')
        audit_path = os.path.join(results_dir, f'{task_id}_audit.json')
        if not os.path.exists(audit_path):
            return 0

        try:
            with open(audit_path, 'r', encoding='utf-8') as audit_file:
                audit = json.load(audit_file)
        except Exception as exc:
            print(f"⚠️ 读取任务审计文件失败: {audit_path} ({exc})")
            return 0

        items = audit.get('items') if isinstance(audit, dict) else None
        if not isinstance(items, list):
            return 0

        task = self._get_task_context_for_link_repair(cursor, task_id)
        task_name, _schedule_id = self._resolve_task_source(cursor, task_id)
        linked = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get('status') not in {'saved', 'duplicate'}:
                continue

            article_id = coerce_int(item.get('db_id'), None)
            if not article_id:
                url = item.get('final_url') or item.get('url')
                if not url:
                    continue
                cursor.execute(
                    """
                    SELECT id
                    FROM articles
                    WHERE status = 'active' AND url = ?
                    LIMIT 1
                    """,
                    (url,)
                )
                row = cursor.fetchone()
                article_id = row['id'] if row else None

            if not article_id:
                continue

            cursor.execute(
                "INSERT OR IGNORE INTO article_tasks (article_id, task_id) VALUES (?, ?)",
                (article_id, task_id)
            )
            cursor.execute(
                """
                UPDATE articles
                SET source_task_id = ?,
                    source_task_name = ?,
                    updated_at = datetime('now', 'localtime')
                WHERE id = ?
                """,
                (task_id, task_name or (task or {}).get('task_name') or task_id, article_id)
            )
            linked += 1

        return linked

    def _split_task_keywords(self, value) -> List[str]:
        keywords = []
        seen = set()
        for item in re.split(r'[,，、;\n\r]+', str(value or '')):
            keyword = item.strip()
            keyword = re.sub(r'^\[[^\]]+\]', '', keyword).strip()
            keyword = re.sub(r'^(标题|標題|正文|内容|內容|文)\s*[:：]', '', keyword).strip()
            key = keyword.lower()
            if keyword and key not in seen:
                keywords.append(keyword)
                seen.add(key)
        return keywords

    def _get_task_context_for_link_repair(self, cursor, task_id: str) -> Optional[Dict]:
        cursor.execute(
            "SELECT task_id, task_name, target_url, keywords, status FROM crawl_tasks WHERE task_id = ?",
            (task_id,)
        )
        row = cursor.fetchone()
        if row:
            task = dict(row)
        else:
            task = {'task_id': task_id, 'task_name': '', 'target_url': '', 'keywords': '', 'status': 'completed'}

        schedule_match = re.match(r'^schedule_(\d+)_', str(task_id or ''))
        if schedule_match:
            cursor.execute(
                "SELECT id, task_name, target_url, keywords FROM scheduled_tasks WHERE id = ?",
                (schedule_match.group(1),)
            )
            schedule_row = cursor.fetchone()
            if schedule_row:
                schedule = dict(schedule_row)
                task['task_name'] = schedule.get('task_name') or task.get('task_name') or ''
                task['target_url'] = schedule.get('target_url') or task.get('target_url') or ''
                task['keywords'] = schedule.get('keywords') or task.get('keywords') or ''

        if not task.get('target_url') or not task.get('keywords'):
            return None

        if not self._crawl_task_exists(cursor, task_id):
            china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                """
                INSERT INTO crawl_tasks (
                    task_id, target_url, task_name, crawl_depth, crawl_mode,
                    page_limit, incremental_mode, keywords, status, progress,
                    articles_found, articles_processed, created_at, updated_at
                ) VALUES (?, ?, ?, 1, 'article_crawl', 100, 0, ?, ?, 0, 0, 0, ?, ?)
                """,
                (
                    task_id,
                    normalize_task_url(task.get('target_url', '')),
                    task.get('task_name') or task_id,
                    task.get('keywords') or '',
                    task.get('status') or 'completed',
                    china_time,
                    china_time,
                )
            )

        return task

    def _crawl_task_exists(self, cursor, task_id: str) -> bool:
        cursor.execute("SELECT 1 FROM crawl_tasks WHERE task_id = ? LIMIT 1", (task_id,))
        return cursor.fetchone() is not None

    def _repair_article_task_links_for_task(self, cursor, task_id: str) -> int:
        """Create missing exact article-task links using this task's own URL and keywords."""
        task = self._get_task_context_for_link_repair(cursor, task_id)
        if not task:
            return 0

        target_url = task.get('target_url') or ''
        domain = urlparse(target_url).netloc.lower()
        domain_without_www = domain[4:] if domain.startswith('www.') else domain
        keywords = self._split_task_keywords(task.get('keywords'))
        if not domain_without_www or not keywords:
            return 0

        keyword_conditions = []
        params = []
        for keyword in keywords:
            like = f"%{keyword}%"
            keyword_conditions.append("(a.title LIKE ? OR a.content LIKE ? OR a.matched_keywords LIKE ?)")
            params.extend([like, like, like])

        domain_like = f"%{domain_without_www}%"
        sql = f"""
            SELECT a.id
            FROM articles a
            WHERE a.status = 'active'
              AND (LOWER(a.domain) = ? OR LOWER(a.domain) = ? OR LOWER(a.domain) LIKE ?)
              AND ({' OR '.join(keyword_conditions)})
              AND NOT EXISTS (
                  SELECT 1
                  FROM article_tasks at
                  WHERE at.article_id = a.id AND at.task_id = ?
              )
            ORDER BY a.last_crawled DESC, a.created_at DESC, a.id DESC
            LIMIT 500
        """
        cursor.execute(sql, [domain, domain_without_www, domain_like, *params, task_id])
        article_ids = [row['id'] for row in cursor.fetchall()]
        if not article_ids:
            return 0

        task_name, _schedule_id = self._resolve_task_source(cursor, task_id)
        for article_id in article_ids:
            cursor.execute(
                "INSERT OR IGNORE INTO article_tasks (article_id, task_id) VALUES (?, ?)",
                (article_id, task_id)
            )
            cursor.execute(
                """
                UPDATE articles
                SET source_task_id = ?,
                    source_task_name = ?,
                    updated_at = datetime('now', 'localtime')
                WHERE id = ?
                """,
                (task_id, task_name, article_id)
            )
        return len(article_ids)
    
    def get_article_by_id(self, article_id: int) -> Optional[Dict]:
        """
        根据ID获取文章详情
        
        Args:
            article_id: 文章ID
            
        Returns:
            Optional[Dict]: 文章详情，不存在返回None
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = "SELECT * FROM articles WHERE id = ? AND status = 'active'"
                    cursor.execute(select_sql, (article_id,))
                    article = cursor.fetchone()
                    
                    if article:
                        article = dict(article)
                        if article['publish_date']:
                            article['publish_date'] = str(article['publish_date'])
                        if article['first_crawled']:
                            article['first_crawled'] = article['first_crawled']
                        if article['last_crawled']:
                            article['last_crawled'] = article['last_crawled']
                        self._hydrate_matched_keywords_for_display(cursor, article)
                    
                    return article
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取文章详情失败: {e}")
            return None
    
    def get_article_by_url(self, url: str) -> Optional[Dict]:
        """
        根据URL获取文章详情
        
        Args:
            url: 文章URL
            
        Returns:
            Optional[Dict]: 文章详情，不存在返回None
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = "SELECT * FROM articles WHERE url = ? AND status = 'active'"
                    cursor.execute(select_sql, (url,))
                    article = cursor.fetchone()
                    
                    if article:
                        article = dict(article)
                        if article['publish_date']:
                            article['publish_date'] = str(article['publish_date'])
                        if article['first_crawled']:
                            article['first_crawled'] = article['first_crawled']
                        if article['last_crawled']:
                            article['last_crawled'] = article['last_crawled']
                        self._hydrate_matched_keywords_for_display(cursor, article)
                    
                    return article
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取文章详情失败: {e}")
            return None
    
    def delete_article(self, article_id: int) -> bool:
        """
        删除文章（软删除）
        
        Args:
            article_id: 文章ID
            
        Returns:
            bool: 删除是否成功
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    update_sql = "UPDATE articles SET status = 'deleted', updated_at = datetime('now', 'localtime') WHERE id = ?"
                    cursor.execute(update_sql, (article_id,))
                    self.connection.commit()
                    print(f"✅ 文章删除成功: ID {article_id}")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 删除文章失败: {e}")
            return False
    
    def delete_article_by_url(self, url: str) -> bool:
        """
        根据URL删除文章（软删除）
        
        Args:
            url: 文章URL
            
        Returns:
            bool: 删除是否成功
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    update_sql = "UPDATE articles SET status = 'deleted', updated_at = datetime('now', 'localtime') WHERE url = ?"
                    cursor.execute(update_sql, (url,))
                    self.connection.commit()
                    print(f"✅ 文章删除成功: {url}")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 删除文章失败: {e}")
            return False
    
    def clear_local_articles(self) -> Dict:
        """Hard-delete all locally stored articles for test resets."""
        result = {
            'success': False,
            'active_articles': 0,
            'total_articles': 0,
            'article_tasks': 0,
            'deleted_articles': 0,
            'deleted_article_tasks': 0
        }

        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute("SELECT COUNT(*) AS total FROM articles WHERE status = 'active'")
                    result['active_articles'] = cursor.fetchone()['total']

                    cursor.execute("SELECT COUNT(*) AS total FROM articles")
                    result['total_articles'] = cursor.fetchone()['total']

                    cursor.execute("SELECT COUNT(*) AS total FROM article_tasks")
                    result['article_tasks'] = cursor.fetchone()['total']

                    cursor.execute("DELETE FROM article_tasks")
                    result['deleted_article_tasks'] = cursor.rowcount if cursor.rowcount is not None else result['article_tasks']

                    cursor.execute("DELETE FROM articles")
                    result['deleted_articles'] = cursor.rowcount if cursor.rowcount is not None else result['total_articles']

                    try:
                        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('articles', 'article_tasks')")
                    except Exception:
                        pass

                    self.connection.commit()
                    result['success'] = True
                    print(
                        "Cleared local articles: "
                        f"{result['deleted_articles']} articles, "
                        f"{result['deleted_article_tasks']} article-task links"
                    )
                    return result
                finally:
                    cursor.close()
        except Exception as e:
            print(f"Failed to clear local articles: {e}")
            return result

    def get_recent_articles(self, limit: int = 50) -> List[Dict]:
        """Return recently crawled active articles for legacy pages."""
        try:
            self._ensure_connection()
            limit = coerce_int(limit, 50, 1, 500)
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute(
                        """
                        SELECT
                            id, url, title, content, domain, publish_date,
                            content_length, extraction_method, quality_score,
                            first_crawled, last_crawled, created_at, updated_at
                        FROM articles
                        WHERE status = 'active'
                        ORDER BY COALESCE(last_crawled, created_at) DESC, id DESC
                        LIMIT ?
                        """,
                        (limit,),
                    )
                    articles = [dict(row) for row in cursor.fetchall()]
                    for article in articles:
                        if article.get('publish_date'):
                            article['publish_date'] = str(article['publish_date'])
                        extracted_at = (
                            article.get('last_crawled')
                            or article.get('created_at')
                            or article.get('first_crawled')
                        )
                        article['extracted_at'] = extracted_at
                        if not article.get('content_length') and article.get('content'):
                            article['content_length'] = len(article['content'])
                    return articles
                finally:
                    cursor.close()
        except Exception as e:
            print(f"Failed to get recent articles: {e}")
            return []

    def get_statistics(self, domain: str = None) -> Dict:
        """
        获取统计信息
        
        Args:
            domain: 域名过滤
            
        Returns:
            Dict: 统计信息
        """
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    where_clause = "WHERE status = 'active'"
                    params = []
                    
                    if domain:
                        where_clause += " AND domain = ?"
                        params.append(domain)
                    
                    # 总文章数
                    count_sql = f"SELECT COUNT(*) as total FROM articles {where_clause}"
                    cursor.execute(count_sql, params)
                    total_articles = cursor.fetchone()['total']
                    
                    # 域名数
                    domain_sql = f"SELECT COUNT(DISTINCT domain) as domains FROM articles {where_clause}"
                    cursor.execute(domain_sql, params)
                    domains = cursor.fetchone()['domains']
                    
                    # 最后爬取时间
                    last_sql = f"SELECT MAX(last_crawled) as last_crawled FROM articles {where_clause}"
                    cursor.execute(last_sql, params)
                    last_crawled = cursor.fetchone()['last_crawled']
                    
                    # 今日新增
                    today_sql = f"""
                    SELECT COUNT(*) as today_new FROM articles 
                    {where_clause} AND DATE(first_crawled) = DATE('now')
                    """
                    cursor.execute(today_sql, params)
                    today_new = cursor.fetchone()['today_new']
                    
                    # 域名统计
                    domain_stats_sql = f"""
                    SELECT domain, COUNT(*) as count 
                    FROM articles {where_clause}
                    GROUP BY domain 
                    ORDER BY count DESC
                    """
                    cursor.execute(domain_stats_sql, params)
                    domain_stats = {row['domain']: row['count'] for row in cursor.fetchall()}
                    
                    return {
                        'total_articles': total_articles,
                        'domains': domains,
                        'last_crawl_time': last_crawled,
                        'today_new_articles': today_new,
                        'domain_stats': domain_stats
                    }
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 获取统计信息失败: {e}")
            return {}
    
    def _extract_domain(self, url: str) -> str:
        """
        从URL提取域名
        
        Args:
            url: URL
            
        Returns:
            str: 域名
        """
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return 'unknown'
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
    
    # ==================== 分类管理相关方法 ====================
    
    def insert_category(self, category_data: Dict) -> Optional[int]:
        """插入分类"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 手动设置中国时间，确保时区正确
                    china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                    
                    insert_sql = """
                    INSERT INTO categories (name, description, display_order, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        category_data.get('name', ''),
                        category_data.get('description', ''),
                        category_data.get('display_order', 0),
                        category_data.get('is_active', True),
                        china_time,
                        china_time
                    )
                    
                    cursor.execute(insert_sql, values)
                    category_id = cursor.lastrowid
                    self.connection.commit()
                    
                    print(f"✅ 分类入库成功: {category_data.get('name', '')} (ID: {category_id})")
                    return category_id
                finally:
                    cursor.close()
                
        except sqlite3.IntegrityError:
            print(f"⚠️ 分类已存在: {category_data.get('name', '')}")
            return None
        except Exception as e:
            print(f"❌ 插入分类失败: {e}")
            return None
    
    def get_categories(self, is_active: bool = None) -> List[Dict]:
        """获取分类列表"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    where_conditions = []
                    params = []
                    
                    if is_active is not None:
                        where_conditions.append("is_active = ?")
                        params.append(is_active)
                    
                    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                    
                    select_sql = f"""
                    SELECT * FROM categories 
                    WHERE {where_clause}
                    ORDER BY display_order, name
                    """
                    
                    cursor.execute(select_sql, params)
                    categories = [dict(row) for row in cursor.fetchall()]
                    
                    return categories
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取分类列表失败: {e}")
            return []
    
    def get_category_by_id(self, category_id: int) -> Optional[Dict]:
        """根据ID获取分类"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = "SELECT * FROM categories WHERE id = ?"
                    cursor.execute(select_sql, (category_id,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取分类失败: {e}")
            return None
    
    def update_category(self, category_id: int, category_data: Dict) -> bool:
        """更新分类"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    update_sql = """
                    UPDATE categories SET
                        name = ?,
                        description = ?,
                        display_order = ?,
                        is_active = ?,
                        updated_at = datetime('now', 'localtime')
                    WHERE id = ?
                    """
                    
                    values = (
                        category_data.get('name', ''),
                        category_data.get('description', ''),
                        category_data.get('display_order', 0),
                        category_data.get('is_active', True),
                        category_id
                    )
                    
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    print(f"✅ 分类更新成功: ID {category_id}")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 更新分类失败: {e}")
            return False
    
    def delete_category(self, category_id: int) -> bool:
        """删除分类"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    delete_sql = "DELETE FROM categories WHERE id = ?"
                    cursor.execute(delete_sql, (category_id,))
                    self.connection.commit()
                    print(f"✅ 分类删除成功: ID {category_id}")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 删除分类失败: {e}")
            return False
    
    # ==================== URL管理相关方法 ====================
    
    def insert_managed_url(self, url_data: Dict) -> Optional[int]:
        """插入管理的URL"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    is_valid_url, url, url_error = validate_http_url(url_data.get('url', ''))
                    if not is_valid_url:
                        print(f"❌ URL格式无效，拒绝入库: {url_data.get('url', '')} ({url_error})")
                        return None
                    domain = self._extract_domain(url)
                    
                    # 🔍 调试：打印传入的完整数据
                    print(f"🔍 insert_managed_url 收到数据: {url_data}")
                    
                    # 🔥 修复：检查是否有category字段而不是category_id
                    category_id = url_data.get('category_id')
                    if category_id is None and 'category' in url_data:
                        # 如果收到的是category名称，需要转换为ID
                        category_name = url_data.get('category')
                        if category_name and category_name != '默认分类':
                            # 根据分类名称查找ID
                            cursor_temp = self.connection.cursor()
                            cursor_temp.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
                            result = cursor_temp.fetchone()
                            if result:
                                category_id = result['id']
                                print(f"🔄 分类名称转换: '{category_name}' -> ID: {category_id}")
                            cursor_temp.close()
                    
                    print(f"🔍 最终的category_id: {category_id} (类型: {type(category_id)})")
                    
                    # 🔥 修复：根据category_id查询分类名称
                    category_text = '默认分类'
                    if category_id:
                        cursor_temp = self.connection.cursor()
                        cursor_temp.execute("SELECT name FROM categories WHERE id = ?", (category_id,))
                        result = cursor_temp.fetchone()
                        if result:
                            category_text = result[0] if isinstance(result, tuple) else result['name']
                            print(f"🔄 从数据库获取分类名称: ID {category_id} -> '{category_text}'")
                        cursor_temp.close()
                    
                    # 如果url_data中有category字段，也可以使用（向后兼容）
                    if 'category' in url_data and url_data['category']:
                        category_text = url_data['category']
                        print(f"🔄 使用传入的category文本: '{category_text}'")
                    
                    # 手动设置中国时间，确保时区正确
                    china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                    
                    insert_sql = """
                    INSERT INTO managed_urls (
                        url, name, description, category_id, category, parent_url_id, domain, is_active,
                        auto_crawl, crawl_frequency, auth_config, requires_auth, auth_config_id, 
                        keywords, days_limit, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        url,
                        url_data.get('name', ''),
                        url_data.get('description', ''),
                        category_id,  # 直接使用提取的值
                        category_text,  # 添加category文本字段
                        url_data.get('parent_url_id'),
                        domain,
                        url_data.get('is_active', True),
                        url_data.get('auto_crawl', False),
                        url_data.get('crawl_frequency', ''),
                        url_data.get('auth_config'),
                        url_data.get('requires_auth', False),  # 🔐 添加认证标志
                        url_data.get('auth_config_id'),  # 🔐 添加认证配置ID
                        url_data.get('keywords', ''),  # 🔥 关键词过滤
                        url_data.get('days_limit', 7),  # 🔥 日期限制（默认7天）
                        china_time,
                        china_time
                    )
                    
                    # 调试日志：打印实际插入的值
                    print(f"💾 数据库插入 - category_id: {category_id}, parent_url_id: {url_data.get('parent_url_id')}")
                    print(f"💾 完整插入值: {values}")
                    
                    cursor.execute(insert_sql, values)
                    url_id = cursor.lastrowid
                    self.connection.commit()
                    
                    # 验证插入：读取刚插入的记录
                    cursor.execute("SELECT category_id FROM managed_urls WHERE id = ?", (url_id,))
                    saved_category = cursor.fetchone()
                    if saved_category:
                        print(f"✅ URL入库成功: {url_data.get('name', url)[:30]}... (ID: {url_id}, category_id已保存: {saved_category['category_id']})")
                    else:
                        print(f"✅ URL入库成功: {url_data.get('name', url)[:30]}... (ID: {url_id})")
                    return url_id
                finally:
                    cursor.close()
                
        except sqlite3.IntegrityError:
            print(f"⚠️ URL已存在: {url}")
            return None
        except Exception as e:
            print(f"❌ 插入URL失败: {e}")
            return None
    
    def get_managed_urls(self, page: int = 1, per_page: int = 20, 
                         category_id: int = None, parent_url_id: int = None, is_active: bool = None) -> Tuple[List[Dict], int]:
        """获取管理的URL列表（支持分类和父级URL筛选）"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    where_conditions = []
                    params = []
                    
                    if category_id:
                        where_conditions.append("mu.category_id = ?")
                        params.append(category_id)
                    
                    if parent_url_id is not None:
                        if parent_url_id == 0:
                            # parent_url_id = 0 表示查询顶级URL（没有父级）
                            where_conditions.append("mu.parent_url_id IS NULL")
                        else:
                            where_conditions.append("mu.parent_url_id = ?")
                            params.append(parent_url_id)
                    
                    if is_active is not None:
                        where_conditions.append("mu.is_active = ?")
                        params.append(is_active)
                    
                    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                    
                    # 获取总数
                    count_sql = f"SELECT COUNT(*) as total FROM managed_urls mu WHERE {where_clause}"
                    cursor.execute(count_sql, params)
                    total = cursor.fetchone()['total']
                    
                    # 获取URL列表（关联分类、父级URL和认证配置信息）
                    offset = (page - 1) * per_page
                    select_sql = f"""
                    SELECT 
                        mu.*,
                        c.name as category_name,
                        parent.name as parent_url_name,
                        ac.name as auth_config_name,
                        ac.login_url as auth_login_url,
                        ac.username as auth_username,
                        ac.password as auth_password,
                        ac.username_selector as auth_username_selector,
                        ac.password_selector as auth_password_selector,
                        ac.submit_selector as auth_submit_selector,
                        ac.wait_after_submit as auth_wait_after_submit
                    FROM managed_urls mu
                    LEFT JOIN categories c ON mu.category_id = c.id
                    LEFT JOIN managed_urls parent ON mu.parent_url_id = parent.id
                    LEFT JOIN auth_configs ac ON mu.auth_config_id = ac.id
                    WHERE {where_clause}
                    ORDER BY mu.created_at DESC
                    LIMIT ? OFFSET ?
                    """
                    params.extend([per_page, offset])
                    
                    cursor.execute(select_sql, params)
                    urls = []
                    for row in cursor.fetchall():
                        url_dict = dict(row)
                        # 如果有auth_config_id，构建auth_config JSON对象
                        if url_dict.get('auth_config_id') and url_dict.get('auth_login_url'):
                            url_dict['auth_config'] = {
                                'name': url_dict.get('auth_config_name'),
                                'login_url': url_dict.get('auth_login_url'),
                                'username': url_dict.get('auth_username'),
                                'password': url_dict.get('auth_password'),
                                'username_selector': url_dict.get('auth_username_selector'),
                                'password_selector': url_dict.get('auth_password_selector'),
                                'submit_selector': url_dict.get('auth_submit_selector'),
                                'wait_after_submit': url_dict.get('auth_wait_after_submit', 5)
                            }
                        # 移除临时字段
                        for key in ['auth_config_name', 'auth_login_url', 'auth_username', 'auth_password',
                                   'auth_username_selector', 'auth_password_selector', 'auth_submit_selector', 'auth_wait_after_submit']:
                            url_dict.pop(key, None)
                        urls.append(url_dict)
                    
                    return urls, total
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 获取URL列表失败: {e}")
            return [], 0
    
    def get_managed_url_by_id(self, url_id: int) -> Optional[Dict]:
        """根据ID获取管理的URL（包含认证配置详情）"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = """
                    SELECT 
                        mu.*,
                        ac.name as auth_config_name,
                        ac.login_url as auth_login_url,
                        ac.username as auth_username,
                        ac.password as auth_password,
                        ac.username_selector as auth_username_selector,
                        ac.password_selector as auth_password_selector,
                        ac.submit_selector as auth_submit_selector,
                        ac.wait_after_submit as auth_wait_after_submit
                    FROM managed_urls mu
                    LEFT JOIN auth_configs ac ON mu.auth_config_id = ac.id
                    WHERE mu.id = ?
                    """
                    cursor.execute(select_sql, (url_id,))
                    result = cursor.fetchone()
                    if result:
                        url_dict = dict(result)
                        # 如果有auth_config_id，构建auth_config JSON对象
                        if url_dict.get('auth_config_id') and url_dict.get('auth_login_url'):
                            url_dict['auth_config'] = {
                                'name': url_dict.get('auth_config_name'),
                                'login_url': url_dict.get('auth_login_url'),
                                'username': url_dict.get('auth_username'),
                                'password': url_dict.get('auth_password'),
                                'username_selector': url_dict.get('auth_username_selector'),
                                'password_selector': url_dict.get('auth_password_selector'),
                                'submit_selector': url_dict.get('auth_submit_selector'),
                                'wait_after_submit': url_dict.get('auth_wait_after_submit', 5)
                            }
                        # 移除临时字段
                        for key in ['auth_config_name', 'auth_login_url', 'auth_username', 'auth_password',
                                   'auth_username_selector', 'auth_password_selector', 'auth_submit_selector', 'auth_wait_after_submit']:
                            url_dict.pop(key, None)
                        return url_dict
                    return None
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 根据ID获取URL失败: {e}")
            return None
    
    def get_managed_url_by_url(self, url: str) -> Optional[Dict]:
        """根据URL获取管理的URL"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = """
                    SELECT mu.*, c.name as category_name
                    FROM managed_urls mu
                    LEFT JOIN categories c ON mu.category_id = c.id
                    WHERE mu.url = ?
                    """
                    cursor.execute(select_sql, (url,))
                    result = cursor.fetchone()
                    return dict(result) if result else None
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 根据URL获取URL失败: {e}")
            return None
    
    def find_source_url_info(self, article_url: str) -> Optional[Dict]:
        """
        根据文章URL查找对应的来源URL信息
        匹配逻辑：找到与文章URL domain相同的managed_url
        
        Returns:
            Dict: {'url_id': int, 'category_id': int, 'category_name': str} 或 None
        """
        try:
            from urllib.parse import urlparse
            article_domain = urlparse(article_url).netloc.lower()
            
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 查找domain匹配且parent_url_id为NULL的managed_url（主URL）
                    select_sql = """
                    SELECT mu.id as url_id, mu.category_id, c.name as category_name
                    FROM managed_urls mu
                    LEFT JOIN categories c ON mu.category_id = c.id
                    WHERE mu.domain = ? AND mu.parent_url_id IS NULL
                    LIMIT 1
                    """
                    cursor.execute(select_sql, (article_domain,))
                    result = cursor.fetchone()
                    
                    if result:
                        return {
                            'url_id': result[0],
                            'category_id': result[1],
                            'category_name': result[2]
                        }
                    return None
                finally:
                    cursor.close()
        except Exception as e:
            print(f"⚠️ 查找来源URL失败: {e}")
            return None
    
    def update_managed_url(self, url_id: int, url_data: Dict) -> bool:
        """更新管理的URL"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 🔥 先获取原有数据，用于填充未传入的字段
                    cursor.execute("SELECT url, name, description, category_id, category FROM managed_urls WHERE id = ?", (url_id,))
                    existing = cursor.fetchone()
                    if not existing:
                        print(f"❌ URL不存在: ID {url_id}")
                        return False
                    
                    existing_url = existing['url'] if isinstance(existing, dict) else existing[0]
                    existing_name = existing['name'] if isinstance(existing, dict) else existing[1]
                    
                    # 🔥 修复：根据category_id查询分类名称
                    category_id = url_data.get('category_id')
                    category_text = '默认分类'
                    
                    if category_id:
                        cursor_temp = self.connection.cursor()
                        cursor_temp.execute("SELECT name FROM categories WHERE id = ?", (category_id,))
                        result = cursor_temp.fetchone()
                        if result:
                            category_text = result[0] if isinstance(result, tuple) else result['name']
                            print(f"🔄 更新URL - 从数据库获取分类名称: ID {category_id} -> '{category_text}'")
                        cursor_temp.close()
                    
                    # 如果url_data中有category字段，也可以使用（向后兼容）
                    if 'category' in url_data and url_data['category']:
                        category_text = url_data['category']
                        print(f"🔄 更新URL - 使用传入的category文本: '{category_text}'")
                    
                    # 🔥 获取要更新的url值，如果没传入则保留原值
                    new_url = url_data.get('url') if url_data.get('url') else existing_url
                    is_valid_url, new_url, url_error = validate_http_url(new_url)
                    if not is_valid_url:
                        print(f"❌ URL格式无效，拒绝更新: {url_data.get('url', '')} ({url_error})")
                        return False
                    print(f"🔄 更新URL地址: '{existing_url}' -> '{new_url}'")
                    
                    update_sql = """
                    UPDATE managed_urls SET
                        url = ?,
                        name = ?,
                        description = ?,
                        category_id = ?,
                        category = ?,
                        parent_url_id = ?,
                        is_active = ?,
                        auto_crawl = ?,
                        crawl_frequency = ?,
                        auth_config = ?,
                        auth_config_id = ?,
                        requires_auth = ?,
                        keywords = ?,
                        days_limit = ?,
                        updated_at = datetime('now', 'localtime')
                    WHERE id = ?
                    """
                    
                    values = (
                        new_url,  # 🔥 修复：添加url字段更新
                        url_data.get('name') if url_data.get('name') else existing_name,
                        url_data.get('description', ''),
                        category_id,
                        category_text,  # 🔥 添加category文本字段
                        url_data.get('parent_url_id'),
                        url_data.get('is_active', True),
                        url_data.get('auto_crawl', False),
                        url_data.get('crawl_frequency', ''),
                        url_data.get('auth_config'),
                        url_data.get('auth_config_id'),
                        1 if url_data.get('requires_auth') else 0,
                        url_data.get('keywords', ''),  # 🔥 关键词过滤
                        url_data.get('days_limit', 7),  # 🔥 日期限制
                        url_id
                    )
                    
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    print(f"✅ URL更新成功: ID {url_id}, category_id={category_id}, category='{category_text}'")
                    return True
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新URL失败: {e}")
            return False
    
    def delete_managed_url(self, url_id: int) -> bool:
        """删除管理的URL"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    delete_sql = "DELETE FROM managed_urls WHERE id = ?"
                    cursor.execute(delete_sql, (url_id,))
                    self.connection.commit()
                    print(f"✅ URL删除成功: ID {url_id}")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 删除URL失败: {e}")
            return False
    
    def update_url_crawl_stats(self, url_id: int, success: bool, last_crawled: datetime = None):
        """更新URL爬取统计信息"""
        try:
            self._ensure_connection()
            with self.lock:
                if last_crawled is None:
                    last_crawled = get_china_time()
                
                update_sql = """
                UPDATE managed_urls SET
                    total_crawls = total_crawls + 1,
                    success_crawls = success_crawls + ?,
                    failed_crawls = failed_crawls + ?,
                    last_crawled = ?,
                    updated_at = datetime('now', 'localtime')
                WHERE id = ?
                """
                
                values = (
                    1 if success else 0,
                    0 if success else 1,
                    last_crawled.isoformat() if isinstance(last_crawled, datetime) else last_crawled,
                    url_id
                )
                
                cursor = self.connection.cursor()
                try:
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    return True
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新URL爬取统计失败: {e}")
            return False
    
    # ==================== 定时任务相关方法 ====================
    
    def insert_scheduled_task(self, task_data: Dict) -> Optional[int]:
        """插入定时任务"""
        try:
            task_data = _normalize_schedule_fields(task_data)
            self._ensure_connection()
            with self.lock:
                # 手动设置中国时间，确保时区正确
                china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                
                insert_sql = """
                INSERT INTO scheduled_tasks (
                    task_name, task_type, target_url, url_id,
                    schedule_type, schedule_time, schedule_day, cron_expression,
                    keywords, is_active, ragflow_kb_id, days_limit, 
                    schedule_weekdays, schedule_monthdays, config, next_run, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                values = (
                    task_data.get('task_name', ''),
                    task_data.get('task_type', 'crawl'),
                    normalize_task_url(task_data.get('target_url', '')),
                    task_data.get('url_id'),
                    task_data.get('schedule_type', 'daily'),
                    _normalize_schedule_time_value(task_data.get('schedule_time')),
                    task_data.get('schedule_day'),
                    task_data.get('cron_expression'),
                    task_data.get('keywords', ''),
                    task_data.get('is_active', True),
                    task_data.get('ragflow_kb_id'),
                    coerce_int(task_data.get('days_limit', 7), 7, 0, 3650),
                    _normalize_schedule_list_value(task_data.get('schedule_weekdays', ''), 0, 6),  # 🔥 每周执行日
                    _normalize_schedule_list_value(task_data.get('schedule_monthdays', ''), 1, 31),  # 🔥 每月执行日
                    json.dumps(task_data.get('config', {})),
                    task_data.get('next_run'),  # 🔥 下次执行时间
                    china_time,
                    china_time
                )
                
                cursor = self.connection.cursor()
                try:
                    cursor.execute(insert_sql, values)
                    task_id = cursor.lastrowid
                    self.connection.commit()
                    print(f"✅ 定时任务入库成功: {task_data.get('task_name', '')} (ID: {task_id})")
                    return task_id
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 插入定时任务失败: {e}")
            return None
    
    def get_scheduled_tasks(self, page: int = 1, per_page: int = 20,
                           is_active: bool = None) -> Tuple[List[Dict], int]:
        """获取定时任务列表"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    where_conditions = []
                    params = []

                    if is_active is not None:
                        where_conditions.append("st.is_active = ?")
                        params.append(is_active)

                    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

                    # 获取总数
                    count_sql = f"SELECT COUNT(*) as total FROM scheduled_tasks st LEFT JOIN managed_urls mu ON st.url_id = mu.id WHERE {where_clause}"
                    cursor.execute(count_sql, params)
                    total = cursor.fetchone()['total']
                    
                    # 获取任务列表（优先按url_id匹配，无则按target_url前缀匹配managed_urls）
                    offset = (page - 1) * per_page
                    select_sql = f"""
                    SELECT st.*,
                        COALESCE(
                            (SELECT name FROM managed_urls WHERE id = st.url_id LIMIT 1),
                            (SELECT name FROM managed_urls
                             WHERE REPLACE(REPLACE(st.target_url,'https://',''),'http://','')
                                   LIKE REPLACE(REPLACE(url,'https://',''),'http://','') || '%'
                             ORDER BY LENGTH(url) DESC LIMIT 1)
                        ) as url_display_name
                    FROM scheduled_tasks st
                    WHERE {where_clause}
                    ORDER BY st.created_at DESC
                    LIMIT ? OFFSET ?
                    """
                    params.extend([per_page, offset])
                    
                    cursor.execute(select_sql, params)
                    tasks = []
                    for row in cursor.fetchall():
                        task = dict(row)
                        if task['config']:
                            try:
                                task['config'] = json.loads(task['config'])
                            except:
                                task['config'] = {}
                        tasks.append(task)
                    
                    return tasks, total
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 获取定时任务列表失败: {e}")
            return [], 0

    def get_scheduled_task(self, task_id: int) -> Optional[Dict]:
        """根据ID获取单个定时任务"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute("""
                        SELECT st.*,
                            COALESCE(
                                (SELECT name FROM managed_urls WHERE id = st.url_id LIMIT 1),
                                (SELECT name FROM managed_urls
                                 WHERE REPLACE(REPLACE(st.target_url,'https://',''),'http://','')
                                       LIKE REPLACE(REPLACE(url,'https://',''),'http://','') || '%'
                                 ORDER BY LENGTH(url) DESC LIMIT 1)
                            ) as url_display_name
                        FROM scheduled_tasks st
                        WHERE st.id = ?
                    """, (task_id,))
                    row = cursor.fetchone()
                    if not row:
                        return None
                    task = dict(row)
                    if task.get('config'):
                        try:
                            task['config'] = json.loads(task['config'])
                        except Exception:
                            task['config'] = {}
                    return task
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取定时任务失败: {e}")
            return None
    
    def update_scheduled_task(self, task_id: int, task_data: Dict) -> bool:
        """更新定时任务"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 🔥 先获取现有任务数据（用于填充缺失字段）
                    cursor.execute("SELECT * FROM scheduled_tasks WHERE id = ?", (task_id,))
                    row = cursor.fetchone()
                    if not row:
                        print(f"❌ 任务不存在: ID {task_id}")
                        return False
                    
                    # 获取现有任务的完整数据
                    columns = [desc[0] for desc in cursor.description]
                    existing_task = dict(zip(columns, row))
                    
                    # 合并现有数据和更新数据（更新数据优先）
                    full_task_data = existing_task.copy()
                    full_task_data.update(task_data)
                    full_task_data = _normalize_schedule_fields(full_task_data)
                    if full_task_data.get('target_url'):
                        full_task_data['target_url'] = normalize_task_url(full_task_data.get('target_url'))
                    
                    # 🔥 用完整数据重新计算next_run
                    next_run = self._calculate_next_run_for_task(full_task_data)
                    
                    update_sql = """
                    UPDATE scheduled_tasks SET
                        task_name = ?,
                        target_url = ?,
                        is_active = ?,
                        schedule_type = ?,
                        schedule_time = ?,
                        schedule_day = ?,
                        cron_expression = ?,
                        keywords = ?,
                        ragflow_kb_id = ?,
                        days_limit = ?,
                        schedule_weekdays = ?,
                        schedule_monthdays = ?,
                        config = ?,
                        next_run = ?,
                        updated_at = datetime('now', 'localtime')
                    WHERE id = ?
                    """
                    
                    # 🔥 使用合并后的完整数据，避免字段被清空
                    values = (
                        full_task_data.get('task_name', ''),
                        normalize_task_url(full_task_data.get('target_url', '')),
                        full_task_data.get('is_active', True),
                        full_task_data.get('schedule_type', 'daily'),
                        _normalize_schedule_time_value(full_task_data.get('schedule_time')),
                        full_task_data.get('schedule_day'),
                        full_task_data.get('cron_expression'),
                        full_task_data.get('keywords', ''),
                        full_task_data.get('ragflow_kb_id'),
                        coerce_int(full_task_data.get('days_limit', 7), 7, 0, 3650),
                        _normalize_schedule_list_value(full_task_data.get('schedule_weekdays', ''), 0, 6),  # 🔥 每周执行日
                        _normalize_schedule_list_value(full_task_data.get('schedule_monthdays', ''), 1, 31),  # 🔥 每月执行日
                        json.dumps(full_task_data.get('config', {})) if isinstance(full_task_data.get('config'), dict) else full_task_data.get('config', '{}'),
                        next_run.isoformat() if next_run else None,
                        task_id
                    )
                    
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    print(f"✅ 定时任务更新成功: ID {task_id}, next_run={next_run}")
                    return True
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新定时任务失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _calculate_next_run_for_task(self, task_data: Dict):
        """Calculate the next run time using the same rules as the schedule API."""
        import calendar
        from datetime import timedelta

        current_time = get_china_time()
        schedule_type = task_data.get('schedule_type', 'daily')
        schedule_time = _normalize_schedule_time_value(task_data.get('schedule_time'))
        schedule_day = task_data.get('schedule_day')

        def parse_time(value):
            parts = str(value or '00:00:00').split(':')
            return (
                coerce_int(parts[0] if len(parts) > 0 else 0, 0, 0, 23),
                coerce_int(parts[1] if len(parts) > 1 else 0, 0, 0, 59),
                coerce_int(parts[2] if len(parts) > 2 else 0, 0, 0, 59),
            )

        def parse_int_list(value, min_value, max_value):
            result = []
            if isinstance(value, (list, tuple, set)):
                raw_items = value
            else:
                raw_items = str(value or '').split(',')
            for item in raw_items:
                parsed = coerce_int(item, None)
                if parsed is not None and min_value <= parsed <= max_value and parsed not in result:
                    result.append(parsed)
            return sorted(result)

        hour, minute, second = parse_time(schedule_time)

        if schedule_type == 'once':
            next_run = current_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
            return next_run if next_run > current_time else current_time

        if schedule_type == 'daily':
            next_run = current_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if next_run <= current_time:
                next_run += timedelta(days=1)
            return next_run

        if schedule_type == 'weekly':
            weekdays = parse_int_list(task_data.get('schedule_weekdays'), 0, 6)
            if not weekdays and schedule_day is not None:
                weekdays = [coerce_int(schedule_day, current_time.weekday(), 0, 6)]
            if not weekdays:
                weekdays = [current_time.weekday()]

            for days_ahead in range(0, 8):
                candidate = current_time + timedelta(days=days_ahead)
                if candidate.weekday() not in weekdays:
                    continue
                next_run = candidate.replace(hour=hour, minute=minute, second=second, microsecond=0)
                if next_run > current_time:
                    return next_run
            return (current_time + timedelta(days=7)).replace(hour=hour, minute=minute, second=second, microsecond=0)

        if schedule_type == 'monthly':
            monthdays = parse_int_list(task_data.get('schedule_monthdays'), 1, 31)
            if not monthdays and schedule_day is not None:
                monthdays = [coerce_int(schedule_day, current_time.day, 1, 31)]
            if not monthdays:
                monthdays = [current_time.day]

            for days_ahead in range(0, 62):
                candidate = current_time + timedelta(days=days_ahead)
                max_day = calendar.monthrange(candidate.year, candidate.month)[1]
                valid_days = {min(day, max_day) for day in monthdays}
                if candidate.day not in valid_days:
                    continue
                next_run = candidate.replace(hour=hour, minute=minute, second=second, microsecond=0)
                if next_run > current_time:
                    return next_run
            return (current_time + timedelta(days=30)).replace(hour=hour, minute=minute, second=second, microsecond=0)

        return current_time + timedelta(hours=1)
    
    def delete_scheduled_task(self, task_id: int) -> bool:
        """删除定时任务"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    delete_sql = "DELETE FROM scheduled_tasks WHERE id = ?"
                    cursor.execute(delete_sql, (task_id,))
                    self.connection.commit()
                    print(f"✅ 定时任务删除成功: ID {task_id}")
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 删除定时任务失败: {e}")
            return False

    def _table_exists(self, cursor, table_name: str) -> bool:
        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None

    def _migrate_existing_schema(self, cursor) -> None:
        """Apply lightweight migrations when connecting to an existing database."""
        if self._table_exists(cursor, 'managed_urls'):
            self._ensure_column_exists(cursor, 'managed_urls', 'access_status', 'TEXT')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_checked_at', 'TIMESTAMP')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_status_code', 'INTEGER')
            self._ensure_column_exists(cursor, 'managed_urls', 'access_error', 'TEXT')
            try:
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_urls_access_status ON managed_urls(access_status)")
            except Exception as exc:
                print(f"⚠️ 创建 access_status 索引失败: {exc}")

        if not self._table_exists(cursor, 'articles'):
            return
        self._ensure_column_exists(cursor, 'articles', 'matched_keywords', 'TEXT')
        self._ensure_column_exists(cursor, 'articles', 'source_task_id', 'TEXT')
        self._ensure_column_exists(cursor, 'articles', 'source_task_name', 'TEXT')
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_articles_source_task_id ON articles(source_task_id)")
        except Exception as exc:
            print(f"⚠️ 创建 source_task_id 索引失败: {exc}")

    def _is_run_marker_stale(self, started_at, stale_seconds: int) -> bool:
        if not started_at:
            return False
        try:
            if isinstance(started_at, datetime):
                parsed = started_at
            else:
                text = str(started_at).strip().replace('Z', '+00:00')
                if ' ' in text and 'T' not in text:
                    text = text.replace(' ', 'T')
                parsed = datetime.fromisoformat(text)
            if parsed.tzinfo:
                parsed = parsed.replace(tzinfo=None)
            return (get_china_time() - parsed).total_seconds() > stale_seconds
        except Exception:
            return False

    def _datetime_to_db_text(self, value) -> str:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def claim_scheduled_task_run(
        self,
        task_id: int,
        lock_id: str,
        started_at=None,
        stale_seconds: int = 3600,
        scheduled_for=None,
        next_run=None,
        run_key: str = None,
    ):
        """Atomically reserve one scheduled run slot before submitting it to workers."""
        if not task_id or not lock_id:
            return False

        self._ensure_connection()
        with self.lock:
            cursor = self.connection.cursor()
            try:
                run_started_at = started_at or get_china_time()
                run_started_text = run_started_at.isoformat() if isinstance(run_started_at, datetime) else str(run_started_at)

                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute(
                    "SELECT is_active, running_lock_id, running_started_at FROM scheduled_tasks WHERE id = ?",
                    (task_id,),
                )
                task_row = cursor.fetchone()
                if not task_row:
                    self.connection.rollback()
                    return False

                is_active = task_row['is_active']
                try:
                    is_active = bool(int(is_active))
                except Exception:
                    is_active = bool(is_active)
                if not is_active:
                    self.connection.rollback()
                    return False

                scheduled_for_text = self._datetime_to_db_text(scheduled_for or run_started_at)
                next_run_text = self._datetime_to_db_text(next_run)
                run_key = run_key or f"schedule:{task_id}:{scheduled_for_text}"

                cursor.execute(
                    """
                    SELECT id, status
                    FROM task_execution_history
                    WHERE run_key = ?
                    LIMIT 1
                    """,
                    (run_key,),
                )
                existing_slot = cursor.fetchone()
                if existing_slot:
                    self.connection.rollback()
                    return False

                cursor.execute(
                    """
                    SELECT id, run_key, scheduled_for, started_at
                    FROM task_execution_history
                    WHERE schedule_id = ?
                      AND status IN ('pending', 'running')
                    ORDER BY COALESCE(scheduled_for, started_at, created_at) DESC
                    LIMIT 1
                    """,
                    (task_id,),
                )
                incomplete_execution = cursor.fetchone()
                if incomplete_execution:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO task_execution_history (
                            schedule_id, task_id, status, run_key, scheduled_for,
                            started_at, completed_at, duration_seconds, articles_found,
                            error_message, result_summary, created_at
                        ) VALUES (?, NULL, 'skipped', ?, ?, ?, ?, 0, 0, ?, ?, ?)
                        """,
                        (
                            task_id,
                            run_key,
                            scheduled_for_text,
                            run_started_text,
                            run_started_text,
                            'Skipped because previous recurring task instance is still incomplete',
                            json.dumps({
                                'message': 'Skipped because previous recurring task instance is still incomplete',
                                'previous_execution_id': incomplete_execution['id'],
                                'previous_run_key': incomplete_execution['run_key'],
                                'previous_scheduled_for': incomplete_execution['scheduled_for'],
                            }),
                            run_started_text,
                        ),
                    )
                    if next_run_text:
                        cursor.execute(
                            """
                            UPDATE scheduled_tasks
                            SET next_run = ?,
                                updated_at = datetime('now', 'localtime')
                            WHERE id = ?
                            """,
                            (next_run_text, task_id),
                        )
                    self.connection.commit()
                    return False

                existing_lock = task_row['running_lock_id']
                if existing_lock:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO task_execution_history (
                            schedule_id, task_id, status, run_key, scheduled_for,
                            started_at, completed_at, duration_seconds, articles_found,
                            error_message, result_summary, created_at
                        ) VALUES (?, NULL, 'skipped', ?, ?, ?, ?, 0, 0, ?, ?, ?)
                        """,
                        (
                            task_id,
                            run_key,
                            scheduled_for_text,
                            run_started_text,
                            run_started_text,
                            'Skipped because scheduled task already has a running lock',
                            json.dumps({
                                'message': 'Skipped because scheduled task already has a running lock',
                                'running_lock_id': existing_lock,
                            }),
                            run_started_text,
                        ),
                    )
                    if next_run_text:
                        cursor.execute(
                            """
                            UPDATE scheduled_tasks
                            SET next_run = ?,
                                updated_at = datetime('now', 'localtime')
                            WHERE id = ?
                            """,
                            (next_run_text, task_id),
                        )
                    self.connection.commit()
                    return False

                cursor.execute(
                    """
                    SELECT started_at
                    FROM task_execution_history
                    WHERE schedule_id = ? AND status = 'running'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (task_id,),
                )
                running_execution = cursor.fetchone()
                if running_execution:
                    self.connection.rollback()
                    return False

                cursor.execute(
                    """
                    SELECT created_at
                    FROM crawl_tasks
                    WHERE task_id LIKE ? AND status IN ('pending', 'running')
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (f'schedule_{task_id}_%',),
                )
                running_crawl = cursor.fetchone()
                if running_crawl:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO task_execution_history (
                            schedule_id, task_id, status, run_key, scheduled_for,
                            started_at, completed_at, duration_seconds, articles_found,
                            error_message, result_summary, created_at
                        ) VALUES (?, NULL, 'skipped', ?, ?, ?, ?, 0, 0, ?, ?, ?)
                        """,
                        (
                            task_id,
                            run_key,
                            scheduled_for_text,
                            run_started_text,
                            run_started_text,
                            'Skipped because a crawl task instance is still pending or running',
                            json.dumps({
                                'message': 'Skipped because a crawl task instance is still pending or running',
                            }),
                            run_started_text,
                        ),
                    )
                    if next_run_text:
                        cursor.execute(
                            """
                            UPDATE scheduled_tasks
                            SET next_run = ?,
                                updated_at = datetime('now', 'localtime')
                            WHERE id = ?
                            """,
                            (next_run_text, task_id),
                        )
                    self.connection.commit()
                    return False

                cursor.execute(
                    """
                    INSERT INTO task_execution_history (
                        schedule_id, task_id, status, run_key, scheduled_for,
                        started_at, articles_found, result_summary, created_at
                    ) VALUES (?, NULL, 'pending', ?, ?, ?, 0, ?, ?)
                    """,
                    (
                        task_id,
                        run_key,
                        scheduled_for_text,
                        run_started_text,
                        json.dumps({'message': 'Scheduled run reserved'}),
                        run_started_text,
                    ),
                )
                execution_id = cursor.lastrowid

                cursor.execute(
                    """
                    UPDATE scheduled_tasks
                    SET running_lock_id = ?,
                        running_started_at = ?,
                        next_run = COALESCE(?, next_run),
                        updated_at = datetime('now', 'localtime')
                    WHERE id = ?
                    """,
                    (lock_id, run_started_text, next_run_text, task_id),
                )
                self.connection.commit()
                if cursor.rowcount <= 0:
                    return False
                return {
                    'claimed': True,
                    'execution_id': execution_id,
                    'run_key': run_key,
                    'scheduled_for': scheduled_for_text,
                    'next_run': next_run_text,
                }
            except Exception as e:
                try:
                    self.connection.rollback()
                except Exception:
                    pass
                print(f"❌ 领取定时任务运行锁失败: {e}")
                return False
            finally:
                cursor.close()

    def release_scheduled_task_run(self, task_id: int, lock_id: str = None) -> bool:
        """Release a scheduled task run lock after the worker exits."""
        if not task_id:
            return False

        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    if lock_id:
                        cursor.execute(
                            """
                            UPDATE scheduled_tasks
                            SET running_lock_id = NULL,
                                running_started_at = NULL,
                                updated_at = datetime('now', 'localtime')
                            WHERE id = ? AND running_lock_id = ?
                            """,
                            (task_id, lock_id),
                        )
                    else:
                        cursor.execute(
                            """
                            UPDATE scheduled_tasks
                            SET running_lock_id = NULL,
                                running_started_at = NULL,
                                updated_at = datetime('now', 'localtime')
                            WHERE id = ?
                            """,
                            (task_id,),
                        )
                    self.connection.commit()
                    return cursor.rowcount > 0
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 释放定时任务运行锁失败: {e}")
            return False
    
    def update_scheduled_task_run_stats(self, task_id: int, success: bool, 
                                       last_run: datetime = None, next_run: datetime = None):
        """更新定时任务运行统计"""
        try:
            self._ensure_connection()
            with self.lock:
                if last_run is None:
                    last_run = get_china_time()
                
                update_sql = """
                UPDATE scheduled_tasks SET
                    total_runs = total_runs + 1,
                    success_runs = success_runs + ?,
                    failed_runs = failed_runs + ?,
                    last_run = ?,
                    next_run = CASE
                        WHEN ? IS NULL THEN next_run
                        WHEN next_run IS NULL THEN ?
                        WHEN datetime(?) > datetime(next_run) THEN ?
                        ELSE next_run
                    END,
                    updated_at = datetime('now', 'localtime')
                WHERE id = ?
                """
                next_run_text = next_run.isoformat() if next_run and isinstance(next_run, datetime) else next_run
                
                values = (
                    1 if success else 0,
                    0 if success else 1,
                    last_run.isoformat() if isinstance(last_run, datetime) else last_run,
                    next_run_text,
                    next_run_text,
                    next_run_text,
                    next_run_text,
                    task_id
                )
                
                cursor = self.connection.cursor()
                try:
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    return True
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新定时任务运行统计失败: {e}")
            return False
    
    # ==================== 爬取任务相关方法 ====================
    
    def insert_crawl_task(self, task_data: Dict) -> Optional[int]:
        """插入爬取任务"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 手动设置中国时间，确保时区正确
                    china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                    
                    insert_sql = """
                    INSERT INTO crawl_tasks (
                        task_id, target_url, task_name, crawl_depth, crawl_mode,
                        page_limit, incremental_mode, keywords, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        task_data.get('task_id', ''),
                        normalize_task_url(task_data.get('target_url', '')),
                        task_data.get('task_name', ''),
                        task_data.get('crawl_depth', 1),
                        task_data.get('crawl_mode', 'standard'),
                        task_data.get('page_limit', 50),
                        task_data.get('incremental_mode', False),
                        task_data.get('keywords', ''),
                        task_data.get('status', 'pending'),
                        china_time,
                        china_time
                    )
                    
                    cursor.execute(insert_sql, values)
                    task_db_id = cursor.lastrowid
                    self.connection.commit()
                    
                    print(f"✅ 爬取任务入库成功: {task_data.get('task_name', task_data.get('task_id', ''))} (ID: {task_db_id})")
                    return task_db_id
                finally:
                    cursor.close()
                
        except sqlite3.IntegrityError:
            print(f"⚠️ 任务ID已存在: {task_data.get('task_id', '')}")
            return None
        except Exception as e:
            print(f"❌ 插入爬取任务失败: {e}")
            return None
    
    def update_crawl_task_status(self, task_id: str, status: str, 
                                 progress: int = None, articles_found: int = None,
                                 articles_processed: int = None, error_message: str = None):
        """更新爬取任务状态"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    update_parts = ["status = ?", "updated_at = datetime('now', 'localtime')"]
                    values = [status]
                    
                    if progress is not None:
                        update_parts.append("progress = ?")
                        values.append(progress)
                    
                    if articles_found is not None:
                        update_parts.append("articles_found = ?")
                        values.append(articles_found)
                    
                    if articles_processed is not None:
                        update_parts.append("articles_processed = ?")
                        values.append(articles_processed)
                    
                    if error_message is not None:
                        update_parts.append("error_message = ?")
                        values.append(error_message)
                    
                    if status == 'running' and not any('started_at' in p for p in update_parts):
                        update_parts.append("started_at = datetime('now', 'localtime')")
                    
                    if status in ['completed', 'failed', 'cancelled']:
                        update_parts.append("completed_at = datetime('now', 'localtime')")
                    
                    update_sql = f"""
                    UPDATE crawl_tasks SET {', '.join(update_parts)}
                    WHERE task_id = ?
                    """
                    values.append(task_id)
                    
                    cursor.execute(update_sql, values)
                    self.connection.commit()
                    return True
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新爬取任务状态失败: {e}")
            return False

    def reset_crawl_task_for_retry(
        self,
        task_id: str,
        target_url: str = None,
        task_name: str = None,
        crawl_depth: int = None,
        crawl_mode: str = 'article_crawl',
        page_limit: int = None,
        incremental_mode: bool = None,
        keywords: str = None,
    ) -> bool:
        """Reset an existing crawl task row so retry reuses the original task_id."""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute("SELECT * FROM crawl_tasks WHERE task_id = ?", (task_id,))
                    row = cursor.fetchone()
                    existing = dict(row) if row else None
                    normalized_url = normalize_task_url(target_url or (existing or {}).get('target_url', ''))
                    china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')

                    if existing:
                        update_sql = """
                        UPDATE crawl_tasks SET
                            target_url = COALESCE(NULLIF(?, ''), target_url),
                            task_name = COALESCE(NULLIF(?, ''), task_name),
                            crawl_depth = COALESCE(?, crawl_depth),
                            crawl_mode = COALESCE(NULLIF(?, ''), crawl_mode),
                            page_limit = COALESCE(?, page_limit),
                            incremental_mode = COALESCE(?, incremental_mode),
                            keywords = COALESCE(?, keywords),
                            status = 'pending',
                            progress = 0,
                            articles_found = 0,
                            articles_processed = 0,
                            started_at = NULL,
                            completed_at = NULL,
                            error_message = NULL,
                            updated_at = ?
                        WHERE task_id = ?
                        """
                        cursor.execute(
                            update_sql,
                            (
                                normalized_url,
                                task_name or '',
                                crawl_depth,
                                crawl_mode or '',
                                page_limit,
                                None if incremental_mode is None else int(bool(incremental_mode)),
                                keywords,
                                china_time,
                                task_id,
                            ),
                        )
                    else:
                        insert_sql = """
                        INSERT INTO crawl_tasks (
                            task_id, target_url, task_name, crawl_depth, crawl_mode,
                            page_limit, incremental_mode, keywords, status, progress,
                            articles_found, articles_processed, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0, 0, ?, ?)
                        """
                        cursor.execute(
                            insert_sql,
                            (
                                task_id,
                                normalized_url,
                                task_name or f'重试任务-{normalized_url}',
                                crawl_depth or 1,
                                crawl_mode or 'article_crawl',
                                page_limit or 50,
                                int(bool(incremental_mode)),
                                keywords or '',
                                china_time,
                                china_time,
                            ),
                        )

                    self.connection.commit()
                    return True
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 重置爬取任务重试状态失败: {e}")
            return False
    
    def get_crawl_tasks(self, page: int = 1, per_page: int = 20,
                       status: str = None) -> Tuple[List[Dict], int]:
        """获取爬取任务列表"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    where_conditions = []
                    params = []
                    
                    if status:
                        where_conditions.append("status = ?")
                        params.append(status)
                    
                    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
                    
                    # 获取总数
                    count_sql = f"SELECT COUNT(*) as total FROM crawl_tasks WHERE {where_clause}"
                    cursor.execute(count_sql, params)
                    total = cursor.fetchone()['total']
                    
                    # 获取任务列表（附带managed_urls中文名）
                    offset = (page - 1) * per_page
                    select_sql = f"""
                    SELECT ct.*,
                        (SELECT name FROM managed_urls
                         WHERE REPLACE(REPLACE(ct.target_url,'https://',''),'http://','')
                               LIKE REPLACE(REPLACE(url,'https://',''),'http://','') || '%'
                         ORDER BY LENGTH(url) DESC LIMIT 1) as url_display_name
                    FROM crawl_tasks ct
                    WHERE {where_clause}
                    ORDER BY ct.created_at DESC
                    LIMIT ? OFFSET ?
                    """
                    params.extend([per_page, offset])
                    
                    cursor.execute(select_sql, params)
                    tasks = [dict(row) for row in cursor.fetchall()]
                    
                    return tasks, total
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 获取爬取任务列表失败: {e}")
            return [], 0
    
    def get_crawl_task_by_task_id(self, task_id: str) -> Optional[Dict]:
        """根据task_id获取爬取任务"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    select_sql = "SELECT * FROM crawl_tasks WHERE task_id = ?"
                    cursor.execute(select_sql, (task_id,))
                    task = cursor.fetchone()
                    return dict(task) if task else None
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取爬取任务失败: {e}")
            return None
    
    # ==================== 任务执行历史相关方法 ====================
    
    def insert_task_execution(self, execution_data: Dict) -> Optional[int]:
        """插入任务执行历史"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 手动设置中国时间，确保时区正确
                    china_time = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                    
                    insert_sql = """
                    INSERT INTO task_execution_history (
                        schedule_id, task_id, status, run_key, scheduled_for, started_at, completed_at,
                        duration_seconds, articles_found, error_message, result_summary, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    values = (
                        execution_data.get('schedule_id'),
                        execution_data.get('task_id'),
                        execution_data.get('status', 'running'),
                        execution_data.get('run_key'),
                        execution_data.get('scheduled_for'),
                        execution_data.get('started_at', china_time),
                        execution_data.get('completed_at'),
                        execution_data.get('duration_seconds'),
                        execution_data.get('articles_found', 0),
                        execution_data.get('error_message'),
                        json.dumps(execution_data.get('result_summary', {})),
                        china_time
                    )
                    
                    cursor.execute(insert_sql, values)
                    execution_id = cursor.lastrowid
                    self.connection.commit()
                    
                    return execution_id
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 插入任务执行历史失败: {e}")
            return None

    def get_task_executions(self, page: int = 1, per_page: int = 20, 
                           status: str = None, schedule_id: str = None) -> Tuple[List[Dict], int]:
        """获取任务执行记录列表"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 构建WHERE条件
                    where_conditions = []
                    params = []
                    
                    if status:
                        where_conditions.append("status = ?")
                        params.append(status)
                    
                    if schedule_id:
                        where_conditions.append("schedule_id = ?")
                        params.append(schedule_id)
                    
                    where_clause = ""
                    if where_conditions:
                        where_clause = f"WHERE {' AND '.join(where_conditions)}"
                    
                    # 获取总数
                    count_sql = f"SELECT COUNT(*) FROM task_execution_history {where_clause}"
                    cursor.execute(count_sql, params)
                    total = cursor.fetchone()[0]
                    
                    # 获取分页数据（关联scheduled_tasks表获取任务名称）
                    offset = (page - 1) * per_page
                    select_sql = f"""
                    SELECT 
                        teh.id, teh.schedule_id, teh.task_id, teh.status, teh.started_at, teh.completed_at,
                        teh.duration_seconds, teh.articles_found, teh.error_message, teh.result_summary,
                        teh.created_at,
                        st.task_name
                    FROM task_execution_history teh
                    LEFT JOIN scheduled_tasks st ON teh.schedule_id = st.id
                    {where_clause}
                    ORDER BY teh.created_at DESC 
                    LIMIT ? OFFSET ?
                    """
                    
                    params.extend([per_page, offset])
                    cursor.execute(select_sql, params)
                    
                    executions = []
                    for row in cursor.fetchall():
                        # 安全解析JSON，处理可能的格式错误
                        result_summary = {}
                        if row[9]:
                            try:
                                parsed = json.loads(row[9])
                                # 确保解析结果是dict
                                if isinstance(parsed, dict):
                                    result_summary = parsed
                                else:
                                    result_summary = {}
                            except (json.JSONDecodeError, TypeError, ValueError) as e:
                                print(f"⚠️ 解析result_summary失败 (ID: {row[0]}): {e}, 原始数据: {row[9][:100] if row[9] else 'None'}")
                                result_summary = {}
                        
                        # 优先使用result_summary中的消息，然后是error_message
                        message = result_summary.get('message', '') if isinstance(result_summary, dict) else ''
                        if not message:
                            message = row[8] or f"任务执行状态: {row[3]}"
                        
                        execution = {
                            'id': str(row[0]),
                            'schedule_id': str(row[1]) if row[1] else None,
                            'task_id': row[2],
                            'task_name': row[11] or '-',  # 从scheduled_tasks表获取的任务名称
                            'status': row[3],
                            'started_at': row[4],
                            'completed_at': row[5],
                            'duration_seconds': row[6],
                            'articles_found': row[7],
                            'error_message': row[8],
                            'result_summary': result_summary,
                            'created_at': row[10],
                            'updated_at': row[5] or row[10],  # 使用completed_at或created_at作为updated_at
                            'message': message  # 使用result_summary.message或error_message或默认消息
                        }
                        executions.append(execution)
                    
                    return executions, total
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 获取任务执行记录失败: {e}")
            return [], 0

    def update_task_execution(self, execution_id: str, update_data: Dict) -> bool:
        """更新任务执行记录"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 构建更新字段
                    update_fields = []
                    params = []
                    
                    for field in ['task_id', 'status', 'run_key', 'scheduled_for', 'started_at',
                                  'completed_at', 'duration_seconds', 'articles_found',
                                  'error_message', 'result_summary']:
                        if field in update_data:
                            update_fields.append(f"{field} = ?")
                            if field == 'result_summary':
                                params.append(json.dumps(update_data[field]))
                            else:
                                params.append(update_data[field])
                    
                    if not update_fields:
                        return True
                    
                    params.append(int(execution_id))
                    
                    update_sql = f"""
                    UPDATE task_execution_history 
                    SET {', '.join(update_fields)}
                    WHERE id = ?
                    """
                    
                    cursor.execute(update_sql, params)
                    self.connection.commit()
                    
                    return cursor.rowcount > 0
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 更新任务执行记录失败: {e}")
            return False

    def delete_task_execution(self, execution_id: str) -> bool:
        """删除单个任务执行记录"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 先检查记录是否存在
                    check_sql = "SELECT id FROM task_execution_history WHERE id = ?"
                    cursor.execute(check_sql, (int(execution_id),))
                    exists = cursor.fetchone()
                    
                    print(f"🔍 检查记录 {execution_id} 是否存在: {exists is not None}")
                    
                    if not exists:
                        print(f"⚠️ 记录 {execution_id} 不存在")
                        return False
                    
                    # 执行删除
                    delete_sql = "DELETE FROM task_execution_history WHERE id = ?"
                    cursor.execute(delete_sql, (int(execution_id),))
                    self.connection.commit()
                    
                    deleted_count = cursor.rowcount
                    print(f"🗑️ 删除了 {deleted_count} 条记录")
                    
                    return deleted_count > 0
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 删除任务执行记录失败: {e}")
            return False

    def clear_task_executions(self) -> bool:
        """清空所有任务执行记录"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    delete_sql = "DELETE FROM task_execution_history"
                    cursor.execute(delete_sql)
                    self.connection.commit()
                    return True
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 清空任务执行记录失败: {e}")
            return False

    def delete_crawl_task_by_task_id(self, task_id: str) -> bool:
        """通过task_id删除爬取任务记录"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    # 先检查记录是否存在
                    check_sql = "SELECT task_id FROM crawl_tasks WHERE task_id = ?"
                    cursor.execute(check_sql, (task_id,))
                    exists = cursor.fetchone()
                    
                    print(f"🔍 检查爬取任务 {task_id} 是否存在: {exists is not None}")
                    
                    if not exists:
                        print(f"⚠️ 爬取任务 {task_id} 不存在")
                        return True  # 不存在也算成功
                    
                    # 执行删除
                    delete_sql = "DELETE FROM crawl_tasks WHERE task_id = ?"
                    cursor.execute(delete_sql, (task_id,))
                    self.connection.commit()
                    
                    deleted_count = cursor.rowcount
                    print(f"🗑️ 删除了 {deleted_count} 条爬取任务记录")
                    
                    return deleted_count > 0
                finally:
                    cursor.close()
                
        except Exception as e:
            print(f"❌ 删除爬取任务记录失败: {e}")
            return False
    
    def get_all_auth_configs(self) -> list:
        """获取所有认证配置"""
        try:
            self._ensure_connection()
            with self.lock:
                cursor = self.connection.cursor()
                try:
                    cursor.execute("SELECT * FROM auth_configs WHERE is_active = 1")
                    configs = []
                    for row in cursor.fetchall():
                        configs.append(dict(row))
                    return configs
                finally:
                    cursor.close()
        except Exception as e:
            print(f"❌ 获取认证配置失败: {e}")
            return []

# 全局数据库实例
sqlite_db = SQLiteDatabase()
