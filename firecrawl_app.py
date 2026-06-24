# -*- coding: utf-8 -*-
import json
import os
import sys


def _configure_stdio():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            try:
                stream.reconfigure(encoding='utf-8', errors='replace')
            except Exception:
                pass


_configure_stdio()

import requests
from flask import Flask, render_template_string, render_template, request, jsonify, send_file, session, redirect, url_for
from datetime import datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        # 如果没有 zoneinfo，使用 pytz 作为后备
        import pytz
        ZoneInfo = None
import uuid
import redis
import time
import threading
from threading import Thread, Lock
from requests.exceptions import RequestException
import re
import json
import io
import zipfile
from typing import Optional
from functools import wraps
from article_link_extractor import ArticleLinkExtractor
from smart_article_extractor import extract_article_content_from_url, clean_article_content
from smart_extraction_api import smart_bp
from incremental_crawl_api import incremental_bp
from sqlite_api import sqlite_bp
from sqlite_database import sqlite_db
from article_management_api import article_management_bp
from url_management_api import url_management_bp
from schedule_management_api import calculate_next_run_time, schedule_management_bp
from schedule_execution_api import schedule_execution_bp
from crawl_task_api import crawl_task_bp
from category_management_api import category_bp
from config_management_api import config_management_bp
from auth_management_api import auth_bp
from user_management_api import user_bp
from user_database import UserDatabase
try:
    import newspaper
    from newspaper import Article
    NEWSPAPER_AVAILABLE = True
except ImportError:
    newspaper = None
    Article = None
    NEWSPAPER_AVAILABLE = False
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import secrets

# 导入拆分的模块
from utils import get_china_time
from decorators import login_required
from content_handlers import extract_with_newspaper3k, extract_article_links_from_list_page, is_valid_article_content
from crawl_options import normalize_crawl_options, public_runtime_config
from url_validation_helper import normalize_task_url
import config
from scheduler import scheduler

app = Flask(__name__)
# 配置 Session
app.secret_key = config.SECRET_KEY or secrets.token_hex(32)
app.config['PERMANENT_SESSION_LIFETIME'] = config.PERMANENT_SESSION_LIFETIME


@app.before_request
def require_login_for_app():
    """Protect all application/API routes except login and static assets."""
    path = request.path or ''
    if request.method == 'OPTIONS':
        return None
    if request.endpoint == 'static' or path.startswith('/static/'):
        return None
    if path in ('/login', '/api/user/login', '/favicon.ico'):
        return None

    token = request.cookies.get('session_token')
    if token and user_db.verify_session(token):
        return None

    if request.is_json or path.startswith('/api/') or '/api/' in path:
        return jsonify({'success': False, 'error': '请先登录'}), 401
    return redirect(url_for('login_page'))

# 初始化用户数据库
user_db = UserDatabase()
user_db.connect()
user_db.create_tables()
user_db.init_default_admin()

# 注意：login_required装饰器和工具函数已移至独立模块
# 从decorators.py导入：login_required
# 从utils.py导入：get_china_time
# 从content_handlers.py导入：extract_with_newspaper3k, extract_article_links_from_list_page, is_valid_article_content

# Newspaper3k提取函数已移至content_handlers.py
# extract_with_newspaper3k, extract_article_links_from_list_page 等函数已从content_handlers导入

# 注册蓝图
app.register_blueprint(smart_bp)
app.register_blueprint(incremental_bp)
app.register_blueprint(sqlite_bp)
app.register_blueprint(article_management_bp)
app.register_blueprint(url_management_bp)
app.register_blueprint(schedule_management_bp)
app.register_blueprint(schedule_execution_bp)
app.register_blueprint(crawl_task_bp)
app.register_blueprint(category_bp)
app.register_blueprint(config_management_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(user_bp)

# Redis配置
REDIS_HOST = config.REDIS_HOST
REDIS_PORT = config.REDIS_PORT
REDIS_DB = config.REDIS_DB
REDIS_PASSWORD = config.REDIS_PASSWORD

# 初始化Redis连接
redis_client = None
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    # 测试Redis连接
    redis_client.ping()
    print("Redis连接成功")
except Exception as e:
    print(f"Redis连接失败: {str(e)}")
    redis_client = None

# 初始化SQLite数据库连接
try:
    if sqlite_db.connect():
        # 确保数据库表已创建
        sqlite_db.create_tables()
        print("SQLite数据库初始化成功")
        
        # 自动迁移旧的URL数据
        try:
            urls, total = sqlite_db.get_managed_urls(1, 1)
            if total == 0:
                print("🔄 检测到数据库中没有URL数据，开始从旧数据源迁移...")
                migrated_count = 0
                
                # 尝试从 json_backup/urls_data.json 迁移
                json_backup_path = os.path.join(os.getcwd(), 'json_backup', 'urls_data.json')
                if os.path.exists(json_backup_path):
                    try:
                        with open(json_backup_path, 'r', encoding='utf-8') as f:
                            old_urls = json.load(f)
                            print(f"📂 从 json_backup/urls_data.json 读取到 {len(old_urls)} 个URL")
                            
                            # 去重：使用字典保存，URL作为key（只保留第一次出现的）
                            unique_urls = {}
                            for url_item in old_urls:
                                url = url_item.get('url')
                                if url and url not in unique_urls:
                                    unique_urls[url] = url_item
                            
                            print(f"🔍 去重后剩余 {len(unique_urls)} 个唯一URL")
                            skipped = 0
                            
                            for url, url_item in unique_urls.items():
                                try:
                                    url_data = {
                                        'url': url,
                                        'name': url_item.get('name', ''),
                                        'description': url_item.get('description', ''),
                                        'category': url_item.get('category', '律师事务所'),
                                        'is_active': True,
                                        'auto_crawl': False,
                                        'crawl_frequency': ''
                                    }
                                    
                                    url_id = sqlite_db.insert_managed_url(url_data)
                                    if url_id:
                                        migrated_count += 1
                                    else:
                                        skipped += 1
                                except Exception as url_e:
                                    print(f"⚠️ 迁移URL失败 {url}: {url_e}")
                                    skipped += 1
                                    continue
                            
                            print(f"✅ 成功从 json_backup 迁移 {migrated_count} 个URL")
                            if skipped > 0:
                                print(f"⚠️ 跳过 {skipped} 个URL（可能已存在或插入失败）")
                    except Exception as json_e:
                        print(f"❌ 读取 json_backup/urls_data.json 失败: {json_e}")
                
                # 如果还是没有数据，尝试从 law_firms_urls.js 迁移
                urls, total = sqlite_db.get_managed_urls(1, 1)
                if total == 0:
                    law_firms_js_path = os.path.join(os.getcwd(), 'static', 'js', 'law_firms_urls.js')
                    if os.path.exists(law_firms_js_path):
                        try:
                            with open(law_firms_js_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                                # 简单解析 JavaScript 数组
                                import re
                                pattern = r'\{\s*id:\s*\d+,\s*name:\s*"([^"]+)",\s*url:\s*"([^"]+)"'
                                matches = re.findall(pattern, content)
                                
                                print(f"📂 从 law_firms_urls.js 解析到 {len(matches)} 个URL")
                                
                                # 去重
                                unique_matches = {}
                                for name, url in matches:
                                    if url not in unique_matches:
                                        unique_matches[url] = name
                                
                                print(f"🔍 去重后剩余 {len(unique_matches)} 个唯一URL")
                                js_skipped = 0
                                
                                for url, name in unique_matches.items():
                                    try:
                                        url_data = {
                                            'url': url,
                                            'name': name,
                                            'description': '',
                                            'category': '律师事务所',
                                            'is_active': True,
                                            'auto_crawl': False,
                                            'crawl_frequency': ''
                                        }
                                        
                                        url_id = sqlite_db.insert_managed_url(url_data)
                                        if url_id:
                                            migrated_count += 1
                                        else:
                                            js_skipped += 1
                                    except Exception as url_e:
                                        print(f"⚠️ 迁移URL失败 {url}: {url_e}")
                                        js_skipped += 1
                                        continue
                                
                                print(f"✅ 成功从 law_firms_urls.js 迁移 {migrated_count} 个URL")
                                if js_skipped > 0:
                                    print(f"⚠️ 跳过 {js_skipped} 个URL（可能已存在或插入失败）")
                        except Exception as js_e:
                            print(f"❌ 读取 law_firms_urls.js 失败: {js_e}")
                
                # 最终统计
                urls, total = sqlite_db.get_managed_urls(1, 1)
                print(f"📊 数据库中现在有 {total} 个URL")
        except Exception as migrate_e:
            print(f"❌ URL数据迁移失败: {migrate_e}")
    else:
        print("SQLite数据库连接失败")
except Exception as e:
    print(f"SQLite数据库初始化失败: {str(e)}")

# 注意: is_valid_article_content, extract_article_content_from_url, clean_article_content 等函数
# 已从相应模块导入（content_handlers.py, smart_article_extractor.py），不在此重复定义
# 爬取结果存储目录（从config导入）
CRAWL_RESULTS_DIR = config.CRAWL_RESULTS_DIR

# 爬取任务状态存储和线程锁
crawl_tasks = {}
task_lock = Lock()
schedule_thread = None
schedule_thread_lock = Lock()
_scheduler_process_lock_handle = None

# 美观的HTML模板（使用Tailwind CSS）
# HTML模板已移至templates目录


def _legacy_firecrawl_disabled_response(status_code=410):
    """Consistent response for old external Firecrawl endpoints."""
    return jsonify({
        'success': False,
        'configured': False,
        'disabled': True,
        'message': '旧版 Firecrawl 外部爬取已停用，请使用首页的通用文章爬取（Playwright）配置。',
        'replacement': '/api/start-article-crawl'
    }), status_code


def _coerce_days_limit(value, default=7):
    """Normalize date-window days; 0 means no date limit."""
    if value in (None, ''):
        return default
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return default


def _coerce_int(value, default=0, min_value=None, max_value=None):
    """Normalize integer request values without letting bad input 500 an API."""
    if value in (None, ''):
        result = default
    else:
        try:
            result = int(float(value))
        except (TypeError, ValueError):
            result = default
    if result is None:
        return None
    if min_value is not None:
        result = max(min_value, result)
    if max_value is not None:
        result = min(max_value, result)
    return result


def _normalize_schedule_list(value, min_value, max_value):
    """Normalize weekly/monthly schedule day values to a comma-separated string."""
    result = []
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = str(value or '').split(',')
    for item in raw_items:
        parsed = _coerce_int(item, None)
        if parsed is not None and min_value <= parsed <= max_value and parsed not in result:
            result.append(parsed)
    return ','.join(str(item) for item in sorted(result))


# Flask路由
@app.route('/')
@login_required
def index():
    return render_template('index.html')


@app.route('/api/crawl-runtime-config', methods=['GET'])
def crawl_runtime_config():
    """Return public defaults for the Playwright article crawler UI."""
    return jsonify(public_runtime_config())


@app.route('/task-extracted-content/<path:task_id>')
@login_required
def task_extracted_content(task_id):
    """任务已提取内容卡片页。"""
    return render_template('task_extracted_content.html', task_id=task_id)


def _split_keywords(value):
    """Split comma/newline/Chinese punctuation separated keyword text."""
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = re.split(r'[,，、;\n\r]+', str(value or ''))

    keywords = []
    seen = set()
    for item in raw_items:
        keyword = str(item or '').strip()
        keyword = re.sub(r'^\[[^\]]+\]', '', keyword).strip()
        keyword = re.sub(r'^(标题|標題|正文|内容|內容|文)\s*[:：]', '', keyword).strip()
        key = keyword.lower()
        if keyword and key not in seen:
            keywords.append(keyword)
            seen.add(key)
    return keywords


def _article_keyword_snippets(title, content, keywords, radius=90, max_snippets=3):
    """Build plain-text body snippets around matched keywords."""
    title_text = re.sub(r'\s+', ' ', str(title or '')).strip()
    searchable_content = re.sub(r'\s+', ' ', str(content or '')).strip()
    if not (title_text or searchable_content) or not keywords:
        return [], []

    lowered_title = title_text.lower()
    lowered = searchable_content.lower()
    matches = []
    matched_keywords = []
    seen_keywords = set()
    for keyword in keywords:
        keyword_lower = keyword.lower()
        if not keyword_lower:
            continue
        if keyword_lower in lowered_title and keyword_lower not in seen_keywords:
            matched_keywords.append(keyword)
            seen_keywords.add(keyword_lower)
        start = lowered.find(keyword_lower)
        if start >= 0:
            matches.append((start, start + len(keyword), keyword))
            if keyword_lower not in seen_keywords:
                matched_keywords.append(keyword)
                seen_keywords.add(keyword_lower)

    if not matches:
        if matched_keywords:
            preview = searchable_content[:180].strip() or title_text
            if len(searchable_content) > 180:
                preview += '...'
            return [preview], matched_keywords
        return [], []

    matches.sort(key=lambda item: item[0])
    snippets = []
    used_ranges = []
    for start, end, _keyword in matches:
        snippet_start = max(0, start - radius)
        snippet_end = min(len(searchable_content), end + radius)

        overlaps = False
        for used_start, used_end in used_ranges:
            if snippet_start <= used_end and snippet_end >= used_start:
                overlaps = True
                break
        if overlaps:
            continue

        snippet = searchable_content[snippet_start:snippet_end].strip()
        if snippet_start > 0:
            snippet = '...' + snippet
        if snippet_end < len(searchable_content):
            snippet = snippet + '...'
        snippets.append(snippet)
        used_ranges.append((snippet_start, snippet_end))
        if len(snippets) >= max_snippets:
            break

    return snippets, matched_keywords


@app.route('/api/tasks/<path:task_id>/extracted-content', methods=['GET'])
def get_task_extracted_content(task_id):
    """Return task-scoped extracted articles with keyword snippets for card display."""
    try:
        page = _coerce_int(request.args.get('page'), 1, 1)
        per_page = _coerce_int(request.args.get('per_page'), 100, 1, 500)

        articles, total = sqlite_db.get_articles_by_task_id(task_id, page, per_page)
        task = sqlite_db.get_crawl_task_by_task_id(task_id)

        cards = []
        for article in articles:
            task_keywords = _split_keywords(article.get('task_keywords'))
            keywords = task_keywords or _split_keywords(article.get('matched_keywords'))
            snippets, matched_keywords = _article_keyword_snippets(
                article.get('title'),
                article.get('content'),
                keywords
            )
            if not snippets:
                continue

            cards.append({
                'id': article.get('id'),
                'title': article.get('title') or '无标题',
                'url': article.get('url') or '',
                'domain': article.get('domain') or '',
                'publish_date': article.get('publish_date') or '',
                'created_at': article.get('created_at') or '',
                'content_length': article.get('content_length') or len(article.get('content') or ''),
                'extraction_method': article.get('extraction_method') or '',
                'quality_score': article.get('quality_score') or 0,
                'matched_keywords': matched_keywords,
                'snippets': snippets,
                'content': article.get('content') or ''
            })

        return jsonify({
            'success': True,
            'task': task or {'task_id': task_id},
            'articles': cards,
            'total': total,
            'matched_total': len(cards),
            'page': page,
            'per_page': per_page
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取任务提取内容失败: {str(e)}'
        }), 500


# URL管理页面
@app.route('/url-management')
@login_required
def url_management():
    return render_template('url_management.html')

# 分类管理页面
@app.route('/category-management')
@login_required
def category_management():
    return redirect('/url-management')

# Newspaper3k爬取页面
@app.route('/newspaper3k')
@login_required
def newspaper3k_page():
    return render_template('newspaper3k.html')

# 任务管理页面
@app.route('/schedule-management')
@login_required
def schedule_management():
    return render_template('schedule_management.html')

@app.route('/schedule-execution-management')
@login_required
def schedule_execution_management():
    return render_template('schedule_execution_management.html')

@app.route('/config-management')
@login_required
def config_management():
    return render_template('config_management.html')

@app.route('/batch-schedule')
@login_required
def batch_schedule():
    return render_template('batch_schedule.html')

@app.route('/thread-monitor')
@login_required
def thread_monitor():
    return render_template('thread_monitor.html')

@app.route('/api/thread-monitor/status')
@login_required
def thread_monitor_status():
    import re as _re
    from datetime import datetime as _dt
    now = get_china_time()
    today_str = now.strftime('%Y-%m-%d')
    now_minutes = now.hour * 60 + now.minute + now.second / 60.0

    def parse_minutes(s):
        if not s: return None
        try:
            clean = str(s).replace('Z', '').strip()
            if 'T' not in clean and ' ' in clean:
                clean = clean.replace(' ', 'T')
            d = _dt.fromisoformat(clean)
            return d.hour * 60 + d.minute + d.second / 60.0
        except:
            return None

    # 1. 获取活跃定时任务的显示名称
    sch_tasks, _ = sqlite_db.get_scheduled_tasks(page=1, per_page=1000, is_active=True)
    sch_map = {s['id']: s for s in sch_tasks}

    # 2. 运行中的任务
    running_raw = scheduler.get_running_tasks()
    running_tasks = []
    for t in running_raw:
        sid = t.get('schedule_id')
        sch = sch_map.get(sid, {})
        display = sch.get('url_display_name') or sch.get('task_name') or t.get('task_name', '未知')
        sm = parse_minutes(t.get('started_at')) or now_minutes
        running_tasks.append({
            'schedule_id': sid,
            'display_name': display,
            'domain': t.get('domain', ''),
            'started_at': str(t.get('started_at', '')),
            'started_minutes': round(sm, 2),
            'now_minutes': round(now_minutes, 2),
            'duration_minutes': round(now_minutes - sm, 1),
            'stop_flag': t.get('stop_flag', False),
            'type': 'running',
            'status': 'running'
        })

    # 3. 今日已完成任务
    completed_today = []
    try:
        with sqlite_db.lock:
            cur = sqlite_db.connection.cursor()
            cur.execute("""
                SELECT ct.task_name, ct.target_url, ct.started_at, ct.completed_at,
                       ct.status, ct.schedule_id, ct.articles_found
                FROM crawl_tasks ct
                WHERE (DATE(ct.started_at)=? OR DATE(ct.completed_at)=?)
                  AND ct.status IN ('completed','failed','timeout','stopped')
                ORDER BY ct.started_at LIMIT 300
            """, (today_str, today_str))
            rows = [dict(r) for r in cur.fetchall()]
        for c in rows:
            sm = parse_minutes(c.get('started_at'))
            cm = parse_minutes(c.get('completed_at')) or now_minutes
            if sm is None: continue
            sid = c.get('schedule_id')
            sch = sch_map.get(sid, {})
            display = sch.get('url_display_name') or sch.get('task_name') or c.get('task_name', '')
            completed_today.append({
                'display_name': display,
                'task_name': c.get('task_name', ''),
                'started_minutes': round(max(0, sm), 2),
                'completed_minutes': round(min(1440, cm), 2),
                'duration_minutes': round(cm - sm, 1),
                'status': c.get('status', 'completed'),
                'articles_found': c.get('articles_found') or 0,
                'type': 'completed'
            })
    except Exception as e:
        print(f'thread-monitor completed query error: {e}')

    # 4. 待执行任务（today future）
    pending_tasks = []
    for st in sch_tasks:
        nr = st.get('next_run')
        if not nr: continue
        try:
            clean = str(nr).replace('Z', '').strip()
            if 'T' not in clean and ' ' in clean:
                clean = clean.replace(' ', 'T')
            ndt = _dt.fromisoformat(clean)
            if ndt.strftime('%Y-%m-%d') == today_str and ndt > now:
                nm = ndt.hour * 60 + ndt.minute
                pending_tasks.append({
                    'schedule_id': st['id'],
                    'display_name': st.get('url_display_name') or st.get('task_name', ''),
                    'next_run_display': ndt.strftime('%H:%M'),
                    'next_minutes': nm
                })
        except:
            continue
    pending_tasks.sort(key=lambda x: x['next_minutes'])

    stats = scheduler.get_concurrent_stats()
    return jsonify({
        'success': True,
        'current_time': now.isoformat(),
        'current_time_display': now.strftime('%H:%M:%S'),
        'now_minutes': round(now_minutes, 2),
        'max_concurrent': scheduler.max_concurrent_tasks,
        'running_tasks': running_tasks,
        'completed_today': completed_today,
        'pending_tasks': pending_tasks,
        'stats': stats
    })

@app.route('/api/thread-monitor/set-concurrent', methods=['POST'])
@login_required
def set_max_concurrent():
    import re as _re
    data = request.get_json(silent=True) or {}
    n = max(1, min(32, int(data.get('max_concurrent', 4))))
    scheduler.max_concurrent_tasks = n
    # 持久化到 .env
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    try:
        with open(env_path, 'r', encoding='utf-8') as f:
            content = f.read()
        content = _re.sub(r'CRAWL_SCHEDULER_MAX_CONCURRENT=\d+',
                          f'CRAWL_SCHEDULER_MAX_CONCURRENT={n}', content)
        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(content)
    except Exception as e:
        print(f'Warning: could not update .env: {e}')
    return jsonify({'success': True, 'max_concurrent': n, 'message': f'并发数已更新为 {n}'})

# API路由：保存API密钥
@app.route('/api/save-api-key', methods=['POST'])
def save_api_key():
    return _legacy_firecrawl_disabled_response()

# 取消任务
@app.route('/api/cancel-task', methods=['POST'])
def cancel_task():
    try:
        data = request.get_json(silent=True) or {}
        task_id = (data.get('task_id') or '').strip()
        if not task_id:
            return jsonify({'success': False, 'message': 'task_id 必填'})

        cancelled_sources = []
        with task_lock:
            task = crawl_tasks.get(task_id)
            if task:
                task['cancelled'] = True
                task['status'] = 'cancelled'
                task['completed_at'] = get_china_time().isoformat()
                cancelled_sources.append('memory')

        db_task = sqlite_db.get_crawl_task_by_task_id(task_id)
        if db_task:
            if db_task.get('status') in ('completed', 'failed', 'cancelled'):
                return jsonify({
                    'success': True,
                    'message': f"任务已是终态: {db_task.get('status')}",
                    'status': db_task.get('status')
                })

            sqlite_db.update_crawl_task_status(
                task_id,
                'cancelled',
                progress=db_task.get('progress', 0),
                error_message='用户取消任务'
            )
            cancelled_sources.append('database')

        schedule_match = re.match(r'^schedule_(\d+)_', task_id)
        if schedule_match:
            schedule_id = _coerce_int(schedule_match.group(1), None, 1)
            if schedule_id is not None:
                try:
                    scheduler.stop_task(schedule_id)
                    cancelled_sources.append('scheduler')
                except Exception as scheduler_error:
                    print(f"⚠️ 通知调度器停止任务失败: {scheduler_error}")
                try:
                    sqlite_db.release_scheduled_task_run(schedule_id)
                    cancelled_sources.append('schedule_lock')
                except Exception as release_error:
                    print(f"⚠️ 释放定时任务运行锁失败: {release_error}")

        if not cancelled_sources:
            return jsonify({'success': False, 'message': '任务不存在'})

        return jsonify({
            'success': True,
            'message': '任务已取消',
            'cancelled_sources': cancelled_sources
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# 重试任务（基于旧任务参数创建新任务）
def _build_retry_payload_from_task(task_id: str) -> Optional[dict]:
    """Build a start-crawl payload from memory, crawl_tasks DB, or schedule config."""
    with task_lock:
        old = crawl_tasks.get(task_id)
    if old:
        return {
            'urls': old.get('urls') or [old.get('url')],
            'task_name': old.get('task_name', ''),
            'depth': old.get('depth', 2),
            'mode': 'article_crawl',
            'limit': old.get('limit', 50),
            'include_links': old.get('include_links', []),
            'exclude_links': old.get('exclude_links', []),
            'keywords': old.get('keywords', ''),
            'kb_id': old.get('kb_id', ''),
            'days_limit': old.get('days_limit', 7),
            'start_date': old.get('start_date'),
            'end_date': old.get('end_date'),
            **(old.get('crawl_options') or {}),
        }

    db_task = sqlite_db.get_crawl_task_by_task_id(task_id)
    if db_task:
        target_url = normalize_task_url(db_task.get('target_url'))
        if target_url:
            return {
                'urls': [target_url],
                'task_name': db_task.get('task_name') or '',
                'depth': db_task.get('crawl_depth') or 1,
                'mode': 'article_crawl',
                'limit': db_task.get('page_limit') or 50,
                'incremental': bool(db_task.get('incremental_mode')),
                'keywords': db_task.get('keywords') or '',
                'days_limit': 7,
            }

    schedule_match = re.match(r'^schedule_(\d+)_', task_id or '')
    if schedule_match:
        schedule_id = _coerce_int(schedule_match.group(1), None, 1)
        schedule = sqlite_db.get_scheduled_task(schedule_id) if schedule_id is not None else None
        if schedule:
            schedule_config = schedule.get('config') if isinstance(schedule.get('config'), dict) else {}
            target_url = normalize_task_url(schedule.get('target_url') or '')
            if not target_url and schedule.get('url_id'):
                managed_url = sqlite_db.get_managed_url_by_id(schedule.get('url_id'))
                target_url = normalize_task_url((managed_url or {}).get('url'))
            if target_url:
                payload = {
                    'urls': [target_url],
                    'task_name': schedule.get('task_name') or '',
                    'depth': schedule_config.get('depth', 1),
                    'mode': 'article_crawl',
                    'limit': schedule_config.get('limit', 50),
                    'keywords': schedule.get('keywords') or schedule_config.get('keywords', ''),
                    'kb_id': schedule.get('ragflow_kb_id') or schedule_config.get('kb_id', ''),
                    'days_limit': schedule.get('days_limit') or schedule_config.get('days_limit', 7),
                }
                payload.update(schedule_config)
                payload['mode'] = 'article_crawl'
                return payload

    return None


def _retry_existing_task(task_id: str, payload: dict):
    urls = payload.get('urls') or ([payload.get('url')] if payload.get('url') else [])
    url = normalize_task_url(urls[0] if urls else '')
    if not url:
        return jsonify({'success': False, 'message': '原任务缺少有效URL，不能重试'})

    with task_lock:
        existing = crawl_tasks.get(task_id, {})
        if existing.get('status') == 'running':
            return jsonify({'success': False, 'message': '任务正在运行，不能重复重试'})

        retry_count = int(existing.get('retry_count') or 0) + 1
        created_at = existing.get('created_at') or get_china_time().isoformat()
        crawl_tasks[task_id] = {
            **existing,
            'task_id': task_id,
            'urls': [url],
            'url': url,
            'task_name': payload.get('task_name') or existing.get('task_name') or f'爬取任务-{url}',
            'depth': payload.get('depth', existing.get('depth', 1)),
            'mode': 'article_crawl',
            'limit': payload.get('limit', existing.get('limit', 50)),
            'include_links': payload.get('include_links', existing.get('include_links', [])),
            'exclude_links': payload.get('exclude_links', existing.get('exclude_links', [])),
            'incremental': bool(payload.get('incremental', existing.get('incremental', False))),
            'keywords': payload.get('keywords', existing.get('keywords', '')),
            'kb_id': payload.get('kb_id', existing.get('kb_id', '')),
            'days_limit': _coerce_days_limit(payload.get('days_limit', existing.get('days_limit', 7)), 7),
            'start_date': payload.get('start_date', existing.get('start_date')),
            'end_date': payload.get('end_date', existing.get('end_date')),
            'crawl_options': normalize_crawl_options(payload),
            'cancelled': False,
            'status': 'pending',
            'progress': 0,
            'created_at': created_at,
            'started_at': None,
            'completed_at': None,
            'data': None,
            'result': None,
            'error': None,
            'logs': [],
            'retry_count': retry_count,
        }

    sqlite_db.reset_crawl_task_for_retry(
        task_id,
        target_url=url,
        task_name=payload.get('task_name') or existing.get('task_name') or f'爬取任务-{url}',
        crawl_depth=payload.get('depth') or existing.get('depth') or 1,
        crawl_mode='article_crawl',
        page_limit=payload.get('limit') or existing.get('limit') or 50,
        incremental_mode=bool(payload.get('incremental', existing.get('incremental', False))),
        keywords=payload.get('keywords', existing.get('keywords', '')),
    )

    _append_task_log(task_id, f"任务重试已开始（第 {retry_count} 次），复用原任务记录")
    thread = Thread(target=run_article_crawl_task, args=(task_id,))
    thread.daemon = True
    thread.start()
    return jsonify({'success': True, 'task_ids': [task_id], 'reused_task_id': task_id, 'message': '已重试原任务'})


@app.route('/api/retry-task', methods=['POST'])
def retry_task():
    try:
        data = request.get_json(silent=True) or {}
        task_id = (data.get('task_id') or '').strip()
        if not task_id:
            return jsonify({'success': False, 'message': 'task_id 必填'})
        new_payload = _build_retry_payload_from_task(task_id)
        if not new_payload and data.get('fallback'):
            fallback = data.get('fallback') if isinstance(data.get('fallback'), dict) else {}
            fallback_url = normalize_task_url(fallback.get('url') or fallback.get('target_url') or '')
            if fallback_url:
                new_payload = {
                    'urls': [fallback_url],
                    'depth': fallback.get('depth') or fallback.get('crawl_depth') or 1,
                    'mode': 'article_crawl',
                    'limit': fallback.get('limit') or fallback.get('page_limit') or 50,
                    'incremental': bool(fallback.get('incremental') or fallback.get('incremental_mode')),
                    'keywords': fallback.get('keywords') or '',
                    'kb_id': fallback.get('kb_id') or fallback.get('ragflow_kb_id') or '',
                    'days_limit': fallback.get('days_limit') or 7,
                    **(fallback.get('crawl_options') if isinstance(fallback.get('crawl_options'), dict) else {}),
                }
        if not new_payload:
            return jsonify({'success': False, 'message': '无法找到原任务参数，不能重试'})
        return _retry_existing_task(task_id, new_payload)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：获取API密钥
@app.route('/api/get-api-key')
def get_api_key():
    return jsonify({
        'success': True,
        'configured': False,
        'disabled': True,
        'api_key': '',
        'api_url': '',
        'message': '旧版 Firecrawl 配置已停用；当前使用 Playwright 通用文章爬取。'
    })

# API路由：提取文章链接并下载
@app.route('/api/extract-and-download-articles', methods=['POST'])
def extract_and_download_articles():
    """Legacy external Firecrawl result endpoint; intentionally disabled."""
    return _legacy_firecrawl_disabled_response()


@app.route('/api/article-stats', methods=['GET'])
def get_article_stats():
    """获取文章数据库统计信息"""
    try:
        db = sqlite_db
        stats = db.get_statistics()
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取统计信息失败: {str(e)}'
        }), 500

# API路由：获取最近爬取的文章
@app.route('/api/recent-articles', methods=['GET'])
def get_recent_articles():
    """获取最近爬取的文章"""
    try:
        limit = request.args.get('limit', 50, type=int)
        db = sqlite_db
        articles = db.get_recent_articles(limit)
        return jsonify({
            'success': True,
            'articles': articles
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取最近文章失败: {str(e)}'
        }), 500

# API路由：使用Newspaper3k提取单个URL内容
@app.route('/api/newspaper3k-extract', methods=['POST'])
def newspaper3k_extract():
    """使用Newspaper3k提取单个URL的内容"""
    try:
        data = request.get_json(silent=True) or {}
        url = data.get('url')
        
        if not url:
            return jsonify({
                'success': False,
                'error': '请提供URL'
            }), 400
        
        # 使用newspaper3k提取内容
        result = extract_with_newspaper3k(url)
        
        if result['success']:
            # 生成任务ID
            task_id = str(uuid.uuid4())
            
            # 保存结果到数据库（可选）
            try:
                db = sqlite_db
                if db:
                    # 保存到文章表
                    article_data = {
                        'url': url,
                        'title': result.get('title', ''),
                        'content': result.get('text_content', ''),
                        'author': result.get('author', ''),
                        'publish_date': result.get('date', ''),
                        'extracted_at': get_china_time().isoformat(),
                        'extraction_method': 'newspaper3k',
                        'task_id': task_id
                    }
                    db.add_article(article_data)
            except Exception as db_error:
                print(f"保存到数据库失败: {db_error}")
            
            return jsonify({
                'success': True,
                'task_id': task_id,
                'data': result
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('error', '提取失败')
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'处理请求失败: {str(e)}'
        }), 500

# API路由：使用Newspaper3k批量提取多个URL
@app.route('/api/newspaper3k-batch', methods=['POST'])
def newspaper3k_batch():
    """使用Newspaper3k批量提取多个URL的内容"""
    try:
        data = request.get_json(silent=True) or {}
        urls = data.get('urls', [])
        
        if not urls:
            return jsonify({
                'success': False,
                'error': '请提供URL列表'
            }), 400
        
        results = []
        batch_id = str(uuid.uuid4())
        
        for url in urls[:10]:  # 限制最多10个URL
            try:
                result = extract_with_newspaper3k(url)
                result['url'] = url
                results.append(result)
                
                # 保存成功的结果到数据库
                if result['success']:
                    try:
                        db = sqlite_db
                        if db:
                            article_data = {
                                'url': url,
                                'title': result.get('title', ''),
                                'content': result.get('text_content', ''),
                                'author': result.get('author', ''),
                                'publish_date': result.get('date', ''),
                                'extracted_at': get_china_time().isoformat(),
                                'extraction_method': 'newspaper3k',
                                'task_id': batch_id
                            }
                            db.add_article(article_data)
                    except Exception as db_error:
                        print(f"保存文章到数据库失败: {db_error}")
                        
            except Exception as e:
                results.append({
                    'url': url,
                    'success': False,
                    'error': f'提取失败: {str(e)}'
                })
                
            # 添加延迟，避免请求过于频繁
            time.sleep(1)
        
        success_count = sum(1 for r in results if r.get('success'))
        
        return jsonify({
            'success': True,
            'batch_id': batch_id,
            'results': results,
            'total_count': len(results),
            'success_count': success_count,
            'failed_count': len(results) - success_count
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'批量处理失败: {str(e)}'
        }), 500

# API路由：从列表页面自动提取文章链接
@app.route('/api/newspaper3k-extract-links', methods=['POST'])
def newspaper3k_extract_links():
    """从列表页面自动提取文章链接"""
    try:
        data = request.get_json(silent=True) or {}
        list_url = data.get('url')
        
        if not list_url:
            return jsonify({
                'success': False,
                'error': '请提供列表页面URL'
            }), 400
        
        # 提取文章链接
        article_links = extract_article_links_from_list_page(list_url)
        
        if article_links['success']:
            return jsonify({
                'success': True,
                'list_url': list_url,
                'article_links': article_links['links'],
                'total_links': len(article_links['links']),
                'extraction_method': 'intelligent_link_extraction'
            })
        else:
            return jsonify({
                'success': False,
                'error': article_links.get('error', '提取链接失败')
            }), 500
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'提取链接失败: {str(e)}'
        }), 500

# API路由：自动提取链接并批量处理文章
@app.route('/api/newspaper3k-auto-extract', methods=['POST'])
def newspaper3k_auto_extract():
    """自动提取列表页面的文章链接并批量处理"""
    try:
        data = request.get_json(silent=True) or {}
        list_url = data.get('url')
        max_articles = data.get('max_articles', 5)  # 默认最多处理5篇文章
        
        if not list_url:
            return jsonify({
                'success': False,
                'error': '请提供列表页面URL'
            }), 400
        
        # 首先提取文章链接
        article_links_result = extract_article_links_from_list_page(list_url)
        
        if not article_links_result['success']:
            return jsonify({
                'success': False,
                'error': f'提取链接失败: {article_links_result.get("error", "未知错误")}'
            }), 500
        
        article_links = article_links_result['links'][:max_articles]  # 限制处理数量
        
        if not article_links:
            return jsonify({
                'success': False,
                'error': '未找到文章链接'
            }), 404
        
        # 批量处理文章
        results = []
        batch_id = str(uuid.uuid4())
        
        for i, link_info in enumerate(article_links):
            try:
                article_url = link_info['url']
                result = extract_with_newspaper3k(article_url)
                result['url'] = article_url
                result['link_text'] = link_info.get('text', '')
                result['index'] = i + 1
                results.append(result)
                
                # 保存成功的结果到数据库
                if result['success']:
                    try:
                        db = sqlite_db
                        if db:
                            article_data = {
                                'url': article_url,
                                'title': result.get('title', link_info.get('text', '')),
                                'content': result.get('text_content', ''),
                                'author': result.get('author', ''),
                                'publish_date': result.get('date', ''),
                                'extracted_at': get_china_time().isoformat(),
                                'extraction_method': 'newspaper3k_auto',
                                'task_id': batch_id,
                                'source_list_url': list_url
                            }
                            db.add_article(article_data)
                    except Exception as db_error:
                        print(f"保存文章到数据库失败: {db_error}")
                        
            except Exception as e:
                results.append({
                    'url': link_info['url'],
                    'link_text': link_info.get('text', ''),
                    'index': i + 1,
                    'success': False,
                    'error': f'提取失败: {str(e)}'
                })
                
            # 添加延迟，避免请求过于频繁
            time.sleep(2)
        
        success_count = sum(1 for r in results if r.get('success'))
        
        return jsonify({
            'success': True,
            'batch_id': batch_id,
            'list_url': list_url,
            'total_links_found': len(article_links_result['links']),
            'processed_count': len(results),
            'success_count': success_count,
            'failed_count': len(results) - success_count,
            'results': results
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'自动提取处理失败: {str(e)}'
        }), 500

# API路由：增量爬取文章
@app.route('/api/incremental-crawl', methods=['POST'])
def incremental_crawl():
    """增量爬取文章"""
    try:
        data = request.get_json(silent=True) or {}
        task_id = data.get('task_id')
        incremental = data.get('incremental', True)
        clean_content = data.get('clean_content', False)
        
        if not task_id:
            return jsonify({'success': False, 'error': '缺少task_id参数'}), 400
        
        # 从任务详情文件中获取markdown内容
        detail_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_detail.json")
        if not os.path.exists(detail_file):
            return jsonify({'success': False, 'error': f'任务详情文件不存在: {task_id}'}), 400
        
        with open(detail_file, 'r', encoding='utf-8') as f:
            task_detail = json.load(f)
        
        # 解析markdown内容
        if isinstance(task_detail, dict) and 'data' in task_detail:
            inner_data = task_detail['data']
            if isinstance(inner_data, dict) and 'data' in inner_data:
                markdown_list = inner_data['data']
                if isinstance(markdown_list, list) and len(markdown_list) > 0:
                    markdown_content = ""
                    base_url = task_detail.get('url', '')
                    
                    for item in markdown_list:
                        if isinstance(item, dict) and 'markdown' in item:
                            markdown_content += item['markdown'] + "\n\n"
                else:
                    return jsonify({'success': False, 'error': '任务详情中没有markdown数据'}), 400
            else:
                return jsonify({'success': False, 'error': '任务详情结构错误'}), 400
        else:
            return jsonify({'success': False, 'error': '任务详情格式错误'}), 400
        
        if not markdown_content:
            return jsonify({'success': False, 'error': 'markdown内容为空'}), 400
        
        # 创建文章链接提取器（启用智能验证）
        extractor = ArticleLinkExtractor(enable_smart_validation=True)
        
        # 从markdown中提取链接
        article_links = extractor.extract_links_from_markdown(markdown_content, base_url)
        
        if not article_links:
            return jsonify({'success': False, 'error': '没有找到有效的文章链接'}), 400
        
        # 增量爬取文章
        crawled_articles = extractor.crawl_articles_incremental(
            article_links, base_url, clean_content, incremental
        )
        
        # 创建ZIP文件
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 添加文章链接总览文件
            overview_content = f"增量爬取报告\n{'='*50}\n\n"
            overview_content += f"爬取时间: {get_china_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
            overview_content += f"原始URL: {base_url}\n"
            overview_content += f"增量模式: {'是' if incremental else '否'}\n"
            overview_content += f"找到文章总数: {len(article_links)}\n"
            overview_content += f"新爬取文章数: {len(crawled_articles)}\n"
            overview_content += f"跳过已存在文章数: {len(article_links) - len(crawled_articles)}\n\n"
            
            if crawled_articles:
                overview_content += "新爬取的文章:\n"
                for i, article in enumerate(crawled_articles, 1):
                    overview_content += f"{i}. {article.get('title', '无标题')}\n"
                    overview_content += f"   URL: {article.get('url', '')}\n"
                    overview_content += f"   发布时间: {article.get('publish_date', '未知')}\n\n"
            else:
                overview_content += "没有新文章需要爬取。\n"
            
            zip_file.writestr("00_增量爬取报告.txt", overview_content.encode('utf-8'))
            
            # 添加新爬取的文章内容
            for i, article in enumerate(crawled_articles, 1):
                content = article.get('content', '')
                title = article.get('title', '无标题')
                publish_date = article.get('publish_date', '未知时间')
                
                # 创建文章文件内容
                article_content = f"标题: {title}\n"
                article_content += f"发布时间: {publish_date}\n"
                article_content += f"URL: {article.get('url', '')}\n"
                article_content += f"爬取时间: {get_china_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
                article_content += f"{'='*50}\n\n"
                article_content += content
                
                # 清理文件名中的特殊字符
                safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{i:02d}_{safe_title[:50]}.txt"
                
                zip_file.writestr(filename, article_content.encode('utf-8'))
        
        # 准备下载
        zip_buffer.seek(0)
        
        # 生成文件名
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        domain = parsed_url.netloc.replace('www.', '')
        current_date = get_china_time().strftime('%Y%m%d_%H%M%S')
        incremental_suffix = "_增量" if incremental else "_全量"
        clean_suffix = "_清洗" if clean_content else ""
        zip_filename = f"{domain}_incremental_crawl_{current_date}{incremental_suffix}{clean_suffix}.zip"
        
        return send_file(
            io.BytesIO(zip_buffer.getvalue()),
            as_attachment=True,
            download_name=zip_filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'增量爬取失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/api/extract-articles-from-task', methods=['POST'])
def extract_articles_from_task():
    """从任务ID中提取文章链接并下载为txt文件"""
    try:
        data = request.get_json(silent=True) or {}
        task_id = data.get('task_id')
        clean_content = data.get('clean_content', False)
        
        if not task_id:
            return jsonify({'success': False, 'error': '缺少task_id参数'}), 400
        
        # 从任务详情文件中获取markdown内容
        detail_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_detail.json")
        if not os.path.exists(detail_file):
            return jsonify({'success': False, 'error': f'任务详情文件不存在: {task_id}'}), 400
        
        with open(detail_file, 'r', encoding='utf-8') as f:
            task_detail = json.load(f)
        
        # 检查是否包含markdown内容
        # 任务详情结构: task_detail.data.data 是一个包含markdown内容的列表
        if isinstance(task_detail, dict) and 'data' in task_detail:
            inner_data = task_detail['data']
            if isinstance(inner_data, dict) and 'data' in inner_data:
                markdown_list = inner_data['data']
                if isinstance(markdown_list, list) and len(markdown_list) > 0:
                    # 合并所有markdown内容
                    markdown_content = ""
                    base_url = task_detail.get('url', '')
                    
                    for item in markdown_list:
                        if isinstance(item, dict) and 'markdown' in item:
                            markdown_content += item['markdown'] + "\n\n"
                else:
                    return jsonify({'success': False, 'error': '任务详情中没有markdown数据'}), 400
            else:
                return jsonify({'success': False, 'error': '任务详情结构错误'}), 400
        else:
            return jsonify({'success': False, 'error': '任务详情格式错误'}), 400
        
        if not markdown_content:
            return jsonify({'success': False, 'error': 'markdown内容为空'}), 400
        
        # 创建文章链接提取器（启用智能验证）
        extractor = ArticleLinkExtractor(enable_smart_validation=True)
        
        # 从markdown中提取链接
        article_links = extractor.extract_links_from_markdown(markdown_content, base_url)
        
        if not article_links:
            return jsonify({'success': False, 'error': '没有找到有效的文章链接'}), 400
        
        # 创建ZIP文件
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # 添加文章链接总览文件
            overview_content = f"文章链接总览\n{'='*50}\n\n"
            overview_content += f"提取时间: {get_china_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
            overview_content += f"原始URL: {base_url}\n"
            overview_content += f"找到文章数量: {len(article_links)}\n\n"
            
            for i, link in enumerate(article_links, 1):
                overview_content += f"{i}. {link['title']}\n"
                overview_content += f"   URL: {link['url']}\n\n"
            
            zip_file.writestr("00_文章链接总览.txt", overview_content.encode('utf-8'))
            
            # 下载每个文章的内容
            for i, link in enumerate(article_links, 1):
                try:
                    # 爬取文章内容
                    article_result = extractor.crawl_article_content(link['url'])
                    
                    if article_result.get('success'):
                        content = article_result.get('content', '')
                        title = article_result.get('title', link['title'])
                        
                        # 如果需要清洗内容（这里content已经是清洗过的）
                        if clean_content:
                            # content已经是清洗过的，不需要再次清洗
                            pass
                        
                        # 获取发布时间
                        publish_date = article_result.get('publish_date', '未知时间')
                        
                        # 创建文章文件内容
                        article_content = f"标题: {title}\n"
                        article_content += f"发布时间: {publish_date}\n"
                        article_content += f"URL: {link['url']}\n"
                        article_content += f"爬取时间: {get_china_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        article_content += f"{'='*50}\n\n"
                        article_content += content
                        
                        # 清理文件名中的特殊字符
                        safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
                        filename = f"{i:02d}_{safe_title[:50]}.txt"
                        
                        zip_file.writestr(filename, article_content.encode('utf-8'))
                        
                except Exception as e:
                    print(f"处理文章 {link['url']} 时出错: {str(e)}")
                    continue
        
        # 准备下载
        zip_buffer.seek(0)
        
        # 生成文件名
        from urllib.parse import urlparse
        parsed_url = urlparse(base_url)
        domain = parsed_url.netloc.replace('www.', '')
        current_date = get_china_time().strftime('%Y%m%d_%H%M%S')
        clean_suffix = "_清洗" if clean_content else ""
        zip_filename = f"{domain}_extracted_articles_{current_date}{clean_suffix}.zip"
        
        return send_file(
            io.BytesIO(zip_buffer.getvalue()),
            as_attachment=True,
            download_name=zip_filename,
            mimetype='application/zip'
        )
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'处理失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/api/extract-links-only', methods=['POST'])
def extract_links_only():
    """从任务ID中提取文章链接（不爬取内容）"""
    try:
        data = request.get_json(silent=True) or {}
        task_id = data.get('task_id')
        
        if not task_id:
            return jsonify({'success': False, 'error': '缺少task_id参数'}), 400
        
        # 从任务详情文件中获取markdown内容
        detail_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_detail.json")
        if not os.path.exists(detail_file):
            return jsonify({'success': False, 'error': f'任务详情文件不存在: {task_id}'}), 400
        
        with open(detail_file, 'r', encoding='utf-8') as f:
            task_detail = json.load(f)
        
        # 检查是否包含markdown内容
        # 任务详情结构: task_detail.data.data 是一个包含markdown内容的列表
        if isinstance(task_detail, dict) and 'data' in task_detail:
            inner_data = task_detail['data']
            if isinstance(inner_data, dict) and 'data' in inner_data:
                markdown_list = inner_data['data']
                if isinstance(markdown_list, list) and len(markdown_list) > 0:
                    # 合并所有markdown内容
                    markdown_content = ""
                    base_url = task_detail.get('url', '')
                    
                    for item in markdown_list:
                        if isinstance(item, dict) and 'markdown' in item:
                            markdown_content += item['markdown'] + "\n\n"
                else:
                    return jsonify({'success': False, 'error': '任务详情中没有markdown数据'}), 400
            else:
                return jsonify({'success': False, 'error': '任务详情结构错误'}), 400
        else:
            return jsonify({'success': False, 'error': '任务详情格式错误'}), 400
        
        if not markdown_content:
            return jsonify({'success': False, 'error': 'markdown内容为空'}), 400
        
        # 创建文章链接提取器（启用智能验证）
        extractor = ArticleLinkExtractor(enable_smart_validation=True)
        
        # 从markdown中提取链接
        article_links = extractor.extract_links_from_markdown(markdown_content, base_url)
        
        if not article_links:
            return jsonify({'success': False, 'error': '没有找到有效的文章链接'}), 400
        
        # 返回链接列表
        return jsonify({
            'success': True,
            'base_url': base_url,
            'total_links': len(article_links),
            'links': article_links[:10]  # 只返回前10个链接
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'处理失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
def _atomic_write_json(path: str, data_obj) -> None:
    tmp_path = path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(data_obj, f, ensure_ascii=False, indent=2)
    try:
        os.replace(tmp_path, path)
    except Exception:
        with open(path, 'w', encoding='utf-8') as f2:
            json.dump(data_obj, f2, ensure_ascii=False, indent=2)

def _load_json_file(path: str) -> list:
    try:
        if not os.path.exists(path):
            return []
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f) or []
    except Exception:
        return []

def _save_schedules(items: list) -> None:
    with _schedules_lock:
        _atomic_write_json(SCHEDULES_FILE, items)

def _save_schedule_execs(items: list) -> None:
    with _schedule_exec_lock:
        _atomic_write_json(SCHEDULE_EXECUTIONS_FILE, items)

def _load_schedules() -> list:
    return _load_json_file(SCHEDULES_FILE)

def _load_schedule_execs() -> list:
    return _load_json_file(SCHEDULE_EXECUTIONS_FILE)

def _load_article_tracking() -> dict:
    """加载文章跟踪数据"""
    return _load_json_file(ARTICLE_TRACKING_FILE) or {}

def _save_article_tracking(tracking_data: dict) -> None:
    """保存文章跟踪数据"""
    with _article_tracking_lock:
        _atomic_write_json(ARTICLE_TRACKING_FILE, tracking_data)

def _is_article_crawled(url: str, publish_date: str = None) -> bool:
    """检查文章是否已经爬取过"""
    tracking_data = _load_article_tracking()
    domain = _domain_from_url(url)
    
    if domain not in tracking_data:
        return False
    
    # 检查URL是否已存在
    for article in tracking_data[domain]:
        if article.get('url') == url:
            # 如果提供了发布日期，检查是否相同
            if publish_date and article.get('publish_date') != publish_date:
                return False
            return True
    
    return False

def _add_crawled_article(url: str, title: str, publish_date: str = None, content_hash: str = None):
    """添加已爬取的文章记录"""
    tracking_data = _load_article_tracking()
    domain = _domain_from_url(url)
    
    if domain not in tracking_data:
        tracking_data[domain] = []
    
    # 检查是否已存在
    for article in tracking_data[domain]:
        if article.get('url') == url:
            # 更新现有记录
            article['title'] = title
            article['publish_date'] = publish_date
            article['content_hash'] = content_hash
            article['last_crawled'] = get_china_time().isoformat()
            _save_article_tracking(tracking_data)
            return
    
    # 添加新记录
    tracking_data[domain].append({
        'url': url,
        'title': title,
        'publish_date': publish_date,
        'content_hash': content_hash,
        'first_crawled': get_china_time().isoformat(),
        'last_crawled': get_china_time().isoformat()
    })
    
    _save_article_tracking(tracking_data)

def _domain_from_url(url: str) -> str:
    """从URL提取域名"""
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        return parsed.netloc or parsed.path
    except:
        return url

def _generate_article_filename(url: str, title: str, publish_date: str = None, index: int = 0) -> str:
    """生成文章文件名：网址+文章标题+发表时间"""
    import re
    from datetime import datetime
    
    # 从URL提取域名
    domain = _domain_from_url(url)
    domain = re.sub(r'[^\w\-.]', '_', domain)  # 清理域名中的特殊字符
    
    # 处理标题 - 如果标题为空或"无标题"，从URL或内容中提取
    if not title or title.strip() in ['无标题', 'Untitled', '']:
        # 从URL路径中提取可能的标题
        url_path = url.split('/')[-1] if '/' in url else url
        url_path = re.sub(r'[^\w\u4e00-\u9fff\-_]', '_', url_path)
        if url_path and url_path != domain:
            clean_title = f"文章_{url_path[:30]}"
        else:
            clean_title = f"文章_{index+1:03d}"
    else:
        # 清理标题
        clean_title = re.sub(r'[^\w\u4e00-\u9fff\-_]', '_', title)
        clean_title = clean_title[:50]  # 限制标题长度
    
    # 处理发布日期
    date_part = ""
    if publish_date:
        # 尝试解析日期并格式化
        try:
            # 尝试多种日期格式
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S']:
                try:
                    parsed_date = datetime.strptime(publish_date[:10], fmt)
                    date_part = f"_{parsed_date.strftime('%Y%m%d')}"
                    break
                except:
                    continue
        except:
            # 如果解析失败，使用原始日期的数字部分
            pattern = r'[^\d]'
            date_part = f"_{re.sub(pattern, '', publish_date[:10])}"
    
    # 如果没有日期，使用当前日期
    if not date_part:
        date_part = f"_{get_china_time().strftime('%Y%m%d')}"
    
    # 组合文件名
    filename = f"{domain}_{clean_title}{date_part}.txt"
    
    # 确保文件名不会太长
    if len(filename) > 200:
        filename = f"{domain}_{clean_title[:30]}{date_part}.txt"
    
    return filename

def _next_id(items: list, id_field: str = 'id') -> int:
    """生成下一个整数ID，如果现有ID是UUID字符串则返回1"""
    if not items:
        return 1
    try:
        # 尝试将所有ID转换为整数
        int_ids = [int(x.get(id_field, 0)) for x in items if x.get(id_field)]
        return max(int_ids) + 1 if int_ids else 1
    except (ValueError, TypeError):
        # 如果ID是UUID字符串或其他非整数格式，返回1
        return 1

# 任务日志工具
def _append_task_log(task_id: str, message: str, max_entries: int = 500) -> None:
    try:
        # 🔥 同时写入文件日志和内存日志
        from crawl_logger import get_crawl_logger
        logger = get_crawl_logger(task_id)
        logger.info(message)  # 写入文件
        
        # 写入内存（用于前端显示）
        with task_lock:
            task = crawl_tasks.get(task_id)
            if not task:
                return
            logs = task.get('logs')
            if logs is None:
                logs = []
                task['logs'] = logs
            timestamp = get_china_time().strftime('%H:%M:%S')
            logs.append(f"[{timestamp}] {message}")
            if len(logs) > max_entries:
                # 保留后半段，避免内存增长
                task['logs'] = logs[-max_entries:]
    except Exception:
        pass

def _normalize_url(u: str) -> str:
    try:
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
        u = (u or '').strip()
        if not u:
            return u
        parsed = urlparse(u)
        scheme = (parsed.scheme or 'https').lower()
        netloc = parsed.netloc.lower()
        path = parsed.path or ''
        # 去尾斜杠（但保留根路径单个斜杠）
        if path.endswith('/') and path != '/':
            path = path[:-1]
        # 清理 fragment
        fragment = ''
        # 过滤无意义 query（排序并去除空值）
        q = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=False) if v]
        q.sort()
        query = urlencode(q)
        normalized = urlunparse((scheme, netloc, path, '', query, fragment))
        return normalized
    except Exception:
        return u

def _import_from_law_firms_js() -> list:
    """宽松解析 static/js/law_firms_urls.js，抽取 name/url 列表。"""
    results = []
    try:
        law_firms_js_path = os.path.join(app.root_path, 'static', 'js', 'law_firms_urls.js')
        if not os.path.exists(law_firms_js_path):
            return results
        with open(law_firms_js_path, 'r', encoding='utf-8') as f:
            content = f.read()
        import re
        # 允许字段顺序不同，中间有其它字段
        pattern = re.compile(r'name\s*:\s*"(.*?)"[\s\S]*?url\s*:\s*"(.*?)"')
        for name, url in pattern.findall(content):
            results.append({'name': name, 'url': url})
    except Exception as e:
        print(f"解析law_firms_urls.js失败: {e}")
    return results

def _load_urls_data():
    """从 urls_data.json 加载列表数据。支持第一次创建。"""
    try:
        if not os.path.exists(URLS_DATA_FILE):
            # 首次运行：如果存在 law_firms_urls.js，则用其初始化一份基础数据
            urls = []
            try:
                matches = _import_from_law_firms_js()
                now = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                next_id = 1
                for item in matches:
                    urls.append({
                        'id': next_id,
                        'url': item['url'],
                        'name': item['name'],
                        'description': '',
                        'parent_id': None,
                        'created_at': now,
                        'updated_at': now
                    })
                    next_id += 1
            except Exception as e:
                print(f"初始化urls_data失败: {e}")
            with open(URLS_DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(urls, f, ensure_ascii=False, indent=2)
            return urls

        with open(URLS_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 兼容缺失字段
            for item in data:
                if 'parent_id' not in item:
                    item['parent_id'] = None

            # 如果文件存在但为空列表，尝试从 law_firms_urls.js 导入并合并
            if not data:
                try:
                    matches = _import_from_law_firms_js()
                    now = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
                    for item in matches:
                        if not any(_normalize_url(u.get('url')) == _normalize_url(item['url']) for u in data):
                            data.append({
                                'id': _generate_next_id(data),
                                'url': item['url'],
                                'name': item['name'],
                                'description': '',
                                'parent_id': None,
                                'created_at': now,
                                'updated_at': now
                            })
                    _save_urls_data(data)
                except Exception as e:
                    print(f"空数据导入law_firms失败: {e}")
            return data
    except Exception as e:
        print(f"读取urls_data.json失败: {e}")
        return []

def _save_urls_data(urls):
    """保存列表到 urls_data.json。"""
    with _urls_file_lock:
        tmp_path = URLS_DATA_FILE + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(urls, f, ensure_ascii=False, indent=2)
        try:
            os.replace(tmp_path, URLS_DATA_FILE)
        except Exception:
            # 回退直接写（极端情况）
            with open(URLS_DATA_FILE, 'w', encoding='utf-8') as f2:
                json.dump(urls, f2, ensure_ascii=False, indent=2)

def _generate_next_id(urls):
    """生成下一个整数ID，如果现有ID是UUID字符串则返回1"""
    if not urls:
        return 1
    try:
        # 尝试将所有ID转换为整数
        int_ids = [int(u.get('id', 0)) for u in urls if u.get('id')]
        return max(int_ids) + 1 if int_ids else 1
    except (ValueError, TypeError):
        # 如果ID是UUID字符串或其他非整数格式，返回1
        return 1

def _delete_with_children(urls, target_ids):
    """递归删除指定id及其所有子节点，返回剩余列表。"""
    remaining = [u for u in urls if u['id'] not in target_ids]
    # 找出被删除节点的子节点
    removed_any = True
    to_remove = set(target_ids)
    while removed_any:
        removed_any = False
        children_ids = [u['id'] for u in remaining if u.get('parent_id') in to_remove]
        if children_ids:
            remaining = [u for u in remaining if u['id'] not in set(children_ids)]
            to_remove = set(children_ids)
            removed_any = True
    return remaining

# API路由：获取URL列表（使用SQLite数据库）
@app.route('/api/urls', methods=['GET'])
def get_urls():
    """获取URL列表 - 使用SQLite数据库"""
    try:
        from sqlite_database import sqlite_db
        
        # 查询参数
        query = (request.args.get('query') or '').strip()
        page = _coerce_int(request.args.get('page'), 1, 1)
        page_size = _coerce_int(request.args.get('page_size'), 20, 1, 10000)
        
        # 从数据库获取URL列表
        urls, total = sqlite_db.get_managed_urls(page, page_size)
        
        # 如果有搜索查询，在内存中过滤
        if query:
            query_lower = query.lower()
            urls = [u for u in urls if 
                   (query_lower in u.get('name', '').lower()) or 
                   (query_lower in u.get('url', '').lower())]
            total = len(urls)
        
        # 转换数据格式以兼容前端
        formatted_urls = []
        for url in urls:
            formatted_urls.append({
                'id': url.get('id'),
                'url': url.get('url'),
                'name': url.get('name'),
                'description': url.get('description', ''),
                'category_id': url.get('category_id'),  # 添加分类ID
                'parent_id': url.get('parent_url_id'),  # 使用parent_url_id
                'created_at': url.get('created_at'),
                'updated_at': url.get('updated_at'),
                'category': url.get('category'),
                'is_active': url.get('is_active', True),
                'auth_config': url.get('auth_config')  # 添加认证配置
            })
        
        # 返回结果
        if page_size > 0:
            return jsonify({
                'success': True, 
                'urls': formatted_urls, 
                'total': total, 
                'page': page, 
                'page_size': page_size
            })
        return jsonify({'success': True, 'urls': formatted_urls, 'total': total})
        
    except Exception as e:
        print(f"❌ 获取URL列表失败: {e}")
        return jsonify({'success': False, 'urls': [], 'error': str(e)})

# API路由：添加URL（使用SQLite数据库）
@app.route('/api/urls', methods=['POST'])
def add_url():
    """添加URL - 使用SQLite数据库"""
    try:
        from sqlite_database import sqlite_db
        from auto_login import AutoLogin
        import asyncio
        import json
        
        data = request.get_json(silent=True) or {}
        raw_url = data.get('url', '').strip()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        # 🔥 修复：使用category_id而不是category
        category_id = data.get('category_id')
        print(f"🔥 收到的category_id: {category_id} (类型: {type(category_id)})")
        
        if not raw_url or not name:
            return jsonify({'success': False, 'message': 'URL和名称不能为空'})
        
        # 检查URL是否已存在
        if sqlite_db.get_managed_url_by_url(raw_url):
            return jsonify({'success': False, 'message': 'URL已存在'})
        
        # 🔐 处理认证配置
        auth_config = data.get('auth_config')
        auth_config_id = data.get('auth_config_id')
        requires_auth = data.get('requires_auth', False)
        auth_result = None
        
        # 方式1：使用已有认证配置
        if auth_config_id:
            print(f"📌 使用已有认证配置 ID: {auth_config_id}")
            # 更新使用次数
            try:
                cursor = sqlite_db.connection.cursor()
                cursor.execute("""
                    UPDATE auth_configs 
                    SET use_count = use_count + 1,
                        last_used_at = datetime('now', 'localtime')
                    WHERE id = ?
                """, (auth_config_id,))
                sqlite_db.connection.commit()
                cursor.close()
            except Exception as e:
                print(f"⚠️ 更新认证配置使用次数失败: {e}")
        
        # 方式2：创建新的认证配置
        elif auth_config and requires_auth:
            print(f"🔐 检测到新认证配置，开始执行登录...")
            
            # 生成认证名称
            auth_name = name.replace('/', '_').replace(':', '_')[:50]
            
            # 执行登录
            auto_login = AutoLogin()
            
            async def do_login():
                auth_config_dict = json.loads(auth_config) if isinstance(auth_config, str) else auth_config
                return await auto_login.login_and_save(auth_config_dict, auth_name)
            
            auth_result = asyncio.run(do_login())
            
            if not auth_result['success']:
                return jsonify({
                    'success': False,
                    'message': f'认证登录失败: {auth_result["message"]}'
                })
            
            print(f"✅ 认证登录成功: {auth_name}")
            
            # 将认证配置保存到auth_configs表
            try:
                auth_config_dict = json.loads(auth_config) if isinstance(auth_config, str) else auth_config
                cursor = sqlite_db.connection.cursor()
                success_indicator = auth_config_dict.get('success_indicator', {})
                
                cursor.execute("""
                    INSERT INTO auth_configs 
                    (name, login_url, username, password, username_selector,
                     password_selector, submit_selector, wait_after_submit,
                     success_indicator_type, success_indicator_value,
                     storage_file, cookies_count, use_count, last_used_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now', 'localtime'))
                """, (
                    auth_name,
                    auth_config_dict.get('login_url', ''),
                    auth_config_dict.get('username', ''),
                    auth_config_dict.get('password', ''),
                    auth_config_dict.get('username_selector', ''),
                    auth_config_dict.get('password_selector', ''),
                    auth_config_dict.get('submit_selector', ''),
                    auth_config_dict.get('wait_after_submit', 5),
                    success_indicator.get('type') if isinstance(success_indicator, dict) else None,
                    success_indicator.get('value') if isinstance(success_indicator, dict) else None,
                    auth_result.get('storage_state', ''),
                    len(auth_result.get('cookies', []))
                ))
                
                auth_config_id = cursor.lastrowid
                sqlite_db.connection.commit()
                cursor.close()
                
                print(f"💾 认证配置已保存到数据库，ID: {auth_config_id}")
                
            except Exception as e:
                print(f"⚠️ 保存认证配置到数据库失败: {e}")
                # 继续执行，使用JSON格式保存
                auth_config_dict['auth_name'] = auth_name
                auth_config = json.dumps(auth_config_dict, ensure_ascii=False)
        
        # 准备数据库数据
        url_data = {
            'url': raw_url,
            'name': name,
            'description': description,
            'category_id': category_id,  # 🔥 修复：使用category_id
            'is_active': True,
            'auto_crawl': False,
            'crawl_frequency': 'manual',
            'requires_auth': requires_auth,  # 🔐 添加认证标志
            'auth_config_id': auth_config_id,  # 🔐 添加认证配置ID
            'auth_config': auth_config if (auth_config and not auth_config_id) else None  # 🔐 添加认证配置JSON（向后兼容）
        }
        print(f"🔥 准备插入的数据: {url_data}")
        
        # 插入数据库
        url_id = sqlite_db.insert_managed_url(url_data)
        if url_id:
            # 格式化返回数据以兼容前端
            new_item = {
                'id': url_id,
                'url': raw_url,
                'name': name,
                'description': description,
                'parent_id': None,
                'created_at': get_china_time().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': get_china_time().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            response_data = {
                'success': True,
                'message': 'URL添加成功',
                'item': new_item,
                'url_id': url_id  # 🔥 添加url_id字段，方便测试
            }
            
            # 如果有认证结果，添加认证状态信息
            if auth_result:
                response_data['auth_status'] = {
                    'success': True,
                    'message': '认证配置已保存并登录成功',
                    'cookies_count': len(auth_result.get('cookies', []))
                }
            
            return jsonify(response_data)
        else:
            return jsonify({'success': False, 'message': '添加URL失败'})
            
    except Exception as e:
        print(f"❌ 添加URL失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)})

# API路由：更新URL（使用SQLite数据库）
@app.route('/api/urls/<int:url_id>', methods=['PUT'])
def update_url(url_id):
    """更新URL - 使用SQLite数据库"""
    try:
        from sqlite_database import sqlite_db
        from auto_login import AutoLogin
        import asyncio
        
        data = request.get_json(silent=True) or {}
        
        # 🔍 调试日志
        print(f"\n{'='*70}")
        print(f"📥 [旧API] 收到更新URL请求: ID={url_id}")
        print(f"📦 接收到的数据键: {list(data.keys()) if data else 'None'}")
        if data and 'auth_config' in data:
            auth_config_preview = str(data['auth_config'])[:100] if data['auth_config'] else 'None'
            print(f"🔐 auth_config 存在: {auth_config_preview}...")
        else:
            print(f"⚠️ auth_config 不存在于请求数据中")
        print(f"{'='*70}\n")
        
        raw_url = data.get('url', '').strip()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        # 🔥 修复：使用category_id而不是category
        category_id = data.get('category_id')
        print(f"🔥 更新URL - 收到的category_id: {category_id} (类型: {type(category_id)})")
        
        auth_config = data.get('auth_config')
        
        if not raw_url or not name:
            return jsonify({'success': False, 'message': 'URL和名称不能为空'})
        
        # 检查URL是否存在
        existing_url = sqlite_db.get_managed_url_by_id(url_id)
        if not existing_url:
            return jsonify({'success': False, 'message': 'URL不存在'})
        
        # 检查是否有其他URL使用同样的地址（排除自己）
        other_url = sqlite_db.get_managed_url_by_url(raw_url)
        if other_url and other_url['id'] != url_id:
            return jsonify({'success': False, 'message': 'URL已被其他记录使用'})
        
        # 处理认证配置
        auth_result = None
        if auth_config:
            print(f"🔐 检测到认证配置，开始执行登录...")
            
            # 生成认证名称
            auth_name = name.replace('/', '_').replace(':', '_')[:50]
            
            # 执行登录
            auto_login = AutoLogin()
            
            async def do_login():
                auth_config_dict = json.loads(auth_config) if isinstance(auth_config, str) else auth_config
                return await auto_login.login_and_save(auth_config_dict, auth_name)
            
            auth_result = asyncio.run(do_login())
            
            if not auth_result['success']:
                return jsonify({
                    'success': False,
                    'message': f'认证登录失败: {auth_result["message"]}'
                })
            
            print(f"✅ 认证登录成功: {auth_name}")
            
            # 更新auth_config，添加auth_name
            if isinstance(auth_config, str):
                auth_config_dict = json.loads(auth_config)
            else:
                auth_config_dict = auth_config
            
            auth_config_dict['auth_name'] = auth_name
            auth_config = json.dumps(auth_config_dict, ensure_ascii=False)
        
        # 准备更新数据
        update_data = {
            'url': raw_url,
            'name': name,
            'description': description,
            'category_id': category_id,  # 🔥 修复：使用category_id
            'auth_config': auth_config  # 添加认证配置
        }
        print(f"🔥 准备更新的数据: {update_data}")
        
        # 更新数据库
        success = sqlite_db.update_managed_url(url_id, update_data)
        if success:
            response_data = {
                'success': True,
                'message': 'URL更新成功'
            }
            
            if auth_result:
                response_data['auth_status'] = {
                    'success': True,
                    'message': '认证配置已保存并登录成功',
                    'cookies_count': len(auth_result.get('cookies', []))
                }
            
            return jsonify(response_data)
        else:
            return jsonify({'success': False, 'message': '更新URL失败'})
            
    except Exception as e:
        print(f"❌ 更新URL失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)})

# API路由：删除URL（使用SQLite数据库）
@app.route('/api/urls/<int:url_id>', methods=['DELETE'])
def delete_url(url_id):
    """删除URL - 使用SQLite数据库"""
    try:
        from sqlite_database import sqlite_db
        
        # 检查URL是否存在
        existing_url = sqlite_db.get_managed_url_by_id(url_id)
        if not existing_url:
            return jsonify({'success': False, 'message': 'URL不存在'})
        
        # 删除URL
        success = sqlite_db.delete_managed_url(url_id)
        if success:
            return jsonify({'success': True, 'message': 'URL删除成功'})
        else:
            return jsonify({'success': False, 'message': '删除URL失败'})
            
    except Exception as e:
        print(f"❌ 删除URL失败: {e}")
        return jsonify({'success': False, 'message': str(e)})

# API路由：批量删除URL（级联）
@app.route('/api/urls/batch-delete', methods=['POST'])
def batch_delete_urls():
    try:
        data = request.get_json(silent=True) or {}
        url_ids = data.get('url_ids', [])
        
        if not url_ids:
            return jsonify({'success': False, 'message': '请选择要删除的URL'})
        # 转为整型
        ids_to_delete = []
        for i in url_ids:
            try:
                ids_to_delete.append(int(i))
            except Exception:
                pass
        urls = _load_urls_data()
        remaining = _delete_with_children(urls, ids_to_delete)
        _save_urls_data(remaining)
        return jsonify({'success': True, 'message': f'成功删除{len(ids_to_delete)}个URL（含子链接）'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# 批量移动父节点
@app.route('/api/urls/batch-move', methods=['POST'])
def batch_move_urls():
    try:
        payload = request.get_json(silent=True) or {}
        ids = payload.get('url_ids', [])
        new_parent_id = payload.get('parent_id', None)
        urls = _load_urls_data()
        # 解析 parent_id
        if new_parent_id is not None:
            try:
                new_parent_id = int(new_parent_id)
            except Exception:
                new_parent_id = None
            if new_parent_id is not None and not any(u['id'] == new_parent_id for u in urls):
                return jsonify({'success': False, 'message': '父链接不存在'})
        # 执行移动（排除自为父情况）
        moved = 0
        move_set = set()
        for i in ids:
            try:
                i = int(i)
            except Exception:
                continue
            for u in urls:
                if u['id'] == i:
                    if new_parent_id == i:
                        break
                    u['parent_id'] = new_parent_id
                    moved += 1
                    move_set.add(i)
                    break
        _save_urls_data(urls)
        return jsonify({'success': True, 'message': f'已移动 {moved} 个URL', 'moved': list(move_set)})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# 导出URL（JSON 或 CSV）
@app.route('/api/urls/export')
def export_urls():
    try:
        fmt = (request.args.get('format') or 'json').lower()
        urls = _load_urls_data()
        if fmt == 'csv':
            import io, csv
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['id','url','name','description','parent_id','created_at','updated_at'])
            for u in urls:
                writer.writerow([
                    u.get('id'), u.get('url'), u.get('name'), u.get('description',''),
                    u.get('parent_id'), u.get('created_at'), u.get('updated_at')
                ])
            output.seek(0)
            return Response(output.getvalue(), mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename="urls_export.csv"'})
        else:
            return jsonify({'success': True, 'urls': urls})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：导入URL（JSON/CSV 简化版：仅支持 JSON 列表 [{url,name,description,parent_id}]）
@app.route('/api/urls/import', methods=['POST'])
def import_urls():
    try:
        payload = request.get_json(silent=True) or {}
        items = payload.get('items', [])
        if not isinstance(items, list) or not items:
            return jsonify({'success': False, 'message': 'items 必须为非空列表'})
        urls = _load_urls_data()
        existing = set(_normalize_url(u.get('url')) for u in urls)
        added = 0
        now = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
        for raw in items:
            try:
                nurl = _normalize_url((raw.get('url') or '').strip())
                name = (raw.get('name') or '').strip() or nurl
                if not nurl:
                    continue
                if nurl in existing:
                    continue
                parent_id = raw.get('parent_id')
                try:
                    parent_id = int(parent_id) if parent_id is not None else None
                except Exception:
                    parent_id = None
                urls.append({
                    'id': _generate_next_id(urls),
                    'url': nurl,
                    'name': name,
                    'description': (raw.get('description') or '').strip(),
                    'parent_id': parent_id,
                    'created_at': now,
                    'updated_at': now
                })
                existing.add(nurl)
                added += 1
            except Exception:
                pass
        _save_urls_data(urls)
        return jsonify({'success': True, 'message': f'成功导入 {added} 条URL'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：更新law_firms_urls.js
@app.route('/api/update-law-firms-urls', methods=['POST'])
def update_law_firms_urls():
    try:
        data = request.get_json(silent=True) or {}
        urls = data.get('urls', [])
        
        # 这里可以更新law_firms_urls.js文件，现在只是返回成功
        return jsonify({'success': True, 'message': 'URL列表更新成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：获取菜单结构（简化版，不显示菜单）
@app.route('/api/get-menu-structure', methods=['POST'])
def get_menu_structure():
    try:
        data = request.get_json(silent=True) or {}
        url = data.get('url')
        
        if not url:
            return jsonify({'success': False, 'message': '请提供URL'})
        
        # 直接返回成功，不显示菜单结构
        return jsonify({'success': True, 'message': 'URL已选择'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# 任务管理相关API
# 兼容性路由：将旧的/api/schedules请求转发到新的API
@app.route('/api/schedules', methods=['GET'])
def get_schedules_compat():
    """兼容旧前端的获取定时任务列表"""
    try:
        tasks, total = sqlite_db.get_scheduled_tasks(1, 1000)
        # 转换为旧格式
        schedules = []
        for task in tasks:
            config = task.get('config', {})
            if isinstance(config, str):
                try:
                    import json
                    config = json.loads(config)
                except:
                    config = {}
            
            # 解析时间 - 改进错误处理
            schedule_time = task.get('schedule_time', '00:00:00')
            if schedule_time:
                time_parts = str(schedule_time).split(':')
            else:
                time_parts = ['00', '00', '00']
            
            # 确保时间部分格式正确
            hour = time_parts[0].zfill(2) if len(time_parts) > 0 else '00'
            minute = time_parts[1].zfill(2) if len(time_parts) > 1 else '00'  
            second = time_parts[2].zfill(2) if len(time_parts) > 2 else '00'
            
            # 处理is_active字段 - 确保布尔值转换正确
            is_active = task.get('is_active')
            if isinstance(is_active, (int, str)):
                enabled = bool(int(is_active)) if str(is_active).isdigit() else bool(is_active)
            else:
                enabled = bool(is_active) if is_active is not None else True
            
            schedules.append({
                'id': str(task.get('id', '')),
                'name': task.get('task_name', '') or f"任务{task.get('id', '')}",
                'url': task.get('target_url', ''),
                'execution_time_hour': hour,
                'execution_time_minute': minute,
                'execution_time_second': second,
                'depth': config.get('depth', 2),
                'mode': config.get('mode', 'standard'),
                'limit': config.get('limit', 50),
                'crawl_options': normalize_crawl_options(config),
                'repeat': task.get('schedule_type', 'once'),
                'enabled': enabled,
                'keywords': task.get('keywords', ''),
                'ragflow_kb_id': task.get('ragflow_kb_id'),
                'days_limit': task.get('days_limit', 7),  # 🔥 添加日期限制字段
                'schedule_weekdays': task.get('schedule_weekdays', ''),  # 🔥 每周执行日
                'schedule_monthdays': task.get('schedule_monthdays', ''),  # 🔥 每月执行日
                'created_at': task.get('created_at', ''),
                'last_run': task.get('last_run', ''),
                'next_run': task.get('next_run', ''),
                'url_display_name': task.get('url_display_name', '')
            })
        
        return jsonify({
            'success': True,
            'schedules': schedules
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取定时任务列表失败: {str(e)}'
        }), 500

@app.route('/api/schedules', methods=['POST'])
def create_schedule_compat():
    """兼容旧前端的创建定时任务"""
    try:
        data = request.get_json(silent=True) or {}
        
        # 转换旧格式到新格式
        hour = _coerce_int(data.get('execution_time_hour'), 0, 0, 23)
        minute = _coerce_int(data.get('execution_time_minute'), 0, 0, 59)
        second = _coerce_int(data.get('execution_time_second'), 0, 0, 59)

        # 确保格式为两位数字字符串，坏输入回落到 00:00:00
        hour_str = f"{hour:02d}"
        minute_str = f"{minute:02d}"
        second_str = f"{second:02d}"
        schedule_time = f"{hour_str}:{minute_str}:{second_str}"
        
        # 构建任务配置
        crawl_options = normalize_crawl_options(data)
        config = {
            'depth': data.get('depth', 2),
            'mode': data.get('mode', 'standard'),
            'limit': data.get('limit', 50),
            **crawl_options
        }
        
        # 准备任务数据
        repeat_type = data.get('repeat', 'once')
        
        # 🔥 根据repeat类型只保存对应的日期字段
        now = get_china_time()
        schedule_weekdays = (
            _normalize_schedule_list(data.get('schedule_weekdays'), 0, 6)
            if repeat_type == 'weekly'
            else ''
        )
        schedule_monthdays = (
            _normalize_schedule_list(data.get('schedule_monthdays'), 1, 31)
            if repeat_type == 'monthly'
            else ''
        )
        if repeat_type == 'weekly' and not schedule_weekdays:
            schedule_weekdays = str(now.weekday())
        if repeat_type == 'monthly' and not schedule_monthdays:
            schedule_monthdays = str(now.day)
        
        target_url = normalize_task_url(data.get('url'))

        task_data = {
            'task_name': data.get('name', f"定时任务-{data.get('url', 'unknown')}"),
            'task_type': 'crawl',
            'target_url': target_url,
            'schedule_type': repeat_type,
            'schedule_time': schedule_time,
            'keywords': data.get('keywords', ''),
            'is_active': data.get('enabled', True),
            'ragflow_kb_id': data.get('ragflow_kb_id'),
            'days_limit': _coerce_days_limit(data.get('days_limit'), 7),
            'schedule_weekdays': schedule_weekdays,
            'schedule_monthdays': schedule_monthdays,
            'config': config
        }

        try:
            next_run = calculate_next_run_time(task_data)
            task_data['next_run'] = next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else None
        except Exception as calc_error:
            print(f"⚠️ 兼容定时任务 next_run 计算失败: {calc_error}")
        
        # 插入到数据库
        task_id = sqlite_db.insert_scheduled_task(task_data)
        
        if task_id:
            return jsonify({
                'success': True,
                'schedule': {
                    'id': str(task_id),
                    'name': task_data['task_name'],
                    'url': task_data['target_url']
                },
                'message': '定时任务添加成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '任务添加失败'
            }), 400
            
    except Exception as e:
        print(f"❌ 添加定时任务失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': f'添加任务失败: {str(e)}'
        }), 500

@app.route('/api/schedules/<schedule_id>', methods=['PATCH'])
def update_schedule_compat(schedule_id):
    """兼容旧前端的更新定时任务"""
    try:
        data = request.get_json(silent=True) or {}
        
        # 转换旧格式到新格式
        update_data = {}
        
        if 'name' in data:
            update_data['task_name'] = data['name']
        
        if 'enabled' in data:
            update_data['is_active'] = data['enabled']
        
        if 'repeat' in data:
            update_data['schedule_type'] = data['repeat']
        
        if 'url' in data:
            update_data['target_url'] = normalize_task_url(data['url'])
        
        if 'execution_time_hour' in data or 'execution_time_minute' in data:
            hour = _coerce_int(data.get('execution_time_hour'), 0, 0, 23)
            minute = _coerce_int(data.get('execution_time_minute'), 0, 0, 59)
            second = _coerce_int(data.get('execution_time_second'), 0, 0, 59)

            hour_str = f"{hour:02d}"
            minute_str = f"{minute:02d}"
            second_str = f"{second:02d}"
            update_data['schedule_time'] = f"{hour_str}:{minute_str}:{second_str}"
        
        crawl_option_keys = [
            'crawl_options', 'wait_for_ms', 'wait_for', 'render_wait_ms', 'max_pages', 'max_empty_pages',
            'detail_max_retries', 'max_extract_attempts', 'date_range_priority',
            'candidate_date_prefilter', 'network_json_enabled', 'supplemental_enabled',
            'supplemental_html', 'supplemental_attributes', 'supplemental_structured',
            'supplemental_scripts', 'supplemental_static_pagination', 'supplemental_feeds',
            'supplemental_sitemaps', 'supplemental_max_per_source',
            'supplemental_max_sitemaps', 'supplemental_max_static_pages',
            'proxy_enabled', 'use_proxy'
        ]
        if any(k in data for k in ['depth', 'mode', 'limit'] + crawl_option_keys):
            config = {}
            if 'depth' in data:
                config['depth'] = data['depth']
            if 'mode' in data:
                config['mode'] = data['mode']
            if 'limit' in data:
                config['limit'] = data['limit']
            config.update(normalize_crawl_options(data))
            update_data['config'] = config
        
        # 🔥 days_limit 直接保存到表字段，不放在config里
        if 'days_limit' in data:
            update_data['days_limit'] = _coerce_days_limit(data.get('days_limit'), 7)
        
        if 'keywords' in data:
            update_data['keywords'] = data['keywords']

        if 'ragflow_kb_id' in data:
            update_data['ragflow_kb_id'] = data['ragflow_kb_id']
        
        # 🔥 每周/每月执行日 - 根据repeat类型只保存对应字段
        repeat_type = data.get('repeat') or update_data.get('schedule_type')
        if 'schedule_weekdays' in data:
            # 如果不是每周类型，清空weekdays
            if repeat_type and repeat_type != 'weekly':
                update_data['schedule_weekdays'] = ''
            else:
                update_data['schedule_weekdays'] = _normalize_schedule_list(data.get('schedule_weekdays'), 0, 6)
        
        if 'schedule_monthdays' in data:
            # 如果不是每月类型，清空monthdays
            if repeat_type and repeat_type != 'monthly':
                update_data['schedule_monthdays'] = ''
            else:
                update_data['schedule_monthdays'] = _normalize_schedule_list(data.get('schedule_monthdays'), 1, 31)
        
        # 🔥 如果切换了repeat类型，确保清空不相关的字段
        if 'repeat' in data:
            if data['repeat'] != 'weekly':
                update_data['schedule_weekdays'] = ''
            if data['repeat'] != 'monthly':
                update_data['schedule_monthdays'] = ''
            if data['repeat'] == 'weekly' and not update_data.get('schedule_weekdays'):
                update_data['schedule_weekdays'] = str(get_china_time().weekday())
            if data['repeat'] == 'monthly' and not update_data.get('schedule_monthdays'):
                update_data['schedule_monthdays'] = str(get_china_time().day)

        success = sqlite_db.update_scheduled_task(int(schedule_id), update_data)
        
        if success:
            return jsonify({
                'success': True,
                'message': '定时任务更新成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '任务更新失败'
            }), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'更新任务失败: {str(e)}'
        }), 500

@app.route('/api/schedules/<schedule_id>', methods=['DELETE'])
def delete_schedule_compat(schedule_id):
    """兼容旧前端的删除定时任务"""
    try:
        success = sqlite_db.delete_scheduled_task(int(schedule_id))
        
        if success:
            return jsonify({
                'success': True,
                'message': '定时任务删除成功'
            })
        else:
            return jsonify({
                'success': False,
                'message': '任务删除失败'
            }), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'删除任务失败: {str(e)}'
        }), 500

# 旧的JSON文件执行记录API路由已移除，现在使用 schedule_execution_api.py 中的数据库版本
# @app.route('/api/schedule_executions', methods=['GET'])
# def get_schedule_executions():

# @app.route('/api/schedule_executions/<execution_id>', methods=['DELETE'])
# def delete_schedule_execution(execution_id):

# @app.route('/api/schedule_executions', methods=['DELETE'])
# def clear_schedule_executions():

# 简易执行器：每分钟检查一次
def _tick_schedules_once():
    """Compatibility no-op; TaskScheduler handles scheduled crawls."""
    print("Legacy schedule tick disabled; TaskScheduler handles scheduled crawls.")
    return


def _acquire_scheduler_process_lock():
    """Allow only one app process in this workspace to run the scheduler."""
    global _scheduler_process_lock_handle
    if _scheduler_process_lock_handle:
        return True

    try:
        lock_dir = CRAWL_RESULTS_DIR or 'crawl_results'
        os.makedirs(lock_dir, exist_ok=True)
        lock_path = os.path.join(lock_dir, 'scheduler.process.lock')
        handle = open(lock_path, 'a+', encoding='utf-8')
        try:
            if os.name == 'nt':
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            print("⚠️ 已检测到另一个进程持有调度器锁，本进程不启动定时调度器")
            return False

        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\nstarted_at={get_china_time().isoformat()}\n")
        handle.flush()
        _scheduler_process_lock_handle = handle
        return True
    except Exception as e:
        print(f"⚠️ 调度器进程锁创建失败，继续启动调度器: {e}")
        return True


def _schedule_runner():
    """定时任务调度器运行器（复用全局TaskScheduler实例）"""
    if not _acquire_scheduler_process_lock():
        return

    print("🚀 启动定时任务调度器...")
    scheduler.start()
    
    # 保持调度器运行
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n⏹️  停止定时任务调度器...")
        scheduler.stop()

def _ensure_schedule_thread():
    global schedule_thread
    with schedule_thread_lock:
        if schedule_thread and schedule_thread.is_alive():
            print("ℹ️ 定时任务调度器线程已在运行，跳过重复启动")
            return schedule_thread

        schedule_thread = Thread(target=_schedule_runner, name='TaskSchedulerThread')
        schedule_thread.daemon = True
        schedule_thread.start()
        return schedule_thread

# API路由：开始爬取
@app.route('/api/start-crawl', methods=['POST'])
def start_crawl():
    try:
        data = request.get_json(silent=True) or {}
        
        # 支持单URL和多个URL
        url = data.get('url')
        urls = data.get('urls', [])
        
        # 如果没有提供urls数组但有单url，转换为数组
        if url and not urls:
            urls = [url]
        
        # 如果没有URL，返回错误
        if not urls:
            return jsonify({'success': False, 'message': '请提供要爬取的URL'})
        
        depth = data.get('depth', 2)
        mode = data.get('mode', 'standard')
        if mode != 'article_crawl':
            return jsonify({
                'success': False,
                'message': '旧版爬取模式已停用，请使用栏目文章爬取'
            }), 400
        limit = data.get('limit', 50)
        include_links = data.get('include_links', [])
        exclude_links = data.get('exclude_links', [])
        incremental = data.get('incremental', False)  # 增量爬取选项
        schedule_execution_id = data.get('schedule_execution_id')  # 定时任务执行记录ID
        keywords = data.get('keywords', '')  # 关键词过滤
        kb_id = data.get('kb_id', '')  # 目标知识库ID
        days_limit = _coerce_days_limit(data.get('days_limit'), 7)
        start_date = data.get('start_date') or data.get('date_start') or data.get('from_date')
        end_date = data.get('end_date') or data.get('date_end') or data.get('to_date')
        crawl_options = normalize_crawl_options(data)
        
        # 为每个URL创建单独的任务
        task_ids = []
        for url_item in urls:
            task_id = str(uuid.uuid4())
            task_ids.append(task_id)
            
            # 保存任务信息到内存
            with task_lock:
                crawl_tasks[task_id] = {
                    'task_id': task_id,
                    'urls': [url_item],  # 单个URL
                    'url': url_item,  # 保持兼容性
                    'depth': depth,
                    'mode': mode,
                    'limit': limit,
                    'include_links': include_links,
                    'exclude_links': exclude_links,
                    'incremental': incremental,  # 增量爬取选项
                    'keywords': keywords,  # 关键词过滤
                    'kb_id': kb_id,  # 目标知识库ID
                    'days_limit': days_limit,  # 日期限制
                    'start_date': start_date,
                    'end_date': end_date,
                    'crawl_options': crawl_options,
                    'cancelled': False,
                    'status': 'pending',
                    'progress': 0,
                    'created_at': get_china_time().isoformat(),
                    'completed_at': None,
                    'data': None,
                    'error': None,
                    'firecrawl_task_id': None,  # 用于存储Firecrawl返回的任务ID
                    'results': [],  # 存储多个URL的爬取结果
                    'logs': [],
                    'schedule_execution_id': schedule_execution_id  # 定时任务执行记录ID
                }
                
            # 同时保存到数据库
            try:
                from sqlite_database import sqlite_db
                task_data = {
                    'task_id': task_id,
                    'target_url': url_item,
                    'task_name': f'爬取任务-{url_item}',
                    'crawl_depth': depth,
                    'crawl_mode': mode,
                    'page_limit': limit,
                    'incremental_mode': incremental,
                    'keywords': keywords,
                    'status': 'pending'
                }
                
                db_id = sqlite_db.insert_crawl_task(task_data)
                if db_id:
                    print(f"✅ 任务已创建并入库: {task_id} (DB ID: {db_id})")
                else:
                    print(f"⚠️ 任务创建成功但入库失败: {task_id}")
                    
            except Exception as e:
                print(f"❌ 任务入库失败: {e}")
                
            print(f"任务已创建: {task_id}")
            print(f"增量爬取选项: {incremental}")
            print(f"当前任务列表: {list(crawl_tasks.keys())}")
            _append_task_log(task_id, '任务创建完成，准备开始')
            
            # 启动新的文章爬取任务，避免误触旧 Firecrawl 链路
            thread = Thread(target=run_article_crawl_task, args=(task_id,))
            thread.daemon = True
            thread.start()
        
        return jsonify({'success': True, 'task_ids': task_ids, 'firecrawl_task_id': None})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# 任务日志分页
@app.route('/api/task-logs')
def get_task_logs():
    try:
        task_id = (request.args.get('task_id') or '').strip()
        if not task_id:
            return jsonify({'success': False, 'message': 'task_id 必填'})
        if request.args.get('page') is not None or request.args.get('page_size') is not None:
            page = _coerce_int(request.args.get('page'), 1, 1)
            page_size = _coerce_int(request.args.get('page_size'), 50, 1, 500)
            offset = (page - 1) * page_size
            limit = page_size
        else:
            offset = _coerce_int(request.args.get('offset'), 0, 0)
            limit = _coerce_int(request.args.get('limit'), 100, 1, 500)
        with task_lock:
            task = crawl_tasks.get(task_id)
            logs = task.get('logs', []) if task else []
        total = len(logs)
        slice_logs = logs[offset: offset + limit] if limit > 0 else logs[offset:]
        return jsonify({'success': True, 'logs': slice_logs, 'total': total, 'offset': offset, 'limit': limit})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# 后台爬取任务
def run_crawl_task(task_id):
    """Compatibility wrapper for old crawl tasks."""
    _append_task_log(task_id, 'Legacy Firecrawl executor disabled; switching to Playwright article crawler')
    return run_article_crawl_task(task_id)


@app.route('/api/app/task/<task_id>')
def get_task_detail(task_id):
    try:
        # 尝试从内存中获取
        with task_lock:
            if task_id in crawl_tasks:
                task = crawl_tasks[task_id].copy()
                return jsonify({'success': True, 'result': task})
        
        # 尝试从文件中获取
        detail_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_detail.json")
        if os.path.exists(detail_file):
            with open(detail_file, 'r', encoding='utf-8') as f:
                detail_data = json.load(f)
                return jsonify({'success': True, 'result': detail_data})
        
        return jsonify({'success': False, 'message': '任务不存在'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：获取任务状态
@app.route('/api/crawl-status')
def crawl_status():
    """获取任务状态 - 优先内存，回退到数据库"""
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        task = None
        
        # 1. 首先从内存中查询（运行中的任务）
        with task_lock:
            if task_id in crawl_tasks:
                task = crawl_tasks[task_id].copy()
                print(f"✅ 从内存找到任务: {task_id}")
        
        # 2. 如果内存中没有，从数据库查询（历史任务）
        if not task:
            try:
                from sqlite_database import sqlite_db
                db_task = sqlite_db.get_crawl_task_by_task_id(task_id)
                
                if db_task:
                    # 转换数据库格式为内存格式
                    task = {
                        'task_id': db_task.get('task_id'),
                        'url': db_task.get('target_url'),
                        'status': db_task.get('status'),
                        'progress': db_task.get('progress', 0),
                        'created_at': db_task.get('created_at'),
                        'started_at': db_task.get('started_at'),
                        'completed_at': db_task.get('completed_at'),
                        'error': db_task.get('error_message'),
                        'articles_found': db_task.get('articles_found', 0),
                        'articles_processed': db_task.get('articles_processed', 0),
                        'firecrawl_task_id': db_task.get('firecrawl_task_id', '')
                    }
                    print(f"✅ 从数据库找到任务: {task_id} (状态: {task['status']})")
                    
            except Exception as e:
                print(f"❌ 从数据库查询任务失败: {e}")
        
        # 3. 如果数据库中也没有，尝试从JSON备份文件中查找
        if not task:
            try:
                # 先检查当前crawl_results目录
                json_backup_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.json")
                # 如果不存在，检查json_backup/crawl_results目录
                if not os.path.exists(json_backup_file):
                    json_backup_file = os.path.join("json_backup", "crawl_results", f"{task_id}.json")
                
                if os.path.exists(json_backup_file):
                    with open(json_backup_file, 'r', encoding='utf-8') as f:
                        backup_task = json.load(f)
                        # 转换JSON格式为内存格式
                        task = {
                            'task_id': backup_task.get('task_id'),
                            'url': backup_task.get('url'),
                            'status': backup_task.get('status'),
                            'progress': 100 if backup_task.get('status') == 'completed' else 0,
                            'created_at': backup_task.get('created_at'),
                            'started_at': backup_task.get('started_at'),
                            'completed_at': backup_task.get('completed_at'),
                            'error': backup_task.get('error'),
                            'data': backup_task.get('data'),
                            'articles_found': 0,  # JSON备份中可能没有这些字段
                            'articles_processed': 0,
                            'firecrawl_task_id': backup_task.get('data', {}).get('id', '') if backup_task.get('data') else ''
                        }
                        print(f"✅ 从JSON备份找到任务: {task_id} (状态: {task['status']}) (文件: {json_backup_file})")
            except Exception as e:
                print(f"❌ 从JSON备份查询任务失败: {e}")
        
        # 4. 如果都没有找到，返回任务不存在
        if not task:
            print(f"❌ 任务不存在: {task_id}")
            print(f"当前内存任务列表: {list(crawl_tasks.keys())}")
            return jsonify({'success': False, 'message': '任务不存在'})
        
        # Return local results only. Legacy external Firecrawl status polling is disabled.
        status_result = None
        if task.get('status') == 'completed' and task.get('data'):
            status_result = task['data']
        elif task.get('firecrawl_task_id'):
            status_result = {
                'disabled': True,
                'message': 'Legacy external Firecrawl status polling is disabled; local task status returned.'
            }
        
        return jsonify({
            'success': True,
            'status': task['status'],
            'progress': task['progress'],
            'firecrawl_task_id': task.get('firecrawl_task_id'),
            'message': task.get('error'),
            'status_result': status_result
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：获取爬取结果列表
def _extract_schedule_id_from_task_id(value):
    match = re.match(r'^schedule_(\d+)_', value or '')
    return match.group(1) if match else ''


def _format_console_schedule_time(schedule):
    if not schedule:
        return '-'
    schedule_time = str(schedule.get('schedule_time') or '').strip()
    if not schedule_time:
        return '-'
    parts = schedule_time.split(':')
    if len(parts) >= 2:
        return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}"
    return schedule_time


def _split_schedule_numbers(value):
    if value in (None, ''):
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = re.split(r'[,，\s]+', str(value).strip())
    numbers = []
    for item in raw_items:
        try:
            numbers.append(int(item))
        except (TypeError, ValueError):
            continue
    return numbers


def _format_console_repeat_setting(schedule):
    if not schedule:
        return '-'

    schedule_type = str(schedule.get('schedule_type') or schedule.get('repeat') or 'once').strip().lower()
    if schedule_type == 'once':
        return '仅执行一次'
    if schedule_type == 'hourly':
        return '每小时'
    if schedule_type == 'daily':
        return '每天'
    if schedule_type == 'weekly':
        weekday_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        weekdays = _split_schedule_numbers(schedule.get('schedule_weekdays'))
        labels = [weekday_names[item] for item in weekdays if 0 <= item <= 6]
        return f"每周（{', '.join(labels)}）" if labels else '每周'
    if schedule_type == 'monthly':
        monthdays = [str(item) for item in _split_schedule_numbers(schedule.get('schedule_monthdays')) if 1 <= item <= 31]
        return f"每月（{', '.join(monthdays)}日）" if monthdays else '每月'
    return schedule_type or '-'


@app.route('/api/crawl-results')
def get_crawl_results():
    """获取爬取结果列表 - 从数据库读取"""
    try:
        from sqlite_database import sqlite_db
        
        results = []
        schedule_cache = {}

        def get_schedule_for_result(task_id, explicit_schedule_id=None):
            schedule_id = explicit_schedule_id or _extract_schedule_id_from_task_id(task_id)
            if not schedule_id:
                return '', None
            schedule_id = str(schedule_id)
            if schedule_id not in schedule_cache:
                try:
                    schedule_cache[schedule_id] = sqlite_db.get_scheduled_task(int(schedule_id))
                except Exception:
                    schedule_cache[schedule_id] = None
            return schedule_id, schedule_cache.get(schedule_id)

        def enrich_result_with_schedule(result):
            schedule_id, schedule = get_schedule_for_result(result.get('task_id'), result.get('schedule_id'))
            result['schedule_id'] = schedule_id
            if schedule:
                result['schedule_name'] = schedule.get('task_name') or result.get('schedule_name') or ''
                result['schedule_time'] = schedule.get('schedule_time') or ''
                result['schedule_type'] = schedule.get('schedule_type') or ''
                result['schedule_weekdays'] = schedule.get('schedule_weekdays') or ''
                result['schedule_monthdays'] = schedule.get('schedule_monthdays') or ''
                result['schedule_execution_time'] = _format_console_schedule_time(schedule)
                result['schedule_repeat'] = _format_console_repeat_setting(schedule)
                result['url_display_name'] = schedule.get('url_display_name') or result.get('url_display_name') or ''
            else:
                result.setdefault('schedule_execution_time', '-')
                result.setdefault('schedule_repeat', '-')
                result.setdefault('url_display_name', '')
            return result
        
        # 首先从数据库获取任务
        try:
            tasks, total = sqlite_db.get_crawl_tasks(1, 100)  # 获取最近100个任务
            
            for task in tasks:
                results.append(enrich_result_with_schedule({
                    'task_id': task.get('task_id'),
                    'schedule_id': _extract_schedule_id_from_task_id(task.get('task_id')),
                    'firecrawl_task_id': task.get('firecrawl_task_id', ''),
                    'url': task.get('target_url', ''),
                    'task_name': task.get('task_name', ''),
                    'keywords': task.get('keywords', ''),
                    'status': task.get('status', 'unknown'),
                    'progress': task.get('progress', 0),
                    'articles_found': task.get('articles_found', 0),
                    'articles_processed': task.get('articles_processed', 0),
                    'created_at': task.get('created_at', ''),
                    'started_at': task.get('started_at', ''),
                    'completed_at': task.get('completed_at', ''),
                    'error_message': task.get('error_message', ''),
                    'url_display_name': task.get('url_display_name', '')
                }))
                
            print(f"✅ 从数据库加载了 {len(results)} 个任务")
            
        except Exception as db_e:
            print(f"❌ 从数据库加载任务失败: {db_e}")
        
        # 补充从内存中获取任务（用于新创建但还没有持久化的任务）
        with task_lock:
            for task_id, task in crawl_tasks.items():
                # 检查是否已在数据库结果中
                if not any(r.get('task_id') == task_id for r in results):
                    results.append(enrich_result_with_schedule({
                        'task_id': task_id,
                        'schedule_id': task.get('schedule_id') or _extract_schedule_id_from_task_id(task_id),
                        'firecrawl_task_id': task.get('firecrawl_task_id', ''),
                        'url': task.get('url', ''),
                        'task_name': task.get('task_name', ''),
                        'keywords': task.get('keywords', ''),
                        'status': task.get('status', 'unknown'),
                        'progress': task.get('progress', 0),
                        'articles_found': task.get('articles_found', 0),
                        'articles_processed': task.get('articles_processed', 0),
                        'created_at': task.get('created_at', ''),
                        'started_at': task.get('started_at', ''),
                        'completed_at': task.get('completed_at', ''),
                        'error_message': task.get('error', '')
                    }))
        
        # 按创建时间倒序排序
        results.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return jsonify({'success': True, 'results': results, 'total': len(results)})
        
    except Exception as e:
        print(f"❌ 获取爬取结果失败: {e}")
        return jsonify({'success': False, 'message': str(e)})

# API路由：获取任务结果
@app.route('/api/get-result')
def get_result():
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        # 尝试从内存中获取
        with task_lock:
            if task_id in crawl_tasks and crawl_tasks[task_id]['data']:
                return jsonify(crawl_tasks[task_id]['data'])
        
        # 尝试从文件中获取
        result_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.json")
        if os.path.exists(result_file):
            with open(result_file, 'r', encoding='utf-8') as f:
                return jsonify(json.load(f))
        
        return jsonify({'success': False, 'message': '结果不存在'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：下载结果（JSON格式）
@app.route('/api/download-result')
def download_result():
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        result_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.json")
        if not os.path.exists(result_file):
            return jsonify({'success': False, 'message': '结果不存在'})
        
        return send_file(result_file, as_attachment=True, download_name=f"{task_id}.json")
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：下载结果（Markdown格式）
@app.route('/api/download-markdown')
def download_markdown():
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        markdown_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.md")
        if not os.path.exists(markdown_file):
            return jsonify({'success': False, 'message': '结果不存在'})
        
        return send_file(markdown_file, as_attachment=True, download_name=f"{task_id}.md")
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：下载单篇文章TXT文件
@app.route('/api/download-article-txt')
def download_article_txt():
    try:
        task_id = request.args.get('task_id')
        article_index = _coerce_int(request.args.get('article_index'), 0, 0)
        
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        # 尝试从JSON文件生成TXT
        result_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.json")
        if not os.path.exists(result_file):
            return jsonify({'success': False, 'message': '结果不存在'})
        
        with open(result_file, 'r', encoding='utf-8') as f:
            result = json.load(f)
        
        # 处理不同的数据结构格式
        if 'data' in result and isinstance(result['data'], list):
            data_list = result['data']
        elif isinstance(result, dict) and 'result' in result:
            crawl_result = result['result']
            if isinstance(crawl_result, list):
                data_list = crawl_result
            else:
                data_list = []
        else:
            data_list = []
        
        if article_index >= len(data_list):
            return jsonify({'success': False, 'message': '文章索引超出范围'})
        
        article = data_list[article_index]
        if not isinstance(article, dict):
            return jsonify({'success': False, 'message': '无效的文章数据'})
        
        # 提取文章信息
        article_url = article.get('url', '')
        article_title = article.get('title', '无标题')
        content = article.get('markdown', '') or article.get('content', '')
        
        # 尝试提取发布日期
        publish_date = None
        if 'metadata' in article and isinstance(article['metadata'], dict):
            publish_date = article['metadata'].get('publishDate') or article['metadata'].get('date')
        
        # 生成文件名
        filename = _generate_article_filename(article_url, article_title, publish_date, article_index)
        
        # 生成TXT内容
        txt_content = f"标题: {article_title}\n"
        txt_content += f"URL: {article_url}\n"
        if publish_date:
            txt_content += f"发布日期: {publish_date}\n"
        txt_content += f"爬取时间: {get_china_time().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        txt_content += f"{content}\n"
        
        # 创建临时TXT文件
        txt_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_article_{article_index}.txt")
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(txt_content)
        
        return send_file(txt_file, as_attachment=True, download_name=filename)
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：批量下载所有文章TXT文件（从数据库获取）
@app.route('/api/download-all-articles-txt')
def download_all_articles_txt():
    try:
        task_id = request.args.get('task_id')
        clean_content = request.args.get('clean', 'false').lower() == 'true'
        
        print(f"📥 下载全部文章TXT请求: task_id={task_id}, clean={clean_content}")
        
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        # 🔥 主要改动：直接从数据库查询该任务的文章
        from sqlite_database import sqlite_db
        from datetime import timedelta
        
        try:
            sqlite_db.connect()
            cursor = sqlite_db.connection.cursor()
            
            articles = []
            
            # 方法1: 通过article_tasks关联表查询（如果有的话）
            try:
                cursor.execute("""
                    SELECT a.url, a.title, a.content, a.publish_date, a.extraction_method, a.quality_score, a.created_at
                    FROM articles a
                    INNER JOIN article_tasks at ON a.id = at.article_id
                    WHERE at.task_id = ?
                    ORDER BY a.created_at DESC
                """, (task_id,))
                articles = cursor.fetchall()
                print(f"📊 通过关联表找到 {len(articles)} 篇文章")
            except Exception as e:
                print(f"⚠️ 关联表查询失败: {e}")
            
            if not articles:
                print(f"❌ 数据库中没有找到文章")
                return jsonify({'success': False, 'message': '数据库中没有找到该任务的文章，请确保任务已成功执行并已建立文章-任务关联'})
            
            # 转换为data_list格式
            data_list = []
            for article in articles:
                url, title, content, publish_date, extraction_method, quality_score, created_at = article
                data_list.append({
                    'url': url,
                    'title': title,
                    'content': content,  # 数据库中已经是清洗后的内容
                    'metadata': {
                        'publishDate': publish_date,
                        'extractionMethod': extraction_method,
                        'qualityScore': quality_score
                    }
                })
            
            print(f"✅ 从数据库获取到 {len(data_list)} 篇文章")
            
        except Exception as e:
            print(f"❌ 从数据库获取文章失败: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({'success': False, 'message': f'从数据库获取文章失败: {str(e)}'})
        
        # 创建ZIP文件
        import zipfile
        import io
        
        print(f"📦 开始创建ZIP文件，包含 {len(data_list)} 篇文章")
        
        # 获取任务URL用于生成ZIP文件名
        task_url = ''
        try:
            cursor.execute("SELECT target_url FROM crawl_tasks WHERE task_id = ?", (task_id,))
            result = cursor.fetchone()
            if result:
                task_url = result[0]
        except:
            pass
        
        zip_buffer = io.BytesIO()
        valid_article_count = 0
        skipped_count = 0
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, article in enumerate(data_list):
                try:
                    if not isinstance(article, dict):
                        print(f"⏭️ 跳过第{i+1}项：不是字典类型")
                        skipped_count += 1
                        continue
                    
                    article_url = article.get('url', '')
                    article_title = article.get('title', '无标题')
                    content = article.get('content', '')  # 数据库中的content已经是清洗后的纯文本
                    
                    if valid_article_count < 3:  # 只显示前3篇的详细信息
                        print(f"📄 处理第{i+1}项: {article_title[:30]}...")
                    
                    if not content or len(content.strip()) < 50:
                        if valid_article_count < 3:
                            print(f"⏭️ 跳过：内容太短 (长度: {len(content) if content else 0})")
                        skipped_count += 1
                        continue
                    
                    # 数据库中的内容已经是清洗好的，无需再次清洗
                    # 如果用户明确要求深度清洗，才进行额外处理
                    if clean_content:
                        content = clean_article_content(content, 'text')
                        if valid_article_count < 3:
                            print(f"🧹 深度清洗后内容长度: {len(content)}")
                    
                    # 获取发布日期
                    publish_date = None
                    if 'metadata' in article and isinstance(article['metadata'], dict):
                        publish_date = article['metadata'].get('publishDate')
                    
                    # 生成文件名：使用文章标题
                    import re
                    # 清理标题，去除不合法的文件名字符
                    safe_title = re.sub(r'[<>:"/\\|?*]', '_', article_title)
                    # 限制文件名长度
                    safe_title = safe_title[:100]
                    # 如果标题为空或"无标题"，使用序号
                    if not safe_title or safe_title.strip() in ['无标题', 'Untitled', '']:
                        safe_title = f"文章_{i+1:03d}"
                    filename = f"{safe_title}.txt"
                    
                    if valid_article_count < 3:
                        print(f"📝 文件名: {filename}")
                    
                    # 生成TXT内容：只保留纯文本，不要元数据
                    txt_content = content
                    
                    # 添加到ZIP文件
                    zip_file.writestr(filename, txt_content.encode('utf-8'))
                    valid_article_count += 1
                    
                    if valid_article_count <= 3:
                        print(f"✅ 成功添加第{valid_article_count}篇文章")
                    
                except Exception as e:
                    print(f"❌ 处理第{i+1}项失败: {e}")
                    skipped_count += 1
                    continue
        
        print(f"📊 ZIP文件创建完成: {valid_article_count} 篇成功, {skipped_count} 篇跳过")
        
        if valid_article_count == 0:
            print(f"❌ 没有有效的文章可供下载")
            return jsonify({'success': False, 'message': '没有找到有效的文章内容'})
        
        zip_buffer.seek(0)
        
        # 生成ZIP文件名：域名+易读的日期时分秒
        from urllib.parse import urlparse
        if task_url:
            parsed_url = urlparse(task_url)
            domain = parsed_url.netloc.replace('www.', '') if parsed_url.netloc else 'articles'
        else:
            domain = 'articles'
        
        # 使用易读的日期时分秒格式：2025-11-06_15-38-34
        current_datetime = get_china_time().strftime('%Y-%m-%d_%H-%M-%S')
        clean_suffix = "_cleaned" if clean_content else ""
        zip_filename = f"{domain}_{current_datetime}{clean_suffix}.zip"
        print(f"📦 ZIP文件名: {zip_filename}")
        
        print(f"📤 发送ZIP文件: {zip_filename}")
        
        # 创建响应
        response = send_file(
            io.BytesIO(zip_buffer.getvalue()),
            as_attachment=True,
            download_name=zip_filename,
            mimetype='application/zip'
        )
        
        # 确保Content-Disposition头正确设置（兼容中文文件名）
        from urllib.parse import quote
        encoded_filename = quote(zip_filename)
        response.headers['Content-Disposition'] = f"attachment; filename*=UTF-8''{encoded_filename}"
        
        return response
    except Exception as e:
        import traceback
        error_msg = str(e)
        print(f"❌ 下载全部文章TXT失败: {error_msg}")
        print(traceback.format_exc())
        return jsonify({'success': False, 'message': f'下载失败: {error_msg}'})

# API路由：下载结果（TXT格式）
@app.route('/api/download-txt')
def download_txt():
    try:
        task_id = request.args.get('task_id')
        clean_content = request.args.get('clean', 'false').lower() == 'true'
        
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        # 尝试从JSON文件生成TXT
        result_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.json")
        if not os.path.exists(result_file):
            return jsonify({'success': False, 'message': '结果不存在'})
        
        with open(result_file, 'r', encoding='utf-8') as f:
            result = json.load(f)
        
        # 生成TXT内容（只保留纯文本）
        txt_content = ""
        
        # 处理不同的数据结构格式
        valid_articles = []  # 存储有效的文章
        
        # 首先检查是否有直接的data数组
        if 'data' in result and isinstance(result['data'], list):
            data_list = result['data']
        elif isinstance(result, dict) and 'result' in result:
            # 处理另一种可能的数据结构
            crawl_result = result['result']
            if isinstance(crawl_result, list):
                data_list = crawl_result
            else:
                data_list = []
        else:
            data_list = []
        
        # 处理数据列表
        if data_list:
            for i, item in enumerate(data_list):
                if isinstance(item, dict):
                    # 处理标题
                    title = item.get('title', item.get('metadata', {}).get('title', '无标题'))
                    
                    # 🔥 修复：处理不同的数据结构
                    content = ""
                    content_type = 'markdown'
                    
                    # 优先级1：直接的content字段（ArticleLinkExtractor格式）
                    if 'content' in item and isinstance(item['content'], str) and item['content']:
                        content = item['content']
                        content_type = 'text'
                    # 优先级2：markdown字段（Firecrawl格式）
                    elif 'markdown' in item and item['markdown']:
                        content = item['markdown']
                        content_type = 'markdown'
                    # 优先级3：嵌套的content（旧格式）
                    elif 'content' in item and isinstance(item['content'], dict):
                        nested_content = item['content']
                        if 'markdown' in nested_content:
                            content = nested_content['markdown']
                            content_type = 'markdown'
                        elif 'content' in nested_content:
                            content = nested_content['content']
                            content_type = 'text'
                    # 优先级4：html字段
                    elif 'html' in item and item['html']:
                        content = item['html']
                        content_type = 'html'
                    
                    # 🔥 修复：对于已经是纯文本的内容，跳过验证（已由爬虫验证过）
                    if content_type == 'text':
                        # 直接使用，不再验证（已由ArticleLinkExtractor验证过）
                        is_valid = len(content.strip()) > 50
                    else:
                        # 检查是否是有效的文章内容
                        is_valid = is_valid_article_content(content, title)
                    
                    if content and is_valid:
                        # 🔥 修复：对于纯文本内容，跳过清洗
                        if clean_content and content_type != 'text':
                            content = clean_article_content(content, content_type)
                        
                        if content and len(content.strip()) > 50:  # 再次检查清洗后的内容
                            valid_articles.append({
                                'url': item.get('url', ''),
                                'title': title,
                                'content': content
                            })
        
        if not valid_articles:
            return jsonify({'success': False, 'message': '没有找到有效的文章内容'})
        
        # 🔥 改为生成ZIP包，每个文章一个txt文件（和主路由保持一致）
        import zipfile
        import io
        import re
        
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, article in enumerate(valid_articles):
                # 生成文件名：使用文章标题
                article_title = article.get('title', '无标题')
                safe_title = re.sub(r'[<>:"/\\|?*]', '_', article_title)
                safe_title = safe_title[:100]
                if not safe_title or safe_title.strip() in ['无标题', 'Untitled', '']:
                    safe_title = f"文章_{i+1:03d}"
                filename = f"{safe_title}.txt"
                
                # txt内容：只有正文
                txt_content = article['content']
                
                # 添加到ZIP
                zip_file.writestr(filename, txt_content.encode('utf-8'))
        
        zip_buffer.seek(0)
        
        # 从URL中提取域名
        url = result.get('url', '')
        from urllib.parse import urlparse
        parsed_url = urlparse(url)
        domain = parsed_url.netloc.replace('www.', '') if parsed_url.netloc else 'articles'
        
        # 生成ZIP文件名：域名+时间
        current_datetime = get_china_time().strftime('%Y-%m-%d_%H-%M-%S')
        clean_suffix = "_cleaned" if clean_content else ""
        zip_filename = f"{domain}_{current_datetime}{clean_suffix}.zip"
        
        # 创建响应
        response = send_file(
            io.BytesIO(zip_buffer.getvalue()),
            as_attachment=True,
            download_name=zip_filename,
            mimetype='application/zip'
        )
        
        # 确保Content-Disposition头正确设置（兼容中文文件名）
        from urllib.parse import quote
        encoded_filename = quote(zip_filename)
        response.headers['Content-Disposition'] = f"attachment; filename*=UTF-8''{encoded_filename}"
        
        return response
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

# API路由：删除结果
@app.route('/api/delete-result', methods=['DELETE'])
def delete_result():
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'message': '缺少任务ID'})
        
        print(f"🗑️ 尝试删除任务结果: {task_id}")
        
        deleted_files = []
        
        # 从内存中删除
        with task_lock:
            if task_id in crawl_tasks:
                del crawl_tasks[task_id]
                print(f"✅ 从内存中删除任务: {task_id}")
            else:
                print(f"⚠️ 内存中未找到任务: {task_id}")
        
        # 从文件系统中删除
        for ext in ['.json', '.md', '.txt', '_detail.json']:
            file_path = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}{ext}")
            if os.path.exists(file_path):
                os.remove(file_path)
                deleted_files.append(f"{task_id}{ext}")
                print(f"✅ 删除文件: {file_path}")
        
        # 从数据库中删除相关记录
        try:
            # 删除爬取任务记录
            sqlite_db.delete_crawl_task_by_task_id(task_id)
            print(f"✅ 从数据库删除爬取任务记录: {task_id}")
        except Exception as db_error:
            print(f"⚠️ 数据库删除失败: {db_error}")
            # 不影响主要删除流程
        
        return jsonify({
            'success': True, 
            'message': f'任务 {task_id} 删除成功',
            'deleted_files': deleted_files
        })
    except Exception as e:
        print(f"❌ 删除任务结果失败: {e}")
        return jsonify({'success': False, 'message': str(e)})

# API路由：清空所有结果
@app.route('/api/clear-results', methods=['POST'])
def clear_results():
    try:
        print("🗑️ 开始清空所有爬取任务...")
        
        # 1. 清空内存中的任务
        with task_lock:
            task_count = len(crawl_tasks)
            crawl_tasks.clear()
            print(f"✅ 清空内存任务: {task_count} 个")
        
        # 2. 清空数据库中的爬取任务
        try:
            from sqlite_database import sqlite_db
            sqlite_db.connect()
            cursor = sqlite_db.connection.cursor()
            
            # 删除所有爬取任务
            cursor.execute("SELECT COUNT(*) FROM crawl_tasks")
            db_task_count = cursor.fetchone()[0]
            
            cursor.execute("DELETE FROM crawl_tasks")
            sqlite_db.connection.commit()
            print(f"✅ 清空数据库任务: {db_task_count} 个")
        except Exception as e:
            print(f"⚠️ 清空数据库任务失败: {e}")
        
        # 3. 清空文件系统中的结果
        file_count = 0
        for filename in os.listdir(CRAWL_RESULTS_DIR):
            file_path = os.path.join(CRAWL_RESULTS_DIR, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                file_count += 1
        print(f"✅ 清空结果文件: {file_count} 个")
        
        print("✅ 所有爬取任务已清空")
        return jsonify({'success': True, 'message': '已清空所有爬取任务'})
    except Exception as e:
        print(f"❌ 清空任务失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)})

# 定期清理过期任务
def clean_expired_tasks():
    while True:
        now = get_china_time()
        expired_task_ids = []
        
        with task_lock:
            for task_id, task in crawl_tasks.items():
                if task['status'] in ['completed', 'error']:
                    try:
                        completed_at = datetime.fromisoformat(task['completed_at'])
                        if (now - completed_at).days >= 7:  # 保留7天
                            expired_task_ids.append(task_id)
                    except:
                        continue
        
        # 删除过期任务
        for task_id in expired_task_ids:
            with task_lock:
                if task_id in crawl_tasks:
                    del crawl_tasks[task_id]
            
            # 删除相关文件
            for ext in ['.json', '.md', '.txt', '_detail.json']:
                file_path = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}{ext}")
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except:
                        continue
        
        time.sleep(86400)  # 每天清理一次

# 启动清理线程
clean_thread = Thread(target=clean_expired_tasks, daemon=True)
clean_thread.start()

# 注意：原有的文章详情提取API已删除，功能已整合到智能提取API中

# API路由：获取文章数据库统计信息
@app.route('/api/get-extracted-articles', methods=['GET'])
def get_extracted_articles():
    """获取已提取的文章列表"""
    try:
        articles_files = []
        if os.path.exists('crawl_results'):
            for filename in os.listdir('crawl_results'):
                if filename.startswith('extracted_articles_') and filename.endswith('.json'):
                    file_path = os.path.join('crawl_results', filename)
                    stat = os.stat(file_path)
                    articles_files.append({
                        'filename': filename,
                        'task_id': filename.split('_')[2].replace('.json', ''),
                        'modified_time': stat.st_mtime,
                        'size': stat.st_size
                    })
        
        # 按修改时间排序
        articles_files.sort(key=lambda x: x['modified_time'], reverse=True)
        
        return jsonify({
            'success': True,
            'files': articles_files
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取文件列表失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/api/get-article-details/<filename>')
def get_article_details(filename):
    """获取特定文件的文章详情"""
    try:
        file_path = os.path.join('crawl_results', filename)
        
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'message': '文件不存在'}), 404
        
        with open(file_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)
        
        return jsonify({
            'success': True,
            'articles': articles,
            'count': len(articles)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'读取文件失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/api/get-website-configs', methods=['GET'])
def get_website_configs():
    """获取支持的网站配置"""
    try:
        config_file = 'website_configs.json'
        if os.path.exists(config_file):
            with open(config_file, 'r', encoding='utf-8') as f:
                configs = json.load(f)
            
            # 只返回网站名称和基本信息
            websites = {}
            for key, config in configs.items():
                websites[key] = {
                    'name': config['name'],
                    'base_url': config['base_url'],
                    'list_url': config['list_url']
                }
            
            return jsonify({
                'success': True,
                'websites': websites
            })
        else:
            return jsonify({
                'success': False,
                'message': '配置文件不存在'
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'获取配置失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/api/start-article-crawl', methods=['POST'])
def start_article_crawl():
    """启动文章爬取任务"""
    try:
        data = request.get_json(silent=True) or {}
        url = data.get('url', '').strip()
        limit = _coerce_int(data.get('limit'), 10, 1, 10000)
        incremental = data.get('incremental', False)
        keywords = data.get('keywords', '').strip()  # 🔥 获取关键词
        kb_id = data.get('kb_id', '').strip()  # 🔥 获取知识库ID
        days_limit = _coerce_days_limit(data.get('days_limit'), 7)
        start_date = data.get('start_date') or data.get('date_start') or data.get('from_date')
        end_date = data.get('end_date') or data.get('date_end') or data.get('to_date')
        crawl_options = normalize_crawl_options(data)
        
        if not url:
            return jsonify({'success': False, 'message': 'URL不能为空'})
        
        # 生成任务ID
        task_id = str(uuid.uuid4())
        
        # 创建任务到内存
        with task_lock:
            crawl_tasks[task_id] = {
                'task_id': task_id,
                'url': url,
                'limit': limit,
                'mode': 'article_crawl',
                'incremental': incremental,
                'keywords': keywords,  # 🔥 保存关键词
                'kb_id': kb_id,  # 🔥 保存知识库ID
                'days_limit': days_limit,  # 🔥 保存日期限制
                'start_date': start_date,
                'end_date': end_date,
                'crawl_options': crawl_options,
                'status': 'pending',
                'progress': 0,
                'created_at': get_china_time().isoformat(),
                'logs': []
            }
        
        # 同时保存到数据库
        try:
            from sqlite_database import sqlite_db
            task_data = {
                'task_id': task_id,
                'target_url': url,
                'task_name': f'文章爬取-{url}',
                'crawl_depth': 1,
                'crawl_mode': 'article_crawl',
                'page_limit': limit,
                'incremental_mode': incremental,
                'status': 'pending'
            }
            
            db_id = sqlite_db.insert_crawl_task(task_data)
            if db_id:
                print(f"✅ 文章爬取任务已创建并入库: {task_id} (DB ID: {db_id})")
            else:
                print(f"⚠️ 文章爬取任务创建成功但入库失败: {task_id}")
                
        except Exception as e:
            print(f"❌ 文章爬取任务入库失败: {e}")
        
        # 启动爬取任务
        thread = threading.Thread(target=run_article_crawl_task, args=(task_id,))
        thread.daemon = True
        thread.start()
        
        print(f"文章爬取任务已创建: {task_id}")
        print(f"增量爬取选项: {incremental}")
        print(f"当前任务列表: {list(crawl_tasks.keys())}")
        
        return jsonify({
            'success': True,
            'task_ids': [task_id],
            'message': '文章爬取任务已启动'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

def _get_url_auth_config(url: str) -> Optional[dict]:
    """
    获取URL的认证配置
    
    Args:
        url: 目标URL
        
    Returns:
        dict: 认证配置（包含auth_name），如果不需要认证返回None
    """
    try:
        from sqlite_database import sqlite_db
        import json
        
        print(f"🔍 _get_url_auth_config: 查找URL={url}")
        
        # 规范化URL进行查找（支持有无协议两种格式）
        from urllib.parse import urlparse
        search_urls = [url]
        
        # 如果有协议，也尝试不带协议的版本
        if url.startswith(('http://', 'https://')):
            parsed = urlparse(url)
            url_without_protocol = parsed.netloc + parsed.path
            if parsed.query:
                url_without_protocol += '?' + parsed.query
            search_urls.append(url_without_protocol)
            print(f"   同时搜索: {url_without_protocol}")
        
        # 先尝试新结构：通过auth_config_id关联
        cursor = sqlite_db.connection.cursor()
        
        # 尝试匹配任一格式的URL
        placeholders = ' OR '.join(['mu.url = ?' for _ in search_urls])
        cursor.execute(f"""
            SELECT mu.auth_config_id, ac.name, ac.storage_file
            FROM managed_urls mu
            LEFT JOIN auth_configs ac ON mu.auth_config_id = ac.id
            WHERE ({placeholders}) AND mu.auth_config_id IS NOT NULL
        """, search_urls)
        
        result = cursor.fetchone()
        
        if result and result['auth_config_id']:
            auth_name = result['name']
            print(f"✅ 找到认证配置（新结构）: auth_config_id={result['auth_config_id']}, auth_name={auth_name}")
            cursor.close()
            return {
                'auth_name': auth_name,
                'auth_config_id': result['auth_config_id']
            }
        
        # 如果新结构找不到，尝试旧结构
        cursor.execute("""
            SELECT auth_config FROM managed_urls WHERE url = ?
        """, (url,))
        
        result = cursor.fetchone()
        cursor.close()
        
        if result and result['auth_config']:
            # 解析认证配置
            auth_config = json.loads(result['auth_config']) if isinstance(result['auth_config'], str) else result['auth_config']
            
            # 检查是否有auth_name（认证配置的关键字段）
            if auth_config.get('auth_name'):
                print(f"✅ 找到认证配置（旧结构）: auth_name={auth_config.get('auth_name')}")
                return auth_config
        
        print(f"ℹ️ URL无需认证")
        return None
        
    except Exception as e:
        print(f"⚠️ 获取认证配置失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def _crawl_with_authentication(
    task_id: str,
    url: str,
    auth_name: str,
    limit: int,
    incremental: bool,
    keywords: str = '',
    kb_id: str = '',
    days_limit: int = 7,
    start_date: str = None,
    end_date: str = None,
    crawl_options: dict = None
) -> dict:
    """
    使用认证信息爬取文章
    
    Args:
        task_id: 任务ID
        url: 目标URL
        auth_name: 认证配置名称
        limit: 仅用于显示（不限制实际爬取数量）
        incremental: 是否增量爬取
        keywords: 关键词过滤（可选）
        kb_id: RAGFlow知识库ID（可选）
        days_limit: 日期限制（天）
        start_date: 指定起始日期（YYYY-MM-DD，可选，优先于days_limit）
        end_date: 指定结束日期（YYYY-MM-DD，可选）
        crawl_options: 通用爬取配置
        
    Returns:
        dict: 爬取结果
    
    注意：
        limit 参数仅用于日志显示，实际会爬取所有提取到的文章
    """
    try:
        import asyncio
        from test_authenticated_crawler import AuthenticatedCrawler
        from sqlite_database import sqlite_db

        crawl_options = normalize_crawl_options(crawl_options)
        wait_seconds = max(1, int((crawl_options.get('wait_for_ms') or 8000) / 1000))
        auth_max_pages = crawl_options.get('max_pages') or float('inf')
        
        _append_task_log(task_id, f"🔑 加载认证配置: {auth_name}")
        
        # 检查认证文件是否存在
        import os
        auth_file = os.path.join('./auth_storage', f'{auth_name}.json')
        if not os.path.exists(auth_file):
            _append_task_log(task_id, f"❌ 认证文件不存在: {auth_file}")
            return {
                'success': False,
                'error': f'认证文件不存在，请重新配置认证',
                'articles': []
            }
        
        _append_task_log(task_id, f"✅ 认证文件已找到")
        
        # 更新进度
        with task_lock:
            crawl_tasks[task_id]['progress'] = 20
        
        # 创建认证爬虫实例
        crawler = AuthenticatedCrawler()
        
        _append_task_log(task_id, "🚀 开始认证爬取...")
        _append_task_log(
            task_id,
            "认证爬取策略: "
            f"等待{crawl_options.get('wait_for_ms')}ms, "
            f"最大翻页{auth_max_pages}, "
            f"连续空页{crawl_options.get('max_empty_pages')}, "
            f"代理{'开' if crawl_options.get('proxy_enabled') else '关'}"
        )
        with task_lock:
            crawl_tasks[task_id]['progress'] = 30
        
        # 执行认证爬取（异步）
        # 🔥 修改：不限制文章数量和页数
        async def do_crawl():
            return await crawler.crawl_with_auth(
                url=url,
                auth_name=auth_name,
                max_articles=float('inf'),  # 🔥 无限制
                max_pages=auth_max_pages,
                wait_time=wait_seconds,
                extract_content=True,
                keywords=keywords,          # 🔥 关键词过滤
                kb_id=kb_id,                # 🔥 知识库ID
                days_limit=days_limit,      # 🔥 日期限制
                start_date=start_date,
                end_date=end_date,
                crawl_options=crawl_options
            )
        
        # 运行异步爬取
        result = asyncio.run(do_crawl())
        
        if result['success']:
            _append_task_log(task_id, f"✅ 认证爬取成功")
            _append_task_log(task_id, f"📊 爬取统计: 新文章 {result['stats']['new_articles']} 篇，跳过 {result['stats']['skipped_articles']} 篇")
            ragflow_stats = result.get('stats', {}).get('ragflow')
            if kb_id and isinstance(ragflow_stats, dict):
                _append_task_log(
                    task_id,
                    "📤 RAGFlow统计: "
                    f"上传 {ragflow_stats.get('uploaded', 0)} | "
                    f"已存在 {ragflow_stats.get('skipped_existing', 0)} | "
                    f"空内容 {ragflow_stats.get('skipped_empty', 0)} | "
                    f"失败 {ragflow_stats.get('failed', 0)}"
                )
            
            # 转换格式以兼容现有系统
            # AuthenticatedCrawler已经自动入库了，这里只需要格式化返回结果
            formatted_articles = []
            for article in result['articles']:
                formatted_articles.append({
                    'url': article['url'],
                    'title': article['title'],
                    'content': article.get('content', ''),
                    'publish_date': article.get('publish_date'),
                    'crawled_at': article.get('crawled_at')
                })
            
            return {
                'success': True,
                'articles': formatted_articles,
                'stats': result['stats']
            }
        else:
            error_msg = result.get('error', '未知错误')
            _append_task_log(task_id, f"❌ 认证爬取失败: {error_msg}")
            return result
            
    except Exception as e:
        error_msg = f"认证爬取异常: {str(e)}"
        _append_task_log(task_id, f"❌ {error_msg}")
        print(f"❌ {error_msg}")
        import traceback
        traceback.print_exc()
        return {
            'success': False,
            'error': error_msg,
            'articles': []
        }

def run_article_crawl_task(task_id):
    """运行文章爬取任务 - 支持自动识别认证"""
    try:
        with task_lock:
            current_task = crawl_tasks.get(task_id, {})
        
        if not current_task:
            _append_task_log(task_id, f"任务不存在: {task_id}")
            return
        
        url = current_task.get('url', '')
        limit = current_task.get('limit', 10)
        incremental = current_task.get('incremental', False)
        keywords = current_task.get('keywords', '')  # 🔥 读取关键词
        kb_id = current_task.get('kb_id', '')  # 🔥 读取知识库ID
        days_limit = current_task.get('days_limit', 7)  # 🔥 读取日期限制
        start_date = current_task.get('start_date')
        end_date = current_task.get('end_date')
        crawl_options = normalize_crawl_options(current_task)
        
        _append_task_log(task_id, f"开始文章爬取: {url}")
        _append_task_log(task_id, f"栏目采集模式: Playwright 主爬，显示限制参数={limit}，增量爬取: {incremental}")
        _append_task_log(
            task_id,
            "爬取策略: "
            f"等待{crawl_options.get('wait_for_ms')}ms, "
            f"最大翻页{crawl_options.get('max_pages')}, "
            f"连续空页{crawl_options.get('max_empty_pages')}, "
            f"详情重试{crawl_options.get('detail_max_retries')}次, "
            f"补充发现{'开' if crawl_options.get('supplemental_enabled') else '关'}, "
            f"网络JSON{'开' if crawl_options.get('network_json_enabled') else '关'}, "
            f"代理{'开' if crawl_options.get('proxy_enabled') else '关'}"
        )
        if keywords:
            _append_task_log(task_id, f"🎯 关键词过滤: {keywords}")
        if kb_id:
            _append_task_log(task_id, f"📤 目标知识库: {kb_id}")
        if start_date or end_date:
            _append_task_log(task_id, f"📅 日期范围: {start_date or '不限'} ~ {end_date or '不限'}")
        elif days_limit and days_limit > 0:
            _append_task_log(task_id, f"📅 日期限制: 最近{days_limit}天")
        _append_task_log(task_id, f"💡 所有提取到的文章链接都会被爬取并保存")
        
        # 更新任务状态
        with task_lock:
            crawl_tasks[task_id]['status'] = 'running'
            crawl_tasks[task_id]['progress'] = 10
        try:
            sqlite_db.update_crawl_task_status(task_id, 'running', progress=10)
        except Exception:
            pass
        
        # 导入必要的模块
        from article_link_extractor import ArticleLinkExtractor
        from sqlite_database import sqlite_db
        
        # 🔍 检查URL是否需要认证
        _append_task_log(task_id, "🔍 检查URL认证配置...")
        auth_config = _get_url_auth_config(url)
        
        if auth_config:
            # 使用认证爬虫
            auth_name = auth_config.get('auth_name')
            _append_task_log(task_id, f"✅ 检测到认证配置: {auth_name}")
            _append_task_log(task_id, "🔐 使用认证爬虫进行爬取...")
            _append_task_log(task_id, "认证模式使用已保存登录态，日期范围/等待/翻页/代理按当前任务配置执行")
            _append_task_log(task_id, "⚠️  认证模式暂不启用公开站点的补充发现源")
            
            # 执行认证爬取
            result = _crawl_with_authentication(
                task_id=task_id,
                url=url,
                auth_name=auth_name,
                limit=limit,
                incremental=incremental,
                keywords=keywords,      # 🔥 传递关键词
                kb_id=kb_id,            # 🔥 传递知识库ID
                days_limit=days_limit,  # 🔥 传递日期限制
                start_date=start_date,
                end_date=end_date,
                crawl_options=crawl_options
            )
        else:
            # 使用 Playwright 栏目爬虫
            _append_task_log(task_id, "无需认证，使用 Playwright 栏目爬虫...")
            _append_task_log(task_id, "策略：Playwright 遍历分页/Tab/加载更多，候选链接统一提取详情")
            _append_task_log(task_id, "目标：不静默遗漏，低质量内容进入失败/跳过统计")
            
            # 导入兼容爬虫（内部已切换为纯 Playwright）
            from hybrid_crawler import HybridCrawler
            
            # 创建 Playwright 栏目爬虫实例
            hybrid = HybridCrawler(db=sqlite_db)
            
            # 执行栏目爬取（自动入库，自动去重）
            with task_lock:
                crawl_tasks[task_id]['progress'] = 30
            
            # 使用日志回调
            def log_callback(msg):
                _append_task_log(task_id, msg)
            
            result = hybrid.crawl_news_site(
                list_url=url,
                limit=limit,
                wait_for=8000,
                keywords=keywords,
                kb_id=kb_id,  # 🔥 传递知识库ID，用于分批上传
                days_limit=days_limit,  # 🔥 传递日期限制
                start_date=start_date,
                end_date=end_date,
                log_callback=log_callback,
                crawl_options=crawl_options,
                task_id=task_id
            )
        
        if result['success']:
            # 获取文章数据（已自动入库）
            articles_data = result.get('articles', [])
            audit_data = result.get('audit') or {}
            
            # 转换格式以兼容原有逻辑
            articles = []
            for item in articles_data:
                # 判断数据格式：认证爬虫返回的是直接格式，普通爬虫返回的有content嵌套
                if 'content' in item and isinstance(item['content'], dict):
                    # ArticleLinkExtractor 格式
                    content_data = item.get('content', {})
                    articles.append({
                        'url': content_data.get('url', ''),
                        'title': content_data.get('title', ''),
                        'content': content_data.get('content', ''),
                        'publish_date': content_data.get('publish_date'),
                        'db_id': item.get('db_id')  # 数据库ID
                    })
                else:
                    # AuthenticatedCrawler 格式（直接格式）
                    articles.append({
                        'url': item.get('url', ''),
                        'title': item.get('title', ''),
                        'content': item.get('content', ''),
                        'publish_date': item.get('publish_date'),
                        'crawled_at': item.get('crawled_at')
                    })

            linked_to_task_count = 0
            for article in articles:
                try:
                    article_db_id = article.get('db_id') or sqlite_db.get_article_id_by_url(article.get('url', ''))
                    if article_db_id and sqlite_db.link_article_to_task(article_db_id, task_id):
                        linked_to_task_count += 1
                except Exception as link_error:
                    _append_task_log(task_id, f"文章任务关联失败: {str(link_error)}")
            if linked_to_task_count:
                _append_task_log(task_id, f"已关联文章到当前任务: {linked_to_task_count} 篇")
            
            # 判断是哪种爬虫
            crawl_type = "认证爬虫" if auth_config else "Playwright栏目爬虫"
            _append_task_log(task_id, f"成功爬取 {len(articles)} 篇文章（已自动入库）- {crawl_type}")
            
            # 显示统计信息
            if 'stats' in result:
                stats = result['stats']
                if 'db_total' in stats:
                    _append_task_log(task_id, f"数据库文章数: {stats.get('db_total', 0)}")
                if 'playwright_links' in stats:
                    _append_task_log(task_id, f"Playwright候选: {stats.get('playwright_links', 0)} 个")
                if 'network_json_candidates' in stats:
                    _append_task_log(
                        task_id,
                        f"网络JSON候选: {stats.get('network_json_candidates', 0)} 个"
                        f"（内联正文 {stats.get('network_json_inline_candidates', 0)} 个）"
                    )
                if stats.get('network_json_responses_checked') is not None:
                    _append_task_log(
                        task_id,
                        f"接口检查: 响应 {stats.get('network_json_responses_checked', 0)} 个 / "
                        f"JSONP {stats.get('network_jsonp_responses', 0)} 个 / "
                        f"脚本 {stats.get('network_script_responses_checked', 0)} 个 / "
                        f"HTML错误 {stats.get('network_html_error_responses', 0)} 个"
                    )
                if stats.get('network_verification_signals'):
                    reasons = ', '.join(
                        sorted({item.get('reason', '') for item in stats.get('network_verification_signals', []) if item.get('reason')})
                    )
                    _append_task_log(task_id, f"接口验证信号: {reasons or '检测到验证/登录要求'}")
                if 'supplemental_links' in stats:
                    _append_task_log(task_id, f"补充发现候选: {stats.get('supplemental_links', 0)} 个")
                if any(k in stats for k in ['html_static_candidates', 'attribute_candidates', 'structured_candidates', 'embedded_script_candidates', 'static_pagination_candidates', 'network_json_candidates', 'feed_candidates', 'sitemap_candidates']):
                    _append_task_log(
                        task_id,
                        "补充来源: "
                        f"HTML {stats.get('html_static_candidates', 0)} / "
                        f"属性 {stats.get('attribute_candidates', 0)} / "
                        f"结构化 {stats.get('structured_candidates', 0)} / "
                        f"脚本 {stats.get('embedded_script_candidates', 0)} / "
                        f"静态分页 {stats.get('static_pagination_candidates', 0)} / "
                        f"网络JSON {stats.get('network_json_candidates', 0)} / "
                        f"RSS {stats.get('feed_candidates', 0)}"
                        f"（内联 {stats.get('feed_inline_candidates', 0)}） / "
                        f"Sitemap {stats.get('sitemap_candidates', 0)}"
                    )
                if 'valid_links' in stats:
                    _append_task_log(task_id, f"候选链接: {stats.get('valid_links', 0)} 个")
                if 'date_unknown' in stats:
                    _append_task_log(task_id, f"日期未知但继续处理: {stats.get('date_unknown', 0)} 篇")
                if 'date_skipped' in stats:
                    _append_task_log(task_id, f"日期超限跳过: {stats.get('date_skipped', 0)} 篇")
                if 'quality_skipped' in stats:
                    _append_task_log(task_id, f"质量不达标跳过: {stats.get('quality_skipped', 0)} 篇")
                if 'keyword_skipped' in stats:
                    _append_task_log(task_id, f"关键词不匹配跳过: {stats.get('keyword_skipped', 0)} 篇")
                if 'extraction_retries' in stats:
                    _append_task_log(task_id, f"详情页提取重试: {stats.get('extraction_retries', 0)} 次")
                if stats.get('recall_risk_count', 0) > 0:
                    _append_task_log(task_id, f"需复核候选: {stats.get('recall_risk_count', 0)} 个（查看审计文件）")
                if 'new_articles' in stats and 'skipped_articles' in stats:
                    _append_task_log(task_id, f"新增: {stats['new_articles']} 篇，跳过: {stats['skipped_articles']} 篇")
            
            # 处理增量爬取
            if incremental:
                _append_task_log(task_id, '开始处理增量爬取记录')
                new_articles_count = 0
                skipped_articles_count = 0
                
                filtered_articles = []
                for article in articles:
                    article_url = article['url']
                    publish_date = article.get('publish_date')
                    
                    # 检查是否已爬取过
                    if _is_article_crawled(article_url, publish_date):
                        skipped_articles_count += 1
                        _append_task_log(task_id, f'跳过已爬取文章: {article["title"][:50]}...')
                    else:
                        # 计算内容哈希用于去重
                        content = article.get('content', '')
                        content_hash = str(hash(content)) if content else None
                        
                        # 记录新文章
                        _add_crawled_article(article_url, article['title'], publish_date, content_hash)
                        filtered_articles.append(article)
                        new_articles_count += 1
                        _append_task_log(task_id, f'记录新文章: {article["title"][:50]}...')
                
                articles = filtered_articles
                _append_task_log(task_id, f'增量爬取完成: 新文章 {new_articles_count} 篇，跳过 {skipped_articles_count} 篇')
            
            # 更新任务状态
            with task_lock:
                crawl_tasks[task_id]['status'] = 'completed'
                crawl_tasks[task_id]['progress'] = 100
                crawl_tasks[task_id]['completed_at'] = get_china_time().isoformat()
                crawl_tasks[task_id]['articles_found'] = len(articles)
                crawl_tasks[task_id]['articles_processed'] = len(articles)
                crawl_tasks[task_id]['result'] = {
                    'success': True,
                    'data': articles,
                    'total_articles': len(articles),
                    'audit_file': f"{task_id}_audit.json" if audit_data else None
                }
            try:
                sqlite_db.update_crawl_task_status(
                    task_id,
                    'completed',
                    progress=100,
                    articles_found=len(articles),
                    articles_processed=len(articles),
                    error_message=''
                )
            except Exception:
                pass
            
            # 保存结果到文件
            result_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}.json")
            try:
                with open(result_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'task_id': task_id,
                        'url': url,
                        'mode': 'article_crawl',
                        'limit': limit,
                        'incremental': incremental,
                        'status': 'completed',
                        'created_at': current_task.get('created_at'),
                        'completed_at': get_china_time().isoformat(),
                        'audit_file': f"{task_id}_audit.json" if audit_data else None,
                        'data': articles
                    }, f, ensure_ascii=False, indent=2)
            except IOError as e:
                _append_task_log(task_id, f"保存结果文件失败: {str(e)}")

            if audit_data:
                audit_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_audit.json")
                try:
                    with open(audit_file, 'w', encoding='utf-8') as f:
                        json.dump(audit_data, f, ensure_ascii=False, indent=2)
                    status_counts = audit_data.get('status_counts', {})
                    _append_task_log(task_id, f"审计文件已生成: {task_id}_audit.json")
                    _append_task_log(task_id, f"审计统计: {status_counts}")
                except IOError as e:
                    _append_task_log(task_id, f"保存审计文件失败: {str(e)}")
            
            _append_task_log(task_id, f"任务完成，共爬取 {len(articles)} 篇文章")
            
        else:
            # 爬取失败
            audit_data = result.get('audit') or {}
            with task_lock:
                crawl_tasks[task_id]['status'] = 'failed'
                crawl_tasks[task_id]['progress'] = 0
                crawl_tasks[task_id]['error'] = result.get('error', '未知错误')
                if audit_data:
                    crawl_tasks[task_id]['audit_file'] = f"{task_id}_audit.json"
            try:
                sqlite_db.update_crawl_task_status(
                    task_id,
                    'failed',
                    progress=0,
                    error_message=result.get('error', '未知错误')
                )
            except Exception:
                pass

            if audit_data:
                audit_file = os.path.join(CRAWL_RESULTS_DIR, f"{task_id}_audit.json")
                try:
                    with open(audit_file, 'w', encoding='utf-8') as f:
                        json.dump(audit_data, f, ensure_ascii=False, indent=2)
                    _append_task_log(task_id, f"失败任务审计文件已生成: {task_id}_audit.json")
                except IOError as e:
                    _append_task_log(task_id, f"保存失败任务审计文件失败: {str(e)}")
            if result.get('stats'):
                stats = result.get('stats') or {}
                _append_task_log(
                    task_id,
                    f"失败诊断: Playwright {stats.get('playwright_links', 0)} / "
                    f"网络JSON {stats.get('network_json_candidates', 0)} / "
                    f"接口响应 {stats.get('network_json_responses_checked', 0)} / "
                    f"JSONP {stats.get('network_jsonp_responses', 0)} / "
                    f"HTML错误 {stats.get('network_html_error_responses', 0)} / "
                    f"补充发现 {stats.get('supplemental_links', 0)}"
                )
                if stats.get('network_verification_signals'):
                    reasons = ', '.join(
                        sorted({item.get('reason', '') for item in stats.get('network_verification_signals', []) if item.get('reason')})
                    )
                    _append_task_log(task_id, f"失败原因提示: {reasons or '检测到验证/登录要求'}")
            
            _append_task_log(task_id, f"爬取失败: {result.get('error', '未知错误')}")
            
    except Exception as e:
        with task_lock:
            if task_id in crawl_tasks:
                crawl_tasks[task_id]['status'] = 'failed'
                crawl_tasks[task_id]['error'] = str(e)
        try:
            sqlite_db.update_crawl_task_status(task_id, 'failed', progress=0, error_message=str(e))
        except Exception:
            pass
        
        _append_task_log(task_id, f"任务执行异常: {str(e)}")

# 文章链接提取和下载功能
@app.route('/api/extract-article-links', methods=['POST'])
def extract_article_links():
    """Legacy external Firecrawl result endpoint; intentionally disabled."""
    return _legacy_firecrawl_disabled_response()


@app.route('/api/extract-articles-from-markdown', methods=['POST'])
def extract_articles_from_markdown():
    """直接从markdown内容中提取文章链接"""
    try:
        data = request.get_json(silent=True) or {}
        markdown_content = data.get('markdown_content')
        base_url = data.get('base_url', '')
        
        if not markdown_content:
            return jsonify({'success': False, 'error': '缺少markdown_content参数'}), 400
        
        # 创建文章链接提取器（启用智能验证）
        extractor = ArticleLinkExtractor(enable_smart_validation=True)
        
        # 提取链接
        links = extractor.extract_links_from_markdown(markdown_content, base_url)
        
        return jsonify({
            'success': True,
            'links_found': len(links),
            'links': links
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'提取失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/api/download-article', methods=['POST'])
def download_article():
    """下载单个文章内容"""
    try:
        data = request.get_json(silent=True) or {}
        article_url = data.get('url')
        
        if not article_url:
            return jsonify({'success': False, 'error': '缺少url参数'}), 400
        
        # 创建文章链接提取器（启用智能验证）
        extractor = ArticleLinkExtractor(enable_smart_validation=True)
        
        # 下载文章内容
        result = extractor.crawl_article_content(article_url)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'下载失败: {str(e)}'
        }), 500

# API路由：获取文章数据库统计信息
@app.route('/login')
def login_page():
    """登录页面"""
    return render_template('login.html')

# 用户管理页面路由（前端已隐藏入口）
@app.route('/user-management')
@login_required
def user_management_page():
    """用户管理页面"""
    return render_template('user_management.html')

# 退出登录路由
@app.route('/logout')
def logout_page():
    """退出登录"""
    # 获取session token
    token = request.cookies.get('session_token')
    if token:
        # 删除会话
        user_db.delete_session(token)
    
    # 创建响应并清除cookie
    response = redirect('/login')
    response.set_cookie('session_token', '', expires=0, httponly=True, samesite='Lax')
    return response

if __name__ == '__main__':
    try:
        # 启动前打印已注册的路由，便于排查404
        try:
            import logging
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
            logging.getLogger(__name__).info('已注册的路由:')
            for rule in app.url_map.iter_rules():
                logging.getLogger(__name__).info(f"  {rule.methods} -> {rule.rule}")
        except Exception:
            pass
        
        # 启动定时任务调度器
        print("🚀 启动定时任务调度器...")
        _ensure_schedule_thread()  # 使用完整的调度器逻辑
        
        print("📤 RAGFlow自动上传：已集成到文章入库流程（每100篇自动上传）")
        
        # 从环境变量读取端口，默认8003
        port = int(os.environ.get('FLASK_PORT', 8003))
        app.run(debug=False, host='0.0.0.0', port=port)
    except Exception as e:
        print(f"Flask 启动失败: {e}")
