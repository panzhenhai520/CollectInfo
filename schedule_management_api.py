#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
定时任务管理API模块
提供定时任务管理和持久化功能
"""

from flask import Blueprint, request, jsonify
from sqlite_database import sqlite_db
from datetime import datetime, timedelta
import json
import config
from logger_utils import log_error, log_info, log_warning, log_success
from url_validation_helper import normalize_task_url, validate_task_url_consistency, suggest_url_source
from utils import coerce_int, get_china_time
from ragflow_client import get_ragflow_client
from crawl_options import normalize_crawl_options

# 创建蓝图
schedule_management_bp = Blueprint('schedule_management_new', __name__, url_prefix='/api/schedule-management')

CRAWL_OPTION_KEYS = {
    'crawl_options', 'config', 'limit', 'depth',
    'wait_for_ms', 'wait_for', 'render_wait_ms',
    'max_pages', 'max_empty_pages',
    'detail_max_retries', 'max_extract_attempts',
    'date_range_priority', 'candidate_date_prefilter',
    'network_json_enabled', 'supplemental_enabled',
    'supplemental_html', 'supplemental_attributes', 'supplemental_structured',
    'supplemental_scripts', 'supplemental_static_pagination',
    'supplemental_feeds', 'supplemental_sitemaps',
    'supplemental_max_per_source', 'supplemental_max_sitemaps',
    'supplemental_max_static_pages',
    'proxy_enabled', 'use_proxy',
}


def normalize_schedule_list(value, min_value, max_value):
    """Normalize weekday/monthday config to a comma-separated string."""
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


def normalize_schedule_time(value):
    parts = str(value or '00:00:00').split(':')
    hour = coerce_int(parts[0] if len(parts) > 0 else 0, 0, 0, 23)
    minute = coerce_int(parts[1] if len(parts) > 1 else 0, 0, 0, 59)
    second = coerce_int(parts[2] if len(parts) > 2 else 0, 0, 0, 59)
    return f"{hour:02d}:{minute:02d}:{second:02d}"

def calculate_next_run_time(task_data: dict) -> datetime:
    """
    计算下次运行时间
    
    Args:
        task_data: 任务数据
        
    Returns:
        datetime: 下次运行时间
    """
    current_time = get_china_time()
    schedule_type = task_data.get('schedule_type', 'once')
    schedule_time = normalize_schedule_time(task_data.get('schedule_time'))
    schedule_day = task_data.get('schedule_day')

    def parse_time(value):
        parts = str(value or '00:00:00').split(':')
        return (
            coerce_int(parts[0] if len(parts) > 0 else 0, 0, 0, 23),
            coerce_int(parts[1] if len(parts) > 1 else 0, 0, 0, 59),
            coerce_int(parts[2] if len(parts) > 2 else 0, 0, 0, 59),
        )

    def parse_int_list(value, min_value, max_value):
        normalized = normalize_schedule_list(value, min_value, max_value)
        return [coerce_int(item, None, min_value, max_value) for item in normalized.split(',') if item]

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
        import calendar

        monthdays = parse_int_list(task_data.get('schedule_monthdays'), 1, 31)
        if not monthdays and schedule_day is not None:
            monthdays = [coerce_int(schedule_day, current_time.day, 1, 31)]
        if not monthdays:
            monthdays = [current_time.day]

        for days_ahead in range(0, 62):
            candidate = current_time + timedelta(days=days_ahead)
            max_day = calendar.monthrange(candidate.year, candidate.month)[1]
            valid_days = [min(day, max_day) for day in monthdays]
            if candidate.day not in valid_days:
                continue
            next_run = candidate.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if next_run > current_time:
                return next_run
        return (current_time + timedelta(days=30)).replace(hour=hour, minute=minute, second=second, microsecond=0)

    return current_time + timedelta(hours=1)

@schedule_management_bp.route('/tasks', methods=['GET'])
def get_tasks():
    """获取定时任务列表"""
    try:
        page = coerce_int(request.args.get('page'), 1, 1)
        per_page = coerce_int(request.args.get('per_page'), 20, 1, 500)
        is_active = request.args.get('is_active')
        
        # 转换is_active为布尔值
        if is_active is not None:
            is_active = is_active.lower() in ('true', '1', 'yes')
        
        tasks, total = sqlite_db.get_scheduled_tasks(page, per_page, is_active)
        
        return jsonify({
            'success': True,
            'tasks': tasks,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        })
    except Exception as e:
        log_error(e, "获取定时任务列表", {"page": page, "per_page": per_page, "is_active": is_active})
        return jsonify({
            'success': False,
            'error': f'获取任务列表失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/tasks', methods=['POST'])
def add_task():
    """添加新定时任务"""
    try:
        data = request.get_json(silent=True) or {}
        
        if not data or not data.get('task_name'):
            return jsonify({
                'success': False,
                'error': '缺少任务名称'
            }), 400

        if data.get('target_url'):
            data['target_url'] = normalize_task_url(data.get('target_url'))
        
        # 验证URL一致性（不阻止，只是警告）
        is_valid, error_msg, _ = validate_task_url_consistency(data)
        if not is_valid:
            log_warning(f"URL验证失败: {error_msg}", "添加定时任务")
            return jsonify({
                'success': False,
                'error': error_msg
            }), 400
        
        # 添加URL来源建议到日志
        suggestion = suggest_url_source(data)
        if '⚠️' in suggestion or 'ℹ️' in suggestion:
            log_info(suggestion, "添加定时任务")

        if 'schedule_weekdays' in data:
            data['schedule_weekdays'] = normalize_schedule_list(data.get('schedule_weekdays'), 0, 6)
        if 'schedule_monthdays' in data:
            data['schedule_monthdays'] = normalize_schedule_list(data.get('schedule_monthdays'), 1, 31)
        data['schedule_time'] = normalize_schedule_time(data.get('schedule_time'))
        data['days_limit'] = coerce_int(data.get('days_limit'), 7, 0, 3650)
        
        # 🔧 自动计算next_run时间
        try:
            next_run = calculate_next_run_time(data)
            data['next_run'] = next_run.strftime('%Y-%m-%d %H:%M:%S')
            log_info(f"自动计算next_run: {data['next_run']}", "添加定时任务")
        except Exception as calc_error:
            log_warning(f"计算next_run失败: {calc_error}", "添加定时任务")
        
        # Ragflow 知识库（可选）
        data['ragflow_kb_id'] = data.get('ragflow_kb_id') or None
        data['mode'] = 'standard'
        config_data = data.get('config') if isinstance(data.get('config'), dict) else {}
        config_data.update(normalize_crawl_options(data))
        config_data.setdefault('mode', 'standard')
        if data.get('limit') is not None:
            config_data['limit'] = data.get('limit')
        if data.get('depth') is not None:
            config_data['depth'] = data.get('depth')
        data['config'] = config_data

        task_id = sqlite_db.insert_scheduled_task(data)
        
        if task_id:
            return jsonify({
                'success': True,
                'task_id': task_id,
                'message': '定时任务添加成功'
            })
        else:
            return jsonify({
                'success': False,
                'error': '任务添加失败'
            }), 400
            
    except Exception as e:
        log_error(e, "添加定时任务", {"data": data})
        return jsonify({
            'success': False,
            'error': f'添加任务失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    """更新定时任务"""
    try:
        data = request.get_json(silent=True) or {}
        
        if not data:
            return jsonify({
                'success': False,
                'error': '缺少更新数据'
            }), 400

        if data.get('target_url'):
            data['target_url'] = normalize_task_url(data.get('target_url'))

        if 'schedule_weekdays' in data:
            data['schedule_weekdays'] = normalize_schedule_list(data.get('schedule_weekdays'), 0, 6)
        if 'schedule_monthdays' in data:
            data['schedule_monthdays'] = normalize_schedule_list(data.get('schedule_monthdays'), 1, 31)
        if 'schedule_time' in data:
            data['schedule_time'] = normalize_schedule_time(data.get('schedule_time'))
        if 'days_limit' in data:
            data['days_limit'] = coerce_int(data.get('days_limit'), 7, 0, 3650)
        
        # Ragflow 知识库（可选）
        if 'ragflow_kb_id' in data and not data.get('ragflow_kb_id'):
            data['ragflow_kb_id'] = None
        if 'mode' in data:
            data['mode'] = 'standard'
        if any(key in data for key in CRAWL_OPTION_KEYS):
            config_data = data.get('config') if isinstance(data.get('config'), dict) else {}
            config_data.update(normalize_crawl_options(data))
            config_data.setdefault('mode', 'standard')
            if data.get('limit') is not None:
                config_data['limit'] = data.get('limit')
            if data.get('depth') is not None:
                config_data['depth'] = data.get('depth')
            data['config'] = config_data

        # 🔧 如果修改了schedule_time，自动重新计算next_run
        if any(key in data for key in ('schedule_time', 'schedule_type', 'schedule_day', 'schedule_weekdays', 'schedule_monthdays')):
            try:
                # 获取现有任务信息
                current_task = sqlite_db.get_scheduled_task(task_id)
                
                if current_task:
                    # 合并现有数据和更新数据
                    merged_data = {**current_task, **data}
                    next_run = calculate_next_run_time(merged_data)
                    data['next_run'] = next_run.strftime('%Y-%m-%d %H:%M:%S')
                    log_info(f"自动重新计算next_run: {data['next_run']}", f"更新任务{task_id}")
            except Exception as calc_error:
                log_warning(f"计算next_run失败: {calc_error}", f"更新任务{task_id}")
        
        success = sqlite_db.update_scheduled_task(task_id, data)
        
        if success:
            return jsonify({
                'success': True,
                'message': '任务更新成功'
            })
        else:
            return jsonify({
                'success': False,
                'error': '任务更新失败'
            }), 500
            
    except Exception as e:
        log_error(e, f"更新定时任务 ID:{task_id}", {"data": data})
        return jsonify({
            'success': False,
            'error': f'更新任务失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/ragflow/datasets', methods=['GET'])
def list_ragflow_datasets():
    """获取 Ragflow 知识库列表"""
    try:
        page = coerce_int(request.args.get('page'), 1, 1)
        page_size = coerce_int(request.args.get('page_size'), 50, 1, 200)
        keywords = request.args.get('keywords', '').strip()

        if not config.is_ragflow_configured():
            return jsonify({
                'success': True,
                'configured': False,
                'upload_enabled': False,
                'datasets': [],
                'message': 'AI Server Parameters RAGFLOW_BASE_URL 和 RAGFLOW_API_KEY are not configured'
            })

        client = get_ragflow_client()
        datasets = client.list_datasets(page=page, page_size=page_size, keywords=keywords)
        upload_enabled = bool(getattr(config, 'RAGFLOW_UPLOAD_ENABLED', True))

        return jsonify({
            'success': True,
            'configured': True,
            'upload_enabled': upload_enabled,
            'message': '' if upload_enabled else 'RAGFlow 已连接，但 RAGFLOW_UPLOAD_ENABLED 已关闭',
            'datasets': datasets
        })
    except Exception as e:
        log_error(e, "获取Ragflow知识库")
        return jsonify({
            'success': False,
            'configured': config.is_ragflow_configured(),
            'upload_enabled': bool(getattr(config, 'RAGFLOW_UPLOAD_ENABLED', False)),
            'datasets': [],
            'error': f'获取Ragflow知识库失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    """删除定时任务"""
    try:
        success = sqlite_db.delete_scheduled_task(task_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': '任务删除成功'
            })
        else:
            return jsonify({
                'success': False,
                'error': '任务删除失败'
            }), 500
            
    except Exception as e:
        log_error(e, f"删除定时任务 ID:{task_id}")
        return jsonify({
            'success': False,
            'error': f'删除任务失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/tasks/<int:task_id>/toggle', methods=['POST'])
def toggle_task(task_id):
    """切换任务启用/禁用状态"""
    try:
        data = request.get_json(silent=True) or {}
        is_active = data.get('is_active', True)
        
        success = sqlite_db.update_scheduled_task(task_id, {'is_active': is_active})
        
        if success:
            return jsonify({
                'success': True,
                'message': f'任务已{"启用" if is_active else "禁用"}'
            })
        else:
            return jsonify({
                'success': False,
                'error': '状态更新失败'
            }), 500
            
    except Exception as e:
        log_error(e, f"切换任务状态 ID:{task_id}", {"is_active": data.get('is_active')})
        return jsonify({
            'success': False,
            'error': f'切换状态失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/tasks/<int:task_id>/run-stats', methods=['POST'])
def update_run_stats(task_id):
    """更新任务运行统计"""
    try:
        data = request.get_json(silent=True) or {}
        success = data.get('success', True)
        next_run = data.get('next_run')
        
        # 解析next_run时间
        if next_run and isinstance(next_run, str):
            try:
                next_run = datetime.fromisoformat(next_run.replace('Z', '+00:00'))
            except:
                next_run = None
        
        result = sqlite_db.update_scheduled_task_run_stats(
            task_id, 
            success, 
            next_run=next_run
        )
        
        if result:
            return jsonify({
                'success': True,
                'message': '统计信息更新成功'
            })
        else:
            return jsonify({
                'success': False,
                'error': '统计信息更新失败'
            }), 500
            
    except Exception as e:
        log_error(e, f"更新任务运行统计 ID:{task_id}", {"data": data})
        return jsonify({
            'success': False,
            'error': f'更新统计信息失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/statistics', methods=['GET'])
def get_schedule_statistics():
    """获取定时任务统计信息"""
    try:
        tasks, total = sqlite_db.get_scheduled_tasks(1, 10000)
        
        active_count = sum(1 for task in tasks if task.get('is_active'))
        
        # 按任务类型统计
        task_types = {}
        for task in tasks:
            task_type = task.get('task_type', 'unknown')
            task_types[task_type] = task_types.get(task_type, 0) + 1
        
        # 按调度类型统计
        schedule_types = {}
        for task in tasks:
            schedule_type = task.get('schedule_type', 'unknown')
            schedule_types[schedule_type] = schedule_types.get(schedule_type, 0) + 1
        
        # 统计运行情况
        total_runs = sum(task.get('total_runs', 0) for task in tasks)
        success_runs = sum(task.get('success_runs', 0) for task in tasks)
        success_rate = (success_runs / total_runs * 100) if total_runs > 0 else 0
        
        return jsonify({
            'success': True,
            'statistics': {
                'total_tasks': total,
                'active_tasks': active_count,
                'inactive_tasks': total - active_count,
                'task_types': task_types,
                'schedule_types': schedule_types,
                'total_runs': total_runs,
                'success_runs': success_runs,
                'failed_runs': total_runs - success_runs,
                'success_rate': round(success_rate, 2)
            }
        })
    except Exception as e:
        log_error(e, "获取定时任务统计信息")
        return jsonify({
            'success': False,
            'error': f'获取统计信息失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/execution-history', methods=['GET'])
def get_execution_history():
    """获取任务执行历史（暂时返回空，后续可扩展）"""
    try:
        return jsonify({
            'success': True,
            'history': [],
            'total': 0
        })
    except Exception as e:
        log_error(e, "获取任务执行历史")
        return jsonify({
            'success': False,
            'error': f'获取执行历史失败: {str(e)}'
        }), 500

@schedule_management_bp.route('/execution-history', methods=['POST'])
def record_execution():
    """记录任务执行"""
    try:
        data = request.get_json(silent=True) or {}
        
        if not data:
            return jsonify({
                'success': False,
                'error': '缺少执行数据'
            }), 400
        
        execution_id = sqlite_db.insert_task_execution(data)
        
        if execution_id:
            return jsonify({
                'success': True,
                'execution_id': execution_id,
                'message': '执行记录已保存'
            })
        else:
            return jsonify({
                'success': False,
                'error': '保存执行记录失败'
            }), 500
            
    except Exception as e:
        log_error(e, "记录任务执行", {"data": data})
        return jsonify({
            'success': False,
            'error': f'记录执行失败: {str(e)}'
        }), 500


@schedule_management_bp.route('/tasks/<int:task_id>/stop', methods=['POST'])
def stop_running_task(task_id):
    """停止正在运行的爬取任务"""
    try:
        from scheduler import scheduler
        
        result = scheduler.stop_task(task_id)
        
        if result.get('success'):
            log_info(f"任务 {task_id} 已停止", "停止任务")
            return jsonify(result)
        else:
            return jsonify(result), 404
            
    except Exception as e:
        log_error(e, f"停止任务 ID:{task_id}")
        return jsonify({
            'success': False,
            'error': f'停止任务失败: {str(e)}'
        }), 500


@schedule_management_bp.route('/running-tasks', methods=['GET'])
def get_running_tasks():
    """获取所有正在运行的任务"""
    try:
        from scheduler import scheduler
        
        running_tasks = scheduler.get_running_tasks()
        stats = scheduler.get_concurrent_stats()
        
        return jsonify({
            'success': True,
            'running_tasks': running_tasks,
            'stats': stats
        })
        
    except Exception as e:
        log_error(e, "获取运行中任务")
        return jsonify({
            'success': False,
            'error': f'获取运行中任务失败: {str(e)}'
        }), 500


@schedule_management_bp.route('/all-keywords', methods=['GET'])
def get_all_keywords():
    """获取系统中所有已配置的关键词（用于批量建任务时快速选用）"""
    try:
        seen = set()
        result = []

        def _add_kw(raw):
            for kw in str(raw or '').replace('，', ',').split(','):
                kw = kw.strip()
                if kw and kw not in seen:
                    seen.add(kw)
                    result.append(kw)

        # 从 scheduled_tasks 收集
        tasks, _ = sqlite_db.get_scheduled_tasks(page=1, per_page=500)
        for t in tasks:
            _add_kw(t.get('keywords', ''))

        # 从 managed_urls 收集
        urls, _ = sqlite_db.get_managed_urls(page=1, per_page=500, is_active=True)
        for u in urls:
            _add_kw(u.get('keywords', ''))

        return jsonify({'success': True, 'keywords': sorted(result)})
    except Exception as e:
        log_error(e, "获取系统关键词")
        return jsonify({'success': False, 'error': str(e)}), 500


@schedule_management_bp.route('/urls-grouped', methods=['GET'])
def get_urls_grouped():
    """获取所有活跃 managed_url 按分类分组（用于批量建任务时选择 URL）"""
    try:
        categories = sqlite_db.get_categories(is_active=None)
        all_urls, _ = sqlite_db.get_managed_urls(page=1, per_page=1000, is_active=True)

        # 按 category_id 分桶
        buckets = {}
        for u in all_urls:
            cid = u.get('category_id') or 0
            buckets.setdefault(cid, []).append({
                'id': u['id'],
                'name': u.get('name') or u.get('url', ''),
                'url': u.get('url', ''),
            })

        # 构造分类列表：找不到对应分类名的桶合并到 cid=0（未分类）
        cat_map = {c['id']: c['name'] for c in categories}
        for cid in list(buckets.keys()):
            if cid and cid not in cat_map:
                buckets.setdefault(0, []).extend(buckets.pop(cid))

        groups = []
        for cid, urls in sorted(buckets.items()):
            groups.append({
                'category_id': cid,
                'category_name': cat_map.get(cid, '未分类') if cid else '未分类',
                'urls': urls,
            })

        return jsonify({'success': True, 'groups': groups})
    except Exception as e:
        log_error(e, "获取分组URL")
        return jsonify({'success': False, 'error': str(e)}), 500


@schedule_management_bp.route('/batch-create', methods=['POST'])
def batch_create_tasks():
    """批量创建定时任务（按 stagger_minutes 错峰）"""
    try:
        data = request.get_json(silent=True) or {}
        url_ids = data.get('url_ids', [])
        if not url_ids:
            return jsonify({'success': False, 'error': '请至少选择一个 URL'}), 400

        task_name_prefix = (data.get('task_name_prefix') or '').strip()
        if not task_name_prefix:
            return jsonify({'success': False, 'error': '请填写任务名称前缀'}), 400

        keywords = (data.get('keywords') or '').strip()
        schedule_type = data.get('schedule_type', 'daily')
        schedule_time = normalize_schedule_time(data.get('schedule_time', '08:00:00'))
        schedule_weekdays = normalize_schedule_list(data.get('schedule_weekdays', ''), 0, 6)
        schedule_monthdays = normalize_schedule_list(data.get('schedule_monthdays', ''), 1, 31)
        cron_expression = data.get('cron_expression', '')
        stagger_minutes = max(0, int(data.get('stagger_minutes', 0) or 0))
        on_overlap = data.get('on_overlap', 'skip')
        task_timeout_hours = max(1, int(data.get('task_timeout_hours', 24) or 24))
        days_limit = coerce_int(data.get('days_limit', 7), 7, 0, 3650)
        ragflow_kb_id = data.get('ragflow_kb_id') or None
        task_type = data.get('task_type', 'crawl')
        config_extra = data.get('config') if isinstance(data.get('config'), dict) else {}

        created_ids = []
        errors = []
        for idx, uid in enumerate(url_ids):
            url_row = sqlite_db.get_managed_url_by_id(uid)
            if not url_row:
                errors.append(f'URL ID {uid} 不存在')
                continue

            url_name = url_row.get('name') or url_row.get('url', '')
            task_name = f"{task_name_prefix}{url_name}" if url_name else f"{task_name_prefix}{uid}"

            # 错峰：base_time + idx * stagger_minutes
            base_task = {
                'task_name': task_name,
                'task_type': task_type,
                'url_id': uid,
                'target_url': '',
                'schedule_type': schedule_type,
                'schedule_time': schedule_time,
                'schedule_weekdays': schedule_weekdays,
                'schedule_monthdays': schedule_monthdays,
                'cron_expression': cron_expression,
                'keywords': keywords,
                'is_active': True,
                'ragflow_kb_id': ragflow_kb_id,
                'days_limit': days_limit,
                'config': {
                    'on_overlap': on_overlap,
                    'task_timeout_hours': task_timeout_hours,
                    **config_extra,
                },
            }

            # 计算错峰后的 next_run
            try:
                base_next_run = calculate_next_run_time(base_task)
                from datetime import timedelta
                staggered_next_run = base_next_run + timedelta(minutes=idx * stagger_minutes)
                base_task['next_run'] = staggered_next_run.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                pass

            task_id = sqlite_db.insert_scheduled_task(base_task)
            if task_id:
                created_ids.append(task_id)
            else:
                errors.append(f'创建任务失败: {task_name}')

        return jsonify({
            'success': True,
            'created': len(created_ids),
            'created_ids': created_ids,
            'errors': errors,
            'message': f'成功创建 {len(created_ids)} 个任务' + (f'，{len(errors)} 个失败' if errors else '')
        })
    except Exception as e:
        log_error(e, "批量创建定时任务")
        return jsonify({'success': False, 'error': str(e)}), 500


@schedule_management_bp.route('/concurrent-stats', methods=['GET'])
def get_concurrent_stats():
    """获取并发统计信息"""
    try:
        from scheduler import scheduler
        
        stats = scheduler.get_concurrent_stats()
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        log_error(e, "获取并发统计")
        return jsonify({
            'success': False,
            'error': f'获取并发统计失败: {str(e)}'
        }), 500

