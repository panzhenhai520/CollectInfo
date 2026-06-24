#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
调度器模块
处理定时任务的调度逻辑
支持多进程并发执行和任务停止
"""

import time
import threading
import traceback
import json
import re
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Optional
from urllib.parse import urlparse
from utils import coerce_int, get_china_time
from ragflow_client import get_ragflow_client
from concurrent.futures import ThreadPoolExecutor, Future
from crawl_options import normalize_crawl_options
from url_validation_helper import normalize_task_url


def _read_int_env(name: str, default: int, min_value: int = None, max_value: int = None) -> int:
    """Read a bounded integer environment setting."""
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _normalize_limit_value(value):
    """Normalize user/config article limits; invalid and infinite values mean no limit."""
    if value in (None, ''):
        return float('inf')
    try:
        if isinstance(value, str) and value.strip().lower() in ('inf', 'infinity', 'unlimited', 'none', '无限制'):
            return float('inf')
        limit = int(float(value))
        return limit if limit > 0 else float('inf')
    except Exception:
        return float('inf')


def _coerce_days_limit(value, default: int = 7) -> int:
    """Normalize date-window days; 0 means no date limit."""
    if value in (None, ''):
        return default
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return default


class TaskScheduler:
    """任务调度器 - 支持多进程并发执行"""
    
    def __init__(self):
        self.schedule_thread = None
        self.running = False
        self.check_interval = 60  # 检查间隔（秒）
        self.startup_grace_period = 120  # 启动宽限期（秒），避免执行启动前积压的任务
        self.startup_time = None  # 启动时间
        self.startup_skip_logged = set()  # 记录已提示过的跳过任务，避免重复提示
        
        # 多任务并发支持。Playwright 是重资源任务，默认值偏保守，避免批量定时任务同时打爆浏览器/内存/数据库。
        self.max_concurrent_tasks = _read_int_env('CRAWL_SCHEDULER_MAX_CONCURRENT', 4, 1, 32)
        self.max_tasks_per_domain = _read_int_env('CRAWL_SCHEDULER_MAX_PER_DOMAIN', 1, 1, 8)
        self.retry_attempts = _read_int_env('CRAWL_SCHEDULER_RETRIES', 2, 0, 5)
        self.retry_backoff_seconds = _read_int_env('CRAWL_SCHEDULER_RETRY_BACKOFF', 20, 0, 600)
        self.domain_cooldown_seconds = _read_int_env('CRAWL_SCHEDULER_DOMAIN_COOLDOWN', 5, 0, 300)
        self.task_timeout_seconds = _read_int_env('CRAWL_SCHEDULER_TASK_TIMEOUT', 7200, 300, 86400)
        self.completed_task_retention_seconds = _read_int_env('CRAWL_SCHEDULER_COMPLETED_RETENTION', 3600, 60, 86400)
        self.executor: Optional[ThreadPoolExecutor] = None
        self._running_tasks: Dict[str, dict] = {}  # {task_key: {future, task_info, stop_flag}}
        self._task_lock = threading.Lock()
        self._domain_running_counts: Dict[str, int] = {}
        self._domain_last_finished_at: Dict[str, float] = {}
        self._throttle_log_times: Dict[str, float] = {}
        self._stats = {
            'submitted': 0,
            'completed': 0,
            'failed': 0,
            'retries': 0,
            'timeouts': 0,
            'throttled_global': 0,
            'throttled_domain': 0,
            'skipped_duplicate': 0
        }
        
        # 🔥 登录锁机制 - 防止多任务同时登录同一网站
        self._login_locks: Dict[str, threading.Lock] = {}  # {domain: Lock}
        self._login_locks_lock = threading.Lock()  # 保护_login_locks字典的锁
        
    def start(self):
        """启动调度器"""
        if self.schedule_thread is None or not self.schedule_thread.is_alive():
            self.running = True
            self.startup_time = get_china_time()  # 记录启动时间
            
            # 启动线程池执行器
            if self.executor is None:
                self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_tasks)
                print(f"🔧 线程池已启动 (最大并发: {self.max_concurrent_tasks})")
                print(f"   单域名并发: {self.max_tasks_per_domain} | 失败重试: {self.retry_attempts}次 | 域名冷却: {self.domain_cooldown_seconds}秒 | 任务超时: {self.task_timeout_seconds}秒")
            
            self.schedule_thread = threading.Thread(target=self._run_schedule_loop, daemon=True)
            self.schedule_thread.start()
            print("✅ 定时任务调度器已启动")
            print(f"   🛡️ 启动宽限期: {self.startup_grace_period}秒（避免执行刚错过的任务）")
    
    def stop(self):
        """停止调度器"""
        self.running = False
        
        # 停止所有运行中的任务
        with self._task_lock:
            for task_key, task_data in self._running_tasks.items():
                task_data['stop_flag'] = True
                task_data['stop_reason'] = 'manual'
                print(f"🛑 发送停止信号: {task_key}")
        
        # 关闭线程池
        if self.executor:
            try:
                # Python 3.9+ 支持 cancel_futures 参数
                self.executor.shutdown(wait=True, cancel_futures=True)
            except TypeError:
                # Python 3.8 及更早版本
                self.executor.shutdown(wait=True)
            self.executor = None
            print("🔧 线程池已关闭")
        
        if self.schedule_thread:
            self.schedule_thread.join(timeout=5)
            print("⏹️  定时任务调度器已停止")
    
    def stop_task(self, task_id: int) -> dict:
        """
        停止指定的爬取任务
        
        Args:
            task_id: 定时任务ID或执行ID
            
        Returns:
            dict: 停止结果
        """
        with self._task_lock:
            # 查找匹配的任务
            stopped_count = 0
            for task_key, task_data in self._running_tasks.items():
                if str(task_id) in task_key or task_data.get('schedule_id') == task_id:
                    if not task_data.get('stop_flag'):
                        task_data['stop_flag'] = True
                        task_data['stop_reason'] = 'manual'
                        stopped_count += 1
                        print(f"🛑 已发送停止信号到任务: {task_key}")
            
            if stopped_count > 0:
                return {'success': True, 'message': f'已停止 {stopped_count} 个任务', 'stopped_count': stopped_count}
            else:
                return {'success': False, 'message': '未找到运行中的任务'}
    
    def get_running_tasks(self) -> list:
        """
        获取所有运行中的任务
        
        Returns:
            list: 运行中的任务列表
        """
        with self._task_lock:
            running = []
            for task_key, task_data in self._running_tasks.items():
                if not task_data.get('completed'):
                    running.append({
                        'task_key': task_key,
                        'schedule_id': task_data.get('schedule_id'),
                        'task_name': task_data.get('task_name'),
                        'domain': task_data.get('domain'),
                        'started_at': task_data.get('started_at'),
                        'attempt': task_data.get('attempt', 0),
                        'stop_flag': task_data.get('stop_flag', False)
                    })
            return running
    
    def get_concurrent_stats(self) -> dict:
        """
        获取并发统计信息
        
        Returns:
            dict: 统计信息
        """
        with self._task_lock:
            running_count = sum(1 for t in self._running_tasks.values() if not t.get('completed'))
            return {
                'max_concurrent': self.max_concurrent_tasks,
                'max_per_domain': self.max_tasks_per_domain,
                'running_count': running_count,
                'available_slots': max(0, self.max_concurrent_tasks - running_count),
                'domain_running': dict(self._domain_running_counts),
                'stats': dict(self._stats)
            }

    def _cleanup_completed_tasks(self):
        """清理已完成的内存任务记录，避免长期定时运行时内存持续增长。"""
        cutoff = time.time() - self.completed_task_retention_seconds
        with self._task_lock:
            expired_keys = [
                task_key
                for task_key, task_data in self._running_tasks.items()
                if task_data.get('completed') and task_data.get('completed_epoch', 0) < cutoff
            ]
            for task_key in expired_keys:
                self._running_tasks.pop(task_key, None)

            # 顺手清理限流日志的旧 key，防止大量域名长期积累。
            log_cutoff = time.time() - max(3600, self.completed_task_retention_seconds)
            for key, logged_at in list(self._throttle_log_times.items()):
                if logged_at < log_cutoff:
                    self._throttle_log_times.pop(key, None)
            
            if expired_keys:
                print(f"已清理 {len(expired_keys)} 条完成任务内存记录")

    def _mark_timed_out_tasks(self):
        """给超过运行时长的任务打停止标记；实际退出依赖爬取流程的超时点/停止检查点。"""
        now = time.time()
        with self._task_lock:
            for task_key, task_data in self._running_tasks.items():
                if task_data.get('completed') or task_data.get('stop_flag'):
                    continue
                started_epoch = task_data.get('started_epoch')
                if not started_epoch:
                    continue
                elapsed = now - started_epoch
                timeout_seconds = int(task_data.get('timeout_seconds') or self.task_timeout_seconds)
                if elapsed > timeout_seconds:
                    task_data['stop_flag'] = True
                    task_data['stop_reason'] = 'timeout'
                    task_data['timeout_flagged_at'] = get_china_time().isoformat()
                    self._stats['timeouts'] += 1
                    print(f"任务 {task_key} 已运行 {int(elapsed)} 秒，超过 {timeout_seconds} 秒，已标记停止")

    def _get_stop_error_message(self, task_key: str) -> str:
        with self._task_lock:
            reason = self._running_tasks.get(task_key, {}).get('stop_reason')
        if reason == 'timeout':
            return '任务执行超时'
        return '任务被用户停止'

    def _extract_audit_data(self, result: dict) -> dict:
        """从调度结果中提取完整审计数据，兼容多层包装。"""
        if not isinstance(result, dict):
            return {}
        audit = result.get('audit')
        if isinstance(audit, dict) and audit:
            return audit
        data = result.get('data')
        if isinstance(data, dict):
            audit = data.get('audit')
            if isinstance(audit, dict) and audit:
                return audit
        nested = result.get('result')
        if isinstance(nested, dict):
            return self._extract_audit_data(nested)
        return {}

    def _persist_audit_file(self, crawl_task_id: str, result: dict) -> dict:
        """将完整审计落盘，数据库只保留文件名和统计摘要。"""
        audit_data = self._extract_audit_data(result)
        if not audit_data or not crawl_task_id:
            return {}

        try:
            try:
                import config
                results_dir = getattr(config, 'CRAWL_RESULTS_DIR', 'crawl_results')
            except Exception:
                results_dir = os.getenv('CRAWL_RESULTS_DIR', 'crawl_results')

            os.makedirs(results_dir, exist_ok=True)
            audit_filename = f"{crawl_task_id}_audit.json"
            audit_path = os.path.join(results_dir, audit_filename)
            with open(audit_path, 'w', encoding='utf-8') as f:
                json.dump(audit_data, f, ensure_ascii=False, indent=2)

            status_counts = audit_data.get('status_counts', {})
            print(f"审计文件已生成: {audit_filename} | {status_counts}")
            return {
                'audit_file': audit_filename,
                'audit_path': audit_path,
                'audit_status_counts': status_counts,
                'audit_total_candidates': audit_data.get('total_candidates_before_db_dedupe'),
                'audit_remaining_candidates': audit_data.get('total_candidates_after_db_dedupe'),
                'recall_status': audit_data.get('recall_status'),
                'recall_risk_count': audit_data.get('recall_risk_count'),
                'recall_risk_reasons': audit_data.get('recall_risk_reasons'),
                'recall_health_score': audit_data.get('recall_health_score'),
                'recall_health_level': audit_data.get('recall_health_level'),
                'recall_health_label': audit_data.get('recall_health_label'),
                'recall_health_reasons': audit_data.get('recall_health_reasons'),
            }
        except Exception as e:
            print(f"保存审计文件失败: {e}")
            return {'audit_save_error': str(e)}

    def _compact_result_summary(self, result: dict, audit_meta: dict = None) -> dict:
        """生成适合写入执行历史的轻量摘要，避免把全文和完整 audit 塞进数据库。"""
        if not isinstance(result, dict):
            return {'message': str(result)[:500]}

        data = result.get('data') if isinstance(result.get('data'), dict) else {}
        stats = {}
        if isinstance(data.get('stats'), dict):
            stats.update(data.get('stats'))
        if isinstance(result.get('stats'), dict):
            stats.update(result.get('stats'))

        summary = {
            'success': result.get('success'),
            'message': result.get('message') or data.get('message') or result.get('error'),
            'articles_found': result.get('articles_found') or stats.get('success', 0),
            'stats': stats,
        }

        for key in (
            'attempts',
            'retry_errors',
            'needs_review',
            'recall_status',
            'recall_risk_count',
            'recall_risk_reasons',
            'recall_health_score',
            'recall_health_level',
            'recall_health_label',
            'recall_health_reasons',
        ):
            if key in result:
                summary[key] = result.get(key)
            elif key in data:
                summary[key] = data.get(key)

        if audit_meta:
            summary.update(audit_meta)

        return summary
    
    def _run_schedule_loop(self):
        """调度循环"""
        from sqlite_database import sqlite_db
        
        print("🔄 定时任务调度循环开始运行")
        
        while self.running:
            try:
                current_time = get_china_time()
                print(f"⏰ [{current_time.strftime('%Y-%m-%d %H:%M:%S')}] 检查待执行的定时任务...")
                
                # 查询所有启用的定时任务
                tasks, total = sqlite_db.get_scheduled_tasks(
                    page=1, 
                    per_page=1000, 
                    is_active=True
                )
                
                if total > 0:
                    print(f"📋 找到 {total} 个启用的定时任务")
                    
                    for task in tasks:
                        try:
                            # 检查任务是否该执行
                            if self._should_execute(task, current_time):
                                _disp = task.get('url_display_name') or task.get('task_name')
                                print(f"🚀 准备执行定时任务: {_disp} (ID: {task.get('id')})")
                                self._execute_task(task, current_time)
                        except Exception as e:
                            print(f"❌ 执行任务 {task.get('id')} 时出错: {e}")
                            traceback.print_exc()
                else:
                    print("📭 没有找到启用的定时任务")
                
                self._mark_timed_out_tasks()
                self._cleanup_completed_tasks()

                # 等待下次检查
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"❌ 调度器错误: {e}")
                traceback.print_exc()
                time.sleep(self.check_interval)
    
    def _should_execute(self, task: dict, current_time: datetime) -> bool:
        """
        判断任务是否应该执行
        
        Args:
            task: 任务信息
            current_time: 当前时间
            
        Returns:
            bool: 是否应该执行
        """
        # 检查任务是否启用
        if not task.get('is_active'):
            return False
        
        # 检查next_run时间
        next_run_str = task.get('next_run')
        if not next_run_str:
            # 如果没有next_run，根据调度类型计算
            return self._calculate_should_run_on_first_time(task, current_time)
        
        try:
            # 解析next_run时间
            if isinstance(next_run_str, str):
                # 处理不同的时间格式
                next_run_str_clean = next_run_str.replace('Z', '+00:00')
                # 如果格式是 "YYYY-MM-DD HH:MM:SS"（SQLite格式），转换为ISO格式
                if ' ' in next_run_str_clean and 'T' not in next_run_str_clean:
                    next_run_str_clean = next_run_str_clean.replace(' ', 'T')
                
                next_run = datetime.fromisoformat(next_run_str_clean)
                # 移除时区信息以便比较
                if next_run.tzinfo:
                    next_run = next_run.replace(tzinfo=None)
            else:
                next_run = next_run_str
            
            # 如果当前时间已经超过或等于next_run时间，则应该执行
            should_run = current_time >= next_run
            
            # 🔥 对于每周/每月任务，必须检查今天是否是配置的执行日
            if should_run:
                schedule_type = task.get('schedule_type', 'once')
                
                # 每周任务：检查星期几
                if schedule_type == 'weekly':
                    weekdays_str = task.get('schedule_weekdays', '')
                    if weekdays_str:
                        weekdays = [int(d.strip()) for d in weekdays_str.split(',') if d.strip().isdigit()]
                        if weekdays and current_time.weekday() not in weekdays:
                            print(f"  ⏭️  任务 {task.get('task_name')} 跳过: 今天不是配置的执行日 (今天周{current_time.weekday()+1}, 配置={weekdays})")
                            return False
                
                # 每月任务：检查日期
                elif schedule_type == 'monthly':
                    monthdays_str = task.get('schedule_monthdays', '')
                    if monthdays_str:
                        monthdays = [int(d.strip()) for d in monthdays_str.split(',') if d.strip().isdigit()]
                        if monthdays and current_time.day not in monthdays:
                            print(f"  ⏭️  任务 {task.get('task_name')} 跳过: 今天不是配置的执行日 (今天{current_time.day}号, 配置={monthdays})")
                            return False
            
            # 🛡️ 启动宽限期检查：避免服务刚启动就执行错过的任务
            if should_run and self.startup_time:
                time_since_startup = (current_time - self.startup_time).total_seconds()
                time_missed = (current_time - next_run).total_seconds()
                
                # 如果任务错过时间超过宽限期，且服务刚启动不久，则跳过
                if time_since_startup < 120 and time_missed > self.startup_grace_period:
                    task_id = task.get('id')
                    # 只在首次提示，避免每分钟都重复显示
                    if task_id not in self.startup_skip_logged:
                        self.startup_skip_logged.add(task_id)
                        print(f"  ⏭️  任务 {task.get('task_name')} 跳过: 服务启动时任务已错过 {int(time_missed//60)} 分钟")
                        print(f"     下次执行: {task.get('next_run', '未知')}")
                    return False
            
            # 添加调试信息
            if should_run:
                print(f"  ✅ 任务 {task.get('task_name')} 应该执行: 当前={current_time}, next_run={next_run}")
            
            return should_run
            
        except Exception as e:
            print(f"⚠️  解析next_run时间失败: {e}, next_run_str={next_run_str}")
            traceback.print_exc()
            return False
    
    def _calculate_should_run_on_first_time(self, task: dict, current_time: datetime) -> bool:
        """
        首次运行时判断是否应该立即执行 - 支持每周/每月
        
        Args:
            task: 任务信息
            current_time: 当前时间
            
        Returns:
            bool: 是否应该执行
        """
        schedule_type = task.get('schedule_type', 'once')
        schedule_time = task.get('schedule_time')
        
        # 对于"仅执行一次"类型，如果没有last_run，可以立即执行
        if schedule_type == 'once':
            return task.get('last_run') is None
        
        # 对于周期性任务，需要检查时间
        if schedule_time:
            try:
                # 解析schedule_time (HH:MM:SS格式)
                time_parts = schedule_time.split(':')
                scheduled_hour = int(time_parts[0])
                scheduled_minute = int(time_parts[1])
                scheduled_second = int(time_parts[2]) if len(time_parts) > 2 else 0
                
                # 检查时间是否匹配
                time_match = (current_time.hour == scheduled_hour and 
                             current_time.minute == scheduled_minute)
                
                if not time_match:
                    return False
                
                # 🔥 对于每周任务，还需检查星期几
                if schedule_type == 'weekly':
                    weekdays_str = task.get('schedule_weekdays', '')
                    if weekdays_str:
                        weekdays = [int(d.strip()) for d in weekdays_str.split(',') if d.strip().isdigit()]
                        if weekdays and current_time.weekday() not in weekdays:
                            return False
                
                # 🔥 对于每月任务，还需检查日期
                elif schedule_type == 'monthly':
                    monthdays_str = task.get('schedule_monthdays', '')
                    if monthdays_str:
                        monthdays = [int(d.strip()) for d in monthdays_str.split(',') if d.strip().isdigit()]
                        if monthdays and current_time.day not in monthdays:
                            return False
                
                return True
                
            except Exception as e:
                print(f"⚠️  解析schedule_time失败: {e}")
        
        return False

    def _parse_task_config(self, task: dict) -> dict:
        """解析任务配置，兼容数据库里的 JSON 字符串和 dict。"""
        task_config = task.get('config')
        if isinstance(task_config, str):
            try:
                parsed = json.loads(task_config)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return task_config if isinstance(task_config, dict) else {}

    def _get_config_int(self, task: dict, keys, default: int, min_value: int = None, max_value: int = None) -> int:
        """从任务 config 中读取整数配置。"""
        task_config = self._parse_task_config(task)
        if isinstance(keys, str):
            keys = [keys]
        value = default
        for key in keys:
            if key in task_config and task_config.get(key) is not None:
                value = task_config.get(key)
                break
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = default
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    def _get_task_timeout_seconds(self, task: dict) -> int:
        """Resolve task runtime timeout; large date-window crawls need more than 30 minutes."""
        task_config = self._parse_task_config(task)

        timeout_seconds = None
        for key in ('task_timeout_seconds', 'timeout_seconds', 'max_runtime_seconds'):
            if key in task_config and task_config.get(key) not in (None, ''):
                timeout_seconds = task_config.get(key)
                break

        if timeout_seconds is None:
            for key in ('task_timeout_minutes', 'timeout_minutes', 'max_runtime_minutes'):
                if key in task_config and task_config.get(key) not in (None, ''):
                    try:
                        timeout_seconds = int(float(task_config.get(key)) * 60)
                    except (TypeError, ValueError):
                        timeout_seconds = None
                    break

        if timeout_seconds is None:
            timeout_seconds = self.task_timeout_seconds

        try:
            timeout_seconds = int(float(timeout_seconds))
        except (TypeError, ValueError):
            timeout_seconds = self.task_timeout_seconds

        return max(300, min(timeout_seconds, 86400))

    def _resolve_task_target_url(self, task: dict) -> Optional[str]:
        """获取定时任务实际 URL，兼容直接 URL 和 URL 管理表引用。"""
        target_url = task.get('target_url')
        if target_url:
            return normalize_task_url(target_url)

        url_id = task.get('url_id')
        if not url_id:
            return None

        try:
            from sqlite_database import sqlite_db
            managed_url = sqlite_db.get_managed_url_by_id(url_id)
            if managed_url:
                return normalize_task_url(managed_url.get('url'))
        except Exception as e:
            print(f"⚠️ 获取URL管理记录失败: {e}")
        return None

    def _get_root_domain(self, url: str) -> str:
        """提取用于限流的根域名；尽量把 www/news/m 等子域归到同一站点。"""
        if not url:
            return ''

        try:
            parsed = urlparse(url if '://' in url else f'https://{url}')
            host = (parsed.hostname or '').lower().strip('.')
            if host.startswith('www.'):
                host = host[4:]
            if not host:
                return ''

            parts = [part for part in host.split('.') if part]
            if len(parts) <= 2:
                return host

            # 粗略兼容 com.cn / co.uk / com.hk 这类二级公共后缀。
            if len(parts[-1]) == 2 and parts[-2] in {'com', 'net', 'org', 'gov', 'edu', 'co'}:
                return '.'.join(parts[-3:])
            return '.'.join(parts[-2:])
        except Exception:
            return ''

    def _log_throttled(self, key: str, message: str, interval_seconds: int = 60):
        """限流日志做节流，避免大量任务每分钟刷屏。"""
        now = time.time()
        last = self._throttle_log_times.get(key, 0)
        if now - last >= interval_seconds:
            print(message)
            self._throttle_log_times[key] = now

    def _release_domain_slot_locked(self, domain: str):
        if not domain:
            return
        current = self._domain_running_counts.get(domain, 0)
        if current <= 1:
            self._domain_running_counts.pop(domain, None)
        else:
            self._domain_running_counts[domain] = current - 1
        self._domain_last_finished_at[domain] = time.time()

    def _is_retryable_error(self, error: Exception) -> bool:
        """判断失败是否值得重试；配置/停止类错误不重试，网络和动态加载类错误重试。"""
        message = str(error)
        non_retryable_markers = [
            '任务被用户停止',
            '任务执行超时',
            '无法获取目标URL',
            '不支持的任务类型'
        ]
        return not any(marker in message for marker in non_retryable_markers)

    def _parse_schedule_datetime(self, value, default: datetime = None) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value or '').strip()
            if not text:
                return default or get_china_time()
            text = text.replace('Z', '+00:00')
            if ' ' in text and 'T' not in text:
                text = text.replace(' ', 'T')
            parsed = datetime.fromisoformat(text)
        if parsed.tzinfo:
            parsed = parsed.replace(tzinfo=None)
        return parsed

    def _get_scheduled_slot(self, task: dict, current_time: datetime) -> datetime:
        try:
            next_run = self._parse_schedule_datetime(task.get('next_run'), current_time)
            return next_run if next_run <= current_time else current_time
        except Exception:
            return current_time

    def _build_run_key(self, task_id, scheduled_for: datetime) -> str:
        slot = scheduled_for.replace(microsecond=0).isoformat() if isinstance(scheduled_for, datetime) else str(scheduled_for)
        return f"schedule:{task_id}:{slot}"
    
    def _execute_task(self, task: dict, current_time: datetime):
        """
        执行定时任务 - 使用受控线程池并发执行
        
        Args:
            task: 任务信息
            current_time: 当前时间
        """
        task_id = task.get('id')
        task_name = task.get('task_name', '未命名任务')
        # url_display_name 由 get_scheduled_tasks JOIN managed_urls 带出；优先用中文名
        url_display_name = task.get('url_display_name') or task_name
        actual_url = self._resolve_task_target_url(task)
        domain = self._get_root_domain(actual_url or task.get('target_url') or '')
        timeout_seconds = self._get_task_timeout_seconds(task)
        scheduled_for = self._get_scheduled_slot(task, current_time)
        next_run_after_claim = self._calculate_next_run(task, scheduled_for)
        
        # 生成唯一任务键
        task_key = f"schedule_{task_id}_{int(current_time.timestamp())}"
        run_key = self._build_run_key(task_id, scheduled_for)
        lock_claimed = False
        reserved_execution_id = None
        
        # 准入控制：同任务不重复、全局限流、同域名限流。被限流的任务不会推进 next_run，下轮继续尝试。
        task_config = self._parse_task_config(task)
        on_overlap = task_config.get('on_overlap', 'skip')  # skip | stop_and_restart

        with self._task_lock:
            for key, data in self._running_tasks.items():
                if data.get('schedule_id') == task_id and not data.get('completed'):
                    if on_overlap == 'stop_and_restart':
                        # 向旧任务发送停止信号，然后继续启动新任务
                        data['stop_flag'] = True
                        data['_stopping'] = True  # 标记为正在停止，不计入并发
                        print(f"🔄 任务 {url_display_name} (ID: {task_id}) stop_and_restart：已通知旧任务停止")
                    else:
                        self._stats['skipped_duplicate'] += 1
                        print(f"⚠️ 任务 {url_display_name} (ID: {task_id}) 已在运行中，跳过")
                        return

            # _stopping 的任务已发出停止信号，不再计入全局并发数
            running_count = sum(1 for data in self._running_tasks.values()
                                if not data.get('completed') and not data.get('_stopping'))
            if running_count >= self.max_concurrent_tasks:
                self._stats['throttled_global'] += 1
                self._log_throttled(
                    'global_capacity',
                    f"全局并发已满 ({running_count}/{self.max_concurrent_tasks})，任务 {url_display_name} 留到下一轮执行"
                )
                return

            if domain:
                domain_running = self._domain_running_counts.get(domain, 0)
                if domain_running >= self.max_tasks_per_domain:
                    self._stats['throttled_domain'] += 1
                    self._log_throttled(
                        f'domain_running:{domain}',
                        f"域名 {domain} 已有 {domain_running} 个任务运行，任务 {url_display_name} 留到下一轮执行"
                    )
                    return

                if self.domain_cooldown_seconds > 0:
                    last_finished = self._domain_last_finished_at.get(domain)
                    if last_finished and time.time() - last_finished < self.domain_cooldown_seconds:
                        self._stats['throttled_domain'] += 1
                        self._log_throttled(
                            f'domain_cooldown:{domain}',
                            f"域名 {domain} 刚完成任务，冷却 {self.domain_cooldown_seconds} 秒后再继续"
                        )
                        return
            
            try:
                from sqlite_database import sqlite_db

                claim_result = sqlite_db.claim_scheduled_task_run(
                    task_id,
                    task_key,
                    current_time,
                    timeout_seconds + 300,
                    scheduled_for=scheduled_for,
                    next_run=next_run_after_claim,
                    run_key=run_key,
                )
                lock_claimed = bool(claim_result and isinstance(claim_result, dict) and claim_result.get('claimed'))
                reserved_execution_id = claim_result.get('execution_id') if lock_claimed else None
            except Exception as claim_error:
                print(f"❌ 领取任务 {url_display_name} (ID: {task_id}) 运行锁失败: {claim_error}")
                return

            if not lock_claimed:
                self._stats['skipped_duplicate'] += 1
                print(f"⚠️ 任务 {url_display_name} (ID: {task_id}) 已有运行记录或锁，跳过本轮提交")
                return

            # 创建停止标志
            task_data = {
                'schedule_id': task_id,
                'task_name': task_name,
                'domain': domain,
                'target_url': actual_url,
                'run_key': run_key,
                'scheduled_for': scheduled_for.isoformat() if isinstance(scheduled_for, datetime) else str(scheduled_for),
                'next_run_after_claim': next_run_after_claim.isoformat() if isinstance(next_run_after_claim, datetime) else next_run_after_claim,
                'execution_id': reserved_execution_id,
                'started_at': current_time.isoformat(),
                'started_epoch': time.time(),
                'timeout_seconds': timeout_seconds,
                'stop_flag': False,
                'completed': False,
                'attempt': 0
            }
            self._running_tasks[task_key] = task_data
            if domain:
                self._domain_running_counts[domain] = self._domain_running_counts.get(domain, 0) + 1
            self._stats['submitted'] += 1
        
        # 确保线程池存在
        if self.executor is None:
            self.executor = ThreadPoolExecutor(max_workers=self.max_concurrent_tasks)
        
        # 提交任务到线程池执行
        def get_stop_flag():
            return self._running_tasks.get(task_key, {}).get('stop_flag', False)
        
        try:
            future = self.executor.submit(
                self._execute_task_worker,
                task,
                current_time,
                task_key,
                get_stop_flag,
                actual_url,
                reserved_execution_id,
                scheduled_for,
                run_key
            )
        except Exception as submit_error:
            with self._task_lock:
                task_data = self._running_tasks.pop(task_key, {})
                self._release_domain_slot_locked(task_data.get('domain'))
                self._stats['failed'] += 1
            if lock_claimed:
                try:
                    from sqlite_database import sqlite_db

                    if reserved_execution_id:
                        sqlite_db.update_task_execution(reserved_execution_id, {
                            'status': 'failed',
                            'completed_at': get_china_time().isoformat(),
                            'error_message': f'提交任务到线程池失败: {submit_error}',
                            'result_summary': {'message': f'提交任务到线程池失败: {submit_error}'}
                        })
                    sqlite_db.release_scheduled_task_run(task_id, task_key)
                except Exception as release_error:
                    print(f"⚠️ 释放任务运行锁失败: {release_error}")
            print(f"❌ 提交任务到线程池失败: {url_display_name} - {submit_error}")
            return

        # 添加完成回调
        future.add_done_callback(lambda f: self._on_task_done(task_key, f))

        with self._task_lock:
            if task_key in self._running_tasks:
                self._running_tasks[task_key]['future'] = future

        print(f"📤 任务 {url_display_name} 已提交到线程池执行 (key: {task_key})")
    
    def _on_task_done(self, task_key: str, future: Future):
        """任务完成回调"""
        with self._task_lock:
            if task_key in self._running_tasks:
                task_data = self._running_tasks[task_key]
                task_data['completed'] = True
                task_data['completed_at'] = get_china_time().isoformat()
                task_data['completed_epoch'] = time.time()
                
                try:
                    if future.cancelled():
                        task_data['status'] = 'cancelled'
                        self._stats['failed'] += 1
                        print(f"⏹️ 任务 {task_key} 已被取消")
                    elif future.exception():
                        task_data['status'] = 'failed'
                        self._stats['failed'] += 1
                        print(f"❌ 任务 {task_key} 执行异常: {future.exception()}")
                    else:
                        result = future.result()
                        if isinstance(result, dict) and result.get('success') is False:
                            task_data['status'] = 'failed'
                            self._stats['failed'] += 1
                        elif isinstance(result, dict) and result.get('needs_review'):
                            task_data['status'] = 'needs_review'
                            self._stats['completed'] += 1
                        else:
                            task_data['status'] = 'completed'
                            self._stats['completed'] += 1
                except Exception as e:
                    print(f"❌ 处理任务完成回调失败: {e}")
                    task_data['status'] = 'failed'
                    self._stats['failed'] += 1
                finally:
                    if not task_data.get('domain_slot_released'):
                        self._release_domain_slot_locked(task_data.get('domain'))
                        task_data['domain_slot_released'] = True
    
    def _execute_task_worker(
        self,
        task: dict,
        current_time: datetime,
        task_key: str,
        get_stop_flag,
        resolved_url: str = None,
        reserved_execution_id=None,
        scheduled_for: datetime = None,
        run_key: str = None,
    ):
        """
        任务执行工作函数 - 在线程池中执行
        
        Args:
            task: 任务信息
            current_time: 当前时间
            task_key: 任务键
            get_stop_flag: 获取停止标志的函数
            resolved_url: 调度准入阶段解析出的实际URL
        """
        from sqlite_database import sqlite_db
        
        task_id = task.get('id')
        task_name = task.get('task_name', '未命名任务')
        task_type = task.get('task_type', 'crawl')
        target_url = resolved_url or task.get('target_url')
        
        execution_id = reserved_execution_id
        crawl_task_id = None
        started_at = current_time.isoformat()
        attempt_errors = []
        result = None
        
        try:
            # 🆕 创建爬虫任务记录（这样就能在爬虫控制台看到）
            crawl_task_id = f"schedule_{task_id}_{int(current_time.timestamp())}"
            try:
                task_config = self._parse_task_config(task)
                
                crawl_task_data = {
                    'task_id': crawl_task_id,
                    'target_url': target_url,
                    'task_name': task_name,
                    'crawl_depth': 1,
                    'crawl_mode': 'list',
                    'page_limit': task_config.get('limit', 50) if isinstance(task_config, dict) else 50,
                    'incremental_mode': False,
                    'keywords': task.get('keywords', '') or (task_config.get('keywords', '') if isinstance(task_config, dict) else ''),
                    'status': 'running'
                }
                sqlite_db.insert_crawl_task(crawl_task_data)
                print(f"📋 创建爬虫任务记录: {crawl_task_id}")
            except Exception as crawl_error:
                print(f"⚠️  创建爬虫任务记录失败: {crawl_error}")
                traceback.print_exc()
                crawl_task_id = None
            
            # 记录任务执行开始。调度准入阶段会预留执行记录，旧路径才新建。
            try:
                if execution_id:
                    sqlite_db.update_task_execution(execution_id, {
                        'task_id': crawl_task_id,
                        'status': 'running',
                        'run_key': run_key,
                        'scheduled_for': scheduled_for.isoformat() if isinstance(scheduled_for, datetime) else scheduled_for,
                        'started_at': started_at,
                        'articles_found': 0,
                        'result_summary': {'message': 'Scheduled run started'}
                    })
                    print(f"📝 更新预留执行记录: {execution_id}")
                else:
                    execution_data = {
                        'schedule_id': task_id,
                        'task_id': crawl_task_id,  # 关联到爬虫任务
                        'status': 'running',
                        'run_key': run_key,
                        'scheduled_for': scheduled_for.isoformat() if isinstance(scheduled_for, datetime) else scheduled_for,
                        'started_at': started_at,
                        'articles_found': 0
                    }
                    execution_id = sqlite_db.insert_task_execution(execution_data)
                    print(f"📝 创建执行记录: {execution_id}")
            except Exception as exec_error:
                print(f"⚠️  创建/更新执行记录失败: {exec_error}")
                traceback.print_exc()
                execution_id = None
            
            # 获取实际的目标URL
            actual_url = target_url or self._resolve_task_target_url(task)
            if not actual_url:
                raise Exception("无法获取目标URL")
            
            max_retries = self._get_config_int(
                task,
                ['retries', 'retry_attempts', 'max_retries'],
                self.retry_attempts,
                0,
                5
            )
            max_attempts = max_retries + 1

            for attempt in range(1, max_attempts + 1):
                if get_stop_flag():
                    print(f"🛑 任务 {task_name} 在第 {attempt} 次尝试前被停止")
                    raise Exception(self._get_stop_error_message(task_key))

                with self._task_lock:
                    if task_key in self._running_tasks:
                        self._running_tasks[task_key]['attempt'] = attempt

                try:
                    print(f"执行任务 {task_name} 第 {attempt}/{max_attempts} 次尝试")

                    if task_type == 'crawl':
                        result = self._execute_crawl_task(task, actual_url, execution_id, get_stop_flag, crawl_task_id)
                    elif task_type == 'extract':
                        result = self._execute_extract_task(task, actual_url, execution_id, get_stop_flag, crawl_task_id)
                    else:
                        raise Exception(f"不支持的任务类型: {task_type}")

                    if not isinstance(result, dict):
                        result = {'success': True, 'data': result}

                    if get_stop_flag():
                        print(f"🛑 任务 {task_name} 执行完成后发现已被停止")
                        result['stopped'] = True
                        raise Exception(self._get_stop_error_message(task_key))

                    if result.get('success') is False:
                        raise Exception(result.get('error') or result.get('message') or '任务返回失败')

                    result['attempts'] = attempt
                    if attempt_errors:
                        result['retry_errors'] = attempt_errors
                    break
                except Exception as attempt_error:
                    error_message = str(attempt_error)
                    attempt_errors.append({
                        'attempt': attempt,
                        'error': error_message,
                        'time': get_china_time().isoformat()
                    })

                    if attempt >= max_attempts or not self._is_retryable_error(attempt_error):
                        raise

                    with self._task_lock:
                        self._stats['retries'] += 1

                    sleep_seconds = self.retry_backoff_seconds * attempt
                    print(f"任务 {task_name} 第 {attempt} 次失败: {error_message}")
                    print(f"   {sleep_seconds} 秒后重试，避免短暂网络/动态加载波动导致漏抓")
                    for _ in range(sleep_seconds):
                        if get_stop_flag():
                            raise Exception(self._get_stop_error_message(task_key))
                        time.sleep(1)

            # 🔥 RAGFlow上传已在爬取过程中分批完成，这里不再需要统一上传
            # 如果配置了 Ragflow 知识库，则尝试同步文章
            # if isinstance(result, dict) and result.get('success') and task.get('ragflow_kb_id'):
            #     try:
            #         ragflow_stats = self._ingest_articles_to_ragflow(task, result)
            #         result['ragflow'] = ragflow_stats
            #         print(
            #             f"📤 Ragflow 同步完成: KB={ragflow_stats.get('kb_id')} | "
            #             f"上传 {ragflow_stats.get('uploaded', 0)}/{ragflow_stats.get('total_candidates', 0)} 篇"
            #         )
            #     except Exception as ragflow_error:
            #         error_msg = f"Ragflow 同步失败: {ragflow_error}"
            #         print(f"⚠️  {error_msg}")
            #         result.setdefault('ragflow', {})['error'] = str(ragflow_error)
            
            # 更新执行记录为成功
            completed_at = get_china_time().isoformat()
            duration = int((get_china_time() - current_time).total_seconds())
            audit_meta = self._persist_audit_file(crawl_task_id, result)
            result_summary = self._compact_result_summary(result, audit_meta)
            summary_stats = result_summary.get('stats') if isinstance(result_summary.get('stats'), dict) else {}
            needs_review = bool(
                result_summary.get('needs_review')
                or result_summary.get('recall_status') == 'needs_review'
                or summary_stats.get('needs_review')
                or (summary_stats.get('recall_risk_count') or 0) > 0
            )
            execution_status = 'needs_review' if needs_review else 'completed'
            
            if execution_id:
                sqlite_db.update_task_execution(execution_id, {
                    'status': execution_status,
                    'completed_at': completed_at,
                    'duration_seconds': duration,
                    'articles_found': result.get('articles_found', 0),
                    'result_summary': result_summary
                })
            else:
                print(f"⚠️  没有执行记录ID，跳过更新")
            
            # 🆕 更新爬虫任务状态为已完成
            if crawl_task_id:
                sqlite_db.update_crawl_task_status(
                    crawl_task_id,
                    execution_status,
                    articles_found=result.get('articles_found', 0),
                    articles_processed=result.get('articles_found', 0)
                )
            
            # 更新任务统计
            next_run = self._calculate_next_run(task, current_time)
            sqlite_db.update_scheduled_task_run_stats(
                task_id, 
                success=True,
                next_run=next_run
            )
            
            if needs_review:
                print(f"⚠️ 定时任务完成但需复核: {task_name}, 下次运行: {next_run}")
            else:
                print(f"✅ 定时任务执行成功: {task_name}, 下次运行: {next_run}")
            return {'success': True, 'needs_review': needs_review, 'result': result}
            
        except Exception as e:
            print(f"❌ 定时任务执行失败: {task_name} - {e}")
            traceback.print_exc()
            
            # 更新执行记录为失败
            if execution_id:
                completed_at = get_china_time().isoformat()
                duration = int((get_china_time() - current_time).total_seconds())
                audit_meta = self._persist_audit_file(crawl_task_id, result or {})
                
                sqlite_db.update_task_execution(execution_id, {
                    'status': 'failed',
                    'completed_at': completed_at,
                    'duration_seconds': duration,
                    'error_message': str(e),
                    'result_summary': {
                        'message': f'任务失败: {str(e)}',
                        'attempt_errors': attempt_errors,
                        **audit_meta
                    }
                })
            else:
                print(f"⚠️  没有执行记录ID，跳过失败记录更新")
            
            # 🆕 更新爬虫任务状态为失败
            if crawl_task_id:
                sqlite_db.update_crawl_task_status(
                    crawl_task_id,
                    'failed',
                    error_message=str(e)
                )
            
            # 更新任务统计（失败）
            next_run = self._calculate_next_run(task, current_time)
            sqlite_db.update_scheduled_task_run_stats(
                task_id, 
                success=False,
                next_run=next_run
            )
            return {
                'success': False,
                'error': str(e),
                'attempt_errors': attempt_errors
            }
        finally:
            try:
                sqlite_db.release_scheduled_task_run(task_id, task_key)
            except Exception as release_error:
                print(f"⚠️ 释放任务运行锁失败: {release_error}")
    
    def _execute_crawl_task(self, task: dict, target_url: str, execution_id: str, get_stop_flag=None, crawl_task_id: str = None) -> dict:
        """
        执行爬取任务 - 使用智能验证（列表页提取链接后验证）
        
        Args:
            task: 任务信息
            target_url: 目标URL
            execution_id: 执行记录ID
            get_stop_flag: 获取停止标志的函数
            
        Returns:
            dict: 执行结果
        """
        _display = task.get('url_display_name')
        print(f"🕷️  开始智能爬取: {_display + ' ' if _display else ''}{target_url}")
        
        # 定义默认停止检查函数
        if get_stop_flag is None:
            get_stop_flag = lambda: False

        # 先解析任务配置，Cookie 刷新也必须尊重任务级代理开关。
        task_config = self._parse_task_config(task)
        crawl_options = normalize_crawl_options(task_config)
        
        # 🔥 任务开始前自动刷新Cookie（避免过期问题）
        login_success = self._refresh_cookies_for_url(
            target_url,
            proxy_enabled=crawl_options.get('proxy_enabled')
        )
        
        # 🔥 如果登录失败，跳过爬取任务
        if not login_success:
            print(f"❌ 登录失败，跳过爬取任务")
            return {
                'success': False,
                'message': '登录失败，无法获取有效Cookie',
                'articles': [],
                'stats': {'total': 0, 'success': 0, 'failed': 0}
            }
        
        # 获取限制（确保task_config是字典）
        if isinstance(task_config, dict):
            limit = _normalize_limit_value(task_config.get('limit', float('inf')))  # 🔥 默认无限制
            # 0 means no date limit; only fall back when the field is missing.
            days_limit_raw = task.get('days_limit')
            if days_limit_raw in (None, ''):
                days_limit_raw = task_config.get('days_limit', 7)
            days_limit = _coerce_days_limit(days_limit_raw, 7)
            start_date = (
                task.get('start_date')
                or task.get('date_start')
                or task_config.get('start_date')
                or task_config.get('date_start')
                or task_config.get('from_date')
            )
            end_date = (
                task.get('end_date')
                or task.get('date_end')
                or task_config.get('end_date')
                or task_config.get('date_end')
                or task_config.get('to_date')
            )
        else:
            print(f"⚠️ 配置格式错误，使用默认无限制")
            limit = float('inf')  # 🔥 无限制
            days_limit = 7  # 默认7天
            start_date = None
            end_date = None
        
        if start_date or end_date:
            date_window = f"{start_date or '不限'} ~ {end_date or '不限'}"
        elif days_limit and days_limit > 0:
            date_window = f"最近{days_limit}天"
        else:
            date_window = "不限制"
        print(
            f"📊 爬取配置: limit={'无限制' if limit == float('inf') else limit}, 日期范围={date_window}, 智能验证=启用, "
            f"等待={crawl_options.get('wait_for_ms')}ms, 最大翻页={crawl_options.get('max_pages')}, "
            f"补充发现={'开' if crawl_options.get('supplemental_enabled') else '关'}, "
            f"网络JSON={'开' if crawl_options.get('network_json_enabled') else '关'}, "
            f"代理={'开' if crawl_options.get('proxy_enabled') else '关'}"
        )
        
        # 🔥 使用实时爬取模式（边提取边爬取，自动登录）
        try:
            from article_link_extractor import ArticleLinkExtractor
            from sqlite_database import sqlite_db
            
            # 创建提取器实例
            extractor = ArticleLinkExtractor(db=sqlite_db, enable_smart_validation=False)
            
            print(f"✅ 使用通用日期优先爬取模式（发现候选 + 详情按日期过滤 + 审计）")
            
            # 获取关键词（如果有）- 优先从task.keywords字段读取
            keywords = task.get('keywords', '') or (task_config.get('keywords', '') if isinstance(task_config, dict) else '')
            
            # 获取知识库ID
            kb_id = task.get('ragflow_kb_id')
            
            # 日期范围优先，limit 只在没有日期范围时作为人工安全阈值。
            result = extractor.crawl_news_site(
                list_url=target_url,
                limit=limit,
                wait_for=crawl_options.get('wait_for_ms', 8000),
                days_limit=days_limit,  # 日期限制
                keywords=keywords,      # 关键词过滤
                kb_id=kb_id,           # 知识库ID
                start_date=start_date,
                end_date=end_date,
                should_stop=get_stop_flag,
                crawl_options=crawl_options,
                task_id=crawl_task_id
            )
            
            if result.get('success'):
                stats = result.get('stats', {})
                articles_found = stats.get('success', 0)
                needs_review = bool(
                    result.get('needs_review')
                    or result.get('recall_status') == 'needs_review'
                    or stats.get('needs_review')
                    or (stats.get('recall_risk_count') or 0) > 0
                )
                
                print(f"✅ 成功提取 {articles_found} 篇文章")
                
                return {
                    'success': True,
                    'needs_review': needs_review,
                    'recall_status': result.get('recall_status'),
                    'articles_found': articles_found,
                    'data': result,
                    'message': (
                        f'智能验证提取 {articles_found} 篇文章，存在需复核项'
                        if needs_review else f'智能验证成功提取 {articles_found} 篇文章'
                    )
                }
            else:
                print(f"❌ 提取失败: {result.get('error', '未知错误')}")
                return {
                    'success': False,
                    'articles_found': 0,
                    'error': result.get('error', '提取失败')
                }
                
        except Exception as e:
            print(f"❌ 智能爬取异常: {e}")
            import traceback as tb
            tb.print_exc()
            
            return {
                'success': False,
                'articles_found': 0,
                'error': f'智能爬取异常: {e}'
            }
    
    def _execute_extract_task(self, task: dict, target_url: str, execution_id: str, get_stop_flag=None, crawl_task_id: str = None) -> dict:
        """
        执行文章提取任务 - 使用智能验证
        
        Args:
            task: 任务信息
            target_url: 目标URL
            execution_id: 执行记录ID
            get_stop_flag: 获取停止标志的函数
            crawl_task_id: 爬虫任务ID（用于创建文章-任务关联）
            
        Returns:
            dict: 执行结果
        """
        _display = task.get('url_display_name')
        print(f"📰 开始智能提取文章: {_display + ' ' if _display else ''}{target_url}")
        
        # 定义默认停止检查函数
        if get_stop_flag is None:
            get_stop_flag = lambda: False
        
        # 解析配置
        task_config = task.get('config')
        if isinstance(task_config, str):
            try:
                task_config = json.loads(task_config)
            except Exception as parse_err:
                print(f"⚠️ 配置解析失败: {parse_err}, 使用默认配置")
                task_config = {}
        elif not task_config:
            task_config = {}
        crawl_options = normalize_crawl_options(task_config)
        
        # 获取限制（确保task_config是字典）
        if isinstance(task_config, dict):
            limit = _normalize_limit_value(task_config.get('limit', float('inf')))  # 🔥 默认无限制
            # 0 means no date limit; only fall back when the field is missing.
            days_limit_raw = task.get('days_limit')
            if days_limit_raw in (None, ''):
                days_limit_raw = task_config.get('days_limit', 7)
            days_limit = _coerce_days_limit(days_limit_raw, 7)
            start_date = (
                task.get('start_date')
                or task.get('date_start')
                or task_config.get('start_date')
                or task_config.get('date_start')
                or task_config.get('from_date')
            )
            end_date = (
                task.get('end_date')
                or task.get('date_end')
                or task_config.get('end_date')
                or task_config.get('date_end')
                or task_config.get('to_date')
            )
        else:
            print(f"⚠️ 配置格式错误，使用默认无限制")
            limit = float('inf')  # 🔥 无限制
            days_limit = 7  # 默认7天
            start_date = None
            end_date = None
        
        if start_date or end_date:
            date_window = f"{start_date or '不限'} ~ {end_date or '不限'}"
        elif days_limit and days_limit > 0:
            date_window = f"最近{days_limit}天"
        else:
            date_window = "不限制"
        print(
            f"📊 提取配置: limit={'无限制' if limit == float('inf') else limit}, 日期范围={date_window}, 智能验证=启用, "
            f"等待={crawl_options.get('wait_for_ms')}ms, 最大翻页={crawl_options.get('max_pages')}, "
            f"补充发现={'开' if crawl_options.get('supplemental_enabled') else '关'}, "
            f"网络JSON={'开' if crawl_options.get('network_json_enabled') else '关'}, "
            f"代理={'开' if crawl_options.get('proxy_enabled') else '关'}"
        )
        
        # 使用 ArticleLinkExtractor 智能提取和验证
        try:
            from article_link_extractor import ArticleLinkExtractor
            from sqlite_database import sqlite_db
            
            # 创建提取器（禁用智能验证器，直接用Playwright提取器）
            extractor = ArticleLinkExtractor(db=sqlite_db, enable_smart_validation=False)
            
            print(f"✅ 使用 Playwright 提取器（smart_article_extractor）")
            
            # 获取关键词（如果有）
            keywords = task.get('keywords', '') or (task_config.get('keywords', '') if isinstance(task_config, dict) else '')
            
            # 获取知识库ID
            kb_id = task.get('ragflow_kb_id')
            
            # 🔥 获取爬取前的文章数量（用于后续创建关联）
            from urllib.parse import urlparse
            domain = urlparse(target_url).netloc.replace('www.', '')
            
            # 执行爬取（自动智能验证 + 入库 + 关键词过滤 + 分批上传 + 日期限制）
            result = extractor.crawl_news_site(
                list_url=target_url,
                limit=limit,
                wait_for=crawl_options.get('wait_for_ms', 8000),
                keywords=keywords,  # 传递关键词
                kb_id=kb_id,  # 🔥 传递知识库ID，用于分批上传
                days_limit=days_limit,  # 传递日期限制
                start_date=start_date,
                end_date=end_date,
                should_stop=get_stop_flag,
                crawl_options=crawl_options
            )
            
            if result.get('success'):
                stats = result.get('stats', {})
                articles_found = stats.get('success', 0)
                
                print(f"✅ 成功提取 {articles_found} 篇文章")
                
                # 🔥 创建文章-任务关联（查询该域名最近入库的文章）
                if crawl_task_id and articles_found > 0:
                    try:
                        cursor = sqlite_db.connection.cursor()
                        # 查询该域名最近入库的文章（按创建时间倒序，取最近的N篇）
                        cursor.execute("""
                            SELECT id FROM articles 
                            WHERE (domain = ? OR domain LIKE ?)
                            ORDER BY created_at DESC
                            LIMIT ?
                        """, (domain, f'%{domain}%', articles_found + 10))  # 多取一些以防遗漏
                        
                        article_ids = [row[0] for row in cursor.fetchall()]
                        cursor.close()
                        
                        # 批量创建关联
                        linked_count = 0
                        for article_id in article_ids:
                            if sqlite_db.link_article_to_task(article_id, crawl_task_id):
                                linked_count += 1
                        
                        print(f"🔗 已创建 {linked_count} 个文章-任务关联")
                    except Exception as link_error:
                        print(f"⚠️ 创建文章-任务关联失败: {link_error}")
                
                return {
                    'success': True,
                    'articles_found': articles_found,
                    'data': result,
                    'message': f'智能验证成功提取 {articles_found} 篇文章'
                }
            else:
                print(f"❌ 提取失败: {result.get('error', '未知错误')}")
                return {
                    'success': False,
                    'articles_found': 0,
                    'error': result.get('error', '提取失败')
                }
                
        except Exception as e:
            print(f"❌ 智能提取异常: {e}")
            traceback.print_exc()
            
            return {
                'success': False,
                'articles_found': 0,
                'error': f'智能提取异常: {e}'
            }
    
    def _calculate_next_run(self, task: dict, current_time: datetime) -> datetime:
        """Calculate the next run time after a scheduled execution."""
        import calendar

        schedule_type = task.get('schedule_type', 'once')
        if schedule_type == 'once':
            return None

        schedule_time = task.get('schedule_time') or '00:00:00'
        schedule_day = task.get('schedule_day')

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

        if schedule_type == 'hourly':
            next_run = current_time.replace(minute=minute, second=second, microsecond=0)
            if next_run <= current_time:
                next_run += timedelta(hours=1)
            return next_run

        if schedule_type == 'daily':
            next_run = current_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if next_run <= current_time:
                next_run += timedelta(days=1)
            return next_run

        if schedule_type == 'weekly':
            weekdays = parse_int_list(task.get('schedule_weekdays'), 0, 6)
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
            monthdays = parse_int_list(task.get('schedule_monthdays'), 1, 31)
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
    
    def _get_login_lock(self, domain: str) -> threading.Lock:
        """
        获取指定域名的登录锁（线程安全）
        
        Args:
            domain: 域名
            
        Returns:
            threading.Lock: 该域名的登录锁
        """
        with self._login_locks_lock:
            if domain not in self._login_locks:
                self._login_locks[domain] = threading.Lock()
            return self._login_locks[domain]
    
    def _refresh_cookies_for_url(self, url: str, proxy_enabled=None) -> bool:
        """
        任务开始前自动刷新Cookie（避免过期问题）
        🔥 使用登录锁机制，确保同一域名的登录操作串行执行
        
        Args:
            url: 目标URL
            proxy_enabled: 任务级代理开关；False 时自动登录强制直连。
            
        Returns:
            bool: 登录是否成功（True=成功或无需登录，False=登录失败）
        """
        try:
            from urllib.parse import urlparse
            from sqlite_database import sqlite_db
            
            # 提取根域名
            domain = urlparse(url).netloc.lower()
            domain_parts = domain.split('.')
            root_domain = '.'.join(domain_parts[-2:]) if len(domain_parts) >= 2 else domain
            
            print(f"🔐 检查 {root_domain} 的认证配置...")
            
            # 查找该域名对应的认证配置
            auth_configs = sqlite_db.get_all_auth_configs() if hasattr(sqlite_db, 'get_all_auth_configs') else []
            
            matched_config = None
            for config in auth_configs:
                login_url = config.get('login_url', '')
                if root_domain in login_url:
                    matched_config = config
                    break
            
            if not matched_config:
                print(f"   ⚠️ 未找到 {root_domain} 的认证配置，跳过Cookie刷新")
                return True  # 无需登录，返回成功
            
            auth_name = matched_config.get('name')
            
            # 🔥 获取该域名的登录锁，确保同一域名的登录操作串行执行
            login_lock = self._get_login_lock(root_domain)
            
            # 尝试获取锁，如果已被占用则等待
            if login_lock.locked():
                print(f"   ⏳ 等待 {root_domain} 的登录锁（其他任务正在登录）...")
            
            with login_lock:
                print(f"   📌 找到认证配置: {auth_name}，开始刷新Cookie...")
                
                # 获取完整的认证配置信息
                cursor = sqlite_db.connection.cursor()
                cursor.execute("SELECT * FROM auth_configs WHERE id = ?", (matched_config.get('id'),))
                row = cursor.fetchone()
                cursor.close()
                
                if not row:
                    print(f"   ⚠️ 无法获取认证配置详情")
                    return False  # 配置错误，返回失败
                
                auth_config_dict = {
                    'login_url': row['login_url'],
                    'username': row['username'],
                    'password': row['password'],
                    'username_selector': row['username_selector'],
                    'password_selector': row['password_selector'],
                    'submit_selector': row['submit_selector'],
                    'wait_after_submit': row['wait_after_submit'] or 5
                }
                
                # 执行登录刷新Cookie
                from url_management_api import execute_auth_login
                refresh_result = execute_auth_login(
                    auth_config_dict,
                    auth_name,
                    proxy_enabled=proxy_enabled
                )
                
                if refresh_result.get('success'):
                    print(f"   ✅ Cookie刷新成功: {auth_name}")
                    return True
                else:
                    print(f"   ❌ Cookie刷新失败: {refresh_result.get('message')}")
                    return False
                
        except Exception as e:
            print(f"⚠️ Cookie刷新出错: {e}")
            return False  # 出错也返回失败


# 全局调度器实例
scheduler = TaskScheduler()
