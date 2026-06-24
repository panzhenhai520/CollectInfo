#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文章管理API模块
提供独立的文章管理功能
"""

from flask import Blueprint, request, jsonify, render_template
from sqlite_database import sqlite_db
from decorators import login_required
import json
from datetime import datetime
from utils import coerce_int, get_china_time


def _quality_issue_report(article):
    content = (article or {}).get('content') or ''
    title = ((article or {}).get('title') or '').strip()
    url = ((article or {}).get('url') or '').strip()
    length = len(content.strip())
    issues = []

    if not title or title in {'无标题', 'None', 'null'}:
        issues.append('标题缺失')
    if not url:
        issues.append('URL缺失')
    if length == 0:
        issues.append('正文为空')
    elif length < 80:
        issues.append('正文过短')
    elif length < 300:
        issues.append('正文偏短')

    lowered = content.lower()
    if any(marker in lowered for marker in ('404 not found', 'page not found', 'access denied', 'forbidden')):
        issues.append('疑似错误页')
    if content.count('http://') + content.count('https://') > 8:
        issues.append('正文URL过多')

    try:
        from smart_article_extractor import evaluate_content_quality
        quality_score = int(evaluate_content_quality(content)) if content else 0
    except Exception:
        quality_score = int((article or {}).get('quality_score') or 0)
        if not quality_score:
            if length >= 1000:
                quality_score = 80
            elif length >= 300:
                quality_score = 65
            elif length >= 80:
                quality_score = 45

    stored_score = int((article or {}).get('quality_score') or 0)
    if stored_score and quality_score:
        quality_score = max(quality_score, stored_score)

    is_good = quality_score >= 60 and not any(
        issue in issues for issue in ('标题缺失', 'URL缺失', '正文为空', '正文过短', '疑似错误页')
    )
    return {
        'article_id': (article or {}).get('id'),
        'title': title,
        'url': url,
        'content_length': length,
        'quality_score': quality_score,
        'issues': issues,
        'issues_count': len(issues),
        'good': is_good
    }


def _diagnose_article_by_id(article_id):
    article = sqlite_db.get_article_by_id(article_id)
    if not article:
        return {'success': False, 'error': '文章不存在'}
    report = _quality_issue_report(article)
    report['success'] = True
    return report


def _diagnose_articles(limit=100):
    limit = coerce_int(limit, 100, 1, 5000)
    articles, total_available = sqlite_db.get_articles(1, limit)
    reports = [_quality_issue_report(article) for article in articles]
    issues_summary = {}
    for report in reports:
        for issue in report['issues']:
            issues_summary[issue] = issues_summary.get(issue, 0) + 1

    good = sum(1 for report in reports if report['good'])
    bad = len(reports) - good
    return {
        'total': len(reports),
        'total_available': total_available,
        'good': good,
        'bad': bad,
        'issues_summary': issues_summary,
        'bad_articles': [report for report in reports if not report['good']][:100]
    }


def _repair_article_content(article_id):
    article = sqlite_db.get_article_by_id(article_id)
    if not article:
        return {'success': False, 'error': '文章不存在'}

    old_report = _quality_issue_report(article)
    try:
        from smart_article_extractor import extract_article_content_from_url
        extracted = extract_article_content_from_url(article.get('url'), skip_db_check=True)
    except Exception as exc:
        return {'success': False, 'error': f'重新提取失败: {exc}'}

    if not extracted or not extracted.get('success'):
        return {
            'success': False,
            'error': (extracted or {}).get('error') or '重新提取未返回有效正文'
        }

    new_content = (extracted.get('content') or '').strip()
    if not new_content:
        return {'success': False, 'error': '重新提取后正文为空'}

    new_title = (extracted.get('title') or article.get('title') or '无标题').strip()
    new_quality = int(extracted.get('score') or extracted.get('quality_score') or 0)
    article_data = {
        'title': new_title,
        'content': new_content,
        'category_id': article.get('category_id'),
        'source_url_id': article.get('source_url_id'),
        'publish_date': extracted.get('publish_date') or article.get('publish_date'),
        'extraction_method': extracted.get('method') or 'manual_repair',
        'quality_score': new_quality
    }

    updated_id = sqlite_db.update_article(article_id, article_data)
    if not updated_id:
        return {'success': False, 'error': '数据库更新失败'}

    updated_article = sqlite_db.get_article_by_id(article_id)
    new_report = _quality_issue_report(updated_article)
    return {
        'success': True,
        'article_id': article_id,
        'old_quality': old_report['quality_score'],
        'new_quality': new_report['quality_score'],
        'old_issues': old_report['issues_count'],
        'new_issues': new_report['issues_count'],
        'improved': new_report['quality_score'] > old_report['quality_score'] or new_report['issues_count'] < old_report['issues_count']
    }

# 创建蓝图
article_management_bp = Blueprint('article_management', __name__, url_prefix='/article-management')

# ==================== 页面路由 ====================

@article_management_bp.route('/')
@login_required
def index():
    """文章管理主页"""
    return render_template('article_management.html')

# ==================== API路由 ====================

@article_management_bp.route('/api/statistics', methods=['GET'])
@login_required
def get_statistics():
    """获取文章统计信息"""
    try:
        domain = request.args.get('domain')
        stats = sqlite_db.get_statistics(domain)
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取统计信息失败: {str(e)}'
        }), 500

@article_management_bp.route('/api/articles', methods=['GET'])
@login_required
def get_articles():
    """获取文章列表"""
    try:
        page = coerce_int(request.args.get('page'), 1, 1)
        per_page = coerce_int(request.args.get('per_page'), 20, 1, 500)
        domain = request.args.get('domain')
        category_id = request.args.get('category_id')
        source_url_id = request.args.get('source_url_id')
        search = request.args.get('search')
        keyword = request.args.get('keyword')
        
        # 转换category_id为整数
        if category_id:
            category_id = coerce_int(category_id, None, 1)
        
        # 转换source_url_id为整数
        if source_url_id:
            source_url_id = coerce_int(source_url_id, None, 1)
        
        articles, total = sqlite_db.get_articles(page, per_page, domain, category_id, source_url_id, search, keyword)
        
        return jsonify({
            'success': True,
            'articles': articles,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取文章列表失败: {str(e)}'
        }), 500


@article_management_bp.route('/api/keyword-map', methods=['GET'])
@login_required
def get_keyword_map():
    """获取关键词信息图谱"""
    try:
        limit = coerce_int(request.args.get('limit'), 500, 1, 5000)
        keywords = sqlite_db.get_keyword_map(limit)
        return jsonify({
            'success': True,
            'keywords': keywords,
            'total_keywords': len(keywords),
            'total_keyword_articles': sum(item.get('article_count', 0) for item in keywords)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取关键词信息图谱失败: {str(e)}'
        }), 500

@article_management_bp.route('/api/article/<int:article_id>', methods=['GET'])
@login_required
def get_article_detail(article_id):
    """获取文章详情"""
    try:
        article = sqlite_db.get_article_by_id(article_id)
        if article:
            return jsonify({
                'success': True,
                'article': article
            })
        else:
            return jsonify({
                'success': False,
                'error': '文章不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取文章详情失败: {str(e)}'
        }), 500

@article_management_bp.route('/api/article/<int:article_id>', methods=['DELETE'])
@login_required
def delete_article(article_id):
    """删除文章"""
    try:
        success = sqlite_db.delete_article(article_id)
        if success:
            return jsonify({
                'success': True,
                'message': '文章删除成功'
            })
        else:
            return jsonify({
                'success': False,
                'error': '文章删除失败'
            }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'删除文章失败: {str(e)}'
        }), 500

@article_management_bp.route('/api/articles/batch-delete', methods=['POST'])
@login_required
def batch_delete_articles():
    """批量删除文章"""
    try:
        data = request.get_json(silent=True) or {}
        article_ids = data.get('article_ids', [])
        
        if not article_ids:
            return jsonify({
                'success': False,
                'error': '缺少文章ID列表'
            }), 400
        
        deleted_count = 0
        failed_items = []
        
        for article_id in article_ids:
            if sqlite_db.delete_article(article_id):
                deleted_count += 1
            else:
                failed_items.append(article_id)
        
        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'failed_items': failed_items,
            'message': f'成功删除 {deleted_count} 篇文章'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'批量删除失败: {str(e)}'
        }), 500

@article_management_bp.route('/api/articles/clear-local', methods=['POST'])
@login_required
def clear_local_articles():
    """Hard-clear local article data for test resets."""
    try:
        data = request.get_json(silent=True) or {}
        if data.get('confirm') != 'DELETE_LOCAL_ARTICLES':
            return jsonify({
                'success': False,
                'error': 'confirmation required'
            }), 400

        result = sqlite_db.clear_local_articles()
        if result.get('success'):
            return jsonify({
                'success': True,
                'result': result,
                'message': 'local articles cleared'
            })

        return jsonify({
            'success': False,
            'result': result,
            'error': 'failed to clear local articles'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'clear local articles failed: {str(e)}'
        }), 500

@article_management_bp.route('/api/domains', methods=['GET'])
@login_required
def get_domains():
    """获取所有域名列表"""
    try:
        stats = sqlite_db.get_statistics()
        domain_stats = stats.get('domain_stats', {})
        
        domains = [
            {'domain': domain, 'count': count}
            for domain, count in domain_stats.items()
        ]
        
        return jsonify({
            'success': True,
            'domains': domains
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'获取域名列表失败: {str(e)}'
        }), 500

@article_management_bp.route('/api/export', methods=['GET'])
@login_required
def export_articles():
    """导出文章数据（支持按分类、来源URL、域名筛选）"""
    try:
        format_type = request.args.get('format', 'zip')  # 默认zip格式
        domain = request.args.get('domain')
        category_id = request.args.get('category_id')
        source_url_id = request.args.get('source_url_id')
        search = request.args.get('search')
        
        # 转换ID参数
        if category_id:
            category_id = coerce_int(category_id, None, 1)
        if source_url_id:
            source_url_id = coerce_int(source_url_id, None, 1)
        
        # 获取所有匹配的文章
        articles, total = sqlite_db.get_articles(1, 10000, domain, category_id, source_url_id, search, keyword)
        
        if format_type == 'json':
            return jsonify({
                'success': True,
                'articles': articles,
                'total': total,
                'exported_at': get_china_time().isoformat()
            })
        elif format_type == 'zip':
            # 生成ZIP文件
            from io import BytesIO
            import zipfile
            
            memory_file = BytesIO()
            
            with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                # 生成文件名前缀
                prefix = ''
                if category_id:
                    # 获取分类名称
                    cat = sqlite_db.get_category_by_id(category_id)
                    if cat:
                        prefix = f"{cat['name']}_"
                elif source_url_id:
                    # 获取来源URL名称
                    url = sqlite_db.get_managed_url_by_id(source_url_id)
                    if url:
                        prefix = f"{url['name']}_"
                elif domain:
                    prefix = f"{domain}_"
                
                # 添加汇总文件
                summary = f"文章下载报告\n"
                summary += f"{'='*60}\n"
                summary += f"下载时间: {get_china_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
                summary += f"总文章数: {total}\n"
                if category_id:
                    cat = sqlite_db.get_category_by_id(category_id)
                    summary += f"筛选条件: 分类 = {cat['name'] if cat else '未知'}\n"
                elif source_url_id:
                    url = sqlite_db.get_managed_url_by_id(source_url_id)
                    summary += f"筛选条件: 来源 = {url['name'] if url else '未知'}\n"
                elif domain:
                    summary += f"筛选条件: 域名 = {domain}\n"
                summary += f"{'='*60}\n\n"
                
                for i, article in enumerate(articles, 1):
                    summary += f"{i}. {article['title']}\n"
                    summary += f"   URL: {article['url']}\n"
                    summary += f"   发布日期: {article.get('publish_date', '未知')}\n\n"
                
                zf.writestr("00_文章列表.txt", summary.encode('utf-8'))
                
                # 添加每篇文章
                for i, article in enumerate(articles, 1):
                    filename = f"{str(i).zfill(3)}_{article['title'][:50]}.txt"
                    # 移除文件名中的非法字符
                    filename = filename.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
                    
                    # 获取文章内容并清理元数据
                    content = article.get('content', '无内容')
                    
                    # 清理内容开头的元数据（标题、URL、发布日期、来源、分类、爬取时间、分隔线）
                    import re
                    lines = content.split('\n')
                    cleaned_lines = []
                    skip_metadata = True
                    
                    for line in lines:
                        # 检测元数据行
                        if skip_metadata:
                            # 跳过以这些关键词开头的行
                            if (line.startswith('标题:') or 
                                line.startswith('URL:') or 
                                line.startswith('发布日期:') or 
                                line.startswith('来源:') or 
                                line.startswith('分类:') or 
                                line.startswith('爬取时间:') or
                                line.strip() == '=' * 60 or
                                line.strip() == ''):
                                continue
                            else:
                                # 遇到第一行非元数据内容，停止跳过
                                skip_metadata = False
                        
                        if not skip_metadata:
                            cleaned_lines.append(line)
                    
                    content = '\n'.join(cleaned_lines).strip()
                    
                    zf.writestr(filename, content.encode('utf-8'))
            
            memory_file.seek(0)
            
            # 生成下载文件名
            timestamp = get_china_time().strftime('%Y%m%d_%H%M%S')
            download_filename = f"{prefix}文章_{timestamp}.zip"
            
            from flask import send_file
            return send_file(
                memory_file,
                mimetype='application/zip',
                as_attachment=True,
                download_name=download_filename
            )
        else:
            return jsonify({
                'success': False,
                'error': '不支持的导出格式'
            }), 400
            
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'导出失败: {str(e)}'
        }), 500


# ==================== 诊断和修复API ====================

@article_management_bp.route('/api/diagnose', methods=['POST'])
@login_required
def diagnose_articles():
    """诊断文章内容质量"""
    try:
        data = request.get_json(silent=True) or {}
        article_id = data.get('article_id')
        limit = data.get('limit', 100)
        
        if article_id:
            # 诊断单篇文章
            result = _diagnose_article_by_id(article_id)
            return jsonify({
                'success': True,
                'result': result
            })
        else:
            # 诊断所有文章
            result = _diagnose_articles(limit=limit)
            return jsonify({
                'success': True,
                'result': result
            })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'诊断失败: {str(e)}'
        }), 500


@article_management_bp.route('/api/fix-article', methods=['POST'])
@login_required
def fix_single_article():
    """修复单篇文章"""
    try:
        data = request.get_json(silent=True) or {}
        article_id = data.get('article_id')
        
        if not article_id:
            return jsonify({
                'success': False,
                'error': '缺少article_id参数'
            }), 400
        
        result = _repair_article_content(article_id)
        return jsonify(result)
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'修复失败: {str(e)}'
        }), 500


@article_management_bp.route('/api/batch-fix', methods=['POST'])
@login_required
def batch_fix_articles():
    """批量修复文章"""
    try:
        data = request.get_json(silent=True) or {}
        article_ids = data.get('article_ids')
        quality_threshold = data.get('quality_threshold', 60)
        
        if article_ids:
            target_ids = article_ids
        else:
            diagnosis = _diagnose_articles(limit=5000)
            target_ids = [
                item['article_id']
                for item in diagnosis.get('bad_articles', [])
                if item.get('quality_score', 0) < quality_threshold
            ]

        result = {
            'total': len(target_ids),
            'success': 0,
            'no_improvement': 0,
            'failed': 0,
            'items': []
        }
        for item_id in target_ids:
            item_result = _repair_article_content(item_id)
            result['items'].append(item_result)
            if item_result.get('success'):
                if item_result.get('improved'):
                    result['success'] += 1
                else:
                    result['no_improvement'] += 1
            else:
                result['failed'] += 1
        
        return jsonify({
            'success': True,
            'result': result
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'批量修复失败: {str(e)}'
        }), 500
