#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
装饰器模块
包含登录验证等装饰器
"""

from functools import wraps
from flask import request, jsonify, redirect, url_for
from user_database import user_db


def login_required(f):
    """需要登录才能访问的装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 检查cookie中的session_token
        token = request.cookies.get('session_token')
        if not token:
            if request.is_json or request.path.startswith('/api/') or '/api/' in request.path:
                return jsonify({'success': False, 'error': '请先登录'}), 401
            return redirect(url_for('login_page'))
        
        # 验证会话
        session_data = user_db.verify_session(token)
        if not session_data:
            if request.is_json or request.path.startswith('/api/') or '/api/' in request.path:
                return jsonify({'success': False, 'error': '会话已过期，请重新登录'}), 401
            return redirect(url_for('login_page'))
        
        # 将用户信息附加到请求上下文
        request.current_user = session_data
        return f(*args, **kwargs)
    return decorated_function

