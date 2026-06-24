#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""在线助手 Chat API - 支持豆包/DeepSeek/Gemini/ChatGPT/Claude 流式对话 + 联网搜索"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Generator, List, Dict

import requests
from flask import Blueprint, Response, jsonify, request, stream_with_context

chat_bp = Blueprint('chat_bp', __name__)

# 配置文件路径
_CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'data')
_CONFIG_FILE = os.path.join(_CONFIG_DIR, 'chat_config.json')

# 模型元信息（固定不变的部分）
MODEL_META = {
    'doubao': {
        'name': '豆包',
        'base_url': 'https://ark.volces.com/api/v3',
        'default_model': '',  # 用户需填写 endpoint ID
        'type': 'openai',
    },
    'deepseek': {
        'name': 'DeepSeek',
        'base_url': 'https://api.deepseek.com/v1',
        'default_model': 'deepseek-chat',
        'type': 'openai',
    },
    'gemini': {
        'name': 'Gemini',
        'base_url': 'https://generativelanguage.googleapis.com/v1beta/openai',
        'default_model': 'gemini-1.5-pro',
        'type': 'openai',
    },
    'chatgpt': {
        'name': 'ChatGPT',
        'base_url': 'https://api.openai.com/v1',
        'default_model': 'gpt-4o',
        'type': 'openai',
    },
    'claude': {
        'name': 'Claude',
        'base_url': 'https://api.anthropic.com/v1',
        'default_model': 'claude-3-5-sonnet-20241022',
        'type': 'anthropic',
    },
}

DEFAULT_CONFIG = {
    'active_model': 'deepseek',
    'ragflow_kb_id': '',
    'models': {k: {'api_key': '', 'model_id': v['default_model']} for k, v in MODEL_META.items()},
}


def _load_config() -> dict:
    os.makedirs(_CONFIG_DIR, exist_ok=True)
    try:
        with open(_CONFIG_FILE, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        # 补全缺失字段
        for k in DEFAULT_CONFIG:
            if k not in cfg:
                cfg[k] = DEFAULT_CONFIG[k]
        for m in MODEL_META:
            if m not in cfg.get('models', {}):
                cfg.setdefault('models', {})[m] = {'api_key': '', 'model_id': MODEL_META[m]['default_model']}
        return cfg
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(DEFAULT_CONFIG)


def _save_config(cfg: dict) -> None:
    os.makedirs(_CONFIG_DIR, exist_ok=True)
    with open(_CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def _web_search(query: str, max_results: int = 5) -> List[Dict]:
    """联网搜索，使用 DuckDuckGo，返回 [{title, body, href}, ...]"""
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=12) as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        return results or []
    except Exception as e:
        print(f'[chat] web search error: {e}')
        return []


def _format_search_context(results: List[Dict], query: str) -> str:
    """将搜索结果格式化为注入系统提示的文本"""
    if not results:
        return ''
    lines = [f'以下是关于「{query}」的最新联网搜索结果（请基于这些信息回答）：\n']
    for i, r in enumerate(results, 1):
        title = r.get('title', '').strip()
        body = r.get('body', '').strip()[:300]
        href = r.get('href', '')
        lines.append(f'[{i}] {title}\n{body}\n来源: {href}\n')
    lines.append('\n请综合以上实时搜索结果回答用户问题，并在适当位置注明信息来源。')
    return '\n'.join(lines)


def _generate_system_prompt(topic: str) -> str:
    """根据选中关键词生成差异化系统提示词"""
    if topic:
        return (
            f"你是时博士（Dr. Shi）的专业助手。当前对话话题是【{topic}】。\n"
            f"请围绕「{topic}」这一主题，为用户提供专业、深入、有洞察力的分析和解答。\n"
            "回答要条理清晰、逻辑严密，结合实际案例，使用中文。"
        )
    return (
        "你是时博士（Dr. Shi）的专业助手，擅长法律、政策、信息聚合分析等领域。\n"
        "请为用户提供专业的分析和解答，中文回复，内容深入，条理清晰。"
    )


def _stream_openai(api_key: str, base_url: str, model_id: str, messages: list, timeout: int = 60) -> Generator:
    """OpenAI兼容接口流式调用"""
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }
    payload = {
        'model': model_id,
        'messages': messages,
        'stream': True,
        'max_tokens': 2048,
    }
    with requests.post(
        f'{base_url}/chat/completions',
        headers=headers,
        json=payload,
        stream=True,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines():
            if not raw_line:
                continue
            line = raw_line.decode('utf-8') if isinstance(raw_line, bytes) else raw_line
            if not line.startswith('data:'):
                continue
            data_str = line[5:].strip()
            if data_str == '[DONE]':
                break
            try:
                chunk = json.loads(data_str)
                delta = chunk.get('choices', [{}])[0].get('delta', {})
                text = delta.get('content', '')
                if text:
                    yield text
            except (json.JSONDecodeError, IndexError, KeyError):
                continue


def _stream_anthropic(api_key: str, model_id: str, messages: list, system_prompt: str, timeout: int = 60) -> Generator:
    """Anthropic Claude API流式调用"""
    headers = {
        'x-api-key': api_key,
        'anthropic-version': '2023-06-01',
        'Content-Type': 'application/json',
    }
    payload = {
        'model': model_id,
        'messages': messages,
        'system': system_prompt,
        'max_tokens': 2048,
        'stream': True,
    }
    with requests.post(
        'https://api.anthropic.com/v1/messages',
        headers=headers,
        json=payload,
        stream=True,
        timeout=timeout,
    ) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines():
            if not raw_line:
                continue
            line = raw_line.decode('utf-8') if isinstance(raw_line, bytes) else raw_line
            if not line.startswith('data:'):
                continue
            data_str = line[5:].strip()
            try:
                ev = json.loads(data_str)
                if ev.get('type') == 'content_block_delta':
                    text = ev.get('delta', {}).get('text', '')
                    if text:
                        yield text
            except (json.JSONDecodeError, KeyError):
                continue


# ─────────────────────────────────────────────
# GET /api/chat/config
# ─────────────────────────────────────────────
@chat_bp.route('/api/chat/config', methods=['GET'])
def get_chat_config():
    cfg = _load_config()
    # 不暴露 API Key 明文，只返回是否已设置
    safe = {
        'active_model': cfg.get('active_model', 'deepseek'),
        'ragflow_kb_id': cfg.get('ragflow_kb_id', ''),
        'models': {},
    }
    for mid, meta in MODEL_META.items():
        model_cfg = cfg.get('models', {}).get(mid, {})
        safe['models'][mid] = {
            'name': meta['name'],
            'model_id': model_cfg.get('model_id', meta['default_model']),
            'has_key': bool(model_cfg.get('api_key', '').strip()),
        }
    return jsonify({'success': True, 'config': safe})


# ─────────────────────────────────────────────
# POST /api/chat/config
# ─────────────────────────────────────────────
@chat_bp.route('/api/chat/config', methods=['POST'])
def save_chat_config():
    data = request.json or {}
    cfg = _load_config()

    if 'active_model' in data:
        cfg['active_model'] = data['active_model']
    if 'ragflow_kb_id' in data:
        cfg['ragflow_kb_id'] = data['ragflow_kb_id']

    # 更新单个模型的配置
    model_id = data.get('model_id')
    if model_id and model_id in MODEL_META:
        m = cfg.setdefault('models', {}).setdefault(model_id, {})
        if 'api_key' in data and data['api_key']:
            m['api_key'] = data['api_key']
        if 'model_name' in data and data['model_name']:
            m['model_id'] = data['model_name']

    _save_config(cfg)
    return jsonify({'success': True, 'message': '配置已保存'})


# ─────────────────────────────────────────────
# POST /api/chat/test
# ─────────────────────────────────────────────
@chat_bp.route('/api/chat/test', methods=['POST'])
def test_chat_connection():
    data = request.json or {}
    model_id = data.get('model_id', '')
    if model_id not in MODEL_META:
        return jsonify({'success': False, 'message': f'未知模型: {model_id}'})

    cfg = _load_config()
    model_cfg = cfg.get('models', {}).get(model_id, {})
    api_key = model_cfg.get('api_key', '').strip()
    model_name = model_cfg.get('model_id', MODEL_META[model_id]['default_model'])

    if not api_key:
        return jsonify({'success': False, 'message': f'{MODEL_META[model_id]["name"]} API Key 未设置'})

    meta = MODEL_META[model_id]
    messages = [{'role': 'user', 'content': 'Hi, reply with one word: OK'}]
    try:
        result_text = ''
        if meta['type'] == 'openai':
            for chunk in _stream_openai(api_key, meta['base_url'], model_name, messages, timeout=15):
                result_text += chunk
                if len(result_text) > 20:
                    break
        else:
            for chunk in _stream_anthropic(api_key, model_name, messages, '', timeout=15):
                result_text += chunk
                if len(result_text) > 20:
                    break

        return jsonify({'success': True, 'message': f'连接成功，响应: {result_text[:30]}'})
    except requests.exceptions.Timeout:
        return jsonify({'success': False, 'message': '连接超时，请检查网络或API地址'})
    except requests.exceptions.HTTPError as e:
        return jsonify({'success': False, 'message': f'HTTP错误: {e.response.status_code} - {e.response.text[:100]}'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'连接失败: {str(e)[:120]}'})


# ─────────────────────────────────────────────
# POST /api/chat/send  (SSE 流式响应)
# ─────────────────────────────────────────────
@chat_bp.route('/api/chat/send', methods=['POST'])
def send_chat_message():
    data = request.json or {}
    model_id = data.get('model', '')
    topic = data.get('topic', '')
    history = data.get('messages', [])  # [{role, content}, ...]
    web_search = bool(data.get('web_search', False))

    if model_id not in MODEL_META:
        return jsonify({'success': False, 'message': f'未知模型: {model_id}'}), 400

    cfg = _load_config()
    model_cfg = cfg.get('models', {}).get(model_id, {})
    api_key = model_cfg.get('api_key', '').strip()
    model_name = model_cfg.get('model_id', MODEL_META[model_id]['default_model'])

    if not api_key:
        def _err():
            yield f'data: {json.dumps({"type":"error","message":MODEL_META[model_id]["name"]+" API Key 未配置，请在设置中填写"})}\n\n'
        return Response(stream_with_context(_err()), mimetype='text/event-stream')

    # 取最后一条用户消息作为搜索 query
    last_user_msg = ''
    for m in reversed(history):
        if m.get('role') == 'user':
            last_user_msg = m.get('content', '')
            break

    meta = MODEL_META[model_id]

    def _generate():
        search_context = ''
        search_snippets = []

        # 联网搜索阶段
        if web_search and last_user_msg:
            yield f'data: {json.dumps({"type":"searching","message":f"正在搜索「{last_user_msg[:40]}」…"})}\n\n'
            try:
                results = _web_search(last_user_msg, max_results=5)
                if results:
                    search_context = _format_search_context(results, last_user_msg)
                    search_snippets = [
                        {'title': r.get('title', ''), 'href': r.get('href', '')}
                        for r in results[:5]
                    ]
                    yield f'data: {json.dumps({"type":"search_done","count":len(results),"snippets":search_snippets})}\n\n'
                else:
                    yield f'data: {json.dumps({"type":"search_done","count":0,"snippets":[]})}\n\n'
            except Exception as se:
                yield f'data: {json.dumps({"type":"search_done","count":0,"snippets":[],"warn":str(se)[:80]})}\n\n'

        # 构建最终系统提示（基础 + 搜索结果）
        base_prompt = _generate_system_prompt(topic)
        system_prompt = (base_prompt + '\n\n' + search_context) if search_context else base_prompt

        # 组装 messages
        if meta['type'] == 'openai':
            full_messages = [{'role': 'system', 'content': system_prompt}] + history
        else:
            full_messages = history

        try:
            if meta['type'] == 'openai':
                for chunk in _stream_openai(api_key, meta['base_url'], model_name, full_messages):
                    yield f'data: {json.dumps({"type":"chunk","content":chunk})}\n\n'
            else:
                for chunk in _stream_anthropic(api_key, model_name, full_messages, system_prompt):
                    yield f'data: {json.dumps({"type":"chunk","content":chunk})}\n\n'
            yield f'data: {json.dumps({"type":"done"})}\n\n'
        except requests.exceptions.Timeout:
            yield f'data: {json.dumps({"type":"error","message":"请求超时，请检查网络"})}\n\n'
        except requests.exceptions.HTTPError as e:
            msg = f'API错误 {e.response.status_code}: {e.response.text[:120]}'
            yield f'data: {json.dumps({"type":"error","message":msg})}\n\n'
        except Exception as e:
            yield f'data: {json.dumps({"type":"error","message":str(e)[:150]})}\n\n'

    return Response(
        stream_with_context(_generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'},
    )


# ─────────────────────────────────────────────
# POST /api/chat/save-article
# ─────────────────────────────────────────────
@chat_bp.route('/api/chat/save-article', methods=['POST'])
def save_chat_article():
    data = request.json or {}
    question = data.get('question', '').strip()
    answer = data.get('answer', '').strip()
    topic = data.get('topic', '').strip()
    model_name = data.get('model_name', 'AI助手')

    if not question or not answer:
        return jsonify({'success': False, 'message': '问题或回答内容为空'})

    from utils import get_china_time
    now_str = get_china_time().strftime('%Y-%m-%d %H:%M:%S')
    unique_id = uuid.uuid4().hex[:12]
    virtual_url = f'ai://chat/{unique_id}'
    title = (question[:60] + '…') if len(question) > 60 else question
    keyword = topic or 'AI对话'

    content = (
        f"问：{question}\n\n"
        f"答：{answer}\n\n"
        f"---\n来源：在线助手（{model_name}）\n时间：{now_str}\n话题：{keyword}"
    )

    # 写入 SQLite
    article_id = None
    try:
        from sqlite_database import sqlite_db
        article_id = sqlite_db.insert_article({
            'url': virtual_url,
            'title': title,
            'content': content,
            'domain': 'ai.chat.local',
            'matched_keywords': keyword,
            'extraction_method': 'ai_chat',
            'quality_score': 80,
        })
    except Exception as e:
        print(f'[chat] 保存文章到SQLite失败: {e}')

    # 上传到 RAGFlow
    ragflow_result = None
    cfg = _load_config()
    kb_id = cfg.get('ragflow_kb_id', '').strip()
    if kb_id:
        try:
            from ragflow_client import RagflowClient
            import hashlib
            client = RagflowClient()
            safe_title = title[:60]
            digest = hashlib.sha1(virtual_url.encode()).hexdigest()[:8]
            file_name = f"{safe_title}_{digest}.txt"
            ragflow_result = client.upload_document_content(kb_id, file_name, content)
        except Exception as e:
            ragflow_result = {'error': str(e)}
            print(f'[chat] 上传RAGFlow失败: {e}')

    return jsonify({
        'success': True,
        'article_id': article_id,
        'ragflow': ragflow_result,
        'message': '已保存' + ('并上传到RAGFlow' if kb_id and not (ragflow_result or {}).get('error') else ''),
    })
