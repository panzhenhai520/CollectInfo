#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
内容处理模块
包含文章提取、清洗等功能
"""

import re
import requests
from newspaper import Article
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


def extract_with_newspaper3k(url):
    """使用 Newspaper3k 提取内容"""
    try:
        # 🔧 标准化URL（转换移动版为桌面版）
        original_url = url
        try:
            from url_transformation_rules import transform_url
            url = transform_url(url, verbose=False, verify=False)
            if url != original_url:
                print(f"🔄 URL已标准化: {original_url[:50]}... → {url[:50]}...")
        except:
            pass  # 如果转换失败，使用原URL
        
        # 创建Article对象
        article = Article(url, language='zh')
        
        # 下载和解析
        article.download()
        article.parse()
        
        # 获取页面源代码来提取链接
        links = []
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            soup = BeautifulSoup(response.content, 'html.parser')
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text_content = link.get_text(strip=True)
                if href and text_content:
                    absolute_url = urljoin(url, href)
                    # 🔧 也转换链接中的移动版URL
                    try:
                        absolute_url = transform_url(absolute_url, verbose=False, verify=False)
                    except:
                        pass
                    links.append({'url': absolute_url, 'text': text_content})
        except:
            pass
        
        return {
            'method': 'Newspaper3k',
            'url': url,  # 返回标准化后的URL
            'success': True,
            'title': article.title,
            'author': ', '.join(article.authors) if article.authors else None,
            'date': str(article.publish_date) if article.publish_date else None,
            'text_content': article.text,
            'text_length': len(article.text) if article.text else 0,
            'links_found': len(links),
            'sample_links': links[:5],  # 前5个链接作为示例
            'summary': article.summary if hasattr(article, 'summary') else None
        }
    except Exception as e:
        return {
            'method': 'Newspaper3k',
            'url': url,
            'success': False,
            'error': str(e)
        }


def extract_article_links_from_list_page(list_url):
    """从列表页面智能提取文章链接"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(list_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        base_domain = urlparse(list_url).netloc
        
        # 移除导航、页脚、侧边栏等非内容区域
        for element in soup(['nav', 'footer', 'aside', 'header']):
            element.decompose()
        
        # 查找所有链接
        all_links = soup.find_all('a', href=True)
        article_links = []
        seen_urls = set()
        
        for link in all_links:
            href = link.get('href', '').strip()
            text = link.get_text(strip=True)
            
            if not href or not text or len(text) < 5:
                continue
            
            # 转换为绝对URL
            absolute_url = urljoin(list_url, href)
            
            # 🔧 转换移动版URL为桌面版（防止爬到移动版链接）
            try:
                from url_transformation_rules import transform_url
                absolute_url = transform_url(absolute_url, verbose=False, verify=False)
            except:
                pass  # 如果转换失败，使用原URL
            
            # 基本过滤条件
            if absolute_url in seen_urls:
                continue
            
            # 检查URL是否在同一个域名下（或相关子域）
            link_domain = urlparse(absolute_url).netloc
            if base_domain not in link_domain and link_domain not in base_domain:
                continue
            
            # 智能识别文章链接的特征
            is_article_link = False
            
            # 1. URL路径特征
            url_path = urlparse(absolute_url).path.lower()
            article_path_patterns = [
                r'/news/',
                r'/article/',
                r'/post/',
                r'/blog/',
                r'/detail/',
                r'/content/',
                r'/story/',
                r'/press/',
                r'/publication/',
                r'/insights/',
                r'/legal-update/',
                r'/expertise/',
                r'/experience/',
                r'/\d{4}/',  # 年份
                r'/\d{4}-\d{2}/',  # 年月
                r'/\d{4}/\d{2}/',  # 年/月
                r'\.html',
                r'\.htm',
                r'\.aspx',
                r'\.php'
            ]
            
            for pattern in article_path_patterns:
                if re.search(pattern, url_path):
                    is_article_link = True
                    break
            
            # 2. 链接文本特征
            if not is_article_link:
                text_lower = text.lower()
                article_text_indicators = [
                    # 中文特征
                    r'[\u4e00-\u9fff].*[\u4e00-\u9fff]',  # 包含中文字符
                    # 标题特征
                    r'.{10,}',  # 较长的文本
                ]
                
                # 排除导航类文本
                navigation_keywords = [
                    '首页', '主页', 'home', 'index',
                    '关于', 'about', '联系', 'contact',
                    '服务', 'service', '产品', 'product',
                    '新闻中心', '更多', 'more', '全部',
                    '上一页', '下一页', 'prev', 'next',
                    '登录', 'login', '注册', 'register',
                    '搜索', 'search', '帮助', 'help',
                    '网站地图', 'sitemap', '法律声明',
                    '隐私政策', 'privacy', '免责声明',
                    '版权', 'copyright', '友情链接',
                    '返回顶部', 'top', '分享', 'share',
                    '打印', 'print', '收藏', 'favorite',
                    '中文', 'en', 'english', '繁体',
                    '简体', '语言', 'language'
                ]
                
                is_navigation = any(keyword in text_lower for keyword in navigation_keywords)
                
                if not is_navigation:
                    for pattern in article_text_indicators:
                        if re.search(pattern, text, re.IGNORECASE):
                            is_article_link = True
                            break
            
            # 3. HTML结构特征
            if not is_article_link:
                # 检查链接是否在文章列表容器中
                parent_classes = []
                parent = link.parent
                while parent and parent.name:
                    if parent.get('class'):
                        parent_classes.extend(parent.get('class'))
                    if parent.get('id'):
                        parent_classes.append(parent.get('id'))
                    parent = parent.parent
                    if len(parent_classes) > 20:  # 避免无限循环
                        break
                
                article_container_keywords = [
                    'news', 'article', 'post', 'blog', 'content',
                    'list', 'item', 'entry', 'story', 'publication',
                    'insight', 'update', 'press', 'media'
                ]
                
                parent_classes_str = ' '.join(parent_classes).lower()
                for keyword in article_container_keywords:
                    if keyword in parent_classes_str:
                        is_article_link = True
                        break
            
            # 4. 过滤明显不是文章的链接
            excluded_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar']
            excluded_keywords = ['mailto:', 'javascript:', 'tel:', '#', 'void(0)']
            
            should_exclude = False
            for ext in excluded_extensions:
                if ext in absolute_url.lower():
                    should_exclude = True
                    break
            
            if not should_exclude:
                for keyword in excluded_keywords:
                    if keyword in absolute_url.lower():
                        should_exclude = True
                        break
            
            if should_exclude:
                continue
            
            # 添加符合条件的链接
            if is_article_link and len(text) >= 5:
                article_links.append({
                    'url': absolute_url,
                    'text': text[:100],  # 限制文本长度
                    'confidence': 'high' if len(text) > 20 else 'medium'
                })
                seen_urls.add(absolute_url)
        
        # 按置信度和文本长度排序
        article_links.sort(key=lambda x: (
            x['confidence'] == 'high',
            len(x['text']),
            -len(x['url'])
        ), reverse=True)
        
        return {
            'success': True,
            'links': article_links[:20],  # 最多返回20个链接
            'total_found': len(article_links),
            'extraction_method': 'intelligent_pattern_matching'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f'提取文章链接失败: {str(e)}'
        }


def is_valid_article_content(content, title=""):
    """
    检查内容是否是有效的文章内容 - 改进版本
    """
    if not content or len(content.strip()) < 50:  # 提高最小长度要求
        return False
    
    # 检查是否包含明显的中文内容
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', content))
    if chinese_chars < 5:  # 至少包含5个中文字符
        return False
    
    # 过滤掉明显不是文章的内容
    invalid_patterns = [
        r'^\[!\[.*?\]\(.*?\)\]\(.*?\)$',  # 纯图片链接
        r'^en\s*$',  # 单独的"en"
        r'^[a-zA-Z\s]+$',  # 纯英文且很短
        r'^[\d\s\-_\.]+$',  # 纯数字和符号
        r'^[^\u4e00-\u9fff]*$',  # 不包含中文
        r'^.{1,20}$',  # 太短的内容
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, content.strip(), re.IGNORECASE):
            return False
    
    # 检查是否包含明显的文章内容特征
    article_indicators = [
        r'年\d+月\d+日',  # 日期格式
        r'近日|最近|近期',  # 时间指示词
        r'据悉|据了解|据报道',  # 新闻常用词
        r'律师|法律|法规|政策',  # 法律相关词
        r'公司|企业|机构|组织',  # 实体词
        r'项目|交易|融资|上市',  # 业务词
        r'成功|完成|获得|荣获',  # 成就词
        r'表示|指出|强调|认为',  # 观点词
        r'根据|按照|依据|规定',  # 依据词
        r'本次|此次|该|此',  # 指代词
        r'君合|合伙人|律师',  # 君合特定词
        r'助力|协助|担任',  # 服务词
        r'两江潮涌|重庆分所|开业盛典',  # 具体文章标题
        r'高端制造|法总闭门会',  # 具体文章标题
        r'破局重生|破产重整',  # 具体文章标题
        r'中企出海|国际规则',  # 具体文章标题
        r'诚挚欢迎|顾问|律师',  # 人员相关
        r'西雅图|华人律师协会',  # 具体内容
        r'溯江而上|聚渝都',  # 具体内容
        r'伦敦国际仲裁院',  # 具体内容
    ]
    
    # 如果包含文章特征，认为是有效内容
    for pattern in article_indicators:
        if re.search(pattern, content, re.IGNORECASE):
            return True
    
    # 如果内容长度足够，也认为是有效的
    if len(content.strip()) > 100:
        return True
    
    return False

