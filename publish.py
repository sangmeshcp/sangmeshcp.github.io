#!/usr/bin/env python3
"""
publish.py — publish a blog post to Views

Usage:
  python3 publish.py --title "My Post" --file post.md --tags "ai,systems"
  python3 publish.py --title "My Post" --content "## Hello\n\nBody text..." --tags "ai"

Called directly by Claude Code when you say "publish this post".
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime


# ── Markdown → HTML ────────────────────────────────────────────────────────────

def inline_md(text):
    text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    text = re.sub(r'\*\*\*(.*?)\*\*\*', r'<strong><em>\1</em></strong>', text)
    text = re.sub(r'\*\*(.*?)\*\*',     r'<strong>\1</strong>', text)
    text = re.sub(r'__(.*?)__',         r'<strong>\1</strong>', text)
    text = re.sub(r'\*((?!\s).*?(?<!\s))\*', r'<em>\1</em>', text)
    text = re.sub(r'_((?!_)(?!\s).*?(?<!\s))_', r'<em>\1</em>', text)
    text = re.sub(r'`([^`]+)`',         r'<code>\1</code>', text)
    text = re.sub(r'\[([^\]]+)\]\((https?://[^\)]+)\)',
                  r'<a href="\2" target="_blank" rel="noopener">\1</a>', text)
    return text


def md_to_html(md):
    lines = md.split('\n')
    out = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Fenced code block
        if line.startswith('```'):
            lang = line[3:].strip()
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].startswith('```'):
                code_lines.append(lines[i])
                i += 1
            code = '\n'.join(code_lines).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
            out.append(f'<pre><code class="language-{lang}">{code}</code></pre>')
            i += 1; continue

        # Blockquote
        if line.startswith('> '):
            bq = []
            while i < len(lines) and lines[i].startswith('> '):
                bq.append(lines[i][2:])
                i += 1
            out.append(f'<blockquote><p>{inline_md(" ".join(bq))}</p></blockquote>')
            continue

        # Headings
        m = re.match(r'^(#{1,4})\s+(.*)', line)
        if m:
            lvl = len(m.group(1))
            out.append(f'<h{lvl}>{inline_md(m.group(2))}</h{lvl}>')
            i += 1; continue

        # Horizontal rule
        if re.match(r'^[-*_]{3,}$', line.strip()):
            out.append('<hr>'); i += 1; continue

        # Unordered list
        if re.match(r'^[-*+] ', line):
            items = []
            while i < len(lines) and re.match(r'^[-*+] ', lines[i]):
                items.append(f'<li>{inline_md(lines[i][2:])}</li>')
                i += 1
            out.append('<ul>' + ''.join(items) + '</ul>')
            continue

        # Ordered list
        if re.match(r'^\d+\. ', line):
            items = []
            while i < len(lines) and re.match(r'^\d+\. ', lines[i]):
                items.append(f'<li>{inline_md(re.sub(r"^\d+\. ", "", lines[i]))}</li>')
                i += 1
            out.append('<ol>' + ''.join(items) + '</ol>')
            continue

        # Blank line
        if not line.strip():
            i += 1; continue

        # Paragraph (accumulate until blank line or block element)
        para = [line]
        i += 1
        while i < len(lines):
            l = lines[i]
            if not l.strip(): break
            if re.match(r'^#{1,4}\s', l): break
            if l.startswith('```'): break
            if l.startswith('> '): break
            if re.match(r'^[-*+] ', l): break
            if re.match(r'^\d+\. ', l): break
            if re.match(r'^[-*_]{3,}$', l.strip()): break
            para.append(l)
            i += 1
        out.append(f'<p>{inline_md(" ".join(para))}</p>')

    return '\n'.join(out)


def extract_summary(html, max_chars=280):
    """Pull plain text from first <p> as summary."""
    plain = re.sub(r'<[^>]+>', ' ', html)
    plain = re.sub(r'\s+', ' ', plain).strip()
    if len(plain) <= max_chars:
        return plain
    cut = plain[:max_chars].rsplit(' ', 1)[0]
    return cut + '…'


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Publish a post to Views')
    parser.add_argument('--title',   required=True, help='Post title')
    parser.add_argument('--content', default='',    help='Markdown content (string)')
    parser.add_argument('--file',    default='',    help='Path to a .md file')
    parser.add_argument('--summary', default='',    help='Short summary (auto-extracted if blank)')
    parser.add_argument('--tags',    default='',    help='Comma-separated tags')
    parser.add_argument('--no-push', action='store_true', help='Skip git commit/push')
    args = parser.parse_args()

    # ── Get markdown content ──────────────────────────────────────────
    md = ''
    if args.file:
        with open(args.file, 'r') as f:
            md = f.read()
    elif args.content:
        md = args.content
    else:
        print('Reading markdown from stdin (Ctrl+D when done)...\n')
        md = sys.stdin.read()

    if not md.strip():
        print('Error: no content provided.'); sys.exit(1)

    # ── Convert ──────────────────────────────────────────────────────
    html = md_to_html(md)
    summary = args.summary.strip() or extract_summary(html)
    tags = [t.strip().lower() for t in args.tags.split(',') if t.strip()]

    # ── Load data ────────────────────────────────────────────────────
    data_path = os.path.join(os.path.dirname(__file__), 'views-data.json')
    with open(data_path, 'r') as f:
        data = json.load(f)

    ids = [p['id'] for p in data.get('posts', [])]
    new_id = max(ids) + 1 if ids else 1

    post = {
        'id':      new_id,
        'date':    datetime.now().strftime('%B %d, %Y'),
        'title':   args.title,
        'summary': summary,
        'content': html,
        'tags':    tags,
    }

    data.setdefault('posts', []).insert(0, post)
    data['meta'] = {
        'last_updated': datetime.now().strftime('%Y-%m-%d'),
        'total_posts':  len(data['posts']),
    }

    with open(data_path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    url = f'https://www.sangameshpatil.com/views.html#post-{new_id}'
    print(f'\n✦ Published: {args.title}')
    print(f'  ID:   {new_id}')
    print(f'  Tags: {", ".join(tags) or "(none)"}')
    print(f'  URL:  {url}\n')

    # ── Git ───────────────────────────────────────────────────────────
    if not args.no_push:
        os.system('git add views-data.json')
        os.system(f'git commit -m "✦ Publish: {args.title}"')
        os.system('git push origin HEAD')
        print('Pushed to remote.')

    return new_id


if __name__ == '__main__':
    main()
