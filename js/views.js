// ===== Views Page - Load & Render Posts =====

let allPosts = [];
let activeTag = null;

async function loadPosts() {
  try {
    const res = await fetch('views-data.json?v=' + Date.now());
    if (!res.ok) throw new Error('Failed to load');
    const data = await res.json();
    allPosts = data.posts || [];
    renderPosts(allPosts);
    renderTags(allPosts);
  } catch (err) {
    document.getElementById('postsList').innerHTML = `
      <div class="empty-state">
        <div class="icon">📭</div>
        <h3>No posts yet</h3>
        <p>Trigger the GitHub Actions workflow to generate the first post.</p>
      </div>`;
  }
}

function renderPosts(posts) {
  const list = document.getElementById('postsList');
  if (!posts.length) {
    list.innerHTML = `
      <div class="empty-state">
        <div class="icon">✦</div>
        <h3>No posts yet</h3>
        <p>Run the <em>Add View</em> GitHub Actions workflow to generate the first post.</p>
      </div>`;
    return;
  }

  list.innerHTML = posts.map(post => `
    <article class="post-card" onclick="openPost(${post.id})" tabindex="0" role="button"
             onkeydown="if(event.key==='Enter') openPost(${post.id})">
      <div class="post-meta">
        <span class="post-date">${post.date}</span>
        ${post.tags && post.tags[0] ? `<span class="post-tag">${post.tags[0]}</span>` : ''}
      </div>
      <h2 class="post-title">${escHtml(post.title)}</h2>
      <p class="post-summary">${escHtml(post.summary)}</p>
      <div class="post-tags">
        ${(post.tags || []).map(t => `<span class="tag" data-tag="${escHtml(t)}">${escHtml(t)}</span>`).join('')}
      </div>
      <span class="post-read-more">Read post →</span>
    </article>
  `).join('');
}

function renderTags(posts) {
  const tagCounts = {};
  posts.forEach(p => (p.tags || []).forEach(t => { tagCounts[t] = (tagCounts[t] || 0) + 1; }));
  const sorted = Object.entries(tagCounts).sort((a, b) => b[1] - a[1]);
  const container = document.getElementById('tagsContainer');
  if (!sorted.length) { document.getElementById('tagsCloud').style.display = 'none'; return; }
  container.innerHTML = sorted.map(([tag]) => `
    <button class="skill-chip tag-filter" onclick="filterByTag('${escHtml(tag)}')">${escHtml(tag)}</button>
  `).join('');
}

function filterByTag(tag) {
  if (activeTag === tag) {
    activeTag = null;
    renderPosts(allPosts);
    document.querySelectorAll('.tag-filter').forEach(b => b.classList.remove('accent'));
  } else {
    activeTag = tag;
    renderPosts(allPosts.filter(p => (p.tags || []).includes(tag)));
    document.querySelectorAll('.tag-filter').forEach(b => {
      b.classList.toggle('accent', b.textContent === tag);
    });
  }
}

function openPost(id) {
  const post = allPosts.find(p => p.id === id);
  if (!post) return;

  const content = document.getElementById('modalContent');
  content.innerHTML = `
    <div class="post-meta" style="margin-bottom: 16px;">
      ${(post.tags || []).map(t => `<span class="post-tag">${escHtml(t)}</span>`).join('')}
    </div>
    <h3>${escHtml(post.title)}</h3>
    <span class="post-date-full">${post.date}</span>
    <div class="post-body">${post.content}</div>
    ${post.prompt ? `<div style="margin-top: 32px; padding-top: 24px; border-top: 1px solid var(--border);">
      <p style="font-size: 0.78rem; color: var(--text-muted); font-family: var(--font-mono);">
        ✦ Generated from prompt: "${escHtml(post.prompt)}"
      </p>
    </div>` : ''}
  `;

  const modal = document.getElementById('postModal');
  modal.classList.add('open');
  document.body.style.overflow = 'hidden';
  modal.scrollTop = 0;
}

function closePost() {
  document.getElementById('postModal').classList.remove('open');
  document.body.style.overflow = '';
}

document.getElementById('modalClose')?.addEventListener('click', closePost);
document.getElementById('postModal')?.addEventListener('click', (e) => {
  if (e.target.id === 'postModal') closePost();
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') closePost();
});

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Init
loadPosts();
