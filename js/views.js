// ===== Views Blog — Listing + Post Reader =====

let allPosts = [];
let currentPost = null;

// ---- Bootstrap ----
async function init() {
  await loadPosts();
  handleRouting();
  window.addEventListener('hashchange', handleRouting);
}

async function loadPosts() {
  try {
    const res = await fetch('views-data.json?v=' + Date.now());
    if (!res.ok) throw new Error('fetch failed');
    const data = await res.json();
    allPosts = data.posts || [];
  } catch (e) {
    allPosts = [];
  }
  renderListing(allPosts);
}

// ---- Routing via URL hash (#post-ID) ----
function handleRouting() {
  const hash = window.location.hash; // e.g. "#post-3"
  const match = hash.match(/^#post-(\d+)$/);
  if (match) {
    const post = allPosts.find(p => p.id === parseInt(match[1]));
    if (post) { showPost(post, false); return; }
  }
  showListing();
}

// ---- Listing ----
function renderListing(posts) {
  document.getElementById('loadingSkeleton').style.display = 'none';
  document.getElementById('postCount').innerHTML = `<strong>${posts.length}</strong> post${posts.length !== 1 ? 's' : ''}`;

  if (!posts.length) {
    document.getElementById('featuredPost').innerHTML = '';
    document.getElementById('postsGrid').innerHTML = '';
    document.getElementById('emptyState').style.display = 'block';
    return;
  }

  document.getElementById('emptyState').style.display = 'none';
  renderTagBar(allPosts);

  const [featured, ...rest] = posts;
  renderFeatured(featured);
  renderGrid(rest);
}

function renderFeatured(post) {
  document.getElementById('featuredPost').innerHTML = `
    <div class="featured-post" onclick="navigateToPost(${post.id})" tabindex="0" role="button"
         onkeydown="if(event.key==='Enter') navigateToPost(${post.id})">
      <div class="featured-label">✦ Latest</div>
      <h2 class="featured-title">${escHtml(post.title)}</h2>
      <p class="featured-summary">${escHtml(post.summary)}</p>
      <div class="post-byline">
        <div class="byline-avatar">S</div>
        <span class="byline-author">Sang Patil</span>
        <span class="byline-sep">·</span>
        <span class="byline-date">${post.date}</span>
        <span class="byline-sep">·</span>
        <span class="byline-read-time">${readTime(post.content)} min read</span>
      </div>
      ${post.tags && post.tags.length ? `
        <div class="post-tags-list">
          ${post.tags.map(t => `<span class="post-tag-pill">${escHtml(t)}</span>`).join('')}
        </div>` : ''}
    </div>`;
}

function renderGrid(posts) {
  if (!posts.length) { document.getElementById('postsGrid').innerHTML = ''; return; }
  document.getElementById('postsGrid').innerHTML = posts.map(post => `
    <article class="post-item" onclick="navigateToPost(${post.id})" tabindex="0" role="button"
             onkeydown="if(event.key==='Enter') navigateToPost(${post.id})">
      <div class="post-item-date">${post.date}</div>
      <h3 class="post-item-title">${escHtml(post.title)}</h3>
      <p class="post-item-summary">${escHtml(post.summary)}</p>
      <div class="post-item-footer">
        ${post.tags && post.tags.length
          ? `<div class="post-tags-list" style="margin:0;">${post.tags.slice(0,2).map(t => `<span class="post-tag-pill">${escHtml(t)}</span>`).join('')}</div>`
          : '<span></span>'}
        <span class="post-item-read">${readTime(post.content)} min read →</span>
      </div>
    </article>`).join('');
}

// ---- Tag filter ----
function renderTagBar(posts) {
  const counts = {};
  posts.forEach(p => (p.tags || []).forEach(t => { counts[t] = (counts[t] || 0) + 1; }));
  const tags = Object.keys(counts).sort((a, b) => counts[b] - counts[a]);
  if (!tags.length) return;

  const bar = document.getElementById('tagBarInner');
  tags.forEach(tag => {
    const btn = document.createElement('button');
    btn.className = 'tag-pill';
    btn.textContent = tag;
    btn.onclick = () => filterTag(tag, btn);
    bar.appendChild(btn);
  });
  document.getElementById('tagBar').style.display = 'block';
}

function filterTag(tag, btn) {
  document.querySelectorAll('.tag-pill').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');

  const filtered = tag ? allPosts.filter(p => (p.tags || []).includes(tag)) : allPosts;
  if (!filtered.length) { renderListing([]); return; }
  const [featured, ...rest] = filtered;
  renderFeatured(featured);
  renderGrid(rest);
}

// ---- Post reader ----
function navigateToPost(id) {
  const post = allPosts.find(p => p.id === id);
  if (!post) return;
  history.pushState(null, '', `#post-${id}`);
  showPost(post, true);
}

function showPost(post, scroll) {
  currentPost = post;
  document.title = `${post.title} — Views by Sang Patil`;

  // Header
  document.getElementById('postHeader').innerHTML = `
    ${post.tags && post.tags.length
      ? `<div class="post-header-tags">${post.tags.map(t => `<span class="post-header-tag">${escHtml(t)}</span>`).join('')}</div>`
      : ''}
    <h1 class="post-full-title">${escHtml(post.title)}</h1>
    <p class="post-full-summary">${escHtml(post.summary)}</p>
    <div class="post-full-meta">
      <div class="byline-avatar">S</div>
      <span class="byline-author">Sang Patil</span>
      <span class="byline-sep" style="color:var(--blog-border)">·</span>
      <span class="byline-date">${post.date}</span>
      <span class="byline-sep" style="color:var(--blog-border)">·</span>
      <span class="byline-read-time">${readTime(post.content)} min read</span>
    </div>`;

  // Body — content is stored as HTML
  document.getElementById('postBody').innerHTML = post.content;

  // Footer
  document.getElementById('postDateFooter').textContent = post.date;

  // Switch views
  document.getElementById('listing-view').classList.add('hidden');
  document.getElementById('post-view').classList.add('active');

  if (scroll) window.scrollTo({ top: 0, behavior: 'smooth' });
}

function showListing() {
  document.title = 'Views by Sang Patil';
  currentPost = null;
  document.getElementById('post-view').classList.remove('active');
  document.getElementById('listing-view').classList.remove('hidden');
  if (!window.location.hash.startsWith('#post-')) return;
  history.pushState(null, '', window.location.pathname);
}

// ---- Copy link ----
function copyPostLink() {
  const url = window.location.origin + window.location.pathname + window.location.hash;
  navigator.clipboard.writeText(url).then(() => {
    const btn = document.getElementById('copyLinkBtn');
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy link'; }, 2000);
  });
}

// ---- Helpers ----
function readTime(html) {
  const text = (html || '').replace(/<[^>]+>/g, ' ');
  const words = text.trim().split(/\s+/).length;
  return Math.max(1, Math.round(words / 220));
}

function escHtml(str) {
  return String(str || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ---- Init ----
init();
