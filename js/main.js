// ===== Modern Portfolio - Main JS =====

// Nav scroll effect
const nav = document.querySelector('nav');
window.addEventListener('scroll', () => {
  nav?.classList.toggle('scrolled', window.scrollY > 20);
});

// Mobile menu
const navToggle = document.getElementById('navToggle');
const mobileMenu = document.getElementById('mobileMenu');
navToggle?.addEventListener('click', () => {
  mobileMenu?.classList.toggle('open');
  navToggle.textContent = mobileMenu?.classList.contains('open') ? '✕' : '☰';
});

// Close mobile menu on link click
document.querySelectorAll('.mobile-menu a').forEach(a => {
  a.addEventListener('click', () => {
    mobileMenu?.classList.remove('open');
    navToggle.textContent = '☰';
  });
});

// Active nav link on scroll
const sections = document.querySelectorAll('section[id]');
const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');

function updateActiveLink() {
  const scrollY = window.scrollY + 100;
  sections.forEach(section => {
    const top = section.offsetTop;
    const height = section.offsetHeight;
    const id = section.getAttribute('id');
    const link = document.querySelector(`.nav-links a[href="#${id}"]`);
    if (scrollY >= top && scrollY < top + height) {
      navLinks.forEach(l => l.classList.remove('active'));
      link?.classList.add('active');
    }
  });
}

window.addEventListener('scroll', updateActiveLink, { passive: true });

// Intersection Observer for fade-in animations
const fadeObserver = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('visible');
    }
  });
}, { threshold: 0.1, rootMargin: '0px 0px -50px 0px' });

document.querySelectorAll('.fade-up, .timeline-item').forEach(el => {
  fadeObserver.observe(el);
});

// Typing animation in hero
const typedEl = document.getElementById('typedTitle');
if (typedEl) {
  const phrases = [
    'Software Engineer',
    'Systems Builder',
    'Cloud Architect',
    'Problem Solver',
  ];
  let phraseIdx = 0;
  let charIdx = 0;
  let deleting = false;
  let pause = false;

  function typeEffect() {
    if (pause) return;
    const current = phrases[phraseIdx];

    if (!deleting) {
      typedEl.textContent = current.slice(0, charIdx + 1);
      charIdx++;
      if (charIdx === current.length) {
        pause = true;
        setTimeout(() => { deleting = true; pause = false; }, 2200);
      }
    } else {
      typedEl.textContent = current.slice(0, charIdx - 1);
      charIdx--;
      if (charIdx === 0) {
        deleting = false;
        phraseIdx = (phraseIdx + 1) % phrases.length;
      }
    }
    setTimeout(typeEffect, deleting ? 60 : 90);
  }
  setTimeout(typeEffect, 800);
}
