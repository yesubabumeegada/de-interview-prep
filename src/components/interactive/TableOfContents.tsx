import React, { useState, useEffect } from 'react';

/**
 * TableOfContents — Auto-generated floating TOC from page headings.
 * Shows on desktop (lg+) as a sticky right sidebar.
 * Highlights current section based on scroll position.
 */

interface TocItem {
  id: string;
  text: string;
  level: number;
}

export default function TableOfContents() {
  const [headings, setHeadings] = useState<TocItem[]>([]);
  const [activeId, setActiveId] = useState<string>('');

  useEffect(() => {
    // Extract headings from the content-renderer
    const contentEl = document.querySelector('.content-renderer');
    if (!contentEl) return;

    const elements = contentEl.querySelectorAll('h2, h3');
    const items: TocItem[] = [];

    elements.forEach((el, index) => {
      // Ensure heading has an ID for linking
      if (!el.id) {
        el.id = `heading-${index}-${el.textContent?.slice(0, 30).replace(/\s+/g, '-').toLowerCase() || index}`;
      }
      items.push({
        id: el.id,
        text: el.textContent || '',
        level: el.tagName === 'H2' ? 2 : 3,
      });
    });

    setHeadings(items);

    // Intersection observer for active heading tracking
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id);
          }
        });
      },
      { rootMargin: '-80px 0px -70% 0px', threshold: 0.1 }
    );

    elements.forEach((el) => observer.observe(el));
    return () => observer.disconnect();
  }, []);

  if (headings.length < 3) return null; // Don't show TOC for short content

  return (
    <nav
      className="hidden 2xl:block fixed right-8 top-[80px] w-[180px] max-h-[calc(100vh-120px)] overflow-y-auto text-xs"
      aria-label="Table of contents"
      style={{ fontSize: '0.7rem' }}
    >
      <p className="font-semibold uppercase tracking-wider text-[var(--color-content-tertiary)] mb-2" style={{ fontSize: '0.65rem' }}>
        On this page
      </p>
      <ul className="space-y-1 border-l-2 border-[var(--color-surface-tertiary)]">
        {headings.map((heading) => (
          <li key={heading.id}>
            <a
              href={`#${heading.id}`}
              onClick={(e) => {
                e.preventDefault();
                document.getElementById(heading.id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
              }}
              className={`block text-xs leading-relaxed py-0.5 transition-all duration-150 border-l-2 -ml-[2px] ${
                heading.level === 3 ? 'pl-5' : 'pl-3'
              } ${
                activeId === heading.id
                  ? 'border-[var(--color-primary)] text-[var(--color-primary)] font-medium'
                  : 'border-transparent text-[var(--color-content-tertiary)] hover:text-[var(--color-content-secondary)] hover:border-[var(--color-surface-tertiary)]'
              }`}
            >
              {heading.text.length > 35 ? heading.text.slice(0, 35) + '...' : heading.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}
