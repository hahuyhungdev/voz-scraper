export const solutionCodes: Record<string, string> = {
  autocomplete: `import React, { useState, useEffect, useRef } from 'react';

const mockFetchMovies = async (query: string): Promise<string[]> => {
  await new Promise((resolve) => setTimeout(resolve, 600)); // Delay 600ms
  if (Math.random() < 0.15) {
    throw new Error('Kết nối API thất bại. Vui lòng bấm tìm lại!');
  }
  const movies = [
    'The Dark Knight', 'Inception', 'Interstellar', 'The Matrix',
    'Pulp Fiction', 'Forrest Gump', 'Fight Club', 'Spirited Away'
  ];
  return movies.filter((movie) => movie.toLowerCase().includes(query.toLowerCase()));
};

export const AutoCompleteSolution: React.FC = () => {
  const [query, setQuery] = useState('');
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedIndex, setSelectedIndex] = useState(-1);
  const [showDropdown, setShowDropdown] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!query.trim()) {
      setSuggestions([]);
      setShowDropdown(false);
      return;
    }

    const timer = setTimeout(async () => {
      setIsLoading(true);
      setError(null);
      try {
        const data = await mockFetchMovies(query);
        setSuggestions(data);
        setShowDropdown(true);
        setSelectedIndex(-1);
      } catch (err) {
        setError((err as Error).message);
        setSuggestions([]);
      } finally {
        setIsLoading(false);
      }
    }, 300); // Debounce 300ms

    return () => clearTimeout(timer);
  }, [query]);

  // Click outside to close dropdown
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (!showDropdown || suggestions.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIndex((prev) => (prev + 1 >= suggestions.length ? 0 : prev + 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIndex((prev) => (prev - 1 < 0 ? suggestions.length - 1 : prev - 1));
    } else if (e.key === 'Enter') {
      if (selectedIndex >= 0) {
        setQuery(suggestions[selectedIndex]);
        setShowDropdown(false);
      }
    } else if (e.key === 'Escape') {
      setShowDropdown(false);
    }
  };

  return (
    <div ref={containerRef} style={{ position: 'relative' }}>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder="Nhập chữ cái tìm kiếm..."
      />
      {isLoading && <div>⏳ Đang tìm...</div>}
      {error && <div>❌ {error}</div>}
      {showDropdown && (
        <ul>
          {suggestions.map((m, idx) => (
            <li key={m} style={{ background: idx === selectedIndex ? '#333' : 'transparent' }}>
              {m}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};`,
  modal: `import React, { useState, useEffect, useRef } from 'react';

export const Modal: React.FC = ({ isOpen, onClose, title, children }) => {
  const modalRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (isOpen) {
      triggerRef.current = document.activeElement as HTMLElement;
      modalRef.current?.focus();
    } else {
      triggerRef.current?.focus();
    }
  }, [isOpen]);

  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    if (isOpen) window.addEventListener('keydown', handleEsc);
    return () => window.removeEventListener('keydown', handleEsc);
  }, [isOpen, onClose]);

  const handleTabKey = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key !== 'Tab' || !modalRef.current) return;
    const focusables = modalRef.current.querySelectorAll<HTMLElement>(
      'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );
    if (focusables.length === 0) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];

    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
    }
  };

  if (!isOpen) return null;

  return (
    <div role="dialog" aria-modal="true" onClick={onClose} className="overlay">
      <div ref={modalRef} tabIndex={-1} onKeyDown={handleTabKey} onClick={(e) => e.stopPropagation()} className="content">
        <h3>{title}</h3>
        <div>{children}</div>
        <button onClick={onClose}>Đóng</button>
      </div>
    </div>
  );
};`,
  'infinite-scroll': `import React, { useState, useEffect, useRef } from 'react';

export const InfiniteScrollSolution: React.FC = () => {
  const [posts, setPosts] = useState<Post[]>([]);
  const [page, setPage] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const sentinelRef = useRef<HTMLDivElement>(null);

  const loadMorePosts = async () => {
    setIsLoading(true);
    const newPosts = await mockFetchPosts(page, 10);
    if (newPosts.length === 0) setHasMore(false);
    else {
      setPosts((prev) => [...prev, ...newPosts]);
      setPage((prev) => prev + 1);
    }
    setIsLoading(false);
  };

  useEffect(() => {
    const observer = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting && hasMore && !isLoading) {
        loadMorePosts();
      }
    });
    if (sentinelRef.current) observer.observe(sentinelRef.current);
    return () => observer.disconnect();
  }, [page, hasMore, isLoading]);

  return (
    <div>
      {posts.map(post => <div key={post.id}>{post.title}</div>)}
      <div ref={sentinelRef}>
        {isLoading && '⏳ Đang tải thêm...'}
      </div>
    </div>
  );
};`,
  'star-rating': `import React, { useState } from 'react';

export const StarRating = ({ maxStars = 5, readOnly = false, value = 0, onChange }) => {
  const [rating, setRating] = useState(value);
  const [hoverRating, setHoverRating] = useState<number | null>(null);
  const displayRating = hoverRating !== null ? hoverRating : rating;

  const handleStarClick = (idx: number) => {
    if (readOnly) return;
    setRating(idx);
    if (onChange) onChange(idx);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (readOnly) return;
    if (e.key === 'ArrowRight') setRating(prev => Math.min(maxStars, prev + 1));
    if (e.key === 'ArrowLeft') setRating(prev => Math.max(0, prev - 1));
  };

  return (
    <div role="slider" tabIndex={0} onKeyDown={handleKeyDown} style={{ display: 'flex' }}>
      {Array.from({ length: maxStars }).map((_, i) => (
        <span
          key={i}
          onMouseEnter={() => !readOnly && setHoverRating(i + 1)}
          onMouseLeave={() => !readOnly && setHoverRating(null)}
          onClick={() => handleStarClick(i + 1)}
          style={{ color: i + 1 <= displayRating ? 'gold' : 'grey', fontSize: '30px' }}
        >
          ★
        </span>
      ))}
    </div>
  );
};`,
  'toast-notification': `import React, { useState } from 'react';

export const ToastSystem = () => {
  const [toasts, setToasts] = useState([]);

  const addToast = (message, type = 'success') => {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
    setTimeout(() => {
      setToasts(prev => prev.filter(t => t.id !== id));
    }, 3000);
  };

  return (
    <div>
      <button onClick={() => addToast('Success Alert!', 'success')}>Trigger</button>
      <div style={{ position: 'fixed', top: 20, right: 20 }}>
        {toasts.map(t => (
          <div key={t.id} className={\`toast \${t.type}\`}>
            {t.message}
          </div>
        ))}
      </div>
    </div>
  );
};`,
  'array-methods': `// 🟢 POLYFILL MAP
Array.prototype.myMap = function(callback, context) {
  if (this == null) throw new TypeError();
  if (typeof callback !== 'function') throw new TypeError();

  const O = Object(this);
  const len = O.length >>> 0;
  const A = new Array(len);

  for (let i = 0; i < len; i++) {
    if (i in O) {
      A[i] = callback.call(context, O[i], i, O);
    }
  }
  return A;
};

// 🟢 POLYFILL REDUCE
Array.prototype.myReduce = function(callback, initialValue) {
  if (this == null) throw new TypeError();
  if (typeof callback !== 'function') throw new TypeError();

  const O = Object(this);
  const len = O.length >>> 0;

  if (len === 0 && initialValue === undefined) {
    throw new TypeError();
  }

  let accumulator = initialValue;
  let startIndex = 0;

  if (initialValue === undefined) {
    let found = false;
    for (let i = 0; i < len; i++) {
      if (i in O) {
        accumulator = O[i];
        startIndex = i + 1;
        found = true;
        break;
      }
    }
    if (!found) throw new TypeError();
  }

  for (let i = startIndex; i < len; i++) {
    if (i in O) {
      accumulator = callback(accumulator, O[i], i, O);
    }
  }
  return accumulator;
};`,
  'debounce-throttle': `// 🟢 IMPLEMENT DEBOUNCE
export function debounce(fn, delay) {
  let timerId = null;

  const debounced = function(...args) {
    const context = this;
    if (timerId) clearTimeout(timerId);

    timerId = setTimeout(() => {
      fn.apply(context, args);
    }, delay);
  };

  debounced.cancel = () => {
    if (timerId) {
      clearTimeout(timerId);
      timerId = null;
    }
  };
  return debounced;
}

// 🟢 IMPLEMENT THROTTLE
export function throttle(fn, limit) {
  let lastFunc = null;
  let lastRan = 0;

  const throttled = function(...args) {
    const context = this;
    if (!lastRan) {
      fn.apply(context, args);
      lastRan = Date.now();
    } else {
      if (lastFunc) clearTimeout(lastFunc);
      const remaining = limit - (Date.now() - lastRan);

      if (remaining <= 0) {
        fn.apply(context, args);
        lastRan = Date.now();
      } else {
        lastFunc = setTimeout(() => {
          fn.apply(context, args);
          lastRan = Date.now();
          lastFunc = null;
        }, remaining);
      }
    }
  };

  throttled.cancel = () => {
    if (lastFunc) {
      clearTimeout(lastFunc);
      lastFunc = null;
    }
    lastRan = 0;
  };
  return throttled;
}`,
  'promise-all': `// 🟢 IMPLEMENT PROMISE.ALL POLYFILL
export function promiseAll(promises) {
  return new Promise((resolve, reject) => {
    if (!Array.isArray(promises)) {
      return reject(new TypeError('Input must be an array'));
    }

    const len = promises.length;
    if (len === 0) return resolve([]);

    const results = new Array(len);
    let completedCount = 0;

    for (let i = 0; i < len; i++) {
      Promise.resolve(promises[i])
        .then((val) => {
          results[i] = val;
          completedCount++;
          if (completedCount === len) {
            resolve(results);
          }
        })
        .catch((err) => {
          reject(err); // Fail-fast
        });
    }
  });
}`
};
