import React, { useState, useEffect, useRef } from 'react';

const mockFetchMovies = async (query: string): Promise<string[]> => {
  await new Promise((resolve) => setTimeout(resolve, 600)); // Delay 600ms
  if (Math.random() < 0.15) {
    throw new Error('Kết nối API thất bại. Vui lòng bấm tìm lại!');
  }
  const movies = [
    'The Dark Knight',
    'Inception',
    'Interstellar',
    'The Matrix',
    'Pulp Fiction',
    'Forrest Gump',
    'Fight Club',
    'Spirited Away',
    'Parasite',
    'Whiplash',
    'The Green Mile',
    'The Godfather',
    'Schindler List',
    'Saving Private Ryan'
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
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowDropdown(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
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
      if (selectedIndex >= 0 && selectedIndex < suggestions.length) {
        setQuery(suggestions[selectedIndex]);
        setShowDropdown(false);
      }
    } else if (e.key === 'Escape') {
      setShowDropdown(false);
    }
  };

  const clearInput = () => {
    setQuery('');
    setSuggestions([]);
    setShowDropdown(false);
  };

  return (
    <div ref={containerRef} style={{ maxWidth: '400px', margin: '20px auto', position: 'relative' }}>
      <label htmlFor="movie-search" style={{ display: 'block', marginBottom: '8px', fontWeight: 'bold' }}>
        Tìm kiếm phim ảnh:
      </label>
      
      <div style={{ position: 'relative', display: 'flex', alignItems: 'center' }}>
        <input
          id="movie-search"
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => query.trim() && setShowDropdown(true)}
          placeholder="Nhập chữ cái (ví dụ: 'in', 'the')..."
          aria-autocomplete="list"
          aria-controls="movie-suggestions"
          aria-expanded={showDropdown}
          style={{
            width: '100%',
            padding: '12px 40px 12px 16px',
            borderRadius: '8px',
            border: '2px solid #444',
            background: '#1a1a1a',
            color: '#fff',
            fontSize: '16px',
            outline: 'none',
            transition: 'border-color 0.2s'
          }}
        />
        
        {query && (
          <button
            onClick={clearInput}
            aria-label="Xoá ô tìm kiếm"
            style={{
              position: 'absolute',
              right: '12px',
              background: 'none',
              border: 'none',
              color: '#888',
              cursor: 'pointer',
              fontSize: '18px'
            }}
          >
            ✕
          </button>
        )}
      </div>

      {isLoading && (
        <div style={{ padding: '12px', color: '#00e5ff', fontSize: '14px' }}>
          ⏳ Đang tìm kiếm...
        </div>
      )}

      {error && (
        <div style={{ padding: '12px', color: '#ff1744', fontSize: '14px' }}>
          ❌ {error}
        </div>
      )}

      {showDropdown && (
        <ul
          id="movie-suggestions"
          role="listbox"
          style={{
            position: 'absolute',
            top: '100%',
            left: 0,
            width: '100%',
            background: '#1f1f1f',
            border: '1px solid #333',
            borderRadius: '8px',
            marginTop: '8px',
            padding: 0,
            listStyle: 'none',
            maxHeight: '200px',
            overflowY: 'auto',
            zIndex: 10,
            boxShadow: '0 4px 12px rgba(0,0,0,0.5)'
          }}
        >
          {suggestions.length === 0 ? (
            <li role="option" aria-selected="false" style={{ padding: '12px', color: '#888', textAlign: 'center' }}>
              🔍 Không tìm thấy kết quả phù hợp
            </li>
          ) : (
            suggestions.map((movie, index) => (
              <li
                key={movie}
                role="option"
                aria-selected={index === selectedIndex}
                onClick={() => {
                  setQuery(movie);
                  setShowDropdown(false);
                }}
                onMouseEnter={() => setSelectedIndex(index)}
                style={{
                  padding: '12px 16px',
                  cursor: 'pointer',
                  color: '#fff',
                  background: index === selectedIndex ? '#333' : 'transparent',
                  transition: 'background 0.1s'
                }}
              >
                {movie}
              </li>
            ))
          )}
        </ul>
      )}
    </div>
  );
};
