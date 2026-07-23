import React, { useState } from 'react';

// Mock function để giả lập gọi API
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const mockFetchMovies = async (query: string): Promise<string[]> => {
  await new Promise((resolve) => setTimeout(resolve, 500)); // Delay 500ms
  if (Math.random() < 0.15) {
    throw new Error('Mạng không ổn định. Vui lòng thử lại!');
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
    'Whiplash'
  ];
  return movies.filter((movie) => movie.toLowerCase().includes(query.toLowerCase()));
};

export const AutoCompletePractice: React.FC = () => {
  const [query, setQuery] = useState('');

  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: AutoComplete Search Input</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của bạn tại đây để giải quyết bài toán. Bạn có thể sử dụng hàm <code>mockFetchMovies(query)</code> để gọi API.
      </p>

      {/* Hãy hoàn thiện giao diện và logic của bạn tại đây */}
      <div style={{ position: 'relative', width: '300px' }}>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Nhập tên phim để tìm kiếm..."
          style={{
            width: '100%',
            padding: '10px',
            borderRadius: '4px',
            border: '1px solid #444',
            background: '#222',
            color: '#fff'
          }}
        />
      </div>
    </div>
  );
};
