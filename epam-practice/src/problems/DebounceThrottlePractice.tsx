import React, { useState } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function debounce(fn: (...args: any[]) => void, delay: number) {
  // Hoàn thiện hàm debounce tại đây
  return fn;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function throttle(fn: (...args: any[]) => void, limit: number) {
  // Hoàn thiện hàm throttle tại đây
  return fn;
}

export const DebounceThrottlePractice: React.FC = () => {
  const [debounceCount, setDebounceCount] = useState(0);
  const [throttleCount, setThrottleCount] = useState(0);

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const handleDebounceClick = debounce(() => {
    setDebounceCount(prev => prev + 1);
  }, 1000);

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const handleThrottleClick = throttle(() => {
    setThrottleCount(prev => prev + 1);
  }, 1000);

  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Debounce & Throttle</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của <code>debounce</code> và <code>throttle</code> vào file <code>src/problems/DebounceThrottlePractice.tsx</code>. Sau đó thử click nhanh nhiều lần vào 2 nút dưới đây để kiểm tra.
      </p>

      <div style={{ display: 'flex', gap: '20px', marginTop: '15px' }}>
        <div>
          {/* Thay bằng sự kiện click thực sự gọi hàm debounce */}
          <button
            onClick={() => setDebounceCount(prev => prev + 1)} // Tạm thời để click trực tiếp
            style={{ padding: '10px 20px', background: '#e040fb', color: '#fff', border: 'none', borderRadius: '4px' }}
          >
            Click Debounce (1s)
          </button>
          <p>Count: {debounceCount}</p>
        </div>

        <div>
          {/* Thay bằng sự kiện click thực sự gọi hàm throttle */}
          <button
            onClick={() => setThrottleCount(prev => prev + 1)} // Tạm thời để click trực tiếp
            style={{ padding: '10px 20px', background: '#00e5ff', color: '#fff', border: 'none', borderRadius: '4px' }}
          >
            Click Throttle (1s)
          </button>
          <p>Count: {throttleCount}</p>
        </div>
      </div>
    </div>
  );
};
