import React, { useState } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function promiseAll(promises: any[]): Promise<any[]> {
  // Hãy viết Polyfill Promise.all của bạn tại đây
  return Promise.resolve([]);
}

export const PromiseAllPractice: React.FC = () => {
  const [output, setOutput] = useState('');

  const runTest = async () => {
    setOutput('Đang chạy test...');
    try {
      const p1 = new Promise((resolve) => setTimeout(() => resolve('P1 success (100ms)'), 100));
      const p2 = Promise.resolve('P2 success immediately');
      const p3 = new Promise((resolve) => setTimeout(() => resolve('P3 success (50ms)'), 50));

      const res = await promiseAll([p1, p2, p3]);
      setOutput(`Kết quả trả về: ${JSON.stringify(res)}\n(Mong muốn: ["P1 success (100ms)", "P2 success immediately", "P3 success (50ms)"])`);
    } catch (e) {
      setOutput(`Lỗi Test: ${(e as Error).message}`);
    }
  };

  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Promise.all Polyfill</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của hàm <code>promiseAll</code> trong file <code>src/problems/PromiseAllPractice.tsx</code>. Bấm nút Test dưới đây để xem kết quả.
      </p>

      <button
        onClick={runTest}
        style={{
          padding: '10px 20px',
          background: '#0070f3',
          color: '#fff',
          border: 'none',
          borderRadius: '4px',
          cursor: 'pointer'
        }}
      >
        Chạy Test Promise.all
      </button>

      <pre style={{ background: '#222', padding: '15px', borderRadius: '4px', color: '#00ff00', marginTop: '15px' }}>
        {output || 'Kết quả test sẽ hiển thị tại đây...'}
      </pre>
    </div>
  );
};
