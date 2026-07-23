import React, { useState } from 'react';

// Chỗ này để viết Polyfill của bạn
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(Array.prototype as any).myMap = function(callback: any, context: any) {
  // Hãy hoàn thiện hàm myMap tại đây
  return []; 
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(Array.prototype as any).myReduce = function(callback: any, initialValue: any) {
  // Hãy hoàn thiện hàm myReduce tại đây
  return null;
};

export const ArrayMethodsPractice: React.FC = () => {
  const [output, setOutput] = useState('');

  const runTest = () => {
    try {
      const arr = [1, 2, 3, 4];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const mapRes = (arr as any).myMap((x: number) => x * 2);
      
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reduceRes = (arr as any).myReduce((acc: number, cur: number) => acc + cur, 0);

      setOutput(`Test Mảng: [1, 2, 3, 4]\nKết quả myMap (x * 2): ${JSON.stringify(mapRes)} (Mong muốn: [2,4,6,8])\nKết quả myReduce (sum): ${reduceRes} (Mong muốn: 10)`);
    } catch (e) {
      setOutput(`Lỗi thực thi: ${(e as Error).message}`);
    }
  };

  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Array myMap & myReduce</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của hàm <code>myMap</code> và <code>myReduce</code> trực tiếp vào file <code>src/problems/ArrayMethodsPractice.tsx</code>. Sau đó bấm nút Test dưới đây để kiểm tra.
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
        Chạy Test Thử Nghiệm
      </button>

      <pre style={{ background: '#222', padding: '15px', borderRadius: '4px', color: '#00ff00', marginTop: '15px' }}>
        {output || 'Kết quả test sẽ hiển thị tại đây...'}
      </pre>
    </div>
  );
};
