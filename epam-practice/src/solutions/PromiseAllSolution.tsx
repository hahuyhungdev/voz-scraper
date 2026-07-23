import React, { useState } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function promiseAll(promises: any[]): Promise<any[]> {
  return new Promise((resolve, reject) => {
    if (!Array.isArray(promises)) {
      return reject(new TypeError('Input must be an array'));
    }

    const len = promises.length;
    if (len === 0) {
      return resolve([]);
    }

    const results = new Array(len);
    let completedCount = 0;

    for (let i = 0; i < len; i++) {
      // Dùng Promise.resolve để bọc các giá trị thường (không phải Promise)
      Promise.resolve(promises[i])
        .then((val) => {
          results[i] = val;
          completedCount++;

          // Khi tất cả đã hoàn thành, resolve mảng kết quả
          if (completedCount === len) {
            resolve(results);
          }
        })
        .catch((err) => {
          // Bất kỳ promise nào reject sẽ reject ngay lập tức (fail-fast)
          reject(err);
        });
    }
  });
}

export const PromiseAllSolution: React.FC = () => {
  const [output, setOutput] = useState('');

  const runSuccessTest = async () => {
    setOutput('Đang chạy Success Test...');
    try {
      const p1 = new Promise((resolve) => setTimeout(() => resolve('P1 (120ms)'), 120));
      const p2 = 'Value thường';
      const p3 = new Promise((resolve) => setTimeout(() => resolve('P3 (60ms)'), 60));

      const res = await promiseAll([p1, p2, p3]);
      setOutput(`🟢 Test Success: Thành công!\nKết quả trả về: ${JSON.stringify(res)}\n(Mong muốn: ["P1 (120ms)", "Value thường", "P3 (60ms)"])`);
    } catch (e) {
      setOutput(`❌ Test Success Lỗi: ${(e as Error).message}`);
    }
  };

  const runFailureTest = async () => {
    setOutput('Đang chạy Failure Test (fail-fast)...');
    try {
      const p1 = new Promise((resolve) => setTimeout(() => resolve('P1 thành công'), 150));
      const p2 = new Promise((_, reject) => setTimeout(() => reject(new Error('Lỗi P2 (50ms)')), 50));
      const p3 = new Promise((resolve) => setTimeout(() => resolve('P3 thành công'), 100));

      await promiseAll([p1, p2, p3]);
      setOutput('❌ Lỗi: Test đáng nhẽ phải thất bại!');
    } catch (e) {
      setOutput(`🟢 Test Failure: Thành công!\nBắt được lỗi thất bại ngay lập tức: "${(e as Error).message}" (Mong muốn bắt được Lỗi P2 trong 50ms)`);
    }
  };

  return (
    <div style={{ padding: '20px', textAlign: 'center' }}>
      <h4>🧪 Sandbox Kiểm Thử Promise.all Polyfill</h4>

      <div style={{ display: 'inline-flex', gap: '12px', margin: '10px 0' }}>
        <button
          onClick={runSuccessTest}
          style={{ padding: '10px 20px', background: '#28a745', border: 'none', color: '#fff', borderRadius: '6px', cursor: 'pointer', fontWeight: 'bold' }}
        >
          Test Cases Thành Công
        </button>

        <button
          onClick={runFailureTest}
          style={{ padding: '10px 20px', background: '#dc3545', border: 'none', color: '#fff', borderRadius: '6px', cursor: 'pointer', fontWeight: 'bold' }}
        >
          Test Cases Thất Bại (Fail-fast)
        </button>
      </div>

      <pre style={{ background: '#111', padding: '16px', border: '1px solid #333', borderRadius: '8px', color: '#00ff00', marginTop: '16px', fontFamily: 'monospace', lineHeight: 1.5, textAlign: 'left' }}>
        {output || 'Chọn một kịch bản kiểm thử ở trên để xem kết quả hoạt động...'}
      </pre>
    </div>
  );
};
