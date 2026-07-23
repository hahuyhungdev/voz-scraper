import React, { useState } from 'react';

// Implementation Polyfills
// eslint-disable-next-line @typescript-eslint/no-explicit-any
(Array.prototype as any).myMap = function(callback: any, context: any) {
  if (this == null) {
    throw new TypeError('Array.prototype.myMap called on null or undefined');
  }
  if (typeof callback !== 'function') {
    throw new TypeError(callback + ' is not a function');
  }

  const O = Object(this);
  const len = O.length >>> 0;
  const A = new Array(len);

  for (let i = 0; i < len; i++) {
    // Xử lý sparse arrays: chỉ gọi callback cho các phần tử tồn tại
    if (i in O) {
      A[i] = callback.call(context, O[i], i, O);
    }
  }

  return A;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(Array.prototype as any).myReduce = function(callback: any, initialValue: any) {
  if (this == null) {
    throw new TypeError('Array.prototype.myReduce called on null or undefined');
  }
  if (typeof callback !== 'function') {
    throw new TypeError(callback + ' is not a function');
  }

  const O = Object(this);
  const len = O.length >>> 0;

  if (len === 0 && initialValue === undefined) {
    throw new TypeError('Reduce of empty array with no initial value');
  }

  let accumulator = initialValue;
  let startIndex = 0;

  // Nếu không truyền initialValue, tìm phần tử thực sự tồn tại đầu tiên làm accumulator
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
    if (!found) {
      throw new TypeError('Reduce of empty array with no initial value');
    }
  }

  for (let i = startIndex; i < len; i++) {
    if (i in O) {
      accumulator = callback(accumulator, O[i], i, O);
    }
  }

  return accumulator;
};

export const ArrayMethodsSolution: React.FC = () => {
  const [output, setOutput] = useState('');

  const runTest = () => {
    let log = '';

    // Test 1: myMap cơ bản
    try {
      const arr1 = [1, 2, 3];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const res1 = (arr1 as any).myMap((x: number) => x * 3);
      log += `🟢 Test 1: myMap [1, 2, 3] * 3\nResult: ${JSON.stringify(res1)} (Mong muốn: [3,6,9])\n\n`;
    } catch (e) {
      log += `❌ Test 1 Lỗi: ${(e as Error).message}\n\n`;
    }

    // Test 2: myMap với context thisArg
    try {
      const arr2 = [1, 2];
      const context = { multiplier: 10 };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const res2 = (arr2 as any).myMap(function(this: typeof context, x: number) {
        return x * this.multiplier;
      }, context);
      log += `🟢 Test 2: myMap với Context Multiplier (10)\nResult: ${JSON.stringify(res2)} (Mong muốn: [10,20])\n\n`;
    } catch (e) {
      log += `❌ Test 2 Lỗi: ${(e as Error).message}\n\n`;
    }

    // Test 3: myReduce có initialValue
    try {
      const arr3 = [1, 2, 3, 4];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const res3 = (arr3 as any).myReduce((acc: number, cur: number) => acc + cur, 100);
      log += `🟢 Test 3: myReduce [1,2,3,4] cộng dồn với initialValue = 100\nResult: ${res3} (Mong muốn: 110)\n\n`;
    } catch (e) {
      log += `❌ Test 3 Lỗi: ${(e as Error).message}\n\n`;
    }

    // Test 4: myReduce không có initialValue
    try {
      const arr4 = [5, 10, 15];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const res4 = (arr4 as any).myReduce((acc: number, cur: number) => acc + cur);
      log += `🟢 Test 4: myReduce [5,10,15] cộng dồn KHÔNG có initialValue\nResult: ${res4} (Mong muốn: 30)\n\n`;
    } catch (e) {
      log += `❌ Test 4 Lỗi: ${(e as Error).message}\n\n`;
    }

    // Test 5: Sparse Array (mảng thưa)
    try {
      // eslint-disable-next-line no-sparse-arrays
      const arr5 = [1, , 3];
      let mapCallsCount = 0;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const mapRes = (arr5 as any).myMap((x: number) => {
        mapCallsCount++;
        return x * 2;
      });

      // eslint-disable-next-line no-sparse-arrays
      const arr6 = [, 2, , 4];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reduceRes = (arr6 as any).myReduce((acc: number, cur: number) => acc + cur);

      log += `🟢 Test 5: Sparse Array [1, , 3] (mảng khuyết index 1)\n` +
             `- myMap Result: ${JSON.stringify(mapRes)} (Mong muốn: [2, null/empty, 6])\n` +
             `- Số lần callback được gọi: ${mapCallsCount} (Mong muốn: 2)\n` +
             `- myReduce [, 2, , 4] Result: ${reduceRes} (Mong muốn: 6)\n`;
    } catch (e) {
      log += `❌ Test 5 Lỗi: ${(e as Error).message}\n\n`;
    }

    setOutput(log);
  };

  return (
    <div style={{ padding: '20px' }}>
      <h4>🧪 Sandbox Kiểm Thử Polyfill Array</h4>
      <button
        onClick={runTest}
        style={{
          padding: '10px 20px',
          background: '#0070f3',
          color: '#fff',
          border: 'none',
          borderRadius: '4px',
          cursor: 'pointer',
          fontWeight: 'bold'
        }}
      >
        Chạy Bộ Kiểm Thử
      </button>

      <pre style={{ background: '#111', padding: '16px', border: '1px solid #333', borderRadius: '8px', color: '#00ff00', marginTop: '16px', fontFamily: 'monospace', lineHeight: 1.5, textAlign: 'left' }}>
        {output || 'Nhấn nút để chạy kiểm thử polyfill myMap và myReduce...'}
      </pre>
    </div>
  );
};
