import React, { useState, useRef } from 'react';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function debounce<T extends (...args: any[]) => void>(fn: T, delay: number) {
  let timerId: ReturnType<typeof setTimeout> | null = null;

  const debounced = function(this: any, ...args: Parameters<T>) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
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

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function throttle<T extends (...args: any[]) => void>(fn: T, limit: number) {
  let lastFunc: ReturnType<typeof setTimeout> | null = null;
  let lastRan: number = 0;

  const throttled = function(this: any, ...args: Parameters<T>) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
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
}

export const DebounceThrottleSolution: React.FC = () => {
  const [logs, setLogs] = useState<string[]>([]);
  const [clickCount, setClickCount] = useState(0);

  const addLog = (msg: string) => {
    setLogs((prev) => [`[${new Date().toLocaleTimeString()}] ${msg}`, ...prev.slice(0, 15)]);
  };

  const incrementClick = () => {
    setClickCount((prev) => prev + 1);
  };

  // Tạo refs để giữ instance của debounce và throttle qua các lần render
  const debouncedFuncRef = useRef(
    debounce(() => {
      addLog('⚡ Debounce execute! (Sau 1s không có click thêm)');
    }, 1000)
  );

  const throttledFuncRef = useRef(
    throttle(() => {
      addLog('⏳ Throttle execute! (Chạy tối đa 1 lần mỗi 1s)');
    }, 1000)
  );

  const handleDebounceClick = () => {
    incrementClick();
    addLog('Click (Debounce triggered...)');
    debouncedFuncRef.current();
  };

  const handleThrottleClick = () => {
    incrementClick();
    addLog('Click (Throttle triggered...)');
    throttledFuncRef.current();
  };

  const handleCancel = () => {
    debouncedFuncRef.current.cancel();
    throttledFuncRef.current.cancel();
    addLog('🚫 Canceled all pending timers.');
  };

  return (
    <div style={{ padding: '20px', textAlign: 'center' }}>
      <h4>⚡ Debounce & Throttle Interactive Logs</h4>
      
      <p style={{ color: '#aaa', fontSize: '14px' }}>Tổng số lần click chuột thực tế: <b>{clickCount}</b></p>

      <div style={{ display: 'inline-flex', gap: '12px', flexWrap: 'wrap', justifyContent: 'center', margin: '10px 0' }}>
        <button
          onClick={handleDebounceClick}
          style={{ padding: '10px 20px', background: '#e040fb', border: 'none', borderRadius: '6px', color: '#fff', cursor: 'pointer', fontWeight: 'bold' }}
        >
          Click Debounce (1s Delay)
        </button>

        <button
          onClick={handleThrottleClick}
          style={{ padding: '10px 20px', background: '#00e5ff', border: 'none', borderRadius: '6px', color: '#000', cursor: 'pointer', fontWeight: 'bold' }}
        >
          Click Throttle (1s Limit)
        </button>

        <button
          onClick={handleCancel}
          style={{ padding: '10px 20px', background: '#444', border: 'none', borderRadius: '6px', color: '#fff', cursor: 'pointer' }}
        >
          Cancel Timers
        </button>
      </div>

      <div style={{ display: 'flex', gap: '20px', marginTop: '20px', justifyContent: 'space-between' }}>
        <div style={{ width: '100%', textAlign: 'left' }}>
          <h5>📋 Nhật Ký Hoạt Động (Logs):</h5>
          <pre style={{ background: '#111', padding: '16px', border: '1px solid #333', borderRadius: '8px', color: '#00ff00', maxHeight: '200px', overflowY: 'auto', fontSize: '13px', lineHeight: 1.4, margin: 0 }}>
            {logs.length === 0 ? 'Thử click vào các nút ở trên để bắt đầu quan sát log...' : logs.join('\n')}
          </pre>
        </div>
      </div>
    </div>
  );
};
