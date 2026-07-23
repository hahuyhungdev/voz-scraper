import React, { useState } from 'react';

type ToastType = 'success' | 'info' | 'warning' | 'error';

interface ToastItem {
  id: number;
  message: string;
  type: ToastType;
  duration?: number;
}

export const ToastSolution: React.FC = () => {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  const addToast = (message: string, type: ToastType, duration = 3000) => {
    const id = Date.now() + Math.random();
    const newToast: ToastItem = { id, message, type, duration };

    setToasts((prev) => [...prev, newToast]);

    // Tự động đóng sau duration
    setTimeout(() => {
      removeToast(id);
    }, duration);
  };

  const removeToast = (id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  const getToastStyles = (type: ToastType) => {
    switch (type) {
      case 'success':
        return { borderLeft: '6px solid #28a745', background: '#1c281f' };
      case 'error':
        return { borderLeft: '6px solid #dc3545', background: '#2c1e20' };
      case 'warning':
        return { borderLeft: '6px solid #ffc107', background: '#2d281a' };
      case 'info':
      default:
        return { borderLeft: '6px solid #0070f3', background: '#1a222f' };
    }
  };

  return (
    <div style={{ padding: '20px', textAlign: 'center' }}>
      <h4>🔔 Toast Alerts System Demo</h4>
      <p style={{ color: '#aaa', fontSize: '14px' }}>Click các nút bên dưới để trigger Toast Notification xếp chồng ở góc phải màn hình.</p>

      <div style={{ display: 'inline-flex', gap: '12px', flexWrap: 'wrap', justifyContent: 'center', marginTop: '10px' }}>
        <button
          onClick={() => addToast('Đã lưu thông tin cấu hình thành công!', 'success')}
          style={{ padding: '10px 16px', background: '#28a745', border: 'none', borderRadius: '6px', color: '#fff', cursor: 'pointer' }}
        >
          Success Toast
        </button>
        <button
          onClick={() => addToast('Phiên làm việc sắp hết hạn trong 5 phút.', 'warning')}
          style={{ padding: '10px 16px', background: '#ffc107', border: 'none', borderRadius: '6px', color: '#000', fontWeight: 'bold', cursor: 'pointer' }}
        >
          Warning Toast
        </button>
        <button
          onClick={() => addToast('Không tìm thấy tệp tin được yêu cầu.', 'error')}
          style={{ padding: '10px 16px', background: '#dc3545', border: 'none', borderRadius: '6px', color: '#fff', cursor: 'pointer' }}
        >
          Error Toast
        </button>
        <button
          onClick={() => addToast('Phiên bản v2.0 đã được cập nhật thành công.', 'info')}
          style={{ padding: '10px 16px', background: '#0070f3', border: 'none', borderRadius: '6px', color: '#fff', cursor: 'pointer' }}
        >
          Info Toast
        </button>
      </div>

      {/* Toast Portal/Container fixed ở góc trên bên phải */}
      <div
        style={{
          position: 'fixed',
          top: '20px',
          right: '20px',
          zIndex: 9999,
          display: 'flex',
          flexDirection: 'column',
          gap: '10px',
          width: '320px',
          pointerEvents: 'none' // Để click xuyên qua vùng trống nếu cần
        }}
      >
        {toasts.map((toast) => {
          const customStyles = getToastStyles(toast.type);
          return (
            <div
              key={toast.id}
              style={{
                pointerEvents: 'auto', // Cho phép click vào bản thân Toast
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                padding: '16px',
                borderRadius: '8px',
                color: '#fff',
                boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
                animation: 'slideIn 0.3s ease forwards',
                fontSize: '14px',
                lineHeight: 1.4,
                ...customStyles
              }}
            >
              <span>{toast.message}</span>
              <button
                onClick={() => removeToast(toast.id)}
                style={{
                  background: 'none',
                  border: 'none',
                  color: '#fff',
                  cursor: 'pointer',
                  fontSize: '16px',
                  marginLeft: '10px',
                  opacity: 0.7
                }}
              >
                ✕
              </button>
            </div>
          );
        })}
      </div>

      {/* Thêm CSS Animation Slide In bằng thẻ style */}
      <style>{`
        @keyframes slideIn {
          from {
            transform: translateX(100%);
            opacity: 0;
          }
          to {
            transform: translateX(0);
            opacity: 1;
          }
        }
      `}</style>
    </div>
  );
};
