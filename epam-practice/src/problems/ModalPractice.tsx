import React, { useState } from 'react';

export const ModalPractice: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Accessible Modal Dialog</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của bạn tại đây để giải quyết bài toán. Cần đảm bảo phím Tab chỉ di chuyển qua lại trong Modal, đóng được bằng Esc hoặc Click outside.
      </p>

      <button
        onClick={() => setIsOpen(true)}
        style={{
          padding: '10px 20px',
          background: '#0070f3',
          color: '#fff',
          border: 'none',
          borderRadius: '4px',
          cursor: 'pointer'
        }}
      >
        Mở Modal
      </button>

      {/* Hoàn thành Modal Component của bạn bên dưới */}
      {isOpen && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            width: '100vw',
            height: '100vh',
            background: 'rgba(0,0,0,0.5)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            zIndex: 1000
          }}
        >
          <div
            style={{
              background: '#222',
              padding: '24px',
              borderRadius: '8px',
              width: '400px',
              border: '1px solid #444'
            }}
          >
            <h4>Tiêu đề Modal</h4>
            <p>Nội dung modal của bạn. Hãy thêm Focus Trapping và nút đóng.</p>
            <button
              onClick={() => setIsOpen(false)}
              style={{
                padding: '8px 16px',
                background: '#444',
                color: '#fff',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer'
              }}
            >
              Đóng
            </button>
          </div>
        </div>
      )}
    </div>
  );
};
