import React from 'react';

export const ToastPractice: React.FC = () => {
  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Toast Notification Alert</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của bạn tại đây để giải quyết bài toán. Hỗ trợ kích hoạt (trigger) các loại toast khác nhau, tự tắt sau 3 giây hoặc click đóng.
      </p>

      {/* Nút bấm để kích hoạt toast */}
      <div style={{ display: 'flex', gap: '10px' }}>
        <button style={{ padding: '8px 16px', background: '#28a745', color: '#fff', border: 'none', borderRadius: '4px' }}>
          Trigger Success
        </button>
        <button style={{ padding: '8px 16px', background: '#dc3545', color: '#fff', border: 'none', borderRadius: '4px' }}>
          Trigger Error
        </button>
      </div>

      {/* Hiển thị container chứa danh sách toast đang hiển thị ở đây */}
    </div>
  );
};
