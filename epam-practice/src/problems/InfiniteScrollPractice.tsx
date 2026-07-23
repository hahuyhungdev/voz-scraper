import React from 'react';

export const InfiniteScrollPractice: React.FC = () => {
  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Infinite Scroll List</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của bạn tại đây để giải quyết bài toán. Bạn nên dùng <code>IntersectionObserver</code> để nhận biết khi phần tử cuối danh sách được hiển thị.
      </p>

      {/* Dựng danh sách và xử lý Infinite Scroll tại đây */}
      <div style={{ maxHeight: '300px', overflowY: 'auto', border: '1px solid #444', padding: '10px' }}>
        <p>Danh sách trống. Hãy viết code để render các items và tự động tải thêm.</p>
      </div>
    </div>
  );
};
