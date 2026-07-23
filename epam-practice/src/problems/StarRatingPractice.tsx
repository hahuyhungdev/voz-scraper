import React, { useState } from 'react';

export const StarRatingPractice: React.FC = () => {
  const [rating, setRating] = useState(0);

  return (
    <div style={{ padding: '20px', border: '1px dashed #666', borderRadius: '8px' }}>
      <h3>📝 Thực hành: Star Rating Component</h3>
      <p style={{ color: '#aaa', fontSize: '14px' }}>
        Hãy viết code logic của bạn tại đây để giải quyết bài toán. Hỗ trợ hover preview, click chọn, phím mũi tên và chế độ read-only.
      </p>

      {/* Dựng thanh đánh giá sao ở đây */}
      <div style={{ display: 'flex', gap: '8px', fontSize: '24px' }}>
        {[1, 2, 3, 4, 5].map((star) => (
          <span
            key={star}
            onClick={() => setRating(star)}
            style={{ cursor: 'pointer', color: star <= rating ? '#ffc107' : '#e4e5e9' }}
          >
            ★
          </span>
        ))}
      </div>
      <p>Rating hiện tại: {rating} / 5</p>
    </div>
  );
};
