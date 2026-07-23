import React, { useState } from 'react';

interface StarRatingProps {
  maxStars?: number;
  readOnly?: boolean;
  value?: number;
  onChange?: (rating: number) => void;
}

export const StarRating: React.FC<StarRatingProps> = ({
  maxStars = 5,
  readOnly = false,
  value = 0,
  onChange
}) => {
  const [rating, setRating] = useState(value);
  const [hoverRating, setHoverRating] = useState<number | null>(null);

  const displayRating = hoverRating !== null ? hoverRating : rating;

  const handleStarClick = (index: number) => {
    if (readOnly) return;
    setRating(index);
    if (onChange) onChange(index);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (readOnly) return;

    if (e.key === 'ArrowRight') {
      const next = Math.min(maxStars, rating + 1);
      setRating(next);
      if (onChange) onChange(next);
    } else if (e.key === 'ArrowLeft') {
      const prev = Math.max(0, rating - 1);
      setRating(prev);
      if (onChange) onChange(prev);
    } else if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      // Thực hiện đánh giá khi bấm phím space hoặc enter
      if (hoverRating !== null) {
        setRating(hoverRating);
        if (onChange) onChange(hoverRating);
      }
    }
  };

  return (
    <div
      role="slider"
      aria-valuenow={rating}
      aria-valuemin={0}
      aria-valuemax={maxStars}
      aria-readonly={readOnly}
      aria-label="Xếp hạng sao"
      tabIndex={readOnly ? -1 : 0}
      onKeyDown={handleKeyDown}
      style={{
        display: 'inline-flex',
        gap: '4px',
        outline: 'none',
        padding: '4px 8px',
        borderRadius: '6px',
        border: readOnly ? 'none' : '1px solid #333',
        background: readOnly ? 'transparent' : '#111',
        cursor: readOnly ? 'default' : 'pointer'
      }}
    >
      {Array.from({ length: maxStars }).map((_, i) => {
        const starIndex = i + 1;
        const isStarred = starIndex <= displayRating;

        return (
          <span
            key={starIndex}
            onMouseEnter={() => !readOnly && setHoverRating(starIndex)}
            onMouseLeave={() => !readOnly && setHoverRating(null)}
            onClick={() => handleStarClick(starIndex)}
            style={{
              fontSize: '32px',
              color: isStarred ? '#ffc107' : '#444',
              transition: 'color 0.15s, transform 0.1s',
              transform: !readOnly && hoverRating === starIndex ? 'scale(1.15)' : 'none'
            }}
          >
            ★
          </span>
        );
      })}
    </div>
  );
};

export const StarRatingSolution: React.FC = () => {
  const [userRating, setUserRating] = useState(3);

  return (
    <div style={{ padding: '20px', textAlign: 'center' }}>
      <h4>⭐ Đánh Giá Khoá Học Live Coding</h4>
      
      <p style={{ color: '#aaa' }}>Interactive Star Rating (Nhấn Tab và dùng phím mũi tên Trái/Phải để chỉnh):</p>
      <StarRating
        maxStars={5}
        value={userRating}
        onChange={(rating) => setUserRating(rating)}
      />
      
      <p style={{ marginTop: '10px' }}>Số sao bạn chọn: {userRating} / 5</p>

      <p style={{ color: '#aaa', marginTop: '20px' }}>Chế độ Read-Only (Không tương tác):</p>
      <StarRating maxStars={5} value={4} readOnly={true} />
    </div>
  );
};
