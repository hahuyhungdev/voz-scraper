import React, { useState, useEffect, useRef } from 'react';

interface Post {
  id: number;
  title: string;
  body: string;
}

const mockFetchPosts = async (page: number, limit: number): Promise<Post[]> => {
  await new Promise((resolve) => setTimeout(resolve, 800)); // Delay 800ms
  // Giả lập tổng cộng tối đa 50 bài viết
  if (page > 5) return [];

  return Array.from({ length: limit }, (_, index) => {
    const id = (page - 1) * limit + index + 1;
    return {
      id,
      title: `Bài viết #${id}: Review phỏng vấn tại EPAM Systems`,
      body: `Nội dung chi tiết của bài viết số ${id}. Chia sẻ kinh nghiệm thực tế về các vòng phỏng vấn, chế độ lương thưởng, đãi ngộ và lộ trình thăng tiến cho lập trình viên.`
    };
  });
};

export const InfiniteScrollSolution: React.FC = () => {
  const [posts, setPosts] = useState<Post[]>([]);
  const [page, setPage] = useState(1);
  const [isLoading, setIsLoading] = useState(false);
  const [hasMore, setHasMore] = useState(true);

  const observerRef = useRef<IntersectionObserver | null>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);

  const loadMorePosts = async () => {
    if (isLoading || !hasMore) return;

    setIsLoading(true);
    try {
      const newPosts = await mockFetchPosts(page, 10);
      if (newPosts.length === 0) {
        setHasMore(false);
      } else {
        setPosts((prev) => [...prev, ...newPosts]);
        setPage((prev) => prev + 1);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    // Thiết lập IntersectionObserver
    observerRef.current = new IntersectionObserver(
      (entries) => {
        const firstEntry = entries[0];
        if (firstEntry.isIntersecting && hasMore && !isLoading) {
          loadMorePosts();
        }
      },
      { threshold: 0.1 }
    );

    const currentSentinel = sentinelRef.current;
    if (currentSentinel) {
      observerRef.current.observe(currentSentinel);
    }

    return () => {
      if (observerRef.current && currentSentinel) {
        observerRef.current.unobserve(currentSentinel);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, hasMore, isLoading]);

  return (
    <div style={{ maxWidth: '600px', margin: '0 auto', padding: '20px' }}>
      <h3 style={{ borderBottom: '1px solid #333', paddingBottom: '10px' }}>📰 Bản Tin Review Công Ty</h3>

      <div
        style={{
          maxHeight: '400px',
          overflowY: 'auto',
          border: '1px solid #333',
          borderRadius: '8px',
          padding: '16px',
          background: '#111'
        }}
      >
        {posts.map((post) => (
          <div
            key={post.id}
            style={{
              padding: '16px',
              borderBottom: '1px solid #222',
              marginBottom: '12px',
              background: '#1a1a1a',
              borderRadius: '6px'
            }}
          >
            <h4 style={{ margin: '0 0 8px 0', color: '#00e5ff' }}>{post.title}</h4>
            <p style={{ margin: 0, fontSize: '14px', color: '#aaa', lineHeight: 1.5 }}>{post.body}</p>
          </div>
        ))}

        {/* Sentinel element làm cột mốc để trigger tải thêm */}
        <div ref={sentinelRef} style={{ height: '20px', margin: '10px 0' }}>
          {isLoading && (
            <div style={{ textAlign: 'center', color: '#00e5ff', fontSize: '14px' }}>
              ⏳ Đang tải thêm bài viết...
            </div>
          )}
          {!hasMore && (
            <div style={{ textAlign: 'center', color: '#888', fontSize: '14px', padding: '10px 0' }}>
              🎉 Đã tải hết bài viết (Tối đa 50 bài).
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
