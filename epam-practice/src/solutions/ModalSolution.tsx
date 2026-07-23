import React, { useState, useEffect, useRef } from 'react';

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  children: React.ReactNode;
}

const Modal: React.FC<ModalProps> = ({ isOpen, onClose, title, children }) => {
  const modalRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLElement | null>(null);

  // Lưu lại focus element trước khi modal mở
  useEffect(() => {
    if (isOpen) {
      triggerRef.current = document.activeElement as HTMLElement;
      // Focus vào modal wrapper để bắt đầu bẫy focus
      modalRef.current?.focus();
    } else {
      // Restore focus khi modal đóng
      triggerRef.current?.focus();
    }
  }, [isOpen]);

  // Nhấn Esc để đóng
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    if (isOpen) {
      window.addEventListener('keydown', handleKeyDown);
    }
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onClose]);

  // Focus Trapping Logic
  const handleTabKey = (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key !== 'Tab' || !modalRef.current) return;

    // Tìm tất cả phần tử có khả năng focus bên trong modal
    const focusableSelector = 'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])';
    const focusables = modalRef.current.querySelectorAll<HTMLElement>(focusableSelector);
    
    if (focusables.length === 0) return;

    const firstElement = focusables[0];
    const lastElement = focusables[focusables.length - 1];

    if (e.shiftKey) {
      // Nhấn Shift + Tab ở phần tử đầu tiên -> chuyển về cuối
      if (document.activeElement === firstElement) {
        e.preventDefault();
        lastElement.focus();
      }
    } else {
      // Nhấn Tab ở phần tử cuối cùng -> chuyển về đầu
      if (document.activeElement === lastElement) {
        e.preventDefault();
        firstElement.focus();
      }
    }
  };

  if (!isOpen) return null;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-labelledby="modal-title"
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        width: '100vw',
        height: '100vh',
        background: 'rgba(0,0,0,0.65)',
        backdropFilter: 'blur(4px)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000
      }}
      onClick={onClose} // Đóng khi click Overlay
    >
      <div
        ref={modalRef}
        tabIndex={-1}
        onKeyDown={handleTabKey}
        onClick={(e) => e.stopPropagation()} // Chặn sủi bọt sự kiện click bên trong modal
        style={{
          background: '#1a1a1a',
          border: '1px solid #333',
          borderRadius: '12px',
          padding: '24px',
          width: '450px',
          maxWidth: '90%',
          outline: 'none',
          boxShadow: '0 8px 32px rgba(0,0,0,0.8)'
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
          <h3 id="modal-title" style={{ margin: 0, fontSize: '20px', color: '#fff' }}>
            {title}
          </h3>
          <button
            onClick={onClose}
            aria-label="Đóng modal"
            style={{
              background: 'none',
              border: 'none',
              color: '#888',
              cursor: 'pointer',
              fontSize: '18px'
            }}
          >
            ✕
          </button>
        </div>

        <div style={{ color: '#ccc', fontSize: '15px', lineHeight: 1.6, marginBottom: '24px' }}>
          {children}
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '12px' }}>
          <button
            onClick={onClose}
            style={{
              padding: '10px 20px',
              borderRadius: '6px',
              border: '1px solid #444',
              background: 'transparent',
              color: '#fff',
              cursor: 'pointer'
            }}
          >
            Hủy Bỏ
          </button>
          <button
            onClick={() => {
              alert('Đã xác nhận thành công!');
              onClose();
            }}
            style={{
              padding: '10px 20px',
              borderRadius: '6px',
              border: 'none',
              background: '#0070f3',
              color: '#fff',
              cursor: 'pointer'
            }}
          >
            Xác Nhận
          </button>
        </div>
      </div>
    </div>
  );
};

export const ModalSolution: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div style={{ textAlign: 'center', padding: '30px' }}>
      <button
        onClick={() => setIsOpen(true)}
        style={{
          padding: '12px 24px',
          background: '#0070f3',
          color: '#fff',
          fontWeight: 'bold',
          border: 'none',
          borderRadius: '8px',
          cursor: 'pointer',
          boxShadow: '0 4px 14px rgba(0,112,243,0.3)'
        }}
      >
        Mở Modal Accessible Demo
      </button>

      <Modal isOpen={isOpen} onClose={() => setIsOpen(false)} title="🔐 Xác Thực Tài Khoản">
        <p>Đây là một hộp thoại Modal chuẩn khả năng truy cập (Accessibility). Thử test các tính năng sau:</p>
        <ol style={{ paddingLeft: '20px', textAlign: 'left' }}>
          <li>Nhấn phím <b>Tab</b> hoặc <b>Shift + Tab</b> để di chuyển vòng quanh. Focus không thể nhảy ra ngoài modal.</li>
          <li>Nhấn phím <b>Escape</b> để đóng modal nhanh.</li>
          <li>Click vào phần nền đen mờ (Overlay) xung quanh để đóng.</li>
        </ol>
        <input
          type="text"
          placeholder="Nhập email của bạn..."
          style={{
            width: '100%',
            padding: '10px',
            background: '#222',
            border: '1px solid #444',
            color: '#fff',
            borderRadius: '4px',
            marginTop: '10px'
          }}
        />
      </Modal>
    </div>
  );
};
