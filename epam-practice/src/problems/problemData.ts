export interface Problem {
  id: string;
  title: string;
  category: 'Vanilla JS' | 'UI Component' | 'React Refactor' | 'DSA';
  difficulty: 'Easy' | 'Medium' | 'Hard';
  description: string;
  requirements: string[];
  hints: string[];
}

export const problems: Problem[] = [
  {
    id: 'autocomplete',
    title: 'AutoComplete Search Input',
    category: 'UI Component',
    difficulty: 'Medium',
    description: 'Dựng ô input tìm kiếm phim hoặc thành phố. Khi người dùng gõ chữ, đợi 300ms (debounce) rồi gọi API fetch dữ liệu gợi ý và hiển thị.',
    requirements: [
      'Có trạng thái Loading spinner khi đang gọi API.',
      'Có trạng thái hiển thị lỗi Error Message khi API thất bại.',
      'Có thông báo "Không tìm thấy kết quả" khi danh sách trống.',
      'Debounce 300ms để tránh spam API.',
      'Accessibility: Sử dụng phím mũi tên Lên/Xuống để di chuyển, Enter để chọn, Esc để ẩn gợi ý.',
      'Clear button (nút X) để xoá nhanh nội dung input.'
    ],
    hints: [
      'Sử dụng useEffect kết hợp với setTimeout để xử lý debounce, đừng quên cleanup timer trong return block.',
      'Thêm tabindex hoặc vai trò aria-attributes (như role="listbox", role="option") để hỗ trợ đầu đọc màn hình.',
      'Sử dụng state selectedIndex để theo dõi item đang được focus bằng bàn phím.'
    ]
  },
  {
    id: 'modal',
    title: 'Accessible Modal Dialog',
    category: 'UI Component',
    difficulty: 'Medium',
    description: 'Dựng một Component Modal tái sử dụng có thể bật/tắt linh hoạt với đầy đủ các chuẩn Accessibility cơ bản.',
    requirements: [
      'Hỗ trợ props isOpen và onClose để điều khiển trạng thái.',
      'Đóng modal khi click vào vùng Overlay mờ xung quanh.',
      'Đóng modal khi bấm phím Escape (Esc) trên bàn phím.',
      'Focus Trapping (Bẫy focus): Khi modal mở, nhấn Tab chỉ di chuyển focus qua lại giữa các phần tử bên trong modal chứ không lọt ra ngoài trang chính.',
      'Trả lại focus cho phần tử đã kích hoạt modal (Trigger Button) sau khi đóng modal.'
    ],
    hints: [
      'Sử dụng ref để lưu trữ phần tử đang active trước khi modal mở.',
      'Bắt sự kiệnkeydown trên window để lắng nghe phím Escape.',
      'Để làm Focus Trapping, tìm tất cả focusable elements trong modal (button, input, a, v.v.), chặn hành vi mặc định của Tab ở phần tử đầu tiên và cuối cùng.'
    ]
  },
  {
    id: 'infinite-scroll',
    title: 'Infinite Scroll List',
    category: 'UI Component',
    difficulty: 'Medium',
    description: 'Dựng danh sách cuộn vô tận. Khi người dùng cuộn đến gần đáy trang, tự động load thêm dữ liệu trang tiếp theo.',
    requirements: [
      'Sử dụng IntersectionObserver API để theo dõi điểm cuộn thay vì scroll event thông thường.',
      'Hiển thị Loading state ở đáy danh sách khi đang tải dữ liệu.',
      'Mock dữ liệu phân trang (ví dụ mỗi lần load 10 item mới).',
      'Xử lý trường hợp đã tải hết dữ liệu (Stop loading & show "End of list").',
      'Đảm bảo không bị trigger gọi API liên tục khi scroll.'
    ],
    hints: [
      'Đặt một phần tử <div ref={sentinelRef}></div> ở ngay phía dưới Loading spinner để làm điểm mốc observer.',
      'Đảm bảo disconnect observer cũ trước khi thiết lập observer mới khi danh sách hoặc trang thay đổi.'
    ]
  },
  {
    id: 'star-rating',
    title: 'Star Rating Component',
    category: 'UI Component',
    difficulty: 'Easy',
    description: 'Dựng Component đánh giá xếp hạng sao (Star Rating) tuỳ chỉnh.',
    requirements: [
      'Cho phép chọn số lượng sao hiển thị (mặc định là 5 sao).',
      'Hover Preview: Khi di chuột qua ngôi sao thứ N, các ngôi sao từ 1 đến N sẽ sáng lên tạm thời.',
      'Click Selection: Click vào ngôi sao thứ N để chọn điểm. Điểm này sẽ giữ nguyên sau khi chuột rời đi.',
      'Accessibility: Nhấn mũi tên Trái/Phải để tăng/giảm sao, Enter để chọn.',
      'Hỗ trợ chế độ read-only (chỉ hiển thị, không cho tương tác).'
    ],
    hints: [
      'Cần có 2 state: rating (điểm đã chọn) và hoverRating (điểm đang di chuột qua). Khi hoverRating !== null, render sao theo hoverRating, ngược lại render theo rating.',
      'Dùng phím ArrowLeft, ArrowRight để thay đổi state rating tạm thời.'
    ]
  },
  {
    id: 'toast-notification',
    title: 'Toast Notification Alert',
    category: 'UI Component',
    difficulty: 'Medium',
    description: 'Dựng hệ thống Toast Alert thông báo nhanh cho ứng dụng.',
    requirements: [
      'Hỗ trợ nhiều loại Toast khác nhau: Success, Info, Warning, Error.',
      'Có hoạt ảnh Slide-in/Fade-in mượt mà khi xuất hiện.',
      'Auto-dismiss: Tự động biến mất sau 3 giây (có thể tuỳ chỉnh thời gian).',
      'Manual-dismiss: Có nút X để đóng thủ công ngay lập tức.',
      'Cho phép trigger nhiều Toast xếp chồng (stack) lên nhau.'
    ],
    hints: [
      'Quản lý danh sách toasts dưới dạng một mảng state [{ id, message, type, duration }].',
      'Sử dụng setTimeout để tự động remove toast dựa theo id của nó.'
    ]
  },
  {
    id: 'array-methods',
    title: 'Array myMap & myReduce Polyfills',
    category: 'Vanilla JS',
    difficulty: 'Easy',
    description: 'Viết lại các hàm map và reduce của mảng (Array.prototype) từ đầu mà không dùng các hàm gốc.',
    requirements: [
      'Array.prototype.myMap(callback, context): Trả về mảng mới, giữ nguyên mảng gốc, truyền đúng index và array gốc vào callback.',
      'Array.prototype.myReduce(callback, initialValue): Tích luỹ giá trị. Nếu không truyền initialValue, lấy phần tử đầu tiên làm accumulator và bắt đầu chạy từ index 1.',
      'Xử lý sparse arrays (mảng thưa, ví dụ [1, , 3]) chỉ chạy callback trên các phần tử thực sự tồn tại.',
      'Ném TypeError nếu callback không phải là function.'
    ],
    hints: [
      'Để kiểm tra phần tử có tồn tại thực sự hay không, sử dụng toán tử in (ví dụ: i in O).',
      'Sử dụng length >>> 0 để convert length về số nguyên dương không dấu chuẩn spec.'
    ]
  },
  {
    id: 'debounce-throttle',
    title: 'Debounce & Throttle Utilities',
    category: 'Vanilla JS',
    difficulty: 'Medium',
    description: 'Tự viết hai hàm bổ trợ kiểm soát tần suất thực thi: debounce và throttle.',
    requirements: [
      'debounce(fn, delay): Hoãn thực thi hàm fn cho đến khi không có lượt gọi nào khác trong khoảng delay.',
      'throttle(fn, limit): Giới hạn hàm fn chỉ được thực thi tối đa 1 lần trong mỗi khoảng thời gian limit.',
      'Đảm bảo giữ đúng ngữ cảnh (this context) và truyền đủ đối số (arguments) vào hàm fn.',
      'Hàm trả về phải cung cấp cơ chế cancel() để huỷ lượt thực thi đang chờ.'
    ],
    hints: [
      'Với debounce, dùng setTimeout và clearTimeout.',
      'Với throttle, có thể dùng cờ inThrottle (boolean) kết hợp setTimeout hoặc dùng timestamp Date.now() để so sánh.'
    ]
  },
  {
    id: 'promise-all',
    title: 'Promise.all Polyfill',
    category: 'Vanilla JS',
    difficulty: 'Medium',
    description: 'Tự viết hàm promiseAll nhận vào một mảng chứa các Promise (hoặc giá trị thường) và trả về một Promise duy nhất.',
    requirements: [
      'Trả về một Promise.',
      'Nếu tất cả promise thành công, resolve mảng kết quả theo ĐÚNG thứ tự đầu vào ban đầu (không phụ thuộc vào thời gian hoàn thành).',
      'Nếu bất kỳ promise nào thất bại (reject), lập tức reject với lỗi đó ngay lập tức (fail-fast).',
      'Hỗ trợ đầu vào chứa cả giá trị không phải Promise (ví dụ số, chuỗi).'
    ],
    hints: [
      'Khởi tạo một mảng kết quả cùng kích thước và đếm số lượng promise đã hoàn thành (completedCount).',
      'Khi completedCount === input.length, resolve promise bên ngoài với mảng kết quả.'
    ]
  }
];
