import React, { useState } from "react";
import { problems, Problem } from "../problems/problemData";
import { solutionCodes } from "../solutions/solutionCodes";
import "../styles/dashboard.css";

// Import Practice Components
import { AutoCompletePractice } from "../problems/AutoCompletePractice";
import { ModalPractice } from "../problems/ModalPractice";
import { InfiniteScrollPractice } from "../problems/InfiniteScrollPractice";
import { StarRatingPractice } from "../problems/StarRatingPractice";
import { ToastPractice } from "../problems/ToastPractice";
import { ArrayMethodsPractice } from "../problems/ArrayMethodsPractice";
import { DebounceThrottlePractice } from "../problems/DebounceThrottlePractice";
import { PromiseAllPractice } from "../problems/PromiseAllPractice";

// Import Solution Components
import { AutoCompleteSolution } from "../solutions/AutoCompleteSolution";
import { ModalSolution } from "../solutions/ModalSolution";
import { InfiniteScrollSolution } from "../solutions/InfiniteScrollSolution";
import { StarRatingSolution } from "../solutions/StarRatingSolution";
import { ToastSolution } from "../solutions/ToastSolution";
import { ArrayMethodsSolution } from "../solutions/ArrayMethodsSolution";
import { DebounceThrottleSolution } from "../solutions/DebounceThrottleSolution";
import { PromiseAllSolution } from "../solutions/PromiseAllSolution";

type TabType = "description" | "practice" | "solution" | "code";

export const Dashboard: React.FC = () => {
  const [activeProblem, setActiveProblem] = useState<Problem>(problems[0]);
  const [activeTab, setActiveTab] = useState<TabType>("description");
  const [copied, setCopied] = useState(false);

  const getPracticeComponent = (id: string) => {
    switch (id) {
      case "autocomplete":
        return <AutoCompletePractice />;
      case "modal":
        return <ModalPractice />;
      case "infinite-scroll":
        return <InfiniteScrollPractice />;
      case "star-rating":
        return <StarRatingPractice />;
      case "toast-notification":
        return <ToastPractice />;
      case "array-methods":
        return <ArrayMethodsPractice />;
      case "debounce-throttle":
        return <DebounceThrottlePractice />;
      case "promise-all":
        return <PromiseAllPractice />;
      default:
        return <div>Practice Component Not Found</div>;
    }
  };

  const getSolutionComponent = (id: string) => {
    switch (id) {
      case "autocomplete":
        return <AutoCompleteSolution />;
      case "modal":
        return <ModalSolution />;
      case "infinite-scroll":
        return <InfiniteScrollSolution />;
      case "star-rating":
        return <StarRatingSolution />;
      case "toast-notification":
        return <ToastSolution />;
      case "array-methods":
        return <ArrayMethodsSolution />;
      case "debounce-throttle":
        return <DebounceThrottleSolution />;
      case "promise-all":
        return <PromiseAllSolution />;
      default:
        return <div>Solution Component Not Found</div>;
    }
  };

  const handleCopyCode = () => {
    const code = solutionCodes[activeProblem.id] || "";
    navigator.clipboard.writeText(code).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  };

  return (
    <div className="dashboard-container">
      {/* Sidebar: Danh sách các bài tập */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <h2>EPAM Vietnam</h2>
          <p>Frontend Interview Practice</p>
        </div>
        <nav className="problem-list" aria-label="Practice problems list">
          {problems.map((problem) => (
            <button
              key={problem.id}
              onClick={() => {
                setActiveProblem(problem);
                setActiveTab("description");
              }}
              className={`problem-item ${activeProblem.id === problem.id ? "active" : ""}`}
              style={{
                background: "none",
                textAlign: "left",
                border: "none",
                width: "100%",
              }}
            >
              <div className="problem-item-title">{problem.title}</div>
              <div className="problem-item-meta">
                <span className="category-badge">{problem.category}</span>
                <span
                  className={`difficulty-badge ${problem.difficulty.toLowerCase()}`}
                >
                  {problem.difficulty}
                </span>
              </div>
            </button>
          ))}
        </nav>
      </aside>

      {/* Main Workspace */}
      <main className="workspace">
        <header className="workspace-header">
          <div className="workspace-title">
            <h1>{activeProblem.title}</h1>
            <span
              className="category-badge"
              style={{ fontSize: "12px", padding: "4px 10px" }}
            >
              {activeProblem.category}
            </span>
            <span
              className={`difficulty-badge ${activeProblem.difficulty.toLowerCase()}`}
              style={{ fontSize: "12px" }}
            >
              {activeProblem.difficulty}
            </span>
          </div>
        </header>

        {/* Tab Selection */}
        <div className="tabs-bar" role="tablist">
          <button
            role="tab"
            aria-selected={activeTab === "description"}
            onClick={() => setActiveTab("description")}
            className={`tab-btn ${activeTab === "description" ? "active" : ""}`}
          >
            📝 Đề Bài & Yêu Cầu
          </button>
          <button
            role="tab"
            aria-selected={activeTab === "practice"}
            onClick={() => setActiveTab("practice")}
            className={`tab-btn ${activeTab === "practice" ? "active" : ""}`}
          >
            💻 Khu Vực Thực Hành
          </button>
          <button
            role="tab"
            aria-selected={activeTab === "solution"}
            onClick={() => setActiveTab("solution")}
            className={`tab-btn ${activeTab === "solution" ? "active" : ""}`}
          >
            🟢 Giải Pháp Mẫu (Solution)
          </button>
          <button
            role="tab"
            aria-selected={activeTab === "code"}
            onClick={() => setActiveTab("code")}
            className={`tab-btn ${activeTab === "code" ? "active" : ""}`}
          >
            📜 Source Code Mẫu
          </button>
        </div>

        {/* Tab Contents */}
        <section
          className="workspace-content"
          aria-label="Selected tab content"
        >
          {activeTab === "description" && (
            <div className="content-card">
              <h3>📖 Mô tả đề bài</h3>
              <p
                style={{
                  lineHeight: 1.6,
                  color: "#e5e7eb",
                  marginBottom: "24px",
                }}
              >
                {activeProblem.description}
              </p>

              <h3>🎯 Yêu cầu chức năng (Functional Requirements)</h3>
              <ul className="requirements-list">
                {activeProblem.requirements.map((req, idx) => (
                  <li key={idx}>{req}</li>
                ))}
              </ul>

              <div className="hints-container">
                <h4>💡 Gợi ý giải pháp (Hints & Tips)</h4>
                <ul
                  style={{
                    paddingLeft: "20px",
                    margin: 0,
                    lineHeight: 1.6,
                    color: "#a5f3fc",
                  }}
                >
                  {activeProblem.hints.map((hint, idx) => (
                    <li key={idx}>{hint}</li>
                  ))}
                </ul>
              </div>

              <div
                style={{
                  marginTop: "24px",
                  padding: "16px",
                  background: "#1c2432",
                  borderRadius: "8px",
                  border: "1px solid #2d3b50",
                }}
              >
                <h4 style={{ margin: "0 0 8px 0", color: "#00e5ff" }}>
                  🛠️ Cách thực hành bài tập này:
                </h4>
                <p
                  style={{
                    margin: 0,
                    fontSize: "14px",
                    lineHeight: 1.5,
                    color: "#ccc",
                  }}
                >
                  Bạn hãy mở file code tương ứng trong project editor: <br />
                  <code
                    style={{
                      background: "#0a0e17",
                      padding: "2px 6px",
                      borderRadius: "4px",
                      color: "#ff79c6",
                      display: "inline-block",
                      marginTop: "6px",
                    }}
                  >
                    /epam-practice/src/problems/
                    {activeProblem.id === "toast-notification"
                      ? "Toast"
                      : activeProblem.id === "array-methods"
                        ? "ArrayMethods"
                        : activeProblem.id === "debounce-throttle"
                          ? "DebounceThrottle"
                          : activeProblem.id === "promise-all"
                            ? "PromiseAll"
                            : activeProblem.title.replace(/\s+/g, "")}
                    Practice.tsx
                  </code>{" "}
                  <br />
                  tiến hành code logic của bạn. Kết quả thay đổi sẽ tự động cập
                  nhật ngay lập tức tại Tab <b>"Khu Vực Thực Hành"</b> ở trên!
                </p>
              </div>
            </div>
          )}

          {activeTab === "practice" && (
            <div className="content-card">
              {getPracticeComponent(activeProblem.id)}
            </div>
          )}

          {activeTab === "solution" && (
            <div className="content-card">
              {getSolutionComponent(activeProblem.id)}
            </div>
          )}

          {activeTab === "code" && (
            <div className="code-block-container">
              <div className="code-header">
                <span>Giải pháp tham khảo cho: {activeProblem.title}</span>
                <button onClick={handleCopyCode} className="copy-btn">
                  {copied ? "✓ Đã Sao Chép!" : "📋 Sao Chép Code"}
                </button>
              </div>
              <pre className="code-pre">
                <code>
                  {solutionCodes[activeProblem.id] ||
                    "// Chưa cập nhật code mẫu."}
                </code>
              </pre>
            </div>
          )}
        </section>
      </main>
    </div>
  );
};
export default Dashboard;
