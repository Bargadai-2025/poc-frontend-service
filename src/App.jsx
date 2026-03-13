import { useState, useRef, useContext } from "react"; // ✅ added useRef
import axios from "axios";
import "./App.css";
import Navbar from "./components/navbar";
import ServiceContextProvider from "./context/servicesProviderContext";
// const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000';
const API_BASE = "https://poc-backend-y.onrender.com";

function App() {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [progress, setProgress] = useState({ processed: 0, total: 0 }); // ✅ added
  const pollingRef = useRef(null); // ✅ added
  const { service } = useContext(ServiceContextProvider);
  // ✅ UNCHANGED
  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      if (
        selectedFile.name.endsWith(".xlsx") ||
        selectedFile.name.endsWith(".xls")
      ) {
        setFile(selectedFile);
        setError(null);
        setResult(null);
      } else {
        setError("Please upload an Excel file (.xlsx or .xls)");
        setFile(null);
      }
    }
  };

  // ✅ UNCHANGED
  const handleDrop = (e) => {
    e.preventDefault();
    const droppedFile = e.dataTransfer.files[0];
    if (droppedFile) {
      if (
        droppedFile.name.endsWith(".xlsx") ||
        droppedFile.name.endsWith(".xls")
      ) {
        setFile(droppedFile);
        setError(null);
        setResult(null);
      } else {
        setError("Please upload an Excel file (.xlsx or .xls)");
      }
    }
  };

  // ✅ UNCHANGED
  const handleDragOver = (e) => {
    e.preventDefault();
  };

  // ✅ CHANGED: now uses background job polling instead of waiting
  const handleUpload = async () => {
    if (!file) {
      setError("Please select a file first");
      return;
    }

    setUploading(true);
    setError(null);
    setResult(null);
    setProgress({ processed: 0, total: 0 });

    const formData = new FormData();
    formData.append("file", file);
    formData.append("service", service); // 👈 important

    try {
      // Step 1: Upload → get job_id instantly (no long wait)
      const uploadRes = await axios.post(
        `${API_BASE}/api/process-excel`,
        formData,
        {
          headers: { "Content-Type": "multipart/form-data" },
        },
      );
      const { job_id } = uploadRes.data;

      // Step 2: Poll every 10 seconds for status
      pollingRef.current = setInterval(async () => {
        try {
          const statusRes = await axios.get(`${API_BASE}/api/jobs/${job_id}`);
          const data = statusRes.data;

          setProgress({
            processed: data.processed || 0,
            total: data.total || 0,
          });

          if (data.status === "done") {
            clearInterval(pollingRef.current);

            // Step 3: Download result Excel
            const downloadRes = await axios.get(
              `${API_BASE}/api/jobs/${job_id}/download`,
              { responseType: "blob" },
            );

            const summary = {
              total_rows: data.total_rows || 0,
              success: data.success || 0,
              incomplete: data.incomplete || 0,
              failed: data.failed || 0,
            };

            const resultFilename =
              file?.name && file.name.endsWith(".xlsx")
                ? `${file.name.replace(/\.xlsx$/i, "")}_result.xlsx`
                : "fraud_detection_result.xlsx";

            setResult({
              summary,
              resultBlob: downloadRes.data,
              resultFilename,
            });
            setUploading(false);

            // Automatically trigger download after a short delay
            setTimeout(() => {
              const url = URL.createObjectURL(downloadRes.data);
              const a = document.createElement("a");
              a.href = url;
              a.download = resultFilename || "fraud_detection_result.xlsx";

              document.body.appendChild(a); // required in some browsers
              a.click(); // trigger download
              a.remove();

              URL.revokeObjectURL(url);
            }, 1500);
          } else if (data.status === "failed") {
            clearInterval(pollingRef.current);
            setError(data.error || "Processing failed. Please try again.");
            setUploading(false);
          }
        } catch (pollErr) {
          console.error("Polling error:", pollErr);
          // Don't stop polling on a single network hiccup
        }
      }, 10000); // poll every 10 seconds
    } catch (err) {
      let message = "Upload failed. Please try again.";
      const isTimeout =
        err.code === "ECONNABORTED" ||
        (err.message && err.message.toLowerCase().includes("timeout"));
      if (isTimeout) {
        message = "Request timed out. Please try again.";
      } else {
        const data = err.response?.data;
        if (data?.detail)
          message = typeof data.detail === "string" ? data.detail : data.detail;
        else if (data instanceof Blob) {
          try {
            const text = await data.text();
            const parsed = JSON.parse(text);
            if (parsed.detail)
              message =
                typeof parsed.detail === "string"
                  ? parsed.detail
                  : parsed.detail;
          } catch (_) {}
        }
      }
      setError(message);
      setUploading(false);
    }
  };

  // ✅ UNCHANGED
  const handleDownload = () => {
    if (result?.resultBlob) {
      const url = URL.createObjectURL(result.resultBlob);
      const a = document.createElement("a");
      a.href = url;
      a.download = result.resultFilename || "fraud_detection_result.xlsx";
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  // ✅ CHANGED: also clears polling interval on reset
  const handleReset = () => {
    if (pollingRef.current) clearInterval(pollingRef.current);
    setFile(null);
    setResult(null);
    setError(null);
    setProgress({ processed: 0, total: 0 });
  };

  return (
    <div className="app">
      <Navbar />
      <div className="container">
        {!result ? (
          <div className="upload-section">
            <div
              className={`dropzone ${file ? "has-file" : ""}`}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
            >
              <input
                type="file"
                id="file-input"
                accept=".xlsx,.xls"
                onChange={handleFileChange}
                style={{ display: "none" }}
              />

              {!file ? (
                <label htmlFor="file-input" className="upload-label">
                  <div className="upload-icon">📄</div>
                  <p className="upload-text">Drag & drop Excel file here</p>
                  <p className="upload-subtext">or click to browse</p>
                  <p className="upload-hint">Supports .xlsx and .xls files</p>
                </label>
              ) : (
                <div className="file-info">
                  <div className="file-icon">✅</div>
                  <p className="file-name">{file.name}</p>
                  <p className="file-size">
                    {(file.size / 1024).toFixed(2)} KB
                  </p>
                  <button onClick={handleReset} className="btn-remove">
                    Remove
                  </button>
                </div>
              )}
            </div>

            {error && <div className="error-message">❌ {error}</div>}

            {file && !uploading && (
              <button onClick={handleUpload} className="btn-primary">
                🚀 Process Excel File
              </button>
            )}

            {/* ✅ CHANGED: shows live progress row count */}
            {uploading && (
              <div className="processing">
                <div className="spinner"></div>
                <p className="processing-text">Processing your file...</p>
                {progress.total > 0 ? (
                  <p className="processing-subtext">
                    Processed {progress.processed} / {progress.total} rows...
                  </p>
                ) : (
                  <p className="processing-subtext">
                    This may take a few minutes
                  </p>
                )}
              </div>
            )}
          </div>
        ) : (
          // ✅ UNCHANGED: result section exactly as before
          <div className="result-section">
            <div className="success-icon">✅</div>
            <h2>Processing Complete!</h2>

            <div className="stats">
              <div className="stat-card">
                <div className="stat-number">{result.summary.total_rows}</div>
                <div className="stat-label">Total Rows</div>
              </div>
              <div className="stat-card success">
                <div className="stat-number">{result.summary.success}</div>
                <div className="stat-label">Successful</div>
              </div>
              <div className="stat-card incomplete">
                <div className="stat-number">
                  {result.summary.incomplete ?? 0}
                </div>
                <div className="stat-label">Incomplete</div>
              </div>
              <div className="stat-card failed">
                <div className="stat-number">{result.summary.failed}</div>
                <div className="stat-label">Failed</div>
              </div>
            </div>

            <div className="button-group">
              <button onClick={handleDownload} className="btn-download">
                📥 Download Result
              </button>
              <button onClick={handleReset} className="btn-secondary">
                🔄 Process Another File
              </button>
            </div>

            <div className="job-info">
              <p>
                Result file:{" "}
                <code>
                  {result.resultFilename || "fraud_detection_result.xlsx"}
                </code>
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
