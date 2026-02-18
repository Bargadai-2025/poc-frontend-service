import { useState } from 'react';
import axios from 'axios';
import './App.css';

function App() {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      if (selectedFile.name.endsWith('.xlsx') || selectedFile.name.endsWith('.xls')) {
        setFile(selectedFile);
        setError(null);
        setResult(null);
      } else {
        setError('Please upload an Excel file (.xlsx or .xls)');
        setFile(null);
      }
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    const droppedFile = e.dataTransfer.files[0];
    if (droppedFile) {
      if (droppedFile.name.endsWith('.xlsx') || droppedFile.name.endsWith('.xls')) {
        setFile(droppedFile);
        setError(null);
        setResult(null);
      } else {
        setError('Please upload an Excel file (.xlsx or .xls)');
      }
    }
  };

  const handleDragOver = (e) => {
    e.preventDefault();
  };

  const handleUpload = async () => {
    if (!file) {
      setError('Please select a file first');
      return;
    }

    setUploading(true);
    setError(null);
    setResult(null);

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await axios.post(
        `${import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'}/api/process-excel`,
        formData,
        {
          headers: {
            'Content-Type': 'multipart/form-data',
          },
          responseType: 'blob',
          timeout: 300000, // 5 minutes timeout
        }
      );

      // API returns the Excel file as body; summary is in headers (backend must send Access-Control-Expose-Headers for these)
      const h = response.headers || {};
      const getHeader = (name) => h[name] ?? h[name.toLowerCase()] ?? '';
      const summary = {
        total_rows: parseInt(getHeader('x-total-rows') || getHeader('X-Total-Rows') || '0', 10),
        success: parseInt(getHeader('x-success-count') || getHeader('X-Success-Count') || '0', 10),
        incomplete: parseInt(getHeader('x-incomplete-count') || getHeader('X-Incomplete-Count') || '0', 10),
        failed: parseInt(getHeader('x-failed-count') || getHeader('X-Failed-Count') || '0', 10),
      };
      const resultFilename = (file?.name && file.name.endsWith('.xlsx'))
        ? `${file.name.replace(/\.xlsx$/i, '')}_result.xlsx`
        : 'fraud_detection_result.xlsx';
      setResult({
        summary,
        resultBlob: response.data,
        resultFilename,
      });
      setUploading(false);
    } catch (err) {
      let message = 'Upload failed. Please try again.';
      const data = err.response?.data;
      if (data?.detail) message = typeof data.detail === 'string' ? data.detail : data.detail;
      else if (data instanceof Blob) {
        try {
          const text = await data.text();
          const parsed = JSON.parse(text);
          if (parsed.detail) message = typeof parsed.detail === 'string' ? parsed.detail : parsed.detail;
        } catch (_) {}
      }
      setError(message);
      setUploading(false);
    }
  };

  const handleDownload = () => {
    if (result?.resultBlob) {
      const url = URL.createObjectURL(result.resultBlob);
      const a = document.createElement('a');
      a.href = url;
      a.download = result.resultFilename || 'fraud_detection_result.xlsx';
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  const handleReset = () => {
    setFile(null);
    setResult(null);
    setError(null);
  };

  return (
    <div className="app">
      <div className="container">
        <div className="header">
          <h1>🔍 Fraud Detection Bulk Processor</h1>
          <p>Upload Excel with email and phone columns</p>
        </div>

        {!result ? (
          <div className="upload-section">
            <div
              className={`dropzone ${file ? 'has-file' : ''}`}
              onDrop={handleDrop}
              onDragOver={handleDragOver}
            >
              <input
                type="file"
                id="file-input"
                accept=".xlsx,.xls"
                onChange={handleFileChange}
                style={{ display: 'none' }}
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
                  <p className="file-size">{(file.size / 1024).toFixed(2)} KB</p>
                  <button onClick={handleReset} className="btn-remove">
                    Remove
                  </button>
                </div>
              )}
            </div>

            {error && (
              <div className="error-message">
                ❌ {error}
              </div>
            )}

            {file && !uploading && (
              <button onClick={handleUpload} className="btn-primary">
                🚀 Process Excel File
              </button>
            )}

            {uploading && (
              <div className="processing">
                <div className="spinner"></div>
                <p className="processing-text">Processing your file...</p>
                <p className="processing-subtext">This may take a few moments</p>
              </div>
            )}
          </div>
        ) : (
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
                <div className="stat-number">{result.summary.incomplete ?? 0}</div>
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
              <p>Result file: <code>{result.resultFilename || 'fraud_detection_result.xlsx'}</code></p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
