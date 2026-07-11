import React, { useEffect, useState } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const CatalogHistoryTimeline = () => {
  const [references, setReferences] = useState([]);
  const [selectedRef, setSelectedRef] = useState('main');
  const [commits, setCommits] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const authHeader = { Authorization: `Bearer ${localStorage.getItem('token')}` };

  // Tải danh sách branch/tag 1 lần khi mở tab
  useEffect(() => {
    axios
      .get(`${API_URL}/catalog/references`, { headers: authHeader })
      .then((res) => setReferences(res.data.references || []))
      .catch(() => setError('Không thể tải danh sách branch/tag từ Nessie.'));
  }, []);

  // Tải lại lịch sử commit mỗi khi đổi ref
  useEffect(() => {
    setLoading(true);
    setError('');
    axios
      .get(`${API_URL}/catalog/history`, {
        headers: authHeader,
        params: { ref: selectedRef, limit: 50 },
      })
      .then((res) => setCommits(res.data.commits || []))
      .catch(() => setError(`Không thể tải lịch sử commit cho ref '${selectedRef}'.`))
      .finally(() => setLoading(false));
  }, [selectedRef]);

  const formatTime = (iso) => {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('vi-VN');
  };

  return (
    <div className="p-6 overflow-y-auto h-full">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-semibold text-gray-800">
            🕒 Lịch sử Phiên bản Catalog (Nessie)
          </h3>
          <p className="text-sm text-gray-400 mt-1">
            Mỗi dòng thời gian dưới đây tương ứng với 1 commit (ingest/merge/tag) trên Iceberg catalog.
          </p>
        </div>
        <select
          value={selectedRef}
          onChange={(e) => setSelectedRef(e.target.value)}
          className="border border-gray-300 rounded-lg px-3 py-2 text-sm bg-white shrink-0"
        >
          {references.length === 0 && <option value="main">main</option>}
          {references.map((r) => (
            <option key={r.name} value={r.name}>
              {r.type === 'TAG' ? '🏷️ ' : '🌿 '}
              {r.name}
            </option>
          ))}
        </select>
      </div>

      {error && (
        <div className="bg-red-50 text-red-600 border border-red-200 p-3 rounded-lg mb-4 text-sm">
          {error}
        </div>
      )}

      {loading ? (
        <p className="text-gray-400 text-sm">Đang tải lịch sử commit...</p>
      ) : commits.length === 0 ? (
        <p className="text-gray-400 text-sm">Chưa có commit nào trên ref này.</p>
      ) : (
        <ol className="relative border-l-2 border-blue-200 ml-3">
          {commits.map((c, idx) => (
            <li key={c.hash || idx} className="mb-8 ml-6">
              <span className="absolute flex items-center justify-center w-6 h-6 bg-blue-100 rounded-full -left-3 ring-4 ring-white text-xs">
                {idx === 0 ? '🟢' : '🔹'}
              </span>
              <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                <div className="flex justify-between items-start gap-4">
                  <p className="font-medium text-gray-800">
                    {c.message || '(không có message)'}
                  </p>
                  <span className="text-xs text-gray-400 whitespace-nowrap">
                    {formatTime(c.commit_time)}
                  </span>
                </div>
                <div className="mt-2 flex items-center gap-3 text-xs text-gray-500">
                  <span className="font-mono bg-gray-100 px-2 py-0.5 rounded">
                    {c.hash ? c.hash.slice(0, 8) : '—'}
                  </span>
                  <span>👤 {c.author || 'unknown'}</span>
                </div>
              </div>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
};

export default CatalogHistoryTimeline;
