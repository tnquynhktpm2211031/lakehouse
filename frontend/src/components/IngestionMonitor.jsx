import React, { useEffect, useState } from 'react';
import axios from 'axios';

const BACKEND = import.meta.env.VITE_BACKEND_URL || 'http://localhost:8000';

const IngestionMonitor = () => {
  const [logs, setLogs] = useState([]);
  const [events, setEvents] = useState([]);

  useEffect(() => {
    const token = localStorage.getItem('token');
    const headers = token ? { Authorization: `Bearer ${token}` } : {};

    // initial fetch
    axios.get(`${BACKEND}/ingest/logs`, { headers })
      .then((res) => setLogs(res.data.slice(0, 10)))
      .catch(() => {});

    // websocket
    let ws;
    try {
      const wsUrl = (BACKEND.startsWith('https') ? 'wss' : 'ws') + '://' + BACKEND.replace(/https?:\/\//, '') + '/ingest/ws';
      ws = new WebSocket(wsUrl);
      ws.onmessage = (e) => {
        try {
          const payload = JSON.parse(e.data);
          setEvents((s) => [payload].concat(s).slice(0, 20));
          // refresh logs on relevant events
          if (payload.event === 'NEW_FILE_RECEIVED' || payload.event === 'FILE_STATUS_UPDATED') {
            axios.get(`${BACKEND}/ingest/logs`, { headers })
              .then((res) => setLogs(res.data.slice(0, 10)))
              .catch(() => {});
          }
        } catch (err) {
          // ignore
        }
      };
    } catch (err) {
      // ignore
    }

    return () => {
      try { ws && ws.close(); } catch (e) {}
    };
  }, []);

  return (
    <div className="ml-4 flex items-center gap-3">
      <div className="text-sm text-gray-500">Ingest</div>
      <div className="bg-white border px-3 py-1 rounded shadow-sm max-w-md">
        <div className="text-xs text-gray-400 mb-1">Recent files</div>
        <div className="space-y-1">
          {logs.length === 0 && <div className="text-xs text-gray-400">No recent files</div>}
          {logs.map((l) => (
            <div key={l.id} className="flex items-center justify-between text-xs">
              <div className="truncate pr-2">{l.file_name} — <span className="text-gray-400">{l.sender}</span></div>
              <div className="text-[11px] flex items-center gap-2">
                <span className="px-2 py-0.5 rounded-full bg-gray-100">{l.pipeline_stage}</span>
                <span className="text-gray-400">{new Date(l.receive_time).toLocaleTimeString()}</span>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default IngestionMonitor;
