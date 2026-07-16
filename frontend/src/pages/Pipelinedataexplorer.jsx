import React, { useEffect, useState } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const LAYER_INFO = {
  bronze: { label: 'Bronze', desc: 'File Parquet thô (PDF/DOCX đã parse)', color: '#B45309' },
  silver: { label: 'Silver', desc: 'Bảng Iceberg đã chuẩn hóa (kpi_cusc_master)', color: '#94A3B8' },
  gold:   { label: 'Gold',   desc: '4 Data Mart phục vụ báo cáo & dashboard', color: '#CA8A04' },
};
const Heardertable = {
    ma_chi_tieu: 'Mã chỉ tiêu',
    nhom_don_vi: 'Nhóm đơn vị',
    ten_phong_ban: 'Tên phòng ban',
    quy_danh_gia: 'Quy trình đánh giá',
    noi_dung_muc_tieu: 'Nội dung mục tiêu',
    dinh_ky_thu_thap: 'Định kỳ thu thập',
    muc_dang_ky: 'Mức đăng ký',
    muc_dat: 'Mức đạt',
    muc_dat_numberic: 'Mức đạt (số)',
    ket_qua_he_thong: 'Kết quả hệ thống',
    nguyen_nhan: 'Nguyên nhân',
    hanh_dong_khac_phuc: 'Hành động khắc phục',
    file_nguon: 'File nguồn',
    thoi_gian_dong_goi_gold: 'Thời gian đóng gói',
}

const GOLD_TABLE_LABELS = {
  kpi_tong_hop_don_vi: 'Tổng hợp theo đơn vị',
  kpi_chi_tiet_dashboard: 'Chi tiết đầy đủ',
  kpi_so_sanh_ky: 'So sánh giữa các kỳ',
  dm_chi_tieu: 'Chú thích / Data Dictionary',
};

const PipelineDataExplorer = () => {
  const [status, setStatus] = useState({ bronze: false, silver: false, gold: false });
  const [activeLayer, setActiveLayer] = useState(null); // 'bronze' | 'silver' | 'gold' | null
  const [activeGoldTable, setActiveGoldTable] = useState('kpi_chi_tiet_dashboard');
  const [previewData, setPreviewData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const authHeader = { Authorization: `Bearer ${localStorage.getItem('token')}` };

  useEffect(() => {
    axios
      .get(`${API_URL}/pipeline/status`, { headers: authHeader })
      .then((res) => setStatus(res.data))
      .catch(() => setError('Không thể tải trạng thái pipeline.'));
  }, []);

  const openLayer = async (layer) => {
    setActiveLayer(layer);
    setError('');
    setPreviewData(null);
    setLoading(true);
    try {
      const url =
        layer === 'gold'
          ? `${API_URL}/pipeline/gold/preview?table=${activeGoldTable}&limit=50`
          : `${API_URL}/pipeline/${layer}/preview?limit=50`;
      const res = await axios.get(url, { headers: authHeader });
      setPreviewData(res.data);
    } catch (e) {
      setError(`Không tải được dữ liệu tầng ${LAYER_INFO[layer].label}. Có thể tầng này chưa chạy.`);
    } finally {
      setLoading(false);
    }
  };

  // Khi đổi bảng Gold trong lúc đang xem Gold, tự tải lại
  useEffect(() => {
    if (activeLayer === 'gold') openLayer('gold');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeGoldTable]);

  const closeModal = () => {
    setActiveLayer(null);
    setPreviewData(null);
    setError('');
  };

  const nodeClass = (layer) => {
    const isDone = status[layer];
    return `relative flex-1 border-2 rounded-xl p-5 text-center transition cursor-pointer ${
      isDone
        ? 'border-green-500 bg-green-50 hover:bg-green-100'
        : 'border-gray-300 bg-gray-50 hover:bg-gray-100 opacity-70'
    }`;
  };

  return (
    <div className="p-6 h-full overflow-y-auto">
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-800">Pipeline Dữ Liệu: Bronze → Silver → Gold</h3>
        <p className="text-sm text-gray-400 mt-1">
          Click vào từng tầng để xem bảng dữ liệu thật đang có ở tầng đó (giống trạng thái chạy trên Airflow).
        </p>
      </div>

      {/* Sơ đồ pipeline dạng node nối tiếp */}
      <div className="flex items-center gap-3 mb-8">
        {['bronze', 'silver', 'gold'].map((layer, idx) => (
          <React.Fragment key={layer}>
            <div className={nodeClass(layer)} onClick={() => openLayer(layer)}>
              <div className="flex items-center justify-center gap-2 mb-1">
                <span
                  className={`w-2.5 h-2.5 rounded-full ${status[layer] ? 'bg-green-500' : 'bg-gray-400'}`}
                />
                <span className="font-semibold text-gray-800">{LAYER_INFO[layer].label}</span>
              </div>
              <p className="text-xs text-gray-500">{LAYER_INFO[layer].desc}</p>
              <p className="text-[11px] text-blue-600 mt-2 underline">Xem bảng dữ liệu →</p>
            </div>
            {idx < 2 && <div className="text-gray-300 text-2xl">➜</div>}
          </React.Fragment>
        ))}
      </div>

      {error && !activeLayer && (
        <div className="bg-red-50 text-red-600 border border-red-200 p-3 rounded-lg mb-4 text-sm">{error}</div>
      )}

      {/* Modal xem dữ liệu */}
      {activeLayer && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-6">
          <div className="bg-white rounded-xl shadow-2xl w-full max-w-6xl max-h-[85vh] flex flex-col overflow-hidden">
            <div className="flex items-center justify-between px-6 py-4 border-b bg-gray-50">
              <div>
                <h4 className="font-semibold text-gray-800">
                  Dữ liệu tầng {LAYER_INFO[activeLayer].label}
                  {previewData?.source_file && (
                    <span className="text-xs text-gray-400 font-normal ml-2">({previewData.source_file})</span>
                  )}
                  {previewData?.source_table && (
                    <span className="text-xs text-gray-400 font-normal ml-2">({previewData.source_table})</span>
                  )}
                </h4>
                {previewData && (
                  <p className="text-xs text-gray-400 mt-0.5">
                    Tổng {previewData.total_rows} dòng — đang hiển thị {previewData.rows?.length || 0} dòng đầu
                  </p>
                )}
              </div>
              <button onClick={closeModal} className="text-gray-400 hover:text-gray-700 text-xl leading-none">
                ✕
              </button>
            </div>

            {/* Chọn bảng khi đang xem Gold (Gold có 4 bảng) */}
            {activeLayer === 'gold' && (
              <div className="flex-inline gap-2 px-6 py-3 border-b bg-white">
                {Object.entries(GOLD_TABLE_LABELS).map(([key, label]) => (
                  <button
                    key={key}
                    onClick={() => setActiveGoldTable(key)}
                    className={`text-xs  h-[40px] px-3 py-2 rounded-full border transition ${
                      activeGoldTable === key
                        ? 'bg-blue-600 text-white border-blue-600'
                        : 'bg-gray-100 text-gray-600 border-gray-200 hover:bg-gray-200'
                    }`}
                  >
                    {label}
                  </button>
                ))}
              </div>
            )}

            <div className="overflow-auto p-4 min-h-[60vh]" >
              {loading && <p className="text-gray-400 text-sm text-center flex items-center justify-center py-10">Đang tải dữ liệu...</p>}
              {error && !loading && <p className="text-red-500 text-sm text-center py-10">{error}</p>}

              {!loading && !error && previewData && (
                <table className="w-full text-xs border-collapse">
                  <thead className="sticky top-[-20px] bg-gray-100 py-10">
                    <tr className="border-b border-gray-200"> 
                      <th className="text-left text-xs px-3 py-2 border-b  text-slate-700 font-semibold whitespace-nowrap">
                        STT
                      </th>
                      {previewData.columns.map((c) => (
                        <th key={c} className="text-left text-xs px-3 py-2 border-b  text-slate-700 font-semibold whitespace-nowrap">
                            {Heardertable[c] || c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewData.rows.map((row, i) => (
                    
                      <tr key={i} className="odd:bg-white even:bg-gray-50">
                        <td className="px-3 py-1.5 border-b border-gray-100 whitespace-nowrap">
                          {i + 1}
                        </td>
                        {previewData.columns.map((c) => (
                          <td key={c} className="px-3 py-1.5 border-b border-gray-100 whitespace-nowrap">
                            {String(row[c] ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PipelineDataExplorer;
