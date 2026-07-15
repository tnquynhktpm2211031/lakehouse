import { useState } from "react";
import {
  Navigate,
  Route,
  BrowserRouter as Router,
  Routes,
} from "react-router-dom";
import AdminDashboard from "./pages/AdminDashboard";
import CatalogHistoryTimeline from "./pages/CatalogHistoryTimeline";
import Login from "./pages/auth/Login";
import UserUpload from "./pages/UserUpload";

function App() {
  const [token, setToken] = useState(localStorage.getItem("token") || "");
  const [role, setRole] = useState(localStorage.getItem("role") || "");


  return (
    <Router>
      <Routes>
        <Route
          path="/"
          element={<Login setToken={setToken} setRole={setRole} />}
        />
        <Route
          path="/login"
          element={<Login setToken={setToken} setRole={setRole} />}
        />

        <Route
          path="/user"
          element={token ? <UserUpload /> : <Navigate to="/login" />}
        />
        <Route
          path="/admin"
          element={
            token && role === "admin" ? (
              <AdminDashboard />
            ) : (
              <Navigate to="/login" />
            )
          }
        />

        <Route
          path="/catalog"
          element={token ? <CatalogHistoryTimeline /> : <Navigate to="/login" />}
        />

        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </Router>
  );
}

export default App;
