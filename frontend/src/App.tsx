import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { useState, useEffect } from 'react'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Positions from './pages/Positions'
import CopyTrading from './pages/CopyTrading'
import BotControl from './pages/BotControl'
import Analytics from './pages/Analytics'
import Logs from './pages/Logs'
import Settings from './pages/Settings'
import Login from './pages/Login'
import Backtest from './pages/Backtest'

function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = localStorage.getItem('legion_token')
  if (!token) return <Navigate to="/login" replace />
  return <>{children}</>
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route
          path="/*"
          element={
            <RequireAuth>
              <Layout>
                <Routes>
                  <Route path="/" element={<Dashboard />} />
                  <Route path="/positions" element={<Positions />} />
                  <Route path="/copy-trading" element={<CopyTrading />} />
                  <Route path="/bot-control" element={<BotControl />} />
                  <Route path="/analytics" element={<Analytics />} />
                  <Route path="/backtest" element={<Backtest />} />
                  <Route path="/logs" element={<Logs />} />
                  <Route path="/settings" element={<Settings />} />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
              </Layout>
            </RequireAuth>
          }
        />
      </Routes>
    </BrowserRouter>
  )
}
