import axios from 'axios'

const BASE_URL = '/api/v1'

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 15000,
})

// Auth interceptor
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('legion_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401) {
      localStorage.removeItem('legion_token')
      window.location.href = '/login'
    }
    return Promise.reject(err)
  }
)

export const authApi = {
  login: (password: string) => api.post('/auth/login', { password }),
}

export const botApi = {
  getStatus: () => api.get('/bot/status'),
  start: () => api.post('/bot/start'),
  stop: () => api.post('/bot/stop'),
  pause: () => api.post('/bot/pause'),
  resume: () => api.post('/bot/resume'),
  emergencyClose: (reason: string) => api.post('/bot/emergency-close', { reason }),
  forceTrade: (direction: string, reason: string) => api.post('/bot/force-trade', { direction, reason }),
}

export const signalApi = {
  getCurrent: () => api.get('/signal/current'),
  getAnalysis: () => api.get('/signal/analysis'),
}

export const positionApi = {
  getAll: (params?: { status?: string; limit?: number; offset?: number }) =>
    api.get('/positions', { params }),
  close: (id: number) => api.post(`/positions/${id}/close`),
}

export const copyTradingApi = {
  getTraders: () => api.get('/copy-traders'),
  addTrader: (data: any) => api.post('/copy-traders', data),
  updateTrader: (id: number, data: any) => api.patch(`/copy-traders/${id}`, data),
  deleteTrader: (id: number) => api.delete(`/copy-traders/${id}`),
  getCopyPositions: (traderId?: number) =>
    api.get('/copy-positions', { params: traderId ? { trader_id: traderId } : {} }),
}

export const settingsApi = {
  get: () => api.get('/settings'),
  update: (data: any) => api.patch('/settings', data),
}

export const analyticsApi = {
  getSummary: (days?: number) => api.get('/analytics/summary', { params: { days } }),
}

export const logsApi = {
  get: (params?: { level?: string; category?: string; limit?: number }) =>
    api.get('/logs', { params }),
  clear: () => api.delete('/logs'),
}

export const marketApi = {
  getTicker: () => api.get('/market/ticker'),
  getContext: () => api.get('/market/context'),
}

export const hyblockApi = {
  getData: () => api.get('/hyblock/data'),
}

export const backtestApi = {
  run: (config: any) => api.post('/backtest/run', config),
  getStatus: () => api.get('/backtest/status'),
  getResults: () => api.get('/backtest/results'),
  cancel: () => api.post('/backtest/cancel'),
  clearCache: () => api.delete('/backtest/cache'),
}

export default api
