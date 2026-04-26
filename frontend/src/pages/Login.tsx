import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { authApi } from '../lib/api'
import { Zap, Lock } from 'lucide-react'
import toast from 'react-hot-toast'

export default function Login() {
  const [password, setPassword] = useState('')
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    try {
      const res = await authApi.login(password)
      localStorage.setItem('legion_token', res.data.token)
      navigate('/')
    } catch (err: any) {
      toast.error(err.response?.data?.detail || 'Invalid password')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen bg-dark-900 flex items-center justify-center p-4">
      {/* Background glow */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-1/3 left-1/2 -translate-x-1/2 -translate-y-1/2 w-96 h-96 bg-brand/5 rounded-full blur-3xl" />
      </div>

      <div className="w-full max-w-sm relative">
        {/* Logo */}
        <div className="flex flex-col items-center gap-4 mb-8">
          <div className="w-16 h-16 rounded-2xl bg-brand flex items-center justify-center glow-brand">
            <Zap size={32} className="text-white" />
          </div>
          <div className="text-center">
            <h1 className="text-2xl font-bold text-white">Vroom Capital</h1>
            <p className="text-sm text-gray-500 mt-1">BTC Futures Trading System</p>
          </div>
        </div>

        {/* Card */}
        <div className="card border-dark-500">
          <h2 className="text-lg font-semibold text-white mb-6 flex items-center gap-2">
            <Lock size={18} className="text-brand" />
            Admin Access
          </h2>

          <form onSubmit={handleLogin} className="space-y-4">
            <div>
              <label className="label">Password</label>
              <input
                type="password"
                className="input"
                placeholder="Enter admin password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoFocus
              />
            </div>

            <button
              type="submit"
              disabled={loading || !password}
              className="btn-primary w-full justify-center py-2.5"
            >
              {loading ? (
                <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              ) : (
                'Sign In'
              )}
            </button>
          </form>
        </div>

        <p className="text-center text-xs text-gray-600 mt-6">
          Vroom Capital · 86X BTC Futures · 24/7 Autonomous
        </p>
      </div>
    </div>
  )
}
