import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { analyticsApi } from '../lib/api'
import { formatUsd, formatPct } from '../lib/utils'
import { BarChart2, TrendingUp, TrendingDown, Target, Activity, DollarSign } from 'lucide-react'
import { clsx } from 'clsx'
import {
  AreaChart, Area, BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell
} from 'recharts'

const PERIOD_OPTIONS = [7, 14, 30, 90, 365]

const tooltipStyle = {
  contentStyle: { background: '#141424', border: '1px solid #20203c', borderRadius: '8px', fontSize: '12px' },
  labelStyle: { color: '#9ca3af' },
  cursor: { fill: 'rgba(99, 102, 241, 0.05)' },
}

export default function Analytics() {
  const [period, setPeriod] = useState(30)

  const { data, isLoading } = useQuery({
    queryKey: ['analytics', period],
    queryFn: () => analyticsApi.getSummary(period).then(r => r.data),
    refetchInterval: 30000,
  })

  const d = data || {}
  const dailyPnl = d.daily_pnl || []

  // Running cumulative PnL
  let running = 0
  const cumulativePnl = dailyPnl.map((item: any) => {
    running += item.pnl
    return { date: item.date, pnl: item.pnl, cumulative: parseFloat(running.toFixed(2)) }
  })

  if (isLoading) {
    return <div className="flex items-center justify-center h-64 text-gray-500">Loading analytics...</div>
  }

  if (!d.total_trades) {
    return (
      <div className="text-center py-20">
        <BarChart2 size={48} className="mx-auto mb-4 text-gray-700" />
        <div className="text-gray-400 font-medium">No trades in this period</div>
        <div className="text-sm text-gray-600">Analytics will appear once you have trade history</div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Analytics</h1>
          <p className="text-sm text-gray-500 mt-0.5">{d.total_trades} trades in last {period} days</p>
        </div>
        {/* Period selector */}
        <div className="flex gap-1 bg-dark-800 border border-dark-600 rounded-lg p-1">
          {PERIOD_OPTIONS.map(p => (
            <button
              key={p}
              onClick={() => setPeriod(p)}
              className={clsx('px-3 py-1.5 rounded-md text-xs font-medium transition-all',
                period === p ? 'bg-brand text-white' : 'text-gray-400 hover:text-white'
              )}
            >
              {p}D
            </button>
          ))}
        </div>
      </div>

      {/* Key metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          {
            label: 'Total PnL', icon: DollarSign,
            value: formatUsd(d.total_pnl_usd),
            color: (d.total_pnl_usd || 0) >= 0 ? 'text-profit' : 'text-loss',
          },
          {
            label: 'Win Rate', icon: Target,
            value: `${d.win_rate || 0}%`,
            sub: `${d.winning_trades}W / ${d.losing_trades}L`,
            color: (d.win_rate || 0) >= 55 ? 'text-profit' : 'text-warning',
          },
          {
            label: 'Avg Win', icon: TrendingUp,
            value: formatPct(d.avg_win_pct),
            color: 'text-profit',
          },
          {
            label: 'Avg Loss', icon: TrendingDown,
            value: formatPct(d.avg_loss_pct),
            color: 'text-loss',
          },
        ].map(({ label, icon: Icon, value, sub, color }) => (
          <div key={label} className="card flex items-start gap-3">
            <div className="w-9 h-9 rounded-lg bg-dark-600 flex items-center justify-center shrink-0">
              <Icon size={18} className="text-gray-400" />
            </div>
            <div>
              <div className="text-xs text-gray-500 uppercase tracking-wide">{label}</div>
              <div className={clsx('text-xl font-bold font-mono mt-0.5', color)}>{value}</div>
              {sub && <div className="text-xs text-gray-500">{sub}</div>}
            </div>
          </div>
        ))}
      </div>

      {/* Secondary metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Profit Factor', value: d.profit_factor?.toFixed(2) || '—', color: 'text-brand-light' },
          { label: 'Best Trade', value: formatPct(d.best_trade_pct), color: 'text-profit' },
          { label: 'Worst Trade', value: formatPct(d.worst_trade_pct), color: 'text-loss' },
          { label: 'Liquidations', value: d.liquidations || 0, color: 'text-warning' },
        ].map(({ label, value, color }) => (
          <div key={label} className="card text-center">
            <div className="text-xs text-gray-500">{label}</div>
            <div className={clsx('text-xl font-bold font-mono mt-1', color)}>{value}</div>
          </div>
        ))}
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Cumulative PnL */}
        <div className="card">
          <h3 className="text-sm font-semibold text-white mb-4">Cumulative PnL</h3>
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={cumulativePnl}>
              <defs>
                <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#6366f1" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#6366f1" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#20203c" />
              <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} />
              <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} tickFormatter={(v) => `$${v}`} />
              <Tooltip {...tooltipStyle} formatter={(v: number) => [`$${v.toFixed(2)}`, 'PnL']} />
              <Area type="monotone" dataKey="cumulative" stroke="#6366f1" strokeWidth={2} fill="url(#pnlGrad)" />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        {/* Daily PnL bars */}
        <div className="card">
          <h3 className="text-sm font-semibold text-white mb-4">Daily PnL</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={dailyPnl}>
              <CartesianGrid strokeDasharray="3 3" stroke="#20203c" />
              <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} />
              <YAxis tick={{ fill: '#6b7280', fontSize: 10 }} tickLine={false} tickFormatter={(v) => `$${v}`} />
              <Tooltip {...tooltipStyle} formatter={(v: number) => [`$${v.toFixed(2)}`, 'PnL']} />
              <Bar dataKey="pnl" radius={[2, 2, 0, 0]}>
                {dailyPnl.map((entry: any, index: number) => (
                  <Cell key={index} fill={entry.pnl >= 0 ? '#10b981' : '#ef4444'} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Trade distribution */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="card">
          <h3 className="text-sm font-semibold text-white mb-4">Direction Split</h3>
          <div className="space-y-3">
            <div>
              <div className="flex justify-between text-xs mb-1.5">
                <span className="text-profit">LONG</span>
                <span className="text-gray-400">{d.long_trades} ({d.total_trades ? Math.round(d.long_trades/d.total_trades*100) : 0}%)</span>
              </div>
              <div className="h-2 bg-dark-600 rounded-full overflow-hidden">
                <div className="h-full bg-profit rounded-full transition-all"
                  style={{ width: `${d.total_trades ? (d.long_trades/d.total_trades*100) : 0}%` }} />
              </div>
            </div>
            <div>
              <div className="flex justify-between text-xs mb-1.5">
                <span className="text-loss">SHORT</span>
                <span className="text-gray-400">{d.short_trades} ({d.total_trades ? Math.round(d.short_trades/d.total_trades*100) : 0}%)</span>
              </div>
              <div className="h-2 bg-dark-600 rounded-full overflow-hidden">
                <div className="h-full bg-loss rounded-full transition-all"
                  style={{ width: `${d.total_trades ? (d.short_trades/d.total_trades*100) : 0}%` }} />
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <h3 className="text-sm font-semibold text-white mb-4">Outcome Distribution</h3>
          <div className="space-y-3">
            {[
              { label: 'Winners', count: d.winning_trades, total: d.total_trades, color: 'bg-profit', textColor: 'text-profit' },
              { label: 'Losers', count: d.losing_trades, total: d.total_trades, color: 'bg-warning', textColor: 'text-warning' },
              { label: 'Liquidated', count: d.liquidations, total: d.total_trades, color: 'bg-loss', textColor: 'text-loss' },
            ].map(({ label, count, total, color, textColor }) => (
              <div key={label}>
                <div className="flex justify-between text-xs mb-1.5">
                  <span className={textColor}>{label}</span>
                  <span className="text-gray-400">{count} ({total ? Math.round(count/total*100) : 0}%)</span>
                </div>
                <div className="h-2 bg-dark-600 rounded-full overflow-hidden">
                  <div className={`h-full ${color} rounded-full transition-all`}
                    style={{ width: `${total ? (count/total*100) : 0}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
