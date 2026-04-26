import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { backtestApi } from '../lib/api'
import { formatPrice, formatPct, formatUsd, formatDate } from '../lib/utils'
import {
  Play, Square, Trash2, BarChart2, TrendingUp, TrendingDown,
  AlertTriangle, Target, Clock, Settings, ChevronDown, ChevronUp,
  ArrowUpRight, ArrowDownRight, RefreshCw
} from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'
import {
  AreaChart, Area, BarChart, Bar, ComposedChart, Line, LineChart,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine,
  Cell, Legend
} from 'recharts'

const tooltipStyle = {
  contentStyle: { background: '#141424', border: '1px solid #20203c', borderRadius: '8px', fontSize: '11px' },
  labelStyle: { color: '#9ca3af' },
}

// ─── Config Form ──────────────────────────────────────────────────────────────

interface BtConfig {
  start_year: number; end_year: number
  initial_capital: number; leverage: number
  position_size_pct: number; liquidation_buffer_usd: number
  tp1_pct: number; tp2_pct: number
  velocity_threshold_pct: number; velocity_window_hours: number
  zone_cooldown_minutes: number; emergency_candles: number
  fomc_caution_days: number
  use_time_filter: boolean; use_velocity_filter: boolean
  use_funding_filter: boolean; use_macro_filter: boolean
  use_zone_system: boolean; use_second_break_rule: boolean
}

const DEFAULT_CONFIG: BtConfig = {
  start_year: 2020, end_year: 2025,
  initial_capital: 1000, leverage: 86,
  position_size_pct: 0.30, liquidation_buffer_usd: 4000,
  tp1_pct: 0.20, tp2_pct: 0.30,
  velocity_threshold_pct: 1.5, velocity_window_hours: 2,
  zone_cooldown_minutes: 120, emergency_candles: 4,
  fomc_caution_days: 7,
  use_time_filter: true, use_velocity_filter: true,
  use_funding_filter: true, use_macro_filter: true,
  use_zone_system: true, use_second_break_rule: true,
}

function ConfigPanel({ config, onChange }: { config: BtConfig; onChange: (c: BtConfig) => void }) {
  const [open, setOpen] = useState(false)

  const toggle = (key: keyof BtConfig) => onChange({ ...config, [key]: !config[key as keyof BtConfig] })
  const set = (key: keyof BtConfig, val: any) => onChange({ ...config, [key]: val })

  const FilterToggle = ({ label, field }: { label: string; field: keyof BtConfig }) => (
    <button
      onClick={() => toggle(field)}
      className={clsx(
        'flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all',
        config[field]
          ? 'bg-brand/10 border-brand/30 text-brand-light'
          : 'bg-dark-700 border-dark-500 text-gray-500'
      )}
    >
      <div className={clsx('w-3 h-3 rounded-full', config[field] ? 'bg-brand' : 'bg-dark-400')} />
      {label}
    </button>
  )

  return (
    <div className="card">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center justify-between w-full"
      >
        <h3 className="text-sm font-semibold text-white flex items-center gap-2">
          <Settings size={16} className="text-brand" />
          Backtest Configuration
        </h3>
        {open ? <ChevronUp size={16} className="text-gray-500" /> : <ChevronDown size={16} className="text-gray-500" />}
      </button>

      {open && (
        <div className="mt-4 space-y-5">
          {/* Date & capital */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {([
              { label: 'Start Year', key: 'start_year', min: 2019, max: 2024 },
              { label: 'End Year', key: 'end_year', min: 2020, max: 2025 },
              { label: 'Starting Capital ($)', key: 'initial_capital', min: 100, max: 100000 },
              { label: 'Leverage', key: 'leverage', min: 10, max: 125 },
            ] as any[]).map(({ label, key, min, max }) => (
              <div key={key}>
                <label className="label">{label}</label>
                <input type="number" className="input" min={min} max={max}
                  value={config[key as keyof BtConfig] as number}
                  onChange={e => set(key as keyof BtConfig, parseFloat(e.target.value))} />
              </div>
            ))}
          </div>

          {/* Strategy params */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {([
              { label: 'Position Size', key: 'position_size_pct', step: 0.01, min: 0.05, max: 0.60 },
              { label: 'Liq Buffer ($)', key: 'liquidation_buffer_usd', min: 500, max: 10000 },
              { label: 'TP1 (%)', key: 'tp1_pct', step: 0.01, min: 0.05, max: 1.0 },
              { label: 'TP2 (%)', key: 'tp2_pct', step: 0.01, min: 0.10, max: 2.0 },
              { label: 'Velocity %', key: 'velocity_threshold_pct', step: 0.1, min: 0.5, max: 5 },
              { label: 'Velocity Hrs', key: 'velocity_window_hours', min: 1, max: 6 },
              { label: 'Zone Cooldown (min)', key: 'zone_cooldown_minutes', min: 30, max: 480 },
              { label: 'Emergency Candles', key: 'emergency_candles', min: 2, max: 8 },
            ] as any[]).map(({ label, key, step, min, max }) => (
              <div key={key}>
                <label className="label">{label}</label>
                <input type="number" className="input" step={step || 1} min={min} max={max}
                  value={config[key as keyof BtConfig] as number}
                  onChange={e => set(key as keyof BtConfig, parseFloat(e.target.value))} />
              </div>
            ))}
          </div>

          {/* Filter toggles */}
          <div>
            <div className="text-xs text-gray-500 mb-2 uppercase tracking-wide">Strategy Filters</div>
            <div className="flex flex-wrap gap-2">
              <FilterToggle label="Time Filter" field="use_time_filter" />
              <FilterToggle label="Velocity Filter" field="use_velocity_filter" />
              <FilterToggle label="Funding Rate" field="use_funding_filter" />
              <FilterToggle label="Macro Calendar" field="use_macro_filter" />
              <FilterToggle label="Zone System" field="use_zone_system" />
              <FilterToggle label="Second Break Rule" field="use_second_break_rule" />
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Summary Cards ─────────────────────────────────────────────────────────────

function SummaryGrid({ s }: { s: any }) {
  const returnColor = s.total_return_pct >= 0 ? 'text-profit' : 'text-loss'

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {/* Key returns */}
      <div className="card col-span-2 md:col-span-2 bg-gradient-to-br from-dark-700 to-dark-800">
        <div className="text-xs text-gray-500 uppercase tracking-wide">Total Return</div>
        <div className={clsx('text-4xl font-bold font-mono mt-1', returnColor)}>
          {formatPct(s.total_return_pct)}
        </div>
        <div className="flex gap-4 mt-2 text-xs text-gray-500">
          <span>${s.initial_capital?.toLocaleString()} → <strong className="text-white">${s.final_capital?.toLocaleString()}</strong></span>
          <span>Peak: <strong className="text-brand">${s.peak_capital?.toLocaleString()}</strong></span>
        </div>
        <div className="text-xs text-gray-600 mt-1">{s.start_date} → {s.end_date}</div>
      </div>

      <div className="card">
        <div className="text-xs text-gray-500">Win Rate</div>
        <div className={clsx('text-3xl font-bold font-mono mt-1', s.win_rate >= 55 ? 'text-profit' : 'text-warning')}>
          {s.win_rate}%
        </div>
        <div className="text-xs text-gray-500 mt-1">{s.winning_trades}W / {s.losing_trades}L / {s.liquidations} liq</div>
      </div>

      <div className="card">
        <div className="text-xs text-gray-500">Profit Factor</div>
        <div className={clsx('text-3xl font-bold font-mono mt-1', s.profit_factor >= 1.5 ? 'text-profit' : 'text-warning')}>
          {s.profit_factor}x
        </div>
        <div className="text-xs text-gray-500 mt-1">Sharpe: {s.sharpe_ratio}</div>
      </div>

      {/* Row 2 */}
      {[
        { label: 'Total Trades', value: s.total_trades, color: 'text-brand-light', sub: `${s.long_trades}L / ${s.short_trades}S` },
        { label: 'Max Drawdown', value: `-${s.max_drawdown_pct}%`, color: 'text-loss', sub: `-$${s.max_drawdown_usd?.toFixed(0)}` },
        { label: 'Avg Win', value: formatPct(s.avg_win_pct), color: 'text-profit', sub: `avg $${s.avg_win_usd}` },
        { label: 'Avg Loss', value: formatPct(s.avg_loss_pct), color: 'text-loss', sub: `avg $${Math.abs(s.avg_loss_usd ?? 0).toFixed(2)}` },
        { label: 'Best Trade', value: formatPct(s.best_trade_pct), color: 'text-profit', sub: '' },
        { label: 'Worst Trade', value: formatPct(s.worst_trade_pct), color: 'text-loss', sub: '' },
        { label: 'Avg Hold Time', value: `${s.avg_holding_hours}h`, color: 'text-gray-300', sub: '' },
        { label: 'Long Win Rate', value: `${s.long_win_rate}%`, color: s.long_win_rate >= 55 ? 'text-profit' : 'text-warning', sub: `Short: ${s.short_win_rate}%` },
      ].map(({ label, value, color, sub }) => (
        <div key={label} className="card">
          <div className="text-xs text-gray-500">{label}</div>
          <div className={clsx('text-xl font-bold font-mono mt-1', color)}>{value}</div>
          {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
        </div>
      ))}
    </div>
  )
}

// ─── Charts ────────────────────────────────────────────────────────────────────

function EquityChart({ data }: { data: any[] }) {
  const formatted = data.map(d => ({
    ...d,
    date: new Date(d.time).toLocaleDateString('en-US', { month: 'short', year: '2-digit' }),
  }))

  return (
    <div className="card">
      <h3 className="text-sm font-semibold text-white mb-4">Equity Curve</h3>
      <ResponsiveContainer width="100%" height={220}>
        <ComposedChart data={formatted}>
          <defs>
            <linearGradient id="eqGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#6366f1" stopOpacity={0.3} />
              <stop offset="100%" stopColor="#6366f1" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#20203c" />
          <XAxis dataKey="date" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} interval="preserveStartEnd" />
          <YAxis yAxisId="eq" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} tickFormatter={v => `$${v.toLocaleString()}`} />
          <YAxis yAxisId="dd" orientation="right" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} tickFormatter={v => `${v}%`} />
          <Tooltip {...tooltipStyle}
            formatter={(v: number, name: string) =>
              name === 'equity' ? [`$${v.toLocaleString()}`, 'Equity'] : [`${v.toFixed(1)}%`, 'Drawdown']
            }
          />
          <Area yAxisId="eq" type="monotone" dataKey="equity" stroke="#6366f1" strokeWidth={2} fill="url(#eqGrad)" />
          <Line yAxisId="dd" type="monotone" dataKey="drawdown" stroke="#ef4444" strokeWidth={1} dot={false} opacity={0.6} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  )
}

function MonthlyChart({ data }: { data: any[] }) {
  return (
    <div className="card">
      <h3 className="text-sm font-semibold text-white mb-4">Monthly P&L</h3>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#20203c" />
          <XAxis dataKey="month" tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} />
          <YAxis tick={{ fill: '#6b7280', fontSize: 9 }} tickLine={false} tickFormatter={v => `$${v}`} />
          <Tooltip {...tooltipStyle} formatter={(v: number) => [`$${v.toFixed(2)}`, 'PnL']} />
          <ReferenceLine y={0} stroke="#374151" />
          <Bar dataKey="pnl" radius={[2, 2, 0, 0]}>
            {data.map((entry, i) => (
              <Cell key={i} fill={entry.pnl >= 0 ? '#10b981' : '#ef4444'} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

// ─── Block Stats ───────────────────────────────────────────────────────────────

function BlockStats({ stats }: { stats: Record<string, number> }) {
  const total = Object.values(stats).reduce((a, b) => a + b, 0)
  const sorted = Object.entries(stats).sort(([, a], [, b]) => b - a)

  return (
    <div className="card">
      <h3 className="text-sm font-semibold text-white mb-4">
        Signal Blocks ({total.toLocaleString()} total filtered signals)
      </h3>
      <div className="space-y-2">
        {sorted.map(([reason, count]) => {
          const pct = Math.round(count / total * 100)
          return (
            <div key={reason}>
              <div className="flex justify-between text-xs mb-1">
                <span className="text-gray-400 font-mono">{reason.replace(/_/g, ' ')}</span>
                <span className="text-gray-300">{count.toLocaleString()} ({pct}%)</span>
              </div>
              <div className="h-1.5 bg-dark-600 rounded-full overflow-hidden">
                <div className="h-full bg-brand/60 rounded-full" style={{ width: `${pct}%` }} />
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─── Trades Table ──────────────────────────────────────────────────────────────

function TradesTable({ trades }: { trades: any[] }) {
  const [page, setPage] = useState(0)
  const [filter, setFilter] = useState('ALL')
  const PAGE_SIZE = 50

  const filtered = filter === 'ALL' ? trades
    : filter === 'WINS' ? trades.filter(t => t.realized_pnl_pct > 0)
    : filter === 'LOSSES' ? trades.filter(t => t.realized_pnl_pct <= 0 && t.status !== 'LIQUIDATED')
    : trades.filter(t => t.status === 'LIQUIDATED')

  const paged = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  const totalPages = Math.ceil(filtered.length / PAGE_SIZE)

  return (
    <div className="card p-0 overflow-hidden">
      <div className="flex items-center justify-between p-4 border-b border-dark-700">
        <h3 className="text-sm font-semibold text-white">{filtered.length} Trades</h3>
        <div className="flex gap-1 bg-dark-700 border border-dark-600 rounded-lg p-1">
          {['ALL', 'WINS', 'LOSSES', 'LIQUIDATED'].map(f => (
            <button
              key={f}
              onClick={() => { setFilter(f); setPage(0) }}
              className={clsx('px-2.5 py-1 rounded text-xs font-medium transition-all',
                filter === f ? 'bg-brand text-white' : 'text-gray-400 hover:text-white'
              )}
            >
              {f}
            </button>
          ))}
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-dark-700">
              {['#', 'Dir', 'Entry Date', 'Entry', 'Exit', 'Zone', 'P&L%', 'P&L $', 'Peak%', 'Hold', 'Exit Reason', 'Status'].map(h => (
                <th key={h} className="table-header text-left">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paged.map((t: any) => {
              const win = t.realized_pnl_pct > 0
              return (
                <tr key={t.id} className={clsx('table-row text-xs',
                  t.status === 'LIQUIDATED' && 'bg-warning/5'
                )}>
                  <td className="table-cell text-gray-500 font-mono">{t.id}</td>
                  <td className="table-cell">
                    <span className={clsx('font-medium flex items-center gap-0.5',
                      t.direction === 'LONG' ? 'text-profit' : 'text-loss'
                    )}>
                      {t.direction === 'LONG' ? <ArrowUpRight size={12} /> : <ArrowDownRight size={12} />}
                      {t.direction}
                    </span>
                  </td>
                  <td className="table-cell text-gray-400 font-mono whitespace-nowrap">
                    {new Date(t.entry_time).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' })}
                  </td>
                  <td className="table-cell font-mono">${t.entry_price?.toLocaleString()}</td>
                  <td className="table-cell font-mono">${t.exit_price?.toLocaleString()}</td>
                  <td className="table-cell font-mono text-gray-400">{t.zone}</td>
                  <td className={clsx('table-cell font-mono font-medium', win ? 'text-profit' : 'text-loss')}>
                    {t.realized_pnl_pct > 0 ? '+' : ''}{t.realized_pnl_pct?.toFixed(1)}%
                  </td>
                  <td className={clsx('table-cell font-mono', win ? 'text-profit' : 'text-loss')}>
                    {t.realized_pnl_usd > 0 ? '+' : ''}${t.realized_pnl_usd?.toFixed(2)}
                  </td>
                  <td className="table-cell font-mono text-brand-light">
                    {t.peak_profit_pct?.toFixed(1)}%
                  </td>
                  <td className="table-cell text-gray-500">{t.holding_hours}h</td>
                  <td className="table-cell text-gray-500 max-w-48 truncate" title={t.exit_reason}>
                    {t.exit_reason}
                  </td>
                  <td className="table-cell">
                    <span className={clsx('badge',
                      t.status === 'LIQUIDATED' ? 'badge-yellow' :
                      win ? 'badge-green' : 'badge-red'
                    )}>
                      {t.status === 'LIQUIDATED' ? 'LIQ' : win ? 'WIN' : 'LOSS'}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      {totalPages > 1 && (
        <div className="flex items-center justify-between p-3 border-t border-dark-700">
          <span className="text-xs text-gray-500">
            Page {page + 1} of {totalPages} · {filtered.length} trades
          </span>
          <div className="flex gap-2">
            <button className="btn-ghost text-xs py-1" disabled={page === 0} onClick={() => setPage(p => p - 1)}>Prev</button>
            <button className="btn-ghost text-xs py-1" disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)}>Next</button>
          </div>
        </div>
      )}
    </div>
  )
}

// ─── Main Page ────────────────────────────────────────────────────────────────

export default function Backtest() {
  const qc = useQueryClient()
  const [config, setConfig] = useState<BtConfig>(DEFAULT_CONFIG)
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const { data: statusData, refetch: refetchStatus } = useQuery({
    queryKey: ['bt-status'],
    queryFn: () => backtestApi.getStatus().then(r => r.data),
    refetchInterval: false,
  })

  const { data: resultsData } = useQuery({
    queryKey: ['bt-results'],
    queryFn: () => backtestApi.getResults().then(r => r.data),
    enabled: statusData?.has_results && !statusData?.running,
    retry: false,
  })

  const isRunning = statusData?.running || false
  const progress = statusData?.progress || 0
  const progressMsg = statusData?.message || ''

  // Poll while running
  useEffect(() => {
    if (isRunning) {
      pollRef.current = setInterval(() => refetchStatus(), 1500)
    } else {
      if (pollRef.current) clearInterval(pollRef.current)
      if (statusData?.has_results) {
        qc.invalidateQueries({ queryKey: ['bt-results'] })
      }
    }
    return () => { if (pollRef.current) clearInterval(pollRef.current) }
  }, [isRunning])

  const runMut = useMutation({
    mutationFn: () => backtestApi.run(config),
    onSuccess: () => {
      toast.success('Backtest started')
      refetchStatus()
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to start backtest'),
  })

  const cancelMut = useMutation({
    mutationFn: () => backtestApi.cancel(),
    onSuccess: () => { toast('Backtest cancelled'); refetchStatus() },
  })

  const cacheMut = useMutation({
    mutationFn: () => backtestApi.clearCache(),
    onSuccess: () => toast.success('Cache cleared'),
  })

  const s = resultsData?.summary
  const cfg = resultsData?.config

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Backtesting</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            Test the Legion strategy against 5 years of real BTC/USDT data
          </p>
        </div>
        <div className="flex gap-2">
          <button
            className="btn-ghost text-xs"
            onClick={() => cacheMut.mutate()}
            disabled={cacheMut.isPending || isRunning}
            title="Clear cached data (forces re-download)"
          >
            <Trash2 size={14} />
            Clear Cache
          </button>
        </div>
      </div>

      {/* Config */}
      <ConfigPanel config={config} onChange={setConfig} />

      {/* Run controls */}
      <div className="flex items-center gap-3">
        {!isRunning ? (
          <button
            className="btn-primary py-2.5 px-6"
            onClick={() => runMut.mutate()}
            disabled={runMut.isPending}
          >
            <Play size={16} />
            Run Backtest ({config.start_year}–{config.end_year})
          </button>
        ) : (
          <button
            className="btn-danger py-2.5 px-6"
            onClick={() => cancelMut.mutate()}
          >
            <Square size={16} />
            Cancel
          </button>
        )}
        {statusData?.has_results && !isRunning && (
          <button className="btn-ghost" onClick={() => qc.invalidateQueries({ queryKey: ['bt-results'] })}>
            <RefreshCw size={14} />
            Reload Results
          </button>
        )}
      </div>

      {/* Progress bar */}
      {isRunning && (
        <div className="card">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm text-white">Running backtest...</span>
            <span className="text-sm font-mono text-brand">{progress.toFixed(0)}%</span>
          </div>
          <div className="h-2 bg-dark-600 rounded-full overflow-hidden">
            <div
              className="h-full bg-brand rounded-full transition-all duration-500"
              style={{ width: `${progress}%` }}
            />
          </div>
          <div className="text-xs text-gray-500 mt-2">{progressMsg}</div>
        </div>
      )}

      {/* Info box for first run */}
      {!isRunning && !statusData?.has_results && (
        <div className="card border-brand/20 bg-brand/5">
          <div className="flex items-start gap-3">
            <BarChart2 size={20} className="text-brand shrink-0 mt-0.5" />
            <div className="text-sm text-gray-400">
              <strong className="text-white">First run downloads ~43,000 candles</strong> from Binance's public API
              (no API key needed). After the first download, data is cached locally so subsequent runs start instantly.
              A full 5-year backtest takes ~30–120 seconds depending on your machine.
            </div>
          </div>
        </div>
      )}

      {/* Results */}
      {resultsData && s && (
        <div className="space-y-6">
          {/* Summary */}
          <SummaryGrid s={s} />

          {/* Active filters */}
          {cfg && (
            <div className="flex flex-wrap gap-2">
              {[
                { key: 'use_time_filter', label: 'Time Filter' },
                { key: 'use_velocity_filter', label: 'Velocity Filter' },
                { key: 'use_funding_filter', label: 'Funding Filter' },
                { key: 'use_macro_filter', label: 'Macro Filter' },
                { key: 'use_second_break_rule', label: 'Second Break Rule' },
              ].map(({ key, label }) => (
                <span key={key} className={clsx('badge',
                  cfg[key] ? 'badge-green' : 'badge-gray'
                )}>
                  {cfg[key] ? '✓' : '✗'} {label}
                </span>
              ))}
              <span className="badge badge-purple">{cfg.leverage}x leverage</span>
              <span className="badge badge-purple">{(cfg.position_size_pct * 100).toFixed(0)}% position</span>
            </div>
          )}

          {/* Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <EquityChart data={resultsData.equity_curve || []} />
            <MonthlyChart data={resultsData.monthly_pnl || []} />
          </div>

          {/* Block stats */}
          {resultsData.block_stats && Object.keys(resultsData.block_stats).length > 0 && (
            <BlockStats stats={resultsData.block_stats} />
          )}

          {/* Trades */}
          {resultsData.trades && <TradesTable trades={resultsData.trades} />}
        </div>
      )}
    </div>
  )
}
