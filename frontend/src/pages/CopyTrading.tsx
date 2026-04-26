import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { copyTradingApi } from '../lib/api'
import { formatUsd, formatDate, timeAgo } from '../lib/utils'
import {
  Users, Plus, Trash2, Edit2, ToggleLeft, ToggleRight,
  TrendingUp, TrendingDown, X, Check, AlertTriangle
} from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'

interface Trader {
  id: number
  nickname: string
  is_active: boolean
  position_size_pct: number | null
  leverage_override: number | null
  max_position_usd: number | null
  copy_longs: boolean
  copy_shorts: boolean
  total_pnl_usd: number
  total_trades: number
  win_trades: number
  win_rate: number
  notes: string | null
  joined_at: string
  last_active: string | null
}

function AddTraderModal({ onClose }: { onClose: () => void }) {
  const qc = useQueryClient()
  const [form, setForm] = useState({
    nickname: '',
    api_key: '',
    api_secret: '',
    position_size_pct: '',
    leverage_override: '',
    max_position_usd: '',
    copy_longs: true,
    copy_shorts: true,
    notes: '',
  })

  const addMut = useMutation({
    mutationFn: () => copyTradingApi.addTrader({
      ...form,
      position_size_pct: form.position_size_pct ? parseFloat(form.position_size_pct) : null,
      leverage_override: form.leverage_override ? parseInt(form.leverage_override) : null,
      max_position_usd: form.max_position_usd ? parseFloat(form.max_position_usd) : null,
    }),
    onSuccess: () => {
      toast.success('Copy trader added')
      qc.invalidateQueries({ queryKey: ['copy-traders'] })
      onClose()
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to add trader'),
  })

  return (
    <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4 overflow-y-auto">
      <div className="card w-full max-w-lg border-2 border-dark-500 my-4">
        <div className="flex items-center justify-between mb-5">
          <h3 className="text-lg font-semibold text-white">Add Copy Trader</h3>
          <button onClick={onClose} className="text-gray-500 hover:text-white"><X size={18} /></button>
        </div>

        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-3">
            <div className="col-span-2">
              <label className="label">Nickname *</label>
              <input className="input" placeholder="e.g. John Doe" value={form.nickname}
                onChange={e => setForm(f => ({ ...f, nickname: e.target.value }))} />
            </div>
            <div className="col-span-2">
              <label className="label">Bitunix API Key *</label>
              <input className="input font-mono text-xs" placeholder="API key"
                value={form.api_key} onChange={e => setForm(f => ({ ...f, api_key: e.target.value }))} />
            </div>
            <div className="col-span-2">
              <label className="label">Bitunix API Secret *</label>
              <input className="input font-mono text-xs" type="password" placeholder="API secret"
                value={form.api_secret} onChange={e => setForm(f => ({ ...f, api_secret: e.target.value }))} />
            </div>
          </div>

          <div className="border-t border-dark-600 pt-4 space-y-3">
            <div className="text-xs font-medium text-gray-400 uppercase tracking-wide">
              Trade Settings (leave blank to use bot defaults)
            </div>
            <div className="grid grid-cols-3 gap-3">
              <div>
                <label className="label">Position Size %</label>
                <input className="input" type="number" placeholder="0.30" step="0.01"
                  value={form.position_size_pct}
                  onChange={e => setForm(f => ({ ...f, position_size_pct: e.target.value }))} />
              </div>
              <div>
                <label className="label">Leverage Override</label>
                <input className="input" type="number" placeholder="86"
                  value={form.leverage_override}
                  onChange={e => setForm(f => ({ ...f, leverage_override: e.target.value }))} />
              </div>
              <div>
                <label className="label">Max Position USD</label>
                <input className="input" type="number" placeholder="500"
                  value={form.max_position_usd}
                  onChange={e => setForm(f => ({ ...f, max_position_usd: e.target.value }))} />
              </div>
            </div>
            <div className="flex gap-4">
              <label className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={form.copy_longs}
                  onChange={e => setForm(f => ({ ...f, copy_longs: e.target.checked }))}
                  className="w-4 h-4 rounded accent-brand" />
                <span className="text-sm text-gray-300">Copy LONGs</span>
              </label>
              <label className="flex items-center gap-2 cursor-pointer">
                <input type="checkbox" checked={form.copy_shorts}
                  onChange={e => setForm(f => ({ ...f, copy_shorts: e.target.checked }))}
                  className="w-4 h-4 rounded accent-brand" />
                <span className="text-sm text-gray-300">Copy SHORTs</span>
              </label>
            </div>
            <div>
              <label className="label">Notes</label>
              <input className="input" placeholder="Optional notes"
                value={form.notes} onChange={e => setForm(f => ({ ...f, notes: e.target.value }))} />
            </div>
          </div>
        </div>

        <div className="flex gap-2 mt-5">
          <button
            className="btn-primary flex-1"
            disabled={!form.nickname || !form.api_key || !form.api_secret || addMut.isPending}
            onClick={() => addMut.mutate()}
          >
            {addMut.isPending ? 'Adding...' : 'Add Trader'}
          </button>
          <button className="btn-ghost" onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}

function TraderCard({ trader }: { trader: Trader }) {
  const qc = useQueryClient()
  const [showDelete, setShowDelete] = useState(false)

  const toggleMut = useMutation({
    mutationFn: () => copyTradingApi.updateTrader(trader.id, { is_active: !trader.is_active }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['copy-traders'] })
      toast.success(trader.is_active ? 'Trader deactivated' : 'Trader activated')
    },
  })

  const deleteMut = useMutation({
    mutationFn: () => copyTradingApi.deleteTrader(trader.id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['copy-traders'] })
      toast.success('Trader removed')
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to delete'),
  })

  const winRate = trader.win_rate || 0

  return (
    <div className={clsx('card border-2 transition-all',
      trader.is_active ? 'border-dark-500' : 'border-dark-700 opacity-60'
    )}>
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="flex items-center gap-2">
            <span className="font-semibold text-white">{trader.nickname}</span>
            <span className={clsx('badge', trader.is_active ? 'badge-green' : 'badge-gray')}>
              {trader.is_active ? 'Active' : 'Inactive'}
            </span>
          </div>
          <div className="text-xs text-gray-500 mt-0.5">
            Joined {timeAgo(trader.joined_at)}
            {trader.last_active && ` · Active ${timeAgo(trader.last_active)}`}
          </div>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={() => toggleMut.mutate()}
            disabled={toggleMut.isPending}
            className="p-1.5 rounded hover:bg-dark-700 text-gray-500 hover:text-white transition-colors"
            title={trader.is_active ? 'Deactivate' : 'Activate'}
          >
            {trader.is_active
              ? <ToggleRight size={20} className="text-profit" />
              : <ToggleLeft size={20} />
            }
          </button>
          <button
            onClick={() => setShowDelete(true)}
            className="p-1.5 rounded hover:bg-dark-700 text-gray-500 hover:text-loss transition-colors"
          >
            <Trash2 size={16} />
          </button>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-2 mb-3">
        <div className="bg-dark-700 rounded p-2 text-center">
          <div className="text-xs text-gray-500">Trades</div>
          <div className="text-lg font-bold text-white">{trader.total_trades}</div>
        </div>
        <div className="bg-dark-700 rounded p-2 text-center">
          <div className="text-xs text-gray-500">Win Rate</div>
          <div className={clsx('text-lg font-bold', winRate >= 55 ? 'text-profit' : 'text-warning')}>
            {winRate}%
          </div>
        </div>
        <div className="bg-dark-700 rounded p-2 text-center">
          <div className="text-xs text-gray-500">Total PnL</div>
          <div className={clsx('text-lg font-bold font-mono',
            trader.total_pnl_usd >= 0 ? 'text-profit' : 'text-loss'
          )}>
            {formatUsd(trader.total_pnl_usd)}
          </div>
        </div>
      </div>

      {/* Settings */}
      <div className="space-y-1 text-xs text-gray-500">
        <div className="flex justify-between">
          <span>Position Size</span>
          <span className="text-gray-300">
            {trader.position_size_pct ? `${(trader.position_size_pct * 100).toFixed(0)}%` : 'Default (30%)'}
          </span>
        </div>
        <div className="flex justify-between">
          <span>Leverage</span>
          <span className="text-gray-300">{trader.leverage_override ? `${trader.leverage_override}x` : 'Default (86x)'}</span>
        </div>
        <div className="flex justify-between">
          <span>Max Position</span>
          <span className="text-gray-300">{trader.max_position_usd ? `$${trader.max_position_usd}` : 'No limit'}</span>
        </div>
        <div className="flex justify-between">
          <span>Copies</span>
          <span className="text-gray-300 flex gap-2">
            {trader.copy_longs && <span className="text-profit">L</span>}
            {trader.copy_shorts && <span className="text-loss">S</span>}
          </span>
        </div>
      </div>

      {trader.notes && (
        <div className="mt-2 text-xs text-gray-600 italic">{trader.notes}</div>
      )}

      {/* Delete confirmation */}
      {showDelete && (
        <div className="mt-3 p-3 bg-loss/10 border border-loss/30 rounded-lg">
          <div className="text-xs text-loss mb-2">Remove this trader? This cannot be undone.</div>
          <div className="flex gap-2">
            <button
              className="btn-danger text-xs py-1 px-2 flex-1"
              disabled={deleteMut.isPending}
              onClick={() => deleteMut.mutate()}
            >
              Remove
            </button>
            <button className="btn-ghost text-xs py-1 px-2" onClick={() => setShowDelete(false)}>
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

export default function CopyTrading() {
  const [showAdd, setShowAdd] = useState(false)

  const { data, isLoading } = useQuery({
    queryKey: ['copy-traders'],
    queryFn: () => copyTradingApi.getTraders().then(r => r.data),
    refetchInterval: 15000,
  })

  const traders: Trader[] = data?.traders || []
  const activeTraders = traders.filter(t => t.is_active)
  const totalPnl = traders.reduce((sum, t) => sum + (t.total_pnl_usd || 0), 0)
  const totalTrades = traders.reduce((sum, t) => sum + (t.total_trades || 0), 0)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Copy Trading</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {traders.length} traders · {activeTraders.length} active
          </p>
        </div>
        <button className="btn-primary" onClick={() => setShowAdd(true)}>
          <Plus size={16} />
          Add Trader
        </button>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Total Traders', value: traders.length, color: 'text-white' },
          { label: 'Active', value: activeTraders.length, color: 'text-profit' },
          { label: 'Total Trades', value: totalTrades, color: 'text-brand-light' },
          { label: 'Combined PnL', value: formatUsd(totalPnl), color: totalPnl >= 0 ? 'text-profit' : 'text-loss' },
        ].map(({ label, value, color }) => (
          <div key={label} className="card text-center">
            <div className="text-xs text-gray-500">{label}</div>
            <div className={clsx('text-xl font-bold font-mono mt-1', color)}>{value}</div>
          </div>
        ))}
      </div>

      {/* Info Banner */}
      <div className="flex items-start gap-3 p-4 bg-brand/5 border border-brand/20 rounded-xl">
        <Users size={18} className="text-brand shrink-0 mt-0.5" />
        <div className="text-sm text-gray-400">
          <strong className="text-white">Copy Trading</strong> automatically mirrors every trade to all active traders
          proportionally to their account balance. Each trader uses their own Bitunix API key and can have
          custom position sizing, leverage, and direction preferences.
        </div>
      </div>

      {/* Traders Grid */}
      {isLoading ? (
        <div className="text-center py-12 text-gray-500">Loading traders...</div>
      ) : traders.length === 0 ? (
        <div className="card text-center py-12">
          <Users size={40} className="mx-auto mb-3 text-gray-700" />
          <div className="text-gray-400 font-medium">No copy traders yet</div>
          <div className="text-sm text-gray-600 mt-1 mb-4">Add traders to start copy trading</div>
          <button className="btn-primary mx-auto" onClick={() => setShowAdd(true)}>
            <Plus size={16} /> Add First Trader
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {traders.map((t) => (
            <TraderCard key={t.id} trader={t} />
          ))}
        </div>
      )}

      {showAdd && <AddTraderModal onClose={() => setShowAdd(false)} />}
    </div>
  )
}
