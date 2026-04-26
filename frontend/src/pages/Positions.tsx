import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { positionApi } from '../lib/api'
import { formatPrice, formatPct, formatUsd, formatDate } from '../lib/utils'
import { clsx } from 'clsx'
import { X, ArrowUpRight, ArrowDownRight, TrendingUp, Filter } from 'lucide-react'
import toast from 'react-hot-toast'

type StatusFilter = 'ALL' | 'OPEN' | 'CLOSED' | 'LIQUIDATED'

export default function Positions() {
  const [filter, setFilter] = useState<StatusFilter>('ALL')
  const [closingId, setClosingId] = useState<number | null>(null)
  const qc = useQueryClient()

  const { data, isLoading } = useQuery({
    queryKey: ['positions', filter],
    queryFn: () => positionApi.getAll({
      status: filter === 'ALL' ? undefined : filter,
      limit: 100
    }).then(r => r.data),
    refetchInterval: 5000,
  })

  const closeMut = useMutation({
    mutationFn: (id: number) => positionApi.close(id),
    onSuccess: (_, id) => {
      toast.success(`Position #${id} closed`)
      setClosingId(null)
      qc.invalidateQueries({ queryKey: ['positions'] })
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to close'),
  })

  const positions = data?.positions || []

  const filters: StatusFilter[] = ['ALL', 'OPEN', 'CLOSED', 'LIQUIDATED']

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Positions</h1>
          <p className="text-sm text-gray-500 mt-0.5">{positions.length} positions</p>
        </div>

        {/* Filter */}
        <div className="flex gap-1 bg-dark-800 border border-dark-600 rounded-lg p-1">
          {filters.map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={clsx('px-3 py-1.5 rounded-md text-xs font-medium transition-all',
                filter === f
                  ? 'bg-brand text-white'
                  : 'text-gray-400 hover:text-white'
              )}
            >
              {f}
            </button>
          ))}
        </div>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          {
            label: 'Open', value: positions.filter((p: any) => p.status === 'OPEN').length,
            color: 'text-brand-light'
          },
          {
            label: 'Closed (Win)', value: positions.filter((p: any) => p.status === 'CLOSED' && (p.realized_pnl_pct || 0) > 0).length,
            color: 'text-profit'
          },
          {
            label: 'Closed (Loss)', value: positions.filter((p: any) => p.status === 'CLOSED' && (p.realized_pnl_pct || 0) <= 0).length,
            color: 'text-loss'
          },
          {
            label: 'Liquidated', value: positions.filter((p: any) => p.status === 'LIQUIDATED').length,
            color: 'text-warning'
          },
        ].map(({ label, value, color }) => (
          <div key={label} className="card text-center">
            <div className="text-xs text-gray-500">{label}</div>
            <div className={clsx('text-2xl font-bold mt-1', color)}>{value}</div>
          </div>
        ))}
      </div>

      {/* Positions Table */}
      <div className="card p-0 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-dark-700">
                {['#', 'Side', 'Status', 'Entry', 'Current', 'Liq Price', 'P&L', 'Peak', 'Zone', 'Opened', 'Action'].map(h => (
                  <th key={h} className="table-header text-left">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                <tr><td colSpan={11} className="text-center py-8 text-gray-500">Loading...</td></tr>
              ) : positions.length === 0 ? (
                <tr>
                  <td colSpan={11} className="text-center py-12 text-gray-500">
                    <TrendingUp size={32} className="mx-auto mb-2 text-gray-700" />
                    No positions found
                  </td>
                </tr>
              ) : positions.map((p: any) => {
                const isLong = p.side === 'LONG'
                const isOpen = p.status === 'OPEN'
                const pnlPct = isOpen ? p.unrealized_pnl_pct : p.realized_pnl_pct
                const pnlPositive = (pnlPct || 0) > 0

                return (
                  <tr key={p.id} className="table-row">
                    <td className="table-cell text-gray-500 font-mono">#{p.id}</td>
                    <td className="table-cell">
                      <span className={clsx('flex items-center gap-1 font-medium',
                        isLong ? 'text-profit' : 'text-loss'
                      )}>
                        {isLong ? <ArrowUpRight size={14} /> : <ArrowDownRight size={14} />}
                        {p.side} {p.leverage}x
                      </span>
                    </td>
                    <td className="table-cell">
                      <span className={clsx('badge',
                        p.status === 'OPEN' ? 'badge-green' :
                        p.status === 'CLOSED' ? (pnlPositive ? 'badge-purple' : 'badge-red') :
                        p.status === 'LIQUIDATED' ? 'badge-yellow' : 'badge-gray'
                      )}>
                        {p.status}
                      </span>
                    </td>
                    <td className="table-cell font-mono">{formatPrice(p.entry_price)}</td>
                    <td className="table-cell font-mono">
                      {isOpen ? formatPrice(p.current_price) : formatPrice(p.exit_price)}
                    </td>
                    <td className="table-cell font-mono text-loss text-xs">{formatPrice(p.liquidation_price)}</td>
                    <td className="table-cell">
                      <span className={clsx('font-mono font-medium',
                        pnlPositive ? 'text-profit' : 'text-loss'
                      )}>
                        {formatPct(pnlPct)}
                      </span>
                      {!isOpen && p.realized_pnl_usd != null && (
                        <div className="text-xs text-gray-500">{formatUsd(p.realized_pnl_usd)}</div>
                      )}
                    </td>
                    <td className="table-cell font-mono text-brand-light">
                      {formatPct(p.peak_profit_pct)}
                    </td>
                    <td className="table-cell text-gray-400 text-xs font-mono">{p.zone || '—'}</td>
                    <td className="table-cell text-gray-400 text-xs">{formatDate(p.opened_at)}</td>
                    <td className="table-cell">
                      {isOpen && (
                        <button
                          className="btn-ghost text-xs py-1 px-2"
                          onClick={() => setClosingId(p.id)}
                          disabled={closeMut.isPending}
                        >
                          <X size={12} />
                          Close
                        </button>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Close confirmation modal */}
      {closingId && (
        <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
          <div className="card w-full max-w-sm border-2 border-dark-500">
            <h3 className="text-lg font-semibold text-white mb-2">Close Position #{closingId}?</h3>
            <p className="text-sm text-gray-400 mb-4">
              This will also close all copy trade positions linked to this trade.
            </p>
            <div className="flex gap-2">
              <button
                className="btn-danger flex-1"
                disabled={closeMut.isPending}
                onClick={() => closeMut.mutate(closingId)}
              >
                {closeMut.isPending ? 'Closing...' : 'Yes, Close'}
              </button>
              <button className="btn-ghost flex-1" onClick={() => setClosingId(null)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
