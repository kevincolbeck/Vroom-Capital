import { useState, useEffect, useRef } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { logsApi } from '../lib/api'
import { formatDate } from '../lib/utils'
import { FileText, Trash2, RefreshCw, AlertTriangle, Info, XCircle, Filter } from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'

const LEVELS = ['ALL', 'INFO', 'WARNING', 'ERROR']
const CATEGORIES = ['ALL', 'BOT', 'POSITION', 'ORDER', 'COPY_TRADE', 'RISK', 'DATA', 'OVERRIDE']

const levelConfig = {
  INFO: { icon: Info, color: 'text-brand-light', bg: 'bg-brand/10 border-brand/20' },
  WARNING: { icon: AlertTriangle, color: 'text-warning', bg: 'bg-warning/10 border-warning/20' },
  ERROR: { icon: XCircle, color: 'text-loss', bg: 'bg-loss/10 border-loss/20' },
}

export default function Logs() {
  const qc = useQueryClient()
  const [level, setLevel] = useState('ALL')
  const [category, setCategory] = useState('ALL')
  const [autoRefresh, setAutoRefresh] = useState(true)
  const bottomRef = useRef<HTMLTableRowElement>(null)

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['logs', level, category],
    queryFn: () => logsApi.get({
      level: level === 'ALL' ? undefined : level,
      category: category === 'ALL' ? undefined : category,
      limit: 200,
    }).then(r => r.data),
    refetchInterval: autoRefresh ? 5000 : false,
  })

  const clearMut = useMutation({
    mutationFn: () => logsApi.clear(),
    onSuccess: () => {
      toast.success('Logs cleared')
      qc.invalidateQueries({ queryKey: ['logs'] })
    },
  })

  const logs = data?.logs || []

  return (
    <div className="space-y-4 h-full flex flex-col">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Bot Logs</h1>
          <p className="text-sm text-gray-500 mt-0.5">{logs.length} entries</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={clsx('btn text-xs py-1.5 px-3 border',
              autoRefresh
                ? 'bg-brand/10 border-brand/20 text-brand-light'
                : 'bg-dark-700 border-dark-500 text-gray-400'
            )}
          >
            <RefreshCw size={12} className={autoRefresh ? 'animate-spin' : ''} />
            Live
          </button>
          <button
            className="btn-ghost text-xs"
            onClick={() => refetch()}
          >
            <RefreshCw size={14} />
            Refresh
          </button>
          <button
            className="btn-danger text-xs py-1.5"
            onClick={() => clearMut.mutate()}
            disabled={clearMut.isPending}
          >
            <Trash2 size={14} />
            Clear
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3">
        <div>
          <div className="text-xs text-gray-500 mb-1">Level</div>
          <div className="flex gap-1 bg-dark-800 border border-dark-600 rounded-lg p-1">
            {LEVELS.map(l => (
              <button
                key={l}
                onClick={() => setLevel(l)}
                className={clsx('px-2.5 py-1 rounded text-xs font-medium transition-all',
                  level === l
                    ? l === 'ERROR' ? 'bg-loss text-white'
                      : l === 'WARNING' ? 'bg-warning text-black'
                      : 'bg-brand text-white'
                    : 'text-gray-400 hover:text-white'
                )}
              >
                {l}
              </button>
            ))}
          </div>
        </div>
        <div>
          <div className="text-xs text-gray-500 mb-1">Category</div>
          <div className="flex flex-wrap gap-1 bg-dark-800 border border-dark-600 rounded-lg p-1">
            {CATEGORIES.map(c => (
              <button
                key={c}
                onClick={() => setCategory(c)}
                className={clsx('px-2.5 py-1 rounded text-xs font-medium transition-all',
                  category === c ? 'bg-brand text-white' : 'text-gray-400 hover:text-white'
                )}
              >
                {c}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Logs */}
      <div className="flex-1 card p-0 overflow-hidden">
        <div className="h-[600px] overflow-y-auto font-mono text-xs">
          {isLoading ? (
            <div className="flex items-center justify-center h-full text-gray-500">Loading...</div>
          ) : logs.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-gray-500">
              <FileText size={32} className="mb-2 text-gray-700" />
              No logs found
            </div>
          ) : (
            <table className="w-full">
              <thead className="sticky top-0 bg-dark-800 border-b border-dark-700">
                <tr>
                  <th className="table-header text-left w-36">Time</th>
                  <th className="table-header text-left w-20">Level</th>
                  <th className="table-header text-left w-24">Category</th>
                  <th className="table-header text-left">Message</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log: any) => {
                  const cfg = levelConfig[log.level as keyof typeof levelConfig]
                  return (
                    <tr key={log.id} className={clsx(
                      'border-b border-dark-700/50 hover:bg-dark-700/30 transition-colors',
                      log.level === 'ERROR' && 'bg-loss/5',
                      log.level === 'WARNING' && 'bg-warning/5',
                    )}>
                      <td className="table-cell text-gray-600 text-xs whitespace-nowrap">
                        {formatDate(log.created_at)}
                      </td>
                      <td className="table-cell">
                        <span className={clsx('badge text-xs', cfg?.bg, cfg?.color)}>
                          {log.level}
                        </span>
                      </td>
                      <td className="table-cell text-gray-500 text-xs">{log.category}</td>
                      <td className="table-cell text-gray-300 break-all">
                        {log.message}
                        {log.details && (
                          <div className="text-gray-600 text-xs mt-0.5 truncate max-w-xl"
                            title={log.details}>
                            {log.details.substring(0, 120)}
                          </div>
                        )}
                      </td>
                    </tr>
                  )
                })}
                <tr ref={bottomRef} />
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  )
}
