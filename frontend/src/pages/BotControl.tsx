import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { botApi, signalApi } from '../lib/api'
import { formatPrice, formatDate } from '../lib/utils'
import {
  Play, Square, Pause, RotateCcw, AlertTriangle, Zap,
  ArrowUpRight, ArrowDownRight, RefreshCw, Shield, Activity
} from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'

export default function BotControl() {
  const qc = useQueryClient()
  const [forceDirection, setForceDirection] = useState<'LONG' | 'SHORT' | null>(null)
  const [forceReason, setForceReason] = useState('')
  const [emergencyReason, setEmergencyReason] = useState('')
  const [showEmergency, setShowEmergency] = useState(false)
  const [showForce, setShowForce] = useState(false)

  const { data: statusData, isLoading } = useQuery({
    queryKey: ['bot-status'],
    queryFn: () => botApi.getStatus().then(r => r.data),
    refetchInterval: 3000,
  })

  const { data: analysisData, refetch: refetchAnalysis, isFetching: isAnalyzing } = useQuery({
    queryKey: ['signal-analysis'],
    queryFn: () => signalApi.getAnalysis().then(r => r.data),
    enabled: false,
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['bot-status'] })

  const startMut = useMutation({
    mutationFn: () => botApi.start(),
    onSuccess: () => { toast.success('Bot started'); invalidate() },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to start'),
  })

  const stopMut = useMutation({
    mutationFn: () => botApi.stop(),
    onSuccess: () => { toast.success('Bot stopped'); invalidate() },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to stop'),
  })

  const pauseMut = useMutation({
    mutationFn: () => botApi.pause(),
    onSuccess: () => { toast.success('Bot paused'); invalidate() },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to pause'),
  })

  const resumeMut = useMutation({
    mutationFn: () => botApi.resume(),
    onSuccess: () => { toast.success('Bot resumed'); invalidate() },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Failed to resume'),
  })

  const emergencyMut = useMutation({
    mutationFn: (reason: string) => botApi.emergencyClose(reason),
    onSuccess: () => {
      toast.success('Emergency close executed — all positions closed')
      setShowEmergency(false)
      invalidate()
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Emergency close failed'),
  })

  const forceMut = useMutation({
    mutationFn: ({ direction, reason }: { direction: string; reason: string }) =>
      botApi.forceTrade(direction, reason),
    onSuccess: (_, { direction }) => {
      toast.success(`Force ${direction} trade opened`)
      setShowForce(false)
      invalidate()
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Force trade failed'),
  })

  const bot = statusData?.bot || {}
  const isRunning = bot.status === 'RUNNING'
  const isPaused = bot.status === 'PAUSED'
  const isStopped = bot.status === 'STOPPED'
  const isError = bot.status === 'ERROR'

  const statusConfigs: Record<string, { color: string; bg: string; dot: string }> = {
    RUNNING: { color: 'text-profit', bg: 'bg-profit/10 border-profit/20', dot: 'bg-profit animate-pulse' },
    STOPPED: { color: 'text-gray-400', bg: 'bg-dark-700 border-dark-500', dot: 'bg-gray-500' },
    PAUSED: { color: 'text-warning', bg: 'bg-warning/10 border-warning/20', dot: 'bg-warning animate-pulse' },
    ERROR: { color: 'text-loss', bg: 'bg-loss/10 border-loss/20', dot: 'bg-loss animate-pulse' },
  }
  const statusConfig = statusConfigs[bot.status || 'STOPPED'] || { color: 'text-gray-400', bg: 'bg-dark-700 border-dark-500', dot: 'bg-gray-500' }

  return (
    <div className="space-y-6 max-w-4xl mx-auto">
      <div>
        <h1 className="text-2xl font-bold text-white">Bot Control</h1>
        <p className="text-sm text-gray-500 mt-0.5">Start, stop, and override the autonomous trading engine</p>
      </div>

      {/* Status Card */}
      <div className={clsx('card border-2', statusConfig.bg)}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className={clsx('w-3 h-3 rounded-full', statusConfig.dot)} />
            <div>
              <div className={clsx('text-xl font-bold', statusConfig.color)}>{bot.status || 'STOPPED'}</div>
              <div className="text-xs text-gray-500 mt-0.5">
                {isRunning && bot.uptime_start ? `Running since ${formatDate(bot.uptime_start)}` : ''}
                {isPaused ? 'Signal generation paused — monitoring continues' : ''}
                {isStopped ? 'Bot is offline' : ''}
                {isError ? `Error: ${bot.error_message || 'Unknown error'}` : ''}
              </div>
            </div>
          </div>
          {statusData?.open_positions > 0 && (
            <div className="flex items-center gap-2 bg-brand/10 border border-brand/20 rounded-lg px-3 py-2">
              <Activity size={14} className="text-brand animate-pulse" />
              <span className="text-sm text-brand-light font-medium">
                {statusData.open_positions} open position{statusData.open_positions > 1 ? 's' : ''}
              </span>
            </div>
          )}
        </div>

        {isError && (
          <div className="mt-3 p-3 bg-loss/10 border border-loss/20 rounded-lg text-xs text-loss">
            {bot.error_message}
          </div>
        )}
      </div>

      {/* Primary Controls */}
      <div className="card">
        <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
          <Zap size={16} className="text-brand" />
          Bot Controls
        </h3>
        <div className="flex flex-wrap gap-3">
          <button
            className="btn-success"
            disabled={isRunning || startMut.isPending}
            onClick={() => startMut.mutate()}
          >
            <Play size={16} />
            Start Bot
          </button>
          <button
            className="btn-danger"
            disabled={isStopped || stopMut.isPending}
            onClick={() => stopMut.mutate()}
          >
            <Square size={16} />
            Stop Bot
          </button>
          {isRunning && (
            <button
              className="btn-warning"
              disabled={pauseMut.isPending}
              onClick={() => pauseMut.mutate()}
            >
              <Pause size={16} />
              Pause Signals
            </button>
          )}
          {isPaused && (
            <button
              className="btn-primary"
              disabled={resumeMut.isPending}
              onClick={() => resumeMut.mutate()}
            >
              <Play size={16} />
              Resume
            </button>
          )}
          <button
            className="btn-ghost"
            onClick={() => refetchAnalysis()}
            disabled={isAnalyzing}
          >
            <RefreshCw size={16} className={isAnalyzing ? 'animate-spin' : ''} />
            Run Analysis
          </button>
        </div>

        <p className="text-xs text-gray-600 mt-3">
          Pause stops new signal generation but continues monitoring open positions for exits.
          Stop completely halts all bot activity.
        </p>
      </div>

      {/* Fresh Analysis */}
      {analysisData && (
        <div className="card">
          <h3 className="text-sm font-semibold text-white mb-4">Live Analysis Result</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
            <div className="bg-dark-700 rounded-lg p-3">
              <div className="text-gray-500 mb-1">Signal</div>
              <div className={clsx('font-bold text-sm',
                analysisData.signal?.direction === 'LONG' ? 'text-profit' :
                analysisData.signal?.direction === 'SHORT' ? 'text-loss' : 'text-gray-400'
              )}>
                {analysisData.signal?.direction || 'NEUTRAL'}
              </div>
              <div className="text-gray-600">{analysisData.signal?.strength}</div>
            </div>
            <div className="bg-dark-700 rounded-lg p-3">
              <div className="text-gray-500 mb-1">BTC Price</div>
              <div className="font-mono font-bold text-white">{formatPrice(analysisData.btc_price)}</div>
            </div>
            <div className="bg-dark-700 rounded-lg p-3">
              <div className="text-gray-500 mb-1">HA 6H / 1H</div>
              <div className="font-mono">
                <span className={analysisData.signal?.ha_6h_color === 'GREEN' ? 'text-profit' : 'text-loss'}>
                  {analysisData.signal?.ha_6h_color?.charAt(0)}
                </span>
                {' / '}
                <span className={analysisData.signal?.ha_1h_color === 'GREEN' ? 'text-profit' : 'text-loss'}>
                  {analysisData.signal?.ha_1h_color?.charAt(0)}
                </span>
              </div>
            </div>
            <div className="bg-dark-700 rounded-lg p-3">
              <div className="text-gray-500 mb-1">Confidence</div>
              <div className="font-mono text-brand">{analysisData.signal?.confidence_score?.toFixed(0)}%</div>
            </div>
          </div>
          {analysisData.signal?.block_reasons?.length > 0 && (
            <div className="mt-3 space-y-1">
              {analysisData.signal.block_reasons.map((r: string, i: number) => (
                <div key={i} className="text-xs text-warning flex items-start gap-1.5">
                  <AlertTriangle size={12} className="shrink-0 mt-0.5" />
                  {r}
                </div>
              ))}
            </div>
          )}
          {analysisData.signal?.entry_reason && (
            <div className="mt-3 p-2 bg-dark-700 rounded text-xs text-gray-400">
              {analysisData.signal.entry_reason}
            </div>
          )}
        </div>
      )}

      {/* Force Trade Override */}
      <div className="card">
        <h3 className="text-sm font-semibold text-white mb-1 flex items-center gap-2">
          <Shield size={16} className="text-warning" />
          Force Trade Override
        </h3>
        <p className="text-xs text-gray-500 mb-4">
          Manually open a position bypassing all signal filters. Use with caution.
        </p>

        {!showForce ? (
          <button className="btn-warning" onClick={() => setShowForce(true)}>
            <Zap size={14} />
            Force Open Trade
          </button>
        ) : (
          <div className="space-y-3">
            <div className="flex gap-2">
              <button
                onClick={() => setForceDirection('LONG')}
                className={clsx('flex-1 btn gap-2 py-2.5 border',
                  forceDirection === 'LONG'
                    ? 'bg-profit text-white border-profit'
                    : 'bg-dark-700 text-gray-300 border-dark-500 hover:border-profit hover:text-profit'
                )}
              >
                <ArrowUpRight size={16} />
                LONG
              </button>
              <button
                onClick={() => setForceDirection('SHORT')}
                className={clsx('flex-1 btn gap-2 py-2.5 border',
                  forceDirection === 'SHORT'
                    ? 'bg-loss text-white border-loss'
                    : 'bg-dark-700 text-gray-300 border-dark-500 hover:border-loss hover:text-loss'
                )}
              >
                <ArrowDownRight size={16} />
                SHORT
              </button>
            </div>
            <div>
              <label className="label">Override Reason</label>
              <input
                className="input"
                placeholder="Why are you overriding?"
                value={forceReason}
                onChange={(e) => setForceReason(e.target.value)}
              />
            </div>
            <div className="flex gap-2">
              <button
                className="btn-warning flex-1"
                disabled={!forceDirection || forceMut.isPending}
                onClick={() => forceMut.mutate({ direction: forceDirection!, reason: forceReason || 'Manual override' })}
              >
                Confirm Force {forceDirection || '...'}
              </button>
              <button className="btn-ghost" onClick={() => setShowForce(false)}>Cancel</button>
            </div>
          </div>
        )}
      </div>

      {/* Emergency Controls */}
      <div className="card border-2 border-loss/20">
        <h3 className="text-sm font-semibold text-loss mb-1 flex items-center gap-2">
          <AlertTriangle size={16} />
          Emergency Controls
        </h3>
        <p className="text-xs text-gray-500 mb-4">
          Immediately close ALL open positions (master + all copy traders). Cannot be undone.
        </p>

        {!showEmergency ? (
          <button
            className="btn-danger"
            onClick={() => setShowEmergency(true)}
          >
            <AlertTriangle size={14} />
            Emergency Close All
          </button>
        ) : (
          <div className="space-y-3">
            <div className="p-3 bg-loss/10 border border-loss/30 rounded-lg text-xs text-loss">
              ⚠️ This will immediately close ALL open positions across ALL copy traders.
              This action cannot be undone.
            </div>
            <div>
              <label className="label">Reason (optional)</label>
              <input
                className="input border-loss/30"
                placeholder="Emergency reason..."
                value={emergencyReason}
                onChange={(e) => setEmergencyReason(e.target.value)}
              />
            </div>
            <div className="flex gap-2">
              <button
                className="btn-danger flex-1"
                disabled={emergencyMut.isPending}
                onClick={() => emergencyMut.mutate(emergencyReason || 'Emergency stop by admin')}
              >
                {emergencyMut.isPending ? 'Closing...' : '⚠️ CONFIRM EMERGENCY CLOSE ALL'}
              </button>
              <button className="btn-ghost" onClick={() => setShowEmergency(false)}>Cancel</button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
