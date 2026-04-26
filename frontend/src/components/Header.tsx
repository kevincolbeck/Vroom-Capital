import { useQuery } from '@tanstack/react-query'
import { botApi } from '../lib/api'
import { formatPrice, formatPct } from '../lib/utils'
import { Wifi, WifiOff } from 'lucide-react'
import { clsx } from 'clsx'

interface HeaderProps {
  wsData: any
}

export default function Header({ wsData }: HeaderProps) {
  const { data: statusData } = useQuery({
    queryKey: ['bot-status'],
    queryFn: () => botApi.getStatus().then(r => r.data),
    refetchInterval: 5000,
  })

  const botStatus = statusData?.bot?.status || 'STOPPED'
  const btcPrice = wsData?.market?.price || statusData?.market?.btc_price || 0
  const fundingRate = wsData?.market?.funding_rate || statusData?.market?.funding_rate || 0
  const isConnected = !!wsData

  const statusConfigs: Record<string, { color: string; dot: string; label: string }> = {
    RUNNING: { color: 'text-profit', dot: 'bg-profit', label: 'RUNNING' },
    STOPPED: { color: 'text-gray-400', dot: 'bg-gray-500', label: 'STOPPED' },
    PAUSED: { color: 'text-warning', dot: 'bg-warning', label: 'PAUSED' },
    ERROR: { color: 'text-loss', dot: 'bg-loss', label: 'ERROR' },
  }
  const statusConfig = statusConfigs[botStatus] || { color: 'text-gray-400', dot: 'bg-gray-500', label: botStatus }

  return (
    <header className="flex items-center justify-between px-6 py-3 bg-dark-800 border-b border-dark-600 shrink-0">
      {/* BTC Price */}
      <div className="flex items-center gap-6">
        <div>
          <div className="text-xs text-gray-500">BTC/USDT</div>
          <div className="text-lg font-bold font-mono text-white">
            {btcPrice > 0 ? `$${btcPrice.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}` : '—'}
          </div>
        </div>
        <div className="hidden sm:block">
          <div className="text-xs text-gray-500">Funding Rate</div>
          <div className={clsx(
            'text-sm font-mono font-medium',
            fundingRate > 0.0001 ? 'text-loss' : fundingRate < -0.0001 ? 'text-profit' : 'text-gray-300'
          )}>
            {fundingRate !== 0 ? `${(fundingRate * 100).toFixed(4)}%` : '—'}
          </div>
        </div>
      </div>

      {/* Status indicators */}
      <div className="flex items-center gap-4">
        {/* WebSocket status */}
        <div className="flex items-center gap-1.5 text-xs">
          {isConnected
            ? <Wifi size={14} className="text-profit" />
            : <WifiOff size={14} className="text-loss" />
          }
          <span className={isConnected ? 'text-profit' : 'text-loss'}>
            {isConnected ? 'Live' : 'Offline'}
          </span>
        </div>

        {/* Bot status */}
        <div className="flex items-center gap-2 bg-dark-700 border border-dark-500 rounded-lg px-3 py-1.5">
          <span className={clsx('status-dot', statusConfig.dot,
            botStatus === 'RUNNING' && 'animate-pulse'
          )} />
          <span className={clsx('text-xs font-medium', statusConfig.color)}>
            {statusConfig.label}
          </span>
        </div>

        {/* Account balance */}
        {statusData?.account && (
          <div className="hidden md:flex items-center gap-2 bg-dark-700 border border-dark-500 rounded-lg px-3 py-1.5">
            <span className="text-xs text-gray-500">Balance</span>
            <span className="text-xs font-mono font-medium text-white">
              ${statusData.account.available?.toFixed(2) || '0.00'}
            </span>
          </div>
        )}
      </div>
    </header>
  )
}
