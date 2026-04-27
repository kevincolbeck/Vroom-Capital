import { useQuery } from '@tanstack/react-query'
import { botApi, positionApi, marketApi } from '../lib/api'
import { formatPrice, formatPct, formatUsd, formatDate, timeAgo } from '../lib/utils'
import {
  TrendingUp, TrendingDown, Activity, DollarSign, Target, AlertTriangle,
  Clock, Zap, ArrowUpRight, ArrowDownRight, BarChart2, Shield
} from 'lucide-react'
import { clsx } from 'clsx'

function MetricCard({ label, value, sub, color = 'white', icon: Icon }: any) {
  return (
    <div className="card flex items-start gap-3">
      {Icon && (
        <div className="w-9 h-9 rounded-lg bg-dark-600 flex items-center justify-center shrink-0 mt-0.5">
          <Icon size={18} className="text-gray-400" />
        </div>
      )}
      <div className="min-w-0">
        <div className="text-xs text-gray-500 uppercase tracking-wide">{label}</div>
        <div className={clsx('text-xl font-bold font-mono mt-0.5', color)}>{value}</div>
        {sub && <div className="text-xs text-gray-500 mt-0.5">{sub}</div>}
      </div>
    </div>
  )
}

function SignalIndicator({ signal }: { signal: any }) {
  if (!signal) return null
  const dir = signal.direction
  const strength = signal.strength
  const blocked = signal.block_reasons?.length > 0

  return (
    <div className="card">
      <div className="card-header">
        <h3 className="text-sm font-semibold text-white flex items-center gap-2">
          <Target size={16} className="text-brand" />
          Current Signal
        </h3>
        <span className={clsx('badge',
          blocked ? 'badge-gray' : dir === 'LONG' ? 'badge-green' : 'badge-red'
        )}>
          {blocked ? 'BLOCKED' : dir || 'NEUTRAL'}
        </span>
      </div>

      <div className="space-y-2">
        {/* HA Trend */}
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-500">6H Trend</span>
          <span className={clsx('font-mono font-medium',
            signal.ha_6h_color === 'GREEN' ? 'text-profit' : 'text-loss'
          )}>
            {signal.ha_6h_color || '—'} ▲
          </span>
        </div>
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-500">1H Trend</span>
          <span className={clsx('font-mono font-medium',
            signal.ha_1h_color === 'GREEN' ? 'text-profit' : 'text-loss'
          )}>
            {signal.ha_1h_color || '—'} ▲
          </span>
        </div>

        {/* Zone */}
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-500">Zone</span>
          <span className="font-mono text-gray-300">{signal.zone_key || '—'} ({signal.zone_position || '—'})</span>
        </div>

        {/* Velocity */}
        {signal.velocity && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">2H Velocity</span>
            <span className={clsx('font-mono',
              (signal.velocity.pct_change || 0) > 0 ? 'text-profit' : 'text-loss'
            )}>
              {signal.velocity.pct_change != null ? formatPct(signal.velocity.pct_change) : '—'}
            </span>
          </div>
        )}

        {/* Confidence */}
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-500">Confidence</span>
          <div className="flex items-center gap-2">
            <div className="w-20 h-1.5 bg-dark-600 rounded-full overflow-hidden">
              <div
                className="h-full bg-brand rounded-full transition-all"
                style={{ width: `${signal.confidence_score || 0}%` }}
              />
            </div>
            <span className="font-mono text-gray-300">{signal.confidence_score?.toFixed(0) || 0}%</span>
          </div>
        </div>

        {/* Block reasons */}
        {signal.block_reasons?.length > 0 && (
          <div className="mt-2 p-2 bg-dark-700 rounded-lg">
            {signal.block_reasons.slice(0, 2).map((r: string, i: number) => (
              <div key={i} className="text-xs text-gray-500 flex items-start gap-1">
                <AlertTriangle size={10} className="text-warning shrink-0 mt-0.5" />
                <span>{r}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

function PositionCard({ position }: { position: any }) {
  const isLong = position.side === 'LONG'
  const pnlPct = position.unrealized_pnl_pct || 0
  const pnlUsd = position.unrealized_pnl_usd ?? (position.margin_used_usd * pnlPct / 100)
  const feesUsd = position.fees_usd ?? 0
  const netPnlUsd = position.net_pnl_usd ?? (pnlUsd - feesUsd)
  const pnlColor = pnlPct > 0 ? 'text-profit' : pnlPct < 0 ? 'text-loss' : 'text-gray-400'
  const netColor = netPnlUsd > 0 ? 'text-profit' : netPnlUsd < 0 ? 'text-loss' : 'text-gray-400'

  const leverage = position.leverage || 75
  const tp1Move = position.entry_price * 0.20 / leverage
  const tp2Move = position.entry_price * 0.30 / leverage
  const tp1 = isLong ? position.entry_price + tp1Move : position.entry_price - tp1Move
  const tp2 = isLong ? position.entry_price + tp2Move : position.entry_price - tp2Move

  const current = position.current_price || position.entry_price
  const tp1Progress = isLong
    ? Math.min(100, Math.max(0, ((current - position.entry_price) / (tp1 - position.entry_price)) * 100))
    : Math.min(100, Math.max(0, ((position.entry_price - current) / (position.entry_price - tp1)) * 100))

  return (
    <div className={clsx('p-4 rounded-xl border-2 transition-all',
      isLong ? 'border-profit/30 bg-profit/5' : 'border-loss/30 bg-loss/5'
    )}>
      {/* Header — direction + P&L */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          {isLong
            ? <ArrowUpRight size={18} className="text-profit" />
            : <ArrowDownRight size={18} className="text-loss" />
          }
          <span className={clsx('font-bold text-sm', isLong ? 'text-profit' : 'text-loss')}>
            {position.side} {position.leverage}x
          </span>
        </div>
        <div className="text-right">
          <div className={clsx('text-lg font-bold font-mono', pnlColor)}>
            {pnlPct >= 0 ? '+' : ''}{pnlPct.toFixed(1)}%
          </div>
          <div className={clsx('text-xs font-mono', pnlColor)}>
            {pnlUsd >= 0 ? '+' : ''}{formatUsd(pnlUsd)}
          </div>
        </div>
      </div>

      {/* Price grid */}
      <div className="grid grid-cols-2 gap-2 text-xs mb-3">
        <div>
          <div className="text-gray-500">Entry</div>
          <div className="font-mono text-white">{formatPrice(position.entry_price)}</div>
        </div>
        <div>
          <div className="text-gray-500">Current</div>
          <div className="font-mono text-white">{formatPrice(current)}</div>
        </div>
        <div>
          <div className="text-gray-500">Liquidation</div>
          <div className="font-mono text-loss">{formatPrice(position.liquidation_price)}</div>
        </div>
        <div>
          <div className="text-gray-500">Peak P&L</div>
          <div className="font-mono text-brand">+{(position.peak_profit_pct || 0).toFixed(1)}%</div>
        </div>
        <div>
          <div className="text-gray-500">Margin</div>
          <div className="font-mono text-gray-300">{formatUsd(position.margin_used_usd)}</div>
        </div>
        <div>
          <div className="text-gray-500">Notional</div>
          <div className="font-mono text-gray-300">{formatUsd(position.position_size_usd)}</div>
        </div>
      </div>

      {/* Fee-adjusted P&L */}
      <div className="bg-dark-700/60 rounded-lg px-3 py-2 mb-3 space-y-1">
        <div className="flex justify-between text-xs">
          <span className="text-gray-500">Gross P&L</span>
          <span className={clsx('font-mono', pnlColor)}>{pnlUsd >= 0 ? '+' : ''}{formatUsd(pnlUsd)}</span>
        </div>
        <div className="flex justify-between text-xs">
          <span className="text-gray-500">Fees (0.12% RT)</span>
          <span className="font-mono text-loss">-{formatUsd(feesUsd)}</span>
        </div>
        <div className="flex justify-between text-xs border-t border-white/10 pt-1">
          <span className="text-gray-400 font-medium">Net P&L</span>
          <span className={clsx('font-mono font-bold', netColor)}>{netPnlUsd >= 0 ? '+' : ''}{formatUsd(netPnlUsd)}</span>
        </div>
      </div>

      {/* TP Levels */}
      <div className="border-t border-white/10 pt-2.5 space-y-1.5">
        <div className="text-xs text-gray-500 font-medium uppercase tracking-wide mb-1.5">Exit Targets</div>
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-400 flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-brand inline-block" />
            Trail activates <span className="text-gray-600">(20% P&L)</span>
          </span>
          <span className="font-mono text-brand font-medium">{formatPrice(tp1)}</span>
        </div>
        <div className="w-full h-1 bg-dark-600 rounded-full overflow-hidden">
          <div
            className="h-full bg-brand rounded-full transition-all"
            style={{ width: `${tp1Progress}%` }}
          />
        </div>
        <div className="flex items-center justify-between text-xs">
          <span className="text-gray-400 flex items-center gap-1">
            <span className="w-2 h-2 rounded-full bg-profit inline-block" />
            Wide trail <span className="text-gray-600">(30% P&L)</span>
          </span>
          <span className="font-mono text-profit font-medium">{formatPrice(tp2)}</span>
        </div>
      </div>

      <div className="mt-2.5 text-xs text-gray-500">
        Opened {timeAgo(position.opened_at)} · Zone {position.zone}
      </div>
    </div>
  )
}

export default function Dashboard() {
  const { data: statusData, isLoading } = useQuery({
    queryKey: ['bot-status'],
    queryFn: () => botApi.getStatus().then(r => r.data),
    refetchInterval: 5000,
  })

  const { data: positionsData } = useQuery({
    queryKey: ['positions-open'],
    queryFn: () => positionApi.getAll({ status: 'OPEN', limit: 5 }).then(r => r.data),
    refetchInterval: 5000,
  })

  const { data: contextData } = useQuery({
    queryKey: ['market-context'],
    queryFn: () => marketApi.getContext().then(r => r.data),
    refetchInterval: 30000,
  })

  const bot = statusData?.bot || {}
  const account = statusData?.account || {}
  const market = statusData?.market || {}
  const signal = statusData?.last_signal
  const openPositions = positionsData?.positions || []
  const timeCtx = contextData?.time || {}
  const macroCtx = contextData?.macro || {}
  const fundingCtx = contextData?.funding || {}

  const winRate = bot.win_rate || 0
  const totalPnl = bot.total_pnl_usd || 0
  // Return % = net closed-trade PnL / current balance. Deposits change the balance
  // but not totalPnl, so they don't inflate this number.
  const accountBalance = account.balance || 0
  const returnPct = accountBalance > 0 ? (totalPnl / accountBalance) * 100 : 0

  return (
    <div className="space-y-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-white">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-0.5">Real-time overview · BTC/USDT Perpetual</p>
        </div>
        <div className="text-xs text-gray-600">
          Last update: {new Date().toLocaleTimeString()}
        </div>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricCard
          label="Account Balance"
          value={formatPrice(account.balance, 2)}
          sub={`Available: ${formatPrice(account.available, 2)}`}
          icon={DollarSign}
          color="text-white"
        />
        <MetricCard
          label="Net Return"
          value={`${returnPct >= 0 ? '+' : ''}${returnPct.toFixed(2)}%`}
          sub={`${totalPnl >= 0 ? '+' : ''}${formatUsd(totalPnl)} after fees · ${bot.total_trades || 0} trades`}
          icon={BarChart2}
          color={totalPnl >= 0 ? 'text-profit' : 'text-loss'}
        />
        <MetricCard
          label="Win Rate"
          value={`${winRate}%`}
          sub={`${bot.winning_trades || 0}W / ${(bot.total_trades || 0) - (bot.winning_trades || 0)}L`}
          icon={Target}
          color={winRate >= 55 ? 'text-profit' : 'text-warning'}
        />
        <MetricCard
          label="Open Positions"
          value={openPositions.length}
          sub={openPositions.length > 0 ? `${openPositions[0]?.side} active` : 'No active positions'}
          icon={Activity}
          color={openPositions.length > 0 ? 'text-brand-light' : 'text-gray-400'}
        />
      </div>

      {/* Main Content Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

        {/* Left: Open Position + Signal */}
        <div className="space-y-4">
          {openPositions.length > 0 ? (
            <div>
              <h3 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                <Activity size={16} className="text-brand animate-pulse" />
                Active Position
              </h3>
              {openPositions.map((p: any) => (
                <PositionCard key={p.id} position={p} />
              ))}
            </div>
          ) : (
            <div className="card text-center py-8">
              <Shield size={32} className="text-gray-600 mx-auto mb-2" />
              <div className="text-sm text-gray-500">No open positions</div>
              <div className="text-xs text-gray-600 mt-1">Bot is scanning for signals</div>
            </div>
          )}

          <SignalIndicator signal={signal} />
        </div>

        {/* Center: Market Context */}
        <div className="space-y-4">
          {/* Time Context */}
          <div className="card">
            <div className="card-header">
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <Clock size={16} className="text-brand" />
                Time Filter
              </h3>
              <span className={clsx('badge',
                timeCtx.risk_level === 'EXTREME' ? 'badge-red' :
                timeCtx.risk_level === 'HIGH' ? 'badge-yellow' :
                timeCtx.risk_level === 'LOW' ? 'badge-green' : 'badge-gray'
              )}>
                {timeCtx.label || 'NEUTRAL'}
              </span>
            </div>
            <div className="grid grid-cols-2 gap-3 text-xs">
              <div className="text-center p-2 rounded-lg bg-dark-700">
                <div className="text-gray-500 mb-1">LONG</div>
                <div className={clsx('font-medium', timeCtx.long_blocked ? 'text-loss' : 'text-profit')}>
                  {timeCtx.long_blocked ? '⛔ BLOCKED' : '✅ ALLOWED'}
                </div>
              </div>
              <div className="text-center p-2 rounded-lg bg-dark-700">
                <div className="text-gray-500 mb-1">SHORT</div>
                <div className={clsx('font-medium', timeCtx.short_blocked ? 'text-loss' : 'text-profit')}>
                  {timeCtx.short_blocked ? '⛔ BLOCKED' : '✅ ALLOWED'}
                </div>
              </div>
            </div>
          </div>

          {/* Macro Context */}
          <div className="card">
            <div className="card-header">
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <AlertTriangle size={16} className="text-brand" />
                Macro Calendar
              </h3>
              <span className={clsx('badge',
                macroCtx.fomc_risk_level === 'EXTREME' ? 'badge-red' :
                macroCtx.fomc_risk_level === 'HIGH' ? 'badge-yellow' :
                macroCtx.fomc_risk_level === 'MODERATE' ? 'badge-yellow' : 'badge-green'
              )}>
                {macroCtx.fomc_risk_level || 'NORMAL'}
              </span>
            </div>
            <div className="space-y-2 text-xs">
              <div className="flex justify-between">
                <span className="text-gray-500">FOMC</span>
                <span className="text-gray-300">
                  {macroCtx.days_to_fomc != null ? `${macroCtx.days_to_fomc} days` : '—'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Quad Witching</span>
                <span className={macroCtx.is_quad_witching ? 'text-warning' : 'text-gray-300'}>
                  {macroCtx.is_quad_witching ? '⚠️ Active' : 'No'}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500">Size Modifier</span>
                <span className={clsx('font-mono', macroCtx.position_size_modifier < 1 ? 'text-warning' : 'text-profit')}>
                  {macroCtx.position_size_modifier != null ? `${macroCtx.position_size_modifier}x` : '—'}
                </span>
              </div>
              <p className="text-gray-600 pt-1">{macroCtx.fomc_description}</p>
            </div>
          </div>

          {/* Funding Rate */}
          <div className="card">
            <div className="card-header">
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <Zap size={16} className="text-brand" />
                Funding Rate
              </h3>
              <span className={clsx('badge',
                fundingCtx.overall_sentiment === 'BEARISH_CONTRARIAN' ? 'badge-red' :
                fundingCtx.overall_sentiment === 'BULLISH_CONTRARIAN' ? 'badge-green' : 'badge-gray'
              )}>
                {fundingCtx.signal_strength || 'NEUTRAL'}
              </span>
            </div>
            <div className="space-y-2 text-xs">
              {Object.entries(fundingCtx.rates || {}).map(([exchange, rate]: any) => (
                <div key={exchange} className="flex justify-between">
                  <span className="text-gray-500 capitalize">{exchange}</span>
                  <span className={clsx('font-mono',
                    rate > 0.0001 ? 'text-loss' : rate < -0.0001 ? 'text-profit' : 'text-gray-300'
                  )}>
                    {rate != null ? `${(rate * 100).toFixed(4)}%` : '—'}
                  </span>
                </div>
              ))}
              <p className="text-gray-600 pt-1 text-xs">{fundingCtx.description}</p>
            </div>
          </div>
        </div>

        {/* Right: Recent Activity */}
        <div className="card">
          <div className="card-header">
            <h3 className="text-sm font-semibold text-white">Performance</h3>
          </div>
          <div className="space-y-4">
            {/* Win rate bar */}
            <div>
              <div className="flex justify-between text-xs mb-1.5">
                <span className="text-gray-500">Win Rate</span>
                <span className="text-white font-mono">{winRate}%</span>
              </div>
              <div className="h-2 bg-dark-600 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-profit to-brand rounded-full transition-all"
                  style={{ width: `${winRate}%` }}
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="bg-dark-700 rounded-lg p-3 text-center">
                <div className="text-xs text-gray-500">Total Trades</div>
                <div className="text-xl font-bold text-white mt-0.5">{bot.total_trades || 0}</div>
              </div>
              <div className="bg-dark-700 rounded-lg p-3 text-center">
                <div className="text-xs text-gray-500">Net Return</div>
                <div className={clsx('text-xl font-bold font-mono mt-0.5',
                  totalPnl >= 0 ? 'text-profit' : 'text-loss'
                )}>
                  {returnPct >= 0 ? '+' : ''}{returnPct.toFixed(2)}%
                </div>
                <div className={clsx('text-xs font-mono mt-0.5',
                  totalPnl >= 0 ? 'text-profit/70' : 'text-loss/70'
                )}>
                  {totalPnl >= 0 ? '+' : ''}{formatUsd(totalPnl)}
                </div>
              </div>
            </div>

            {/* Strategy params */}
            <div className="border-t border-dark-700 pt-3 space-y-2 text-xs">
              <div className="text-gray-500 font-medium uppercase tracking-wide mb-2">Strategy</div>
              {[
                ['Leverage', '75x Cross'],
                ['Position Size', '$3,250 liq buffer'],
                ['Liq Buffer', '$3,250'],
                ['TP1 / TP2', '20% / 30%'],
                ['Exit', 'Trailing stop + HA reversal'],
              ].map(([k, v]) => (
                <div key={k} className="flex justify-between">
                  <span className="text-gray-500">{k}</span>
                  <span className="text-gray-300">{v}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
