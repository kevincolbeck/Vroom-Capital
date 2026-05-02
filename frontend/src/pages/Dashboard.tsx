import { useQuery } from '@tanstack/react-query'
import { botApi, positionApi, marketApi, hyblockApi } from '../lib/api'
import { formatPrice, formatPct, formatUsd, formatDate, timeAgo } from '../lib/utils'
import {
  TrendingUp, TrendingDown, Activity, DollarSign, Target, AlertTriangle,
  Zap, ArrowUpRight, ArrowDownRight, BarChart2, Shield
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
        {/* HA majority vote */}
        {signal.ha_6h_color && signal.ha_1h_color && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">HA Vote</span>
            <div className="flex items-center gap-1 font-mono">
              {[
                { label: '6H', color: signal.ha_6h_color },
                { label: '1H', color: signal.ha_1h_color },
                { label: '3M', color: signal.ha_3m_color },
              ].map(({ label, color }) => (
                <span key={label} className={clsx(
                  'px-1 rounded text-[10px]',
                  color === 'GREEN' ? 'text-profit bg-profit/10' : color === 'RED' ? 'text-loss bg-loss/10' : 'text-gray-500 bg-dark-600'
                )}>
                  {label}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* 6H trend */}
        {signal.ha_6h_trend && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">6H Trend</span>
            <span className={clsx('font-mono font-medium',
              signal.ha_6h_trend?.includes('BULLISH') ? 'text-profit' :
              signal.ha_6h_trend?.includes('BEARISH') ? 'text-loss' : 'text-gray-400'
            )}>
              {signal.ha_6h_trend?.replace(/_/g, ' ').toLowerCase()}
            </span>
          </div>
        )}

        {/* 6H ratio */}
        {signal.ha_6h_green_count != null && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">6H Ratio</span>
            <span className={clsx('font-mono font-medium',
              signal.ha_6h_green_count > signal.ha_6h_red_count ? 'text-profit' :
              signal.ha_6h_red_count   > signal.ha_6h_green_count ? 'text-loss' : 'text-gray-400'
            )}>
              {signal.ha_6h_green_count}/{(signal.ha_6h_green_count + signal.ha_6h_red_count)} GREEN
            </span>
          </div>
        )}

        {/* 1H consecutive streak */}
        {signal.ha_1h_consecutive != null && signal.ha_1h_color && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">1H Streak</span>
            <span className={clsx('font-mono font-medium',
              signal.ha_1h_color === 'GREEN' ? 'text-profit' : 'text-loss'
            )}>
              {signal.ha_1h_color} × {signal.ha_1h_consecutive}
            </span>
          </div>
        )}

        {/* Cascade Direction */}
        {signal.hyblock?.liq_levels?.cascade_direction && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">Cascade Dir</span>
            <span className={clsx('font-mono font-semibold',
              signal.hyblock.liq_levels.cascade_direction === 'LONG' ? 'text-profit' : 'text-loss'
            )}>
              {signal.hyblock.liq_levels.cascade_direction === 'LONG' ? '▲' : '▼'}{' '}
              {signal.hyblock.liq_levels.cascade_direction}
            </span>
          </div>
        )}

        {/* MII */}
        {signal.hyblock?.market_imbalance_index != null && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">MII 15m</span>
            <div className="flex items-center gap-1.5">
              <span className={clsx('font-mono font-medium',
                signal.hyblock.market_imbalance_index > 0.3 ? 'text-profit' :
                signal.hyblock.market_imbalance_index < -0.3 ? 'text-loss' : 'text-gray-400'
              )}>
                {signal.hyblock.market_imbalance_index > 0 ? '+' : ''}{signal.hyblock.market_imbalance_index?.toFixed(3)}
              </span>
              <span className="text-gray-600 font-mono text-[10px]">
                {signal.hyblock.mii_sustained_bars ?? 0}h sustained
              </span>
            </div>
          </div>
        )}

        {/* CVD */}
        {signal.hyblock?.cvd != null && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">CVD</span>
            <span className={clsx('font-mono font-medium',
              signal.hyblock.cvd > 0.5 ? 'text-profit' :
              signal.hyblock.cvd < -0.5 ? 'text-loss' : 'text-gray-400'
            )}>
              {signal.hyblock.cvd > 0 ? '+' : ''}{Number(signal.hyblock.cvd).toFixed(2)}
            </span>
          </div>
        )}

        {/* OI Delta */}
        {signal.hyblock?.oi_delta_pct != null && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">OI Δ</span>
            <span className={clsx('font-mono font-medium',
              signal.hyblock.oi_delta_pct > 2 ? 'text-profit' :
              signal.hyblock.oi_delta_pct < -2 ? 'text-loss' : 'text-gray-400'
            )}>
              {signal.hyblock.oi_delta_pct > 0 ? '+' : ''}{Number(signal.hyblock.oi_delta_pct).toFixed(2)}%
            </span>
          </div>
        )}

        {/* HA Scoring Breakdown */}
        {signal.ha_6h_body_pct != null && (
          <div className="border-t border-dark-700 pt-2 mt-1 space-y-1.5">
            <div className="text-gray-600 text-[10px] uppercase tracking-wide">HA Scoring</div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-gray-500">6H Body</span>
              <span className={clsx('font-mono font-medium',
                signal.ha_6h_body_pct >= 20 ? 'text-profit' :
                signal.ha_6h_body_pct >= 10 ? 'text-warning' : 'text-gray-500'
              )}>
                {Number(signal.ha_6h_body_pct).toFixed(1)}%
                <span className="text-gray-600 ml-1">
                  ({signal.ha_6h_body_pct >= 20 ? '+30' : signal.ha_6h_body_pct >= 10 ? '+15' : '+0'})
                </span>
              </span>
            </div>
            {signal.ha_prev_6h_color && (
              <div className="flex items-center justify-between text-xs">
                <span className="text-gray-500">6H Confirm</span>
                <span className={clsx('font-mono font-medium',
                  signal.ha_prev_6h_color === signal.ha_6h_color ? 'text-profit' : 'text-gray-500'
                )}>
                  {signal.ha_prev_6h_color === signal.ha_6h_color ? 'YES (+15)' : 'NO (+0)'}
                </span>
              </div>
            )}
            {signal.ha_1h_aligned_count != null && (
              <div className="flex items-center justify-between text-xs">
                <span className="text-gray-500">1H Momentum</span>
                <span className={clsx('font-mono font-medium',
                  signal.ha_1h_aligned_count >= 3 ? 'text-profit' :
                  signal.ha_1h_aligned_count >= 2 ? 'text-warning' : 'text-gray-500'
                )}>
                  {signal.ha_1h_aligned_count}/4
                  <span className="text-gray-600 ml-1">(+{(signal.ha_1h_aligned_count / 4 * 40).toFixed(0)})</span>
                </span>
              </div>
            )}
          </div>
        )}

        {/* Retail Positioning */}
        {signal.hyblock?.true_retail_long_pct != null && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">True Retail</span>
            <span className={clsx('font-mono font-medium',
              signal.hyblock.true_retail_long_pct > 60 ? 'text-loss' :
              signal.hyblock.true_retail_long_pct < 40 ? 'text-profit' : 'text-gray-400'
            )}>
              {Number(signal.hyblock.true_retail_long_pct).toFixed(1)}%L / {Number(signal.hyblock.true_retail_short_pct).toFixed(1)}%S
            </span>
          </div>
        )}

        {/* Prev Day Structure */}
        {signal.hyblock?.prev_day_structure && signal.hyblock.prev_day_structure !== 'UNKNOWN' && (
          <div className="flex items-center justify-between text-xs">
            <span className="text-gray-500">PD Structure</span>
            <span className={clsx('font-mono font-medium',
              signal.hyblock.prev_day_structure === 'ABOVE_PDH' ? 'text-profit' :
              signal.hyblock.prev_day_structure === 'BELOW_PDL' ? 'text-loss' : 'text-gray-400'
            )}>
              {signal.hyblock.prev_day_structure.replace('_', ' ')}
            </span>
          </div>
        )}

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
          <div className="mt-2 p-2 bg-dark-700 rounded-lg space-y-1">
            {signal.block_reasons.map((r: string, i: number) => (
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

  const { data: hyblockData } = useQuery({
    queryKey: ['hyblock-data'],
    queryFn: () => hyblockApi.getData().then(r => r.data),
    refetchInterval: 60000,
  })

  const bot = statusData?.bot || {}
  const account = statusData?.account || {}
  const market = statusData?.market || {}
  const signal = statusData?.last_signal
  const openPositions = positionsData?.positions || []
  const macroCtx = contextData?.macro || {}
  const fundingCtx = contextData?.funding || {}
  const spotCtx = contextData?.spot_flow || {}

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

          {/* Hyblock Capital Intelligence */}
          {hyblockData?.available && (
            <div className="card">
              <div className="card-header">
                <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                  <Zap size={16} className="text-brand" />
                  Hyblock Capital
                </h3>
                <span className={clsx('badge',
                  hyblockData.cascade_risk === 'CRITICAL' ? 'badge-red' :
                  hyblockData.cascade_risk === 'HIGH'     ? 'badge-yellow' :
                  hyblockData.cascade_risk === 'MEDIUM'   ? 'badge-yellow' : 'badge-green'
                )}>
                  {hyblockData.cascade_risk || 'LOW'} CASCADE
                </span>
              </div>
              <div className="space-y-2 text-xs">
                {/* OBI Slope */}
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">OBI Depth Slope</span>
                  <span className={clsx('font-mono font-medium',
                    hyblockData.obi_slope_direction === 'BULLISH' ? 'text-profit' :
                    hyblockData.obi_slope_direction === 'BEARISH' ? 'text-loss' : 'text-gray-400'
                  )}>
                    {hyblockData.obi_slope_direction === 'BULLISH' ? '▲' :
                     hyblockData.obi_slope_direction === 'BEARISH' ? '▼' : '—'}{' '}
                    {hyblockData.obi_slope_direction}
                  </span>
                </div>
                {/* Whale sentiment */}
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">Whale Flow</span>
                  <span className={clsx('font-mono font-medium',
                    hyblockData.whale_sentiment === 'BULLISH' ? 'text-profit' :
                    hyblockData.whale_sentiment === 'BEARISH' ? 'text-loss' : 'text-gray-400'
                  )}>
                    {hyblockData.whale_sentiment}
                  </span>
                </div>
                {/* Top traders (contrarian) */}
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">Top Traders</span>
                  <span className={clsx('font-mono font-medium',
                    hyblockData.top_trader_sentiment === 'BULLISH' ? 'text-profit' :
                    hyblockData.top_trader_sentiment === 'BEARISH' ? 'text-loss' : 'text-gray-400'
                  )}>
                    {hyblockData.top_trader_sentiment}
                    <span className="text-gray-600 font-normal"> (fade)</span>
                  </span>
                </div>
                {/* Volume delta */}
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">Volume Delta</span>
                  <span className={clsx('font-mono font-medium',
                    hyblockData.volume_delta_sentiment === 'BUY_DOMINANT'  ? 'text-profit' :
                    hyblockData.volume_delta_sentiment === 'SELL_DOMINANT' ? 'text-loss' : 'text-gray-400'
                  )}>
                    {hyblockData.volume_delta_sentiment?.replace('_', ' ')}
                  </span>
                </div>
                {/* OI trend */}
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">OI Trend</span>
                  <span className={clsx('font-mono',
                    hyblockData.oi_trend === 'RISING'  ? 'text-profit' :
                    hyblockData.oi_trend === 'FALLING' ? 'text-loss' : 'text-gray-400'
                  )}>
                    {hyblockData.oi_trend}
                  </span>
                </div>
                {/* Fragility */}
                <div className="flex justify-between items-center">
                  <span className="text-gray-500">Book Fragility</span>
                  <span className={clsx('font-mono',
                    hyblockData.fragility_level === 'HIGH'   ? 'text-loss' :
                    hyblockData.fragility_level === 'MEDIUM' ? 'text-warning' : 'text-profit'
                  )}>
                    {hyblockData.fragility_level}
                  </span>
                </div>
                {/* Volume Ratio */}
                {hyblockData.volume_ratio != null && (
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">Volume Ratio</span>
                    <span className={clsx('font-mono font-medium',
                      hyblockData.volume_ratio > 0.1  ? 'text-profit' :
                      hyblockData.volume_ratio < -0.1 ? 'text-loss' : 'text-gray-400'
                    )}>
                      {hyblockData.volume_ratio > 0 ? '+' : ''}{Number(hyblockData.volume_ratio).toFixed(3)}
                    </span>
                  </div>
                )}
                {/* Buy/Sell Count Ratio */}
                {hyblockData.buy_sell_count_ratio != null && (
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">B/S Count</span>
                    <span className={clsx('font-mono font-medium',
                      hyblockData.buy_sell_count_ratio > 0.1  ? 'text-profit' :
                      hyblockData.buy_sell_count_ratio < -0.1 ? 'text-loss' : 'text-gray-400'
                    )}>
                      {hyblockData.buy_sell_count_ratio > 0 ? '+' : ''}{Number(hyblockData.buy_sell_count_ratio).toFixed(3)}
                    </span>
                  </div>
                )}
                {/* CVD */}
                {hyblockData.cvd != null && (
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">CVD (20 bars)</span>
                    <span className={clsx('font-mono font-medium',
                      hyblockData.cvd > 0.5  ? 'text-profit' :
                      hyblockData.cvd < -0.5 ? 'text-loss' : 'text-gray-400'
                    )}>
                      {hyblockData.cvd > 0 ? '+' : ''}{Number(hyblockData.cvd).toFixed(2)}
                    </span>
                  </div>
                )}
                {/* OI Delta */}
                {hyblockData.oi_delta_pct != null && (
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">OI Delta</span>
                    <span className={clsx('font-mono font-medium',
                      hyblockData.oi_delta_pct > 2  ? 'text-profit' :
                      hyblockData.oi_delta_pct < -2 ? 'text-loss' : 'text-gray-400'
                    )}>
                      {hyblockData.oi_delta_pct > 0 ? '+' : ''}{Number(hyblockData.oi_delta_pct).toFixed(2)}%
                    </span>
                  </div>
                )}
                {/* Liq Level Cascade */}
                {hyblockData.liq_levels?.cascade_direction && (
                  <div className="border-t border-dark-700 pt-2 space-y-1.5">
                    <div className="flex justify-between items-center">
                      <span className="text-gray-500 font-medium">Liq Cascade</span>
                      <span className={clsx('font-mono font-semibold text-xs',
                        hyblockData.liq_levels.cascade_direction === 'LONG' ? 'text-profit' : 'text-loss'
                      )}>
                        {hyblockData.liq_levels.cascade_direction === 'LONG' ? '▲ LONG' : '▼ SHORT'}
                      </span>
                    </div>
                    {hyblockData.liq_levels.long_cluster_pct != null && (
                      <div className="flex justify-between text-[11px]">
                        <span className="text-gray-600">LONG cluster ↓</span>
                        <span className="font-mono text-profit/80">
                          -{hyblockData.liq_levels.long_cluster_pct?.toFixed(2)}%
                          {hyblockData.liq_levels.long_cluster_size > 0 && (
                            <span className="text-gray-500"> · {hyblockData.liq_levels.long_cluster_size?.toFixed(0)} BTC</span>
                          )}
                          {hyblockData.liq_levels.long_cluster_price && (
                            <span className="text-gray-600"> @ {formatPrice(hyblockData.liq_levels.long_cluster_price)}</span>
                          )}
                        </span>
                      </div>
                    )}
                    {hyblockData.liq_levels.short_cluster_pct != null && (
                      <div className="flex justify-between text-[11px]">
                        <span className="text-gray-600">SHORT cluster ↑</span>
                        <span className="font-mono text-loss/80">
                          +{hyblockData.liq_levels.short_cluster_pct?.toFixed(2)}%
                          {hyblockData.liq_levels.short_cluster_size > 0 && (
                            <span className="text-gray-500"> · {hyblockData.liq_levels.short_cluster_size?.toFixed(0)} BTC</span>
                          )}
                          {hyblockData.liq_levels.short_cluster_price && (
                            <span className="text-gray-600"> @ {formatPrice(hyblockData.liq_levels.short_cluster_price)}</span>
                          )}
                        </span>
                      </div>
                    )}
                  </div>
                )}
                {/* Liq clusters (heatmap) */}
                {(hyblockData.liq_clusters?.above_pct || hyblockData.liq_clusters?.below_pct || hyblockData.liq_clusters?.above_wide_pct || hyblockData.liq_clusters?.below_wide_pct) && (
                  <div className="border-t border-dark-700 pt-2 space-y-1">
                    <div className="text-gray-500 mb-1">Liq Heatmap</div>
                    {(hyblockData.liq_clusters.above_pct != null || hyblockData.liq_clusters.above_wide_pct != null) && (
                      <div className="flex justify-between text-[11px]">
                        <span className="text-gray-600">Above</span>
                        <span className="font-mono text-loss/80">
                          {(() => {
                            const useWide = !(hyblockData.liq_clusters.above_size > 0) && (hyblockData.liq_clusters.above_wide_size > 0);
                            const pct = useWide ? hyblockData.liq_clusters.above_wide_pct : hyblockData.liq_clusters.above_pct;
                            const size = useWide ? hyblockData.liq_clusters.above_wide_size : hyblockData.liq_clusters.above_size;
                            return <>
                              +{pct}%
                              {size > 0 && <span className="text-gray-500"> · {Number(size).toFixed(0)} BTC</span>}
                            </>;
                          })()}
                        </span>
                      </div>
                    )}
                    {(hyblockData.liq_clusters.below_pct != null || hyblockData.liq_clusters.below_wide_pct != null) && (
                      <div className="flex justify-between text-[11px]">
                        <span className="text-gray-600">Below</span>
                        <span className="font-mono text-profit/80">
                          {(() => {
                            const useWide = !(hyblockData.liq_clusters.below_size > 0) && (hyblockData.liq_clusters.below_wide_size > 0);
                            const pct = useWide ? hyblockData.liq_clusters.below_wide_pct : hyblockData.liq_clusters.below_pct;
                            const size = useWide ? hyblockData.liq_clusters.below_wide_size : hyblockData.liq_clusters.below_size;
                            return <>
                              -{pct}%
                              {size > 0 && <span className="text-gray-500"> · {Number(size).toFixed(0)} BTC</span>}
                            </>;
                          })()}
                        </span>
                      </div>
                    )}
                  </div>
                )}
                {/* Market Imbalance Index */}
                {hyblockData.market_imbalance_index != null && (
                  <div className="flex justify-between items-center">
                    <span className="text-gray-500">MII 15m</span>
                    <span className={clsx('font-mono font-medium',
                      hyblockData.market_imbalance_index > 0.1  ? 'text-profit' :
                      hyblockData.market_imbalance_index < -0.1 ? 'text-loss' : 'text-gray-400'
                    )}>
                      {hyblockData.market_imbalance_index > 0 ? '+' : ''}{Number(hyblockData.market_imbalance_index).toFixed(3)}
                    </span>
                  </div>
                )}
                {/* True Retail + Global Accounts */}
                {hyblockData.true_retail_long_pct != null && (
                  <div className="border-t border-dark-700 pt-2 space-y-1.5">
                    <div className="text-gray-500 mb-1">Positioning (Contrarian)</div>
                    <div className="flex justify-between text-[11px]">
                      <span className="text-gray-600">True Retail</span>
                      <span className={clsx('font-mono',
                        hyblockData.true_retail_long_pct > 60 ? 'text-loss' :
                        hyblockData.true_retail_long_pct < 40 ? 'text-profit' : 'text-gray-400'
                      )}>
                        {Number(hyblockData.true_retail_long_pct).toFixed(1)}%L / {Number(hyblockData.true_retail_short_pct).toFixed(1)}%S
                      </span>
                    </div>
                    {hyblockData.global_accounts_long_pct != null && (
                      <div className="flex justify-between text-[11px]">
                        <span className="text-gray-600">Global Accts</span>
                        <span className={clsx('font-mono',
                          hyblockData.global_accounts_long_pct > 60 ? 'text-loss' :
                          hyblockData.global_accounts_long_pct < 40 ? 'text-profit' : 'text-gray-400'
                        )}>
                          {Number(hyblockData.global_accounts_long_pct).toFixed(1)}%L / {Number(hyblockData.global_accounts_short_pct).toFixed(1)}%S
                        </span>
                      </div>
                    )}
                    {hyblockData.net_ls_delta != null && (
                      <div className="flex justify-between text-[11px]">
                        <span className="text-gray-600">Net L/S Δ</span>
                        <span className={clsx('font-mono',
                          hyblockData.net_ls_delta > 0.05 ? 'text-profit' :
                          hyblockData.net_ls_delta < -0.05 ? 'text-loss' : 'text-gray-400'
                        )}>
                          {hyblockData.net_ls_delta > 0 ? '+' : ''}{Number(hyblockData.net_ls_delta).toFixed(3)}
                        </span>
                      </div>
                    )}
                  </div>
                )}
                {/* Previous Day Levels */}
                {hyblockData.prev_day_structure && hyblockData.prev_day_structure !== 'UNKNOWN' && (
                  <div className="border-t border-dark-700 pt-2 space-y-1.5">
                    <div className="flex justify-between items-center">
                      <span className="text-gray-500">PD Structure</span>
                      <span className={clsx('font-mono font-semibold text-xs',
                        hyblockData.prev_day_structure === 'ABOVE_PDH' ? 'text-profit' :
                        hyblockData.prev_day_structure === 'BELOW_PDL' ? 'text-loss' : 'text-gray-400'
                      )}>
                        {hyblockData.prev_day_structure.replace(/_/g, ' ')}
                      </span>
                    </div>
                    <div className="flex justify-between text-[11px]">
                      <span className="text-gray-600">PDH / PDL</span>
                      <span className="font-mono text-gray-500">
                        {hyblockData.prev_day_high ? formatPrice(hyblockData.prev_day_high) : '—'} / {hyblockData.prev_day_low ? formatPrice(hyblockData.prev_day_low) : '—'}
                      </span>
                    </div>
                  </div>
                )}
                {/* Cumulative Liq Bias + Compression */}
                <div className="border-t border-dark-700 pt-2 space-y-1.5">
                  {hyblockData.cumulative_liq_bias && hyblockData.cumulative_liq_bias !== 'BALANCED' && (
                    <div className="flex justify-between items-center text-xs">
                      <span className="text-gray-500">Liq Zone Bias</span>
                      <span className={clsx('font-mono font-medium',
                        hyblockData.cumulative_liq_bias === 'SHORT_HEAVY' ? 'text-profit' :
                        hyblockData.cumulative_liq_bias === 'LONG_HEAVY' ? 'text-loss' : 'text-gray-400'
                      )}>
                        {hyblockData.cumulative_liq_bias?.replace('_', ' ')}
                      </span>
                    </div>
                  )}
                  {hyblockData.is_compressed != null && (
                    <div className="flex justify-between items-center text-xs">
                      <span className="text-gray-500">4H Compression</span>
                      <span className={clsx('font-mono',
                        hyblockData.is_compressed ? 'text-warning' : 'text-gray-400'
                      )}>
                        {hyblockData.is_compressed ? `⚡ COMPRESSED (${Number(hyblockData.compression_ratio).toFixed(2)}x)` : `${Number(hyblockData.compression_ratio ?? 1).toFixed(2)}x normal`}
                      </span>
                    </div>
                  )}
                </div>
                {/* Avg leverage */}
                {hyblockData.avg_leverage_raw > 0 && (
                  <div className="flex justify-between items-center border-t border-dark-700 pt-2">
                    <span className="text-gray-500">Avg Leverage</span>
                    <span className="font-mono text-gray-300">{Number(hyblockData.avg_leverage_raw).toFixed(1)}x</span>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Spot Order Flow */}
          <div className="card">
            <div className="card-header">
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <BarChart2 size={16} className="text-brand" />
                Spot Order Flow
              </h3>
              <span className={clsx('badge',
                spotCtx.pressure?.pressure === 'BUY'  ? 'badge-green' :
                spotCtx.pressure?.pressure === 'SELL' ? 'badge-red' : 'badge-gray'
              )}>
                {spotCtx.pressure?.pressure || 'N/A'}
              </span>
            </div>
            {!spotCtx.available ? (
              <div className="text-xs text-gray-600 py-2">Spot flow data unavailable</div>
            ) : (
              <div className="space-y-2 text-xs">
                <div className="flex justify-between">
                  <span className="text-gray-500">Bid/Ask Ratio</span>
                  <span className={clsx('font-mono',
                    (spotCtx.pressure?.ratio ?? 1) > 1.3 ? 'text-profit' :
                    (spotCtx.pressure?.ratio ?? 1) < 0.77 ? 'text-loss' : 'text-gray-300'
                  )}>
                    {spotCtx.pressure?.ratio?.toFixed(2) ?? '—'}x
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Divergence</span>
                  <span className={clsx('font-mono',
                    spotCtx.divergence?.includes('BULLISH') ? 'text-profit' :
                    spotCtx.divergence?.includes('BEARISH') ? 'text-loss' : 'text-gray-300'
                  )}>
                    {spotCtx.divergence?.toLowerCase().replace(/_/g, ' ') ?? '—'}
                  </span>
                </div>
                <div className="text-gray-600 pt-1">
                  {spotCtx.exchange_count ?? 0} exchanges · {spotCtx.exchanges?.join(', ') ?? '—'}
                </div>
              </div>
            )}
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
