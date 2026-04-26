import { useState, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { settingsApi } from '../lib/api'
import { Settings as SettingsIcon, Shield, Key, Sliders, AlertTriangle, Check } from 'lucide-react'
import { clsx } from 'clsx'
import toast from 'react-hot-toast'

interface SettingsSection {
  title: string
  icon: any
  fields: SettingsField[]
}

interface SettingsField {
  key: string
  label: string
  description: string
  type: 'number' | 'boolean' | 'password' | 'text'
  step?: number
  min?: number
  max?: number
  unit?: string
}

const SECTIONS: SettingsSection[] = [
  {
    title: 'API Configuration',
    icon: Key,
    fields: [
      { key: 'bitunix_api_key', label: 'Bitunix API Key', description: 'Your Bitunix Futures API key', type: 'text' },
      { key: 'bitunix_api_secret', label: 'Bitunix API Secret', description: 'Your Bitunix Futures API secret', type: 'password' },
    ],
  },
  {
    title: 'Risk Management',
    icon: Shield,
    fields: [
      { key: 'leverage', label: 'Leverage', description: 'Cross margin leverage (Bitunix max for BTC: 75x)', type: 'number', min: 1, max: 75, unit: 'x' },
      { key: 'position_size_pct', label: 'Position Size', description: 'Percentage of account balance per trade', type: 'number', step: 0.01, min: 0.05, max: 0.60, unit: '% (0.30 = 30%)' },
      { key: 'liquidation_buffer_usd', label: 'Liquidation Buffer', description: 'Minimum distance from entry to liquidation in USD', type: 'number', min: 500, max: 10000, unit: 'USD' },
      { key: 'max_concurrent_positions', label: 'Max Concurrent Positions', description: 'Maximum number of open positions at once', type: 'number', min: 1, max: 5 },
    ],
  },
  {
    title: 'Take Profit Levels',
    icon: Sliders,
    fields: [
      { key: 'tp1_pct', label: 'TP1 Target', description: 'First profit target — trailing stop activates here', type: 'number', step: 0.01, min: 0.05, max: 1.0, unit: '% on margin (0.20 = 20%)' },
      { key: 'tp2_pct', label: 'TP2 Target', description: 'Second profit target', type: 'number', step: 0.01, min: 0.10, max: 2.0, unit: '% on margin (0.30 = 30%)' },
    ],
  },
  {
    title: 'Signal Filters',
    icon: Sliders,
    fields: [
      { key: 'velocity_threshold_pct', label: 'Velocity Threshold', description: 'Block trades if price moved more than this % in last N hours', type: 'number', step: 0.1, min: 0.5, max: 5.0, unit: '%' },
      { key: 'velocity_window_hours', label: 'Velocity Window', description: 'Hours to measure velocity over', type: 'number', min: 1, max: 6, unit: 'hours' },
      { key: 'zone_cooldown_minutes', label: 'Zone Cooldown', description: 'Minutes before signaling the same zone+direction again', type: 'number', min: 30, max: 480, unit: 'minutes' },
      { key: 'fomc_caution_days', label: 'FOMC Caution Days', description: 'Days before FOMC to apply risk reduction', type: 'number', min: 1, max: 14, unit: 'days' },
      { key: 'emergency_candles', label: 'Emergency Candles', description: 'Consecutive opposing 1H HA candles to trigger emergency exit', type: 'number', min: 2, max: 8 },
    ],
  },
  {
    title: 'Copy Trading',
    icon: Sliders,
    fields: [
      { key: 'copy_trading_enabled', label: 'Enable Copy Trading', description: 'Automatically copy all trades to registered traders', type: 'boolean' },
    ],
  },
  {
    title: 'Security',
    icon: Shield,
    fields: [
      { key: 'admin_password', label: 'Admin Password', description: 'Password to access this dashboard', type: 'password' },
    ],
  },
]

export default function Settings() {
  const qc = useQueryClient()
  const [values, setValues] = useState<Record<string, any>>({})
  const [changed, setChanged] = useState<Set<string>>(new Set())

  const { data: settingsData, isLoading } = useQuery({
    queryKey: ['settings'],
    queryFn: () => settingsApi.get().then(r => r.data),
  })

  useEffect(() => {
    if (settingsData) {
      setValues(settingsData)
    }
  }, [settingsData])

  const saveMut = useMutation({
    mutationFn: (updates: Record<string, any>) => settingsApi.update(updates),
    onSuccess: (_, updates) => {
      const keys = Object.keys(updates)
      toast.success(`Saved: ${keys.join(', ')}`)
      setChanged(prev => {
        const next = new Set(prev)
        keys.forEach(k => next.delete(k))
        return next
      })
      qc.invalidateQueries({ queryKey: ['settings'] })
    },
    onError: (e: any) => toast.error(e.response?.data?.detail || 'Save failed'),
  })

  const handleChange = (key: string, value: any) => {
    setValues(prev => ({ ...prev, [key]: value }))
    setChanged(prev => new Set(prev).add(key))
  }

  const handleSave = (fieldKeys: string[]) => {
    const updates: Record<string, any> = {}
    fieldKeys.forEach(k => {
      if (changed.has(k)) {
        updates[k] = values[k]
      }
    })
    if (Object.keys(updates).length === 0) {
      toast('No changes to save')
      return
    }
    saveMut.mutate(updates)
  }

  if (isLoading) {
    return <div className="flex items-center justify-center h-64 text-gray-500">Loading settings...</div>
  }

  return (
    <div className="space-y-6 max-w-2xl mx-auto">
      <div>
        <h1 className="text-2xl font-bold text-white">Settings</h1>
        <p className="text-sm text-gray-500 mt-0.5">Configure the bot, risk parameters, and API keys</p>
      </div>

      {/* API Key Status */}
      {!values.has_api_key && (
        <div className="flex items-start gap-3 p-4 bg-warning/5 border border-warning/20 rounded-xl">
          <AlertTriangle size={18} className="text-warning shrink-0 mt-0.5" />
          <div>
            <div className="text-sm font-medium text-warning">No API Key Configured</div>
            <div className="text-xs text-gray-500 mt-0.5">
              The bot is in paper trading mode. Enter your Bitunix API key below to enable live trading.
            </div>
          </div>
        </div>
      )}

      {changed.size > 0 && (
        <div className="flex items-center gap-3 p-3 bg-brand/5 border border-brand/20 rounded-xl">
          <AlertTriangle size={16} className="text-brand" />
          <span className="text-sm text-brand-light">
            {changed.size} unsaved change{changed.size > 1 ? 's' : ''}
          </span>
        </div>
      )}

      {SECTIONS.map(section => {
        const fieldKeys = section.fields.map(f => f.key)
        const sectionChanged = fieldKeys.some(k => changed.has(k))

        return (
          <div key={section.title} className="card">
            <div className="flex items-center justify-between mb-5">
              <h3 className="text-sm font-semibold text-white flex items-center gap-2">
                <section.icon size={16} className="text-brand" />
                {section.title}
              </h3>
              {sectionChanged && (
                <button
                  className="btn-primary text-xs py-1.5"
                  disabled={saveMut.isPending}
                  onClick={() => handleSave(fieldKeys)}
                >
                  <Check size={12} />
                  Save
                </button>
              )}
            </div>

            <div className="space-y-4">
              {section.fields.map(field => (
                <div key={field.key}>
                  <div className="flex items-start justify-between mb-1">
                    <div>
                      <label className="text-sm font-medium text-gray-200">{field.label}</label>
                      {changed.has(field.key) && (
                        <span className="ml-2 text-xs text-brand">modified</span>
                      )}
                    </div>
                    {field.unit && (
                      <span className="text-xs text-gray-500">{field.unit}</span>
                    )}
                  </div>
                  <p className="text-xs text-gray-500 mb-2">{field.description}</p>

                  {field.type === 'boolean' ? (
                    <button
                      onClick={() => handleChange(field.key, !values[field.key])}
                      className={clsx('relative flex items-center gap-3 p-3 rounded-lg border transition-all cursor-pointer',
                        values[field.key]
                          ? 'bg-profit/10 border-profit/30'
                          : 'bg-dark-700 border-dark-500'
                      )}
                    >
                      <div className={clsx('w-10 h-5 rounded-full transition-all relative',
                        values[field.key] ? 'bg-profit' : 'bg-dark-500'
                      )}>
                        <div className={clsx('absolute top-0.5 w-4 h-4 rounded-full bg-white transition-all',
                          values[field.key] ? 'left-5' : 'left-0.5'
                        )} />
                      </div>
                      <span className={clsx('text-sm font-medium',
                        values[field.key] ? 'text-profit' : 'text-gray-400'
                      )}>
                        {values[field.key] ? 'Enabled' : 'Disabled'}
                      </span>
                    </button>
                  ) : (
                    <input
                      type={field.type === 'password' ? 'password' : field.type === 'number' ? 'number' : 'text'}
                      className={clsx('input', changed.has(field.key) && 'border-brand/40 ring-1 ring-brand/20')}
                      value={values[field.key] ?? ''}
                      step={field.step}
                      min={field.min}
                      max={field.max}
                      onChange={(e) => handleChange(
                        field.key,
                        field.type === 'number' ? parseFloat(e.target.value) : e.target.value
                      )}
                    />
                  )}
                </div>
              ))}
            </div>

            {sectionChanged && (
              <div className="mt-4 pt-4 border-t border-dark-700">
                <button
                  className="btn-primary w-full justify-center"
                  disabled={saveMut.isPending}
                  onClick={() => handleSave(fieldKeys)}
                >
                  {saveMut.isPending ? 'Saving...' : `Save ${section.title}`}
                </button>
              </div>
            )}
          </div>
        )
      })}

      {/* Danger Zone */}
      <div className="card border-2 border-loss/20">
        <h3 className="text-sm font-semibold text-loss mb-3 flex items-center gap-2">
          <AlertTriangle size={16} />
          Danger Zone
        </h3>
        <p className="text-xs text-gray-500 mb-3">
          These actions affect all bot operations. Use with extreme caution.
        </p>
        <div className="space-y-2">
          <div className="p-3 bg-dark-700 rounded-lg">
            <div className="text-sm text-white font-medium">Paper Trading Mode</div>
            <div className="text-xs text-gray-500 mt-0.5">
              {values.has_api_key
                ? 'Live trading is ACTIVE. Remove API keys to switch to paper trading.'
                : 'Currently in paper trading mode — no real trades are executed.'
              }
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
