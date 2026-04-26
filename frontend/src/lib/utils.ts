export function formatPrice(price: number | null | undefined, decimals = 0): string {
  if (price == null) return '—'
  return `$${price.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`
}

export function formatPct(pct: number | null | undefined, decimals = 2): string {
  if (pct == null) return '—'
  const sign = pct > 0 ? '+' : ''
  return `${sign}${pct.toFixed(decimals)}%`
}

export function formatUsd(amount: number | null | undefined, decimals = 2): string {
  if (amount == null) return '—'
  const sign = amount > 0 ? '+' : ''
  return `${sign}$${Math.abs(amount).toFixed(decimals)}`
}

export function formatDate(iso: string | null | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleString('en-US', {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false
  })
}

export function timeAgo(iso: string | null | undefined): string {
  if (!iso) return '—'
  const diff = Date.now() - new Date(iso).getTime()
  const mins = Math.floor(diff / 60000)
  const hrs = Math.floor(mins / 60)
  const days = Math.floor(hrs / 24)
  if (days > 0) return `${days}d ago`
  if (hrs > 0) return `${hrs}h ago`
  if (mins > 0) return `${mins}m ago`
  return 'just now'
}

export function clsxMerge(...classes: (string | undefined | null | false)[]): string {
  return classes.filter(Boolean).join(' ')
}
