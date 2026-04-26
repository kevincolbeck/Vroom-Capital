import { NavLink, useNavigate } from 'react-router-dom'
import {
  LayoutDashboard, TrendingUp, Users, Settings, Activity,
  FileText, Zap, ChevronLeft, ChevronRight, LogOut, FlaskConical
} from 'lucide-react'
import { clsx } from 'clsx'

const navItems = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/positions', icon: TrendingUp, label: 'Positions' },
  { to: '/copy-trading', icon: Users, label: 'Copy Trading' },
  { to: '/bot-control', icon: Zap, label: 'Bot Control' },
  { to: '/analytics', icon: Activity, label: 'Analytics' },
  { to: '/backtest', icon: FlaskConical, label: 'Backtest' },
  { to: '/logs', icon: FileText, label: 'Logs' },
  { to: '/settings', icon: Settings, label: 'Settings' },
]

interface SidebarProps {
  collapsed: boolean
  onToggle: () => void
}

export default function Sidebar({ collapsed, onToggle }: SidebarProps) {
  const navigate = useNavigate()

  const handleLogout = () => {
    localStorage.removeItem('legion_token')
    navigate('/login')
  }

  return (
    <aside
      className={clsx(
        'flex flex-col bg-dark-800 border-r border-dark-600 transition-all duration-300 shrink-0',
        collapsed ? 'w-16' : 'w-56'
      )}
    >
      {/* Logo */}
      <div className={clsx(
        'flex items-center gap-3 px-4 py-5 border-b border-dark-600',
        collapsed && 'justify-center px-2'
      )}>
        <div className="w-8 h-8 rounded-lg bg-brand flex items-center justify-center shrink-0 glow-brand">
          <Zap size={18} className="text-white" />
        </div>
        {!collapsed && (
          <div>
            <div className="text-sm font-bold text-white">VROOM CAPITAL</div>
            <div className="text-xs text-brand-light">BTC Futures</div>
          </div>
        )}
      </div>

      {/* Navigation */}
      <nav className="flex-1 p-2 space-y-1 overflow-y-auto">
        {navItems.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) =>
              clsx(
                'flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150',
                collapsed && 'justify-center px-2',
                isActive
                  ? 'text-brand-light bg-brand/10 border border-brand/20'
                  : 'text-gray-400 hover:text-white hover:bg-dark-700'
              )
            }
            title={collapsed ? label : undefined}
          >
            <Icon size={18} className="shrink-0" />
            {!collapsed && <span>{label}</span>}
          </NavLink>
        ))}
      </nav>

      {/* Bottom */}
      <div className={clsx('p-2 border-t border-dark-600 space-y-1')}>
        <button
          onClick={handleLogout}
          className={clsx(
            'w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium text-gray-500 hover:text-red-400 hover:bg-dark-700 transition-all',
            collapsed && 'justify-center px-2'
          )}
          title={collapsed ? 'Logout' : undefined}
        >
          <LogOut size={18} className="shrink-0" />
          {!collapsed && <span>Logout</span>}
        </button>

        <button
          onClick={onToggle}
          className={clsx(
            'w-full flex items-center gap-3 px-3 py-2 rounded-lg text-xs text-gray-600 hover:text-gray-400 hover:bg-dark-700 transition-all',
            collapsed && 'justify-center'
          )}
        >
          {collapsed ? <ChevronRight size={16} /> : (
            <>
              <ChevronLeft size={16} />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>
    </aside>
  )
}
