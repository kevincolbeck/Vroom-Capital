import { useState, useEffect } from 'react'
import { useLocation } from 'react-router-dom'
import Sidebar from './Sidebar'
import Header from './Header'
import { wsClient } from '../lib/websocket'

interface LayoutProps {
  children: React.ReactNode
}

export default function Layout({ children }: LayoutProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const [wsData, setWsData] = useState<any>(null)

  useEffect(() => {
    wsClient.connect()
    const unsub = wsClient.onMessage((data) => setWsData(data))
    return () => {
      unsub()
    }
  }, [])

  return (
    <div className="flex h-screen bg-dark-900 overflow-hidden">
      <Sidebar collapsed={sidebarCollapsed} onToggle={() => setSidebarCollapsed(!sidebarCollapsed)} />
      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <Header wsData={wsData} />
        <main className="flex-1 overflow-y-auto p-6">
          {children}
        </main>
      </div>
    </div>
  )
}
