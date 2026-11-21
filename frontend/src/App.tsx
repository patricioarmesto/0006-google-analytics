import { useState } from 'react'
import { BreakIt } from './BreakIt'
import { Dashboard } from './Dashboard'
import { DemoSite } from './DemoSite'
import { sendBurst, type BurstProgress } from './analytics'
import type { Page } from './analytics'

type Tab = 'demo' | 'dashboard' | 'romper'

// El progreso del burst vive en App porque App nunca se desmonta. Si viviera
// en BreakIt, al cambiar de tab el componente se crearía de nuevo con 0 y
// parecería que el envío se perdió, aunque los 10.000 POST sigan en vuelo.
const emptyBurst: BurstProgress = { done: 0, accepted: 0, duplicate: 0, error: 0 }

export function App() {
  const [tab, setTab] = useState<Tab>('demo')
  const [refreshKey, setRefreshKey] = useState(0)
  const [currentPage, setCurrentPage] = useState<Page>('/')
  const [progress, setProgress] = useState<BurstProgress>(emptyBurst)
  const [running, setRunning] = useState(false)

  async function runBurst() {
    setRunning(true)
    setProgress(emptyBurst)
    await sendBurst('/productos', setProgress)
    setRunning(false)
    setRefreshKey((k) => k + 1)
  }

  const tabs: { id: Tab; label: string }[] = [
    { id: 'demo', label: 'Sitio demo' },
    { id: 'dashboard', label: 'Dashboard' },
    { id: 'romper', label: 'Cómo romperlo' },
  ]

  return (
    <main className="mx-auto max-w-6xl px-6 py-10">
      <header className="mb-8">
        <p className="text-xs uppercase tracking-widest text-sky-400">¿Cómo funciona...?</p>
        <h1 className="mt-1 text-3xl font-semibold">Google Analytics</h1>
        <p className="mt-2 text-sm text-slate-400">
          Sitio: <code className="text-sky-300">demo</code> · Vista actual: <code className="text-sky-300">{currentPage}</code> · Sesión: la misma para las tres páginas · Eventos: pageview, click, custom_event
        </p>
      </header>

      <nav className="mb-8 flex gap-2 border-b border-slate-800 pb-4">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
              tab === t.id ? 'bg-sky-500 text-white' : 'bg-slate-900 text-slate-300 hover:bg-slate-800'
            }`}
          >
            {t.label}
          </button>
        ))}
      </nav>

      {tab === 'demo' && <DemoSite onViewChange={setCurrentPage} />}
      {tab === 'dashboard' && <Dashboard refreshKey={refreshKey} />}
      {tab === 'romper' && (
        <BreakIt progress={progress} running={running} onRun={() => void runBurst()} />
      )}
    </main>
  )
}
