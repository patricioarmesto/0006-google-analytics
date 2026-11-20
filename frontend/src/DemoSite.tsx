import { useEffect, useState } from 'react'
import { PAGES, sendEvent, type Page } from './analytics'

// Sin react-router: tres vistas con estado + History API. La librería de
// routing no aporta nada para un demo de tres páginas y sumaría ruido.
export function DemoSite({ onViewChange }: { onViewChange: (page: Page) => void }) {
  const [page, setPage] = useState<Page>(() => (PAGES.includes(location.pathname as Page) ? (location.pathname as Page) : '/'))

  useEffect(() => {
    history.pushState({}, '', page)
    onViewChange(page)
    void sendEvent(page, 'pageview')
  }, [page, onViewChange])

  const go = (next: Page) => setPage(next)

  return (
    <section className="space-y-6">
      <nav className="flex gap-2">
        {PAGES.map((p) => (
          <button
            key={p}
            onClick={() => go(p)}
            className={`rounded-lg px-4 py-2 text-sm font-medium transition ${
              page === p ? 'bg-sky-500 text-white' : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
            }`}
          >
            {p === '/' ? 'Home' : p.slice(1)}
          </button>
        ))}
      </nav>

      <div className="rounded-2xl border border-slate-800 bg-slate-900 p-8">
        <h2 className="text-2xl font-semibold capitalize">{page === '/' ? 'Home' : page.slice(1)}</h2>
        <p className="mt-2 text-slate-400">
          Cada vez que entrás a esta página se manda un <code className="text-sky-300">pageview</code>. Los botones de abajo mandan otros tipos.
        </p>
        <div className="mt-6 flex flex-wrap gap-3">
          <button
            onClick={() => void sendEvent(page, 'click')}
            className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold hover:bg-emerald-500"
          >
            Botón de click
          </button>
          <button
            onClick={() => void sendEvent(page, 'custom_event')}
            className="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold hover:bg-violet-500"
          >
            Custom event
          </button>
        </div>
      </div>
    </section>
  )
}
