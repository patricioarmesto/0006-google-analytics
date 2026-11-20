import { useCallback, useEffect, useState } from 'react'
import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts'
import { SITE } from './analytics'

type Day = { day: string; views: number }
type TopPage = { page: string; views: number }
type Funnel = { pageviews: number; clicks: number; custom_events: number }
type Realtime = { events: number; latest: unknown[] }

async function get<T>(mode: string): Promise<T> {
  const res = await fetch(`/api/stats?${mode}&site=${SITE}`)
  if (!res.ok) throw new Error(`${mode}: ${res.status}`)
  return (await res.json()) as T
}

export function Dashboard({ refreshKey }: { refreshKey: number }) {
  const [days, setDays] = useState<Day[]>([])
  const [top, setTop] = useState<TopPage[]>([])
  const [funnel, setFunnel] = useState<Funnel>({ pageviews: 0, clicks: 0, custom_events: 0 })
  const [realtime, setRealtime] = useState<Realtime>({ events: 0, latest: [] })
  const [error, setError] = useState('')

  // Polling de 5 s, igual que el panel en vivo de GA: no hace falta WebSocket
  // para un ejercicio donde la gránularidad es de segundos.
  const load = useCallback(async () => {
    try {
      const [d, t, f, r] = await Promise.all([
        get<Day[]>('pageviews-by-day'),
        get<TopPage[]>('top-pages'),
        get<Funnel>('funnel'),
        get<Realtime>('realtime'),
      ])
      setDays(d)
      setTop(t)
      setFunnel(f)
      setRealtime(r)
      setError('')
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  useEffect(() => {
    void load()
    const id = setInterval(() => void load(), 5000)
    return () => clearInterval(id)
  }, [load, refreshKey])

  const funnelData = [
    { step: 'pageview', value: funnel.pageviews },
    { step: 'click', value: funnel.clicks },
    { step: 'custom_event', value: funnel.custom_events },
  ]

  return (
    <section className="space-y-6">
      {error && <p className="rounded-lg bg-red-950 p-3 text-sm text-red-300">Error: {error}</p>}

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-4 text-lg font-semibold">Pageviews por día</h2>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={days}>
              <CartesianGrid stroke="#1e293b" />
              <XAxis dataKey="day" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} allowDecimals={false} />
              <Tooltip />
              <Line type="monotone" dataKey="views" stroke="#38bdf8" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-4 text-lg font-semibold">Embudo (sesiones únicas)</h2>
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={funnelData}>
              <CartesianGrid stroke="#1e293b" />
              <XAxis dataKey="step" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} allowDecimals={false} />
              <Tooltip />
              <Bar dataKey="value" fill="#a78bfa" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <h2 className="mb-4 text-lg font-semibold">Top páginas</h2>
          <table className="w-full text-sm">
            <thead className="text-left text-slate-400">
              <tr><th className="pb-2">Página</th><th className="pb-2 text-right">Views</th></tr>
            </thead>
            <tbody>
              {top.map((p) => (
                <tr key={p.page} className="border-t border-slate-800">
                  <td className="py-2 font-mono text-sky-300">{p.page}</td>
                  <td className="py-2 text-right tabular-nums">{p.views}</td>
                </tr>
              ))}
              {top.length === 0 && <tr><td colSpan={2} className="py-4 text-slate-500">Sin datos todavía</td></tr>}
            </tbody>
          </table>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Tiempo real</h2>
            <span className="flex items-center gap-2 text-xs text-emerald-400">
              <span className="h-2 w-2 animate-pulse rounded-full bg-emerald-400" />Actualiza cada 5 s
            </span>
          </div>
          <p className="mt-6 text-5xl font-semibold tabular-nums text-emerald-300">{realtime.events}</p>
          <p className="mt-1 text-sm text-slate-400">eventos en los últimos 5 minutos</p>
          <p className="mt-6 text-xs text-slate-500">
            El contador viene de ClickHouse, no de Redis: lo que ves es lo que ya se persistió.
          </p>
        </div>
      </div>
    </section>
  )
}
