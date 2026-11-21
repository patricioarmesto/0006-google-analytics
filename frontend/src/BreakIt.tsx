import type { BurstProgress } from './analytics'

// Componente presentacional: el estado del burst vive en App para que el
// contador siga visible aunque el usuario cambie de tab a mitad del envío.
export function BreakIt({
  progress,
  running,
  onRun,
}: {
  progress: BurstProgress
  running: boolean
  onRun: () => void
}) {
  return (
    <section className="grid gap-6 lg:grid-cols-2">
      <div className="rounded-2xl border border-amber-800/60 bg-amber-950/30 p-6">
        <h2 className="text-lg font-semibold text-amber-200">Mandar 10.000 eventos de golpe</h2>
        <p className="mt-3 text-sm text-amber-100/80">
          Dispara 10.000 <code className="text-amber-300">POST /collect</code> con ~100 requests en vuelo.
          Es el mismo flujo de siempre, pero en escala: el cuello no está en el collector, está en lo que pasa después.
        </p>
        <button
          disabled={running}
          onClick={onRun}
          className="mt-5 rounded-lg bg-amber-500 px-5 py-3 text-sm font-bold text-slate-950 hover:bg-amber-400 disabled:opacity-50"
        >
          {running ? `Enviando… ${progress.done}/10000` : 'Mandar 10.000 eventos de golpe'}
        </button>
        <dl className="mt-6 grid grid-cols-3 gap-3 text-center text-sm">
          <div className="rounded-lg bg-slate-900/60 p-3"><dt className="text-slate-400">Aceptados</dt><dd className="text-lg text-emerald-300">{progress.accepted}</dd></div>
          <div className="rounded-lg bg-slate-900/60 p-3"><dt className="text-slate-400">Duplicados</dt><dd className="text-lg text-sky-300">{progress.duplicate}</dd></div>
          <div className="rounded-lg bg-slate-900/60 p-3"><dt className="text-slate-400">Errores</dt><dd className="text-lg text-red-300">{progress.error}</dd></div>
        </dl>
      </div>

      <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6 text-sm text-slate-300">
        <h2 className="mb-3 text-lg font-semibold text-slate-100">Qué mirar en los logs</h2>
        <pre className="mb-4 overflow-x-auto rounded-lg bg-slate-950 p-3 text-xs text-emerald-300">
          docker compose logs -f consumer
        </pre>
        <ul className="list-disc space-y-2 pl-5">
          <li><strong className="text-sky-300">batch: flush OK de 1000 elementos</strong> — aparece unas 10 veces. Un solo INSERT por batch, no 10.000.</li>
          <li><strong className="text-sky-300">duplicate</strong> — si apretás dos veces, Redis reconoce los <code>event_id</code> y el collector no publica.</li>
          <li><strong className="text-amber-300">ClickHouse caído</strong> — pará <code>docker compose stop clickhouse</code>, mandá eventos y mirá cómo el consumer conserva el batch y reintenta con backoff. Al levantarlo, los datos aparecen.</li>
          <li>Después del pico, el dashboard se actualiza solo: es polling de 5 s sobre ClickHouse.</li>
        </ul>
      </div>
    </section>
  )
}
