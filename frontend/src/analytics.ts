// El evento se arma en el navegador, igual que lo haría el script real de GA.
// crypto.randomUUID() da el event_id; la session_id vive en sessionStorage para
// que las tres páginas compartan sesión sin backend de sesiones.
export const SITE = 'demo'
export const PAGES = ['/', '/productos', '/contacto'] as const

export type Page = (typeof PAGES)[number]
export type EventType = 'pageview' | 'click' | 'custom_event'
export type SendResult = 'accepted' | 'duplicate' | 'error'

const sessionKey = 'ga-session-id'
let sessionId = sessionStorage.getItem(sessionKey)
if (!sessionId) {
  sessionId = crypto.randomUUID()
  sessionStorage.setItem(sessionKey, sessionId)
}

export async function sendEvent(page: Page, type: EventType): Promise<SendResult> {
  const event = {
    event_id: crypto.randomUUID(),
    session_id: sessionId,
    page,
    type,
    timestamp: new Date().toISOString(),
  }
  try {
    const res = await fetch('/collect', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(event),
    })
    if (!res.ok) return 'error'
    const body = (await res.json()) as { status: SendResult }
    return body.status
  } catch {
    return 'error'
  }
}

export type BurstProgress = { done: number; accepted: number; duplicate: number; error: number }

// 10.000 requests de golpe pueden ahogar al navegador y al collector. Con
// ~100 en vuelo el pico es constante y se ve el efecto real: dedup en Redis,
// batches de 1000 en el consumer y, si se cae ClickHouse, reintentos en logs.
// El writer de Kafka usa BatchTimeout corto para que cada POST no espere 1 s.
export async function sendBurst(
  page: Page,
  onProgress: (p: BurstProgress) => void,
  total = 10_000,
  concurrency = 100,
): Promise<BurstProgress> {
  const progress: BurstProgress = { done: 0, accepted: 0, duplicate: 0, error: 0 }
  let next = 0
  async function worker() {
    while (next < total) {
      next++
      const result = await sendEvent(page, 'custom_event')
      progress.done++
      if (result === 'accepted') progress.accepted++
      else if (result === 'duplicate') progress.duplicate++
      else progress.error++
      if (progress.done % 100 === 0) onProgress({ ...progress })
    }
  }
  await Promise.all(Array.from({ length: concurrency }, worker))
  onProgress({ ...progress })
  return progress
}
