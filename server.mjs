import express from 'express'
import cors from 'cors'
import { google } from 'googleapis'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)

const credentials = JSON.parse(
  fs.readFileSync(
    path.join(__dirname, 'logical-bolt-486419-a8-1e30a914f891.json'),
    'utf-8'
  )
)

const app = express()
app.use(cors())
app.use(express.json({ limit: '1mb' }))

// Persistent state file for audit log, velocity snapshots, cost config, feedback
const STATE_FILE = path.join(__dirname, 'app-state.json')
function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) {
      return JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'))
    }
  } catch {
    // ignore
  }
  return {
    auditLog: [],
    snapshots: {},
    costConfig: {},
    feedback: {},
    users: [{ email: 'owner@demo.com', password: 'demo123', role: 'owner', name: 'Owner' }],
    sessions: {},
    tenantConfig: { hotThreshold: 0.65, warmThreshold: 0.4, targetHotPct: 0.25 }
  }
}
function saveState(state) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2))
  } catch (e) {
    console.error('saveState failed:', e.message)
  }
}
const APP_STATE = loadState()

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: credentials.client_email,
    private_key: credentials.private_key
  },
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
})

const sheets = google.sheets({ version: 'v4', auth })

const SHEETS = {
  website: '1wJkTdDIU334IfbUZt9TA_HFM4UzHySCgolbNLY8fTt0',
  offline: '1ZKFWbjLMdr8PepI25Uz3z-LLIEnHwnrohsBMU-Fb9Sw',
  marketing: '1sLgXHKAZeFksG_s_oDu5wrMDuqBafmbjO-lVO1XZd0I',
  broker: '1MmCcO5BlpUjn3KZ7fbdP9qyii6pM-CAJkC0o1Fnqybo',
  whatsapp: '1HXoojJHxMyg54PPNd9BbaFIZnO69FOke4_688GvD4BM'
}

async function loadLeads(spreadsheetId) {
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'A1:J'
  })
  const rows = result.data.values || []

  if (rows.length <= 1) {
    return []
  }

  const [header, ...dataRows] = rows
  const index = {}
  header.forEach((col, i) => {
    index[String(col).trim().toLowerCase()] = i
  })

  const pick = (row, keys) => {
    for (const k of keys) {
      const i = index[k]
      if (typeof i === 'number') return row[i] || ''
    }
    return ''
  }

  return dataRows.map((row, ri) => ({
    id: pick(row, ['lead id', 'id']) || String(ri + 1),
    name: pick(row, ['name']),
    phone: pick(row, ['phone']),
    email: pick(row, ['email']),
    city: pick(row, ['city']),
    budget: pick(row, ['budget']),
    property: pick(row, ['property type', 'property']),
    inquiryDate: pick(row, ['inquiry date', 'date']),
    source: pick(row, ['source']),
    status: pick(row, ['status']),
    raw: row
  }))
}

async function loadOfflineLeads(spreadsheetId) {
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'A1:J'
  })
  const rows = result.data.values || []

  if (rows.length <= 1) {
    return []
  }

  const [header, ...dataRows] = rows
  const index = {}
  header.forEach((col, i) => {
    index[String(col).trim().toLowerCase()] = i
  })

  const pick = (row, keys) => {
    for (const k of keys) {
      const i = index[k]
      if (typeof i === 'number') return row[i] || ''
    }
    return ''
  }

  return dataRows.map((row, ri) => ({
    id: pick(row, ['lead id', 'id']) || String(ri + 1),
    name: pick(row, ['name']),
    phone: pick(row, ['phone']),
    email: '',
    city: '',
    budget: pick(row, ['budget']),
    property: pick(row, ['project visited', 'project']),
    inquiryDate: pick(row, ['visit date', 'date']),
    source: pick(row, ['source']),
    status: pick(row, ['status']),
    raw: row,
    feedback: pick(row, ['feedback']),
    salesExec: pick(row, ['sales exec', 'sales executive'])
  }))
}

async function loadMarketingLeads(spreadsheetId) {
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'A1:J'
  })
  const rows = result.data.values || []

  if (rows.length <= 1) {
    return []
  }

  const [header, ...dataRows] = rows
  const index = {}
  header.forEach((col, i) => {
    index[String(col).trim().toLowerCase()] = i
  })

  const pick = (row, keys) => {
    for (const k of keys) {
      const i = index[k]
      if (typeof i === 'number') return row[i] || ''
    }
    return ''
  }

  return dataRows.map((row, ri) => ({
    id: pick(row, ['lead id', 'id']) || String(ri + 1),
    name: pick(row, ['name']),
    phone: pick(row, ['phone']),
    email: '',
    city: pick(row, ['city']),
    budget: pick(row, ['budget']),
    property: pick(row, ['campaign name', 'campaign']),
    inquiryDate: '',
    source: pick(row, ['source']),
    status: pick(row, ['status']),
    raw: row,
    campaignName: pick(row, ['campaign name', 'campaign']),
    platform: pick(row, ['platform']),
    adClicks: pick(row, ['ad clicks'])
  }))
}

async function loadBrokerLeads(spreadsheetId) {
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'A1:J'
  })
  const rows = result.data.values || []

  if (rows.length <= 1) {
    return []
  }

  const [header, ...dataRows] = rows
  const index = {}
  header.forEach((col, i) => {
    index[String(col).trim().toLowerCase()] = i
  })

  const pick = (row, keys) => {
    for (const k of keys) {
      const i = index[k]
      if (typeof i === 'number') return row[i] || ''
    }
    return ''
  }

  return dataRows.map((row, ri) => ({
    id: pick(row, ['lead id', 'id']) || String(ri + 1),
    name: pick(row, ['client name', 'name']),
    phone: pick(row, ['phone']),
    email: '',
    city: pick(row, ['city']),
    budget: pick(row, ['budget']),
    property: pick(row, ['property type', 'property']),
    inquiryDate: '',
    source: pick(row, ['source']),
    status: pick(row, ['status']),
    raw: row,
    brokerName: pick(row, ['broker name']),
    commission: pick(row, ['commission %', 'commission'])
  }))
}

async function loadWhatsappLeads(spreadsheetId) {
  const result = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: 'A1:K'
  })
  const rows = result.data.values || []

  if (rows.length <= 1) {
    return []
  }

  const [header, ...dataRows] = rows
  const index = {}
  header.forEach((col, i) => {
    index[String(col).trim().toLowerCase()] = i
  })

  const pick = (row, keys) => {
    for (const k of keys) {
      const i = index[k]
      if (typeof i === 'number') return row[i] || ''
    }
    return ''
  }

  return dataRows.map((row, ri) => ({
    id: pick(row, ['lead id', 'id']) || String(ri + 1),
    name: pick(row, ['name']),
    phone: pick(row, ['phone']),
    email: '',
    city: pick(row, ['location', 'city']),
    budget: pick(row, ['budget']),
    property: '',
    inquiryDate: '',
    source: pick(row, ['source']),
    status: pick(row, ['status']),
    raw: row,
    interested: pick(row, ['interested', 'interestec', 'interest']),
    visitReq: pick(row, ['visit req', 'visit requ', 'visit request', 'visit requested']),
    lastResp: pick(row, [
      'last resp',
      'last respon',
      'last response',
      'last response days'
    ]),
    chatSummary: pick(row, ['chat sum', 'chat summ', 'chat summar', 'chat summary'])
  }))
}

app.get('/api/website-leads', async (req, res) => {
  try {
    const leads = await loadLeads(SHEETS.website)
    res.json({ leads })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to fetch website leads' })
  }
})

app.get('/api/offline-leads', async (req, res) => {
  try {
    const leads = await loadOfflineLeads(SHEETS.offline)
    res.json({ leads })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to fetch offline leads' })
  }
})

app.get('/api/marketing-leads', async (req, res) => {
  try {
    const leads = await loadMarketingLeads(SHEETS.marketing)
    res.json({ leads })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to fetch marketing leads' })
  }
})

app.get('/api/broker-leads', async (req, res) => {
  try {
    const leads = await loadBrokerLeads(SHEETS.broker)
    res.json({ leads })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to fetch broker leads' })
  }
})

app.get('/api/whatsapp-leads', async (req, res) => {
  try {
    const leads = await loadWhatsappLeads(SHEETS.whatsapp)
    res.json({ leads })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to fetch WhatsApp leads' })
  }
})

// ============================================================
// Unification, Naive Bayes, Bayesian Network, Forecast
// ============================================================

function normalizePhone(phone) {
  return String(phone || '').replace(/\D/g, '').slice(-10)
}

function parseBudgetCrore(budget) {
  if (!budget) return null
  const s = String(budget).toLowerCase().replace(/,/g, '')
  const nums = s.match(/[\d.]+/g)
  if (!nums) return null
  const values = nums.map(Number).filter((n) => !Number.isNaN(n))
  if (!values.length) return null
  const avg = values.reduce((a, b) => a + b, 0) / values.length
  if (s.includes('cr')) return avg
  if (s.includes('lakh') || s.includes('l')) return avg / 100
  if (avg > 1000000) return avg / 10000000
  if (avg > 1000) return avg / 100
  return avg
}

function budgetBucket(budget) {
  const cr = parseBudgetCrore(budget)
  if (cr === null) return 'unknown'
  if (cr >= 1) return 'high'
  if (cr >= 0.5) return 'mid'
  return 'low'
}

// Status normalization — handles common typos, casing, trailing spaces, synonyms
function normalizeStatus(status) {
  const raw = String(status || '').toLowerCase().trim().replace(/[!?.*]+$/g, '')
  if (!raw) return 'unknown'
  // Normalize whitespace
  const s = raw.replace(/\s+/g, ' ')

  // Converted / won (including common typos)
  if (/(convert(ed)?|booke?d|book\s?ing\s?done|closed\s?won|closed-?won|won|sold|purchased|token\s?paid|paid|confirmed)/i.test(s))
    return 'converted'

  // Lost / dead (including typos)
  if (/(lost|drop(ped)?|closed\s?lost|closed-?lost|dead|not\s?interest(ed)?|rejected|cancel(led)?|unqualified|no\s?longer)/i.test(s))
    return 'lost'

  // Qualified / hot (with common typos: qualifed, quallified)
  if (/(hot|qual\w*|interest(ed)?|site\s?visit|visit\s?schedul|negotiat|ready|serious|warm)/i.test(s))
    return 'qualified'

  // Open / in progress
  if (/(in\s?progress|open|working|follow\s?up|contacted|reach(ed)?|call(ed)?|pending|new|initiated)/i.test(s))
    return 'in_progress'

  return 'in_progress'
}

// Competitor detection — parse chat/feedback for mentions of known builders
const COMPETITOR_PATTERNS = [
  { name: 'Prestige',   re: /\bprestige\b/i },
  { name: 'Sobha',      re: /\bsobha\b/i },
  { name: 'Brigade',    re: /\bbrigade\b/i },
  { name: 'Godrej',     re: /\bgodrej\b/i },
  { name: 'DLF',        re: /\bdlf\b/i },
  { name: 'Lodha',      re: /\blodha\b/i },
  { name: 'Oberoi',     re: /\boberoi\b/i },
  { name: 'Embassy',    re: /\bembassy\b/i },
  { name: 'Mantri',     re: /\bmantri\b/i },
  { name: 'Puravankara',re: /\bpuravankara\b/i },
  { name: 'Shapoorji',  re: /\bshapoorji\b/i },
  { name: 'Tata',       re: /\btata\s?(housing|realty|value)?\b/i },
  { name: 'Mahindra',   re: /\bmahindra\b/i }
]

function detectCompetitors(lead) {
  const text = `${lead.chatSummary || ''} ${lead.feedback || ''} ${lead.property || ''}`.toLowerCase()
  if (!text.trim()) return []
  const hits = []
  for (const c of COMPETITOR_PATTERNS) {
    if (c.re.test(text)) hits.push(c.name)
  }
  return hits
}

// Semantic keyword scoring over chat/feedback text — catches strong intent phrases
const HOT_PHRASES = [
  { phrase: 'loan approved',     weight: 0.15 },
  { phrase: 'pre-?approved',     weight: 0.12 },
  { phrase: 'ready to book',     weight: 0.20 },
  { phrase: 'token',             weight: 0.15 },
  { phrase: 'booking amount',    weight: 0.18 },
  { phrase: 'closing',           weight: 0.10 },
  { phrase: 'final(i[sz]ed|ising)', weight: 0.10 },
  { phrase: 'site visit',        weight: 0.10 },
  { phrase: 'block(ed|ing)? (a |the )?unit', weight: 0.15 },
  { phrase: 'emi',               weight: 0.05 },
  { phrase: 'possession',        weight: 0.05 },
  { phrase: 'negotiat',          weight: 0.08 },
  { phrase: 'discount',          weight: 0.06 }
]
const COLD_PHRASES = [
  { phrase: 'just browsing',     weight: -0.10 },
  { phrase: 'not interested',    weight: -0.20 },
  { phrase: 'no longer',         weight: -0.15 },
  { phrase: 'dropped',           weight: -0.10 },
  { phrase: 'only asking',       weight: -0.08 },
  { phrase: 'future reference',  weight: -0.08 }
]

function chatSemanticScore(lead) {
  const text = `${lead.chatSummary || ''} ${lead.feedback || ''}`.toLowerCase()
  if (!text.trim()) return { score: 0, matches: [] }
  let score = 0
  const matches = []
  for (const p of [...HOT_PHRASES, ...COLD_PHRASES]) {
    const re = new RegExp(p.phrase, 'i')
    if (re.test(text)) {
      score += p.weight
      matches.push({ phrase: p.phrase, weight: p.weight })
    }
  }
  return { score: Math.max(-0.3, Math.min(0.3, score)), matches }
}

function daysSince(dateStr) {
  if (!dateStr) return null
  const t = Date.parse(String(dateStr))
  if (Number.isNaN(t)) return null
  return Math.max(0, Math.floor((Date.now() - t) / 86400000))
}

function parseNumber(v) {
  const n = Number(String(v || '').replace(/[^\d.-]/g, ''))
  return Number.isNaN(n) ? null : n
}

// Cache layer — TTL 120s. Invalidate manually via /api/refresh.
const CACHE = { data: null, sourceErrors: {}, expiresAt: 0 }
const CACHE_TTL_MS = 120_000

async function loadAllSources({ force = false } = {}) {
  const now = Date.now()
  if (!force && CACHE.data && CACHE.expiresAt > now) {
    return { ...CACHE.data, _cached: true, _sourceErrors: CACHE.sourceErrors }
  }

  const sourceErrors = {}
  const safe = async (name, fn) => {
    try {
      return await fn()
    } catch (e) {
      sourceErrors[name] = e.message
      return []
    }
  }

  const [website, offline, marketing, broker, whatsapp] = await Promise.all([
    safe('website', () => loadLeads(SHEETS.website)),
    safe('offline', () => loadOfflineLeads(SHEETS.offline)),
    safe('marketing', () => loadMarketingLeads(SHEETS.marketing)),
    safe('broker', () => loadBrokerLeads(SHEETS.broker)),
    safe('whatsapp', () => loadWhatsappLeads(SHEETS.whatsapp))
  ])

  CACHE.data = { website, offline, marketing, broker, whatsapp }
  CACHE.sourceErrors = sourceErrors
  CACHE.expiresAt = now + CACHE_TTL_MS
  return { ...CACHE.data, _cached: false, _sourceErrors: sourceErrors }
}

function invalidateCache() {
  CACHE.data = null
  CACHE.expiresAt = 0
}

function unifyLeads(sources) {
  const byKey = new Map()

  const tag = (list, channel) => {
    for (const lead of list) {
      const key = normalizePhone(lead.phone) || `${channel}:${lead.id}`
      const existing = byKey.get(key)
      const record = existing || {
        key,
        id: lead.id,
        name: lead.name || existing?.name || '',
        phone: lead.phone || existing?.phone || '',
        email: lead.email || existing?.email || '',
        city: lead.city || existing?.city || '',
        budget: lead.budget || existing?.budget || '',
        property: lead.property || existing?.property || '',
        inquiryDate: lead.inquiryDate || existing?.inquiryDate || '',
        source: lead.source || existing?.source || '',
        status: '',
        channels: new Set(),
        statuses: [],
        interactions: 0,
        adClicks: 0,
        visitRequested: false,
        interestedFlag: false,
        lastRespDays: null,
        chatSummary: '',
        feedback: '',
        salesExec: '',
        brokerName: '',
        campaignName: '',
        platform: ''
      }

      record.channels.add(channel)
      record.interactions += 1
      if (lead.status) record.statuses.push(lead.status)

      if (channel === 'whatsapp') {
        if (/yes|y|true|interest/i.test(lead.interested || '')) record.interestedFlag = true
        if (/yes|y|true|req/i.test(lead.visitReq || '')) record.visitRequested = true
        const resp = parseNumber(lead.lastResp)
        if (resp !== null) record.lastRespDays = resp
        if (lead.chatSummary) record.chatSummary = lead.chatSummary
      }
      if (channel === 'marketing') {
        const clicks = parseNumber(lead.adClicks) || 0
        record.adClicks += clicks
        record.campaignName = lead.campaignName || record.campaignName
        record.platform = lead.platform || record.platform
      }
      if (channel === 'offline') {
        record.feedback = lead.feedback || record.feedback
        record.salesExec = lead.salesExec || record.salesExec
        record.visitRequested = true
      }
      if (channel === 'broker') {
        record.brokerName = lead.brokerName || record.brokerName
      }

      byKey.set(key, record)
    }
  }

  tag(sources.website, 'website')
  tag(sources.offline, 'offline')
  tag(sources.marketing, 'marketing')
  tag(sources.broker, 'broker')
  tag(sources.whatsapp, 'whatsapp')

  return Array.from(byKey.values()).map((r) => {
    const normStatuses = r.statuses.map(normalizeStatus)
    let finalStatus = 'unknown'
    if (normStatuses.includes('converted')) finalStatus = 'converted'
    else if (normStatuses.includes('lost')) finalStatus = 'lost'
    else if (normStatuses.includes('qualified')) finalStatus = 'qualified'
    else if (normStatuses.includes('in_progress')) finalStatus = 'in_progress'
    return {
      ...r,
      status: finalStatus,
      rawStatus: r.statuses[r.statuses.length - 1] || '',
      channels: Array.from(r.channels),
      budgetBucket: budgetBucket(r.budget),
      daysSinceInquiry: daysSince(r.inquiryDate)
    }
  })
}

// ============================================================
// Numeric feature vectorization (for logistic regression + similarity)
// ============================================================

const FEATURE_NAMES = [
  'budget_cr',         // normalized budget, capped at 5 Cr
  'has_budget',
  'high_budget',       // >= 1 Cr
  'interactions',      // normalized by 10
  'channels',          // normalized by 5
  'visit_requested',
  'ad_clicks',         // normalized by 20
  'interested_flag',
  'has_chat',
  'has_sales_exec',
  'has_broker',
  'city_stated',
  'recency',           // 1 - days_since_inquiry/60, fresh = 1
  'responsiveness'     // 1 - last_resp_days/30, fresh = 1, 0.5 if null
]

function vectorize(lead) {
  const budgetCr = parseBudgetCrore(lead.budget)
  const budgetNorm = budgetCr === null ? 0 : Math.min(budgetCr / 5, 1)
  const recency =
    lead.daysSinceInquiry === null
      ? 0.5
      : Math.max(0, 1 - lead.daysSinceInquiry / 60)
  const responsiveness =
    lead.lastRespDays === null
      ? 0.5
      : Math.max(0, 1 - lead.lastRespDays / 30)

  return [
    budgetNorm,
    lead.budget ? 1 : 0,
    budgetCr !== null && budgetCr >= 1 ? 1 : 0,
    Math.min(lead.interactions / 10, 1),
    Math.min(lead.channels.length / 5, 1),
    lead.visitRequested ? 1 : 0,
    Math.min(lead.adClicks / 20, 1),
    lead.interestedFlag ? 1 : 0,
    lead.chatSummary ? 1 : 0,
    lead.salesExec ? 1 : 0,
    lead.brokerName ? 1 : 0,
    lead.city ? 1 : 0,
    recency,
    responsiveness
  ]
}

// ============================================================
// Logistic Regression — gradient descent with L2 regularization
// ============================================================

function sigmoid(z) {
  if (z >= 0) return 1 / (1 + Math.exp(-z))
  const ez = Math.exp(z)
  return ez / (1 + ez)
}

function trainLogReg(X, y, { lr = 0.1, epochs = 800, lambda = 0.01 } = {}) {
  const n = X.length
  const d = X[0]?.length || 0
  const w = new Array(d).fill(0)
  let b = 0

  if (!n) return { w, b }

  for (let epoch = 0; epoch < epochs; epoch++) {
    const gradW = new Array(d).fill(0)
    let gradB = 0
    for (let i = 0; i < n; i++) {
      let z = b
      for (let j = 0; j < d; j++) z += w[j] * X[i][j]
      const p = sigmoid(z)
      const err = p - y[i]
      gradB += err
      for (let j = 0; j < d; j++) gradW[j] += err * X[i][j]
    }
    for (let j = 0; j < d; j++) {
      w[j] -= lr * (gradW[j] / n + lambda * w[j])
    }
    b -= lr * (gradB / n)
  }

  return { w, b }
}

function predictLogReg(model, x) {
  let z = model.b
  for (let j = 0; j < x.length; j++) z += model.w[j] * x[j]
  return sigmoid(z)
}

function kFoldCrossValidate(X, y, k = 5) {
  const n = X.length
  if (n < 4) return { accuracy: 0, auc: 0, folds: 0 }
  const folds = Math.min(k, n)
  const indices = X.map((_, i) => i)
  // Shuffle deterministically for reproducibility
  for (let i = indices.length - 1; i > 0; i--) {
    const j = Math.floor(((i * 2654435761) % (i + 1)))
    ;[indices[i], indices[j]] = [indices[j], indices[i]]
  }

  let totalCorrect = 0
  let totalEval = 0
  const preds = []
  const truths = []

  for (let f = 0; f < folds; f++) {
    const testIdx = indices.filter((_, i) => i % folds === f)
    const trainIdx = indices.filter((_, i) => i % folds !== f)
    if (!trainIdx.length || !testIdx.length) continue
    const trainX = trainIdx.map((i) => X[i])
    const trainY = trainIdx.map((i) => y[i])
    const model = trainLogReg(trainX, trainY)
    for (const i of testIdx) {
      const p = predictLogReg(model, X[i])
      const predLabel = p >= 0.5 ? 1 : 0
      if (predLabel === y[i]) totalCorrect += 1
      totalEval += 1
      preds.push(p)
      truths.push(y[i])
    }
  }

  const accuracy = totalEval ? totalCorrect / totalEval : 0
  return { accuracy, preds, truths, folds }
}

// Holdout validation — deterministic 80/20 split, trains on 80, evaluates on 20.
// More honest than training-set accuracy when >= 20 labels available.
function holdoutEval(X, y) {
  const n = X.length
  if (n < 10) return { accuracy: null, precision: null, recall: null, holdoutSize: 0 }
  const idx = X.map((_, i) => i)
  for (let i = idx.length - 1; i > 0; i--) {
    const j = Math.floor(((i * 2654435761) % (i + 1)))
    ;[idx[i], idx[j]] = [idx[j], idx[i]]
  }
  const split = Math.floor(n * 0.8)
  const trainIdx = idx.slice(0, split)
  const testIdx = idx.slice(split)
  if (!testIdx.length || !trainIdx.length) return { accuracy: null, precision: null, recall: null, holdoutSize: 0 }

  const model = trainLogReg(trainIdx.map((i) => X[i]), trainIdx.map((i) => y[i]))
  let tp = 0, fp = 0, tn = 0, fn = 0
  for (const i of testIdx) {
    const p = predictLogReg(model, X[i])
    const pred = p >= 0.5 ? 1 : 0
    if (pred === 1 && y[i] === 1) tp++
    else if (pred === 1 && y[i] === 0) fp++
    else if (pred === 0 && y[i] === 0) tn++
    else fn++
  }
  const accuracy = (tp + tn) / testIdx.length
  const precision = (tp + fp) ? tp / (tp + fp) : null
  const recall = (tp + fn) ? tp / (tp + fn) : null
  return { accuracy, precision, recall, holdoutSize: testIdx.length, tp, fp, tn, fn }
}

function trainIntentModel(unified) {
  const labelOf = (lead) => {
    if (lead.status === 'converted' || lead.status === 'qualified') return 1
    if (lead.status === 'lost') return 0
    return null
  }

  const labeled = unified
    .map((l) => ({ lead: l, label: labelOf(l), x: vectorize(l) }))
    .filter((x) => x.label !== null)

  const X = labeled.map((r) => r.x)
  const y = labeled.map((r) => r.label)

  // Full-data model for production predictions
  const model =
    X.length > 0 ? trainLogReg(X, y) : { w: new Array(FEATURE_NAMES.length).fill(0), b: 0 }

  // Accuracy estimation via both 5-fold CV and a held-out 80/20 split
  const cv = kFoldCrossValidate(X, y, 5)
  const holdout = holdoutEval(X, y)

  const featureImportance = FEATURE_NAMES.map((name, i) => ({
    name,
    weight: Number((model.w[i] || 0).toFixed(4))
  })).sort((a, b) => Math.abs(b.weight) - Math.abs(a.weight))

  const predict = (lead) => {
    const x = vectorize(lead)
    const prob = predictLogReg(model, x)
    return { score: prob, vector: x }
  }

  return {
    predict,
    model,
    cvAccuracy: cv.accuracy,
    holdout,
    labeledCount: labeled.length,
    positiveCount: y.filter((v) => v === 1).length,
    negativeCount: y.filter((v) => v === 0).length,
    featureImportance
  }
}

// Lead velocity — compares current score to prior snapshot, detects acceleration.
function computeVelocity(lead, currentScore) {
  const snap = APP_STATE.snapshots[lead.key]
  if (!snap) return { delta: 0, trend: 'new' }
  const delta = currentScore - snap.score
  let trend = 'flat'
  if (delta > 0.1) trend = 'accelerating'
  else if (delta < -0.1) trend = 'cooling'
  return { delta: Number(delta.toFixed(3)), trend, prior: snap.score, priorAt: snap.at }
}

function saveSnapshots(enriched) {
  const now = Date.now()
  for (const lead of enriched) {
    APP_STATE.snapshots[lead.key] = { score: Number(lead.score.toFixed(3)), at: now }
  }
  // Trim to avoid unbounded growth — keep last 10k
  const keys = Object.keys(APP_STATE.snapshots)
  if (keys.length > 10000) {
    const trimmed = {}
    keys.slice(-10000).forEach((k) => (trimmed[k] = APP_STATE.snapshots[k]))
    APP_STATE.snapshots = trimmed
  }
  saveState(APP_STATE)
}

// Confidence per lead — based on how much training data informed this prediction
function leadConfidence(labeledCount) {
  if (labeledCount >= 100) return 'High'
  if (labeledCount >= 30) return 'Medium'
  if (labeledCount >= 10) return 'Low'
  return 'Insufficient data'
}

// Dynamic threshold calibration — picks thresholds so top targetPct = Hot
function calibrateThresholds(scores, targetHotPct = 0.25, targetWarmPct = 0.5) {
  if (!scores.length) return { hot: 0.65, warm: 0.4 }
  const sorted = [...scores].sort((a, b) => b - a)
  const hotIdx = Math.max(0, Math.floor(sorted.length * targetHotPct) - 1)
  const warmIdx = Math.max(0, Math.floor(sorted.length * (targetHotPct + targetWarmPct)) - 1)
  return {
    hot: Number(sorted[hotIdx].toFixed(3)),
    warm: Number(sorted[warmIdx].toFixed(3))
  }
}

// ============================================================
// Cosine similarity — for "similar past converters"
// ============================================================

function cosineSimilarity(a, b) {
  let dot = 0
  let na = 0
  let nb = 0
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i]
    na += a[i] * a[i]
    nb += b[i] * b[i]
  }
  if (na === 0 || nb === 0) return 0
  return dot / (Math.sqrt(na) * Math.sqrt(nb))
}

function findSimilarConverters(lead, allLeads, k = 3) {
  const leadVec = vectorize(lead)
  const converters = allLeads.filter(
    (l) => l.key !== lead.key && (l.status === 'converted' || l.status === 'qualified')
  )
  if (!converters.length) return []
  const scored = converters.map((c) => ({
    lead: c,
    similarity: cosineSimilarity(leadVec, vectorize(c))
  }))
  return scored
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, k)
    .map((s) => ({
      key: s.lead.key,
      name: s.lead.name || s.lead.phone,
      city: s.lead.city,
      budget: s.lead.budget,
      status: s.lead.status,
      similarity: Number(s.similarity.toFixed(3))
    }))
}

// ============================================================
// Aging & velocity
// ============================================================

function agingBucket(lead) {
  const d = lead.daysSinceInquiry ?? lead.lastRespDays ?? null
  if (d === null) return 'Unknown'
  if (d < 3) return 'Fresh'
  if (d < 7) return 'Active'
  if (d < 14) return 'Aging'
  if (d < 30) return 'Stale'
  return 'Dead'
}

// ============================================================
// Next-Best-Action engine
// ============================================================

function buildMessageTemplate(lead, action) {
  const name = lead.name || 'there'
  const city = lead.city || 'your preferred area'
  const budget = lead.budget || 'your budget range'
  const visitLine = lead.visitRequested
    ? 'Since you already visited, we can fast-track the booking formalities.'
    : `We'd love to host you at the project — I can block a 45-min slot this week.`

  switch (action) {
    case 'schedule_visit':
      return `Hi ${name}, this is from the sales team. We have 2 fresh ${budget} inventory matches in ${city}. ${visitLine} Which day works — Saturday or Sunday?`
    case 'rescue_silent':
      return `Hi ${name}, circling back — I noticed you went quiet on our earlier chat. Three new towers launched near ${city} this week and pricing is locked for 14 days. Can I send the pricing sheet on WhatsApp?`
    case 'nurture_warm':
      return `Hi ${name}, sharing the updated ${budget} price sheet and floor plans for ${city}. Happy to walk you through the EMI options whenever you're free.`
    case 'portal_callback':
      return `Hi ${name}, we saw you checking the listing. Want me to shortlist 3 options in ${city} that match ${budget}? I can send them in 10 mins.`
    case 'close_fast':
      return `Hi ${name}, token amount window closes this week for the A-block units. Want me to hold one for you tonight?`
    default:
      return `Hi ${name}, checking in to see if you'd like more details about ${city} inventory.`
  }
}

function nextBestAction(lead, silent) {
  if (lead.status === 'converted') {
    return {
      action: 'retention',
      label: 'Send thank-you + referral ask',
      urgency: 'Low',
      eta: 'Next week',
      channel: recommendedChannel(lead),
      message: `Hi ${lead.name || ''}, thanks for choosing us. Know anyone else looking in ${lead.city || 'the area'}? 2% referral bonus applies.`
    }
  }
  if (lead.status === 'lost') {
    return {
      action: 'long_nurture',
      label: 'Monthly nurture drip',
      urgency: 'Low',
      eta: 'Next month',
      channel: 'Email',
      message: `Hi ${lead.name || ''}, sharing our quarterly market update.`
    }
  }
  if (silent.isSilent) {
    return {
      action: 'rescue_silent',
      label: 'Rescue silent high-intent buyer',
      urgency: 'High',
      eta: 'Within 4 hours',
      channel: 'WhatsApp',
      message: buildMessageTemplate(lead, 'rescue_silent')
    }
  }
  if (
    lead.budget &&
    !lead.visitRequested &&
    lead.interactions >= 2
  ) {
    return {
      action: 'schedule_visit',
      label: 'Schedule site visit',
      urgency: 'High',
      eta: 'Today',
      channel: recommendedChannel(lead),
      message: buildMessageTemplate(lead, 'schedule_visit')
    }
  }
  if (
    lead.visitRequested &&
    (lead.status === 'qualified' || lead.status === 'in_progress')
  ) {
    return {
      action: 'close_fast',
      label: 'Push toward token / booking',
      urgency: 'High',
      eta: 'Today',
      channel: 'Call',
      message: buildMessageTemplate(lead, 'close_fast')
    }
  }
  if (lead.adClicks >= 3 && lead.interactions < 2) {
    return {
      action: 'portal_callback',
      label: 'Engage high-click dormant',
      urgency: 'Medium',
      eta: 'Today',
      channel: 'WhatsApp',
      message: buildMessageTemplate(lead, 'portal_callback')
    }
  }
  return {
    action: 'nurture_warm',
    label: 'Warm nurture with pricing sheet',
    urgency: 'Medium',
    eta: 'Tomorrow',
    channel: recommendedChannel(lead),
    message: buildMessageTemplate(lead, 'nurture_warm')
  }
}

// ============================================================
// Score-breakdown explainer (which features contributed most)
// ============================================================

function explainScore(lead, model) {
  const x = vectorize(lead)
  const contribs = FEATURE_NAMES.map((name, i) => ({
    feature: name,
    value: Number(x[i].toFixed(3)),
    weight: Number((model.w[i] || 0).toFixed(3)),
    contribution: Number((x[i] * (model.w[i] || 0)).toFixed(3))
  }))
  return contribs
    .sort((a, b) => Math.abs(b.contribution) - Math.abs(a.contribution))
    .slice(0, 6)
}

// Bayesian Belief Network for silent-buyer detection.
// Nodes: Budget, VisitRequested, Interactions → HighIntent
//        LastRespDays, Interactions → Silent
//        HighIntent & Silent → SilentHighBuyer
function bayesianSilentBuyer(lead) {
  const pHighIntentGivenBudget = { high: 0.75, mid: 0.55, low: 0.3, unknown: 0.4 }
  const pHighIntentGivenVisit = lead.visitRequested ? 0.85 : 0.35
  const pHighIntentGivenInteractions =
    lead.interactions >= 4 ? 0.8 : lead.interactions >= 2 ? 0.55 : 0.3

  const priors = [
    pHighIntentGivenBudget[lead.budgetBucket] ?? 0.4,
    pHighIntentGivenVisit,
    pHighIntentGivenInteractions
  ]
  const pHighIntent =
    priors.reduce((a, b) => a + b, 0) / priors.length

  const resp = lead.lastRespDays
  let pSilent
  if (resp === null) {
    pSilent = lead.interactions === 0 ? 0.7 : 0.4
  } else if (resp >= 14) pSilent = 0.85
  else if (resp >= 7) pSilent = 0.65
  else if (resp >= 3) pSilent = 0.35
  else pSilent = 0.1

  const pSilentHighBuyer = pHighIntent * pSilent
  const isSilent = pSilent >= 0.5 && pHighIntent >= 0.5
  return { pHighIntent, pSilent, pSilentHighBuyer, isSilent }
}

// Rule-based Seriousness Score — explainable, works without labeled data.
// Returns a 0–1 score plus the top contributing reasons.
function computeSeriousnessScore(lead) {
  const signals = []
  const add = (weight, label, condition) => {
    if (condition) signals.push({ weight, label })
  }

  const budgetCr = parseBudgetCrore(lead.budget)
  add(0.15, 'Explicit budget stated', !!lead.budget)
  add(0.10, 'High budget (≥ ₹1 Cr)', budgetCr !== null && budgetCr >= 1)
  add(0.05, 'Mid budget (₹50L – ₹1Cr)', budgetCr !== null && budgetCr >= 0.5 && budgetCr < 1)
  add(0.20, 'Site visit requested or completed', lead.visitRequested)
  add(0.15, 'Multi-channel presence', lead.channels.length >= 2)
  add(0.10, 'High interactions (≥ 4)', lead.interactions >= 4)
  add(0.05, 'Moderate interactions (≥ 2)', lead.interactions >= 2 && lead.interactions < 4)
  add(0.10, 'Recent response (≤ 7 days)', lead.lastRespDays !== null && lead.lastRespDays <= 7)
  add(0.05, 'Fresh inquiry (≤ 7 days)', lead.daysSinceInquiry !== null && lead.daysSinceInquiry <= 7)
  add(0.05, 'Strong ad engagement', lead.adClicks >= 3)
  add(0.05, 'Preferred location stated', !!lead.city)
  add(0.10, 'Explicit interest flagged on WhatsApp', lead.interestedFlag)
  add(0.05, 'Assigned to sales executive', !!lead.salesExec)
  add(0.05, 'Routed through broker', !!lead.brokerName)

  const raw = signals.reduce((acc, s) => acc + s.weight, 0)
  const score = Math.min(1, raw)
  const reasons = signals
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 4)
    .map((s) => s.label)

  let tier
  if (score >= 0.65) tier = 'Hot'
  else if (score >= 0.4) tier = 'Warm'
  else tier = 'Cold'

  return { score, reasons, tier }
}

function priorityFor(lead, nb, bb, seriousness) {
  if (lead.status === 'converted' || lead.status === 'lost') return 'Monitor'
  if (seriousness.tier === 'Hot' || bb.isSilent) return 'Now'
  if (seriousness.tier === 'Warm' || nb.intent === 'high') return 'Soon'
  return 'Monitor'
}

function recommendedChannel(lead) {
  if (lead.channels.includes('whatsapp')) return 'WhatsApp'
  if (lead.phone) return 'Call'
  if (lead.email) return 'Email'
  return 'WhatsApp'
}

function nextFollowUp(priority, lead) {
  if (priority === 'Now') return 'Today'
  if (priority === 'Soon') return 'Tomorrow'
  if (lead.daysSinceInquiry !== null && lead.daysSinceInquiry > 30) return 'In 7 days'
  return 'In 5 days'
}

function enrichLeads(unified) {
  const lr = trainIntentModel(unified)
  const mlWeight = Math.min(0.7, lr.labeledCount / 40)
  const confidence = leadConfidence(lr.labeledCount)

  // First pass: compute raw blended scores to enable dynamic threshold calibration
  const firstPass = unified.map((lead) => {
    const prediction = lr.predict(lead)
    const silent = bayesianSilentBuyer(lead)
    const seriousness = computeSeriousnessScore(lead)
    const semantic = chatSemanticScore(lead)
    // Blend: ML + seriousness + semantic adjustment
    const base =
      mlWeight * prediction.score + (1 - mlWeight) * seriousness.score
    const blendedScore = Math.max(0, Math.min(1, base + semantic.score))
    return { lead, prediction, silent, seriousness, semantic, blendedScore }
  })

  // Calibrate thresholds — allow tenant override from state, else use defaults
  const cfg = APP_STATE.tenantConfig || {}
  let hotThreshold = cfg.hotThreshold ?? 0.65
  let warmThreshold = cfg.warmThreshold ?? 0.4
  if (cfg.autoCalibrate !== false) {
    const cal = calibrateThresholds(firstPass.map((p) => p.blendedScore), cfg.targetHotPct || 0.25, cfg.targetWarmPct || 0.5)
    hotThreshold = cal.hot
    warmThreshold = cal.warm
  }

  const enriched = firstPass.map(({ lead, prediction, silent, seriousness, semantic, blendedScore }) => {
    const intent =
      blendedScore >= hotThreshold ? 'high' : blendedScore >= warmThreshold ? 'medium' : 'low'
    const tier = intent === 'high' ? 'Hot' : intent === 'medium' ? 'Warm' : 'Cold'
    const priority = priorityFor(lead, { intent }, silent, { ...seriousness, tier })
    const aging = agingBucket(lead)
    const staleHot = tier === 'Hot' && (aging === 'Aging' || aging === 'Stale')
    const competitors = detectCompetitors(lead)
    const velocity = computeVelocity(lead, blendedScore)
    return {
      ...lead,
      intent,
      tier,
      score: blendedScore,
      mlScore: prediction.score,
      semantic,
      seriousness,
      silent: silent.isSilent,
      silentProbability: silent.pSilentHighBuyer,
      priority,
      recommendedChannel: recommendedChannel(lead),
      nextFollowUp: nextFollowUp(priority, lead),
      aging,
      staleHot,
      featureVector: prediction.vector,
      nextBestAction: nextBestAction(lead, silent),
      competitors,
      velocity,
      confidence
    }
  })

  // Attach similar past converters (computed after enrichment so vectors exist)
  for (const lead of enriched) {
    lead.similarConverters = findSimilarConverters(lead, unified, 3)
  }

  // Save score snapshots for next velocity calculation
  saveSnapshots(enriched)

  return {
    enriched,
    model: lr.model,
    modelAccuracy: lr.cvAccuracy,
    holdout: lr.holdout,
    labeledCount: lr.labeledCount,
    positiveCount: lr.positiveCount,
    negativeCount: lr.negativeCount,
    featureImportance: lr.featureImportance,
    mlWeight,
    thresholds: { hot: hotThreshold, warm: warmThreshold },
    confidence
  }
}

function buildOverview(enriched) {
  const total = enriched.length
  const highIntent = enriched.filter((l) => l.intent === 'high').length
  const mediumIntent = enriched.filter((l) => l.intent === 'medium').length
  const lowIntent = enriched.filter((l) => l.intent === 'low').length
  const silentBuyers = enriched.filter((l) => l.silent).length
  const converted = enriched.filter((l) => l.status === 'converted').length

  const pipelineValue = enriched.reduce((acc, lead) => {
    const cr = parseBudgetCrore(lead.budget) || 0
    return acc + cr * lead.score
  }, 0)

  const predictedConversions = Math.round(
    enriched.reduce((acc, lead) => acc + lead.score, 0)
  )

  const funnel = {
    captured: total,
    qualified: highIntent + mediumIntent,
    siteVisits: enriched.filter((l) => l.visitRequested).length,
    booked: converted
  }

  const sourceMap = new Map()
  for (const lead of enriched) {
    const src = lead.source || 'Unknown'
    if (!sourceMap.has(src)) sourceMap.set(src, { source: src, captured: 0, converted: 0 })
    const entry = sourceMap.get(src)
    entry.captured += 1
    if (lead.status === 'converted') entry.converted += 1
  }
  const leadSources = Array.from(sourceMap.values()).map((s, i) => ({
    id: i + 1,
    source: s.source,
    channel: s.source,
    captured: s.captured,
    converted: s.converted
  }))

  const execMap = new Map()
  for (const lead of enriched) {
    const exec = lead.salesExec || 'Unassigned'
    if (!execMap.has(exec)) execMap.set(exec, { name: exec, high: 0, medium: 0, silent: 0, value: 0 })
    const e = execMap.get(exec)
    if (lead.intent === 'high') e.high += 1
    if (lead.intent === 'medium') e.medium += 1
    if (lead.silent) e.silent += 1
    e.value += (parseBudgetCrore(lead.budget) || 0) * lead.score
  }
  const salesPipeline = Array.from(execMap.values()).map((e) => ({
    ...e,
    value: Number(e.value.toFixed(2))
  }))

  return {
    totals: { total, highIntent, mediumIntent, lowIntent, silentBuyers, converted },
    predictedConversions,
    pipelineValue: Number(pipelineValue.toFixed(2)),
    funnel,
    leadSources,
    salesPipeline
  }
}

function buildForecast(enriched) {
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May']
  const avgScore =
    enriched.length ? enriched.reduce((a, b) => a + b.score, 0) / enriched.length : 0.5
  const baseRevenue = enriched.reduce((acc, lead) => {
    const cr = parseBudgetCrore(lead.budget) || 0
    return acc + cr * lead.score
  }, 0) / Math.max(months.length, 1)

  return months.map((month, i) => ({
    month,
    probability: Math.min(0.95, avgScore + i * 0.04),
    expectedRevenue: Number((baseRevenue * (1 + i * 0.12)).toFixed(2))
  }))
}

function buildCampaignRoi(enriched) {
  const costs = APP_STATE.costConfig || {}
  const map = new Map()
  for (const lead of enriched) {
    const campaign = lead.campaignName || lead.platform || lead.source || 'Direct'
    if (!map.has(campaign)) {
      map.set(campaign, {
        campaign,
        leads: 0,
        converted: 0,
        qualified: 0,
        adClicks: 0,
        weightedRevenue: 0,
        totalBudget: 0
      })
    }
    const row = map.get(campaign)
    row.leads += 1
    if (lead.status === 'converted') row.converted += 1
    if (lead.status === 'qualified') row.qualified += 1
    row.adClicks += lead.adClicks || 0
    const cr = parseBudgetCrore(lead.budget) || 0
    row.weightedRevenue += cr * lead.score
    row.totalBudget += cr
  }
  return Array.from(map.values())
    .map((row) => {
      const cost = Number(costs[row.campaign] || 0)
      const expectedRevenueCr = Number(row.weightedRevenue.toFixed(2))
      const expectedRevenueRs = expectedRevenueCr * 1_00_00_000
      const roi = cost > 0 ? Number(((expectedRevenueRs - cost) / cost).toFixed(2)) : null
      return {
        ...row,
        conversionRate: row.leads ? Number(((row.converted / row.leads) * 100).toFixed(1)) : 0,
        avgTicketSize: row.leads ? Number((row.totalBudget / row.leads).toFixed(2)) : 0,
        expectedRevenue: expectedRevenueCr,
        cost,
        costPerLead: row.leads && cost ? Number((cost / row.leads).toFixed(0)) : null,
        roi
      }
    })
    .sort((a, b) => b.expectedRevenue - a.expectedRevenue)
}

// Project-level pipeline — groups by property / project name
function buildProjectPipeline(enriched) {
  const map = new Map()
  for (const lead of enriched) {
    const project = (lead.property || '').trim() || 'Unspecified'
    if (!map.has(project)) {
      map.set(project, { project, leads: 0, hot: 0, warm: 0, cold: 0, silent: 0, converted: 0, pipelineValue: 0, topCity: new Map() })
    }
    const row = map.get(project)
    row.leads += 1
    if (lead.tier === 'Hot') row.hot += 1
    if (lead.tier === 'Warm') row.warm += 1
    if (lead.tier === 'Cold') row.cold += 1
    if (lead.silent) row.silent += 1
    if (lead.status === 'converted') row.converted += 1
    row.pipelineValue += (parseBudgetCrore(lead.budget) || 0) * lead.score
    const c = lead.city || 'Unknown'
    row.topCity.set(c, (row.topCity.get(c) || 0) + 1)
  }
  return Array.from(map.values())
    .map((r) => ({
      project: r.project,
      leads: r.leads,
      hot: r.hot,
      warm: r.warm,
      cold: r.cold,
      silent: r.silent,
      converted: r.converted,
      pipelineValue: Number(r.pipelineValue.toFixed(2)),
      topCity: [...r.topCity.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || 'Unknown'
    }))
    .sort((a, b) => b.pipelineValue - a.pipelineValue)
}

// Response-time SLA — uses days_since_inquiry as a proxy since we don't capture
// a proper "first_response_at". Target: <1 day ideal, <3 within SLA, >3 breach.
function buildResponseSla(enriched) {
  const execMap = new Map()
  for (const lead of enriched) {
    const exec = lead.salesExec || 'Unassigned'
    if (!execMap.has(exec)) {
      execMap.set(exec, { exec, leads: 0, ideal: 0, within: 0, breached: 0, unknown: 0, sumDays: 0, countedDays: 0 })
    }
    const row = execMap.get(exec)
    row.leads += 1
    const d = lead.daysSinceInquiry
    if (d === null || d === undefined) {
      row.unknown += 1
    } else {
      row.sumDays += d
      row.countedDays += 1
      if (d <= 1) row.ideal += 1
      else if (d <= 3) row.within += 1
      else row.breached += 1
    }
  }
  const result = Array.from(execMap.values()).map((r) => ({
    exec: r.exec,
    leads: r.leads,
    ideal: r.ideal,
    within: r.within,
    breached: r.breached,
    unknown: r.unknown,
    avgDays: r.countedDays ? Number((r.sumDays / r.countedDays).toFixed(1)) : null,
    breachPct: r.leads ? Number(((r.breached / r.leads) * 100).toFixed(1)) : 0
  }))
  const totals = {
    leads: enriched.length,
    ideal: result.reduce((a, r) => a + r.ideal, 0),
    within: result.reduce((a, r) => a + r.within, 0),
    breached: result.reduce((a, r) => a + r.breached, 0)
  }
  totals.idealPct = totals.leads ? Number(((totals.ideal / totals.leads) * 100).toFixed(1)) : 0
  totals.breachPct = totals.leads ? Number(((totals.breached / totals.leads) * 100).toFixed(1)) : 0
  return { byExec: result.sort((a, b) => a.breachPct - b.breachPct), totals }
}

// Daily digest — one paragraph summary for the owner's inbox
function buildDigest(enriched, overview) {
  const hot = enriched.filter((l) => l.tier === 'Hot').length
  const silent = enriched.filter((l) => l.silent).length
  const staleHot = enriched.filter((l) => l.staleHot).length
  const accelerating = enriched.filter((l) => l.velocity && l.velocity.trend === 'accelerating').length
  const topCampaign = overview.leadSources.reduce((best, s) => {
    const rate = s.captured ? s.converted / s.captured : 0
    const bestRate = best && best.captured ? best.converted / best.captured : 0
    return rate > bestRate ? s : best
  }, null)

  const headlines = []
  if (hot) headlines.push(`${hot} Hot leads active today`)
  if (silent) headlines.push(`${silent} silent buyer${silent === 1 ? '' : 's'} need re-engagement`)
  if (staleHot) headlines.push(`${staleHot} Hot lead${staleHot === 1 ? '' : 's'} aging past 7 days`)
  if (accelerating) headlines.push(`${accelerating} lead${accelerating === 1 ? '' : 's'} showing rising intent`)
  if (topCampaign) headlines.push(`Best source: ${topCampaign.source}`)

  return {
    generatedAt: new Date().toISOString(),
    headlines,
    keyMetrics: {
      pipelineValueCr: overview.pipelineValue,
      predictedConversions: overview.predictedConversions,
      hot,
      silent,
      staleHot,
      accelerating
    }
  }
}

// Smart assignment — given unassigned Hot leads, suggest a rep based on load + past conversion
function buildAssignmentSuggestions(enriched) {
  const unassigned = enriched.filter((l) => !l.salesExec && l.tier === 'Hot' && l.status !== 'converted' && l.status !== 'lost')
  const execStats = new Map()
  for (const lead of enriched) {
    const exec = lead.salesExec
    if (!exec) continue
    if (!execStats.has(exec)) execStats.set(exec, { exec, activeLeads: 0, converted: 0, total: 0, avgBudget: 0, sumBudget: 0 })
    const s = execStats.get(exec)
    s.total += 1
    if (lead.status === 'converted') s.converted += 1
    if (lead.status !== 'converted' && lead.status !== 'lost') s.activeLeads += 1
    const cr = parseBudgetCrore(lead.budget) || 0
    s.sumBudget += cr
  }
  const execs = Array.from(execStats.values()).map((s) => ({
    exec: s.exec,
    activeLeads: s.activeLeads,
    conversionRate: s.total ? Number(((s.converted / s.total) * 100).toFixed(1)) : 0,
    avgBudget: s.total ? Number((s.sumBudget / s.total).toFixed(2)) : 0
  }))

  const suggestions = unassigned.map((lead) => {
    if (!execs.length) return { leadKey: lead.key, leadName: lead.name || lead.phone, suggestedExec: null, reason: 'No sales executives assigned in any sheet yet' }
    // Round-robin favoring execs with lowest active load, tie-break by conversion rate
    const ranked = [...execs].sort((a, b) => {
      if (a.activeLeads !== b.activeLeads) return a.activeLeads - b.activeLeads
      return b.conversionRate - a.conversionRate
    })
    const choice = ranked[0]
    return {
      leadKey: lead.key,
      leadName: lead.name || lead.phone,
      budget: lead.budget,
      suggestedExec: choice.exec,
      reason: `Lightest current load (${choice.activeLeads} active), ${choice.conversionRate}% historical conversion`
    }
  })
  return { suggestions: suggestions.slice(0, 20), execs }
}

function buildTodayInbox(enriched) {
  const open = enriched.filter(
    (l) => l.status !== 'converted' && l.status !== 'lost'
  )
  const urgent = open
    .filter((l) => l.nextBestAction.urgency === 'High' && !l.silent)
    .sort((a, b) => b.score - a.score)
    .slice(0, 6)
  const rescue = open
    .filter((l) => l.silent || l.staleHot)
    .sort((a, b) => b.score - a.score)
    .slice(0, 6)
  const warm = open
    .filter(
      (l) =>
        l.tier === 'Warm' &&
        l.nextBestAction.urgency === 'Medium' &&
        !l.silent
    )
    .sort((a, b) => b.score - a.score)
    .slice(0, 6)
  const highValue = open
    .sort(
      (a, b) =>
        (parseBudgetCrore(b.budget) || 0) * b.score -
        (parseBudgetCrore(a.budget) || 0) * a.score
    )
    .slice(0, 4)

  const slim = (l) => ({
    key: l.key,
    name: l.name || l.phone,
    phone: l.phone,
    budget: l.budget,
    city: l.city,
    intent: l.intent,
    tier: l.tier,
    score: Number(l.score.toFixed(3)),
    channels: l.channels,
    aging: l.aging,
    silent: l.silent,
    staleHot: l.staleHot,
    nextBestAction: l.nextBestAction
  })

  return {
    urgent: urgent.map(slim),
    rescue: rescue.map(slim),
    warm: warm.map(slim),
    highValue: highValue.map(slim),
    summary: {
      urgentCount: urgent.length,
      rescueCount: rescue.length,
      warmCount: warm.length,
      silentCount: open.filter((l) => l.silent).length,
      staleHotCount: open.filter((l) => l.staleHot).length
    }
  }
}

const SHEET_HEALTH_CACHE = new Map()

async function checkSheetHealth(name, spreadsheetId, range) {
  const start = Date.now()
  try {
    const result = await sheets.spreadsheets.values.get({ spreadsheetId, range })
    const rows = result.data.values || []
    const rowCount = Math.max(0, rows.length - 1)
    const latency = Date.now() - start
    const health = {
      name,
      spreadsheetId,
      status: 'Synced',
      rowCount,
      latency,
      lastSync: new Date().toISOString(),
      error: null
    }
    SHEET_HEALTH_CACHE.set(name, health)
    return health
  } catch (err) {
    const health = {
      name,
      spreadsheetId,
      status: 'Error',
      rowCount: SHEET_HEALTH_CACHE.get(name)?.rowCount ?? 0,
      latency: Date.now() - start,
      lastSync: SHEET_HEALTH_CACHE.get(name)?.lastSync || null,
      error: err.message
    }
    SHEET_HEALTH_CACHE.set(name, health)
    return health
  }
}

async function computeAll() {
  const sources = await loadAllSources()
  const sourceErrors = sources._sourceErrors || {}
  const cached = !!sources._cached
  const unified = unifyLeads(sources)
  const enrichmentResult = enrichLeads(unified)
  const { enriched } = enrichmentResult
  const overview = buildOverview(enriched)
  const forecast = buildForecast(enriched)
  const campaignRoi = buildCampaignRoi(enriched)
  const today = buildTodayInbox(enriched)
  const projects = buildProjectPipeline(enriched)
  const sla = buildResponseSla(enriched)
  const digest = buildDigest(enriched, overview)
  const assignment = buildAssignmentSuggestions(enriched)
  return {
    enriched,
    overview,
    forecast,
    campaignRoi,
    today,
    projects,
    sla,
    digest,
    assignment,
    sourceErrors,
    cached,
    ...enrichmentResult
  }
}

function serializeLead(l) {
  return {
    ...l,
    score: Number(l.score.toFixed(3)),
    mlScore: Number(l.mlScore.toFixed(3)),
    silentProbability: Number(l.silentProbability.toFixed(3)),
    seriousness: {
      score: Number(l.seriousness.score.toFixed(3)),
      tier: l.seriousness.tier,
      reasons: l.seriousness.reasons
    },
    semantic: l.semantic
      ? { score: Number(l.semantic.score.toFixed(3)), matches: l.semantic.matches }
      : { score: 0, matches: [] },
    competitors: l.competitors || [],
    velocity: l.velocity || { delta: 0, trend: 'new' },
    confidence: l.confidence || 'Insufficient data',
    featureVector: l.featureVector.map((v) => Number(v.toFixed(3)))
  }
}

app.get('/api/unified-leads', async (req, res) => {
  try {
    const { enriched, modelAccuracy, labeledCount, mlWeight, positiveCount, negativeCount } =
      await computeAll()
    res.json({
      leads: enriched.map(serializeLead),
      modelAccuracy: Number(modelAccuracy.toFixed(3)),
      labeledCount,
      positiveCount,
      negativeCount,
      mlWeight: Number(mlWeight.toFixed(2))
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to build unified leads' })
  }
})

app.get('/api/overview', async (req, res) => {
  try {
    const { overview, modelAccuracy, holdout, labeledCount, today, thresholds, confidence, sourceErrors, digest } =
      await computeAll()
    res.json({
      ...overview,
      modelAccuracy: Number((modelAccuracy ?? 0).toFixed(3)),
      holdout,
      labeledCount,
      todaySummary: today.summary,
      thresholds,
      confidence,
      sourceErrors: sourceErrors || {},
      headlines: digest?.headlines || []
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: err.message })
  }
})

app.get('/api/today', async (req, res) => {
  try {
    const { today, modelAccuracy, labeledCount } = await computeAll()
    res.json({ ...today, modelAccuracy, labeledCount })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to build today inbox' })
  }
})

app.get('/api/lead/:key', async (req, res) => {
  try {
    const { enriched, model } = await computeAll()
    const lead = enriched.find((l) => l.key === req.params.key)
    if (!lead) return res.status(404).json({ error: 'Lead not found' })
    const contributions = explainScore(lead, model)
    res.json({ lead: serializeLead(lead), contributions, featureNames: FEATURE_NAMES })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to load lead' })
  }
})

app.get('/api/model', async (req, res) => {
  try {
    const { model, modelAccuracy, labeledCount, featureImportance, positiveCount, negativeCount } =
      await computeAll()
    res.json({
      accuracy: Number(modelAccuracy.toFixed(3)),
      labeledCount,
      positiveCount,
      negativeCount,
      featureNames: FEATURE_NAMES,
      featureImportance,
      bias: Number(model.b.toFixed(4))
    })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to build model metadata' })
  }
})

app.get('/api/campaigns', async (req, res) => {
  try {
    const { campaignRoi } = await computeAll()
    res.json({ campaigns: campaignRoi })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to build campaign ROI' })
  }
})

app.get('/api/sources/status', async (req, res) => {
  try {
    const checks = await Promise.all([
      checkSheetHealth('Website leads', SHEETS.website, 'A1:J'),
      checkSheetHealth('Offline walk-ins', SHEETS.offline, 'A1:J'),
      checkSheetHealth('Marketing campaigns', SHEETS.marketing, 'A1:J'),
      checkSheetHealth('Broker referrals', SHEETS.broker, 'A1:J'),
      checkSheetHealth('WhatsApp chatbot', SHEETS.whatsapp, 'A1:K')
    ])
    res.json({ sources: checks })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to check source status' })
  }
})

app.get('/api/automation-queue', async (req, res) => {
  try {
    const { enriched } = await computeAll()
    const order = { Now: 0, Soon: 1, Monitor: 2 }
    const queue = enriched
      .filter((l) => l.status !== 'converted' && l.status !== 'lost')
      .sort((a, b) => {
        const p = order[a.priority] - order[b.priority]
        if (p !== 0) return p
        return b.score - a.score
      })
      .slice(0, 25)
      .map((l, i) => ({
        id: i + 1,
        leadName: l.name || l.phone || 'Unknown',
        priority: l.priority,
        nextFollowUp: l.nextFollowUp,
        channel: l.recommendedChannel,
        mode: l.priority === 'Now' ? 'Workflow' : 'Manual',
        intent: l.intent,
        silent: l.silent,
        score: Number(l.score.toFixed(3))
      }))
    res.json({ queue })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to build automation queue' })
  }
})

app.get('/api/forecast', async (req, res) => {
  try {
    const { forecast } = await computeAll()
    res.json({ forecast })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Failed to build forecast' })
  }
})

// ============================================================
// New endpoints: refresh, projects, sla, digest, assignment,
// campaigns/costs, feedback, audit, writeback, llm, auth
// ============================================================

app.post('/api/refresh', (req, res) => {
  invalidateCache()
  res.json({ ok: true })
})

app.get('/api/projects', async (req, res) => {
  try {
    const { projects } = await computeAll()
    res.json({ projects })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.get('/api/sla', async (req, res) => {
  try {
    const { sla } = await computeAll()
    res.json(sla)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.get('/api/digest', async (req, res) => {
  try {
    const { digest } = await computeAll()
    res.json(digest)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

app.get('/api/assignment-suggestions', async (req, res) => {
  try {
    const { assignment } = await computeAll()
    res.json(assignment)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Cost input for ROI: GET reads, POST saves
app.get('/api/costs', (req, res) => {
  res.json({ costs: APP_STATE.costConfig || {} })
})

app.post('/api/costs', (req, res) => {
  const input = req.body?.costs || {}
  APP_STATE.costConfig = { ...(APP_STATE.costConfig || {}), ...input }
  APP_STATE.auditLog = APP_STATE.auditLog || []
  APP_STATE.auditLog.push({ at: new Date().toISOString(), event: 'costs.updated', payload: input })
  saveState(APP_STATE)
  invalidateCache()
  res.json({ ok: true, costs: APP_STATE.costConfig })
})

// Tenant config (thresholds, calibration)
app.get('/api/config', (req, res) => {
  res.json({ tenantConfig: APP_STATE.tenantConfig })
})

app.post('/api/config', (req, res) => {
  APP_STATE.tenantConfig = { ...(APP_STATE.tenantConfig || {}), ...(req.body || {}) }
  APP_STATE.auditLog.push({ at: new Date().toISOString(), event: 'config.updated', payload: req.body })
  saveState(APP_STATE)
  invalidateCache()
  res.json({ ok: true, tenantConfig: APP_STATE.tenantConfig })
})

// Feedback: rep marks lead tried/won/lost/no-response
app.post('/api/feedback', (req, res) => {
  const { leadKey, outcome, note, userEmail } = req.body || {}
  if (!leadKey || !outcome) return res.status(400).json({ error: 'leadKey and outcome required' })
  const allowed = ['won', 'lost', 'tried_worked', 'tried_no_response', 'reassign']
  if (!allowed.includes(outcome)) {
    return res.status(400).json({ error: `outcome must be one of ${allowed.join(', ')}` })
  }
  APP_STATE.feedback = APP_STATE.feedback || {}
  if (!APP_STATE.feedback[leadKey]) APP_STATE.feedback[leadKey] = []
  const entry = { at: new Date().toISOString(), outcome, note: note || '', user: userEmail || 'anonymous' }
  APP_STATE.feedback[leadKey].push(entry)
  APP_STATE.auditLog.push({ at: entry.at, event: 'feedback.added', payload: { leadKey, outcome, user: entry.user } })
  saveState(APP_STATE)
  res.json({ ok: true, leadKey, entry })
})

app.get('/api/feedback/:key', (req, res) => {
  const entries = (APP_STATE.feedback || {})[req.params.key] || []
  res.json({ leadKey: req.params.key, entries })
})

app.get('/api/audit', (req, res) => {
  const limit = Math.min(500, Number(req.query.limit) || 100)
  const log = (APP_STATE.auditLog || []).slice(-limit).reverse()
  res.json({ log })
})

// Write back AI scores to sheets — adds AI_Score / AI_Tier / AI_Action columns if missing.
// Only scans for rows that already exist in the source sheets (matches by Lead ID).
app.post('/api/writeback', async (req, res) => {
  try {
    const { enriched } = await computeAll()
    const results = {}
    const scoreByLeadId = new Map()
    for (const lead of enriched) {
      if (lead.id) scoreByLeadId.set(String(lead.id), lead)
    }

    const sheetTargets = [
      { name: 'website', id: SHEETS.website },
      { name: 'offline', id: SHEETS.offline },
      { name: 'marketing', id: SHEETS.marketing },
      { name: 'broker', id: SHEETS.broker },
      { name: 'whatsapp', id: SHEETS.whatsapp }
    ]

    for (const t of sheetTargets) {
      try {
        const all = await sheets.spreadsheets.values.get({ spreadsheetId: t.id, range: 'A1:ZZ' })
        const rows = all.data.values || []
        if (rows.length < 2) {
          results[t.name] = { status: 'skipped', reason: 'no data rows' }
          continue
        }
        const header = rows[0].map((h) => String(h).trim())
        const normalized = header.map((h) => h.toLowerCase())
        const idIdx = normalized.findIndex((h) => h === 'lead id' || h === 'id')
        if (idIdx < 0) {
          results[t.name] = { status: 'skipped', reason: 'no Lead ID column' }
          continue
        }

        // Ensure AI_Score, AI_Tier, AI_Action columns exist
        const ensureCol = (label) => {
          const idx = normalized.indexOf(label.toLowerCase())
          if (idx >= 0) return idx
          header.push(label)
          normalized.push(label.toLowerCase())
          return header.length - 1
        }
        const scoreCol = ensureCol('AI_Score')
        const tierCol = ensureCol('AI_Tier')
        const actionCol = ensureCol('AI_Action')

        // If we added new columns, write the header row first
        if (header.length !== rows[0].length) {
          await sheets.spreadsheets.values.update({
            spreadsheetId: t.id,
            range: `A1:${colLetter(header.length - 1)}1`,
            valueInputOption: 'RAW',
            requestBody: { values: [header] }
          })
        }

        const updates = []
        let matched = 0
        for (let r = 1; r < rows.length; r++) {
          const leadId = String(rows[r][idIdx] || '').trim()
          if (!leadId) continue
          const lead = scoreByLeadId.get(leadId)
          if (!lead) continue
          matched += 1
          const rowNum = r + 1
          updates.push({
            range: `${colLetter(scoreCol)}${rowNum}`,
            values: [[Math.round(lead.score * 100) + '%']]
          })
          updates.push({
            range: `${colLetter(tierCol)}${rowNum}`,
            values: [[lead.tier]]
          })
          updates.push({
            range: `${colLetter(actionCol)}${rowNum}`,
            values: [[lead.nextBestAction?.label || '']]
          })
        }

        if (updates.length) {
          await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: t.id,
            requestBody: { valueInputOption: 'USER_ENTERED', data: updates }
          })
        }
        results[t.name] = { status: 'ok', matched, updatedCells: updates.length }
      } catch (err) {
        results[t.name] = { status: 'error', error: err.message }
      }
    }

    APP_STATE.auditLog.push({ at: new Date().toISOString(), event: 'writeback.completed', payload: { results } })
    saveState(APP_STATE)
    invalidateCache()
    res.json({ ok: true, results })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Column-letter helper for sheet ranges
function colLetter(n) {
  let s = ''
  let v = n + 1
  while (v > 0) {
    const m = (v - 1) % 26
    s = String.fromCharCode(65 + m) + s
    v = Math.floor((v - 1) / 26)
  }
  return s
}

// LLM-personalized message — supports Groq (recommended, fast + free tier)
// or Anthropic. Set GROQ_API_KEY or ANTHROPIC_API_KEY in env to activate.
// Optional: GROQ_MODEL (default: llama-3.3-70b-versatile)
app.post('/api/llm-message', async (req, res) => {
  const { leadKey, intent } = req.body || {}
  if (!leadKey) return res.status(400).json({ error: 'leadKey required' })
  try {
    const { enriched } = await computeAll()
    const lead = enriched.find((l) => l.key === leadKey)
    if (!lead) return res.status(404).json({ error: 'lead not found' })

    const groqKey = process.env.GROQ_API_KEY
    const anthropicKey = process.env.ANTHROPIC_API_KEY

    if (!groqKey && !anthropicKey) {
      return res.json({
        source: 'template',
        message: lead.nextBestAction.message,
        note: 'Set GROQ_API_KEY (recommended) or ANTHROPIC_API_KEY env variable to enable AI-personalized messages.'
      })
    }

    const prompt = `You are an Indian real-estate sales assistant. Draft a short (60-80 words), warm WhatsApp message in English to a prospective buyer with this profile:
Name: ${lead.name || 'there'}
City: ${lead.city || 'unspecified'}
Budget: ${lead.budget || 'unspecified'}
Property type: ${lead.property || 'unspecified'}
Current intent: ${intent || lead.tier}
Competitors considered: ${(lead.competitors || []).join(', ') || 'none mentioned'}
Chat summary so far: ${lead.chatSummary || 'none'}
Last rep action: ${lead.nextBestAction.label}

Write only the message body — no salutation in brackets, no signature, no "Hi {name}" placeholder. Start with "Hi ${lead.name || 'there'}". Use Indian real-estate tone. Reference concrete signals from the profile.`

    // Try Groq first (faster + generous free tier)
    if (groqKey) {
      const model = process.env.GROQ_MODEL || 'llama-3.3-70b-versatile'
      try {
        const resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            authorization: `Bearer ${groqKey}`
          },
          body: JSON.stringify({
            model,
            messages: [{ role: 'user', content: prompt }],
            max_tokens: 300,
            temperature: 0.7
          })
        })
        if (resp.ok) {
          const data = await resp.json()
          const text = (data.choices?.[0]?.message?.content || '').trim()
          return res.json({ source: 'groq', model, message: text || lead.nextBestAction.message })
        }
        const errTxt = await resp.text()
        console.warn('Groq error, trying fallback:', errTxt.slice(0, 200))
      } catch (err) {
        console.warn('Groq fetch error:', err.message)
      }
    }

    // Fallback: Anthropic
    if (anthropicKey) {
      const resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': anthropicKey,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify({
          model: process.env.ANTHROPIC_MODEL || 'claude-sonnet-4-5',
          max_tokens: 300,
          messages: [{ role: 'user', content: prompt }]
        })
      })
      if (!resp.ok) {
        const txt = await resp.text()
        return res.status(502).json({
          source: 'template',
          message: lead.nextBestAction.message,
          error: `LLM error: ${txt.slice(0, 200)}`
        })
      }
      const data = await resp.json()
      const text = (data.content?.[0]?.text || '').trim()
      return res.json({ source: 'anthropic', message: text || lead.nextBestAction.message })
    }

    // Should not reach here — either key was present above
    return res.json({
      source: 'template',
      message: lead.nextBestAction.message,
      note: 'All LLM providers failed. Falling back to template.'
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Voice-memo transcript receiver (browser sends transcript from Web Speech API)
app.post('/api/voice-memo', (req, res) => {
  const { leadKey, transcript, userEmail } = req.body || {}
  if (!leadKey || !transcript) return res.status(400).json({ error: 'leadKey and transcript required' })
  APP_STATE.feedback = APP_STATE.feedback || {}
  if (!APP_STATE.feedback[leadKey]) APP_STATE.feedback[leadKey] = []
  const entry = {
    at: new Date().toISOString(),
    outcome: 'voice_memo',
    note: transcript,
    user: userEmail || 'anonymous'
  }
  APP_STATE.feedback[leadKey].push(entry)
  APP_STATE.auditLog.push({ at: entry.at, event: 'voice_memo.saved', payload: { leadKey, chars: transcript.length, user: entry.user } })
  saveState(APP_STATE)
  res.json({ ok: true, saved: entry })
})

// Session-based auth (stored in app-state.json)
function genSessionId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36) + Math.random().toString(36).slice(2)
}

app.post('/api/auth/login', (req, res) => {
  const { email, password } = req.body || {}
  if (!email || !password) return res.status(400).json({ error: 'email and password required' })
  const user = (APP_STATE.users || []).find(
    (u) => u.email.toLowerCase() === String(email).toLowerCase() && u.password === password
  )
  if (!user) return res.status(401).json({ error: 'invalid credentials' })
  const sessionId = genSessionId()
  APP_STATE.sessions = APP_STATE.sessions || {}
  APP_STATE.sessions[sessionId] = { email: user.email, role: user.role, name: user.name, at: new Date().toISOString() }
  APP_STATE.auditLog.push({ at: new Date().toISOString(), event: 'auth.login', payload: { email: user.email } })
  saveState(APP_STATE)
  res.json({ ok: true, sessionId, user: { email: user.email, role: user.role, name: user.name } })
})

app.post('/api/auth/register', (req, res) => {
  const { email, password, role, name } = req.body || {}
  if (!email || !password) return res.status(400).json({ error: 'email and password required' })
  APP_STATE.users = APP_STATE.users || []
  if (APP_STATE.users.find((u) => u.email.toLowerCase() === String(email).toLowerCase())) {
    return res.status(409).json({ error: 'user already exists' })
  }
  APP_STATE.users.push({ email, password, role: role === 'sales' ? 'sales' : 'owner', name: name || email })
  APP_STATE.auditLog.push({ at: new Date().toISOString(), event: 'auth.register', payload: { email } })
  saveState(APP_STATE)
  res.json({ ok: true })
})

app.post('/api/auth/logout', (req, res) => {
  const { sessionId } = req.body || {}
  if (sessionId && APP_STATE.sessions) {
    delete APP_STATE.sessions[sessionId]
    saveState(APP_STATE)
  }
  res.json({ ok: true })
})

app.get('/api/auth/session', (req, res) => {
  const sessionId = req.query.sessionId
  const sess = sessionId && APP_STATE.sessions ? APP_STATE.sessions[sessionId] : null
  if (!sess) return res.status(401).json({ error: 'no session' })
  res.json({ user: sess })
})

// Lightweight health + source-error passthrough
app.get('/api/health', async (req, res) => {
  try {
    const { sourceErrors, cached } = await computeAll()
    const errored = Object.keys(sourceErrors || {}).length
    res.json({
      ok: errored === 0,
      cached,
      sourceErrors
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message })
  }
})

// Update overview to include sourceErrors + thresholds + confidence
app.listen(4000, () => {
  console.log('✅ Server running on http://localhost:4000')
})
