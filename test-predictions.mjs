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

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: credentials.client_email,
    private_key: credentials.private_key
  },
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
})

const sheets = google.sheets({ version: 'v4', auth })

const TARGETS = {
  website:  { spreadsheetId: '1wJkTdDIU334IfbUZt9TA_HFM4UzHySCgolbNLY8fTt0', gid: 1099783648 },
  offline:  { spreadsheetId: '1ZKFWbjLMdr8PepI25Uz3z-LLIEnHwnrohsBMU-Fb9Sw', gid: 597593892 },
  broker:   { spreadsheetId: '1MmCcO5BlpUjn3KZ7fbdP9qyii6pM-CAJkC0o1Fnqybo', gid: 1573565402 },
  whatsapp: { spreadsheetId: '1HXoojJHxMyg54PPNd9BbaFIZnO69FOke4_688GvD4BM', gid: 1530328212 },
  marketing:{ spreadsheetId: '1sLgXHKAZeFksG_s_oDu5wrMDuqBafmbjO-lVO1XZd0I', gid: 1177810620 }
}

const today = new Date()
const daysAgo = (n) => {
  const d = new Date(today)
  d.setDate(d.getDate() - n)
  return d.toISOString().slice(0, 10)
}

// TEST CASE PHONES — unique so we can validate predictions by lookup
const T = {
  hotA: '9900000001',   // expected: Hot (multi-channel, fresh, visited, high budget)
  hotB: '9900000002',   // expected: Hot (whatsapp visit + budget + interest)
  coldA: '9900000003',  // expected: Cold (no budget, no activity, marked lost)
  coldB: '9900000004',  // expected: Cold (vague, one-time, old)
  silent: '9900000005', // expected: Silent (high budget, visit req, but 20+ days dormant)
  warmA: '9900000006'   // expected: Warm (budget + city, single channel, recent)
}

const websiteRows = [
  { 'Lead ID': 'TST-W1', Name: 'TEST-Hot-Arjun', Phone: T.hotA, Email: 'hotA@t.com', City: 'Whitefield', Budget: '₹2.5 Cr', 'Property Type': 'Villa', 'Inquiry Date': daysAgo(1), Source: 'Website', Status: 'Qualified' },
  { 'Lead ID': 'TST-W2', Name: 'TEST-Cold-Nobody', Phone: T.coldA, Email: '', City: '', Budget: '', 'Property Type': '', 'Inquiry Date': daysAgo(40), Source: 'Website', Status: 'Lost' },
  { 'Lead ID': 'TST-W3', Name: 'TEST-Cold-Browser', Phone: T.coldB, Email: '', City: '', Budget: '', 'Property Type': '', 'Inquiry Date': daysAgo(55), Source: 'Website', Status: 'Lost' },
  { 'Lead ID': 'TST-W4', Name: 'TEST-Warm-Kavya', Phone: T.warmA, Email: 'kavya@t.com', City: 'Koramangala', Budget: '₹85 Lakh', 'Property Type': '3BHK', 'Inquiry Date': daysAgo(2), Source: 'Website', Status: 'In Progress' }
]

const offlineRows = [
  { 'Lead ID': 'TST-O1', Name: 'TEST-Hot-Arjun', Phone: T.hotA, 'Project Visited': 'Prestige Lakeside', 'Visit Date': daysAgo(2), Budget: '₹2.5 Cr', Source: 'Walk-in', Status: 'Hot', Feedback: 'Very interested, wants financing info', 'Sales Exec': 'Anita' }
]

const whatsappRows = [
  { 'Lead ID': 'TST-WA1', Name: 'TEST-Hot-Arjun', Phone: T.hotA, Budget: '₹2.5 Cr', Location: 'Whitefield', Interested: 'Yes', 'Visit Requested': 'Yes', 'Last Response Days': '1', 'Chat Summary': 'Asked about home loan tie-up and Saturday visit', Source: 'WhatsApp', Status: 'Hot' },
  { 'Lead ID': 'TST-WA2', Name: 'TEST-Hot-Raghav', Phone: T.hotB, Budget: '₹1.4 Cr', Location: 'Hebbal', Interested: 'Yes', 'Visit Requested': 'Yes', 'Last Response Days': '2', 'Chat Summary': 'Confirmed visit, asked about loan EMI', Source: 'WhatsApp', Status: 'Qualified' },
  { 'Lead ID': 'TST-WA3', Name: 'TEST-Silent-Vikas', Phone: T.silent, Budget: '₹1.2 Cr', Location: 'Sarjapur', Interested: 'Yes', 'Visit Requested': 'Yes', 'Last Response Days': '22', 'Chat Summary': 'Went quiet after asking about 3BHK pricing', Source: 'WhatsApp', Status: 'In Progress' }
]

const marketingRows = [
  { 'Lead ID': 'TST-M1', Name: 'TEST-Hot-Arjun', Phone: T.hotA, City: 'Whitefield', Budget: '₹2.5 Cr', 'Campaign Name': 'Villa Premium', Platform: 'Google Ads', 'Ad Clicks': '11', Source: 'Google', Status: 'Hot' },
  { 'Lead ID': 'TST-M2', Name: 'TEST-Warm-Kavya', Phone: T.warmA, City: 'Koramangala', Budget: '₹85 Lakh', 'Campaign Name': 'Summer Launch', Platform: 'Meta Ads', 'Ad Clicks': '4', Source: 'Meta', Status: 'In Progress' }
]

async function resolveTabName(spreadsheetId, gid) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId, fields: 'sheets.properties' })
  const match = (meta.data.sheets || []).find((s) => s.properties.sheetId === gid)
  if (!match) throw new Error(`no tab for gid=${gid}`)
  return match.properties.title
}

async function appendRows(target, rows, label) {
  if (!rows.length) return
  const tabName = await resolveTabName(target.spreadsheetId, target.gid)
  const hdrRes = await sheets.spreadsheets.values.get({
    spreadsheetId: target.spreadsheetId,
    range: `'${tabName}'!A1:Z1`
  })
  const headers = ((hdrRes.data.values && hdrRes.data.values[0]) || []).map((h) => String(h).trim())
  const normalized = headers.map((h) => h.toLowerCase())
  const values = rows.map((row) => {
    const entry = {}
    for (const [k, v] of Object.entries(row)) entry[k.toLowerCase()] = v
    return headers.map((_, i) => entry[normalized[i]] ?? '')
  })
  await sheets.spreadsheets.values.append({
    spreadsheetId: target.spreadsheetId,
    range: `'${tabName}'!A1`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  })
  console.log(`  seeded ${rows.length} rows → ${label}`)
}

async function seed() {
  console.log('Seeding prediction test cases...')
  await appendRows(TARGETS.website, websiteRows, 'website')
  await appendRows(TARGETS.offline, offlineRows, 'offline')
  await appendRows(TARGETS.whatsapp, whatsappRows, 'whatsapp')
  await appendRows(TARGETS.marketing, marketingRows, 'marketing')
}

async function validate() {
  console.log('\nWaiting 3s for sheets to settle, then validating predictions...\n')
  await new Promise((r) => setTimeout(r, 3000))

  const res = await fetch('http://localhost:4000/api/unified-leads')
  const data = await res.json()
  const byPhone = new Map()
  for (const l of data.leads) {
    const p = String(l.phone || '').replace(/\D/g, '').slice(-10)
    if (p) byPhone.set(p, l)
  }

  const cases = [
    { name: 'Hot-Arjun (4 channels + visit + ₹2.5Cr)',   phone: T.hotA,   expect: 'Hot' },
    { name: 'Hot-Raghav (WA visit + ₹1.4Cr)',            phone: T.hotB,   expect: 'Hot' },
    { name: 'Cold-Nobody (no budget + Lost + 40d old)',   phone: T.coldA,  expect: 'Cold' },
    { name: 'Cold-Browser (no budget + Lost + 55d old)',  phone: T.coldB,  expect: 'Cold' },
    { name: 'Silent-Vikas (visit req but 22d dormant)',   phone: T.silent, expectSilent: true },
    { name: 'Warm-Kavya (budget + 2 channels)',          phone: T.warmA,  expect: 'Warm' }
  ]

  let pass = 0
  let fail = 0
  for (const c of cases) {
    const lead = byPhone.get(c.phone)
    if (!lead) {
      console.log(`  ❌ ${c.name}: NOT FOUND in unified leads`)
      fail++
      continue
    }
    const tier = lead.tier
    const score = (lead.score * 100).toFixed(0) + '%'
    const silent = lead.silent
    if (c.expectSilent) {
      if (silent) {
        console.log(`  ✅ ${c.name}: silent=${silent} (score ${score}) — PASS`)
        pass++
      } else {
        console.log(`  ❌ ${c.name}: expected silent, got silent=${silent} tier=${tier} score=${score}`)
        fail++
      }
    } else {
      if (tier === c.expect) {
        console.log(`  ✅ ${c.name}: tier=${tier} score=${score} — PASS`)
        pass++
      } else {
        console.log(`  ❌ ${c.name}: expected ${c.expect}, got ${tier} score=${score}`)
        fail++
      }
    }
  }

  console.log(`\n${pass}/${pass + fail} test cases passed.`)
  console.log(`Overall model: ${data.leads.length} unified leads, accuracy: ${data.modelAccuracy}, labeled: ${data.labeledCount}`)
}

const cmd = process.argv[2] || 'all'
if (cmd === 'seed') {
  seed().catch((e) => (console.error(e), process.exit(1)))
} else if (cmd === 'validate') {
  validate().catch((e) => (console.error(e), process.exit(1)))
} else {
  seed()
    .then(validate)
    .catch((e) => (console.error(e), process.exit(1)))
}
