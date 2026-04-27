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

const SPREADSHEET_ID = '1sLgXHKAZeFksG_s_oDu5wrMDuqBafmbjO-lVO1XZd0I'
const GID = 1177810620

const P = {
  arjun: '9811001122',
  neha: '9811002233',
  kavita: '9811004455',
  ramesh: '9811005566',
  amit: '9811007788',
  priya: '9811008899',
  vikram: '9811001001'
}

const rows = [
  { 'Lead ID': 'M-301', Name: 'Kavita Raman',   Phone: P.kavita, City: 'Indiranagar',    Budget: '₹70 Lakh', 'Campaign Name': 'Summer Launch 2026',   Platform: 'Meta Ads',   'Ad Clicks': '7',  Source: 'Meta',   Status: 'In Progress' },
  { 'Lead ID': 'M-302', Name: 'Ramesh Gupta',   Phone: P.ramesh, City: 'Whitefield',     Budget: '₹1.1 Cr',  'Campaign Name': 'Luxury Living',         Platform: 'Google Ads', 'Ad Clicks': '12', Source: 'Google', Status: 'Qualified' },
  { 'Lead ID': 'M-303', Name: 'Neha Kapoor',    Phone: P.neha,   City: 'Hebbal',         Budget: '₹2.4 Cr',  'Campaign Name': 'Villa Connoisseur',     Platform: 'Meta Ads',   'Ad Clicks': '9',  Source: 'Meta',   Status: 'Hot' },
  { 'Lead ID': 'M-304', Name: 'Amit Singh',     Phone: P.amit,   City: 'Yelahanka',      Budget: '',         'Campaign Name': 'General Retargeting',   Platform: 'Meta Ads',   'Ad Clicks': '1',  Source: 'Meta',   Status: 'Cold' },
  { 'Lead ID': 'M-305', Name: 'Priya Nair',     Phone: P.priya,  City: 'Koramangala',    Budget: '₹90 Lakh', 'Campaign Name': 'Koramangala Living',    Platform: 'Google Ads', 'Ad Clicks': '5',  Source: 'Google', Status: 'Qualified' },
  { 'Lead ID': 'M-306', Name: 'Vikram Rao',     Phone: P.vikram, City: 'Hebbal',         Budget: '₹1.6 Cr',  'Campaign Name': 'Villa Connoisseur',     Platform: 'Meta Ads',   'Ad Clicks': '2',  Source: 'Meta',   Status: 'Lost' },
  { 'Lead ID': 'M-307', Name: 'Arjun Mehta',    Phone: P.arjun,  City: 'Whitefield',     Budget: '₹1.2 Cr',  'Campaign Name': 'Premium 3BHK',          Platform: 'Google Ads', 'Ad Clicks': '8',  Source: 'Google', Status: 'Qualified' }
]

async function run() {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: 'sheets.properties' })
  const tab = (meta.data.sheets || []).find((s) => s.properties.sheetId === GID)
  if (!tab) {
    const avail = (meta.data.sheets || [])
      .map((s) => `${s.properties.title} (gid=${s.properties.sheetId})`)
      .join(', ')
    throw new Error(`No tab for gid=${GID}. Available: ${avail}`)
  }
  const tabName = tab.properties.title
  const hdrRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `'${tabName}'!A1:Z1`
  })
  const headers = ((hdrRes.data.values && hdrRes.data.values[0]) || []).map((h) => String(h).trim())
  if (!headers.length) {
    throw new Error(`tab "${tabName}" has no header row`)
  }
  const normalized = headers.map((h) => h.toLowerCase())
  const values = rows.map((row) => {
    const entry = {}
    for (const [k, v] of Object.entries(row)) entry[k.toLowerCase()] = v
    return headers.map((_, i) => entry[normalized[i]] ?? '')
  })
  const res = await sheets.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: `'${tabName}'!A1`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  })
  console.log(`✅ marketing: appended ${rows.length} rows to tab "${tabName}"`)
  console.log(`   headers: ${headers.join(', ')}`)
  console.log(`   range: ${res.data.updates?.updatedRange}`)
}

run().catch((err) => {
  console.error('❌', err.message)
  process.exit(1)
})
