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

const SPREADSHEET_ID = '1HXoojJHxMyg54PPNd9BbaFIZnO69FOke4_688GvD4BM'
const GID = 1530328212

// The values we intended for each Lead ID.
const patches = {
  'WA-501': { 'Visit Requested': 'Yes', 'Last Response Days': '1' },
  'WA-502': { 'Visit Requested': 'Yes', 'Last Response Days': '2' },
  'WA-503': { 'Visit Requested': 'No',  'Last Response Days': '4' },
  'WA-504': { 'Visit Requested': 'Yes', 'Last Response Days': '5' },
  'WA-505': { 'Visit Requested': 'Yes', 'Last Response Days': '18' },
  'WA-506': { 'Visit Requested': 'No',  'Last Response Days': '25' },
  'WA-507': { 'Visit Requested': 'No',  'Last Response Days': '30' }
}

function colLetter(n) {
  let s = ''
  n += 1
  while (n > 0) {
    const m = (n - 1) % 26
    s = String.fromCharCode(65 + m) + s
    n = Math.floor((n - 1) / 26)
  }
  return s
}

async function run() {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: 'sheets.properties' })
  const tab = (meta.data.sheets || []).find((s) => s.properties.sheetId === GID)
  if (!tab) throw new Error('tab not found')
  const tabName = tab.properties.title

  const all = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: `'${tabName}'!A1:Z`
  })
  const rows = all.data.values || []
  if (rows.length < 2) {
    console.log('no data rows, nothing to patch')
    return
  }
  const header = rows[0].map((h) => String(h).trim())
  const idxLeadId = header.findIndex((h) => h.toLowerCase() === 'lead id')
  const idxVisit = header.findIndex((h) => h.toLowerCase() === 'visit requested')
  const idxResp = header.findIndex((h) => h.toLowerCase() === 'last response days')
  if (idxLeadId < 0 || idxVisit < 0 || idxResp < 0) {
    throw new Error(`required headers missing. Got: ${header.join(', ')}`)
  }

  const updates = []
  for (let r = 1; r < rows.length; r++) {
    const leadId = String(rows[r][idxLeadId] || '').trim()
    const patch = patches[leadId]
    if (!patch) continue
    const row1Based = r + 1
    updates.push({
      range: `'${tabName}'!${colLetter(idxVisit)}${row1Based}`,
      values: [[patch['Visit Requested']]]
    })
    updates.push({
      range: `'${tabName}'!${colLetter(idxResp)}${row1Based}`,
      values: [[patch['Last Response Days']]]
    })
  }

  if (!updates.length) {
    console.log('⚠️  no matching Lead IDs found to patch')
    return
  }

  const res = await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { valueInputOption: 'USER_ENTERED', data: updates }
  })
  console.log(`✅ patched ${updates.length / 2} rows (${res.data.totalUpdatedCells} cells)`)
}

run().catch((err) => {
  console.error('❌', err.message)
  process.exit(1)
})
