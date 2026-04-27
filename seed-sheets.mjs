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

// Targets provided by the user — each URL pins a specific gid (tab).
const TARGETS = {
  website:  { spreadsheetId: '1wJkTdDIU334IfbUZt9TA_HFM4UzHySCgolbNLY8fTt0', gid: 1099783648 },
  offline:  { spreadsheetId: '1ZKFWbjLMdr8PepI25Uz3z-LLIEnHwnrohsBMU-Fb9Sw', gid: 597593892 },
  broker:   { spreadsheetId: '1MmCcO5BlpUjn3KZ7fbdP9qyii6pM-CAJkC0o1Fnqybo', gid: 1573565402 },
  whatsapp: { spreadsheetId: '1HXoojJHxMyg54PPNd9BbaFIZnO69FOke4_688GvD4BM', gid: 1530328212 }
  // marketing intentionally omitted — no link provided
}

// Today and recent dates
const today = new Date()
const daysAgo = (n) => {
  const d = new Date(today)
  d.setDate(d.getDate() - n)
  return d.toISOString().slice(0, 10)
}

// Shared phone numbers trigger multi-channel dedup → Hot tier
const P = {
  arjun: '9811001122',
  neha: '9811002233',
  sidd: '9811003344',
  kavita: '9811004455',
  ramesh: '9811005566',
  test1: '9811006677',
  amit: '9811007788',
  priya: '9811008899',
  rahul: '9811009900',
  vikram: '9811001001'
}

const websiteRows = [
  { 'Lead ID': 'W-101', Name: 'Arjun Mehta', Phone: P.arjun, Email: 'arjun.m@example.com', City: 'Whitefield', Budget: '₹1.2 Cr', 'Property Type': '3BHK Apartment', 'Inquiry Date': daysAgo(3), Source: 'Website', Status: 'Qualified' },
  { 'Lead ID': 'W-102', Name: 'Neha Kapoor', Phone: P.neha, Email: 'neha.k@example.com', City: 'Hebbal', Budget: '₹2.4 Cr', 'Property Type': 'Villa', 'Inquiry Date': daysAgo(1), Source: 'Website', Status: 'Hot' },
  { 'Lead ID': 'W-103', Name: 'Siddharth Iyer', Phone: P.sidd, Email: 'sid.i@example.com', City: 'Sarjapur', Budget: '₹85 Lakh', 'Property Type': '2BHK Apartment', 'Inquiry Date': daysAgo(2), Source: 'Website', Status: 'Qualified' },
  { 'Lead ID': 'W-104', Name: 'Kavita Raman', Phone: P.kavita, Email: 'kavita.r@example.com', City: 'Indiranagar', Budget: '₹70 Lakh', 'Property Type': '2BHK Apartment', 'Inquiry Date': daysAgo(5), Source: 'Portal', Status: 'In Progress' },
  { 'Lead ID': 'W-105', Name: 'Rahul Sharma', Phone: P.rahul, Email: 'rahul.s@example.com', City: 'Electronic City', Budget: '₹55 Lakh', 'Property Type': '2BHK Apartment', 'Inquiry Date': daysAgo(4), Source: 'Website', Status: 'Converted' },
  { 'Lead ID': 'W-106', Name: 'Test Walker', Phone: P.test1, Email: '', City: '', Budget: '', 'Property Type': '', 'Inquiry Date': daysAgo(20), Source: 'Website', Status: 'Cold' },
  { 'Lead ID': 'W-107', Name: 'Vikram Rao', Phone: P.vikram, Email: 'vikram.r@example.com', City: 'Hebbal', Budget: '₹1.6 Cr', 'Property Type': '4BHK Villa', 'Inquiry Date': daysAgo(10), Source: 'Website', Status: 'Lost' },
  { 'Lead ID': 'W-108', Name: 'Priya Nair', Phone: P.priya, Email: 'priya.n@example.com', City: 'Koramangala', Budget: '₹90 Lakh', 'Property Type': '3BHK Apartment', 'Inquiry Date': daysAgo(6), Source: 'Website', Status: 'Qualified' }
]

const offlineRows = [
  { 'Lead ID': 'O-201', Name: 'Arjun Mehta', Phone: P.arjun, 'Project Visited': 'Prestige Lakeside', 'Visit Date': daysAgo(2), Budget: '₹1.2 Cr', Source: 'Walk-in', Status: 'Hot', Feedback: 'Loved the 3BHK layout, discussing financing', 'Sales Exec': 'Anita' },
  { 'Lead ID': 'O-202', Name: 'Siddharth Iyer', Phone: P.sidd, 'Project Visited': 'Sobha Dream Acres', 'Visit Date': daysAgo(1), Budget: '₹85 Lakh', Source: 'Walk-in', Status: 'Qualified', Feedback: 'Requested floor plan variants', 'Sales Exec': 'Karthik' },
  { 'Lead ID': 'O-203', Name: 'Rahul Sharma', Phone: P.rahul, 'Project Visited': 'Brigade Cornerstone', 'Visit Date': daysAgo(4), Budget: '₹55 Lakh', Source: 'Walk-in', Status: 'Converted', Feedback: 'Booking token paid', 'Sales Exec': 'Anita' },
  { 'Lead ID': 'O-204', Name: 'Priya Nair', Phone: P.priya, 'Project Visited': 'Godrej Reflections', 'Visit Date': daysAgo(6), Budget: '₹90 Lakh', Source: 'Walk-in', Status: 'Qualified', Feedback: 'Wants corner unit, follow up on availability', 'Sales Exec': 'Karthik' },
  { 'Lead ID': 'O-205', Name: 'Global Realty LLP', Phone: '9811010101', 'Project Visited': 'Prestige Commercial Tower', 'Visit Date': daysAgo(3), Budget: '₹3.5 Cr', Source: 'Walk-in', Status: 'Hot', Feedback: 'Commercial deal — leadership presentation scheduled', 'Sales Exec': 'Meera' },
  { 'Lead ID': 'O-206', Name: 'Kunal Desai', Phone: '9811020202', 'Project Visited': 'Sobha Valley', 'Visit Date': daysAgo(8), Budget: '₹95 Lakh', Source: 'Walk-in', Status: 'In Progress', Feedback: 'Comparing with competitor, shared EMI calc', 'Sales Exec': 'Karthik' }
]

const marketingRows = [
  { 'Lead ID': 'M-301', Name: 'Kavita Raman', Phone: P.kavita, City: 'Indiranagar', Budget: '₹70 Lakh', 'Campaign Name': 'Summer Launch 2026', Platform: 'Meta Ads', 'Ad Clicks': '7', Source: 'Meta', Status: 'In Progress' },
  { 'Lead ID': 'M-302', Name: 'Ramesh Gupta', Phone: P.ramesh, City: 'Whitefield', Budget: '₹1.1 Cr', 'Campaign Name': 'Luxury Living', Platform: 'Google Ads', 'Ad Clicks': '12', Source: 'Google', Status: 'Qualified' },
  { 'Lead ID': 'M-303', Name: 'Neha Kapoor', Phone: P.neha, City: 'Hebbal', Budget: '₹2.4 Cr', 'Campaign Name': 'Villa Connoisseur', Platform: 'Meta Ads', 'Ad Clicks': '9', Source: 'Meta', Status: 'Hot' },
  { 'Lead ID': 'M-304', Name: 'Amit Singh', Phone: P.amit, City: 'Yelahanka', Budget: '', 'Campaign Name': 'General Retargeting', Platform: 'Meta Ads', 'Ad Clicks': '1', Source: 'Meta', Status: 'Cold' },
  { 'Lead ID': 'M-305', Name: 'Priya Nair', Phone: P.priya, City: 'Koramangala', Budget: '₹90 Lakh', 'Campaign Name': 'Koramangala Living', Platform: 'Google Ads', 'Ad Clicks': '5', Source: 'Google', Status: 'Qualified' },
  { 'Lead ID': 'M-306', Name: 'Vikram Rao', Phone: P.vikram, City: 'Hebbal', Budget: '₹1.6 Cr', 'Campaign Name': 'Villa Connoisseur', Platform: 'Meta Ads', 'Ad Clicks': '2', Source: 'Meta', Status: 'Lost' }
]

const brokerRows = [
  { 'Lead ID': 'B-401', 'Client Name': 'Neha Kapoor', Phone: P.neha, City: 'Hebbal', Budget: '₹2.4 Cr', 'Property Type': 'Villa', 'Broker Name': 'Sunil Realty', 'Commission %': '2.0', Source: 'Broker', Status: 'Hot' },
  { 'Lead ID': 'B-402', 'Client Name': 'Ramesh Gupta', Phone: P.ramesh, City: 'Whitefield', Budget: '₹1.1 Cr', 'Property Type': '3BHK Apartment', 'Broker Name': 'Metro Homes', 'Commission %': '1.8', Source: 'Broker', Status: 'Qualified' },
  { 'Lead ID': 'B-403', 'Client Name': 'Anil Verma', Phone: '9811030303', City: 'JP Nagar', Budget: '₹80 Lakh', 'Property Type': '2BHK Apartment', 'Broker Name': 'Metro Homes', 'Commission %': '1.8', Source: 'Broker', Status: 'In Progress' },
  { 'Lead ID': 'B-404', 'Client Name': 'Deepak Shah', Phone: '9811040404', City: 'CBD', Budget: '₹4.0 Cr', 'Property Type': 'Penthouse', 'Broker Name': 'Premium Partners', 'Commission %': '2.5', Source: 'Broker', Status: 'Hot' }
]

const whatsappRows = [
  { 'Lead ID': 'WA-501', Name: 'Arjun Mehta', Phone: P.arjun, Location: 'Whitefield', Budget: '₹1.2 Cr', Interested: 'Yes', 'Visit Req': 'Yes', 'Last Resp': '1', 'Chat Summary': 'Confirmed site visit Saturday, asked about home loan tie-up', Source: 'WhatsApp', Status: 'Hot' },
  { 'Lead ID': 'WA-502', Name: 'Neha Kapoor', Phone: P.neha, Location: 'Hebbal', Budget: '₹2.4 Cr', Interested: 'Yes', 'Visit Req': 'Yes', 'Last Resp': '2', 'Chat Summary': 'Requested villa brochure and amenity list', Source: 'WhatsApp', Status: 'Hot' },
  { 'Lead ID': 'WA-503', Name: 'Ramesh Gupta', Phone: P.ramesh, Location: 'Whitefield', Budget: '₹1.1 Cr', Interested: 'Yes', 'Visit Req': 'No', 'Last Resp': '4', 'Chat Summary': 'Comparing with two other projects, wants pricing sheet', Source: 'WhatsApp', Status: 'Qualified' },
  { 'Lead ID': 'WA-504', Name: 'Priya Nair', Phone: P.priya, Location: 'Koramangala', Budget: '₹90 Lakh', Interested: 'Yes', 'Visit Req': 'Yes', 'Last Resp': '5', 'Chat Summary': 'Interested in 3BHK corner units, EMI query answered', Source: 'WhatsApp', Status: 'Qualified' },
  { 'Lead ID': 'WA-505', Name: 'Silent Sameer', Phone: '9811050505', Location: 'Bellandur', Budget: '₹1.0 Cr', Interested: 'Yes', 'Visit Req': 'Yes', 'Last Resp': '18', 'Chat Summary': 'Went silent after first chat — flagged as silent high-intent', Source: 'WhatsApp', Status: 'In Progress' },
  { 'Lead ID': 'WA-506', Name: 'Amit Singh', Phone: P.amit, Location: 'Yelahanka', Budget: '', Interested: 'No', 'Visit Req': 'No', 'Last Resp': '25', 'Chat Summary': 'Only browsing, no serious inquiry', Source: 'WhatsApp', Status: 'Cold' },
  { 'Lead ID': 'WA-507', Name: 'Test Walker', Phone: P.test1, Location: '', Budget: '', Interested: 'No', 'Visit Req': 'No', 'Last Resp': '30', 'Chat Summary': 'One-off message, no response to follow-up', Source: 'WhatsApp', Status: 'Cold' }
]

async function resolveTabName(spreadsheetId, gid) {
  const meta = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets.properties'
  })
  const match = (meta.data.sheets || []).find(
    (s) => s.properties && s.properties.sheetId === gid
  )
  if (!match) {
    const avail = (meta.data.sheets || [])
      .map((s) => `${s.properties.title} (gid=${s.properties.sheetId})`)
      .join(', ')
    throw new Error(`No tab found for gid=${gid}. Available: ${avail}`)
  }
  return match.properties.title
}

async function getHeaders(spreadsheetId, tabName) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `'${tabName}'!A1:Z1`
  })
  const header = (res.data.values && res.data.values[0]) || []
  return header.map((h) => String(h).trim())
}

async function appendRows(target, rows, label) {
  const { spreadsheetId, gid } = target
  const tabName = await resolveTabName(spreadsheetId, gid)
  const headers = await getHeaders(spreadsheetId, tabName)
  if (!headers.length) {
    console.log(`⚠️  ${label}: no header row on tab "${tabName}", skipping`)
    return
  }
  const normalized = headers.map((h) => h.toLowerCase())
  const values = rows.map((row) => {
    const entry = {}
    for (const [k, v] of Object.entries(row)) entry[k.toLowerCase()] = v
    return headers.map((_, i) => entry[normalized[i]] ?? '')
  })

  const res = await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `'${tabName}'!A1`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values }
  })
  console.log(
    `✅ ${label}: appended ${rows.length} rows to tab "${tabName}" (headers: ${headers.join(', ')})`
  )
  return { tabName, updatedRange: res.data.updates?.updatedRange }
}

async function run() {
  console.log('Seeding Google Sheets with realistic leads...\n')
  const results = {}
  results.website  = await appendRows(TARGETS.website,  websiteRows,  'website')
  results.offline  = await appendRows(TARGETS.offline,  offlineRows,  'offline')
  results.broker   = await appendRows(TARGETS.broker,   brokerRows,   'broker')
  results.whatsapp = await appendRows(TARGETS.whatsapp, whatsappRows, 'whatsapp')
  console.log('\nResolved tab names (for server.mjs if different from first tab):')
  for (const [k, v] of Object.entries(results)) {
    if (v) console.log(`  ${k}: "${v.tabName}"`)
  }
  console.log('\nDone.')
}

run().catch((err) => {
  console.error('❌ Seed failed:', err.message)
  process.exit(1)
})
