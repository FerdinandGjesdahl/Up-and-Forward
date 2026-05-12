#!/usr/bin/env node

import { createReadStream, readFileSync } from 'node:fs';
import { promises as fs } from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import webpush from 'web-push';

// Load .env file manually (no extra dependency needed)
try {
  const envContent = readFileSync(new URL('.env', import.meta.url), 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    if (!(key in process.env)) process.env[key] = val;
  }
} catch { /* no .env file, use system env */ }

const ROOT_DIR = process.cwd();
const PORT = Number(process.env.PORT || 8787);
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || '';
const OPENAI_MODEL = process.env.OPENAI_MODEL || 'gpt-4.1-mini';
const PUSH_CONTACT = process.env.PUSH_CONTACT || 'mailto:alerts@upandforward.local';
const MONITOR_INTERVAL_MS = Number(process.env.MONITOR_INTERVAL_MS || 24 * 60 * 60 * 1000);
const MONITOR_ENABLED = !['0', 'false', 'off', 'disabled'].includes(String(process.env.MONITOR_ENABLED || 'true').toLowerCase());
const RESEND_API_KEY = process.env.RESEND_API_KEY || '';
const MAIL_FROM = process.env.MAIL_FROM || 'Up and Forward <alerts@upandforward.local>';
const ALERT_EMAILS = process.env.ALERT_EMAILS || '';

const MAX_BODY_SIZE = 2 * 1024 * 1024;
const MAX_PAGE_CHARS = 220_000;
const MAX_CONTEXT_CHARS = 280_000;
const MAX_MODEL_SOURCES = 3;
const MAX_HEURISTIC_JOBS = 250;
const MAX_FOLLOWUP_LINKS = 20;
const MAX_AI_LINK_CANDIDATES = 200;
const MAX_AI_SELECTED_LINKS = 20;
const MAX_ITERATIVE_PAGES = 24;
const FETCH_TIMEOUT_MS = 15_000;
const ATS_FETCH_TIMEOUT_MS = 15_000;
const PLAYWRIGHT_TIMEOUT_MS = Number(process.env.PLAYWRIGHT_TIMEOUT_MS || 25_000);
const EXTRACT_TIMEOUT_MS = Number(process.env.EXTRACT_TIMEOUT_MS || 120_000);
const RENDERED_FETCH = String(process.env.RENDERED_FETCH || 'always').toLowerCase();
const MAX_RENDERED_LINKS = 1500;
const MAX_RENDERED_TEXT_CHARS = 180_000;
const MAX_ATS_JOBS = 500;
const DATA_DIR = path.join(ROOT_DIR, 'data');
const VAPID_KEYS_FILE = path.join(DATA_DIR, 'vapid_keys.json');
const PUSH_SUBSCRIPTIONS_FILE = path.join(DATA_DIR, 'push_subscriptions.json');
const EMAIL_SUBSCRIPTIONS_FILE = path.join(DATA_DIR, 'email_subscriptions.json');
const WATCH_URLS_FILE = path.join(DATA_DIR, 'watch_urls.json');
const MONITOR_SEEN_FILE = path.join(DATA_DIR, 'monitor_seen_jobs.json');
const MONITOR_LATEST_FILE = path.join(DATA_DIR, 'latest_monitor_jobs.json');

const JOB_KEYWORDS = [
  'jobb', 'job', 'jobs', 'jobbannonser', 'karriere', 'career', 'careers',
  'stilling', 'stillinger', 'position', 'positions', 'vacancy', 'vacancies',
  'intern', 'internship', 'summer intern', 'graduate', 'trainee', 'student',
  'deltid', 'heltid', 'fulltime', 'parttime', 'sok', 'soknad', 'apply'
];

const META_KEYWORDS = ['frist', 'deadline', 'posted', 'lagt ut', 'publisert', 'location', 'sted'];

const LOCATION_HINTS = [
  'oslo', 'trondheim', 'bergen', 'stavanger', 'tromso', 'kristiansand', 'aalesund',
  'london', 'stockholm', 'copenhagen', 'new york', 'remote', 'hybrid', 'digitalt'
];

const ATS_HOST_HINTS = [
  'greenhouse.io',
  'job-boards.greenhouse.io',
  'lever.co',
  'workdayjobs.com',
  'myworkdayjobs.com',
  'myworkdaysite.com',
  'smartrecruiters.com',
  'ashbyhq.com',
  'teamtailor.com',
  'recruitee.com',
  'breezy.hr'
];

const MONTH_MAP = {
  jan: 0, january: 0, januar: 0,
  feb: 1, february: 1, februar: 1,
  mar: 2, march: 2, mars: 2,
  apr: 3, april: 3,
  may: 4, mai: 4,
  jun: 5, june: 5, juni: 5,
  jul: 6, july: 6, juli: 6,
  aug: 7, august: 7,
  sep: 8, sept: 8, september: 8,
  oct: 9, okt: 9, october: 9, oktober: 9,
  nov: 10, november: 10,
  dec: 11, des: 11, december: 11, desember: 11
};

const MIME_TYPES = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.webmanifest': 'application/manifest+json; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.txt': 'text/plain; charset=utf-8'
};

function sendJson(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*'
  });
  res.end(body);
}

function sendText(res, statusCode, message) {
  res.writeHead(statusCode, {
    'Content-Type': 'text/plain; charset=utf-8',
    'Content-Length': Buffer.byteLength(message),
    'Access-Control-Allow-Origin': '*'
  });
  res.end(message);
}

async function ensureDataDir() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function readJsonFile(filePath, fallbackValue) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    if (error?.code === 'ENOENT') return fallbackValue;
    throw error;
  }
}

async function writeJsonFile(filePath, value) {
  await ensureDataDir();
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function resolvePathname(pathname) {
  const cleanPath = pathname === '/' ? '/index.html' : pathname;
  const decoded = decodeURIComponent(cleanPath);
  const resolved = path.resolve(ROOT_DIR, `.${decoded}`);
  const insideRoot = resolved === ROOT_DIR || resolved.startsWith(`${ROOT_DIR}${path.sep}`);
  return insideRoot ? resolved : null;
}

async function serveStatic(req, res, pathname) {
  const target = resolvePathname(pathname);
  if (!target) {
    sendText(res, 403, 'Forbidden');
    return;
  }

  let stat;
  try {
    stat = await fs.stat(target);
  } catch {
    sendText(res, 404, 'Not found');
    return;
  }

  if (stat.isDirectory()) {
    sendText(res, 403, 'Directory listing is disabled');
    return;
  }

  const ext = path.extname(target).toLowerCase();
  const contentType = MIME_TYPES[ext] || 'application/octet-stream';

  res.writeHead(200, {
    'Content-Type': contentType,
    'Content-Length': stat.size,
    'Cache-Control': 'no-store'
  });

  if (req.method === 'HEAD') {
    res.end();
    return;
  }

  createReadStream(target).pipe(res);
}

async function readJsonBody(req) {
  const chunks = [];
  let size = 0;

  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_SIZE) {
      throw new Error('Request body too large');
    }
    chunks.push(chunk);
  }

  const text = Buffer.concat(chunks).toString('utf8');
  if (!text.trim()) return {};

  try {
    return JSON.parse(text);
  } catch {
    throw new Error('Invalid JSON body');
  }
}

function withTimeout(promise, timeoutMs, message) {
  let timeout = null;
  const timeoutPromise = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  return Promise.race([promise, timeoutPromise]).finally(() => clearTimeout(timeout));
}

function cleanText(value) {
  return String(value || '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/!\[[^\]]*]\([^)]*\)/g, ' ')
    .replace(/\[[^\]]*]\([^)]*\)/g, ' ')
    .replace(/[`*_#>|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function hasKeyword(text, keywords) {
  const lower = String(text || '').toLowerCase();
  return keywords.some((keyword) => lower.includes(keyword));
}

function countMatches(text, regex) {
  const matches = text.match(regex);
  return matches ? matches.length : 0;
}

function toAbsoluteUrl(value, baseUrl) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const absolute = new URL(raw, baseUrl).href;
    const parsed = new URL(absolute);
    if (!['http:', 'https:'].includes(parsed.protocol)) return '';
    return parsed.href;
  } catch {
    return '';
  }
}

function pathSegmentsFromUrl(url) {
  try {
    return new URL(url).pathname
      .split('/')
      .map((segment) => decodeURIComponent(segment.trim()))
      .filter(Boolean);
  } catch {
    return [];
  }
}

function titleCaseFromSlug(value) {
  return cleanText(String(value || '')
    .replace(/[-_]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2'))
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function uniqueNonEmpty(values) {
  return [...new Set(values.map((value) => cleanText(value)).filter(Boolean))];
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function firstStringFromKeys(object, keys) {
  if (!object || typeof object !== 'object') return '';

  for (const key of keys) {
    const value = object[key];
    if (typeof value === 'string' && cleanText(value)) return cleanText(value);
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  }

  return '';
}

function cleanLocationValue(value) {
  if (!value) return '';
  if (typeof value === 'string') return cleanText(value);
  if (Array.isArray(value)) {
    return uniqueNonEmpty(value.map((item) => cleanLocationValue(item))).join(', ');
  }
  if (typeof value === 'object') {
    const direct = firstStringFromKeys(value, [
      'name',
      'location',
      'locationsText',
      'city',
      'addressLocality',
      'addressRegion',
      'addressCountry',
      'country',
      'remote'
    ]);
    if (direct) return direct;

    const address = value.address ? cleanLocationValue(value.address) : '';
    if (address) return address;
  }
  return '';
}

function dateFromTimestamp(value) {
  const num = Number(value);
  if (!Number.isFinite(num) || num <= 0) return '';
  const ms = num < 10_000_000_000 ? num * 1000 : num;
  return toIsoDate(new Date(ms));
}

function normalizeAnyDate(value) {
  if (typeof value === 'number') return dateFromTimestamp(value);
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (/^\d{10,13}$/.test(raw)) return dateFromTimestamp(raw);
  return normalizePostedDate(raw);
}

function compactJsonSnippet(value, maxLength = 650) {
  try {
    return cleanText(JSON.stringify(value).slice(0, maxLength));
  } catch {
    return '';
  }
}

function looksLikeJobUrl(url, baseUrl) {
  const absolute = toAbsoluteUrl(url, baseUrl);
  if (!absolute) return false;

  const parsed = new URL(absolute);
  const lower = absolute.toLowerCase();
  const pathname = parsed.pathname.toLowerCase();

  if (/\.(js|css|png|jpe?g|gif|svg|webp|ico|pdf|woff2?|ttf|map)$/i.test(pathname)) {
    return false;
  }

  if (/^(\/(karriere|career|careers|jobs?|joblistings?|stillinger?|stilling))\/?$/.test(pathname)) {
    return false;
  }

  if (/(\/login|\/signin|\/signup|\/register|\/privacy|\/terms|\/contact)(\/|$)/.test(lower)) {
    return false;
  }

  if (/(\/karriere\/|\/career\/|\/careers\/|\/jobs?\/|\/joblistings?\/|\/stilling|\/stillinger|\/vacanc|\/position|\/positions?\/|\/postings?\/|\/openings?\/|\/requisition|\/req\/|\/apply\/)/.test(lower)) {
    return true;
  }

  if (ATS_HOST_HINTS.some((hint) => parsed.hostname.includes(hint))) {
    const segments = parsed.pathname.split('/').filter(Boolean);
    if (segments.length >= 2) return true;
  }

  if (/\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i.test(lower)) {
    return true;
  }

  return /(intern|graduate|trainee|summer|jobb|job)/.test(lower);
}

function looksLikeJobText(value) {
  const text = cleanText(value).toLowerCase();
  if (!text) return false;
  return hasKeyword(text, JOB_KEYWORDS);
}

function titleFromUrl(url, baseUrl) {
  const absolute = toAbsoluteUrl(url, baseUrl);
  if (!absolute) return '';
  try {
    const parsed = new URL(absolute);
    const slug = parsed.pathname.split('/').filter(Boolean).pop() || '';
    return cleanText(slug.replace(/[-_]+/g, ' '));
  } catch {
    return '';
  }
}

function canonicalUrl(value, baseUrl) {
  const absolute = toAbsoluteUrl(value, baseUrl);
  if (!absolute) return '';
  try {
    const parsed = new URL(absolute);
    return `${parsed.origin}${parsed.pathname}`.toLowerCase();
  } catch {
    return absolute.toLowerCase();
  }
}

function canonicalJobUrlKey(value, baseUrl) {
  const absolute = toAbsoluteUrl(value, baseUrl);
  if (!absolute) return '';
  try {
    const parsed = new URL(absolute);
    const pathname = parsed.pathname.replace(/\/+$/, '') || '/';
    return `${parsed.hostname.toLowerCase()}${pathname.toLowerCase()}`;
  } catch {
    return absolute.replace(/^https?:\/\//i, '').replace(/[?#].*$/, '').replace(/\/+$/, '').toLowerCase();
  }
}

function jobIdFromUrl(value, baseUrl) {
  const absolute = toAbsoluteUrl(value, baseUrl);
  if (!absolute) return '';
  try {
    const parsed = new URL(absolute);
    const lastSegment = parsed.pathname.split('/').filter(Boolean).pop() || '';
    const slugMatch = lastSegment.match(/^(\d{2,})(?:[-_]|$)/);
    return slugMatch ? slugMatch[1].toLowerCase() : '';
  } catch {
    return '';
  }
}

function cleanupExtractedTitle(value, url = '', sourceUrl = '') {
  let title = cleanText(value);
  if (!title) return '';

  title = title.replace(/^\d{2,}\s+/, '');
  title = title.replace(/\s+/g, ' ').trim();

  if (title.includes('•')) {
    title = title.split('•')[0].trim();
  }

  const slugTitle = titleFromUrl(url, sourceUrl).replace(/^\d{2,}\s+/, '').trim();
  if (slugTitle && (
    title.length > 120
    || /\b\d{1,2}\.?\s+(jan|januar|feb|februar|mar|mars|apr|april|mai|may|jun|juni|jul|juli|aug|september|okt|oct|nov|des|dec)\b/i.test(title)
    || /\b(klasse|snarest|sommerjobb|deltid|fulltid|masteroppgave)\b/i.test(title)
  )) {
    return slugTitle;
  }

  return title;
}

function isRejectedJobTitle(value) {
  const title = cleanText(value).toLowerCase();
  if (!title) return true;
  if (/^!?\[?\s*image\s*\d*\s*:/i.test(title)) return true;
  if (/^image\s*\d*\s*:/i.test(title)) return true;
  if (/\b(logo|banner|thumbnail|cover image|job posting image)\b/i.test(title)) return true;
  if (/^(sorter|sort|filter|filtrer|sok|søk|jobbtyper|sted|location|locations|category|categories)$/i.test(title)) return true;
  if (/^.+\s+job posting$/i.test(title) && title.split(/\s+/).length <= 6) return true;
  return false;
}

function parseRelativeDate(rawText, referenceDate = new Date()) {
  const text = String(rawText || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!text) return null;

  if (/(just now|akkurat naa|na nettopp)/.test(text)) return new Date(referenceDate);
  if (/(today|i dag)/.test(text)) return new Date(referenceDate);
  if (/(yesterday|i gaar|igaar)/.test(text)) {
    const d = new Date(referenceDate);
    d.setDate(d.getDate() - 1);
    return d;
  }

  const patterns = [
    /(?:lagt ut for|posted)\s*(\d+)\s*([a-zA-Z.]+)/i,
    /(\d+)\s*([a-zA-Z.]+)\s*(?:siden|ago)/i
  ];

  let amount = null;
  let unitRaw = '';

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match) {
      amount = Number(match[1]);
      unitRaw = String(match[2] || '').toLowerCase().replace(/\./g, '');
      break;
    }
  }

  if (!Number.isFinite(amount) || amount < 0 || !unitRaw) return null;

  const unit = unitRaw
    .replace('minutt', 'minute')
    .replace('minutter', 'minutes')
    .replace('time', 'hour')
    .replace('timer', 'hours')
    .replace('dag', 'day')
    .replace('dager', 'days')
    .replace('uke', 'week')
    .replace('uker', 'weeks')
    .replace('maaned', 'month')
    .replace('maaneder', 'months')
    .replace('mnd', 'month');

  const date = new Date(referenceDate);
  if (unit.startsWith('minute')) date.setMinutes(date.getMinutes() - amount);
  else if (unit.startsWith('hour')) date.setHours(date.getHours() - amount);
  else if (unit.startsWith('day')) date.setDate(date.getDate() - amount);
  else if (unit.startsWith('week')) date.setDate(date.getDate() - (amount * 7));
  else if (unit.startsWith('month')) date.setMonth(date.getMonth() - amount);
  else return null;

  return date;
}

function parseAbsoluteDate(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return null;

  let match = raw.match(/\b(\d{4})-(\d{2})-(\d{2})\b/);
  if (match) {
    const y = Number(match[1]);
    const m = Number(match[2]) - 1;
    const d = Number(match[3]);
    const date = new Date(Date.UTC(y, m, d));
    if (!Number.isNaN(date.getTime())) return date;
  }

  match = raw.match(/\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b/);
  if (match) {
    const d = Number(match[1]);
    const m = Number(match[2]) - 1;
    const yRaw = Number(match[3]);
    const y = yRaw < 100 ? 2000 + yRaw : yRaw;
    const date = new Date(Date.UTC(y, m, d));
    if (!Number.isNaN(date.getTime())) return date;
  }

  match = raw.match(/\b(\d{1,2})\.?\s+([a-zA-Z.]+)\s+(\d{4})\b/);
  if (match) {
    const d = Number(match[1]);
    const monthKey = String(match[2]).toLowerCase().replace(/\./g, '');
    const y = Number(match[3]);
    const month = MONTH_MAP[monthKey];
    if (Number.isInteger(month)) {
      const date = new Date(Date.UTC(y, month, d));
      if (!Number.isNaN(date.getTime())) return date;
    }
  }

  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function toIsoDate(dateLike) {
  const date = dateLike instanceof Date ? dateLike : new Date(dateLike);
  if (Number.isNaN(date.getTime())) return '';
  return date.toISOString().slice(0, 10);
}

function normalizePostedDate(value, referenceDate = new Date()) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  const rel = parseRelativeDate(raw, referenceDate);
  if (rel) return toIsoDate(rel);

  const abs = parseAbsoluteDate(raw);
  if (abs) return toIsoDate(abs);

  return '';
}

function parseLocationFromText(value) {
  const text = cleanText(value).toLowerCase();
  if (!text) return '';

  const hits = LOCATION_HINTS.filter((hint) => text.includes(hint));
  if (!hits.length) return '';
  const unique = [...new Set(hits)];
  return unique.map((item) => item.replace(/\b\w/g, (c) => c.toUpperCase())).join(', ');
}

function flattenObjects(value, bucket = []) {
  if (!value) return bucket;
  if (Array.isArray(value)) {
    for (const item of value) flattenObjects(item, bucket);
    return bucket;
  }
  if (typeof value === 'object') {
    bucket.push(value);
    for (const item of Object.values(value)) flattenObjects(item, bucket);
  }
  return bucket;
}

function parseLocationFromJsonLd(jobLocation) {
  if (!jobLocation) return '';
  if (typeof jobLocation === 'string') return cleanText(jobLocation);

  if (Array.isArray(jobLocation)) {
    const joined = jobLocation.map((item) => parseLocationFromJsonLd(item)).filter(Boolean);
    return [...new Set(joined)].join(', ');
  }

  if (typeof jobLocation === 'object') {
    const address = jobLocation.address || jobLocation;
    if (typeof address === 'string') return cleanText(address);
    if (address && typeof address === 'object') {
      const parts = [
        address.streetAddress,
        address.addressLocality,
        address.addressRegion,
        address.addressCountry
      ].filter(Boolean).map((part) => cleanText(part));
      return [...new Set(parts)].join(', ');
    }
  }

  return '';
}

function extractJobsFromJsonLd(text, sourceUrl) {
  if (!/<script/i.test(text)) return [];

  const jobs = [];
  const scriptRegex = /<script[^>]*type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;

  for (const match of text.matchAll(scriptRegex)) {
    const raw = String(match[1] || '').trim();
    if (!raw) continue;

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      continue;
    }

    const nodes = flattenObjects(parsed);
    for (const node of nodes) {
      const nodeType = node?.['@type'];
      const types = Array.isArray(nodeType) ? nodeType : [nodeType];
      const isJobPosting = types.some((t) => String(t || '').toLowerCase() === 'jobposting');
      if (!isJobPosting) continue;

      const title = cleanText(node.title || node.name || '');
      if (!title) continue;

      const url = toAbsoluteUrl(node.url || node.applyUrl || sourceUrl, sourceUrl) || sourceUrl;
      jobs.push({
        company: cleanText(node?.hiringOrganization?.name || ''),
        title,
        location: parseLocationFromJsonLd(node.jobLocation || node.location),
        posted_date: normalizePostedDate(node.datePosted || node.dateCreated || node.validFrom || ''),
        url,
        snippet: cleanText(JSON.stringify(node).slice(0, 500))
      });
    }
  }

  return jobs;
}

const GENERIC_TITLE_KEYS = [
  'title',
  'name',
  'text',
  'jobTitle',
  'job_title',
  'position',
  'role',
  'postingTitle',
  'displayName'
];

const GENERIC_URL_KEYS = [
  'url',
  'absolute_url',
  'absoluteUrl',
  'hostedUrl',
  'applyUrl',
  'jobUrl',
  'externalUrl',
  'externalPath',
  'path',
  'link',
  'href'
];

const GENERIC_COMPANY_KEYS = [
  'company',
  'companyName',
  'organization',
  'employer',
  'bank',
  'hiringOrganization'
];

const GENERIC_LOCATION_KEYS = [
  'location',
  'locations',
  'locationsText',
  'jobLocation',
  'city',
  'office',
  'offices'
];

const GENERIC_DATE_KEYS = [
  'posted_date',
  'postedDate',
  'date_posted',
  'datePosted',
  'published_at',
  'publishedAt',
  'publishedDate',
  'created_at',
  'createdAt',
  'releasedDate',
  'firstPublished',
  'updated_at',
  'updatedAt'
];

const GENERIC_ID_KEYS = [
  'id',
  'jobId',
  'job_id',
  'requisitionId',
  'requisition_id',
  'reqId',
  'externalId',
  'external_id'
];

function nestedHiringOrgName(value) {
  if (!value) return '';
  if (typeof value === 'string') return cleanText(value);
  if (typeof value === 'object') {
    return firstStringFromKeys(value, ['name', 'companyName', 'legalName']);
  }
  return '';
}

function rawJobFromGenericObject(node, sourceUrl, defaultCompany = '') {
  if (!node || typeof node !== 'object' || Array.isArray(node)) return null;

  const title = firstStringFromKeys(node, GENERIC_TITLE_KEYS);
  if (!title || title.length < 3 || title.length > 220) return null;

  const urlRaw = firstStringFromKeys(node, GENERIC_URL_KEYS);
  const idRaw = firstStringFromKeys(node, GENERIC_ID_KEYS);
  let url = toAbsoluteUrl(urlRaw, sourceUrl);
  if (!url && idRaw && looksLikeJobText(`${title} ${compactJsonSnippet(node, 240)}`)) {
    url = `${sourceUrl.replace(/#.*$/, '')}#job-${encodeURIComponent(idRaw)}`;
  }
  if (!url) return null;

  const companyRaw = firstStringFromKeys(node, GENERIC_COMPANY_KEYS)
    || nestedHiringOrgName(node.hiringOrganization)
    || defaultCompany;

  const locationRaw = GENERIC_LOCATION_KEYS
    .map((key) => cleanLocationValue(node[key]))
    .find(Boolean) || '';

  const dateRaw = firstStringFromKeys(node, GENERIC_DATE_KEYS);
  const snippet = compactJsonSnippet(node);
  const signal = `${title} ${urlRaw} ${companyRaw} ${locationRaw} ${snippet}`;
  const hasJobKey = Object.keys(node).some((key) => /job|posting|position|requisition|opening|vacanc/i.test(key));

  if (!looksLikeJobUrl(url, sourceUrl) && !looksLikeJobText(signal) && !hasJobKey) {
    return null;
  }

  return {
    id: idRaw,
    company: cleanText(companyRaw),
    title,
    location: locationRaw,
    posted_date: normalizeAnyDate(dateRaw),
    url,
    snippet
  };
}

function extractJobsFromJsonObject(value, sourceUrl, defaultCompany = '') {
  const jobs = [];
  const nodes = flattenObjects(value);

  for (const node of nodes) {
    const job = rawJobFromGenericObject(node, sourceUrl, defaultCompany);
    if (job) jobs.push(job);
    if (jobs.length >= MAX_ATS_JOBS) break;
  }

  return jobs;
}

function decodeBasicHtmlEntities(value) {
  return String(value || '')
    .replace(/&quot;/g, '"')
    .replace(/&#34;/g, '"')
    .replace(/&#x22;/gi, '"')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function extractJobsFromEmbeddedJson(text, sourceUrl) {
  if (!/<script/i.test(text)) return [];

  const jobs = [];
  const scriptRegex = /<script\b([^>]*)>([\s\S]*?)<\/script>/gi;

  for (const match of text.matchAll(scriptRegex)) {
    const attributes = String(match[1] || '').toLowerCase();
    const isJsonScript = /type=["']application\/json["']/.test(attributes)
      || /id=["']__next_data__["']/.test(attributes)
      || /id=["']__nuxt/.test(attributes)
      || /id=["']app-data/.test(attributes);

    if (!isJsonScript) continue;

    const raw = decodeBasicHtmlEntities(String(match[2] || '').trim());
    if (!raw || raw.length > 1_500_000) continue;

    try {
      const parsed = JSON.parse(raw);
      jobs.push(...extractJobsFromJsonObject(parsed, sourceUrl));
    } catch {
      // Ignore non-JSON script payloads.
    }

    if (jobs.length >= MAX_ATS_JOBS) break;
  }

  return jobs;
}

function extractJobsFromMarkdownLinks(text, sourceUrl) {
  const jobs = [];
  const linkRegex = /\[([^\]]{1,260})\]\((https?:\/\/[^\s)]+|\/[^\s)]+)\)/gi;

  for (const match of String(text || '').matchAll(linkRegex)) {
    const title = cleanText(match[1]);
    const url = toAbsoluteUrl(match[2], sourceUrl);
    if (!title || !url) continue;
    if (!looksLikeJobUrl(url, sourceUrl) && !looksLikeJobText(title)) continue;

    jobs.push({
      company: '',
      title,
      location: parseLocationFromText(title),
      posted_date: normalizePostedDate(title),
      url,
      snippet: title
    });

    if (jobs.length >= MAX_ATS_JOBS) break;
  }

  return jobs;
}

function extractTitleFromSnippet(snippet, fallbackUrl, sourceUrl) {
  const cleaned = cleanText(snippet);
  if (!cleaned) return titleFromUrl(fallbackUrl, sourceUrl);

  const heading = cleaned.match(/(?:^|\s)###\s*([^#]+)$/i);
  if (heading && cleanText(heading[1])) {
    return cleanText(heading[1]).slice(0, 180);
  }

  const cut = cleaned.split(/\b(Frist|Deadline|Lagt ut|Posted|Apply|Sok)\b/i)[0];
  const candidate = cleanText(cut).slice(0, 180);
  if (candidate && candidate.length >= 6) return candidate;

  return titleFromUrl(fallbackUrl, sourceUrl);
}

function extractJobsFromHtmlAnchors(text, sourceUrl) {
  if (!/<a\b/i.test(text)) return [];

  const jobs = [];
  const anchorRegex = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let count = 0;

  for (const match of text.matchAll(anchorRegex)) {
    count += 1;
    if (count > 6000) break;

    const url = toAbsoluteUrl(match[1], sourceUrl);
    if (!url) continue;

    const anchorText = cleanText(match[2]);
    if (!looksLikeJobUrl(url, sourceUrl) && !looksLikeJobText(anchorText)) continue;

    const title = anchorText || titleFromUrl(url, sourceUrl);
    if (!title) continue;

    jobs.push({
      company: '',
      title,
      location: parseLocationFromText(anchorText),
      posted_date: normalizePostedDate(anchorText),
      url,
      snippet: anchorText
    });
  }

  return jobs;
}

function extractJobsFromRelevantLines(text, sourceUrl) {
  const jobs = [];
  const lines = String(text || '').split(/\r?\n/);
  const urlPattern = /(https?:\/\/[^\s)"'<>]+|\/(?:karriere|career|careers|jobs?|joblistings?|stillinger?|stilling)[^\s)"'<>]*)/gi;

  const indexes = new Set();
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const lower = line.toLowerCase();
    const hasUrl = urlPattern.test(line);
    urlPattern.lastIndex = 0;
    const hasSignal = hasKeyword(lower, JOB_KEYWORDS) || hasKeyword(lower, META_KEYWORDS);
    if (!hasUrl && !hasSignal) continue;

    for (let j = -2; j <= 2; j += 1) {
      const idx = i + j;
      if (idx >= 0 && idx < lines.length) indexes.add(idx);
    }
  }

  const sorted = [...indexes].sort((a, b) => a - b);
  for (const idx of sorted) {
    const snippet = lines.slice(Math.max(0, idx - 1), Math.min(lines.length, idx + 3)).join(' ');
    if (snippet.length > 1400) continue;
    if (/self\.__next_f\.push|webpack|polyfills|__next|<script/i.test(snippet)) continue;
    const urls = [...snippet.matchAll(urlPattern)].map((match) => match[1]).filter(Boolean);
    urlPattern.lastIndex = 0;
    if (!urls.length) continue;

    for (const rawUrl of urls.slice(0, 3)) {
      const url = toAbsoluteUrl(rawUrl, sourceUrl);
      if (!url) continue;

      const title = extractTitleFromSnippet(snippet, url, sourceUrl);
      if (!title) continue;

      if (!looksLikeJobUrl(url, sourceUrl)) {
        continue;
      }

      jobs.push({
        company: '',
        title,
        location: parseLocationFromText(snippet),
        posted_date: normalizePostedDate(snippet),
        url,
        snippet: cleanText(snippet).slice(0, 400)
      });
    }
  }

  return jobs;
}

function extractHeuristicJobsFromText(text, sourceUrl) {
  const candidates = [
    ...extractJobsFromJsonLd(text, sourceUrl),
    ...extractJobsFromEmbeddedJson(text, sourceUrl),
    ...extractJobsFromMarkdownLinks(text, sourceUrl),
    ...extractJobsFromHtmlAnchors(text, sourceUrl),
    ...extractJobsFromRelevantLines(text, sourceUrl)
  ];
  return candidates;
}

function scoreFollowupLink(url, sourceUrl) {
  const absolute = toAbsoluteUrl(url, sourceUrl);
  if (!absolute) return Number.NEGATIVE_INFINITY;

  const parsed = new URL(absolute);
  const sourceHost = new URL(sourceUrl).hostname;
  const lower = absolute.toLowerCase();
  const pathLower = parsed.pathname.toLowerCase();

  let score = 0;
  if (looksLikeJobUrl(absolute, sourceUrl)) score += 40;
  if (hasKeyword(pathLower, JOB_KEYWORDS)) score += 16;
  if (parsed.hostname === sourceHost) score += 8;
  if (ATS_HOST_HINTS.some((hint) => parsed.hostname.includes(hint))) score += 35;
  if (/(\/job\/|\/jobs\/|\/careers?\/|\/karriere\/|\/stillinger?\/|\/positions?\/|\/vacanc)/.test(pathLower)) score += 20;

  if (/(\/privacy|\/terms|\/cookies|\/contact|\/about|\/faq|\/news|\/blog)(\/|$)/.test(lower)) score -= 35;
  if (/\.(png|jpe?g|gif|svg|webp|ico|pdf|css|js|map|woff2?|ttf|zip|docx?)$/.test(pathLower)) score -= 80;
  if (canonicalUrl(absolute, sourceUrl) === canonicalUrl(sourceUrl, sourceUrl)) score -= 90;

  return score;
}

function extractFollowupLinksFromText(text, sourceUrl) {
  const body = String(text || '');
  if (!body) return [];

  const rawLinks = [];
  const markdownLinkRegex = /\[[^\]]{1,240}\]\((https?:\/\/[^\s)]+|\/[^\s)]+)\)/gi;
  for (const match of body.matchAll(markdownLinkRegex)) {
    rawLinks.push(match[1]);
  }

  const anchorHrefRegex = /<a\b[^>]*href=["']([^"']+)["']/gi;
  for (const match of body.matchAll(anchorHrefRegex)) {
    rawLinks.push(match[1]);
  }

  const bareUrlRegex = /https?:\/\/[^\s)"'<>]+/gi;
  for (const match of body.matchAll(bareUrlRegex)) {
    rawLinks.push(match[0]);
  }

  const ranked = new Map();
  for (const rawLink of rawLinks) {
    const absolute = toAbsoluteUrl(rawLink, sourceUrl);
    if (!absolute) continue;
    const score = scoreFollowupLink(absolute, sourceUrl);
    if (score < 20) continue;

    const key = canonicalUrl(absolute, sourceUrl);
    const existing = ranked.get(key);
    if (!existing || score > existing.score) {
      ranked.set(key, { url: absolute, score });
    }
  }

  return [...ranked.values()]
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_FOLLOWUP_LINKS)
    .map((entry) => entry.url);
}

function extractLinkCandidatesFromText(text, sourceUrl) {
  const body = String(text || '');
  if (!body) return [];

  const candidates = new Map();

  function addCandidate(rawUrl, label = '', snippet = '') {
    const url = toAbsoluteUrl(rawUrl, sourceUrl);
    if (!url) return;
    let parsed;
    try {
      parsed = new URL(url);
    } catch {
      return;
    }
    if (/\.(png|jpe?g|gif|svg|webp|ico|pdf|css|js|map|woff2?|ttf|zip|docx?)$/i.test(parsed.pathname)) return;
    if (/(\/privacy|\/terms|\/cookies|\/contact|\/about|\/faq|\/news|\/blog)(\/|$)/i.test(url)) return;

    const key = canonicalUrl(url, sourceUrl);
    const textLabel = cleanText(label || titleFromUrl(url, sourceUrl)).slice(0, 180);
    if (isRejectedJobTitle(textLabel)) return;
    const textSnippet = cleanText(snippet || label || '').slice(0, 260);
    const score = scoreFollowupLink(url, sourceUrl)
      + (looksLikeJobText(`${textLabel} ${textSnippet}`) ? 18 : 0)
      + (ATS_HOST_HINTS.some((hint) => parsed.hostname.includes(hint)) ? 30 : 0);
    if (score < 12) return;

    const existing = candidates.get(key);
    if (!existing || score > existing.score) {
      candidates.set(key, {
        url,
        text: textLabel,
        snippet: textSnippet,
        score
      });
    }
  }

  const markdownLinkRegex = /\[([^\]]{1,240})\]\((https?:\/\/[^\s)]+|\/[^\s)]+)\)/gi;
  for (const match of body.matchAll(markdownLinkRegex)) {
    if (match.index > 0 && body[match.index - 1] === '!') continue;
    addCandidate(match[2], match[1], match[0]);
  }

  const anchorHrefRegex = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  for (const match of body.matchAll(anchorHrefRegex)) {
    addCandidate(match[1], cleanText(match[2]), match[0]);
  }

  const bareUrlRegex = /https?:\/\/[^\s)"'<>]+/gi;
  for (const match of body.matchAll(bareUrlRegex)) {
    addCandidate(match[0], titleFromUrl(match[0], sourceUrl), match[0]);
  }

  return [...candidates.values()]
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_AI_LINK_CANDIDATES);
}

function isLikelyLocaleSegment(segment) {
  return /^[a-z]{2}(?:-[a-z]{2})?$/i.test(segment);
}

function workdayInfoFromUrl(sourceUrl) {
  const parsed = new URL(sourceUrl);
  const hostMatch = parsed.hostname.match(/^([^.]+)\.wd(\d+)\.(?:myworkdayjobs|myworkdaysite)\.com$/i);
  if (!hostMatch) return null;

  const directApiMatch = parsed.pathname.match(/\/wday\/cxs\/([^/]+)\/([^/]+)\/jobs/i);
  const tenant = decodeURIComponent(directApiMatch?.[1] || hostMatch[1]);
  const instance = hostMatch[2];
  const segments = pathSegmentsFromUrl(sourceUrl);
  const locale = segments.find(isLikelyLocaleSegment) || 'en-US';
  const boardCandidates = [];

  if (directApiMatch?.[2]) boardCandidates.push(decodeURIComponent(directApiMatch[2]));

  for (let i = 0; i < segments.length; i += 1) {
    const segment = segments[i];
    const lower = segment.toLowerCase();
    if (isLikelyLocaleSegment(segment)) {
      if (segments[i + 1]) boardCandidates.push(segments[i + 1]);
      continue;
    }
    if (['job', 'jobs', 'search', 'details'].includes(lower)) continue;
    if (lower.length > 1) boardCandidates.push(segment);
  }

  return {
    tenant,
    instance,
    locale,
    boards: uniqueNonEmpty(boardCandidates).slice(0, 4),
    origin: parsed.origin
  };
}

async function fetchWorkdayAtsJobs(sourceUrl) {
  const info = workdayInfoFromUrl(sourceUrl);
  if (!info?.boards.length) return { jobs: [], sources: [], errors: [] };

  const jobs = [];
  const sources = [];
  const errors = [];

  for (const board of info.boards) {
    const apiUrl = `https://${info.tenant}.wd${info.instance}.myworkdayjobs.com/wday/cxs/${encodeURIComponent(info.tenant)}/${encodeURIComponent(board)}/jobs`;
    let offset = 0;
    const limit = 100;
    let total = Infinity;

    while (offset < total && jobs.length < MAX_ATS_JOBS) {
      try {
        const payload = {
          limit,
          offset,
          searchText: '',
          appliedFacets: {}
        };
        const data = await fetchJsonWithTimeout(apiUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });

        const postings = Array.isArray(data?.jobPostings) ? data.jobPostings : [];
        total = Number(data?.total || postings.length || 0);
        if (!postings.length) break;

        sources.push(`workday:${board}`);

        for (const job of postings) {
          const externalPath = cleanText(job.externalPath || job.url || '');
          const url = externalPath
            ? `${info.origin}/${info.locale}/${encodeURIComponent(board)}${externalPath.startsWith('/') ? externalPath : `/${externalPath}`}`
            : sourceUrl;

          jobs.push({
            id: job.bulletFields?.[0] || job.jobId || job.id || externalPath,
            company: titleCaseFromSlug(info.tenant),
            title: cleanText(job.title || job.name || ''),
            location: cleanLocationValue(job.locationsText || job.location || job.locations),
            posted_date: normalizeAnyDate(job.postedOn || job.postedDate || job.startDate || job.updatedOn),
            url,
            snippet: compactJsonSnippet(job)
          });
        }

        offset += postings.length;
        if (postings.length < limit) break;
      } catch (error) {
        errors.push(`workday:${board}: ${error.message}`);
        break;
      }
    }
  }

  return { jobs, sources: uniqueNonEmpty(sources), errors };
}

function greenhouseBoardFromUrl(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.includes('greenhouse.io')) return '';

  const apiMatch = parsed.pathname.match(/\/v1\/boards\/([^/]+)/i);
  if (apiMatch) return decodeURIComponent(apiMatch[1]);

  const [firstSegment] = pathSegmentsFromUrl(sourceUrl);
  if (!firstSegment || ['embed', 'jobs'].includes(firstSegment.toLowerCase())) return '';
  return firstSegment;
}

async function fetchGreenhouseAtsJobs(sourceUrl) {
  const board = greenhouseBoardFromUrl(sourceUrl);
  if (!board) return { jobs: [], sources: [], errors: [] };

  const apiUrl = `https://boards-api.greenhouse.io/v1/boards/${encodeURIComponent(board)}/jobs?content=true`;
  try {
    const data = await fetchJsonWithTimeout(apiUrl);
    const postings = Array.isArray(data?.jobs) ? data.jobs : [];
    const jobs = postings.slice(0, MAX_ATS_JOBS).map((job) => ({
      id: job.id,
      company: titleCaseFromSlug(board),
      title: cleanText(job.title),
      location: cleanLocationValue(job.location || job.offices),
      posted_date: normalizeAnyDate(job.first_published || job.updated_at || job.created_at),
      url: toAbsoluteUrl(job.absolute_url || job.url, sourceUrl) || sourceUrl,
      snippet: compactJsonSnippet(job)
    }));
    return { jobs, sources: [`greenhouse:${board}`], errors: [] };
  } catch (error) {
    return { jobs: [], sources: [], errors: [`greenhouse:${board}: ${error.message}`] };
  }
}

function leverCompanyFromUrl(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.includes('lever.co')) return '';
  const [company] = pathSegmentsFromUrl(sourceUrl);
  return company || '';
}

async function fetchLeverAtsJobs(sourceUrl) {
  const company = leverCompanyFromUrl(sourceUrl);
  if (!company) return { jobs: [], sources: [], errors: [] };

  const apiUrl = `https://api.lever.co/v0/postings/${encodeURIComponent(company)}?mode=json`;
  try {
    const data = await fetchJsonWithTimeout(apiUrl);
    const postings = Array.isArray(data) ? data : [];
    const jobs = postings.slice(0, MAX_ATS_JOBS).map((job) => ({
      id: job.id,
      company: titleCaseFromSlug(company),
      title: cleanText(job.text || job.title),
      location: cleanLocationValue(job.categories?.location || job.location),
      posted_date: normalizeAnyDate(job.createdAt || job.updatedAt),
      url: toAbsoluteUrl(job.hostedUrl || job.applyUrl || job.url, sourceUrl) || sourceUrl,
      snippet: compactJsonSnippet(job)
    }));
    return { jobs, sources: [`lever:${company}`], errors: [] };
  } catch (error) {
    return { jobs: [], sources: [], errors: [`lever:${company}: ${error.message}`] };
  }
}

function ashbyOrgFromUrl(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.includes('ashbyhq.com')) return '';
  const [org] = pathSegmentsFromUrl(sourceUrl);
  return org || '';
}

async function fetchAshbyAtsJobs(sourceUrl) {
  const org = ashbyOrgFromUrl(sourceUrl);
  if (!org) return { jobs: [], sources: [], errors: [] };

  const apiUrl = `https://api.ashbyhq.com/posting-api/job-board/${encodeURIComponent(org)}`;
  try {
    const data = await fetchJsonWithTimeout(apiUrl);
    const postings = Array.isArray(data?.jobs) ? data.jobs : [];
    const jobs = postings.slice(0, MAX_ATS_JOBS).map((job) => ({
      id: job.id || job.jobId,
      company: titleCaseFromSlug(org),
      title: cleanText(job.title || job.name),
      location: cleanLocationValue(job.location || job.locationName),
      posted_date: normalizeAnyDate(job.publishedDate || job.createdAt || job.updatedAt),
      url: toAbsoluteUrl(job.jobUrl || job.applyUrl || job.url, sourceUrl)
        || toAbsoluteUrl(`/${org}/${job.id || job.jobId || ''}`, sourceUrl)
        || sourceUrl,
      snippet: compactJsonSnippet(job)
    }));
    return { jobs, sources: [`ashby:${org}`], errors: [] };
  } catch (error) {
    return { jobs: [], sources: [], errors: [`ashby:${org}: ${error.message}`] };
  }
}

function smartRecruitersCompanyFromUrl(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.includes('smartrecruiters.com')) return '';
  const [company] = pathSegmentsFromUrl(sourceUrl);
  return company || '';
}

async function fetchSmartRecruitersAtsJobs(sourceUrl) {
  const company = smartRecruitersCompanyFromUrl(sourceUrl);
  if (!company) return { jobs: [], sources: [], errors: [] };

  const jobs = [];
  const errors = [];
  let offset = 0;
  const limit = 100;
  let total = Infinity;

  while (offset < total && jobs.length < MAX_ATS_JOBS) {
    const apiUrl = `https://api.smartrecruiters.com/v1/companies/${encodeURIComponent(company)}/postings?limit=${limit}&offset=${offset}`;
    try {
      const data = await fetchJsonWithTimeout(apiUrl);
      const postings = Array.isArray(data?.content) ? data.content : [];
      total = Number(data?.totalFound || postings.length || 0);
      if (!postings.length) break;

      for (const job of postings) {
        jobs.push({
          id: job.id || job.uuid,
          company: cleanText(job.company?.name || titleCaseFromSlug(company)),
          title: cleanText(job.name || job.title),
          location: cleanLocationValue(job.location),
          posted_date: normalizeAnyDate(job.releasedDate || job.createdOn || job.updatedOn),
          url: toAbsoluteUrl(job.ref || job.applyUrl || job.url, sourceUrl)
            || `https://jobs.smartrecruiters.com/${encodeURIComponent(company)}/${job.id || job.uuid || ''}`,
          snippet: compactJsonSnippet(job)
        });
      }

      offset += postings.length;
      if (postings.length < limit) break;
    } catch (error) {
      errors.push(`smartrecruiters:${company}: ${error.message}`);
      break;
    }
  }

  return {
    jobs,
    sources: jobs.length ? [`smartrecruiters:${company}`] : [],
    errors
  };
}

async function fetchTeamtailorAtsJobs(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.includes('teamtailor.com')) return { jobs: [], sources: [], errors: [] };

  const endpoints = [
    `${parsed.origin}/jobs.json`,
    `${parsed.origin}/jobs?format=json`
  ];
  const errors = [];

  for (const endpoint of endpoints) {
    try {
      const data = await fetchJsonWithTimeout(endpoint);
      const jobs = extractJobsFromJsonObject(data, sourceUrl, parsed.hostname.replace(/\.teamtailor\.com$/i, ''));
      if (jobs.length) {
        return { jobs: jobs.slice(0, MAX_ATS_JOBS), sources: [`teamtailor:${parsed.hostname}`], errors };
      }
    } catch (error) {
      errors.push(`teamtailor:${endpoint}: ${error.message}`);
    }
  }

  return { jobs: [], sources: [], errors };
}

async function fetchRecruiteeAtsJobs(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.endsWith('.recruitee.com')) return { jobs: [], sources: [], errors: [] };

  const apiUrl = `${parsed.origin}/api/offers/`;
  try {
    const data = await fetchJsonWithTimeout(apiUrl);
    const postings = Array.isArray(data?.offers) ? data.offers : [];
    const jobs = postings.slice(0, MAX_ATS_JOBS).map((job) => ({
      id: job.id || job.slug,
      company: titleCaseFromSlug(parsed.hostname.replace(/\.recruitee\.com$/i, '')),
      title: cleanText(job.title),
      location: cleanLocationValue(job.location || job.city || job.country),
      posted_date: normalizeAnyDate(job.created_at || job.updated_at),
      url: toAbsoluteUrl(job.careers_url || job.url || job.slug, sourceUrl) || sourceUrl,
      snippet: compactJsonSnippet(job)
    }));
    return { jobs, sources: jobs.length ? [`recruitee:${parsed.hostname}`] : [], errors: [] };
  } catch (error) {
    return { jobs: [], sources: [], errors: [`recruitee:${parsed.hostname}: ${error.message}`] };
  }
}

async function fetchBreezyAtsJobs(sourceUrl) {
  const parsed = new URL(sourceUrl);
  if (!parsed.hostname.endsWith('.breezy.hr')) return { jobs: [], sources: [], errors: [] };

  const apiUrl = `${parsed.origin}/json`;
  try {
    const data = await fetchJsonWithTimeout(apiUrl);
    const postings = Array.isArray(data) ? data : (Array.isArray(data?.jobs) ? data.jobs : []);
    const jobs = postings.slice(0, MAX_ATS_JOBS).map((job) => ({
      id: job.id || job._id || job.slug,
      company: titleCaseFromSlug(parsed.hostname.replace(/\.breezy\.hr$/i, '')),
      title: cleanText(job.name || job.title),
      location: cleanLocationValue(job.location),
      posted_date: normalizeAnyDate(job.creation_date || job.created_at || job.updated_at),
      url: toAbsoluteUrl(job.url || job.apply_url || job.slug, sourceUrl) || sourceUrl,
      snippet: compactJsonSnippet(job)
    }));
    return { jobs, sources: jobs.length ? [`breezy:${parsed.hostname}`] : [], errors: [] };
  } catch (error) {
    return { jobs: [], sources: [], errors: [`breezy:${parsed.hostname}: ${error.message}`] };
  }
}

async function fetchAtsJobs(sourceUrl) {
  const adapters = [
    fetchWorkdayAtsJobs,
    fetchGreenhouseAtsJobs,
    fetchLeverAtsJobs,
    fetchAshbyAtsJobs,
    fetchSmartRecruitersAtsJobs,
    fetchTeamtailorAtsJobs,
    fetchRecruiteeAtsJobs,
    fetchBreezyAtsJobs
  ];

  const results = await Promise.allSettled(adapters.map((adapter) => adapter(sourceUrl)));
  const jobs = [];
  const sources = [];
  const errors = [];

  for (const result of results) {
    if (result.status === 'rejected') {
      errors.push(result.reason?.message || String(result.reason));
      continue;
    }
    jobs.push(...result.value.jobs);
    sources.push(...result.value.sources);
    errors.push(...result.value.errors);
  }

  return {
    jobs: jobs.slice(0, MAX_ATS_JOBS),
    sources: uniqueNonEmpty(sources),
    errors
  };
}

function mergeAndNormalizeJobs(rawJobs, sourceUrl) {
  const host = new URL(sourceUrl).hostname;
  const nowIso = new Date().toISOString();
  const byCanonicalUrl = new Map();

  function titleQualityScore(value) {
    const title = String(value || '').trim();
    const lower = title.toLowerCase();
    let score = 0;
    score += Math.min(title.length, 110);
    if (title.length >= 12 && title.length <= 120) score += 45;
    if (title.length > 140) score -= 30;
    if (title.length > 180) score -= 70;
    if (/[a-z]/i.test(title)) score += 30;
    if (/\s/.test(title)) score += 15;
    if (looksLikeJobText(title)) score += 20;
    if (/•/.test(title)) score -= 20;
    if (/\b\d{1,2}[:.]\d{2}\b/.test(title)) score -= 20;
    if (/^(title:|url source:|markdown content:)/.test(lower)) score -= 120;
    if (/https?:\/\//.test(lower)) score -= 120;
    if (/^\s*[0-9a-f][0-9a-f\s-]{20,}\s*$/i.test(title)) score -= 80;
    if (/\.(js|css|png|jpe?g|svg|webp|woff2?|ttf)$/i.test(lower)) score -= 120;
    if (/^(sorter|sok|jobbtyper|sted)$/i.test(lower)) score -= 100;
    if (/\b(lagt ut|frist|deadline|heltid|deltid|fulltid|snarest)\b/i.test(title)) score -= 45;
    return score;
  }

  function candidateQualityScore(candidate) {
    let score = titleQualityScore(candidate.title);
    if (candidate.company && candidate.company !== host) score += 30;
    if (!candidate.company || candidate.company === host) score -= 15;
    if (candidate.posted_date) score += 8;
    if (candidate.location) score += 4;
    if (candidate.title.split(/\s+/).length > 20) score -= 15;
    return score;
  }

  for (const raw of rawJobs) {
    if (!raw || typeof raw !== 'object') continue;

    let url = toAbsoluteUrl(raw.url || raw.link || raw.href || sourceUrl, sourceUrl);
    if (!url) url = sourceUrl;

    const titleRaw = raw.title || raw.role || raw.position || raw.name || '';
    const title = cleanupExtractedTitle(titleRaw, url, sourceUrl) || titleFromUrl(url, sourceUrl);
    if (!title || title.length < 3) continue;
    if (isRejectedJobTitle(title)) continue;
    const titleLower = title.toLowerCase();
    if (/^(title:|url source:|markdown content:)/.test(titleLower)) continue;
    if (/https?:\/\//.test(titleLower)) continue;
    if (/\.(js|css|png|jpe?g|svg|webp|woff2?|ttf)$/.test(titleLower)) continue;
    if (/^\s*[0-9a-f][0-9a-f\s-]{20,}\s*$/i.test(title)) continue;
    if (!/[a-z]/i.test(title)) continue;
    const looksGenericBoardTitle = /^(jobbannonser|karrieremuligheter|careers?|jobs?|job listings?)$/i.test(titleLower);

    const signalText = `${title} ${raw.snippet || ''}`;
    const urlLooksLikeJob = looksLikeJobUrl(url, sourceUrl);
    if (!urlLooksLikeJob && !looksLikeJobText(signalText)) continue;
    if (!urlLooksLikeJob && looksGenericBoardTitle) continue;
    if (!urlLooksLikeJob && title.length < 18) continue;

    const company = cleanText(raw.company || raw.bank || raw.organization || raw.employer || '') || host;
    const location = cleanText(raw.location || raw.city || parseLocationFromText(raw.snippet || ''));
    const postedDate = normalizeAnyDate(
      raw.posted_date || raw.postedDate || raw.date_posted || raw.date || raw.published_at || raw.snippet || '',
      new Date()
    );

    const urlObj = new URL(url);
    const isExternalNonAts = urlObj.hostname !== host && !ATS_HOST_HINTS.some((hint) => urlObj.hostname.includes(hint));
    if (isExternalNonAts && company === host) continue;

    const normalizedUrlKey = canonicalJobUrlKey(url, sourceUrl);
    const explicitRawId = String(raw.id || raw.job_id || raw.jobId || raw.requisitionId || raw.reqId || '').trim();
    const rawId = /^ai[_\s-]/i.test(explicitRawId) ? '' : cleanText(explicitRawId);
    const urlJobId = jobIdFromUrl(url, sourceUrl);
    const stableJobId = urlJobId || rawId;
    const dedupeKey = stableJobId
      ? `${new URL(sourceUrl).hostname}:job:${stableJobId}`.toLowerCase()
      : normalizedUrlKey.toLowerCase();
    const candidate = {
      id: rawId || `ai_${Math.random().toString(16).slice(2, 12)}`,
      bank: company,
      company,
      title: title.slice(0, 220),
      location: location.slice(0, 140),
      url,
      posted_date: postedDate,
      found_at: nowIso,
      is_new: Boolean(postedDate)
    };

    const existing = byCanonicalUrl.get(dedupeKey);
    if (!existing) {
      byCanonicalUrl.set(dedupeKey, candidate);
    } else {
      const newScore = candidateQualityScore(candidate);
      const existingScore = candidateQualityScore(existing);
      if (newScore > existingScore) {
        byCanonicalUrl.set(dedupeKey, candidate);
      }
    }
  }

  const normalized = [...byCanonicalUrl.values()].slice(0, MAX_HEURISTIC_JOBS);
  return normalized;
}

function scoreJobSignal(text) {
  const sample = String(text || '').slice(0, 350_000).toLowerCase();
  let score = 0;

  score += countMatches(sample, /\b(job|jobs|career|careers|karriere|stilling|stillinger|internship|graduate|trainee|deltid|heltid)\b/g) * 2;
  score += countMatches(sample, /\b(frist|deadline|apply|sok|lagt ut|posted|publisert)\b/g);
  score += Math.min(countMatches(sample, /https?:\/\/[^\s)\]"]+/g), 60);
  score += countMatches(sample, /\/(karriere|career|careers|jobs|job|stilling|stillinger)\//g) * 4;

  if (sample.includes('markdown content') || sample.includes('### ')) score += 20;
  if (sample.includes('<script') && sample.length > 180_000) score -= 30;
  if (sample.length > 600_000) score -= 15;

  return score;
}

async function fetchTextWithTimeout(url, options = {}) {
  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs || FETCH_TIMEOUT_MS);
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const { headers = {}, timeoutMs: _timeoutMs, ...fetchOptions } = options;
  try {
    const response = await fetch(url, {
      ...fetchOptions,
      signal: controller.signal,
      headers: {
        Accept: 'text/html,application/xhtml+xml,text/plain,application/json;q=0.9,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (compatible; UpForwardJobBot/1.0)',
        ...headers
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const text = await response.text();
    if (!text.trim()) {
      throw new Error('Empty response');
    }
    return text;
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error(`Timeout after ${timeoutMs}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJsonWithTimeout(url, options = {}) {
  const text = await fetchTextWithTimeout(url, {
    ...options,
    timeoutMs: options.timeoutMs || ATS_FETCH_TIMEOUT_MS,
    headers: {
      Accept: 'application/json,text/plain;q=0.9,*/*;q=0.8',
      ...(options.headers || {})
    }
  });

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Invalid JSON from ${url}`);
  }
}

let playwrightModulePromise = null;

async function getPlaywrightModule() {
  if (!playwrightModulePromise) {
    playwrightModulePromise = import('playwright').catch((error) => ({ __loadError: error }));
  }

  const module = await playwrightModulePromise;
  if (module.__loadError) {
    throw new Error(`Playwright is not installed or could not load: ${module.__loadError.message}`);
  }

  return module;
}

async function dismissCommonCookieBanners(page) {
  const labels = [
    'Accept all',
    'Accept All',
    'Accept cookies',
    'I accept',
    'Allow all',
    'Godta alle',
    'Godta',
    'Aksepter',
    'Enig',
    'OK'
  ];

  for (const label of labels) {
    try {
      const button = page.getByRole('button', { name: label }).first();
      if (await button.isVisible({ timeout: 500 })) {
        await button.click({ timeout: 1000 });
        return;
      }
    } catch {
      // Cookie banners vary a lot; failure to click one should not block scraping.
    }
  }
}

async function autoScrollPage(page) {
  try {
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let totalHeight = 0;
        const distance = 700;
        const maxHeight = Math.max(document.body?.scrollHeight || 0, 12_000);
        const timer = setInterval(() => {
          window.scrollBy(0, distance);
          totalHeight += distance;
          if (totalHeight >= maxHeight || totalHeight >= 18_000) {
            clearInterval(timer);
            resolve();
          }
        }, 120);
      });
    });
  } catch {
    // Some pages disallow evaluation. The initial render is still useful.
  }
}

async function fetchRenderedSource(url) {
  if (['0', 'false', 'off', 'disabled'].includes(RENDERED_FETCH)) {
    throw new Error('Rendered fetch is disabled');
  }

  const { chromium } = await getPlaywrightModule();
  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-dev-shm-usage']
  });

  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 1200 },
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      locale: 'en-US'
    });
    const page = await context.newPage();
    page.setDefaultTimeout(PLAYWRIGHT_TIMEOUT_MS);
    page.setDefaultNavigationTimeout(PLAYWRIGHT_TIMEOUT_MS);

    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: PLAYWRIGHT_TIMEOUT_MS });
    await page.waitForLoadState('networkidle', { timeout: Math.min(PLAYWRIGHT_TIMEOUT_MS, 8000) }).catch(() => {});
    await dismissCommonCookieBanners(page);
    await autoScrollPage(page);
    await page.waitForTimeout(600).catch(() => {});

    const title = await page.title().catch(() => '');
    const bodyText = await page.locator('body').innerText({ timeout: 3000 }).catch(() => '');
    const links = await page.$$eval('a[href]', (anchors, maxLinks) => anchors
      .slice(0, maxLinks)
      .map((anchor) => {
        const text = (anchor.innerText || anchor.textContent || '').replace(/\s+/g, ' ').trim();
        const href = anchor.href;
        return text && href ? `[${text}](${href})` : '';
      })
      .filter(Boolean)
      .join('\n'), MAX_RENDERED_LINKS).catch(() => '');
    const html = await page.content().catch(() => '');

    return [
      `Rendered title: ${title}`,
      '',
      'Rendered body text:',
      bodyText.slice(0, MAX_RENDERED_TEXT_CHARS),
      '',
      'Rendered links:',
      links,
      '',
      'Rendered HTML:',
      html.slice(0, MAX_PAGE_CHARS)
    ].join('\n');
  } finally {
    await browser.close().catch(() => {});
  }
}

async function fetchPageSources(url) {
  const cleaned = url.replace(/^https?:\/\//i, '');
  const attempts = [
    { label: 'r.jina.ai-http', url: `https://r.jina.ai/http://${cleaned}` },
    { label: 'r.jina.ai-https', url: `https://r.jina.ai/https://${cleaned}` },
    { label: 'direct', url }
  ];

  const errors = [];
  const candidates = [];

  for (const attempt of attempts) {
    try {
      const text = await fetchTextWithTimeout(attempt.url);

      candidates.push({
        via: attempt.label,
        text,
        score: scoreJobSignal(text)
      });
    } catch (error) {
      errors.push(`${attempt.label}: ${error.message}`);
    }
  }

  if (!['0', 'false', 'off', 'disabled', 'never'].includes(RENDERED_FETCH)) {
    try {
      const text = await fetchRenderedSource(url);
      candidates.push({
        via: 'playwright-rendered',
        text,
        score: scoreJobSignal(text)
      });
    } catch (error) {
      errors.push(`playwright-rendered: ${error.message}`);
    }
  }

  if (!candidates.length) {
    throw new Error(`Could not fetch page (${errors.join(' | ')})`);
  }

  candidates.sort((a, b) => b.score - a.score);
  return {
    primary: candidates[0],
    candidates,
    errors
  };
}

function extractFocusedText(text) {
  const trimmed = String(text || '').trim();
  if (!trimmed) return '';
  if (trimmed.length <= MAX_PAGE_CHARS) return trimmed;

  const lines = trimmed.split(/\r?\n/);
  const selected = new Set();

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const lower = line.toLowerCase();
    const hasUrl = /https?:\/\/|\/karriere\/|\/career\/|\/jobs?\//i.test(line);
    const hasSignal = hasKeyword(lower, JOB_KEYWORDS) || hasKeyword(lower, META_KEYWORDS);
    if (!hasUrl && !hasSignal) continue;

    for (let j = -2; j <= 2; j += 1) {
      const idx = i + j;
      if (idx >= 0 && idx < lines.length) selected.add(idx);
    }
  }

  for (let i = 0; i < Math.min(lines.length, 120); i += 1) selected.add(i);

  const sorted = [...selected].sort((a, b) => a - b);
  let out = '';

  for (const idx of sorted) {
    const line = lines[idx];
    if (!line) continue;
    if (out.length + line.length + 1 > MAX_PAGE_CHARS) break;
    out += `${line}\n`;
  }

  if (out.length < 12_000) {
    return trimmed.slice(0, MAX_PAGE_CHARS);
  }

  return out;
}

function buildModelContext(sourceUrl, sourceCandidates, heuristicJobs) {
  const nowDate = new Date().toISOString().slice(0, 10);
  const parts = [
    `Source URL: ${sourceUrl}`,
    `Extraction date: ${nowDate}`,
    'You may use extraction date to convert relative dates like "2 days ago" to YYYY-MM-DD.'
  ];

  if (heuristicJobs.length) {
    const hints = heuristicJobs
      .slice(0, 80)
      .map((job) => ({
        company: job.company,
        title: job.title,
        url: job.url,
        posted_date: job.posted_date
      }));
    parts.push('\nHeuristic candidates (not always perfect):');
    parts.push(JSON.stringify(hints, null, 2));
  }

  const topSources = sourceCandidates.slice(0, MAX_MODEL_SOURCES);
  topSources.forEach((candidate, index) => {
    parts.push(`\n===== Source ${index + 1}: ${candidate.via} (score ${candidate.score}) =====`);
    parts.push(extractFocusedText(candidate.text));
  });

  return parts.join('\n').slice(0, MAX_CONTEXT_CHARS);
}

function readOutputText(payload) {
  if (typeof payload.output_text === 'string' && payload.output_text.trim()) {
    return payload.output_text;
  }

  const chunks = [];
  if (Array.isArray(payload.output)) {
    for (const item of payload.output) {
      if (!Array.isArray(item.content)) continue;
      for (const content of item.content) {
        if (content?.type === 'output_text' && typeof content.text === 'string') {
          chunks.push(content.text);
        }
      }
    }
  }

  return chunks.join('\n').trim();
}

function parseJsonFromText(text) {
  const trimmed = String(text || '').trim();
  if (!trimmed) throw new Error('OpenAI returned empty content');

  try {
    return JSON.parse(trimmed);
  } catch {
    // Continue with fallback patterns.
  }

  const fenceMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenceMatch) {
    try {
      return JSON.parse(fenceMatch[1]);
    } catch {
      // Continue with object/array fallback.
    }
  }

  const objMatch = trimmed.match(/\{[\s\S]*\}/);
  if (objMatch) {
    try {
      return JSON.parse(objMatch[0]);
    } catch {
      // Continue.
    }
  }

  const arrMatch = trimmed.match(/\[[\s\S]*\]/);
  if (arrMatch) {
    try {
      return JSON.parse(arrMatch[0]);
    } catch {
      // Continue.
    }
  }

  throw new Error('OpenAI did not return parseable JSON');
}

async function callOpenAiExtraction(sourceUrl, contextText) {
  const prompt = [
    'Extract real job postings from the provided content.',
    'Return JSON only. No markdown, no explanation.',
    'Schema:',
    '{"jobs":[{"company":"","title":"","location":"","posted_date":"","url":"","snippet":""}]}',
    'Rules:',
    '- Keep only actual job listings (exclude nav links, filters, categories, policy links).',
    '- Convert posted_date to YYYY-MM-DD when possible, otherwise empty string.',
    '- Convert relative dates (e.g., "2 days ago", "lagt ut for 3 dager siden") using the extraction date.',
    '- Use absolute URLs if possible.',
    '- Include snippet as a short evidence string from the source for each job.'
  ].join('\n');

  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      temperature: 0,
      max_output_tokens: 3200,
      input: [
        {
          role: 'system',
          content: [{ type: 'input_text', text: prompt }]
        },
        {
          role: 'user',
          content: [{ type: 'input_text', text: `Source URL: ${sourceUrl}\n\nContent:\n${contextText}` }]
        }
      ]
    })
  });

  const rawBody = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(rawBody);
  } catch {
    if (!response.ok) throw new Error(`OpenAI HTTP ${response.status}: ${rawBody.slice(0, 240)}`);
    throw new Error('OpenAI returned invalid JSON payload');
  }

  if (!response.ok) {
    const apiMessage = payload?.error?.message || rawBody.slice(0, 240);
    throw new Error(`OpenAI HTTP ${response.status}: ${apiMessage}`);
  }

  const outputText = readOutputText(payload);
  const parsed = parseJsonFromText(outputText);

  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.jobs)) return parsed.jobs;
  return [];
}

async function callOpenAiLinkSelection(sourceUrl, pageText, linkCandidates) {
  if (!OPENAI_API_KEY || !linkCandidates.length) {
    return linkCandidates.slice(0, MAX_AI_SELECTED_LINKS).map((link) => link.url);
  }

  const links = linkCandidates.slice(0, MAX_AI_LINK_CANDIDATES).map((link, index) => ({
    index,
    url: link.url,
    text: link.text,
    snippet: link.snippet,
    score: link.score
  }));

  const prompt = [
    'You are choosing which links to fetch for a universal job scraper.',
    'Return JSON only. No markdown, no explanation.',
    'Schema: {"links":[{"url":"","reason":""}]}',
    'Choose links that are likely to contain job listings, career listings, internship listings, or actual job detail pages.',
    'Prefer pages with many listings over generic landing pages.',
    'Reject privacy, terms, news, blog, social media, login, unrelated company pages, images, PDFs, and assets.',
    `Return at most ${MAX_AI_SELECTED_LINKS} links.`,
    'Use only URLs from the provided candidates.'
  ].join('\n');

  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      temperature: 0,
      max_output_tokens: 1400,
      input: [
        { role: 'system', content: [{ type: 'input_text', text: prompt }] },
        {
          role: 'user',
          content: [{
            type: 'input_text',
            text: [
              `Source URL: ${sourceUrl}`,
              '',
              'Initial page focused text:',
              extractFocusedText(pageText).slice(0, 40_000),
              '',
              'Candidate links:',
              JSON.stringify(links, null, 2)
            ].join('\n')
          }]
        }
      ]
    })
  });

  const rawBody = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(rawBody);
  } catch {
    if (!response.ok) throw new Error(`OpenAI link selection HTTP ${response.status}: ${rawBody.slice(0, 240)}`);
    throw new Error('OpenAI link selection returned invalid JSON payload');
  }

  if (!response.ok) {
    const apiMessage = payload?.error?.message || rawBody.slice(0, 240);
    throw new Error(`OpenAI link selection HTTP ${response.status}: ${apiMessage}`);
  }

  const parsed = parseJsonFromText(readOutputText(payload));
  const selected = Array.isArray(parsed?.links) ? parsed.links : [];
  const allowed = new Set(linkCandidates.map((link) => link.url));
  return selected
    .map((link) => String(link?.url || '').trim())
    .filter((url) => allowed.has(url))
    .slice(0, MAX_AI_SELECTED_LINKS);
}

function buildIterativeExtractionContext(sourceUrl, pages, heuristicJobs) {
  const nowDate = new Date().toISOString().slice(0, 10);
  const parts = [
    `Source URL: ${sourceUrl}`,
    `Extraction date: ${nowDate}`,
    'Task: extract every real job posting visible in the provided pages.'
  ];

  if (heuristicJobs.length) {
    parts.push('\nHeuristic candidates. These are hints only; correct mistakes and remove false positives:');
    parts.push(JSON.stringify(heuristicJobs.slice(0, 120).map((job) => ({
      company: job.company,
      title: job.title,
      location: job.location,
      posted_date: job.posted_date,
      url: job.url
    })), null, 2));
  }

  pages.slice(0, MAX_ITERATIVE_PAGES).forEach((page, index) => {
    const text = extractFocusedText(page.text).slice(0, index === 0 ? 70_000 : 45_000);
    parts.push(`\n===== Page ${index + 1}: ${page.url} (${page.via}, score ${page.score}) =====`);
    parts.push(text);
  });

  return parts.join('\n').slice(0, MAX_CONTEXT_CHARS);
}

async function extractJobsFromIterativeContext(sourceUrl, pages, heuristicJobs) {
  if (!OPENAI_API_KEY) return [];

  const prompt = [
    'You are a precise job extraction engine for arbitrary career pages.',
    'Return JSON only. No markdown, no explanation.',
    'Schema:',
    '{"jobs":[{"company":"","title":"","location":"","posted_date":"","url":"","snippet":""}]}',
    'Rules:',
    '- Extract all actual open job postings, internships, graduate roles, trainee roles, and part-time/full-time roles.',
    '- Exclude navigation links, categories, filters, employer profiles, event pages, articles, privacy/terms, and generic career pages that are not individual jobs.',
    '- If a page lists job cards, each card is a separate job.',
    '- company is the employer/organization for that posting. If not explicit, infer from the page/domain.',
    '- title must be the actual role title, not a filename, image label, card label, or generic "job posting" text.',
    '- posted_date must be YYYY-MM-DD when available. Convert relative dates using the extraction date. Leave empty if not present.',
    '- url must be the best direct job/application/detail URL. Use absolute URLs.',
    '- snippet should be short evidence from the source.',
    '- Do not duplicate the same job; keep the best title and direct URL.'
  ].join('\n');

  const context = buildIterativeExtractionContext(sourceUrl, pages, heuristicJobs);
  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      temperature: 0,
      max_output_tokens: 6000,
      input: [
        { role: 'system', content: [{ type: 'input_text', text: prompt }] },
        { role: 'user', content: [{ type: 'input_text', text: context }] }
      ]
    })
  });

  const rawBody = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(rawBody);
  } catch {
    if (!response.ok) throw new Error(`OpenAI extraction HTTP ${response.status}: ${rawBody.slice(0, 240)}`);
    throw new Error('OpenAI extraction returned invalid JSON payload');
  }

  if (!response.ok) {
    const apiMessage = payload?.error?.message || rawBody.slice(0, 240);
    throw new Error(`OpenAI extraction HTTP ${response.status}: ${apiMessage}`);
  }

  const parsed = parseJsonFromText(readOutputText(payload));
  if (Array.isArray(parsed)) return parsed;
  if (Array.isArray(parsed?.jobs)) return parsed.jobs;
  return [];
}

async function extractJobsWithOpenAi(sourceUrl, sources, heuristicJobs) {
  if (!OPENAI_API_KEY) return [];

  const context = buildModelContext(sourceUrl, sources, heuristicJobs);
  const primaryRaw = await callOpenAiExtraction(sourceUrl, context);
  let merged = mergeAndNormalizeJobs(primaryRaw, sourceUrl);

  if (merged.length >= 8 || sources.length <= 1) {
    return merged;
  }

  const secondaryContext = buildModelContext(sourceUrl, [sources[0]], heuristicJobs);
  const secondaryRaw = await callOpenAiExtraction(sourceUrl, secondaryContext);
  merged = mergeAndNormalizeJobs([...merged, ...secondaryRaw], sourceUrl);
  return merged;
}

function crawlUrlKey(url, sourceUrl) {
  const absolute = toAbsoluteUrl(url, sourceUrl);
  if (!absolute) return '';
  try {
    const parsed = new URL(absolute);
    parsed.hash = '';
    parsed.searchParams.sort();
    return parsed.href.toLowerCase();
  } catch {
    return canonicalUrl(absolute, sourceUrl);
  }
}

function sameSiteOrKnownAts(url, sourceUrl) {
  try {
    const parsed = new URL(url);
    const source = new URL(sourceUrl);
    if (parsed.hostname === source.hostname) return true;
    if (parsed.hostname.endsWith(`.${source.hostname}`)) return true;
    if (source.hostname.endsWith(`.${parsed.hostname}`)) return true;
    return ATS_HOST_HINTS.some((hint) => parsed.hostname.includes(hint));
  } catch {
    return false;
  }
}

function shouldCrawlLink(candidate, sourceUrl) {
  if (!candidate?.url) return false;
  let parsed;
  try {
    parsed = new URL(candidate.url);
  } catch {
    return false;
  }

  const lower = candidate.url.toLowerCase();
  const pathLower = parsed.pathname.toLowerCase();
  if (!['http:', 'https:'].includes(parsed.protocol)) return false;
  if (/\.(png|jpe?g|gif|svg|webp|ico|pdf|css|js|map|woff2?|ttf|zip|docx?|xlsx?)$/i.test(pathLower)) return false;
  if (/(\/privacy|\/terms|\/cookies|\/contact|\/about|\/faq|\/news|\/blog|\/login|\/signin|\/signup)(\/|$)/i.test(lower)) return false;
  if (!sameSiteOrKnownAts(candidate.url, sourceUrl)) return false;

  const signal = `${candidate.text || ''} ${candidate.snippet || ''} ${pathLower}`;
  return candidate.score >= 18 || looksLikeJobUrl(candidate.url, sourceUrl) || looksLikeJobText(signal);
}

function addCrawlCandidate(candidateMap, candidate, sourceUrl, discoveredFrom = '') {
  if (!shouldCrawlLink(candidate, sourceUrl)) return;
  const key = crawlUrlKey(candidate.url, sourceUrl);
  if (!key) return;

  const existing = candidateMap.get(key);
  const next = {
    ...candidate,
    discovered_from: discoveredFrom || candidate.discovered_from || sourceUrl
  };

  if (!existing || next.score > existing.score) {
    candidateMap.set(key, next);
  }
}

async function fetchCrawlerPage(url) {
  const fetched = await fetchPageSources(url);
  const rawJobs = [];
  const links = new Map();

  for (const candidate of fetched.candidates.slice(0, MAX_MODEL_SOURCES)) {
    rawJobs.push(...extractHeuristicJobsFromText(candidate.text, url));
    for (const link of extractLinkCandidatesFromText(candidate.text, url)) {
      const key = crawlUrlKey(link.url, url);
      if (!key) continue;
      const existing = links.get(key);
      if (!existing || link.score > existing.score) links.set(key, link);
    }
  }

  return {
    page: {
      url,
      via: fetched.primary.via,
      score: fetched.primary.score,
      text: fetched.primary.text
    },
    rawJobs,
    links: [...links.values()],
    sourceCandidates: fetched.candidates,
    errors: fetched.errors
  };
}

async function callOpenAiCrawlPlanner(sourceUrl, pages, heuristicJobs, linkCandidates, visitedKeys) {
  const usableCandidates = linkCandidates
    .filter((candidate) => !visitedKeys.has(crawlUrlKey(candidate.url, sourceUrl)))
    .slice(0, MAX_AI_LINK_CANDIDATES)
    .map((candidate, index) => ({
      index,
      url: candidate.url,
      text: candidate.text,
      snippet: candidate.snippet,
      score: candidate.score,
      discovered_from: candidate.discovered_from
    }));

  if (!usableCandidates.length) return [];

  if (!OPENAI_API_KEY) {
    return usableCandidates.slice(0, MAX_AI_SELECTED_LINKS).map((candidate) => candidate.url);
  }

  const prompt = [
    'You are the link-following planner for a universal job scraper.',
    'Return JSON only. No markdown, no explanation.',
    'Schema: {"links":[{"url":"","reason":""}]}',
    'Goal: choose the next pages that most likely reveal actual job postings or job detail cards.',
    'Prefer direct job detail URLs and listing pages with multiple jobs.',
    'Follow relevant sub-job links from the same site or known ATS hosts.',
    'Reject navigation, filters, employer profiles without jobs, events, articles, privacy/terms, login, PDFs, images, and social links.',
    `Return at most ${MAX_AI_SELECTED_LINKS} URLs. Use only URLs from candidates.`
  ].join('\n');

  const state = {
    source_url: sourceUrl,
    fetched_pages: pages.slice(-6).map((page) => ({
      url: page.url,
      via: page.via,
      score: page.score,
      focused_text: extractFocusedText(page.text).slice(0, 10_000)
    })),
    current_job_hints: heuristicJobs.slice(0, 80).map((job) => ({
      company: job.company,
      title: job.title,
      url: job.url,
      posted_date: job.posted_date
    })),
    candidates: usableCandidates
  };

  const response = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      temperature: 0,
      max_output_tokens: 1800,
      input: [
        { role: 'system', content: [{ type: 'input_text', text: prompt }] },
        { role: 'user', content: [{ type: 'input_text', text: JSON.stringify(state, null, 2) }] }
      ]
    })
  });

  const rawBody = await response.text();
  let payload = {};
  try {
    payload = JSON.parse(rawBody);
  } catch {
    if (!response.ok) throw new Error(`OpenAI crawl planner HTTP ${response.status}: ${rawBody.slice(0, 240)}`);
    throw new Error('OpenAI crawl planner returned invalid JSON payload');
  }

  if (!response.ok) {
    const apiMessage = payload?.error?.message || rawBody.slice(0, 240);
    throw new Error(`OpenAI crawl planner HTTP ${response.status}: ${apiMessage}`);
  }

  const parsed = parseJsonFromText(readOutputText(payload));
  const selected = Array.isArray(parsed?.links) ? parsed.links : [];
  const allowed = new Set(usableCandidates.map((candidate) => candidate.url));
  return selected
    .map((link) => String(link?.url || '').trim())
    .filter((url) => allowed.has(url))
    .slice(0, MAX_AI_SELECTED_LINKS);
}

function validateSourceUrl(sourceUrl) {
  if (!sourceUrl) {
    throw new Error('Missing "url" in request body');
  }

  const parsedUrl = new URL(sourceUrl);
  if (!['http:', 'https:'].includes(parsedUrl.protocol)) {
    throw new Error('Only http/https URLs are supported');
  }

  return parsedUrl;
}

async function extractJobsForUrl(sourceUrl) {
  const parsedUrl = validateSourceUrl(sourceUrl);
  const ats = await fetchAtsJobs(parsedUrl.href);
  const atsJobs = mergeAndNormalizeJobs(ats.jobs, parsedUrl.href);

  const pages = [];
  const sourceCandidates = [];
  const sourceErrors = [];
  const heuristicRaw = [];
  const candidateMap = new Map();
  const visited = new Set();
  const queued = new Set();
  const followedLinks = [];
  const plannerWarnings = [];
  const crawlQueue = [parsedUrl.href];
  queued.add(crawlUrlKey(parsedUrl.href, parsedUrl.href));

  while (crawlQueue.length && pages.length < MAX_ITERATIVE_PAGES) {
    const batchSize = pages.length ? Math.min(4, crawlQueue.length, MAX_ITERATIVE_PAGES - pages.length) : 1;
    const batch = crawlQueue.splice(0, batchSize);
    const results = await Promise.allSettled(batch.map((url) => fetchCrawlerPage(url)));

    for (let i = 0; i < results.length; i += 1) {
      const url = batch[i];
      const key = crawlUrlKey(url, parsedUrl.href);
      visited.add(key);

      const result = results[i];
      if (result.status === 'rejected') {
        sourceErrors.push(`${url}: ${result.reason?.message || String(result.reason)}`);
        continue;
      }

      pages.push(result.value.page);
      heuristicRaw.push(...result.value.rawJobs);
      sourceCandidates.push(...result.value.sourceCandidates);
      sourceErrors.push(...result.value.errors);

      for (const link of result.value.links) {
        addCrawlCandidate(candidateMap, link, parsedUrl.href, url);
      }
    }

    if (pages.length >= MAX_ITERATIVE_PAGES) break;

    const heuristicJobsForPlanning = mergeAndNormalizeJobs([...atsJobs, ...heuristicRaw], parsedUrl.href);
    const candidates = [...candidateMap.values()]
      .filter((candidate) => {
        const key = crawlUrlKey(candidate.url, parsedUrl.href);
        return key && !visited.has(key) && !queued.has(key);
      })
      .sort((a, b) => b.score - a.score);

    if (!candidates.length) continue;

    let selectedLinks = [];
    try {
      selectedLinks = await callOpenAiCrawlPlanner(parsedUrl.href, pages, heuristicJobsForPlanning, candidates, visited);
    } catch (error) {
      plannerWarnings.push(error.message);
      selectedLinks = candidates.slice(0, MAX_AI_SELECTED_LINKS).map((candidate) => candidate.url);
    }

    for (const link of selectedLinks) {
      const key = crawlUrlKey(link, parsedUrl.href);
      if (!key || visited.has(key) || queued.has(key)) continue;
      crawlQueue.push(link);
      queued.add(key);
      followedLinks.push(link);
      if (pages.length + crawlQueue.length >= MAX_ITERATIVE_PAGES) break;
    }
  }

  const heuristicJobs = mergeAndNormalizeJobs([...atsJobs, ...heuristicRaw], parsedUrl.href);

  let warning = plannerWarnings.join(' | ');
  let openAiJobs = [];
  try {
    const aiRaw = await extractJobsFromIterativeContext(parsedUrl.href, pages, heuristicJobs);
    openAiJobs = mergeAndNormalizeJobs(aiRaw, parsedUrl.href);
  } catch (error) {
    warning = warning ? `${warning} | ${error.message}` : error.message;
    try {
      openAiJobs = await extractJobsWithOpenAi(parsedUrl.href, sourceCandidates, heuristicJobs);
    } catch (fallbackError) {
      warning = `${warning} | ${fallbackError.message}`;
    }
  }

  const jobs = mergeAndNormalizeJobs([...atsJobs, ...heuristicJobs, ...openAiJobs], parsedUrl.href);
  const linkCandidates = [...candidateMap.values()]
    .sort((a, b) => b.score - a.score)
    .slice(0, 20);

  return {
    jobs,
    meta: {
      count: jobs.length,
      ats_count: atsJobs.length,
      ats_sources: ats.sources,
      ats_errors: ats.errors,
      heuristic_count: heuristicJobs.length,
      openai_count: openAiJobs.length,
      via: pages[0]?.via || '',
      followup_scanned: pages.length - 1,
      followup_links: followedLinks,
      link_candidates: linkCandidates.slice(0, 20).map((item) => ({ url: item.url, text: item.text, score: item.score })),
      source_candidates: sourceCandidates.map((item) => ({ via: item.via, score: item.score })).slice(0, 60),
      source_errors: sourceErrors,
      model: OPENAI_MODEL,
      source_url: parsedUrl.href,
      warning: warning || (OPENAI_API_KEY ? '' : 'OPENAI_API_KEY is not set. Returned heuristic-only extraction.')
    }
  };
}

async function handleExtractJobs(req, res) {
  let body;
  try {
    body = await readJsonBody(req);
  } catch (error) {
    sendJson(res, 400, { error: error.message });
    return;
  }

  try {
    const result = await withTimeout(
      extractJobsForUrl(String(body.url || '').trim()),
      EXTRACT_TIMEOUT_MS,
      `Extraction timed out after ${EXTRACT_TIMEOUT_MS}ms`
    );
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
}

let vapidConfigPromise = null;

async function getVapidConfig() {
  if (vapidConfigPromise) return vapidConfigPromise;

  vapidConfigPromise = (async () => {
    await ensureDataDir();
    let keys = null;

    if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
      keys = {
        publicKey: process.env.VAPID_PUBLIC_KEY,
        privateKey: process.env.VAPID_PRIVATE_KEY
      };
    } else {
      keys = await readJsonFile(VAPID_KEYS_FILE, null);
      if (!keys?.publicKey || !keys?.privateKey) {
        keys = webpush.generateVAPIDKeys();
        await writeJsonFile(VAPID_KEYS_FILE, keys);
      }
    }

    webpush.setVapidDetails(PUSH_CONTACT, keys.publicKey, keys.privateKey);
    return keys;
  })();

  return vapidConfigPromise;
}

async function loadPushSubscriptions() {
  const subscriptions = await readJsonFile(PUSH_SUBSCRIPTIONS_FILE, []);
  return Array.isArray(subscriptions) ? subscriptions.filter((sub) => sub?.endpoint) : [];
}

async function savePushSubscription(subscription) {
  if (!subscription?.endpoint) {
    throw new Error('Invalid push subscription');
  }

  const subscriptions = await loadPushSubscriptions();
  const byEndpoint = new Map(subscriptions.map((item) => [item.endpoint, item]));
  byEndpoint.set(subscription.endpoint, {
    ...subscription,
    saved_at: new Date().toISOString()
  });
  const next = [...byEndpoint.values()];
  await writeJsonFile(PUSH_SUBSCRIPTIONS_FILE, next);
  return next.length;
}

async function prunePushSubscriptions(deadEndpoints) {
  if (!deadEndpoints.length) return;
  const dead = new Set(deadEndpoints);
  const subscriptions = await loadPushSubscriptions();
  await writeJsonFile(PUSH_SUBSCRIPTIONS_FILE, subscriptions.filter((sub) => !dead.has(sub.endpoint)));
}

async function sendPushToAll(payload) {
  await getVapidConfig();
  const subscriptions = await loadPushSubscriptions();
  const deadEndpoints = [];
  let sent = 0;

  await Promise.allSettled(subscriptions.map(async (subscription) => {
    try {
      await webpush.sendNotification(subscription, JSON.stringify(payload));
      sent += 1;
    } catch (error) {
      if (error?.statusCode === 404 || error?.statusCode === 410) {
        deadEndpoints.push(subscription.endpoint);
      } else {
        console.warn(`Push notification failed: ${error.message}`);
      }
    }
  }));

  await prunePushSubscriptions(deadEndpoints);
  return { sent, removed: deadEndpoints.length };
}

async function handlePushPublicKey(_req, res) {
  try {
    const keys = await getVapidConfig();
    sendJson(res, 200, { publicKey: keys.publicKey });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
}

async function handlePushSubscribe(req, res) {
  try {
    const body = await readJsonBody(req);
    const count = await savePushSubscription(body.subscription || body);
    sendJson(res, 200, { ok: true, subscription_count: count });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

async function handleSendTestNotification(_req, res) {
  try {
    const result = await sendPushToAll({
      title: 'Up and Forward',
      body: 'Test notification is working.',
      url: '/dashboard.html',
      tag: 'upforward-test'
    });
    sendJson(res, 200, { ok: true, ...result });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
}

function isValidEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
}

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function envAlertEmails() {
  return uniqueNonEmpty(ALERT_EMAILS.split(',').map((email) => normalizeEmail(email)))
    .filter(isValidEmail);
}

async function loadEmailSubscriptions() {
  const saved = await readJsonFile(EMAIL_SUBSCRIPTIONS_FILE, []);
  const savedEmails = Array.isArray(saved)
    ? saved.map((item) => normalizeEmail(typeof item === 'string' ? item : item?.email)).filter(isValidEmail)
    : [];
  return uniqueNonEmpty([...envAlertEmails(), ...savedEmails]);
}

async function saveEmailSubscription(email) {
  const normalized = normalizeEmail(email);
  if (!isValidEmail(normalized)) {
    throw new Error('Invalid email address');
  }

  const subscriptions = await readJsonFile(EMAIL_SUBSCRIPTIONS_FILE, []);
  const byEmail = new Map(
    (Array.isArray(subscriptions) ? subscriptions : [])
      .map((item) => [normalizeEmail(typeof item === 'string' ? item : item?.email), item])
      .filter(([address]) => isValidEmail(address))
  );

  byEmail.set(normalized, {
    email: normalized,
    saved_at: new Date().toISOString()
  });

  await writeJsonFile(EMAIL_SUBSCRIPTIONS_FILE, [...byEmail.values()]);
  return loadEmailSubscriptions();
}

function formatJobEmailHtml(jobs) {
  const rows = jobs.slice(0, 40).map((job) => {
    const title = escapeHtml(job.title || 'Untitled role');
    const company = escapeHtml(job.company || job.bank || 'Unknown company');
    const location = job.location ? escapeHtml(job.location) : 'Location not listed';
    const date = job.posted_date ? escapeHtml(job.posted_date) : 'Date not listed';
    const url = escapeHtml(job.url || '#');
    return `
      <tr>
        <td style="padding:18px 0;border-bottom:1px solid #e5e7eb;">
          <div style="font-size:12px;font-weight:700;color:#2563eb;text-transform:uppercase;letter-spacing:.04em;">${company}</div>
          <div style="font-size:18px;font-weight:800;color:#111827;margin:5px 0 8px;">${title}</div>
          <div style="font-size:13px;color:#6b7280;margin-bottom:12px;">${location} · Posted ${date}</div>
          <a href="${url}" style="display:inline-block;background:#2563eb;color:#fff;text-decoration:none;border-radius:8px;padding:10px 14px;font-size:13px;font-weight:700;">Open role</a>
        </td>
      </tr>
    `;
  }).join('');

  const extra = jobs.length > 40
    ? `<p style="color:#6b7280;font-size:13px;margin:16px 0 0;">Plus ${jobs.length - 40} more new roles in your dashboard.</p>`
    : '';

  return `<!doctype html>
    <html>
      <body style="margin:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#111827;">
        <div style="max-width:680px;margin:0 auto;padding:32px 16px;">
          <div style="background:#0a0c10;border-radius:16px 16px 0 0;padding:28px 30px;color:#fff;">
            <div style="font-size:13px;font-weight:800;color:#60a5fa;text-transform:uppercase;letter-spacing:.08em;">Up and Forward</div>
            <h1 style="font-size:28px;line-height:1.15;margin:10px 0 0;">${jobs.length} new job${jobs.length === 1 ? '' : 's'} found</h1>
            <p style="color:#aeb6c4;margin:10px 0 0;font-size:15px;">Your monitored career pages were scanned automatically.</p>
          </div>
          <div style="background:#fff;border:1px solid #e5e7eb;border-top:0;border-radius:0 0 16px 16px;padding:8px 30px 28px;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">${rows}</table>
            ${extra}
            <p style="color:#9ca3af;font-size:12px;margin:24px 0 0;">You are receiving this because your email is subscribed to Up and Forward job alerts.</p>
          </div>
        </div>
      </body>
    </html>`;
}

function formatJobEmailText(jobs) {
  return [
    `Up and Forward found ${jobs.length} new job${jobs.length === 1 ? '' : 's'}:`,
    '',
    ...jobs.map((job) => [
      `${job.title || 'Untitled role'} — ${job.company || job.bank || 'Unknown company'}`,
      [job.location, job.posted_date].filter(Boolean).join(' · '),
      job.url || ''
    ].filter(Boolean).join('\n')),
    '',
    'Open your dashboard for the full list.'
  ].join('\n\n');
}

async function sendEmail(to, subject, html, text) {
  if (!RESEND_API_KEY) {
    return { skipped: true, reason: 'RESEND_API_KEY is not set' };
  }

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${RESEND_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from: MAIL_FROM,
      to,
      subject,
      html,
      text
    })
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload?.message || `Resend returned HTTP ${response.status}`);
  }
  return payload;
}

async function sendNewJobsEmail(newJobs) {
  if (!newJobs.length) return { sent: 0, skipped: 0 };

  const recipients = await loadEmailSubscriptions();
  if (!recipients.length) return { sent: 0, skipped: 0, reason: 'No email subscribers' };

  const subject = `${newJobs.length} new job${newJobs.length === 1 ? '' : 's'} from Up and Forward`;
  const html = formatJobEmailHtml(newJobs);
  const text = formatJobEmailText(newJobs);
  let sent = 0;
  let skipped = 0;

  const results = await Promise.allSettled(recipients.map((email) => sendEmail(email, subject, html, text)));
  for (const result of results) {
    if (result.status === 'fulfilled' && !result.value?.skipped) sent += 1;
    else skipped += 1;
  }
  return { sent, skipped, recipient_count: recipients.length };
}

async function handleEmailSubscribe(req, res) {
  try {
    const body = await readJsonBody(req);
    const subscribers = await saveEmailSubscription(body.email);
    sendJson(res, 200, {
      ok: true,
      email_count: subscribers.length,
      mail_configured: Boolean(RESEND_API_KEY)
    });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

async function loadWatchUrls() {
  const urls = await readJsonFile(WATCH_URLS_FILE, []);
  return Array.isArray(urls) ? urls.filter(Boolean) : [];
}

async function saveWatchUrls(urls) {
  await writeJsonFile(WATCH_URLS_FILE, uniqueNonEmpty(urls));
}

function jobFingerprint(job) {
  const url = String(job?.url || '').trim().toLowerCase().replace(/[?#].*$/, '');
  const id = String(job?.id || '').trim().toLowerCase();
  const title = String(job?.title || '').trim().toLowerCase();
  const company = String(job?.company || job?.bank || '').trim().toLowerCase();
  return cleanText([url, id, title, company].filter(Boolean).join('|')).toLowerCase();
}

async function markJobsSeen(jobs) {
  const seen = await readJsonFile(MONITOR_SEEN_FILE, {});
  const now = new Date().toISOString();
  let added = 0;

  for (const job of jobs) {
    const key = jobFingerprint(job);
    if (!key || seen[key]) continue;
    seen[key] = now;
    added += 1;
  }

  await writeJsonFile(MONITOR_SEEN_FILE, seen);
  return added;
}

async function handleWatchUrl(req, res) {
  try {
    const body = await readJsonBody(req);
    const parsedUrl = validateSourceUrl(String(body.url || '').trim());
    const urls = await loadWatchUrls();
    if (!urls.includes(parsedUrl.href)) {
      urls.push(parsedUrl.href);
      await saveWatchUrls(urls);
    }

    const seededJobs = Array.isArray(body.jobs) ? body.jobs : null;
    const result = seededJobs
      ? { jobs: seededJobs }
      : await extractJobsForUrl(parsedUrl.href);
    const seeded = await markJobsSeen(result.jobs);
    await writeJsonFile(MONITOR_LATEST_FILE, {
      updated_at: new Date().toISOString(),
      jobs: result.jobs
    });

    sendJson(res, 200, {
      ok: true,
      watched_url: parsedUrl.href,
      watch_count: urls.length,
      initial_count: result.jobs.length,
      seeded_count: seeded
    });
  } catch (error) {
    sendJson(res, 400, { error: error.message });
  }
}

async function handleWatchUrls(_req, res) {
  try {
    const urls = await loadWatchUrls();
    const subscriptions = await loadPushSubscriptions();
    const emails = await loadEmailSubscriptions();
    sendJson(res, 200, {
      urls,
      watch_count: urls.length,
      subscription_count: subscriptions.length,
      email_count: emails.length,
      mail_configured: Boolean(RESEND_API_KEY),
      monitor_enabled: MONITOR_ENABLED,
      monitor_interval_ms: MONITOR_INTERVAL_MS
    });
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
}

let monitorRunning = false;

async function runMonitorCycle() {
  if (monitorRunning) {
    return { ok: false, skipped: true, reason: 'Monitor is already running' };
  }
  monitorRunning = true;
  try {
    const urls = await loadWatchUrls();
    if (!urls.length) {
      return {
        ok: true,
        scanned_urls: 0,
        latest_count: 0,
        new_count: 0,
        push: { sent: 0, removed: 0 },
        email: { sent: 0, skipped: 0 }
      };
    }

    const seen = await readJsonFile(MONITOR_SEEN_FILE, {});
    const latestJobs = [];
    const newJobs = [];
    const errors = [];
    const now = new Date().toISOString();

    for (const url of urls) {
      try {
        const result = await extractJobsForUrl(url);
        latestJobs.push(...result.jobs);

        for (const job of result.jobs) {
          const key = jobFingerprint(job);
          if (!key || seen[key]) continue;
          seen[key] = now;
          newJobs.push({ ...job, is_new: true });
        }
      } catch (error) {
        errors.push({ url, error: error.message });
        console.warn(`Monitor failed for ${url}: ${error.message}`);
      }
    }

    await writeJsonFile(MONITOR_SEEN_FILE, seen);
    await writeJsonFile(MONITOR_LATEST_FILE, { updated_at: now, jobs: latestJobs });

    const pushResults = [];
    for (const job of newJobs) {
      pushResults.push(await sendPushToAll({
        title: job.title || 'New job posted',
        body: `${job.company || job.bank || 'New listing'}${job.location ? ` · ${job.location}` : ''}`,
        url: job.url || '/dashboard.html',
        tag: `job-${jobFingerprint(job).slice(0, 48)}`
      }));
    }
    const push = pushResults.reduce((acc, item) => ({
      sent: acc.sent + Number(item?.sent || 0),
      removed: acc.removed + Number(item?.removed || 0)
    }), { sent: 0, removed: 0 });
    const email = await sendNewJobsEmail(newJobs);

    if (newJobs.length) {
      console.log(`Monitor found ${newJobs.length} new job(s). Email sent to ${email.sent || 0} recipient(s).`);
    }

    return {
      ok: true,
      scanned_urls: urls.length,
      latest_count: latestJobs.length,
      new_count: newJobs.length,
      error_count: errors.length,
      errors,
      push,
      email
    };
  } finally {
    monitorRunning = false;
  }
}

async function handleRefreshWatchUrls(_req, res) {
  try {
    const result = await withTimeout(
      runMonitorCycle(),
      Math.max(EXTRACT_TIMEOUT_MS, 180_000),
      'Monitor refresh timed out'
    );
    sendJson(res, 200, result);
  } catch (error) {
    sendJson(res, 500, { error: error.message });
  }
}

function startMonitor() {
  if (!MONITOR_ENABLED) {
    console.log('Job monitor is disabled.');
    return;
  }

  setTimeout(() => {
    runMonitorCycle().catch((error) => console.warn(`Monitor failed: ${error.message}`));
  }, 5_000);

  setInterval(() => {
    runMonitorCycle().catch((error) => console.warn(`Monitor failed: ${error.message}`));
  }, MONITOR_INTERVAL_MS);
}

const server = http.createServer(async (req, res) => {
  const method = req.method || 'GET';
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (method === 'OPTIONS' && url.pathname.startsWith('/api/')) {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end();
    return;
  }

  if (method === 'GET' && url.pathname === '/api/health') {
    const watchUrls = await loadWatchUrls().catch(() => []);
    const subscriptions = await loadPushSubscriptions().catch(() => []);
    const emails = await loadEmailSubscriptions().catch(() => []);
    sendJson(res, 200, {
      ok: true,
      openai_key_loaded: Boolean(OPENAI_API_KEY),
      model: OPENAI_MODEL,
      monitor_enabled: MONITOR_ENABLED,
      monitor_interval_ms: MONITOR_INTERVAL_MS,
      watch_count: watchUrls.length,
      subscription_count: subscriptions.length,
      email_count: emails.length,
      mail_configured: Boolean(RESEND_API_KEY)
    });
    return;
  }

  if (method === 'GET' && url.pathname === '/api/push-public-key') {
    await handlePushPublicKey(req, res);
    return;
  }

  if (method === 'POST' && url.pathname === '/api/push-subscribe') {
    await handlePushSubscribe(req, res);
    return;
  }

  if (method === 'POST' && url.pathname === '/api/send-test-notification') {
    await handleSendTestNotification(req, res);
    return;
  }

  if (method === 'POST' && url.pathname === '/api/email-subscribe') {
    await handleEmailSubscribe(req, res);
    return;
  }

  if (method === 'GET' && url.pathname === '/api/watch-urls') {
    await handleWatchUrls(req, res);
    return;
  }

  if (method === 'POST' && url.pathname === '/api/watch-url') {
    await handleWatchUrl(req, res);
    return;
  }

  if (method === 'POST' && url.pathname === '/api/refresh-watch-urls') {
    await handleRefreshWatchUrls(req, res);
    return;
  }

  if (method === 'POST' && url.pathname === '/api/extract-jobs') {
    await handleExtractJobs(req, res);
    return;
  }

  if (method === 'GET' || method === 'HEAD') {
    await serveStatic(req, res, url.pathname);
    return;
  }

  sendText(res, 405, 'Method not allowed');
});

server.listen(PORT, () => {
  console.log(`Local server running on http://localhost:${PORT}`);
  if (!OPENAI_API_KEY) {
    console.log('OPENAI_API_KEY is missing. Returning heuristic-only extraction for /api/extract-jobs.');
  }
  startMonitor();
});
