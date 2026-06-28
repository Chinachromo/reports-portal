import http from "node:http";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 3000);
const APP_VERSION = "reports-20260628-auto-persistent-data";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "reports123";
const INGEST_TOKEN = process.env.INGEST_TOKEN || "";
const PERSISTENT_DATA_DIR = "/var/data";
const DATA_DIR = process.env.DATA_DIR || (fssync.existsSync(PERSISTENT_DATA_DIR) ? PERSISTENT_DATA_DIR : path.join(__dirname, "data"));
const PUBLIC_DIR = path.join(__dirname, "public");
const REPORTS_FILE = path.join(DATA_DIR, "reports.json");
const REPORT_FILES_DIR = path.join(DATA_DIR, "report-files");

let writeQueue = Promise.resolve();

async function ensureData() {
  await fs.mkdir(REPORT_FILES_DIR, { recursive: true });
  if (!fssync.existsSync(REPORTS_FILE)) await fs.writeFile(REPORTS_FILE, "[]");
}

async function readJson(file, fallback) {
  try {
    return JSON.parse(await fs.readFile(file, "utf8"));
  } catch {
    return fallback;
  }
}

async function writeJson(file, value) {
  await fs.writeFile(file, JSON.stringify(value, null, 2));
}

function queuedWrite(fn) {
  const next = writeQueue.then(fn, fn);
  writeQueue = next.catch(() => {});
  return next;
}

function send(res, status, payload, headers = {}) {
  const isText = typeof payload === "string" || Buffer.isBuffer(payload);
  res.writeHead(status, {
    "content-type": isText ? "text/plain; charset=utf-8" : "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...headers,
  });
  res.end(isText ? payload : JSON.stringify(payload));
}

function sendJson(res, status, payload) {
  send(res, status, payload);
}

async function readBody(req, limit = 80 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw new Error("上传文件太大");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function normalizeBatch(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/[\s_\-–—/\\]+/g, "")
    .trim()
    .toLowerCase();
}

function sanitizeFilename(value) {
  const name = String(value || "report")
    .normalize("NFKC")
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, "_")
    .replace(/\s+/g, " ")
    .trim();
  return name || "report";
}

function sampleBuffer(buffer, limit = 8 * 1024 * 1024) {
  if (!Buffer.isBuffer(buffer) || buffer.length <= limit) return buffer;
  const half = Math.floor(limit / 2);
  return Buffer.concat([buffer.slice(0, half), Buffer.from("\n"), buffer.slice(buffer.length - half)]);
}

function compactSearchText(value) {
  return String(value || "")
    .normalize("NFKC")
    .replace(/\0/g, " ")
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]+/g, " ")
    .replace(/\s+/g, " ")
    .slice(0, 600000);
}

function decodeUtf16Be(buffer) {
  const sample = sampleBuffer(buffer, 2 * 1024 * 1024);
  let text = "";
  for (let index = 0; index + 1 < sample.length; index += 2) {
    const code = sample.readUInt16BE(index);
    if (code === 0) continue;
    if (code >= 32 && code <= 0xfffd) text += String.fromCharCode(code);
    if (text.length > 200000) break;
  }
  return text;
}

function extractPdfLiteralText(text) {
  const matches = String(text || "").match(/\((?:\\.|[^\\)]){1,160}\)/g) || [];
  return matches
    .slice(0, 3000)
    .map((item) =>
      item
        .slice(1, -1)
        .replace(/\\([nrtbf()\\])/g, " ")
        .replace(/\\\d{1,3}/g, " ")
    )
    .join(" ");
}

function cleanBatchCandidate(value) {
  const cleaned = String(value || "")
    .normalize("NFKC")
    .trim()
    .replace(/^[\s:：#.\-_/\\]+/, "")
    .replace(/\.(pdf|docx?|xlsx?|xls|jpe?g|png)$/i, "")
    .replace(/[\s,，;；.。)）\]}】]+$/g, "")
    .replace(/[^A-Za-z0-9._\-\/]/g, "")
    .slice(0, 48);
  const rejectedWords = new Set(["report", "number", "num", "no", "date", "certificate", "product", "item", "name"]);
  return rejectedWords.has(cleaned.toLowerCase()) ? "" : cleaned;
}

function batchCandidatesFromText(text, source = "text") {
  const input = compactSearchText(text);
  const candidates = [];
  const labelledPatterns = [
    /(?:LOT|BATCH)\s*(?:\.?\s*(?:NO|NUMBER|NUM)\.?)\s*[:：#.\-]*\s*([A-Za-z0-9][A-Za-z0-9._\-\/]{2,47})/gi,
    /(?:LOT|BATCH)\s*[#：:]\s*([A-Za-z0-9][A-Za-z0-9._\-\/]{2,47})/gi,
    /(?:批号|批次号|生产批号)\s*[:：#.\-]*\s*([A-Za-z0-9][A-Za-z0-9._\-\/]{2,47})/gi,
  ];

  for (const pattern of labelledPatterns) {
    let match;
    while ((match = pattern.exec(input)) !== null) {
      const value = cleanBatchCandidate(match[1]);
      if (value && value.length >= 3) candidates.push({ value, source, confidence: source === "filename" ? 0.72 : 0.88 });
    }
  }

  if (source === "filename") {
    const genericMatches = input.match(/\b(?:LOT|BATCH)[A-Za-z0-9._\-\/]{3,47}\b/gi) || [];
    for (const item of genericMatches) {
      const value = cleanBatchCandidate(item);
      if (value && value.length >= 4) candidates.push({ value, source, confidence: 0.62 });
    }
  }

  return candidates;
}

function filenameBatchCandidates(fileName, mode = "leading") {
  const name = String(fileName || "").normalize("NFKC");
  const readableName = name.replace(/[._\-]+/g, " ");
  const candidates = [...batchCandidatesFromText(readableName, "filename")];
  const modeValue = mode || "leading";

  if (modeValue === "leading") {
    const dateCodeMatches = name.match(/\b[A-Za-z]?\d{6,8}[-_]\d{2,5}[A-Za-z0-9_-]*\b/g) || [];
    for (const item of dateCodeMatches) {
      candidates.push({ value: item, source: "filename-date-code", confidence: 1.15 });
    }
  }

  if (modeValue === "s-number") {
    const matches = name.match(/\bS\d{6,}\b/gi) || [];
    for (const item of matches) candidates.push({ value: item, source: "filename-s-number", confidence: 1.1 });
  }

  if (modeValue === "wo-number") {
    const matches = [...name.matchAll(/\bWO\s*#?\s*([A-Za-z0-9._\-\/]{4,})/gi)];
    for (const match of matches) candidates.push({ value: `WO${match[1]}`, source: "filename-wo-number", confidence: 1.1 });
  }

  return candidates;
}

function searchableTextFromFile(file) {
  const sample = sampleBuffer(file.body || Buffer.alloc(0));
  const latin = sample.toString("latin1");
  return [
    file.filename || "",
    sample.toString("utf8"),
    latin,
    decodeUtf16Be(sample),
    extractPdfLiteralText(latin),
  ].join("\n");
}

function rankBatchCandidates(rawCandidates) {
  const scoreByKey = new Map();

  for (const candidate of rawCandidates) {
    const value = cleanBatchCandidate(candidate.value);
    if (!value) continue;
    const key = normalizeBatch(value);
    if (!key || key.length < 3) continue;
    const existing = scoreByKey.get(key) || { value, score: 0, count: 0, sources: new Set(), files: new Set() };
    existing.score += candidate.confidence || 0.5;
    existing.count += 1;
    existing.sources.add(candidate.source || "text");
    if (candidate.fileName) existing.files.add(candidate.fileName);
    scoreByKey.set(key, existing);
  }

  const candidates = [...scoreByKey.values()]
    .map((item) => ({
      value: item.value,
      score: Number(item.score.toFixed(2)),
      count: item.count,
      sources: [...item.sources],
      files: [...item.files],
    }))
    .sort((a, b) => b.score - a.score || b.count - a.count || a.value.localeCompare(b.value))
    .slice(0, 10);

  const best = candidates[0] || null;
  const second = candidates[1] || null;
  const isClear = Boolean(best && (!second || best.score >= second.score + 0.5 || normalizeBatch(best.value) === normalizeBatch(second.value)));
  return { batchNo: isClear ? best.value : "", candidates };
}

function detectBatchFromFiles(files, options = {}) {
  const detections = [];
  const allCandidates = [];
  const filenameMode = options.filenameMode || "leading";

  for (const file of files) {
    const originalName = sanitizeFilename(file.filename);
    const fileCandidates = [
      ...filenameBatchCandidates(originalName, filenameMode),
      ...batchCandidatesFromText(searchableTextFromFile(file), "report"),
    ].map((candidate) => ({ ...candidate, fileName: originalName }));
    const fileRanking = rankBatchCandidates(fileCandidates);
    allCandidates.push(...fileCandidates);
    detections.push({
      originalName,
      batchNo: fileRanking.batchNo,
      candidates: fileRanking.candidates.map((candidate) => candidate.value).slice(0, 8),
    });
  }

  const aggregate = rankBatchCandidates(allCandidates);
  return { ...aggregate, detections, groups: detectedGroups(detections) };
}

function detectedGroups(detections) {
  const groups = new Map();
  for (const item of detections) {
    if (!item.batchNo) continue;
    const key = normalizeBatch(item.batchNo);
    const group = groups.get(key) || { batchNo: item.batchNo, files: [] };
    group.files.push(item.originalName);
    groups.set(key, group);
  }
  return [...groups.values()].sort((a, b) => a.batchNo.localeCompare(b.batchNo));
}

function savedGroups(records) {
  const groups = new Map();
  for (const record of records) {
    const key = record.batchKey || normalizeBatch(record.batchNo);
    const group = groups.get(key) || { batchNo: record.batchNo, count: 0, reports: [] };
    group.count += 1;
    group.reports.push(publicReport(record));
    groups.set(key, group);
  }
  return [...groups.values()].sort((a, b) => a.batchNo.localeCompare(b.batchNo));
}

function mimeType(fileName, fallback = "application/octet-stream") {
  const ext = path.extname(fileName).toLowerCase();
  return {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".pdf": "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls": "application/vnd.ms-excel",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".doc": "application/msword",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
  }[ext] || fallback;
}

function parseMultipart(buffer, contentType) {
  const boundary = contentType.match(/boundary=(?:"([^"]+)"|([^;]+))/)?.[1] || contentType.match(/boundary=(?:"([^"]+)"|([^;]+))/)?.[2];
  if (!boundary) throw new Error("没有找到上传边界");
  const delimiter = Buffer.from(`--${boundary}`);
  const parts = [];
  let start = buffer.indexOf(delimiter);
  while (start >= 0) {
    const next = buffer.indexOf(delimiter, start + delimiter.length);
    if (next < 0) break;
    let part = buffer.slice(start + delimiter.length, next);
    if (part.slice(0, 2).toString() === "\r\n") part = part.slice(2);
    if (part.slice(-2).toString() === "\r\n") part = part.slice(0, -2);
    const headerEnd = part.indexOf(Buffer.from("\r\n\r\n"));
    if (headerEnd > 0) {
      const headerText = part.slice(0, headerEnd).toString("latin1");
      const body = part.slice(headerEnd + 4);
      const name = headerText.match(/name="([^"]+)"/)?.[1] || "";
      const filename = headerText.match(/filename="([^"]*)"/)?.[1] || "";
      const contentTypeMatch = headerText.match(/content-type:\s*([^\r\n]+)/i)?.[1] || "";
      parts.push({ name, filename, contentType: contentTypeMatch.trim(), body });
    }
    start = next;
  }
  return parts;
}

function partText(parts, name) {
  return parts.find((part) => part.name === name)?.body?.toString("utf8").trim() || "";
}

function publicReport(record) {
  return {
    id: record.id,
    batchNo: record.batchNo,
    originalName: record.originalName || "report",
    size: record.size || 0,
    uploadedAt: record.uploadedAt || "",
    uploadedAtText: record.uploadedAt ? new Date(record.uploadedAt).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" }) : "",
  };
}

function requireAdmin(req, res) {
  if (req.headers["x-admin-code"] === ADMIN_PASSWORD) return true;
  sendJson(res, 403, { error: "请输入正确的管理员口令" });
  return false;
}

function requireIngest(req, res) {
  if (INGEST_TOKEN && req.headers["x-ingest-token"] === INGEST_TOKEN) return true;
  sendJson(res, 403, { error: "自动上传口令不正确，或尚未启用自动上传" });
  return false;
}

async function saveUploadedReports(parts, source = "admin") {
  const manualBatchNo = partText(parts, "batchNo");
  const filenameMode = partText(parts, "filenameMode") || "leading";
  const files = parts.filter((part) => (part.name === "files" || part.name === "file") && part.filename && part.body?.length);
  if (!files.length) throw new Error("请上传报告文件");

  const reports = await readJson(REPORTS_FILE, []);
  const saved = [];
  const perFileDetections = new Map(files.map((file) => [file, detectBatchFromFiles([file], { filenameMode })]));
  const unresolved = [];

  if (!manualBatchNo) {
    for (const file of files) {
      const detection = perFileDetections.get(file);
      if (!detection?.batchNo) {
        const originalName = sanitizeFilename(file.filename);
        const candidates = detection?.candidates?.map((item) => item.value).slice(0, 5) || [];
        unresolved.push(candidates.length ? `${originalName}（可能是：${candidates.join("、")}）` : originalName);
      }
    }
  }

  if (unresolved.length) {
    throw new Error(`有 ${unresolved.length} 个文件无法明确识别批次号：${unresolved.slice(0, 5).join("；")}。请把这些文件单独上传并手动填写批次号。`);
  }

  for (const file of files) {
    const detection = perFileDetections.get(file);
    const batchNo = (manualBatchNo || detection?.batchNo || "").trim();
    if (!batchNo) throw new Error("没有自动识别到批次号，请手动填写后再上传。");
    const batchKey = normalizeBatch(batchNo);
    const id = crypto.randomUUID();
    const originalName = sanitizeFilename(file.filename);
    const ext = path.extname(originalName).toLowerCase();
    const storedName = `${id}${ext || ".bin"}`;
    const storedPath = path.join(REPORT_FILES_DIR, storedName);
    await fs.writeFile(storedPath, file.body);
    const record = {
      id,
      batchNo: batchNo.trim(),
      batchKey,
      originalName,
      storedName,
      contentType: file.contentType || mimeType(originalName),
      size: file.body.length,
      uploadedAt: new Date().toISOString(),
      source,
      autoDetectedBatch: !manualBatchNo,
    };
    reports.push(record);
    saved.push(record);
  }

  await writeJson(REPORTS_FILE, reports);
  return { saved, groups: savedGroups(saved) };
}

async function handleApi(req, res, url) {
  if (url.pathname === "/api/health") {
    return sendJson(res, 200, {
      ok: true,
      version: APP_VERSION,
      dataDir: DATA_DIR,
      persistentStorage: DATA_DIR === PERSISTENT_DATA_DIR,
      ingestEnabled: Boolean(INGEST_TOKEN),
    });
  }

  if (url.pathname === "/api/reports" && req.method === "GET") {
    const batchNo = url.searchParams.get("batch") || "";
    const batchKey = normalizeBatch(batchNo);
    if (!batchKey) return sendJson(res, 400, { error: "请输入批次号" });
    const reports = await readJson(REPORTS_FILE, []);
    const matches = reports
      .filter((record) => record.batchKey === batchKey)
      .sort((a, b) => String(b.uploadedAt).localeCompare(String(a.uploadedAt)))
      .map(publicReport);
    return sendJson(res, 200, { batchNo, reports: matches });
  }

  if (url.pathname.startsWith("/api/reports/download/") && req.method === "GET") {
    const id = decodeURIComponent(url.pathname.replace("/api/reports/download/", ""));
    const reports = await readJson(REPORTS_FILE, []);
    const report = reports.find((record) => record.id === id);
    if (!report) return sendJson(res, 404, { error: "没有找到这个报告" });
    const filePath = path.join(REPORT_FILES_DIR, report.storedName || "");
    if (!filePath.startsWith(REPORT_FILES_DIR) || !fssync.existsSync(filePath)) {
      return sendJson(res, 404, { error: "报告文件不存在" });
    }
    const data = await fs.readFile(filePath);
    const filename = sanitizeFilename(report.originalName || "report");
    res.writeHead(200, {
      "content-type": report.contentType || mimeType(filename),
      "content-length": data.length,
      "content-disposition": `attachment; filename="${encodeURIComponent(filename)}"; filename*=UTF-8''${encodeURIComponent(filename)}`,
      "cache-control": "private, no-store",
    });
    res.end(data);
    return;
  }

  if (url.pathname === "/api/admin/reports/upload" && req.method === "POST") {
    if (!requireAdmin(req, res)) return;
    const body = await readBody(req);
    const parts = parseMultipart(body, req.headers["content-type"] || "");
    return queuedWrite(async () => {
      const result = await saveUploadedReports(parts, "admin");
      sendJson(res, 200, {
        ok: true,
        batchNo: result.saved[0]?.batchNo || "",
        count: result.saved.length,
        groups: result.groups,
        reports: result.saved.map(publicReport),
      });
    });
  }

  if (url.pathname === "/api/admin/reports/detect" && req.method === "POST") {
    if (!requireAdmin(req, res)) return;
    const body = await readBody(req);
    const parts = parseMultipart(body, req.headers["content-type"] || "");
    const files = parts.filter((part) => (part.name === "files" || part.name === "file") && part.filename && part.body?.length);
    if (!files.length) return sendJson(res, 400, { error: "请先选择报告文件" });
    const filenameMode = partText(parts, "filenameMode") || "leading";
    return sendJson(res, 200, { ok: true, ...detectBatchFromFiles(files, { filenameMode }) });
  }

  if (url.pathname === "/api/ingest/report" && req.method === "POST") {
    if (!requireIngest(req, res)) return;
    const body = await readBody(req);
    const parts = parseMultipart(body, req.headers["content-type"] || "");
    return queuedWrite(async () => {
      const result = await saveUploadedReports(parts, "ingest");
      sendJson(res, 200, { ok: true, count: result.saved.length, groups: result.groups, reports: result.saved.map(publicReport) });
    });
  }

  sendJson(res, 404, { error: "接口不存在" });
}

async function serveStatic(req, res, url) {
  const requested = decodeURIComponent(url.pathname === "/" || url.pathname === "/admin" ? "/index.html" : url.pathname);
  const safePath = path
    .normalize(requested)
    .replace(/^[/\\]+/, "")
    .replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(PUBLIC_DIR, safePath);
  if (!filePath.startsWith(PUBLIC_DIR)) return send(res, 403, "Forbidden");
  try {
    const data = await fs.readFile(filePath);
    res.writeHead(200, {
      "content-type": mimeType(filePath),
      "cache-control": "no-store",
    });
    res.end(data);
  } catch {
    send(res, 404, `文件没有找到。版本：${APP_VERSION}。`);
  }
}

await ensureData();

http
  .createServer(async (req, res) => {
    try {
      const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
      if (url.pathname.startsWith("/api/")) await handleApi(req, res, url);
      else await serveStatic(req, res, url);
    } catch (error) {
      sendJson(res, 500, { error: error.message || "服务器出错" });
    }
  })
  .listen(PORT, () => {
    console.log(`REPORTS running on http://localhost:${PORT}`);
  });
