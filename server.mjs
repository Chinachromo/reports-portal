import http from "node:http";
import fs from "node:fs/promises";
import fssync from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 3000);
const APP_VERSION = "reports-20260627-mvp";
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || "reports123";
const INGEST_TOKEN = process.env.INGEST_TOKEN || "";
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
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
    productName: record.productName || "",
    productCode: record.productCode || "",
    supplierName: record.supplierName || "",
    reportType: record.reportType || "Report",
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
  const batchNo = partText(parts, "batchNo");
  if (!batchNo) throw new Error("请填写批次号");
  const files = parts.filter((part) => (part.name === "files" || part.name === "file") && part.filename && part.body?.length);
  if (!files.length) throw new Error("请上传报告文件");

  const batchKey = normalizeBatch(batchNo);
  const productName = partText(parts, "productName");
  const productCode = partText(parts, "productCode");
  const supplierName = partText(parts, "supplierName");
  const reportType = partText(parts, "reportType") || "Report";
  const reports = await readJson(REPORTS_FILE, []);
  const saved = [];

  for (const file of files) {
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
      productName,
      productCode,
      supplierName,
      reportType,
      originalName,
      storedName,
      contentType: file.contentType || mimeType(originalName),
      size: file.body.length,
      uploadedAt: new Date().toISOString(),
      source,
    };
    reports.push(record);
    saved.push(record);
  }

  await writeJson(REPORTS_FILE, reports);
  return saved;
}

async function handleApi(req, res, url) {
  if (url.pathname === "/api/health") {
    return sendJson(res, 200, {
      ok: true,
      version: APP_VERSION,
      dataDir: DATA_DIR,
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
      const saved = await saveUploadedReports(parts, "admin");
      sendJson(res, 200, { ok: true, count: saved.length, reports: saved.map(publicReport) });
    });
  }

  if (url.pathname === "/api/ingest/report" && req.method === "POST") {
    if (!requireIngest(req, res)) return;
    const body = await readBody(req);
    const parts = parseMultipart(body, req.headers["content-type"] || "");
    return queuedWrite(async () => {
      const saved = await saveUploadedReports(parts, "ingest");
      sendJson(res, 200, { ok: true, count: saved.length, reports: saved.map(publicReport) });
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
