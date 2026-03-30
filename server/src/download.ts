import fs from "fs";
import path from "path";
import https from "https";
import http from "http";

const SECRET_KEY = process.env.ANNAS_SECRET_KEY || "";
const BASE_URL = process.env.ANNAS_BASE_URL || "annas-archive.gl";
const DOWNLOAD_PATH = process.env.ANNAS_DOWNLOAD_PATH || "./downloads";

interface FastDownloadResponse {
  download_url?: string;
  error?: string;
}

function fetch(url: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    client.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location).then(resolve, reject);
      }
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => resolve(data));
      res.on("error", reject);
    }).on("error", reject);
  });
}

function downloadFile(url: string, dest: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    client.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return downloadFile(res.headers.location, dest).then(resolve, reject);
      }
      if (res.statusCode !== 200) {
        return reject(new Error(`Download failed with status ${res.statusCode}`));
      }
      const fileStream = fs.createWriteStream(dest);
      res.pipe(fileStream);
      fileStream.on("finish", () => {
        fileStream.close();
        resolve();
      });
      fileStream.on("error", reject);
    }).on("error", reject);
  });
}

export async function download(md5: string, filename?: string): Promise<{ filePath: string; error?: string }> {
  if (!SECRET_KEY) {
    return { filePath: "", error: "ANNAS_SECRET_KEY not configured" };
  }

  const apiUrl = `https://${BASE_URL}/dyn/api/fast_download.json?md5=${md5}&key=${SECRET_KEY}`;

  // Check cache first
  const subdir = md5.slice(0, 2);
  const cacheDir = path.join(DOWNLOAD_PATH, subdir);
  const existingFiles = fs.existsSync(cacheDir)
    ? fs.readdirSync(cacheDir).filter((f) => f.startsWith(md5))
    : [];

  if (existingFiles.length > 0) {
    return { filePath: path.join(cacheDir, existingFiles[0]) };
  }

  let resp: FastDownloadResponse;
  try {
    const body = await fetch(apiUrl);
    resp = JSON.parse(body);
  } catch (e) {
    return { filePath: "", error: `API request failed: ${e}` };
  }

  if (resp.error) {
    return { filePath: "", error: resp.error };
  }
  if (!resp.download_url) {
    return { filePath: "", error: "No download URL in response" };
  }

  const ext = filename ? path.extname(filename) : "";
  const destFilename = filename || `${md5}${ext}`;
  fs.mkdirSync(cacheDir, { recursive: true });
  const destPath = path.join(cacheDir, destFilename);

  try {
    await downloadFile(resp.download_url, destPath);
  } catch (e) {
    return { filePath: "", error: `Download failed: ${e}` };
  }

  return { filePath: destPath };
}
