import https from "https";
import http from "http";

const DOMAINS = ["annas-archive.gl", "annas-archive.gd", "annas-archive.pk"];

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

export async function getDownloadUrl(md5: string, secretKey: string): Promise<{ downloadUrl?: string; error?: string }> {
  if (!secretKey) {
    return { error: "No secret key provided. An Anna's Archive membership secret key is required for downloads. Configure it via the X-Annas-Secret-Key header in your MCP client settings." };
  }

  let resp: FastDownloadResponse | undefined;
  let lastError = "";
  for (const domain of DOMAINS) {
    const apiUrl = `https://${domain}/dyn/api/fast_download.json?md5=${md5}&key=${secretKey}`;
    try {
      const body = await fetch(apiUrl);
      resp = JSON.parse(body);
      break;
    } catch (e) {
      lastError = `${e}`;
    }
  }

  if (!resp) {
    return { error: `All domains failed. Last error: ${lastError}` };
  }

  if (resp.error) {
    if (resp.error === "Invalid secret key") {
      return { error: "Invalid secret key. Check that your Anna's Archive membership secret key is correct. You can find it at https://annas-archive.gl/account ." };
    }
    return { error: resp.error };
  }
  if (!resp.download_url) {
    return { error: "No download URL in response" };
  }

  return { downloadUrl: resp.download_url };
}
