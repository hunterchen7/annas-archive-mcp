import { isMd5 } from "./identifiers.js";
import { invalidateKey, keyValidationError, validateKey } from "./auth.js";
import { assertSafeOutboundUrl, safeRequest } from "./safeHttp.js";

const DOMAINS = ["annas-archive.gl", "annas-archive.gd", "annas-archive.pk"];
const ALLOWED_HOSTS = new Set(DOMAINS);

interface FastDownloadResponse {
  download_url?: string;
  error?: string;
}

export async function getDownloadUrl(md5: string, secretKey: string): Promise<{ downloadUrl?: string; error?: string }> {
  if (!isMd5(md5)) {
    return { error: "Invalid MD5: expected exactly 32 hexadecimal characters." };
  }
  md5 = md5.toLowerCase();
  const authError = keyValidationError(await validateKey(secretKey));
  if (authError) {
    return { error: authError.message };
  }

  let resp: FastDownloadResponse | undefined;
  for (const domain of DOMAINS) {
    const apiUrl = new URL(`https://${domain}/dyn/api/fast_download.json`);
    apiUrl.searchParams.set("md5", md5);
    apiUrl.searchParams.set("key", secretKey);
    try {
      const response = await safeRequest(apiUrl, {
        allowedHosts: ALLOWED_HOSTS,
        maxBytes: 1024 * 1024,
        maxRedirects: 2,
      });
      if (response.statusCode !== 200) {
        throw new Error(`AA API returned HTTP ${response.statusCode}`);
      }
      resp = JSON.parse(response.body.toString("utf-8"));
      break;
    } catch (e) {
      const rawCode = typeof e === "object" && e !== null && "code" in e
        ? String((e as { code?: unknown }).code)
        : "request_failed";
      const code = /^[A-Z0-9_]{1,40}$/i.test(rawCode) ? rawCode : "request_failed";
      console.error(`Anna's Archive download API request failed for ${domain}: ${code}`);
    }
  }

  if (!resp) {
    return { error: "Could not reach Anna's Archive to request a download URL." };
  }

  if (resp.error) {
    if (resp.error === "Invalid secret key") {
      invalidateKey(secretKey);
      return { error: "Invalid secret key. Check that your Anna's Archive membership secret key is correct. You can find it at https://annas-archive.gl/account ." };
    }
    return { error: "Anna's Archive rejected the download request." };
  }
  if (!resp.download_url) {
    return { error: "No download URL in response" };
  }

  try {
    const safeUrl = await assertSafeOutboundUrl(resp.download_url);
    return { downloadUrl: safeUrl.toString() };
  } catch {
    return { error: "Anna's Archive returned an unsafe download URL" };
  }
}
