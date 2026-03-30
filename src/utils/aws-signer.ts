/**
 * AWS Signature v4 Signing for Cloudflare Workers
 * Implements the AWS SigV4 signing process using Web Crypto API (no Node.js deps).
 * Reference: https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
 */

export interface AwsCredentials {
    accessKeyId: string;
    secretAccessKey: string;
    region: string;
}

// ── Helpers ──

function toHex(buffer: ArrayBuffer): string {
    return [...new Uint8Array(buffer)]
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
}

async function hmacSha256(key: ArrayBuffer | Uint8Array, message: string): Promise<ArrayBuffer> {
    const cryptoKey = await crypto.subtle.importKey(
        'raw',
        key instanceof ArrayBuffer ? key : key.buffer,
        { name: 'HMAC', hash: 'SHA-256' },
        false,
        ['sign']
    );
    return crypto.subtle.sign('HMAC', cryptoKey, new TextEncoder().encode(message));
}

async function sha256(message: string): Promise<string> {
    const hash = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(message));
    return toHex(hash);
}

function getAmzDate(): { amzDate: string; dateStamp: string } {
    const now = new Date();
    const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, '').slice(0, 15) + 'Z';
    const dateStamp = amzDate.slice(0, 8);
    return { amzDate, dateStamp };
}

// ── Main Signing Function ──

export async function signAwsRequest(
    method: string,
    url: string,
    headers: Record<string, string>,
    body: string,
    credentials: AwsCredentials,
    service: string
): Promise<Record<string, string>> {
    const { amzDate, dateStamp } = getAmzDate();
    const parsedUrl = new URL(url);

    // Canonical headers (must be sorted, lowercase)
    const signedHeaderKeys = ['content-type', 'host', 'x-amz-date'];
    const canonicalHeaders =
        `content-type:${headers['Content-Type'] || 'application/x-www-form-urlencoded'}\n` +
        `host:${parsedUrl.host}\n` +
        `x-amz-date:${amzDate}\n`;

    const signedHeaders = signedHeaderKeys.join(';');

    // Canonical request
    const payloadHash = await sha256(body);
    const canonicalRequest = [
        method,
        parsedUrl.pathname || '/',
        parsedUrl.search ? parsedUrl.search.slice(1) : '', // query string without '?'
        canonicalHeaders,
        signedHeaders,
        payloadHash,
    ].join('\n');

    // String to sign
    const credentialScope = `${dateStamp}/${credentials.region}/${service}/aws4_request`;
    const canonicalRequestHash = await sha256(canonicalRequest);
    const stringToSign = [
        'AWS4-HMAC-SHA256',
        amzDate,
        credentialScope,
        canonicalRequestHash,
    ].join('\n');

    // Signing key
    const kDate = await hmacSha256(
        new TextEncoder().encode(`AWS4${credentials.secretAccessKey}`),
        dateStamp
    );
    const kRegion = await hmacSha256(kDate, credentials.region);
    const kService = await hmacSha256(kRegion, service);
    const kSigning = await hmacSha256(kService, 'aws4_request');

    // Signature
    const signature = toHex(await hmacSha256(kSigning, stringToSign));

    // Authorization header
    const authorization = `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;

    return {
        ...headers,
        'x-amz-date': amzDate,
        'Authorization': authorization,
    };
}
