export interface ApnsPayload {
    aps: {
        alert: {
            title: string;
            body: string;
        };
        sound?: string;
        badge?: number;
    };
    [key: string]: any;
}

/**
 * Converts a PEM string (PKCS#8 format private key) to a DER Uint8Array
 */
function pemToDer(pem: string): Uint8Array {
    const pemHeader = "-----BEGIN PRIVATE KEY-----";
    const pemFooter = "-----END PRIVATE KEY-----";
    const cleanPem = pem
        .replace(pemHeader, "")
        .replace(pemFooter, "")
        .replace(/\s/g, "");
    
    const rawBinary = atob(cleanPem);
    const buffer = new Uint8Array(rawBinary.length);
    for (let i = 0; i < rawBinary.length; i++) {
        buffer[i] = rawBinary.charCodeAt(i);
    }
    return buffer;
}

/**
 * Encodes an ArrayBuffer to a base64url string
 */
function base64url(arrayBuffer: ArrayBufferLike): string {
    const bytes = new Uint8Array(arrayBuffer);
    let binary = "";
    for (let i = 0; i < bytes.byteLength; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary)
        .replace(/=/g, "")
        .replace(/\+/g, "-")
        .replace(/\//g, "_");
}

/**
 * Encodes a regular string to base64url format
 */
function stringToBase64Url(str: string): string {
    const enc = new TextEncoder();
    return base64url(enc.encode(str).buffer);
}

/**
 * Generates APNS signed JSON Web Token (JWT) using ES256
 */
export async function generateApnsJwt(
    privateKeyPem: string,
    keyId: string,
    teamId: string
): Promise<string> {
    const header = JSON.stringify({ alg: "ES256", kid: keyId });
    const payload = JSON.stringify({ iss: teamId, iat: Math.floor(Date.now() / 1000) });

    const headerB64 = stringToBase64Url(header);
    const payloadB64 = stringToBase64Url(payload);
    const message = `${headerB64}.${payloadB64}`;

    const der = pemToDer(privateKeyPem);
    const key = await crypto.subtle.importKey(
        "pkcs8",
        der,
        {
            name: "ECDSA",
            namedCurve: "P-256"
        },
        false,
        ["sign"]
    );

    const encoder = new TextEncoder();
    const signature = await crypto.subtle.sign(
        {
            name: "ECDSA",
            hash: { name: "SHA-256" }
        },
        key,
        encoder.encode(message)
    );

    const signatureB64 = base64url(signature);
    return `${message}.${signatureB64}`;
}

/**
 * Sends a push notification to Apple Push Notification service (APNS)
 */
export async function sendApnsNotification(
    env: {
        APNS_PRIVATE_KEY?: string;
        APNS_KEY_ID?: string;
        APNS_TEAM_ID?: string;
        APNS_APP_BUNDLE_ID?: string;
        ENVIRONMENT?: string;
    },
    deviceToken: string,
    title: string,
    bodyText: string,
    customData: Record<string, any> = {}
): Promise<boolean> {
    const privateKey = env.APNS_PRIVATE_KEY;
    const keyId = env.APNS_KEY_ID;
    const teamId = env.APNS_TEAM_ID;
    const bundleId = env.APNS_APP_BUNDLE_ID || "com.keoflex.apex";

    if (!privateKey || !keyId || !teamId) {
        console.warn("⚠️ APNS credentials missing. Skipping push notification.");
        return false;
    }

    try {
        const token = await generateApnsJwt(privateKey, keyId, teamId);
        
        // APNS endpoint
        const isProd = env.ENVIRONMENT === "production";
        const host = isProd 
            ? "https://api.push.apple.com" 
            : "https://api.development.push.apple.com";
            
        const url = `${host}/3/device/${deviceToken}`;

        const payload: ApnsPayload = {
            aps: {
                alert: {
                    title,
                    body: bodyText
                },
                sound: "default"
            },
            ...customData
        };

        const res = await fetch(url, {
            method: "POST",
            headers: {
                "Authorization": `bearer ${token}`,
                "apns-topic": bundleId,
                "apns-push-type": "alert",
                "Content-Type": "application/json"
            },
            body: JSON.stringify(payload)
        });

        if (res.ok) {
            console.log(`✅ Push notification sent successfully to token: ${deviceToken.substring(0, 10)}...`);
            return true;
        } else {
            const errText = await res.text();
            console.error(`❌ APNS returned error status ${res.status}: ${errText}`);
            return false;
        }
    } catch (err) {
        console.error("❌ APNS send failed:", err);
        return false;
    }
}
