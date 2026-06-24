import type { Env } from '../index';
import { 
    geminiUrl, 
    geminiFallbackUrl, 
    GEMINI_PRO_MODEL, 
    GEMINI_LITE_MODEL, 
    GEMINI_PRO_FALLBACK, 
    GEMINI_LITE_FALLBACK 
} from '../config/gemini';
import { logGeminiError } from './gemini-logger';

export interface GeminiFetchOptions extends RequestInit {
    timeoutMs?: number;
    activityName?: string;
    maxRetries?: number;
}

export async function fetchGemini(
    env: Env,
    tier: 'lite' | 'pro',
    init: GeminiFetchOptions
): Promise<Response> {
    const timeoutMs = init.timeoutMs || 120000; // Increased default timeout to 120s for massive SEC context windows
    const maxRetries = init.maxRetries ?? (tier === 'lite' ? 5 : 3);
    const activityName = init.activityName || 'fetchGemini';

    const primaryUrl = geminiUrl(env.GEMINI_API_KEY, tier);
    const fallbackUrl = geminiFallbackUrl(env.GEMINI_API_KEY, tier);

    let attempt = 0;
    let fallbackTriggered = false;

    while (true) {
        attempt++;
        const currentUrl = fallbackTriggered ? fallbackUrl : primaryUrl;
        const currentModel = fallbackTriggered 
            ? (tier === 'lite' ? GEMINI_LITE_FALLBACK : GEMINI_PRO_FALLBACK)
            : (tier === 'lite' ? GEMINI_LITE_MODEL : GEMINI_PRO_MODEL);

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        let res: Response | null = null;
        let caughtError: any = null;

        try {
            res = await fetch(currentUrl, {
                ...init,
                signal: controller.signal,
            });
        } catch (err: any) {
            caughtError = err;
        } finally {
            clearTimeout(timer);
        }

        // Determine if we encountered a transient / retryable issue
        let isRetryable = false;
        let errorMessage = '';

        if (caughtError) {
            isRetryable = true;
            if (caughtError.name === 'AbortError') {
                errorMessage = `Timeout after ${timeoutMs}ms`;
            } else {
                errorMessage = caughtError.message || String(caughtError);
            }
        } else if (res) {
            const status = res.status;
            if (status === 429 || status === 500 || status === 502 || status === 503 || status === 504) {
                isRetryable = true;
                errorMessage = `HTTP ${status}`;
                if (status === 429) {
                    try {
                        const cloned = res.clone();
                        const text = await cloned.text();
                        if (text.includes('spending cap') || text.includes('RESOURCE_EXHAUSTED')) {
                            isRetryable = false;
                            errorMessage = `HTTP 429 (Spending Cap Exceeded)`;
                        }
                    } catch (_) {}
                }
            } else if (status === 400) {
                try {
                    const cloned = res.clone();
                    const text = await cloned.text();
                    if (
                        text.includes('location is not supported') || 
                        text.includes('LocationNotSupported') || 
                        text.includes('not supported for the API use')
                    ) {
                        isRetryable = true;
                        errorMessage = `HTTP 400 (User Location Not Supported)`;
                    }
                } catch (_) {}
            }
        }

        if (isRetryable) {
            const isOverload = errorMessage.includes('503') || errorMessage.includes('429');
            const isLocationError = errorMessage.includes('User Location Not Supported');
            const currentMaxRetries = maxRetries; // Use standard retries (e.g., 3) to prevent worker timeouts

            // If we have retries left on the current model, do standard exponential backoff and retry (skip if location block)
            if (!isLocationError && attempt <= currentMaxRetries) {
                const backoffMultiplier = 2.0; // Standard exponential backoff multiplier
                const baseDelay = Math.pow(backoffMultiplier, attempt) * 1000;
                const jitter = Math.random() * 1000; // 1s jitter to avoid thundering herd
                const maxDelay = 10000; // Limit max delay to 10s to stay within Cloudflare execution limits
                const delayMs = Math.min(maxDelay, baseDelay + jitter);
                
                console.warn(`⚠️ Gemini ${currentModel} encountered error: ${errorMessage}. Retrying in ${(delayMs / 1000).toFixed(1)}s (attempt ${attempt}/${currentMaxRetries})...`);
                await new Promise(r => setTimeout(r, delayMs));
                continue;
            }

            // Retries exhausted on primary OR immediate switch on location error -> Switch to fallback
            if (!fallbackTriggered) {
                console.warn(`⚠️ Gemini primary ${currentModel} ${isLocationError ? 'immediately falling back' : 'exhausted retries'} due to error: ${errorMessage}. Falling back to alternative model.`);
                await logGeminiError(env, currentModel, activityName, new Error(`[Primary Exhausted, Falling Back] ${errorMessage}`));
                
                fallbackTriggered = true;
                attempt = 0; // Reset attempts for the fallback model
                continue;
            }

            // Retries exhausted on fallback -> Log and fail
            console.error(`❌ Gemini fallback ${currentModel} also failed: ${errorMessage}. Bubbling up error.`);
            await logGeminiError(env, currentModel, activityName, new Error(`Fallback exhausted: ${errorMessage}`));
            
            if (caughtError) {
                throw caughtError;
            }
            return res!;
        }

        // Success or non-retryable response (e.g. 400 Bad Request, 403 Forbidden, 404, etc.)
        return res!;
    }
}
