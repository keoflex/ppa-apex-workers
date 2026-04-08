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

/**
 * Robust fetch wrapper for Gemini API calls.
 * Implements exponential backoff for 503/429 errors, Request abort timeouts, and model fallbacks.
 */
export async function fetchGemini(
    env: Env,
    tier: 'lite' | 'pro',
    init: GeminiFetchOptions
): Promise<Response> {
    const timeoutMs = init.timeoutMs || 120000; // Increased default timeout to 120s for massive SEC context windows
    const maxRetries = init.maxRetries ?? 2;
    const activityName = init.activityName || 'fetchGemini';

    const primaryUrl = geminiUrl(env.GEMINI_API_KEY, tier);
    const fallbackUrl = geminiFallbackUrl(env.GEMINI_API_KEY, tier);

    let attempt = 0;
    let fallbackTriggered = false;

    while (attempt <= maxRetries) {
        attempt++;
        const currentUrl = fallbackTriggered ? fallbackUrl : primaryUrl;
        const currentModel = fallbackTriggered 
            ? (tier === 'lite' ? GEMINI_LITE_FALLBACK : GEMINI_PRO_FALLBACK)
            : (tier === 'lite' ? GEMINI_LITE_MODEL : GEMINI_PRO_MODEL);

        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const res = await fetch(currentUrl, {
                ...init,
                signal: controller.signal,
            });

            // 503 (Unavailable) or 429 (Too Many Requests) - candidate for retry
            if (res.status === 503 || res.status === 429) {
                if (attempt <= maxRetries && !fallbackTriggered) {
                    console.warn(`⚠️ Gemini ${currentModel} returned ${res.status}. Retrying in ${attempt * 2}s...`);
                    await new Promise(r => setTimeout(r, attempt * 2000));
                    continue; // Retry primary
                }
                
                // Max retries reached on primary, trigger fallback and try ONE more time
                if (!fallbackTriggered) {
                    console.warn(`⚠️ Gemini primary ${currentModel} exhausted retries. Falling back to alternative model.`);
                    await logGeminiError(env, currentModel, activityName, new Error(`Exhausted retries on HTTP ${res.status}`));
                    
                    fallbackTriggered = true;
                    attempt = maxRetries; // Sets attempt to max so it executes exactly once more
                    continue; 
                }
            }
            
            // If it's a 500 internal server error on primary, just jump straight to fallback
            if (res.status >= 500 && !fallbackTriggered) {
                console.warn(`⚠️ Gemini primary ${currentModel} returned ${res.status}. Instantly falling back to alternative model.`);
                await logGeminiError(env, currentModel, activityName, new Error(`HTTP ${res.status} on primary`));
                fallbackTriggered = true;
                attempt = maxRetries;
                continue;
            }

            // Return whatever we have (success or non-retryable error)
            return res;

        } catch (error: any) {
            // AbortError -> Timeout
            if (error?.name === 'AbortError') {
                if (!fallbackTriggered) {
                    console.warn(`⚠️ Gemini primary ${currentModel} timed out after ${timeoutMs}ms. Falling back.`);
                    await logGeminiError(env, currentModel, activityName, new Error(`Timeout after ${timeoutMs}ms`));
                    fallbackTriggered = true;
                    attempt = maxRetries;
                    continue;
                }
            }
            
            // Fetch level panic, if not fallback yet, fallback
            if (!fallbackTriggered) {
                console.warn(`⚠️ Gemini primary ${currentModel} fetch failed:`, error);
                await logGeminiError(env, currentModel, activityName, error);
                fallbackTriggered = true;
                attempt = maxRetries;
                continue;
            }

            // If we've already done fallback, bubble up the error
            throw error;
            
        } finally {
            clearTimeout(timer);
        }
    }

    throw new Error('Gemini fetch exhausted all fallback options');
}
