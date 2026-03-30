/**
 * Central Gemini Configuration
 * Change the model version here and it propagates everywhere.
 */

export const GEMINI_PRO_MODEL = 'gemini-2.5-flash';
export const GEMINI_PRO_FALLBACK = 'gemini-2.5-pro';

export const GEMINI_LITE_MODEL = 'gemini-2.5-flash-lite';
export const GEMINI_LITE_FALLBACK = 'gemini-2.5-flash';

const GEMINI_BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/models';

/**
 * Build the full Gemini API URL for the specified tier with your API key appended.
 */
export function geminiUrl(apiKey: string, tier: 'pro' | 'lite' = 'pro'): string {
    const model = tier === 'lite' ? GEMINI_LITE_MODEL : GEMINI_PRO_MODEL;
    return `${GEMINI_BASE_URL}/${model}:generateContent?key=${apiKey}`;
}

export function geminiFallbackUrl(apiKey: string, tier: 'pro' | 'lite' = 'pro'): string {
    const model = tier === 'lite' ? GEMINI_LITE_FALLBACK : GEMINI_PRO_FALLBACK;
    return `${GEMINI_BASE_URL}/${model}:generateContent?key=${apiKey}`;
}
