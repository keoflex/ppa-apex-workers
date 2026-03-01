/**
 * Central Gemini Configuration
 * Change the model version here and it propagates everywhere.
 */

export const GEMINI_MODEL = 'gemini-3-flash-preview';

export const GEMINI_BASE_URL = 'https://generativelanguage.googleapis.com/v1beta/models';

export const GEMINI_REST_URL = `${GEMINI_BASE_URL}/${GEMINI_MODEL}:generateContent`;

/**
 * Build the full Gemini API URL with your API key appended.
 */
export function geminiUrl(apiKey: string): string {
    return `${GEMINI_REST_URL}?key=${apiKey}`;
}
