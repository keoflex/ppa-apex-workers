/**
 * Utility: Safe Gemini Response Parser
 * Prevents "Unexpected end of JSON input" crashes when Gemini returns
 * empty bodies, truncated responses, or safety-blocked content.
 *
 * Usage:
 *   const { text, finishReason } = await safeGeminiResponseParse(geminiRes);
 *   if (!text) { // handle gracefully }
 */

/**
 * Parsed result from a Gemini API response.
 */
export interface GeminiParseResult {
    /** The extracted text from the first candidate's parts, or null if absent. */
    text: string | null;
    /** Gemini's finishReason (e.g., STOP, MAX_TOKENS, SAFETY, OTHER). */
    finishReason: string | null;
    /** True if the response body was empty or unparseable JSON. */
    wasEmpty: boolean;
    /** The raw parsed JSON object (for debugging). */
    raw: any;
}

/**
 * Safely parses a Gemini API Response object.
 * - Reads body as text first (never throws on empty body)
 * - Gracefully handles empty responses, truncated JSON, and blocked content
 * - Extracts the first candidate's text content
 */
export async function safeGeminiResponseParse(response: Response): Promise<GeminiParseResult> {
    const bodyText = await response.text();

    if (!bodyText || bodyText.trim().length === 0) {
        return { text: null, finishReason: null, wasEmpty: true, raw: null };
    }

    let parsed: any;
    try {
        parsed = JSON.parse(bodyText);
    } catch (e) {
        // The HTTP response body itself is malformed JSON
        console.warn('⚠️ Gemini response body was not valid JSON:', (e as Error).message);
        return { text: null, finishReason: null, wasEmpty: false, raw: bodyText };
    }

    const candidate = parsed?.candidates?.[0];
    const finishReason = candidate?.finishReason || null;
    // Join ALL text parts — Gemini can split long output across multiple parts,
    // and taking only the first part silently truncates the response.
    const parts = candidate?.content?.parts;
    const text = Array.isArray(parts)
        ? (parts.filter((p: any) => typeof p.text === 'string').map((p: any) => p.text).join('') || null)
        : null;

    if (finishReason && finishReason !== 'STOP') {
        console.warn(`⚠️ Gemini finishReason: ${finishReason} (response may be truncated)`);
    }

    return { text, finishReason, wasEmpty: false, raw: parsed };
}
