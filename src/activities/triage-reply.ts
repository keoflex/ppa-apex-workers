/**
 * Activity: Triage Inbound Reply
 * Classifies an inbound email reply using Gemini 2.0 Flash (JSON mode) via REST.
 * Uses native fetch — no Node.js built-ins, compatible with Cloudflare Workers.
 * Falls back to keyword matching if the Gemini call fails.
 */
import type { Env } from '../index';

export interface TriageInput {
    senderName: string;
    senderCompany: string;
    subject: string;
    body: string;
}

export type TriageCategory = 'direct_strike' | 'info_seekers' | 'referral_pivot' | 'not_now';

export interface TriageResult {
    category: TriageCategory;
    confidence: number;
    reasoning: string;
    modelUsed: string;
}

// ---------------------------------------------------------------------------
// Fallback: keyword-matching triage (original logic, preserved for resilience)
// ---------------------------------------------------------------------------

const KEYWORDS: Record<TriageCategory, string[]> = {
    direct_strike: ['schedule', 'call', 'meeting', 'interested', "let's talk", 'available', 'set up'],
    info_seekers: ['more details', 'send over', 'report', 'fee structure', 'requirements', 'deck'],
    referral_pivot: ['not my area', 'talk to', 'introduce', 'connect you', 'better suited', 'forward'],
    not_now: ['quiet period', 'not relevant', 'remove me', 'fully allocated', 'not interested', 'unsubscribe'],
};

function keywordTriage(input: TriageInput): TriageResult {
    const bodyLower = input.body.toLowerCase();
    let bestCategory: TriageCategory = 'not_now';
    let maxMatches = 0;

    for (const [category, keywords] of Object.entries(KEYWORDS) as [TriageCategory, string[]][]) {
        const matches = keywords.filter((kw) => bodyLower.includes(kw)).length;
        if (matches > maxMatches) {
            maxMatches = matches;
            bestCategory = category;
        }
    }

    return {
        category: bestCategory,
        confidence: 0.62,
        reasoning: `Keyword analysis detected ${bestCategory} intent with ${maxMatches} signal match(es).`,
        modelUsed: 'keyword-fallback',
    };
}

// ---------------------------------------------------------------------------
// Gemini REST endpoint (JSON mode)
// ---------------------------------------------------------------------------

const GEMINI_REST_URL =
    'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent';

const VALID_CATEGORIES: TriageCategory[] = [
    'direct_strike',
    'info_seekers',
    'referral_pivot',
    'not_now',
];

function isValidCategory(value: unknown): value is TriageCategory {
    return typeof value === 'string' && (VALID_CATEGORIES as string[]).includes(value);
}

// ---------------------------------------------------------------------------
// Main activity
// ---------------------------------------------------------------------------

export async function triageReply(env: Env, input: TriageInput): Promise<TriageResult> {
    console.log(`🏷️ Triaging reply from ${input.senderName} @ ${input.senderCompany} via Gemini...`);

    const systemPrompt = `You are an expert sales reply analyst for PPA+, an elite institutional outreach firm.

Your task is to classify inbound email replies into EXACTLY one of these four categories:

- "direct_strike"   — The sender is interested and wants to schedule a call, meeting, or next step. High intent.
- "info_seekers"    — The sender wants more information, a deck, fee structure, report, or due-diligence material before committing.
- "referral_pivot"  — The sender is redirecting to another contact, suggesting it's not their area, or offering to connect you elsewhere.
- "not_now"         — The sender is not interested now: quiet period, fully allocated, asking to be removed, or clearly declining.

OUTPUT FORMAT — return a JSON object ONLY (no markdown, no extra text):
{
  "triage_category": "<one of the four values above>",
  "confidence": <float between 0.0 and 1.0>,
  "ai_reasoning": "<1-2 sentence explanation of your classification decision>"
}`;

    const userPrompt = `INBOUND REPLY:
FROM: ${input.senderName} (${input.senderCompany})
SUBJECT: ${input.subject}
BODY:
${input.body}

Classify this reply now.`;

    try {
        const response = await fetch(`${GEMINI_REST_URL}?key=${env.GEMINI_API_KEY}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: {
                    parts: [{ text: systemPrompt }],
                },
                contents: [
                    {
                        role: 'user',
                        parts: [{ text: userPrompt }],
                    },
                ],
                generationConfig: {
                    temperature: 0.1,          // Low temperature for consistent classification
                    maxOutputTokens: 256,
                    responseMimeType: 'application/json',
                },
            }),
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Gemini API error ${response.status}: ${errText}`);
        }

        const geminiData = await response.json() as {
            candidates?: Array<{
                content?: { parts?: Array<{ text?: string }> };
                finishReason?: string;
            }>;
        };

        const rawText = geminiData?.candidates?.[0]?.content?.parts?.[0]?.text;
        if (!rawText) {
            throw new Error('Gemini returned an empty response');
        }

        const parsed = JSON.parse(rawText) as {
            triage_category?: unknown;
            confidence?: unknown;
            ai_reasoning?: unknown;
        };

        // Validate the category is strictly one of our enum values
        if (!isValidCategory(parsed.triage_category)) {
            throw new Error(`Gemini returned unknown category: "${parsed.triage_category}"`);
        }

        const confidence = typeof parsed.confidence === 'number'
            ? Math.min(1, Math.max(0, parsed.confidence))
            : 0.75;

        const reasoning = typeof parsed.ai_reasoning === 'string'
            ? parsed.ai_reasoning
            : 'Classified by Gemini.';

        const result: TriageResult = {
            category: parsed.triage_category,
            confidence,
            reasoning,
            modelUsed: 'gemini-2.0-flash',
        };

        console.log(`✅ Triaged as: ${result.category} (${result.confidence.toFixed(2)} confidence) — Gemini`);
        return result;
    } catch (err) {
        console.error('❌ Gemini triage failed, falling back to keyword matching:', err);
        return keywordTriage(input);
    }
}
