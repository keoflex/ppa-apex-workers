import type { Env } from '../index';
import { fetchGemini } from '../utils/gemini-fetch';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

/**
 * Per-company research synthesis for a mobile strike. Turns the raw enriched lead + the user's
 * request into the things that make this a RESEARCH tool (not just a lead list):
 *   - a real relevance score (0–100) + one-line reason tied to the user's specific request
 *   - "Research Findings" that directly answer what the user asked to learn (informationToGather)
 *   - additional decision-makers (the buying committee) beyond the single enriched contact
 *
 * Grounded in the data we actually have; instructed NOT to invent facts (says so when unknown).
 */
export interface MobileResearchInput {
    company: string;
    industry?: string;
    trigger?: string;
    enrichmentContext?: string;     // signals / executive research / revenue / employees, joined
    primaryContactName?: string;    // already-enriched contact, so synthesis suggests OTHERS
    primaryContactTitle?: string;
    senderCompany: string;
    senderNarrative: string;
    targetAudience: string;
    market: string;
    desiredOutcome: string;
    informationToGather: string;
    idealCustomerProfile?: string;
    exclusions?: string;
}

export interface MobileResearch {
    relevanceScore: number;          // 0–100
    relevanceReason: string;
    researchFindings: string[];      // answers the user's info request
    additionalContacts: { name: string; title: string }[];
}

const FALLBACK: MobileResearch = { relevanceScore: 80, relevanceReason: '', researchFindings: [], additionalContacts: [] };

export async function synthesizeMobileResearch(env: Env, input: MobileResearchInput): Promise<MobileResearch> {
    const system = `You are a B2B research analyst preparing a briefing for ${input.senderCompany}.
ABOUT ${input.senderCompany}: ${input.senderNarrative || 'a business seeking aligned opportunities'}.
THE USER'S REQUEST: find "${input.targetAudience}" in "${input.market}" for the goal of "${input.desiredOutcome}".
${input.idealCustomerProfile ? `IDEAL CUSTOMER PROFILE: ${input.idealCustomerProfile}.` : ''}
${input.exclusions ? `EXCLUDE / AVOID (score these very low): ${input.exclusions}.` : ''}

Assess the target company below and return ONLY this JSON:
{
  "relevanceScore": <integer 0-100, how well THIS company matches the user's request + ICP; near 0 if it matches an exclusion or is off-target>,
  "relevanceReason": "<one concise sentence: why this company fits (or doesn't) the user's specific request>",
  "researchFindings": ["<3-5 short bullets that DIRECTLY answer what the user asked to learn>"],
  "additionalContacts": [{"name":"<real name or 'Unknown'>","title":"<role>"}]
}

RULES:
- researchFindings must answer the user's information request: "${input.informationToGather || 'general fit and recent business signals'}". Use only the data provided below or well-known public facts. If something isn't known, say "Not available in current data" rather than inventing it.
- additionalContacts: up to 3 LIKELY decision-makers for this goal OTHER than ${input.primaryContactName || 'the primary contact'}. Give a real name only if you are confident; otherwise set name to "Unknown" and still provide the role/title. Never fabricate emails.
- Be specific and factual. No marketing fluff.`;

    const user = `TARGET COMPANY: ${input.company}
Industry: ${input.industry || 'Unknown'}
Recent signal / trigger: ${input.trigger || 'None provided'}
Known data: ${input.enrichmentContext || 'Limited'}
Primary contact already identified: ${input.primaryContactName || 'Unknown'}${input.primaryContactTitle ? ` (${input.primaryContactTitle})` : ''}`;

    try {
        const res = await fetchGemini(env, 'lite', {
            activityName: 'synthesize-mobile-research',
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: { parts: [{ text: system }] },
                contents: [{ role: 'user', parts: [{ text: user }] }],
                generationConfig: {
                    temperature: 0.35,
                    maxOutputTokens: 1500,
                    responseMimeType: 'application/json',
                    responseSchema: {
                        type: 'OBJECT',
                        properties: {
                            relevanceScore: { type: 'INTEGER' },
                            relevanceReason: { type: 'STRING' },
                            researchFindings: { type: 'ARRAY', items: { type: 'STRING' } },
                            additionalContacts: {
                                type: 'ARRAY',
                                items: { type: 'OBJECT', properties: { name: { type: 'STRING' }, title: { type: 'STRING' } }, required: ['title'] },
                            },
                        },
                        required: ['relevanceScore', 'relevanceReason', 'researchFindings'],
                    },
                },
            }),
        });
        if (!res.ok) return FALLBACK;
        const { text } = await safeGeminiResponseParse(res);
        const parsed = safeJsonParse<Partial<MobileResearch>>(text || '', {});
        const score = Math.max(0, Math.min(100, Math.round(Number(parsed?.relevanceScore ?? 80))));
        return {
            relevanceScore: Number.isFinite(score) ? score : 80,
            relevanceReason: typeof parsed?.relevanceReason === 'string' ? parsed.relevanceReason : '',
            researchFindings: Array.isArray(parsed?.researchFindings) ? parsed!.researchFindings!.filter(x => typeof x === 'string').slice(0, 6) : [],
            additionalContacts: Array.isArray(parsed?.additionalContacts)
                ? parsed!.additionalContacts!.filter((c: any) => c && c.title).map((c: any) => ({ name: String(c.name || 'Unknown'), title: String(c.title) })).slice(0, 3)
                : [],
        };
    } catch {
        return FALLBACK;
    }
}
