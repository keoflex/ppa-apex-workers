import type { Env } from '../index';
import type { EnrichedLead } from './enrich-lead';
import { fetchGemini } from '../utils/gemini-fetch';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

/**
 * Generates a cold-outreach draft for a MOBILE strike.
 *
 * CRITICAL: this is fully isolated from the Polsinelli web engine (`generate-draft.ts`), which
 * pulls the sender identity from `system_settings` (Polsinelli Public Affairs / Fred Polsinelli).
 * A mobile strike must be tailored ENTIRELY to the mobile user's own business — the sender is the
 * mobile user, and the email must never reference Polsinelli or any third party.
 */
export interface MobileDraftInput {
    lead: EnrichedLead;          // the TARGET company + executive
    senderName: string;          // the mobile user (their name, or company if no name)
    senderCompany: string;       // the mobile user's company
    senderNarrative: string;     // what the mobile user's business does / offers
    desiredOutcome: string;      // e.g. "Wholesale Placement"
    targetCompany: string;
    triggerContext?: string;     // why this target surfaced (signal / reason)
}

export interface MobileDraft {
    subject: string;
    body: string;
}

/** Strips any residual third-party / Polsinelli references as a defense-in-depth safety net. */
export function sanitizeMobileText(text: string | null | undefined): string {
    if (!text) return '';
    let out = text;
    // Remove explicit Polsinelli / web-engine persona references that must never reach a mobile client.
    const banned = [
        /Polsinelli Public Affairs(,?\s*LLC)?/gi,
        /Polsinelli/gi,
        /Posinelli/gi,
        /Public Affairs Group/gi,
        /Fred Polsinelli/gi,
        /Rob O['’]?Neill/gi,
    ];
    for (const re of banned) out = out.replace(re, '');
    // Collapse artifacts left behind (double spaces / stray separators).
    out = out.replace(/[ \t]{2,}/g, ' ').replace(/\n{3,}/g, '\n\n').replace(/^[\s\-–—|]+|[\s\-–—|]+$/g, '').trim();
    return out;
}

export async function generateMobileDraft(env: Env, input: MobileDraftInput): Promise<MobileDraft> {
    const execName = input.lead.executiveName && input.lead.executiveName !== 'Unknown'
        ? input.lead.executiveName
        : '';
    const execTitle = input.lead.executiveTitle && input.lead.executiveTitle !== 'Unknown'
        ? input.lead.executiveTitle
        : 'a senior leader';

    const personName = (input.senderName || '').trim();
    const senderCompany = (input.senderCompany || '').trim();
    // A real personal name exists only if it's non-empty AND distinct from the company name —
    // otherwise the model must NOT pretend the company is a person ("My name is KeoCompany").
    const hasPerson = personName.length > 0 && personName.toLowerCase() !== senderCompany.toLowerCase();
    const senderLabel = hasPerson ? personName : (senderCompany || 'the sender');
    const signOff = hasPerson
        ? `${personName}${senderCompany ? `\n${senderCompany}` : ''}`
        : (senderCompany || personName);

    const identityLine = hasPerson
        ? `You are ${personName}, reaching out on behalf of ${senderCompany || 'your own business'}.`
        : `You are writing on behalf of ${senderCompany || 'the sender business'}. You do NOT have a personal name to use — introduce yourself as being FROM ${senderCompany || 'the company'} (e.g. "I'm reaching out from ${senderCompany || 'our company'}…"), and NEVER write "My name is ${senderCompany || 'the company'}" or treat the company name as a person.`;

    const systemPrompt = `${identityLine} You are writing a brief, warm, highly personal cold outreach email.

ABOUT THE SENDER (write in the first person as this business):
${input.senderNarrative || 'A business reaching out to explore a mutually beneficial opportunity.'}

GOAL OF THE EMAIL: ${input.desiredOutcome || 'open a conversation about working together'}.

ABSOLUTE RULES:
1. Write in the first person. ${hasPerson ? `The sender is ${personName} of ${senderCompany || 'their company'}.` : `The sender is the business ${senderCompany || ''}; there is NO individual person's name — never invent one and never use the company name as a person's name.`}
2. ONLY two organizations may appear: the sender (${senderCompany || senderLabel}) and the recipient's company (${input.targetCompany}).
3. NEVER mention or sign off as any other company, firm, or person — in particular NEVER "Polsinelli", "Public Affairs", or any third-party advisor. There is no agency involved; the sender is reaching out directly.
4. Tone: confident, human, specific, brief (under ~140 words). Sound like a real founder/operator, not an automated system.
5. End with EXACTLY this sign-off (and nothing after it):
${signOff}

Return ONLY a JSON object: {"subject":"...","body":"<full email including greeting and sign-off>"}.`;

    const userPrompt = `RECIPIENT:
- Name: ${execName || 'Not identified — open with a warm greeting without a name'}
- Title: ${execTitle}
- Company: ${input.targetCompany}
- Why they're a fit / recent signal: ${input.triggerContext || 'strong alignment with what we offer'}

Write the outreach email now, tailored to ${senderCompany || senderLabel}'s offering and this recipient.`;

    try {
        const response = await fetchGemini(env, 'lite', {
            activityName: 'generate-mobile-draft',
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: { parts: [{ text: systemPrompt }] },
                contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
                generationConfig: {
                    temperature: 0.7,
                    maxOutputTokens: 1024,
                    responseMimeType: 'application/json',
                    responseSchema: {
                        type: 'OBJECT',
                        properties: { subject: { type: 'STRING' }, body: { type: 'STRING' } },
                        required: ['subject', 'body'],
                    },
                },
            }),
        });

        if (!response.ok) throw new Error(`Gemini API error ${response.status}`);

        const { text: rawText } = await safeGeminiResponseParse(response);
        const parsed = safeJsonParse<{ subject?: string; body?: string } | null>(rawText, null);
        if (!parsed?.subject || !parsed?.body) throw new Error('Mobile draft parse failed / missing fields');

        // Belt-and-suspenders: scrub any third-party reference the model may have slipped in.
        return {
            subject: sanitizeMobileText(parsed.subject) || 'Exploring a partnership',
            body: sanitizeMobileText(parsed.body),
        };
    } catch (err) {
        console.warn(`⚠️ Mobile draft generation failed for ${input.targetCompany}:`, err);
        // Clean, sender-tailored fallback — never uses the company name as a person.
        const greeting = execName ? `Hi ${execName.split(' ')[0]},` : 'Hi there,';
        const intro = hasPerson
            ? `I'm ${personName}${senderCompany ? ` from ${senderCompany}` : ''}.`
            : `I'm reaching out from ${senderCompany || 'our company'}.`;
        const body = `${greeting}\n\n${intro} ${input.senderNarrative ? input.senderNarrative + ' ' : ''}I came across ${input.targetCompany} and saw a strong fit for ${input.desiredOutcome || 'working together'}. Would you be open to a short conversation this week?\n\nBest,\n${signOff}`;
        return { subject: `${input.targetCompany} × ${senderCompany || senderLabel}`, body };
    }
}
