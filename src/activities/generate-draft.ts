/**
 * Activity: Generate Strike Draft
 * Drafts a personalized trust-proxy cold email using Gemini 2.0 Flash via REST.
 * Uses native fetch — no Node.js built-ins, compatible with Cloudflare Workers.
 */
import type { Env } from '../index';
import type { EnrichedLead } from './enrich-lead';

export interface DraftInput {
    lead: EnrichedLead;
    persona: string;
    triggerHeadline: string;
}

export interface StrikeDraft {
    personaUsed: string;
    subject: string;
    body: string;
    modelUsed: string;
    confidenceScore: number;
}

// ---------------------------------------------------------------------------
// Persona config
// ---------------------------------------------------------------------------

const PERSONA_SIGNATURES: Record<string, { name: string; title: string; group: string; bio: string }> = {
    "Rob O'Neill": {
        name: "Rob O'Neill",
        title: 'Senior Strategic Advisor',
        group: 'PPA+ Institutional Group',
        bio: 'Former Navy SEAL, decorated veteran, nationally recognized speaker and strategist with deep relationships across institutional capital markets.',
    },
    'Bo Dietl': {
        name: 'Bo Dietl',
        title: 'Chairman, Strategic Advisory',
        group: 'PPA+ Global Intelligence',
        bio: 'Decorated former NYPD detective turned corporate intelligence executive, with a network spanning law enforcement, finance, and global corporate security.',
    },
    'Todd Zeile': {
        name: 'Todd Zeile',
        title: 'Managing Director',
        group: 'PPA+ Capital Markets',
        bio: 'Former MLB All-Star with post-retirement career in financial services, known for building high-trust relationships in alternative investments.',
    },
};

const GEMINI_REST_URL =
    'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent';

// ---------------------------------------------------------------------------
// Fallback template — used when Gemini call fails
// ---------------------------------------------------------------------------

function buildFallbackDraft(input: DraftInput, signature: string): StrikeDraft {
    const body = [
        `${input.lead.executiveName},`,
        '',
        `Congratulations on the recent development — ${input.triggerHeadline.toLowerCase()}. The strategic implications are significant.`,
        '',
        `I've been advising a number of institutional players navigating similar terrain, and there's a particular opportunity that could provide meaningful alpha for your organization.`,
        '',
        `Would 15 minutes this week make sense? I can walk through the specifics.`,
        '',
        `Best regards,`,
        signature,
    ].join('\n');

    return {
        personaUsed: input.persona,
        subject: `${input.triggerHeadline.split(' ').slice(0, 4).join(' ')} — Strategic Access`,
        body,
        modelUsed: 'fallback-template',
        confidenceScore: 0.55,
    };
}

// ---------------------------------------------------------------------------
// Main activity
// ---------------------------------------------------------------------------

export async function generateDraft(env: Env, input: DraftInput): Promise<StrikeDraft> {
    console.log(`✍️ Generating strike draft as ${input.persona} via Gemini 2.0 Flash...`);

    const personaConfig =
        PERSONA_SIGNATURES[input.persona] ?? PERSONA_SIGNATURES["Rob O'Neill"];
    const signatureBlock = `${personaConfig.name}\n${personaConfig.title}\n${personaConfig.group}`;

    // Build the system prompt
    const systemPrompt = `You are ${personaConfig.name}, ${personaConfig.title} at ${personaConfig.group}.

BACKGROUND: ${personaConfig.bio}

YOUR VOICE:
- Authoritative, direct, and warm. Never salesy.
- Reference real context (the trigger event, the company, the executive's role).
- Maximum 4 short paragraphs. No bullet points. No HTML.
- Close with a soft one-question CTA asking for 15 minutes.
- Always end with your literal signature block:
${signatureBlock}

OUTPUT FORMAT:
Respond with a JSON object ONLY (no markdown, no extra text):
{
  "subject": "<compelling 8–12 word subject line referencing the trigger>",
  "body": "<full email body including salutation and signature>"
}`;

    const userPrompt = `TRIGGER EVENT: ${input.triggerHeadline}

PROSPECT PROFILE:
- Name: ${input.lead.executiveName}
- Title: ${input.lead.executiveTitle}
- Company: ${input.lead.company}
- Company Revenue / AUM: ${input.lead.companyRevenue}
- Team Size: ${input.lead.employeeCount} employees
- Recent Signals: ${input.lead.signals.join('; ')}

Write the personalized cold outreach email now.`;

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
                    temperature: 0.7,
                    maxOutputTokens: 1024,
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

        // Parse the JSON payload from the model
        const parsed = JSON.parse(rawText) as { subject?: string; body?: string };

        if (!parsed.subject || !parsed.body) {
            throw new Error('Gemini response missing subject or body fields');
        }

        const draft: StrikeDraft = {
            personaUsed: input.persona,
            subject: parsed.subject,
            body: parsed.body,
            modelUsed: 'gemini-2.0-flash',
            // Gemini doesn't return logprobs; use a fixed high-confidence value
            confidenceScore: 0.91,
        };

        console.log(`✅ Draft generated via Gemini (subject: "${draft.subject}")`);
        return draft;
    } catch (err) {
        console.error('❌ Gemini draft generation failed, using fallback template:', err);
        return buildFallbackDraft(input, signatureBlock);
    }
}
