/**
 * Activity: Generate Strike Draft
 * Drafts a personalized trust-proxy cold email using Gemini 3 Flash via REST.
 * Uses native fetch — no Node.js built-ins, compatible with Cloudflare Workers.
 */
import type { Env } from '../index';
import type { EnrichedLead } from './enrich-lead';

export interface DraftInput {
    lead: EnrichedLead;
    persona: string;
    triggerHeadline: string;
    triggerArticleText?: string;
    partnerProfiles?: any[];  // M:N — array of partner companies
}

export interface StrikeDraft {
    personaUsed: string;
    subject: string;
    body: string;
    modelUsed: string;
    confidenceScore: number;
}

// ---------------------------------------------------------------------------
import { getRow } from '../utils/supabase';

// ---------------------------------------------------------------------------

const GEMINI_REST_URL =
    'https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent';

// ---------------------------------------------------------------------------
// Fallback template — used when Gemini call fails
// ---------------------------------------------------------------------------

function buildFallbackDraft(input: DraftInput, signature: string): StrikeDraft {
    const name = (input.lead.executiveName && input.lead.executiveName !== 'Unknown')
        ? input.lead.executiveName.split(' ')[0]
        : '';
    const greeting = name ? `${name},` : 'Good morning,';

    const body = [
        greeting,
        '',
        `I noticed ${input.lead.company}'s recent move — ${input.triggerHeadline}. This kind of transaction typically opens a window where the right advisory relationships make a material difference in how the integration plays out.`,
        '',
        `We've been working with firms navigating similar situations, particularly around the strategic and regulatory dimensions that tend to emerge in the months following a deal like this.`,
        '',
        `Would it make sense to connect briefly this week? I'd be happy to share some perspective on what we're seeing in the market.`,
        '',
        `Best,`,
        signature,
    ].join('\n');

    return {
        personaUsed: input.persona,
        subject: `${input.lead.company} — a thought on what comes next`,
        body,
        modelUsed: 'fallback-template',
        confidenceScore: 0.55,
    };
}

// ---------------------------------------------------------------------------
// Main activity
// ---------------------------------------------------------------------------

export async function generateDraft(env: Env, input: DraftInput): Promise<StrikeDraft> {
    // 1. Fetch Global System Settings
    const { data: settings, ok, error } = await getRow(env, 'system_settings', 1);

    if (!ok || !settings) {
        console.error('❌ Failed to load system settings for draft generation', error);
        throw new Error('System settings required for generation but failed to load.');
    }

    const {
        company_name,
        company_description,
        default_sender_name,
        default_sender_title,
        default_sender_group
    } = settings;

    const signatureBlock = `${default_sender_name}\n${default_sender_title}\n${company_name} - ${default_sender_group}`;

    console.log(`✍️ Generating strike draft as ${default_sender_name} via Gemini 3 Flash...`);

    // Determine if we have a real contact name
    const hasRealName = input.lead.executiveName
        && input.lead.executiveName !== 'Unknown'
        && !input.lead.executiveName.toLowerCase().includes('decision-maker');

    // Build the system prompt
    const systemPrompt = `You are ${default_sender_name}, ${default_sender_title} at ${company_name}.

ABOUT YOUR FIRM:
${company_description || `${company_name} is a consulting and advisory firm specializing in strategic counsel for institutional clients navigating complex transactions, regulatory environments, and market transitions.`}
${input.partnerProfiles && input.partnerProfiles.length > 0 ? `
PARTNER SYNDICATE ALIGNMENT:
You are operating as a Strategic Syndicate Director. This outreach is part of a multi-partner campaign. Evaluate the strengths of each partner below against the target's trigger event and weave the MOST RELEVANT partner capability into the email naturally. You do NOT need to mention every partner — focus on the 1-2 most strategic alignments.

${input.partnerProfiles.map((p: any, i: number) => `Partner ${i + 1}: ${p.name}
Positioning: ${p.partner_positioning || p.ai_summary || p.value_proposition || 'Strategic partner'}
${p.domain ? `Domain: ${p.domain}` : ''}`).join('\n\n')}

Frame ${company_name} as the strategic advisor orchestrating this syndicate of elite capabilities for the prospect.` : ''}

WRITING STYLE RULES — THIS IS CRITICAL:
1. Write like a thoughtful, experienced professional sending a brief personal note. NOT like a marketing email. NOT like a cold sales pitch. Think: a senior partner reaching out after reading about a deal in the morning paper.
2. NEVER start with "Congratulations." NEVER use phrases like "strategic implications are significant" or "meaningful alpha" or "navigating similar terrain" — these scream AI-generated.
3. Open with a SPECIFIC, intelligent observation about the deal/event. Reference actual details from the article — dollar amounts, counterparties, market context, what makes this deal interesting. Show you actually read and understood the news.
4. In the second paragraph, draw a genuine connection to your firm's expertise. Be concrete about HOW you could help — not vague platitudes about "opportunities." For example: "We recently helped a mid-cap bank work through the regulatory sequencing after a similar acquisition" is much better than "We advise firms in similar situations."
5. Keep the closing casual and low-pressure. Something like "Happy to share a few thoughts if useful" or "Worth a quick call?" — NOT "Would 15 minutes this week make sense?"
6. Tone: Confident but human. Knowledgeable but not lecturing. Warm but brief. You should sound like someone the reader would actually want to get coffee with.
7. Maximum 3-4 SHORT paragraphs. No bullet points. No HTML. No bold text. No emojis.
8. ${hasRealName ? `Address them by first name: "${input.lead.executiveName.split(' ')[0]},"` : 'DO NOT say "Unknown." Start with a warm professional opening like "Good morning," or simply begin with your observation directly.'}
9. End with ONLY this signature block:
Best,
${signatureBlock}

OUTPUT FORMAT:
Respond with a JSON object ONLY (no markdown, no extra text):
{
  "subject": "<concise 6-10 word subject line that sounds human — reference the company or deal specifically>",
  "body": "<full email body including greeting and signature>"
}`;

    const userPrompt = `TRIGGER EVENT: ${input.triggerHeadline}

ARTICLE CONTEXT (use specific details from this to make the email feel researched and personal):
${input.triggerArticleText || 'No article text available — use the trigger headline and what you know about the deal type to craft an intelligent email.'}

PROSPECT:
- Name: ${hasRealName ? input.lead.executiveName : 'Not yet identified — use a warm greeting without a name'}
- Title: ${input.lead.executiveTitle !== 'Unknown' ? input.lead.executiveTitle : 'Senior leadership'}
- Company: ${input.lead.company}
- Est. Revenue / AUM: ${input.lead.companyRevenue || 'Not available'}
- Team Size: ${input.lead.employeeCount || 'Not available'}
- Market Signals: ${input.lead.signals?.length ? input.lead.signals.join('; ') : 'Recent M&A activity'}

${input.lead.executiveResearch ? `EXECUTIVE BACKGROUND:\n${input.lead.executiveResearch}` : ''}

Write the outreach email. Sound human. Reference specific deal details. Be brief.`;

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
                    temperature: 0.85,
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

        // Gemini 3 may return multiple parts (text + thoughtSignature). Find the text part.
        const parts = geminiData?.candidates?.[0]?.content?.parts || [];
        const rawText = parts.find((p: any) => p.text)?.text;
        if (!rawText) {
            throw new Error('Gemini returned an empty response');
        }

        // Parse the JSON payload from the model
        const parsed = JSON.parse(rawText) as { subject?: string; body?: string };

        if (!parsed.subject || !parsed.body) {
            throw new Error('Gemini response missing subject or body fields');
        }

        const draft: StrikeDraft = {
            personaUsed: default_sender_name,
            subject: parsed.subject,
            body: parsed.body,
            modelUsed: 'gemini-3-flash-preview',
            confidenceScore: 0.91,
        };

        console.log(`✅ Draft generated via Gemini (subject: "${draft.subject}")`);
        return draft;
    } catch (err) {
        console.error('❌ Gemini draft generation failed, using fallback template:', err);
        return buildFallbackDraft(input, signatureBlock);
    }
}
