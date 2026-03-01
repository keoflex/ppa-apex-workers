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

import { GEMINI_REST_URL } from '../config/gemini';

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
        `Congratulations on the news — ${input.triggerHeadline}. This kind of transition opens a rare window where the right partnerships and advisory relationships can make a material difference in setting the trajectory for the organization.`,
        '',
        `We've been working closely with leaders navigating similar transitions, specifically by leveraging our training initiatives and bridging partnership opportunities that align seamlessly with new leadership mandates.`,
        '',
        `Would it make sense to connect briefly this week? I'd love to explore how we can bridge some strategic connections that might benefit your team from day one.`,
        '',
        `Best,`,
        signature,
    ].join('\n');

    return {
        personaUsed: input.persona,
        subject: `${input.lead.company} — a thought on your new role`,
        body,
        modelUsed: 'fallback-template',
        confidenceScore: 0.55,
    };
}

// ---------------------------------------------------------------------------
// Main activity
// ---------------------------------------------------------------------------

export async function generateDraft(env: Env, input: DraftInput): Promise<StrikeDraft> {
    // 1. Fetch Global System Settings (with fallback defaults)
    let settings: any = null;
    try {
        const result = await getRow(env, 'system_settings', 1);
        if (result.ok && result.data) {
            settings = result.data;
        }
    } catch (settingsErr) {
        console.warn('⚠️ System settings fetch failed, using defaults:', settingsErr);
    }

    // Use settings or sensible defaults
    const company_name = settings?.company_name || 'Polsinelli Public Affairs, LLC';
    const company_description = settings?.company_description || '';
    const default_sender_name = settings?.default_sender_name || 'Fred Polsinelli';
    const default_sender_title = settings?.default_sender_title || 'CEO';
    const default_sender_group = settings?.default_sender_group || 'Consulting Agency';

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
You are operating as a Strategic Syndicate Director. This outreach is part of a multi-partner campaign. Evaluate the strengths of each partner below against the target's trigger event and weave the MOST RELEVANT partner capability into the email naturally to bridge connections. You do NOT need to mention every partner — focus on the 1-2 most strategic alignments that could genuinely benefit their organization.

${input.partnerProfiles.map((p: any, i: number) => `Partner ${i + 1}: ${p.name}
Positioning: ${p.partner_positioning || p.ai_summary || p.value_proposition || 'Strategic partner'}
${p.ideal_customer_profile ? `Ideal Customer Profile: ${p.ideal_customer_profile}` : ''}
${p.ai_alignment_rules ? `Strict Alignment Rules: ${p.ai_alignment_rules}` : ''}
${p.domain ? `Domain: ${p.domain}` : ''}`).join('\n\n')}

Frame ${company_name} as the strategic advisor orchestrating this syndicate of elite capabilities for the prospect.` : ''}

WRITING STYLE RULES — THIS IS CRITICAL:
1. Write like a thoughtful, experienced professional sending a brief, warm, highly personal note. Speak DIRECTLY to the recipient in the first person (i.e. "I saw your recent appointment" instead of "I saw Michelle was appointed"). Do NOT refer to the recipient in the third person if the article is about them.
2. If the trigger event is about their new role or appointment, congratulate them personally on the position as your opening hook. Make it engaging, human, and genuine.
3. Open with a SPECIFIC, intelligent observation about the deal/event. Reference actual details from the article — dollar amounts, counterparties, market context, what makes this deal interesting. Show you actually read and understood the news.
4. In the second paragraph, draw a genuine connection showing how our training initiatives, employee benefits, or strategic partnership capabilities can support their new mandate and benefit their organization.
5. If they were recently appointed, review the PARTNER SYNDICATE ALIGNMENT (if provided) to actively bridge connections and suggest specific partnership opportunities that align with their agenda.
6. Keep the closing casual and low-pressure. Something like "Happy to share a few thoughts if useful" or "Worth a quick call?" — NOT "Would 15 minutes this week make sense?"
7. Tone: Confident but human. Knowledgeable but not lecturing. Warm but brief. You should sound like someone the reader would actually want to get coffee with. NEVER sound like an automated system.
8. Maximum 3-4 SHORT paragraphs. No bullet points. No HTML. No bold text. No emojis.
9. ${hasRealName ? `Address them by first name: "${input.lead.executiveName.split(' ')[0]},"` : 'DO NOT say "Unknown." Start with a warm professional opening like "Good morning," or simply begin with your observation directly.'}
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
                    maxOutputTokens: 4096,
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

        // Gemini 3 returns thought parts (reasoning) alongside the actual text.
        // Filter out thought parts and find the real JSON output.
        const parts = geminiData?.candidates?.[0]?.content?.parts || [];

        // First: try non-thought parts (the actual model output)
        const nonThoughtParts = parts.filter((p: any) => p.text && !p.thought);
        let rawText = nonThoughtParts.length > 0
            ? nonThoughtParts[nonThoughtParts.length - 1].text  // Take LAST non-thought part
            : null;

        // Fallback: if no non-thought parts, try any part with text
        if (!rawText) {
            rawText = parts.filter((p: any) => p.text).pop()?.text;
        }

        if (!rawText) {
            throw new Error('Gemini returned an empty response');
        }

        // Clean the text: strip markdown code fences if Gemini wraps JSON in ```json...```
        let cleanedText = rawText.trim();
        if (cleanedText.startsWith('```')) {
            cleanedText = cleanedText.replace(/^```(?:json)?\s*\n?/, '').replace(/\n?```\s*$/, '');
        }

        // Parse the JSON payload from the model
        // Gemini sometimes outputs JSON with literal newlines inside strings, which is invalid.
        // Fix: escape unescaped newlines within JSON string values before parsing.
        let parsed: { subject?: string; body?: string };
        try {
            parsed = JSON.parse(cleanedText);
        } catch {
            // Attempt to fix common JSON issues: literal newlines in string values
            const fixedText = cleanedText
                .replace(/\r\n/g, '\\n')
                .replace(/\n/g, '\\n')
                .replace(/\t/g, '\\t');
            try {
                parsed = JSON.parse(fixedText);
            } catch {
                // Last resort: regex extract subject and body
                const subjectMatch = cleanedText.match(/"subject"\s*:\s*"([^"]+)"/);
                const bodyMatch = cleanedText.match(/"body"\s*:\s*"([\s\S]+?)"\s*[,}]/);
                if (subjectMatch && bodyMatch) {
                    parsed = {
                        subject: subjectMatch[1],
                        body: bodyMatch[1].replace(/\\n/g, '\n').replace(/\\"/g, '"'),
                    };
                } else {
                    throw new Error(`Failed to parse Gemini JSON even with fallbacks. Raw (first 300 chars): ${cleanedText.slice(0, 300)}`);
                }
            }
        }

        if (!parsed.subject || !parsed.body) {
            throw new Error(`Gemini response missing subject or body fields. Got: ${cleanedText.slice(0, 200)}`);
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
        // Store last error for diagnostics
        (generateDraft as any).__lastError = String(err);
        const fallback = buildFallbackDraft(input, signatureBlock);
        (fallback as any).__fallbackReason = String(err);
        return fallback;
    }
}
