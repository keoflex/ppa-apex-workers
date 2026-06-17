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
    steeringNotes?: string;   // Custom instructions from reviewer or campaign objective (e.g. "focus on Polsinelli service areas")
    stepContext?: {
        stepNumber: number;
        totalSteps: number;
        stepPrompt: string;
        previousEmails: string[];
    };
    goldDrafts?: any[];          // Pre-fetched gold draft exemplars
    partnerTemplates?: any[];    // Pre-fetched validated partner email templates
    messagingDirectives?: any[]; // Pre-fetched active daily messaging directives
}

export interface StrikeDraft {
    personaUsed: string;
    subject: string;
    subjectB?: string;
    body: string;
    modelUsed: string;
    confidenceScore: number;
}

// ---------------------------------------------------------------------------
import { getRow, fetchRows } from '../utils/supabase';

// ---------------------------------------------------------------------------

import { fetchGemini } from '../utils/gemini-fetch';
import { GEMINI_PRO_MODEL } from '../config/gemini';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// ---------------------------------------------------------------------------
// Clean First Name Extraction Helper
// ---------------------------------------------------------------------------

function cleanNameString(name: string | null | undefined): string {
    if (!name) return '';
    const parts = name.trim().split(/\s+/);
    const cleanedParts: string[] = [];
    const seenLetters = new Set<string>();
    let initialCount = 0;
    
    for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        const letterOnly = part.replace(/[^a-zA-Z]/g, '').toLowerCase();
        
        if (letterOnly.length === 1) {
            if (seenLetters.has(letterOnly) || initialCount >= 2) {
                continue;
            }
            seenLetters.add(letterOnly);
            initialCount++;
        } else if (letterOnly.length > 1) {
            if (cleanedParts.length > 0 && cleanedParts[cleanedParts.length - 1].replace(/[^a-zA-Z]/g, '').toLowerCase() === letterOnly) {
                continue;
            }
        }
        cleanedParts.push(part);
    }
    
    const finalName = cleanedParts.join(' ');
    if (finalName.length < 2) return '';
    return finalName;
}

function getCleanFirstName(name: string | null | undefined): string {
    if (!name || name === 'Unknown') return '';
    const parts = name.trim().split(/\s+/);
    const prefixes = ['dr.', 'dr', 'mr.', 'mr', 'ms.', 'ms', 'mrs.', 'mrs', 'prof.', 'prof'];
    
    let first = parts[0];
    if (prefixes.includes(first.toLowerCase()) && parts.length > 1) {
        first = parts[1];
    }
    
    // If the name is an initial (e.g. 'S.' or 'S') or very short, try to find a full name part
    if ((first.length <= 2 || (first.length === 2 && first.endsWith('.'))) && parts.length > 1) {
        for (const part of parts) {
            if (!prefixes.includes(part.toLowerCase()) && part.length > 2 && !part.includes('.')) {
                return part;
            }
        }
    }
    
    return first;
}

// ---------------------------------------------------------------------------
// Fallback template — used when Gemini call fails
// ---------------------------------------------------------------------------

function buildFallbackDraft(input: DraftInput, signature: string): StrikeDraft {
    const cleanedName = input.lead.executiveName ? cleanNameString(input.lead.executiveName) : '';
    const firstName = getCleanFirstName(cleanedName);
    const hasRealName = firstName.replace(/[^a-zA-Z]/g, '').length > 1
        && !firstName.toLowerCase().includes('decision-maker')
        && cleanedName.toLowerCase() !== 'unknown';
        
    const greeting = hasRealName ? `${firstName},` : 'Good morning,';
    const company = input.lead.company || 'your organization';

    // Keep the fallback strictly neutral: the trigger may be a layoff, lawsuit, or regulatory
    // action, so never congratulate, and never interpolate a missing headline as "undefined".
    const newsLine = input.triggerHeadline
        ? `I came across the recent news regarding ${company} — "${input.triggerHeadline}" — and it prompted me to reach out.`
        : `I've been following recent developments at ${company} and wanted to reach out directly.`;

    const body = [
        greeting,
        '',
        `${newsLine} Moments like this tend to raise strategic questions where the right advisory relationships and partnerships can make a material difference.`,
        '',
        `We work closely with leaders navigating similar situations — from strategic communications to partnership and advisory support aligned with their immediate priorities.`,
        '',
        `Happy to share a few thoughts if useful.`,
        '',
        `Best,`,
        signature,
    ].join('\n');

    return {
        personaUsed: input.persona,
        subject: `${company} — a quick note`,
        body,
        modelUsed: 'fallback-template',
        confidenceScore: 0.4,
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

    // ── Fetch Gold Drafts (few-shot exemplars) ──
    let goldDrafts = input.goldDrafts || [];
    if (goldDrafts.length === 0) {
        try {
            // Fetch up to 3 active gold drafts, preferring those matching the target type
            const targetTypeFilter = input.lead.executiveTitle?.toLowerCase().includes('reporter') ? 'reporter' : 'general';
            goldDrafts = await fetchRows(env, `gold_drafts?is_active=eq.true&order=usage_count.asc&limit=3`);
            // If we got general ones, try to find target-specific ones too
            if (targetTypeFilter !== 'general') {
                const specificDrafts = await fetchRows(env, `gold_drafts?is_active=eq.true&target_type=eq.${targetTypeFilter}&order=usage_count.asc&limit=2`);
                if (specificDrafts.length > 0) goldDrafts = [...specificDrafts, ...goldDrafts.slice(0, 1)];
            }
        } catch (e) {
            console.warn('⚠️ Failed to fetch gold drafts:', e);
        }
    }

    // ── Fetch Partner Templates (if partners attached) ──
    let partnerTemplates = input.partnerTemplates || [];
    if (partnerTemplates.length === 0 && input.partnerProfiles && input.partnerProfiles.length > 0) {
        try {
            for (const partner of input.partnerProfiles.slice(0, 3)) {
                const templates = await fetchRows(env, `partner_email_templates?partner_id=eq.${partner.id}&is_validated=eq.true&is_active=eq.true&limit=2`);
                partnerTemplates.push(...templates);
            }
        } catch (e) {
            console.warn('⚠️ Failed to fetch partner templates:', e);
        }
    }

    // ── Fetch Daily Messaging Directives ──
    let messagingDirectives = input.messagingDirectives || [];
    if (messagingDirectives.length === 0) {
        try {
            const now = new Date().toISOString();
            messagingDirectives = await fetchRows(env, `daily_messaging_directives?is_active=eq.true&or=(expires_at.is.null,expires_at.gt.${now})&order=priority.asc&limit=5`);
        } catch (e) {
            console.warn('⚠️ Failed to fetch messaging directives:', e);
        }
    }

    // Determine if we have a real contact name
    const cleanedName = input.lead.executiveName ? cleanNameString(input.lead.executiveName) : '';
    const firstName = getCleanFirstName(cleanedName);
    const hasRealName = firstName.replace(/[^a-zA-Z]/g, '').length > 1
        && !firstName.toLowerCase().includes('decision-maker')
        && cleanedName.toLowerCase() !== 'unknown';

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
${partnerTemplates.length > 0 ? `
PARTNER-SPECIFIC TEMPLATES (use these as structural and tonal references when incorporating partner value):
${partnerTemplates.map((t: any, i: number) => `Template ${i + 1}: "${t.name}"
Use Case: ${t.use_case || 'introduction'}
Subject: ${t.subject}
Body:
${t.body}`).join('\n\n---\n\n')}

Use these validated templates as inspiration for how to weave partner messaging into the email naturally.` : ''}
${goldDrafts.length > 0 ? `
FOUNDATION DRAFTS (exemplar emails — match their tone, strategic framing, and relationship-building approach):
These are high-value, human-crafted emails written by our team. Study their style, structure, voice, and messaging sophistication. Produce a new email that preserves these qualities while customizing content for the specific prospect.

${goldDrafts.map((d: any, i: number) => `--- Exemplar ${i + 1}: "${d.name}" (Category: ${d.category}) ---
Subject: ${d.subject}
Body:
${d.body}`).join('\n\n')}

IMPORTANT: Do NOT copy these drafts verbatim. Use them as a stylistic and strategic foundation. Adapt the content, references, and value propositions to the specific prospect and trigger event.` : ''}
${messagingDirectives.length > 0 ? `
DAILY MESSAGING PRIORITIES (adapt all emails to reflect these current strategic themes):
${messagingDirectives.map((d: any, i: number) => `${i + 1}. [${d.title}] ${d.directive}`).join('\n')}

Weave these priorities naturally into the email's strategic angle where relevant. Do not force them if they don't fit the prospect.` : ''}

WRITING STYLE RULES — THIS IS CRITICAL:
1. Write like a thoughtful, experienced professional sending a brief, warm, highly personal note. Speak DIRECTLY to the recipient in the first person (i.e. "I saw your recent appointment" instead of "I saw Michelle was appointed"). Do NOT refer to the recipient in the third person if the article is about them.
2. If the trigger event is about their new role or appointment, congratulate them personally on the position as your opening hook. Make it engaging, human, and genuine.
3. Open with a SPECIFIC, intelligent observation about the deal/event. Reference actual details from the article — dollar amounts, counterparties, market context, what makes this deal interesting. Show you actually read and understood the news.
4. In the second paragraph, draw a genuine connection showing how our training initiatives, employee benefits, or strategic partnership capabilities can support their new mandate and benefit their organization.
5. If they were recently appointed, review the PARTNER SYNDICATE ALIGNMENT (if provided) to actively bridge connections and suggest specific partnership opportunities that align with their agenda.
6. Keep the closing casual and low-pressure. Something like "Happy to share a few thoughts if useful" or "Worth a quick call?" — NOT "Would 15 minutes this week make sense?"
7. Tone: Confident but human. Knowledgeable but not lecturing. Warm but brief. You should sound like someone the reader would actually want to get coffee with. NEVER sound like an automated system.
8. Maximum 3-4 SHORT paragraphs. No bullet points. No HTML. No bold text. No emojis.
9. ${hasRealName ? `Address them by first name: "${firstName},"` : 'DO NOT say "Unknown." Start with a warm professional opening like "Good morning," or simply begin with your observation directly.'}
9. End with ONLY this signature block:
Best,
${signatureBlock}

OUTPUT FORMAT:
Respond with ONLY a JSON object (no markdown, no extra text):
{
  "subject": "<concise 6-10 word subject line A that sounds human — reference the company or deal specifically>",
  "subjectB": "<concise 6-10 word alternative subject line B that uses a different angle, styling, or question>",
  "body": "<full email body including greeting and signature>"
}`;

    const userPrompt = `TRIGGER EVENT: ${input.triggerHeadline}

ARTICLE CONTEXT (use specific details from this to make the email feel researched and personal):
${input.triggerArticleText || 'No article text available — use the trigger headline and what you know about the deal type to craft an intelligent email.'}

PROSPECT:
- Name: ${hasRealName ? cleanedName : 'Not yet identified — use a warm greeting without a name'}
- Title: ${input.lead.executiveTitle !== 'Unknown' ? input.lead.executiveTitle : 'Senior leadership'}
- Company: ${input.lead.company}
- Est. Revenue / AUM: ${input.lead.companyRevenue || 'Not available'}
- Team Size: ${input.lead.employeeCount || 'Not available'}
- Market Signals: ${input.lead.signals?.length ? input.lead.signals.join('; ') : 'Recent M&A activity'}

${input.lead.executiveResearch ? `EXECUTIVE BACKGROUND:\n${input.lead.executiveResearch}` : ''}

${input.stepContext ? `SEQUENCE CONTEXT (THIS IS A FOLLOW-UP EMAIL):
This is step ${input.stepContext.stepNumber} of ${input.stepContext.totalSteps} in a sequence.
PREVIOUS EMAILS SENT:
${input.stepContext.previousEmails.map((e, i) => `--- Email ${i + 1} ---\n${e}`).join('\n\n')}

YOUR INSTRUCTION FOR THIS SPECIFIC STEP:
${input.stepContext.stepPrompt}` : ''}

Write the outreach email. Sound human. Reference specific deal details. Be brief. ${input.stepContext ? 'Ensure it reads naturally as a follow-up to the previous emails, referencing them lightly if appropriate, but primarily focusing on the specific step instruction.' : ''}

${input.steeringNotes ? `CAMPAIGN OBJECTIVE / STEERING INSTRUCTIONS (incorporate these directly into the email strategy and rewrite):
${input.steeringNotes}` : ''}`;

    try {
        const response = await fetchGemini(env, 'pro', {
            activityName: 'generate-draft',
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
                    temperature: 0.70,
                    maxOutputTokens: 8192,
                    responseMimeType: 'application/json',
                    responseSchema: {
                        type: "OBJECT",
                        properties: {
                            subject: { type: "STRING", description: "Primary subject line A (focus on deal/role)" },
                            subjectB: { type: "STRING", description: "Alternative subject line B (different style/angle)" },
                            body: { type: "STRING" }
                        },
                        required: ["subject", "subjectB", "body"]
                    }
                },
            }),
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Gemini API error ${response.status}: ${errText}`);
        }

        const { text: rawText, finishReason } = await safeGeminiResponseParse(response);

        if (!rawText) {
            throw new Error('Gemini returned an empty response');
        }
        if (finishReason === 'MAX_TOKENS') {
            // A truncated body would be "repaired" into valid JSON that ends mid-sentence — never send that.
            throw new Error('Gemini draft hit MAX_TOKENS (truncated output)');
        }

        const parsed = safeJsonParse<{ subject?: string; subjectB?: string; body?: string } | null>(rawText, null);
        if (!parsed || !parsed.subject || !parsed.body) {
            throw new Error(`Gemini response failed to parse or missing subject/body fields. Got: ${rawText.slice(0, 200)}`);
        }

        const draft: StrikeDraft = {
            personaUsed: default_sender_name,
            // Leave subjectB undefined when the model omits it — downstream A/B logic handles null,
            // whereas a synthesized "<subject> - alt" would be sent verbatim to real prospects.
            subjectB: parsed.subjectB || undefined,
            subject: parsed.subject,
            body: parsed.body,
            modelUsed: GEMINI_PRO_MODEL,
            confidenceScore: 0.91,
        };

        console.log(`✅ Draft generated via Gemini (subject: "${draft.subject}")`);

        // Increment usage_count for gold drafts that were used
        if (goldDrafts.length > 0) {
            for (const gd of goldDrafts) {
                try {
                    const { patchRow } = await import('../utils/supabase');
                    await patchRow(env, 'gold_drafts', { usage_count: (gd.usage_count || 0) + 1 }, 'id', gd.id);
                } catch (_) { /* non-blocking */ }
            }
        }

        return draft;
    } catch (err) {
        console.error('❌ Gemini draft generation failed, using fallback template:', err);
        await logGeminiError(env, 'pro-generate-draft', 'generate-draft', err);
        // Store last error for diagnostics
        (generateDraft as any).__lastError = String(err);
        const fallback = buildFallbackDraft(input, signatureBlock);
        (fallback as any).__fallbackReason = String(err);
        return fallback;
    }
}
