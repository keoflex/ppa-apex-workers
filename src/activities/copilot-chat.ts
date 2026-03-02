/**
 * PPA+ APEX — Copilot Brain
 *
 * Gemini Function Calling activity for the APEX Copilot.
 * Defines tools, executes them against Supabase/Queue, and returns
 * structured JSON for the frontend to render (including Generative UI data).
 */

import type { Env } from '../index';
import { geminiUrl } from '../config/gemini';

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

export interface CopilotMessage {
    role: 'user' | 'assistant' | 'model';
    text: string;
    uiData?: any;
    toolUsed?: string;
}

export interface PageContext {
    type: 'strike' | 'campaign' | 'contact';
    entityId?: string | number;
    data: Record<string, any>;
}

export interface CopilotRequest {
    message: string;
    history: CopilotMessage[];
    pageContext?: PageContext;
}

export interface CopilotResponse {
    role: 'assistant';
    text: string;
    uiData?: any;
    toolUsed?: string;
}

// ──────────────────────────────────────────────────────────────────────────────
// Gemini Tool Declarations (Function Calling)
// ──────────────────────────────────────────────────────────────────────────────

const TOOL_DECLARATIONS = [
    {
        name: 'search_pending_strikes',
        description: 'Search for pending strike campaigns awaiting human approval. Use this when the user asks about pending strikes, drafts to review, or outreach waiting for approval. Returns a list of strike campaigns with lead details.',
        parameters: {
            type: 'OBJECT',
            properties: {
                keyword: {
                    type: 'STRING',
                    description: 'Optional keyword to filter strikes by company name, lead name, or email subject',
                },
                industry: {
                    type: 'STRING',
                    description: 'Optional industry filter (e.g. "Financial Technology", "Healthcare")',
                },
            },
        },
    },
    {
        name: 'create_campaign',
        description: 'Create a new outreach campaign in the system. Use this when the user wants to start a new campaign or initiative.',
        parameters: {
            type: 'OBJECT',
            properties: {
                name: {
                    type: 'STRING',
                    description: 'Name for the new campaign',
                },
                objective: {
                    type: 'STRING',
                    description: 'Description or objective of the campaign',
                },
            },
            required: ['name'],
        },
    },
    {
        name: 'dispatch_custom_strike',
        description: 'Dispatch an immediate, ad-hoc outreach strike against a specific target company. Bypasses the cron scheduler and immediately runs the AI pipeline (Exa sensing → Apollo enrichment → Gemini drafting). Use when the user wants to immediately target a specific company.',
        parameters: {
            type: 'OBJECT',
            properties: {
                target_company: {
                    type: 'STRING',
                    description: 'Name of the company to target',
                },
                context: {
                    type: 'STRING',
                    description: 'Additional context about why this company is being targeted (e.g. "Recent acquisition announcement")',
                },
            },
            required: ['target_company'],
        },
    },
    {
        name: 'research_entity',
        description: 'Search the web (via Exa) for additional intelligence about a company, executive, or topic. Use this when the user wants to learn more, asks for research, or says "tell me more about..." or "research this company." Returns web search results with key findings.',
        parameters: {
            type: 'OBJECT',
            properties: {
                query: {
                    type: 'STRING',
                    description: 'Search query — e.g. the company name, executive name, or topic to research',
                },
                entity_type: {
                    type: 'STRING',
                    description: 'Type of entity: "company", "person", or "topic"',
                },
            },
            required: ['query'],
        },
    },
    {
        name: 'save_intelligence',
        description: 'Save newly discovered intelligence back to the knowledge base. Use this when the user explicitly wants to add research findings to the system — e.g. "add this", "save this info", "update the record with this". Merges data into the enrichment_data field.',
        parameters: {
            type: 'OBJECT',
            properties: {
                target_table: {
                    type: 'STRING',
                    description: 'Which table to update: "lead_targets" or "crm_companies"',
                },
                target_id: {
                    type: 'NUMBER',
                    description: 'ID of the record to update',
                },
                intelligence_key: {
                    type: 'STRING',
                    description: 'Key under enrichment_data to store this — e.g. "chat_research", "additional_intel", "competitive_analysis"',
                },
                intelligence_value: {
                    type: 'STRING',
                    description: 'The intelligence text or summary to save',
                },
            },
            required: ['target_table', 'target_id', 'intelligence_key', 'intelligence_value'],
        },
    },
];

// ──────────────────────────────────────────────────────────────────────────────
// Supabase REST helpers (inline for this activity)
// ──────────────────────────────────────────────────────────────────────────────

function sbHeaders(env: Env): Record<string, string> {
    return {
        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json',
        Prefer: 'return=representation',
    };
}

async function sbQuery(env: Env, path: string): Promise<any[]> {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, {
        method: 'GET',
        headers: sbHeaders(env),
    });
    if (!res.ok) return [];
    return (await res.json()) as any[];
}

async function sbInsert(env: Env, table: string, data: Record<string, unknown>): Promise<any> {
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/${table}`, {
        method: 'POST',
        headers: sbHeaders(env),
        body: JSON.stringify(data),
    });
    if (!res.ok) {
        const err = await res.text();
        throw new Error(`Insert failed: ${err}`);
    }
    const rows = (await res.json()) as any[];
    return rows[0];
}

// ──────────────────────────────────────────────────────────────────────────────
// Tool Execution
// ──────────────────────────────────────────────────────────────────────────────

async function executeTool(
    name: string,
    args: Record<string, any>,
    env: Env,
): Promise<{ result: any; uiData?: any }> {
    switch (name) {
        case 'search_pending_strikes': {
            // Build PostgREST query with optional filters
            let query = `strike_campaigns?status=eq.pending_hitl&select=id,email_subject,drafted_body,persona_used,created_at,target_id,lead_targets(id,company,executive_name,executive_title,trigger_event)&order=created_at.desc&limit=10`;

            const rows = await sbQuery(env, query);

            // Apply client-side keyword/industry filter if provided
            let filtered = rows;
            if (args.keyword) {
                const kw = args.keyword.toLowerCase();
                filtered = filtered.filter((r: any) => {
                    const lt = r.lead_targets || {};
                    return (
                        (lt.company || '').toLowerCase().includes(kw) ||
                        (lt.executive_name || '').toLowerCase().includes(kw) ||
                        (r.email_subject || '').toLowerCase().includes(kw) ||
                        (lt.trigger_event || '').toLowerCase().includes(kw)
                    );
                });
            }

            const uiData = filtered.map((r: any) => ({
                id: r.id,
                subject: r.email_subject,
                persona: r.persona_used,
                leadName: r.lead_targets?.executive_name || 'Unknown',
                company: r.lead_targets?.company || 'Unknown',
                title: r.lead_targets?.executive_title || '',
                triggerEvent: r.lead_targets?.trigger_event || '',
                createdAt: r.created_at,
            }));

            return {
                result: { count: uiData.length, strikes: uiData },
                uiData,
            };
        }

        case 'create_campaign': {
            const row = await sbInsert(env, 'campaigns', {
                name: args.name,
                objective: args.objective || '',
                status: 'draft',
            });

            return {
                result: {
                    success: true,
                    campaignId: row?.id,
                    name: args.name,
                    message: `Campaign "${args.name}" created successfully.`,
                },
            };
        }

        case 'dispatch_custom_strike': {
            await env.STRIKE_QUEUE.send({
                type: 'custom_strike',
                target_company: args.target_company,
                context: args.context || '',
                dispatched_at: new Date().toISOString(),
            });

            return {
                result: {
                    success: true,
                    target: args.target_company,
                    message: `Custom strike dispatched against "${args.target_company}". The AI pipeline is now running.`,
                },
            };
        }

        case 'research_entity': {
            const query = args.query;
            const entityType = args.entity_type || 'company';

            // Use Exa search to find intelligence
            try {
                const exaRes = await fetch('https://api.exa.ai/search', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': env.EXA_API_KEY,
                    },
                    body: JSON.stringify({
                        query: `${query} ${entityType === 'person' ? 'executive background career' : 'company news funding growth'}`,
                        numResults: 5,
                        useAutoprompt: true,
                        type: 'neural',
                        contents: { text: { maxCharacters: 1500 } },
                    }),
                });

                if (!exaRes.ok) {
                    return { result: { error: `Exa search failed: ${exaRes.status}` } };
                }

                const exaData = (await exaRes.json()) as any;
                const results = (exaData.results || []).map((r: any) => ({
                    title: r.title,
                    url: r.url,
                    excerpt: r.text?.slice(0, 500) || '',
                    publishedDate: r.publishedDate,
                }));

                // Ask Gemini to summarize the findings
                const summaryRes = await fetch(geminiUrl(env.GEMINI_API_KEY), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{
                            role: 'user',
                            parts: [{
                                text: `Summarize the following web search results about "${query}" into a concise intelligence brief. Focus on key facts, recent developments, financials, leadership, and strategic opportunities for a law firm looking to provide legal services.\n\nResults:\n${results.map((r: any) => `- ${r.title}: ${r.excerpt}`).join('\n\n')}`,
                            }],
                        }],
                        generationConfig: { temperature: 0.3, maxOutputTokens: 1500 },
                    }),
                });

                let summary = '';
                if (summaryRes.ok) {
                    const summaryData = (await summaryRes.json()) as any;
                    summary = summaryData?.candidates?.[0]?.content?.parts?.[0]?.text || '';
                }

                return {
                    result: {
                        query,
                        resultCount: results.length,
                        summary,
                        sources: results,
                        canSave: true,
                    },
                    uiData: {
                        type: 'research_results',
                        summary,
                        sources: results,
                    },
                };
            } catch (err: any) {
                return { result: { error: `Research failed: ${err.message}` } };
            }
        }

        case 'save_intelligence': {
            const { target_table, target_id, intelligence_key, intelligence_value } = args;

            if (!['lead_targets', 'crm_companies'].includes(target_table)) {
                return { result: { error: 'Invalid table. Must be lead_targets or crm_companies.' } };
            }

            try {
                // Fetch current enrichment_data
                const current = await sbQuery(
                    env,
                    `${target_table}?id=eq.${target_id}&select=enrichment_data`
                );
                const existing = current?.[0]?.enrichment_data || {};

                // Merge new intelligence
                const updated = {
                    ...existing,
                    [intelligence_key]: intelligence_value,
                    [`${intelligence_key}_added_at`]: new Date().toISOString(),
                    [`${intelligence_key}_source`]: 'copilot_chat',
                };

                // Update the record
                const res = await fetch(
                    `${env.SUPABASE_URL}/rest/v1/${target_table}?id=eq.${target_id}`,
                    {
                        method: 'PATCH',
                        headers: sbHeaders(env),
                        body: JSON.stringify({ enrichment_data: updated }),
                    }
                );

                if (!res.ok) {
                    const err = await res.text();
                    throw new Error(`Update failed: ${err}`);
                }

                return {
                    result: {
                        success: true,
                        message: `Intelligence saved to ${target_table}#${target_id} under "${intelligence_key}".`,
                        key: intelligence_key,
                    },
                };
            } catch (err: any) {
                return { result: { error: `Save failed: ${err.message}` } };
            }
        }

        default:
            return { result: { error: `Unknown tool: ${name}` } };
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Main Copilot Chat Function
// ──────────────────────────────────────────────────────────────────────────────

function buildSystemPrompt(pageContext?: PageContext): string {
    let prompt = `You are the APEX Copilot — a strategic AI assistant embedded in the PPA+ APEX outreach platform.

You have access to the following tools:
- search_pending_strikes: Find pending outreach campaigns waiting for human review/approval
- create_campaign: Create new outreach campaigns
- dispatch_custom_strike: Immediately target a specific company with the full AI pipeline
- research_entity: Search the web for additional intelligence about a company, person, or topic. Use this when the user asks for more info or says "research this."
- save_intelligence: Save newly discovered intelligence back to the knowledge base. Only use when the user explicitly wants to add or save data.

You should:
- Be concise, professional, and strategic in your responses
- Proactively use tools when the user's intent clearly maps to one
- When showing pending strikes, summarize the results conversationally AND return the data for the UI to render as interactive cards
- When actions succeed, confirm clearly with specifics
- When asked general questions, respond helpfully without tools
- When research_entity returns results, present the key findings clearly and let the user know they can add them to the knowledge base

You are speaking to a senior business development professional. Be direct and action-oriented.`;

    if (pageContext) {
        const ctx = pageContext;
        const d = ctx.data || {};
        prompt += `\n\n--- CURRENT PAGE CONTEXT ---\nThe user is currently viewing a ${ctx.type}.`;

        if (ctx.type === 'strike') {
            prompt += `\nStrike Opportunity Details:
- Company: ${d.targetCompany || 'N/A'}
- Executive: ${d.targetExecutive || 'N/A'}
- Title: ${d.executiveTitle || 'N/A'}
- Trigger Event: ${d.triggerEvent || 'N/A'}
- Persona Used: ${d.personaUsed || 'N/A'}
- Email Subject: ${d.emailSubject || 'N/A'}
- Status: ${d.status || 'N/A'}`;
            if (d.enrichmentData) {
                const ed = d.enrichmentData;
                if (ed.trigger_summary) prompt += `\n- Market Opportunity: ${ed.trigger_summary.slice(0, 800)}`;
                if (ed.executive_research) prompt += `\n- Executive Background: ${ed.executive_research.slice(0, 800)}`;
                if (ed.company_intelligence) prompt += `\n- Company Intel: ${typeof ed.company_intelligence === 'string' ? ed.company_intelligence.slice(0, 500) : JSON.stringify(ed.company_intelligence).slice(0, 500)}`;
                if (ed.email) prompt += `\n- Email: ${ed.email}`;
                if (ed.phone) prompt += `\n- Phone: ${ed.phone}`;
                if (ed.revenue) prompt += `\n- Revenue: ${ed.revenue}`;
                if (ed.employees) prompt += `\n- Employees: ${ed.employees}`;
            }
            if (ctx.entityId) prompt += `\nTarget ID (for save_intelligence): lead_targets#${ctx.entityId}`;
        } else if (ctx.type === 'campaign') {
            prompt += `\nCampaign Details:
- Name: ${d.name || 'N/A'}
- Objective: ${d.objective || 'N/A'}
- Status: ${d.status || 'N/A'}
- Partner Count: ${d.partnerCount || 0}
- Strike Count: ${d.strikeCount || 0}`;
        } else if (ctx.type === 'contact') {
            prompt += `\nContact/Company Details:
- Company: ${d.companyName || 'N/A'}
- Industry: ${d.industry || 'N/A'}
- Revenue: ${d.revenue || 'N/A'}
- Employees: ${d.employees || 'N/A'}
- Territory: ${d.territory || 'N/A'}`;
            if (ctx.entityId) prompt += `\nCompany ID (for save_intelligence): crm_companies#${ctx.entityId}`;
        }

        prompt += `\n--- END CONTEXT ---
CRITICAL: The data above describes what the user is currently looking at. When they say "this company", "this strike", "tell me about them", or any similar reference, ALWAYS use the context data above. NEVER ask "which company?" when you have context data. Start your answer using the company/entity name from the context.`;
    }

    return prompt;
}

export async function copilotChat(
    request: CopilotRequest,
    env: Env,
): Promise<CopilotResponse> {
    // Build Gemini conversation history
    const contents: any[] = [];

    for (const msg of request.history) {
        if (msg.role === 'user') {
            contents.push({ role: 'user', parts: [{ text: msg.text }] });
        } else if (msg.role === 'assistant' || msg.role === 'model') {
            contents.push({ role: 'model', parts: [{ text: msg.text }] });
        }
    }

    // Add current message
    contents.push({ role: 'user', parts: [{ text: request.message }] });

    // First Gemini call — with tool declarations
    const geminiPayload = {
        system_instruction: { parts: [{ text: buildSystemPrompt(request.pageContext) }] },
        contents,
        tools: [{ function_declarations: TOOL_DECLARATIONS }],
        generationConfig: { temperature: 0.7, maxOutputTokens: 2048 },
    };

    const res1 = await fetch(geminiUrl(env.GEMINI_API_KEY), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(geminiPayload),
    });

    if (!res1.ok) {
        const errText = await res1.text();
        console.error(`[copilot] Gemini error ${res1.status}: ${errText.slice(0, 300)}`);
        return {
            role: 'assistant',
            text: 'I encountered an issue connecting to my AI backend. Please try again.',
        };
    }

    const data1 = (await res1.json()) as any;
    const candidate = data1?.candidates?.[0];
    const parts = candidate?.content?.parts || [];

    // Check if Gemini returned a function call
    const fnCallPart = parts.find((p: any) => p.functionCall);

    if (!fnCallPart) {
        // No tool call — just return the text
        const textPart = parts.find((p: any) => p.text);
        return {
            role: 'assistant',
            text: textPart?.text || 'I didn\'t quite catch that. Could you rephrase?',
        };
    }

    // ── Execute the tool ──
    const { name, args } = fnCallPart.functionCall;
    console.log(`[copilot] Tool call: ${name}`, JSON.stringify(args));

    let toolResult: { result: any; uiData?: any };
    try {
        toolResult = await executeTool(name, args || {}, env);
    } catch (err: any) {
        console.error(`[copilot] Tool execution error:`, err);
        toolResult = { result: { error: err.message } };
    }

    // ── Second Gemini call — pass function response back ──
    const contents2 = [
        ...contents,
        { role: 'model', parts: [{ functionCall: { name, args } }] },
        {
            role: 'function',
            parts: [
                {
                    functionResponse: {
                        name,
                        response: toolResult.result,
                    },
                },
            ],
        },
    ];

    const res2 = await fetch(geminiUrl(env.GEMINI_API_KEY), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            system_instruction: { parts: [{ text: buildSystemPrompt(request.pageContext) }] },
            contents: contents2,
            generationConfig: { temperature: 0.7, maxOutputTokens: 2048 },
        }),
    });

    let finalText = `I executed the "${name}" action. Here are the results.`;

    if (res2.ok) {
        const data2 = (await res2.json()) as any;
        const textPart2 = data2?.candidates?.[0]?.content?.parts?.find((p: any) => p.text);
        if (textPart2?.text) {
            finalText = textPart2.text;
        }
    }

    return {
        role: 'assistant',
        text: finalText,
        uiData: toolResult.uiData,
        toolUsed: name,
    };
}
