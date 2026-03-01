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

export interface CopilotRequest {
    message: string;
    history: CopilotMessage[];
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

        default:
            return { result: { error: `Unknown tool: ${name}` } };
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Main Copilot Chat Function
// ──────────────────────────────────────────────────────────────────────────────

const SYSTEM_PROMPT = `You are the APEX Copilot — a strategic AI assistant embedded in the PPA+ APEX outreach platform.

You have access to the following tools:
- search_pending_strikes: Find pending outreach campaigns waiting for human review/approval
- create_campaign: Create new outreach campaigns
- dispatch_custom_strike: Immediately target a specific company with the full AI pipeline

You should:
- Be concise, professional, and strategic in your responses
- Proactively use tools when the user's intent clearly maps to one
- When showing pending strikes, summarize the results conversationally AND return the data for the UI to render as interactive cards
- When actions succeed, confirm clearly with specifics
- When asked general questions, respond helpfully without tools

You are speaking to a senior business development professional. Be direct and action-oriented.`;

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
        system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
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
            system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
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
