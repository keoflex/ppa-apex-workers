/**
 * PPA+ APEX — Strike Engine Worker (Entry Point)
 *
 * Cloudflare Worker handling:
 * 1. HTTP requests (manual triggers, health checks, webhook receivers)
 * 2. Queue consumption (strike pipeline processing)
 * 3. Durable Object exports (HITL gate)
 */
import { HitlGateDurableObject } from './durable-objects/hitl-gate';
import { senseTriggers, senseTriggersForAgent, type MarketTrigger } from './activities/sense-triggers';
import { senseSecFilings, senseSecFilingsForQuery } from './activities/sense-sec-filings';
import { senseCourtFilings, senseCourtFilingsForQuery } from './activities/sense-court-filings';
import { senseNews, senseNewsForQuery } from './activities/sense-news';
import { deduplicateTriggers } from './utils/dedup';
import { enrichLead, type EnrichedLead } from './activities/enrich-lead';
import type { DraftInput } from './activities/generate-draft';
import { generateDraft } from './activities/generate-draft';
import { generateTerritoryBriefings } from './tasks/generate-briefing';
import { geminiUrl, GEMINI_REST_URL } from './config/gemini';
import { executeCampaign } from './activities/execute-campaign';
import { triageReply } from './activities/triage-reply';
import { copilotChat, type CopilotRequest } from './activities/copilot-chat';
import { insertRow, patchRow, fetchRow } from './utils/supabase';

export { HitlGateDurableObject };

export interface Env {
    HITL_GATE: DurableObjectNamespace;
    STRIKE_QUEUE: Queue;
    SUPABASE_URL: string;
    SUPABASE_SERVICE_ROLE_KEY: string;
    GEMINI_API_KEY: string;
    EXA_API_KEY: string;
    APOLLO_API_KEY: string;
    SMARTLEAD_API_KEY: string;
    WORKER_SECRET: string;
    ENVIRONMENT: string;
    COURTLISTENER_API_KEY?: string;
    NEWSDATA_API_KEY?: string;
    CRM_ATTACHMENTS?: R2Bucket;  // enable after R2 activation in dashboard
}

export default {
    /**
     * HTTP handler — manual triggers and health checks.
     */
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);

        // ── CORS headers for cross-origin browser calls ──
        const corsHeaders: Record<string, string> = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-worker-secret, X-R2-Key',
        };

        // Handle preflight OPTIONS
        if (request.method === 'OPTIONS') {
            return new Response(null, { status: 204, headers: corsHeaders });
        }

        // Helper to attach CORS headers to every response
        const jsonWithCors = (data: any, init?: ResponseInit) => {
            const headers = new Headers(init?.headers);
            for (const [k, v] of Object.entries(corsHeaders)) headers.set(k, v);
            headers.set('Content-Type', 'application/json');
            return new Response(JSON.stringify(data), { ...init, headers });
        };

        // Health check
        if (url.pathname === '/health') {
            return jsonWithCors({
                status: 'operational',
                service: 'PPA+ APEX Strike Engine',
                version: '1.0.0',
                durableObjects: true,
            });
        }

        // Apollo diagnostic test
        if (url.pathname === '/api/test-apollo' && request.method === 'GET') {
            const hasKey = !!env.APOLLO_API_KEY;
            const keyPrefix = env.APOLLO_API_KEY ? env.APOLLO_API_KEY.substring(0, 8) + '...' : 'NOT SET';
            const results: Record<string, any> = { apolloKeyPresent: hasKey, apolloKeyPrefix: keyPrefix };

            const endpoints = [
                { name: 'people/match', url: 'https://api.apollo.io/v1/people/match', body: { first_name: 'Satya', last_name: 'Nadella', organization_name: 'Microsoft' } },
                { name: 'people/search', url: 'https://api.apollo.io/v1/people/search', body: { q_organization_name: 'Microsoft', person_seniorities: ['c_suite'], per_page: 2 } },
                { name: 'mixed_people/search', url: 'https://api.apollo.io/v1/mixed_people/search', body: { q_organization_name: 'Microsoft', person_seniorities: ['c_suite'], per_page: 2 } },
                { name: 'organizations/enrich', url: 'https://api.apollo.io/v1/organizations/enrich', body: { domain: 'microsoft.com' } },
                { name: 'mixed_companies/search', url: 'https://api.apollo.io/v1/mixed_companies/search', body: { q_organization_name: 'Microsoft', per_page: 1 } },
            ];

            for (const ep of endpoints) {
                try {
                    const r = await fetch(ep.url, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'x-api-key': env.APOLLO_API_KEY || '' },
                        body: JSON.stringify(ep.body),
                    });
                    const data = await r.json() as any;
                    results[ep.name] = {
                        status: r.status,
                        accessible: r.status !== 403,
                        error: data.error || null,
                        sampleData: data.person?.email || data.people?.[0]?.email || data.organization?.name || 'check response',
                    };
                } catch (err) {
                    results[ep.name] = { error: String(err) };
                }
            }

            return jsonWithCors(results);
        }

        // Raw Apollo response diagnostic
        if (url.pathname === '/api/test-apollo-raw' && request.method === 'GET') {
            try {
                const res = await fetch('https://api.apollo.io/v1/people/match', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': env.APOLLO_API_KEY || '' },
                    body: JSON.stringify({
                        first_name: 'Glauber',
                        last_name: 'Correa',
                        organization_name: 'Agibank',
                    }),
                });
                const data = await res.json() as any;
                return jsonWithCors({
                    status: res.status,
                    hasEmail: !!data.person?.email,
                    email: data.person?.email || null,
                    hasPhoneNumbers: !!data.person?.phone_numbers?.length,
                    phoneNumbers: data.person?.phone_numbers || [],
                    phoneNumber: data.person?.phone_number || null,
                    sanitizedPhone: data.person?.sanitized_phone || null,
                    organizationName: data.person?.organization?.name || null,
                    organizationDomain: data.person?.organization?.primary_domain || null,
                    linkedinUrl: data.person?.linkedin_url || null,
                    title: data.person?.title || null,
                    seniority: data.person?.seniority || null,
                    allPersonKeys: data.person ? Object.keys(data.person) : [],
                });
            } catch (err) {
                return jsonWithCors({ error: String(err) });
            }
        }

        // Reveal phone number on-demand (costs 8 mobile credits per reveal)
        if (url.pathname === '/api/reveal-phone' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    firstName: string;
                    lastName: string;
                    organizationName: string;
                    email?: string;
                };

                console.log(`📞 Phone reveal requested for ${body.firstName} ${body.lastName} at ${body.organizationName}`);

                const controller = new AbortController();
                const timeout = setTimeout(() => controller.abort(), 10000);

                // Apollo requires a webhook_url for phone reveals — it processes async
                const webhookUrl = `https://ppa-apex-workers.fred-78e.workers.dev/api/phone-webhook`;

                const res = await fetch('https://api.apollo.io/v1/people/match', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': env.APOLLO_API_KEY || '',
                    },
                    body: JSON.stringify({
                        first_name: body.firstName,
                        last_name: body.lastName,
                        organization_name: body.organizationName,
                        ...(body.email ? { email: body.email } : {}),
                        reveal_phone_number: true,
                        webhook_url: webhookUrl,
                    }),
                    signal: controller.signal,
                });

                clearTimeout(timeout);

                if (!res.ok) {
                    const errBody = await res.text().catch(() => '');
                    console.error(`[reveal-phone] Apollo returned ${res.status}: ${errBody}`);
                    return jsonWithCors({ error: `Apollo returned ${res.status}`, detail: errBody }, { status: 502 });
                }

                const data = await res.json() as any;
                const person = data.person;

                // Check if phone numbers came back immediately (sometimes they do)
                const phones = (person?.phone_numbers || []).map((p: any) => ({
                    number: p.sanitized_number || p.raw_number || '',
                    type: p.type || 'unknown',
                })).filter((p: any) => p.number);

                if (phones.length > 0) {
                    console.log(`📞 Found ${phones.length} phone numbers immediately`);
                    return jsonWithCors({
                        status: 'revealed',
                        phones,
                        primaryPhone: phones[0]?.number || null,
                        primaryPhoneType: phones[0]?.type || null,
                    });
                }

                // Phone reveal is processing async — Apollo will push to webhook
                console.log(`📞 Phone reveal submitted — Apollo will push via webhook`);
                return jsonWithCors({
                    status: 'processing',
                    message: 'Phone reveal submitted. Apollo is processing — the phone number will appear shortly after refreshing.',
                    primaryPhone: null,
                });
            } catch (error: any) {
                if (error?.name === 'AbortError') {
                    return jsonWithCors({ error: 'Apollo phone reveal timed out (10s)' }, { status: 504 });
                }
                console.error('[reveal-phone] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Enrich a CRM contact via Apollo (find email, phone, LinkedIn)
        // Strategy: try people/match first, fall back to people/search for partial names
        if (url.pathname === '/api/enrich-contact' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    firstName: string;
                    lastName: string;
                    organizationName: string;
                    domain?: string;
                    title?: string;
                };

                console.log(`🔍 Contact enrich: ${body.firstName} ${body.lastName} at ${body.organizationName} (domain: ${body.domain || 'none'})`);

                let person: any = null;

                // Strategy 1: people/match (best when we have first + last name)
                if (body.firstName && body.lastName) {
                    const matchRes = await fetch('https://api.apollo.io/v1/people/match', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'x-api-key': env.APOLLO_API_KEY || '' },
                        body: JSON.stringify({
                            first_name: body.firstName, last_name: body.lastName,
                            organization_name: body.organizationName,
                            ...(body.domain ? { domain: body.domain } : {}),
                        }),
                    });
                    if (matchRes.ok) {
                        const matchData = await matchRes.json() as any;
                        if (matchData.person) person = matchData.person;
                    }
                }

                // Strategy 2: people/search (works with partial names, uses org + title)
                if (!person) {
                    console.log('🔍 Falling back to people/search...');
                    const searchBody: any = {
                        person_titles: body.title ? [body.title] : [],
                        q_organization_name: body.organizationName || undefined,
                        q_person_name: `${body.firstName}${body.lastName ? ' ' + body.lastName : ''}`,
                        per_page: 3,
                        page: 1,
                    };
                    if (body.domain) {
                        searchBody.organization_domains = [body.domain];
                    }

                    const searchRes = await fetch('https://api.apollo.io/v1/mixed_people/search', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'x-api-key': env.APOLLO_API_KEY || '' },
                        body: JSON.stringify(searchBody),
                    });

                    if (searchRes.ok) {
                        const searchData = await searchRes.json() as any;
                        const people = searchData.people || [];
                        if (people.length > 0) {
                            // Pick best match: prefer same org
                            person = people.find((p: any) =>
                                p.organization?.name?.toLowerCase().includes(body.organizationName.toLowerCase())
                            ) || people[0];
                        }
                    }
                }

                if (!person) {
                    return jsonWithCors({ status: 'not_found', message: 'No match found in Apollo' });
                }

                const phones = (person.phone_numbers || []).map((p: any) => ({
                    number: p.sanitized_number || p.raw_number || '', type: p.type || 'unknown',
                })).filter((p: any) => p.number);

                const fullName = [person.first_name, person.last_name].filter(Boolean).join(' ') || null;

                return jsonWithCors({
                    status: 'found',
                    email: person.email || null,
                    phone: phones[0]?.number || null,
                    linkedin_url: person.linkedin_url || null,
                    title: person.title || null,
                    full_name: fullName,
                    headline: person.headline || null,
                    city: person.city || null,
                    state: person.state || null,
                    seniority: person.seniority || null,
                    departments: person.departments || [],
                });
            } catch (error: any) {
                if (error?.name === 'AbortError') {
                    return jsonWithCors({ error: 'Apollo enrich timed out (10s)' }, { status: 504 });
                }
                console.error('[enrich-contact] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // AI Resync — Generate company intelligence via Gemini
        if (url.pathname === '/api/ai-resync' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    companyName: string;
                    domain?: string;
                    industry?: string;
                    existingSummary?: string;
                };

                console.log(`🧠 AI Resync requested for ${body.companyName}`);

                const GEMINI_URL = GEMINI_REST_URL;

                const systemPrompt = `You are a strategic business intelligence analyst. Research and generate a concise but comprehensive profile of the specified company.

OUTPUT FORMAT: Respond with a JSON object ONLY (no markdown):
{
  "summary": "3-4 sentence strategic intelligence brief covering: what the company does, their market position, key differentiators, and any notable recent developments or strategic moves. Be specific and factual.",
  "industry": "Primary industry classification (e.g. 'Financial Technology', 'Enterprise SaaS')",
  "revenue": "Estimated revenue range if inferable (e.g. '$1B-5B', '$50M+'), or null",
  "headcount": "Estimated headcount range if inferable (e.g. '5,000-10,000', '200+'), or null"
}`;

                const userPrompt = `Company: ${body.companyName}
${body.domain ? `Domain: ${body.domain}` : ''}
${body.industry ? `Known Industry: ${body.industry}` : ''}
${body.existingSummary ? `Existing Intel (enrich and expand): ${body.existingSummary}` : ''}

Generate a strategic intelligence profile for this company. Be factual, specific, and concise.`;

                const geminiRes = await fetch(`${GEMINI_URL}?key=${env.GEMINI_API_KEY}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        system_instruction: { parts: [{ text: systemPrompt }] },
                        contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
                        generationConfig: { temperature: 0.7, maxOutputTokens: 1024, responseMimeType: 'application/json' },
                    }),
                });

                if (!geminiRes.ok) {
                    const errText = await geminiRes.text();
                    console.error(`[ai-resync] Gemini error ${geminiRes.status}: ${errText}`);
                    return jsonWithCors({ error: `Gemini API error ${geminiRes.status}: ${errText.slice(0, 200)}` }, { status: 502 });
                }

                const geminiData = await geminiRes.json() as any;
                const parts = geminiData?.candidates?.[0]?.content?.parts || [];
                const rawText = parts.find((p: any) => p.text)?.text;

                if (!rawText) {
                    console.error('[ai-resync] Gemini empty response:', JSON.stringify(geminiData).slice(0, 300));
                    return jsonWithCors({ error: 'Gemini returned empty response' }, { status: 502 });
                }

                let parsed: any;
                try {
                    parsed = JSON.parse(rawText);
                } catch {
                    console.error('[ai-resync] Failed to parse Gemini output:', rawText.slice(0, 300));
                    // Try to extract from markdown code block
                    const jsonMatch = rawText.match(/```json?\s*([\s\S]*?)```/);
                    if (jsonMatch) {
                        parsed = JSON.parse(jsonMatch[1].trim());
                    } else {
                        return jsonWithCors({ summary: rawText.slice(0, 500), industry: null, revenue: null, headcount: null });
                    }
                }
                console.log(`✅ AI Resync complete for ${body.companyName}`);

                return jsonWithCors({
                    summary: parsed.summary || null,
                    industry: parsed.industry || null,
                    revenue: parsed.revenue || null,
                    headcount: parsed.headcount || null,
                });
            } catch (error: any) {
                console.error('[ai-resync] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Phone webhook receiver — Apollo pushes phone data here
        if (url.pathname === '/api/phone-webhook' && request.method === 'POST') {
            try {
                const body = await request.json() as any;
                console.log(`📞 Phone webhook received:`, JSON.stringify(body).slice(0, 500));

                // Apollo sends the person data with phone numbers
                const person = body.person || body;
                const phones = (person.phone_numbers || []).map((p: any) => ({
                    number: p.sanitized_number || p.raw_number || '',
                    type: p.type || 'unknown',
                })).filter((p: any) => p.number);

                if (phones.length > 0 && person.email) {
                    // Find the lead_target by email and update enrichment_data
                    const supabaseUrl = env.SUPABASE_URL;
                    const supabaseKey = env.SUPABASE_SERVICE_ROLE_KEY;
                    if (supabaseUrl && supabaseKey) {
                        // Search for the lead target with this email
                        const searchRes = await fetch(
                            `${supabaseUrl}/rest/v1/lead_targets?enrichment_data->>email=eq.${encodeURIComponent(person.email)}&select=id,enrichment_data`,
                            {
                                headers: {
                                    'apikey': supabaseKey,
                                    'Authorization': `Bearer ${supabaseKey}`,
                                },
                            }
                        );
                        if (searchRes.ok) {
                            const targets = await searchRes.json() as any[];
                            for (const target of targets) {
                                const existing = target.enrichment_data || {};
                                existing.phone = phones[0].number;
                                await fetch(
                                    `${supabaseUrl}/rest/v1/lead_targets?id=eq.${target.id}`,
                                    {
                                        method: 'PATCH',
                                        headers: {
                                            'apikey': supabaseKey,
                                            'Authorization': `Bearer ${supabaseKey}`,
                                            'Content-Type': 'application/json',
                                            'Prefer': 'return=minimal',
                                        },
                                        body: JSON.stringify({ enrichment_data: existing }),
                                    }
                                );
                                console.log(`📞 Phone saved to lead_target ${target.id}: ${phones[0].number}`);
                            }
                        }
                    }
                }

                return jsonWithCors({ status: 'received', phones: phones.length });
            } catch (error) {
                console.error('[phone-webhook] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // ── Shared-secret guard for protected routes ──
        const protectedPaths = ['/api/execute', '/api/copilot/chat'];
        if (protectedPaths.includes(url.pathname) && request.method === 'POST') {
            const secret = request.headers.get('x-worker-secret');
            if (!secret || secret !== env.WORKER_SECRET) {
                return jsonWithCors(
                    { error: 'Unauthorized — invalid or missing x-worker-secret header' },
                    { status: 401 },
                );
            }
        }

        // ── APEX Copilot Chat ──
        if (url.pathname === '/api/copilot/chat' && request.method === 'POST') {
            try {
                const body = await request.json() as CopilotRequest;
                if (!body.message) {
                    return jsonWithCors({ error: 'Missing "message" field' }, { status: 400 });
                }
                console.log(`🤖 Copilot: "${body.message.slice(0, 80)}"`);
                const result = await copilotChat(body, env);
                return jsonWithCors(result);
            } catch (error: any) {
                console.error('[copilot] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Manual strike trigger → push to Queue for async processing
        if (url.pathname === '/api/trigger-strike' && request.method === 'POST') {
            try {
                const body = await request.json() as { campaignId: number; persona: string };

                if (env.STRIKE_QUEUE) {
                    await env.STRIKE_QUEUE.send({
                        campaignId: body.campaignId,
                        persona: body.persona || "Rob O'Neill",
                        source: 'manual',
                    });
                    return jsonWithCors(
                        { status: 'queued', campaignId: body.campaignId },
                        { status: 202 },
                    );
                }

                // Fallback: run inline if Queue not available (local dev)
                console.log(`🚀 Manual strike trigger (inline) | Campaign #${body.campaignId}`);
                const triggers = await senseTriggers(env);
                if (triggers.length === 0) {
                    return Response.json({ error: 'No triggers detected' }, { status: 404 });
                }

                const selectedTrigger = triggers[0];
                const enrichedLead = await enrichLead(env, {
                    company: selectedTrigger.company,
                    executiveName: selectedTrigger.executiveName,
                    executiveTitle: selectedTrigger.executiveTitle,
                });

                const draft = await generateDraft(env, {
                    lead: enrichedLead,
                    persona: body.persona || "Rob O'Neill",
                    triggerHeadline: selectedTrigger.headline,
                });

                return Response.json({
                    status: 'draft_ready',
                    trigger: selectedTrigger,
                    lead: enrichedLead,
                    draft,
                });
            } catch (error) {
                console.error('Strike trigger error:', error);
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Execute delivery (called by Next.js approve endpoint)
        if (url.pathname === '/api/execute' && request.method === 'POST') {
            try {
                const body = await request.json() as { workflowId: string; action: string; senderAccounts?: string[] };

                // Resolve sender email addresses → SmartLead account IDs
                let emailAccountIds: number[] | undefined;
                if (body.senderAccounts?.length && env.SUPABASE_URL && env.SUPABASE_SERVICE_ROLE_KEY) {
                    try {
                        const emailList = body.senderAccounts.map(e => `"${e}"`).join(',');
                        const lookupRes = await fetch(
                            `${env.SUPABASE_URL}/rest/v1/crm_settings_senders?email=in.(${emailList})&select=smartlead_account_id`,
                            {
                                headers: {
                                    'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                                    'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                },
                            }
                        );
                        if (lookupRes.ok) {
                            const senders = await lookupRes.json() as any[];
                            emailAccountIds = senders
                                .map(s => parseInt(s.smartlead_account_id, 10))
                                .filter(id => !isNaN(id));
                            console.log(`📧 Resolved ${emailAccountIds.length} sender account IDs for strike ${body.workflowId}`);
                        }
                    } catch (e) {
                        console.warn('[execute] Failed to resolve sender accounts:', e);
                    }
                }

                const result = await executeCampaign(env, {
                    campaignId: 0,
                    workflowId: body.workflowId,
                    emailAccountIds,
                });
                return Response.json({ status: 'delivered', result });
            } catch (error) {
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Triage a reply (for testing)
        if (url.pathname === '/api/triage' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    senderName: string;
                    senderCompany: string;
                    subject: string;
                    body: string;
                };
                const result = await triageReply(env, body);
                return Response.json(result);
            } catch (error) {
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Debug: test pipeline inline to see what fails
        if (url.pathname === '/api/test-pipeline' && request.method === 'POST') {
            const steps: Record<string, any> = {};
            try {
                // Step A: One Exa query
                const exaRes = await fetch('https://api.exa.ai/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': env.EXA_API_KEY },
                    body: JSON.stringify({
                        query: `mergers and acquisitions announcement ${new Date().getFullYear()} financial services`,
                        numResults: 2, type: 'neural', useAutoprompt: true,
                        contents: { text: { maxCharacters: 800 }, highlights: { numSentences: 2, highlightsPerUrl: 1 } },
                    }),
                });
                const exaData = await exaRes.json() as any;
                const results = exaData.results || [];
                steps.exa = { status: exaRes.status, count: results.length, titles: results.map((r: any) => r.title) };

                if (results.length === 0) return jsonWithCors({ steps, error: 'No Exa results' });

                // Step B: Gemini extraction
                const itemsPrompt = results.map((r: any, i: number) =>
                    `[Item ${i}]\nTitle: ${r.title}\nText: ${r.text ?? r.highlights?.join(' ') ?? ''}\nURL: ${r.url}`
                ).join('\n\n');

                const geminiRes = await fetch(
                    geminiUrl(env.GEMINI_API_KEY),
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            system_instruction: { parts: [{ text: 'Extract company and executive. Return JSON array: [{"index":0,"company":"Name","executiveName":"John","executiveTitle":"CEO"}]. If no executive named, use "Unknown".' }] },
                            contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                            generationConfig: { temperature: 0.1, responseMimeType: 'application/json' },
                        }),
                    }
                );
                const geminiBody = await geminiRes.json() as any;
                const parts = geminiBody?.candidates?.[0]?.content?.parts || [];
                const rawText = parts.find((p: any) => p.text)?.text;
                steps.gemini = { status: geminiRes.status, partsCount: parts.length, rawText: rawText?.slice(0, 500) };

                if (rawText) {
                    const extracted = JSON.parse(rawText);
                    steps.extracted = extracted;

                    // Step C: Enrich first result
                    if (extracted.length > 0) {
                        const e = extracted[0];
                        const enriched = await enrichLead(env, {
                            company: e.company,
                            executiveName: e.executiveName,
                            executiveTitle: e.executiveTitle,
                        });
                        steps.enriched = { name: enriched.executiveName, title: enriched.executiveTitle, company: enriched.company };

                        // Step D: Generate draft
                        const draft = await generateDraft(env, {
                            lead: enriched,
                            persona: 'Fred polsinelli',
                            triggerHeadline: results[0].title,
                            triggerArticleText: results[0].text || results[0].highlights?.join(' ') || '',
                        });
                        steps.draft = { subject: draft.subject, bodyLen: draft.body.length };
                    }
                }

                return jsonWithCors({ success: true, steps });
            } catch (err) {
                steps.error = String(err);
                steps.stack = (err as any)?.stack?.slice(0, 300);
                return jsonWithCors({ success: false, steps }, { status: 500 });
            }
        }
        // Apollo diagnostic endpoint
        if (url.pathname === '/api/test-apollo' && request.method === 'POST') {
            try {
                const body = await request.json() as { company: string };
                const company = body.company || 'Fifth Third Bancorp';
                const diag: any = { company, searches: {} };

                // Test 1: x-api-key header auth (old method)
                const r1 = await fetch('https://api.apollo.io/v1/mixed_people/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': env.APOLLO_API_KEY },
                    body: JSON.stringify({
                        q_organization_name: company,
                        person_titles: ['CEO', 'CFO', 'President'],
                        page: 1, per_page: 3,
                    }),
                });
                diag.searches.headerAuth = { status: r1.status, statusText: r1.statusText };
                if (r1.ok) {
                    const d1 = await r1.json() as any;
                    diag.searches.headerAuth.total = d1.pagination?.total_entries;
                    diag.searches.headerAuth.people = (d1.people || []).slice(0, 3).map((p: any) => `${p.first_name} ${p.last_name} (${p.title})`);
                }

                // Test 2: api_key in body (new method)
                const r2 = await fetch('https://api.apollo.io/v1/mixed_people/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        api_key: env.APOLLO_API_KEY,
                        q_organization_name: company,
                        person_titles: ['CEO', 'CFO', 'President'],
                        page: 1, per_page: 3,
                    }),
                });
                diag.searches.bodyAuth = { status: r2.status, statusText: r2.statusText };
                if (r2.ok) {
                    const d2 = await r2.json() as any;
                    diag.searches.bodyAuth.total = d2.pagination?.total_entries;
                    diag.searches.bodyAuth.people = (d2.people || []).slice(0, 3).map((p: any) => `${p.first_name} ${p.last_name} (${p.title})`);
                }

                // Test 3: people/match endpoint with api_key in body
                const r3 = await fetch('https://api.apollo.io/v1/people/match', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        api_key: env.APOLLO_API_KEY,
                        organization_name: company,
                        title: 'CEO',
                    }),
                });
                diag.searches.peopleMatch = { status: r3.status, statusText: r3.statusText };
                if (r3.ok) {
                    const d3 = await r3.json() as any;
                    if (d3.person) {
                        diag.searches.peopleMatch.person = `${d3.person.first_name} ${d3.person.last_name} (${d3.person.title})`;
                    }
                }

                return jsonWithCors(diag);
            } catch (err) {
                return jsonWithCors({ error: String(err) }, { status: 500 });
            }
        }

        // Dispatch specific agent
        if (request.method === 'POST' && url.pathname.startsWith('/api/dispatch-agent/')) {
            try {
                const agentIdStr = url.pathname.split('/').pop();
                if (!agentIdStr) throw new Error('Missing agent ID');
                const agentId = parseInt(agentIdStr, 10);

                const { fetchRow, patchRow } = await import('./utils/supabase');
                const agentRows = await fetchRow(env, 'agents', 'id', agentId);
                if (!agentRows || agentRows.length === 0) {
                    return jsonWithCors({ error: 'Agent not found' }, { status: 404 });
                }
                const agent = agentRows[0];

                await patchRow(env, 'agents', { status: 'running' }, 'id', agent.id);

                if (env.STRIKE_QUEUE) {
                    await env.STRIKE_QUEUE.send({
                        campaignId: 0,
                        persona: agent.persona || "Rob O'Neill",
                        action: 'dispatch_agent',
                        agentId: agent.id,
                    });
                }
                return jsonWithCors({ status: 'queued', agentId: agent.id });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Search Mission — run a one-off targeted search query through the pipeline
        if (url.pathname === '/api/search-mission' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    query: string;
                    persona?: string;
                    maxResults?: number;
                    runId?: string;
                };

                if (!body.query || body.query.trim().length < 5) {
                    return jsonWithCors({ error: 'Search query too short (min 5 characters)' }, { status: 400 });
                }

                console.log(`🎯 Search mission received: "${body.query}"`);

                if (env.STRIKE_QUEUE) {
                    await env.STRIKE_QUEUE.send({
                        campaignId: 0,
                        persona: body.persona || "Rob O'Neill",
                        action: 'search_mission',
                        searchQuery: body.query,
                        maxResults: body.maxResults || 5,
                        runId: body.runId || null,
                    });
                    return jsonWithCors({ status: 'queued', query: body.query }, { status: 202 });
                }

                return jsonWithCors({ error: 'Queue not available' }, { status: 503 });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Re-enrich a lead target (Research Further)
        if (url.pathname === '/api/re-enrich' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    leadId: number;
                    company: string;
                    executiveName: string;
                    executiveTitle: string;
                };

                console.log(`🔄 Re-enrichment requested for lead ${body.leadId}: ${body.executiveName} at ${body.company}`);

                const result = await enrichLead(env, {
                    company: body.company,
                    executiveName: body.executiveName,
                    executiveTitle: body.executiveTitle,
                    leadId: body.leadId,
                });

                // ── Exa Company Intelligence Search ──
                let companyIntel: any = null;
                if (env.EXA_API_KEY) {
                    try {
                        console.log(`🕵️ Exa company intelligence search for ${body.company}...`);

                        // Search for company website, news, press releases
                        const exaRes = await fetch('https://api.exa.ai/search', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'x-api-key': env.EXA_API_KEY,
                            },
                            body: JSON.stringify({
                                query: `${body.company} company official website about`,
                                numResults: 5,
                                useAutoprompt: true,
                                contents: {
                                    text: { maxCharacters: 2000 },
                                    highlights: { numSentences: 3, highlightsPerUrl: 2 },
                                },
                            }),
                        });

                        if (exaRes.ok) {
                            const exaData = await exaRes.json() as any;
                            const results = exaData.results || [];

                            // Extract key info from results
                            const sources = results.map((r: any) => ({
                                title: r.title || '',
                                url: r.url || '',
                                text: (r.text || '').slice(0, 500),
                                highlights: r.highlights || [],
                            }));

                            // Find company website (first result that looks like company domain)
                            const companyDomain = result.companyDomain || '';
                            const websiteResult = sources.find((s: any) =>
                                s.url.includes(companyDomain) ||
                                s.url.includes(body.company.toLowerCase().replace(/[^a-z0-9]/g, ''))
                            );

                            // Combine all text for Gemini summarization
                            const allText = sources.map((s: any) => `Source: ${s.url}\n${s.text}`).join('\n\n');

                            // Summarize with Gemini
                            if (allText && env.GEMINI_API_KEY) {
                                const { GEMINI_REST_URL } = await import('./config/gemini');
                                const geminiRes = await fetch(`${GEMINI_REST_URL}?key=${env.GEMINI_API_KEY}`, {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        contents: [{
                                            role: 'user', parts: [{
                                                text: `Based on the following web search results about "${body.company}", provide a concise company intelligence summary in JSON format:

${allText}

Return ONLY a JSON object:
{
  "website": "company's official website URL",
  "description": "2-3 sentence description of what the company does",
  "keyFacts": ["fact 1", "fact 2", "fact 3"],
  "recentNews": ["news item 1", "news item 2"],
  "industry": "primary industry",
  "headquarters": "city, state if known"
}` }]
                                        }],
                                        generationConfig: { temperature: 0.2, maxOutputTokens: 1024, responseMimeType: 'application/json' },
                                    }),
                                });

                                if (geminiRes.ok) {
                                    const geminiData = await geminiRes.json() as any;
                                    const parts = geminiData?.candidates?.[0]?.content?.parts || [];
                                    const nonThought = parts.filter((p: any) => p.text && !p.thought);
                                    const rawText = nonThought.length > 0 ? nonThought[nonThought.length - 1].text : parts.find((p: any) => p.text)?.text;
                                    if (rawText) {
                                        try {
                                            companyIntel = JSON.parse(rawText.trim());
                                            console.log(`✅ Company intel summarized: ${companyIntel.description?.slice(0, 60)}...`);
                                        } catch {
                                            console.warn('⚠️ Failed to parse Gemini company intel JSON');
                                        }
                                    }
                                }
                            }

                            // Add source links
                            if (companyIntel) {
                                companyIntel.sources = sources.map((s: any) => ({ title: s.title, url: s.url }));
                                companyIntel.website = companyIntel.website || websiteResult?.url || '';
                            }
                        }
                    } catch (err) {
                        console.warn('⚠️ Exa company research failed:', err);
                    }
                }

                return jsonWithCors({
                    status: 'completed',
                    email: result.email || null,
                    emailSource: result.emailSource || null,
                    emailConfidence: result.emailConfidence || 'none',
                    phone: result.phone || null,
                    linkedinUrl: result.linkedinUrl || null,
                    companyDomain: result.companyDomain || null,
                    companyRevenue: result.companyRevenue || null,
                    employeeCount: result.employeeCount || null,
                    executiveName: result.executiveName || null,
                    executiveTitle: result.executiveTitle || null,
                    executiveResearch: result.executiveResearch || null,
                    patternEmails: result.patternEmails || [],
                    otherContacts: (result.otherContacts || []).map((c: any) => ({
                        name: c.name,
                        title: c.title,
                        email: c.email,
                        phone: c.phone,
                        linkedinUrl: c.linkedinUrl || c.linkedin_url,
                        seniority: c.seniority,
                    })),
                    companyIntel: companyIntel || null,
                });
            } catch (error) {
                console.error('[re-enrich] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // ── R2 File Storage Endpoints ──
        // Upload: PUT /api/r2/upload  (X-R2-Key header specifies storage key)
        if (url.pathname === '/api/r2/upload' && request.method === 'PUT') {
            try {
                if (!env.CRM_ATTACHMENTS) return jsonWithCors({ error: 'R2 storage not configured. Enable R2 in Cloudflare dashboard.' }, { status: 503 });
                const key = request.headers.get('X-R2-Key');
                if (!key) return jsonWithCors({ error: 'Missing X-R2-Key header' }, { status: 400 });

                const contentType = request.headers.get('Content-Type') || 'application/octet-stream';
                const body = await request.arrayBuffer();

                await env.CRM_ATTACHMENTS.put(key, body, {
                    httpMetadata: { contentType },
                });

                const fileUrl = `${url.origin}/api/r2/file/${key}`;
                return jsonWithCors({ status: 'uploaded', url: fileUrl, key });
            } catch (error) {
                console.error('[R2 upload] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Get file: GET /api/r2/file/*
        if (url.pathname.startsWith('/api/r2/file/') && request.method === 'GET') {
            try {
                if (!env.CRM_ATTACHMENTS) return jsonWithCors({ error: 'R2 storage not configured' }, { status: 503 });
                const key = url.pathname.replace('/api/r2/file/', '');
                const object = await env.CRM_ATTACHMENTS.get(key);
                if (!object) return jsonWithCors({ error: 'Not found' }, { status: 404 });

                const headers = new Headers(corsHeaders);
                headers.set('Content-Type', object.httpMetadata?.contentType || 'application/octet-stream');
                headers.set('Content-Disposition', `inline; filename="${key.split('/').pop()}"`);
                headers.set('Cache-Control', 'public, max-age=3600');

                return new Response(object.body, { headers });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Delete file: DELETE /api/r2/file/*
        if (url.pathname.startsWith('/api/r2/file/') && request.method === 'DELETE') {
            try {
                if (!env.CRM_ATTACHMENTS) return jsonWithCors({ error: 'R2 storage not configured' }, { status: 503 });
                const key = url.pathname.replace('/api/r2/file/', '');
                await env.CRM_ATTACHMENTS.delete(key);
                return jsonWithCors({ status: 'deleted' });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Regenerate or Deep Research trigger
        if (url.pathname === '/api/regenerate' && request.method === 'POST') {
            try {
                const body = await request.json() as { workflowId: string; action: 'regenerate' | 'research'; steeringNotes?: string };
                await env.STRIKE_QUEUE.send({
                    campaignId: 0,
                    workflowId: body.workflowId,
                    action: body.action,
                    persona: "Rob O'Neill",
                    steeringNotes: body.steeringNotes || null,
                });
                return jsonWithCors({ status: 'queued' });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // ── Generate Variants from Gold Draft ──────────────────────────────
        if (url.pathname === '/api/generate-variants' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    goldDraft: { name: string; category: string; subject: string; body: string };
                    reporters: { name: string; email?: string; outlet?: string; beat?: string; recentWork?: string }[];
                    messagingDirectives: { title: string; directive: string }[];
                    persona?: string;
                };

                if (!body.goldDraft || !body.reporters?.length) {
                    return jsonWithCors({ error: 'goldDraft and reporters[] are required' }, { status: 400 });
                }

                const apiKey = env.GEMINI_API_KEY;
                if (!apiKey) {
                    return jsonWithCors({ error: 'GEMINI_API_KEY not configured' }, { status: 500 });
                }

                // Fetch system settings for persona/signature
                let settings: any = null;
                try {
                    const settingsRows = await fetchRow(env, 'system_settings', 'id', 1);
                    if (settingsRows.length > 0) settings = settingsRows[0];
                } catch { /* use defaults */ }

                const senderName = body.persona || settings?.default_sender_name || 'Fred Polsinelli';
                const senderTitle = settings?.default_sender_title || 'CEO';
                const companyName = settings?.company_name || 'Polsinelli Public Affairs, LLC';
                const signatureBlock = `${senderName}\n${senderTitle}\n${companyName}`;

                const { geminiUrl } = await import('./config/gemini');
                const gemUrl = geminiUrl(apiKey);

                const variants: any[] = [];
                const batchId = `batch-${crypto.randomUUID().slice(0, 12)}`;

                // Process reporters sequentially (avoid rate limits)
                for (const reporter of body.reporters.slice(0, 20)) {
                    try {
                        const prompt = `You are ${senderName}, ${senderTitle} at ${companyName}.

FOUNDATION DRAFT (study this email's tone, style, strategic framing, and relationship-building approach):
Subject: ${body.goldDraft.subject}
Body:
${body.goldDraft.body}

${body.messagingDirectives.length > 0 ? `DAILY MESSAGING PRIORITIES (weave these into the email naturally):
${body.messagingDirectives.map((d, i) => `${i + 1}. [${d.title}] ${d.directive}`).join('\n')}
` : ''}
TARGET REPORTER:
Name: ${reporter.name}
${reporter.outlet ? `Outlet: ${reporter.outlet}` : ''}
${reporter.beat ? `Beat/Coverage: ${reporter.beat}` : ''}
${reporter.recentWork ? `Recent Work: ${reporter.recentWork}` : ''}

TASK:
Generate a customized version of the foundation draft for this specific reporter. Preserve the original's:
- Voice, tone, and strategic sophistication
- Core messaging and value propositions
- Relationship-building approach

But ADAPT:
- The opening hook to reference the reporter's specific beat or recent work
- The angle to align with what this reporter typically covers
- Any daily messaging priorities that naturally fit
- The call-to-action to be relevant to their coverage area

RULES:
1. Keep the same length and structure as the foundation draft
2. Sound like a thoughtful, experienced professional — NOT an automated system
3. Maximum 3-4 SHORT paragraphs. No bullet points. No HTML. No bold text. No emojis.
4. Address them by first name: "${reporter.name.split(' ')[0]},"
5. End with ONLY this signature block:
Best,
${signatureBlock}

Return ONLY valid JSON: {"subject": "...", "body": "..."}`;

                        const geminiRes = await fetch(gemUrl, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                contents: [{ parts: [{ text: prompt }] }],
                                generationConfig: {
                                    temperature: 0.8,
                                    maxOutputTokens: 2000,
                                    responseMimeType: 'application/json',
                                },
                            }),
                        });

                        if (!geminiRes.ok) {
                            console.error(`❌ Gemini error for ${reporter.name}:`, await geminiRes.text());
                            variants.push({
                                reporter: reporter.name,
                                email: reporter.email,
                                outlet: reporter.outlet,
                                error: 'Gemini generation failed',
                            });
                            continue;
                        }

                        const geminiData = await geminiRes.json() as any;
                        const rawText = geminiData?.candidates?.[0]?.content?.parts?.[0]?.text || '';
                        let parsed: { subject: string; body: string };

                        try {
                            const cleaned = rawText.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
                            parsed = JSON.parse(cleaned);
                        } catch {
                            // Fallback: use raw text as body
                            parsed = { subject: body.goldDraft.subject, body: rawText };
                        }

                        variants.push({
                            reporter: reporter.name,
                            email: reporter.email,
                            outlet: reporter.outlet,
                            beat: reporter.beat,
                            subject: parsed.subject,
                            body: parsed.body,
                            batchId,
                        });

                        console.log(`✅ Variant generated for ${reporter.name} (${reporter.outlet || 'unknown outlet'})`);
                    } catch (reporterErr) {
                        console.error(`❌ Error generating variant for ${reporter.name}:`, reporterErr);
                        variants.push({
                            reporter: reporter.name,
                            email: reporter.email,
                            error: String(reporterErr),
                        });
                    }
                }

                return jsonWithCors({
                    status: 'complete',
                    batchId,
                    total: body.reporters.length,
                    generated: variants.filter(v => !v.error).length,
                    variants,
                });
            } catch (error) {
                console.error('❌ Generate variants error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Smartlead webhook — inbound reply receiver
        if (url.pathname === '/api/webhook/smartlead' && request.method === 'POST') {
            // Parse the payload synchronously
            const body = await request.json() as {
                campaign_id?: number | string;
                from_email?: string;
                from_name?: string;
                from_company?: string;
                subject?: string;
                text_body?: string;
                html_body?: string;
            };

            // Parse campaign_id as BIGSERIAL number
            const campaignId = typeof body.campaign_id === 'string'
                ? parseInt(body.campaign_id, 10)
                : body.campaign_id ?? 0;

            const senderName = body.from_name || body.from_email || 'Unknown';
            const senderCompany = body.from_company || 'Unknown';
            const subject = body.subject || '(no subject)';
            const replyBody = body.text_body || body.html_body || '';

            // Return 200 IMMEDIATELY — defer processing via waitUntil
            ctx.waitUntil(
                (async () => {
                    try {
                        const triageResult = await triageReply(env, {
                            senderName,
                            senderCompany,
                            subject,
                            body: replyBody,
                        });

                        // Look up the originating strike campaign for bucketing
                        let strategicCampaignId: string | null = null;
                        let assignedTo: string | null = null;
                        if (campaignId > 0) {
                            const { fetchRow } = await import('./utils/supabase');
                            const scRows = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                            if (scRows && scRows.length > 0) {
                                strategicCampaignId = scRows[0].campaign_id || null;
                                assignedTo = scRows[0].assigned_to || null;
                            }
                        }

                        // Write to triage_replies table with campaign bucketing
                        const insertResult = await insertRow(env, 'triage_replies', {
                            campaign_id: campaignId > 0 ? campaignId : null,
                            strategic_campaign_id: strategicCampaignId,
                            assigned_to: assignedTo,
                            sender_name: senderName,
                            sender_company: senderCompany,
                            subject,
                            body: replyBody,
                            category: triageResult.category,
                            confidence: triageResult.confidence,
                            preview: replyBody.slice(0, 200),
                        });

                        if (insertResult.ok) {
                            console.log(`✅ Webhook processed: ${senderName} → ${triageResult.category}${strategicCampaignId ? ` (Campaign ${strategicCampaignId})` : ''}`);
                        }

                        // Mark campaign as 'replied'
                        if (campaignId > 0) {
                            const { patchRow } = await import('./utils/supabase');
                            await patchRow(env, 'strike_campaigns', { status: 'replied' }, 'id', campaignId);
                        }
                    } catch (err) {
                        console.error('❌ Webhook background processing error:', err);
                    }
                })()
            );

            // Smartlead gets 200 instantly — no timeout risk
            return Response.json({ status: 'accepted' });
        }

        // ── Temporary Diagnostic: Test full generateDraft pipeline ──────
        if (url.pathname === '/api/test-draft' && request.method === 'POST') {
            try {
                const body = await request.json() as { workflowId?: string };
                if (!body.workflowId) return jsonWithCors({ error: 'Missing workflowId' }, { status: 400 });

                const { fetchRow } = await import('./utils/supabase');

                // Fetch campaign
                const campaignRows = await fetchRow(env, 'strike_campaigns', 'workflow_id', body.workflowId);
                if (!campaignRows?.length) return jsonWithCors({ error: 'Campaign not found' });
                const campaign = campaignRows[0];

                // Fetch lead target
                const targetRows = await fetchRow(env, 'lead_targets', 'id', campaign.target_id);
                if (!targetRows?.length) return jsonWithCors({ error: 'Target not found' });
                const lead = targetRows[0];

                // Build enriched lead from existing data (skip Apollo)
                const existingEnrichment = lead.enrichment_data || {};
                const enrichedLead = {
                    company: lead.company,
                    executiveName: lead.executive_name || 'Unknown',
                    executiveTitle: lead.executive_title || 'Unknown',
                    companyDomain: existingEnrichment.company_domain || '',
                    companyRevenue: existingEnrichment.revenue || null,
                    employeeCount: existingEnrichment.employees || null,
                    signals: existingEnrichment.signals || [],
                    linkedinUrl: existingEnrichment.linkedin_url || null,
                    email: existingEnrichment.email || '',
                    executiveResearch: existingEnrichment.executive_research || '',
                };

                const triggerArticleText = existingEnrichment.trigger_summary || '';

                // Run generateDraft
                const { generateDraft } = await import('./activities/generate-draft');
                const draft = await generateDraft(env, {
                    lead: enrichedLead,
                    persona: 'Fred Polsinelli',
                    triggerHeadline: lead.trigger_event,
                    triggerArticleText,
                    partnerProfiles: [],
                });

                return jsonWithCors({
                    success: true,
                    isFallback: draft.body.includes('This kind of transition opens a rare window'),
                    fallbackReason: (draft as any).__fallbackReason || null,
                    subject: draft.subject,
                    bodyPreview: draft.body.slice(0, 300),
                    modelUsed: draft.modelUsed,
                    confidence: draft.confidenceScore,
                });
            } catch (err) {
                return jsonWithCors({ error: String(err), stack: (err as any)?.stack?.slice(0, 500) }, { status: 500 });
            }
        }

        return Response.json({ error: 'Not found' }, { status: 404 });
    },

    /**
     * Queue handler — processes strike pipeline messages asynchronously.
     * This prevents HTTP request timeouts when AI APIs take 15+ seconds.
     */
    async queue(batch: MessageBatch<any>, env: Env): Promise<void> {
        for (const msg of batch.messages) {
            try {
                // ── Handle custom_strike from Copilot ──
                if (msg.body?.type === 'custom_strike') {
                    const { target_company, context } = msg.body;
                    console.log(`🎯 Custom strike dispatched for: ${target_company}`);
                    // For now, log and acknowledge — full pipeline integration is a future phase
                    console.log(`   Context: ${context || 'none'}`);
                    console.log(`   → Ad-hoc pipeline would run: Exa sense → Apollo enrich → Gemini draft`);
                    msg.ack();
                    continue;
                }

                const { campaignId, persona, workflowId: regenerateWorkflowId, action, steeringNotes } = msg.body;
                console.log(`📨 Queue processing: Campaign #${campaignId} | Persona: ${persona} | Action: ${action || 'new'}`);

                // Helper to gracefully reset agent status at the end of the pipeline run
                const resetAgentStatus = async (env: Env, agentId: number) => {
                    try {
                        const { patchRow } = await import('./utils/supabase');
                        await patchRow(env, 'agents', { status: 'active', last_activity: new Date().toISOString() }, 'id', agentId);
                        console.log(`🔄 Reset Agent #${agentId} to 'active' state.`);
                    } catch (e) {
                        console.error(`❌ Failed to reset agent state for #${agentId}`, e);
                    }
                };

                if (action === 'regenerate' || action === 'research') {
                    if (!regenerateWorkflowId) {
                        console.error('Missing workflowId for regenerate action');
                        msg.ack();
                        continue;
                    }

                    const { fetchRow, patchRow } = await import('./utils/supabase');
                    const campaignRows = await fetchRow(env, 'strike_campaigns', 'workflow_id', regenerateWorkflowId);
                    if (!campaignRows || campaignRows.length === 0) {
                        console.error('Campaign not found for regeneration');
                        msg.ack();
                        continue;
                    }
                    const campaign = campaignRows[0];
                    const targetRows = await fetchRow(env, 'lead_targets', 'id', campaign.target_id);
                    if (!targetRows || targetRows.length === 0) {
                        console.error('Target not found for regeneration');
                        msg.ack();
                        continue;
                    }
                    const lead = targetRows[0];

                    // Try to re-enrich but fall back to existing data if Apollo fails/times out
                    let enrichedLead: any;
                    try {
                        enrichedLead = await enrichLead(env, {
                            company: lead.company,
                            executiveName: lead.executive_name,
                            executiveTitle: lead.executive_title,
                            leadId: lead.id,
                        });

                        // Update the lead_targets row with the fresh enrichment data
                        if (enrichedLead.executiveName && enrichedLead.executiveName !== 'Key Decision-Maker') {
                            const { patchRow: patchLead } = await import('./utils/supabase');
                            await patchLead(env, 'lead_targets', {
                                executive_name: enrichedLead.executiveName,
                                executive_title: enrichedLead.executiveTitle,
                            }, 'id', lead.id);
                        }
                    } catch (enrichErr) {
                        console.warn('⚠️ Re-enrichment failed, using existing lead data:', enrichErr);
                        // Fall back to existing lead data
                        const existingEnrichment = lead.enrichment_data || {};
                        enrichedLead = {
                            company: lead.company,
                            executiveName: lead.executive_name || 'Unknown',
                            executiveTitle: lead.executive_title || 'Unknown',
                            companyRevenue: existingEnrichment.revenue || null,
                            employeeCount: existingEnrichment.employees || null,
                            signals: existingEnrichment.signals || [],
                            linkedinUrl: existingEnrichment.linkedin_url || null,
                            email: existingEnrichment.email || '',
                            executiveResearch: existingEnrichment.executive_research || '',
                        };
                    }

                    // Extract trigger article text from stored enrichment for better context
                    const triggerArticleText = lead.enrichment_data?.trigger_summary || '';

                    // Fetch system settings for the sender persona
                    const { getRow } = await import('./utils/supabase');
                    const { data: settings } = await getRow(env, 'system_settings', 1);
                    const senderName = settings?.default_sender_name || persona || "Rob O'Neill";

                    // Fetch Partner Profiles (M:N) if this strike belongs to a strategic campaign
                    let partnerProfiles: any[] = [];
                    let campaignObjective: string | undefined = undefined;
                    if (campaign.campaign_id) {
                        const { fetchRow } = await import('./utils/supabase');
                        // Query campaign_partners join table
                        const cpUrl = `${env.SUPABASE_URL}/rest/v1/campaign_partners?campaign_id=eq.${campaign.campaign_id}&select=company_id`;
                        const cpRes = await fetch(cpUrl, {
                            headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` },
                        });
                        const cpRows = cpRes.ok ? await cpRes.json() as any[] : [];
                        for (const cp of cpRows) {
                            const pRows = await fetchRow(env, 'crm_companies', 'id', cp.company_id);
                            if (pRows && pRows.length > 0) partnerProfiles.push(pRows[0]);
                        }

                        // Fetch the campaign objective
                        const campRows = await fetchRow(env, 'campaigns', 'id', campaign.campaign_id);
                        if (campRows && campRows.length > 0) campaignObjective = campRows[0].objective || undefined;
                    }

                    const draft = await generateDraft(env, {
                        lead: enrichedLead,
                        persona: senderName,
                        triggerHeadline: lead.trigger_event,
                        triggerArticleText,
                        partnerProfiles,
                        steeringNotes: steeringNotes || campaignObjective || undefined,
                    });

                    await patchRow(env, 'strike_campaigns', {
                        email_subject: draft.subject,
                        drafted_body: draft.body,
                        persona_used: senderName,
                        status: 'pending_hitl'
                    }, 'workflow_id', regenerateWorkflowId);

                    console.log(`✅ Regenerated Draft for Workflow #${regenerateWorkflowId}`);
                    msg.ack();
                    continue;
                }

                let triggers = [];
                let strategicCampaignId: string | null = null;
                let partnerProfiles: any[] = [];
                let campaignObjective: string | undefined = undefined;

                if (action === 'dispatch_agent' && msg.body.agentId) {
                    const { fetchRow, patchRow } = await import('./utils/supabase');
                    const agentRows = await fetchRow(env, 'agents', 'id', msg.body.agentId);
                    if (!agentRows || agentRows.length === 0) {
                        console.error(`Agent ${msg.body.agentId} not found for dispatch`);
                        msg.ack();
                        continue;
                    }
                    const agent = agentRows[0];

                    // Create a pipeline_run record for agent dispatches so results are visible in the UI
                    if (!msg.body.runId) {
                        try {
                            const runRes = await insertRow(env, 'pipeline_runs', {
                                run_type: 'agent_dispatch',
                                agent_name: agent.name || `Agent #${agent.id}`,
                                status: 'running',
                                triggered_by: 'manual',
                                metadata: { agent_id: agent.id, domain: agent.domain },
                                started_at: new Date().toISOString(),
                            });
                            if (runRes?.ok && runRes.data?.[0]) msg.body.runId = runRes.data[0].id;
                        } catch (_) { /* non-blocking */ }
                    }

                    // M:N: Query campaign_agents for all campaign assignments
                    const caUrl = `${env.SUPABASE_URL}/rest/v1/campaign_agents?agent_id=eq.${msg.body.agentId}&select=campaign_id`;
                    const caRes = await fetch(caUrl, {
                        headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` },
                    });
                    const caRows = caRes.ok ? await caRes.json() as any[] : [];

                    if (caRows.length > 0) {
                        // Use first campaign assignment for this run
                        strategicCampaignId = caRows[0].campaign_id;

                        // Fetch all partners for this campaign via campaign_partners
                        const cpUrl = `${env.SUPABASE_URL}/rest/v1/campaign_partners?campaign_id=eq.${strategicCampaignId}&select=company_id`;
                        const cpRes = await fetch(cpUrl, {
                            headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` },
                        });
                        const cpRows = cpRes.ok ? await cpRes.json() as any[] : [];
                        for (const cp of cpRows) {
                            const pRows = await fetchRow(env, 'crm_companies', 'id', cp.company_id);
                            if (pRows && pRows.length > 0) partnerProfiles.push(pRows[0]);
                        }

                        // Fetch the campaign objective
                        if (strategicCampaignId) {
                            const campRows = await fetchRow(env, 'campaigns', 'id', strategicCampaignId);
                            if (campRows && campRows.length > 0) campaignObjective = campRows[0].objective || undefined;
                        }

                        console.log(`📋 Agent #${msg.body.agentId} → Campaign ${strategicCampaignId} with ${partnerProfiles.length} partners`);
                    }

                    triggers = await senseTriggersForAgent(env, agent);

                    if (triggers.length === 0) {
                        // Exa returned nothing — try secondary sources as fallback
                        console.log(`⚠️ Exa returned 0 for Agent #${agent.id}, trying secondary sources...`);
                        const [secResult, courtResult, newsResult] = await Promise.allSettled([
                            senseSecFilings(env),
                            senseCourtFilings(env),
                            senseNews(env),
                        ]);
                        const fallbackTriggers: MarketTrigger[] = [];
                        const sourceStatus: Record<string, string> = {};
                        for (const [label, result] of [
                            ['SEC', secResult],
                            ['Court', courtResult],
                            ['News', newsResult],
                        ] as [string, PromiseSettledResult<MarketTrigger[]>][]) {
                            if (result.status === 'fulfilled') {
                                fallbackTriggers.push(...result.value);
                                sourceStatus[label] = `${result.value.length} results`;
                            } else {
                                sourceStatus[label] = 'failed';
                                console.warn(`⚠️ Fallback ${label} failed:`, result.reason);
                            }
                        }
                        triggers = deduplicateTriggers(fallbackTriggers);
                        console.log(`📡 Fallback: ${fallbackTriggers.length} raw → ${triggers.length} unique from secondary sources`);

                        if (triggers.length === 0) {
                            // Truly nothing from any source
                            await resetAgentStatus(env, agent.id);
                            console.log(`⚠️ No triggers from any source for Agent #${agent.id}`);
                            if (msg.body.runId) {
                                try {
                                    await patchRow(env, 'pipeline_runs', {
                                        status: 'completed',
                                        triggers_found: 0,
                                        leads_enriched: 0,
                                        drafts_generated: 0,
                                        completed_at: new Date().toISOString(),
                                        metadata: {
                                            agent_id: agent.id,
                                            result: 'no_triggers',
                                            exa_query: agent.exa_query,
                                            fallback_attempted: true,
                                            fallback_sources: sourceStatus,
                                        },
                                    }, 'id', msg.body.runId);
                                } catch (_) { /* non-blocking */ }
                            }
                            msg.ack();
                            continue;
                        }
                        console.log(`✅ Fallback recovered ${triggers.length} triggers from secondary sources`);
                    }
                } else if (action === 'search_mission' && msg.body.searchQuery) {
                    // Search Mission: fan out to ALL 4 sources in parallel
                    console.log(`🎯 Processing search mission: "${msg.body.searchQuery}"`);
                    const searchQuery = msg.body.searchQuery;
                    const virtualAgent = {
                        id: 0,
                        name: 'Search Mission',
                        exa_query: searchQuery,
                        max_leads_per_run: msg.body.maxResults || 5,
                    };

                    // Fan out to all sources in parallel with per-source error capture
                    const sourceNames = ['Exa.ai', 'SEC EDGAR', 'CourtListener', 'News'];
                    const [exaResult, secResult, courtResult, newsResult] = await Promise.allSettled([
                        senseTriggersForAgent(env, virtualAgent),
                        senseSecFilingsForQuery(env, searchQuery),
                        senseCourtFilingsForQuery(env, searchQuery),
                        senseNewsForQuery(env, searchQuery),
                    ]);

                    // Log per-source status for diagnostics
                    const sourceResults = [exaResult, secResult, courtResult, newsResult];
                    const sourceStatus: Record<string, string> = {};
                    const sourceErrors: string[] = [];
                    sourceResults.forEach((r, i) => {
                        if (r.status === 'fulfilled') {
                            sourceStatus[sourceNames[i]] = `${r.value.length} results`;
                        } else {
                            sourceStatus[sourceNames[i]] = 'failed';
                            sourceErrors.push(`${sourceNames[i]}: ${String(r.reason).slice(0, 200)}`);
                            console.error(`❌ ${sourceNames[i]} failed:`, r.reason);
                        }
                    });
                    console.log(`📡 Source results:`, JSON.stringify(sourceStatus));

                    // Check if a campaign was assigned to this mission
                    if (msg.body.campaignId) {
                        strategicCampaignId = msg.body.campaignId;
                        const { fetchRow } = await import('./utils/supabase');

                        // Fetch the campaign objective
                        const campRows = await fetchRow(env, 'campaigns', 'id', msg.body.campaignId);
                        if (campRows && campRows.length > 0) campaignObjective = campRows[0].objective || undefined;

                        // Fetch all partners for this campaign via campaign_partners
                        const cpUrl = `${env.SUPABASE_URL}/rest/v1/campaign_partners?campaign_id=eq.${msg.body.campaignId}&select=company_id`;
                        const cpRes = await fetch(cpUrl, {
                            headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` },
                        });
                        const cpRows = cpRes.ok ? await cpRes.json() as any[] : [];
                        for (const cp of cpRows) {
                            const pRows = await fetchRow(env, 'crm_companies', 'id', cp.company_id);
                            if (pRows && pRows.length > 0) partnerProfiles.push(pRows[0]);
                        }
                        console.log(`📋 Search Mission → Campaign ${msg.body.campaignId} with ${partnerProfiles.length} partners attached.`);
                    }

                    // Merge all successful results
                    const allTriggers: MarketTrigger[] = [];
                    for (const result of sourceResults) {
                        if (result.status === 'fulfilled') allTriggers.push(...result.value);
                    }

                    // Deduplicate across sources
                    triggers = deduplicateTriggers(allTriggers);
                    console.log(`📡 Mission found ${allTriggers.length} raw → ${triggers.length} unique triggers across all sources`);

                    if (triggers.length === 0) {
                        console.log(`⚠️ No triggers found for search mission`);
                        if (msg.body.runId) {
                            await patchRow(env, 'pipeline_runs', {
                                status: 'completed',
                                triggers_found: 0,
                                leads_enriched: 0,
                                drafts_generated: 0,
                                completed_at: new Date().toISOString(),
                                metadata: {
                                    query: searchQuery,
                                    result: 'no_triggers',
                                    sources_checked: 4,
                                    source_status: sourceStatus,
                                    ...(sourceErrors.length > 0 ? { source_errors: sourceErrors } : {}),
                                },
                            }, 'id', msg.body.runId);
                        }
                        msg.ack();
                        continue;
                    }
                } else if (action === 'process_external_trigger' && msg.body.trigger) {
                    // External trigger from additional sources (SEC, Court, News)
                    const t = msg.body.trigger as MarketTrigger;
                    console.log(`📡 Processing external trigger: ${t.company} (${t.source})`);
                    triggers = [t];
                } else {
                    // Step 1: Sense market triggers (Global fallback if no agent specified)
                    triggers = await senseTriggers(env);
                    if (triggers.length === 0) {
                        console.log(`⚠️ No global triggers found for Campaign #${campaignId}`);
                        msg.ack();
                        continue;
                    }
                }

                // Process ALL non-duplicate triggers (not just the first)
                const newTriggers = [];
                for (const t of triggers) {
                    const existing = await fetchRow(env, 'lead_targets', 'company', t.company);
                    if (existing.length === 0) {
                        newTriggers.push(t);
                    } else {
                        console.log(`🔄 Skipping duplicate: ${t.company}`);
                    }
                }

                if (newTriggers.length === 0) {
                    console.log(`⚠️ All ${triggers.length} triggers already have leads — skipping.`);
                    if (action === 'dispatch_agent' && msg.body.agentId) {
                        await resetAgentStatus(env, msg.body.agentId);
                    }
                    msg.ack();
                    continue;
                }

                console.log(`🎯 Processing ${newTriggers.length} new triggers out of ${triggers.length} total`);

                for (const selectedTrigger of newTriggers) {
                    try {
                        // Step 2: Enrich lead via Gemini + Apollo + Exa
                        const enrichedLead = await enrichLead(env, {
                            company: selectedTrigger.company,
                            executiveName: selectedTrigger.executiveName,
                            executiveTitle: selectedTrigger.executiveTitle,
                        });

                        // Step 2.5: Compliance & Suppression Check
                        let isSuppressed = false;
                        let suppressionReason = '';
                        if (enrichedLead.email || enrichedLead.companyDomain) {
                            const { fetchRow } = await import('./utils/supabase');

                            if (enrichedLead.email) {
                                const emailMatches = await fetchRow(env, 'suppression_list', 'email', enrichedLead.email);
                                if (emailMatches && emailMatches.length > 0) {
                                    isSuppressed = true;
                                    suppressionReason = emailMatches[0].reason;
                                }
                            }

                            if (!isSuppressed && enrichedLead.companyDomain) {
                                const domainMatches = await fetchRow(env, 'suppression_list', 'domain', enrichedLead.companyDomain);
                                if (domainMatches && domainMatches.length > 0) {
                                    isSuppressed = true;
                                    suppressionReason = domainMatches[0].reason;
                                }
                            }
                        }

                        // Step 3: Generate personalized email draft via Gemini
                        let draft = { subject: 'Suppressed Contact', body: `This contact was flagged against the suppression list. Reason: ${suppressionReason || 'Unknown'}` };
                        if (!isSuppressed) {
                            draft = await generateDraft(env, {
                                lead: enrichedLead,
                                persona: persona || "Rob O'Neill",
                                triggerHeadline: selectedTrigger.headline,
                                triggerArticleText: selectedTrigger.articleText || '',
                                partnerProfiles,
                                steeringNotes: campaignObjective || undefined,
                            });
                        }

                        // Step 4: Save lead + campaign to Supabase
                        const workflowId = `wf-${crypto.randomUUID().slice(0, 12)}`;

                        const currentAgentId = (selectedTrigger as any).agentId || msg.body.agentId || null;
                        const targetRes = await insertRow(env, 'lead_targets', {
                            company: enrichedLead.company || selectedTrigger.company,
                            executive_name: enrichedLead.executiveName || selectedTrigger.executiveName,
                            executive_title: enrichedLead.executiveTitle || selectedTrigger.executiveTitle,
                            trigger_event: selectedTrigger.headline,
                            trigger_source: selectedTrigger.sourceUrl || null,
                            trigger_relevance: selectedTrigger.relevanceScore || 95,
                            discovered_by_agent: currentAgentId,
                            enrichment_data: {
                                data_source: selectedTrigger.source || 'Exa.ai',
                                revenue: enrichedLead.companyRevenue,
                                employees: enrichedLead.employeeCount,
                                signals: enrichedLead.signals,
                                linkedin_url: enrichedLead.linkedinUrl,
                                email: enrichedLead.email || '',
                                phone: enrichedLead.phone || '',
                                executive_research: enrichedLead.executiveResearch || '',
                                trigger_summary: selectedTrigger.articleText || '',
                                sources: [
                                    ...(selectedTrigger.sourceUrl ? [{ label: 'Trigger Source', url: selectedTrigger.sourceUrl }] : []),
                                    ...(enrichedLead.linkedinUrl ? [{ label: 'LinkedIn Profile', url: enrichedLead.linkedinUrl }] : []),
                                ],
                                source: 'gemini+apollo+exa',
                                enriched_at: new Date().toISOString(),
                                // Mission alignment metadata (set when strike comes from a search mission)
                                ...(msg.body.missionId ? {
                                    mission_aligned: true,
                                    mission_id: msg.body.missionId,
                                    mission_name: msg.body.missionName || 'Search Mission',
                                    territory_id: msg.body.territoryId || null,
                                    territory_name: msg.body.territoryName || null,
                                } : {}),
                            },
                        });

                        if (targetRes.ok && targetRes.data && targetRes.data.length > 0) {
                            const targetId = targetRes.data[0].id;
                            const campaignRes = await insertRow(env, 'strike_campaigns', {
                                target_id: targetId,
                                status: isSuppressed ? 'suppressed' : 'pending_hitl',
                                persona_used: persona,
                                email_subject: draft.subject,
                                drafted_body: draft.body,
                                workflow_id: workflowId,
                                campaign_id: strategicCampaignId,
                                agent_id: currentAgentId,
                            });

                            // Initialize HITL Gate only if not suppressed
                            if (!isSuppressed && campaignRes.ok && campaignRes.data?.[0]?.id) {
                                try {
                                    const gateId = env.HITL_GATE.idFromName(workflowId);
                                    const gate = env.HITL_GATE.get(gateId);
                                    await gate.fetch(new Request('https://hitl/init', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({
                                            campaignId: campaignRes.data[0].id,
                                            workflowId,
                                            persona,
                                            draftSubject: draft.subject,
                                            draftBody: draft.body,
                                        }),
                                    }));
                                } catch (gateErr) {
                                    console.warn('⚠️ HITL Gate init failed (non-blocking):', gateErr);
                                }
                            }

                            console.log(`✅ Lead #${targetId} created: ${enrichedLead.executiveName} @ ${enrichedLead.company} — "${draft.subject}"`);

                            // Update agent stats immediately
                            const agentId = (selectedTrigger as any).agentId || 1;
                            try {
                                const agentRow = await fetchRow(env, 'agents', 'id', agentId);
                                if (agentRow.length > 0) {
                                    const a = agentRow[0] as any;
                                    await patchRow(env, 'agents', {
                                        triggers_submitted: (a.triggers_submitted || 0) + 1,
                                        drafts_generated: (a.drafts_generated || 0) + 1,
                                        active_pipelines: 1,
                                        status: 'active',
                                        last_activity: new Date().toISOString(),
                                    }, 'id', agentId);
                                }
                            } catch (_) { /* non-blocking */ }
                        }

                        if (action === 'dispatch_agent' && (selectedTrigger as any).agentId) {
                            await resetAgentStatus(env, (selectedTrigger as any).agentId);
                        }
                    } catch (triggerErr) {
                        console.error(`❌ Failed to process trigger for ${selectedTrigger.company}:`, triggerErr);
                    }
                }

                console.log(`✅ Pipeline complete: ${newTriggers.length} triggers processed`);

                // Update pipeline_runs record if this was a search mission
                if (action === 'search_mission' && msg.body.runId) {
                    try {
                        await patchRow(env, 'pipeline_runs', {
                            status: 'completed',
                            triggers_found: triggers.length,
                            leads_enriched: newTriggers.length,
                            drafts_generated: newTriggers.length,
                            completed_at: new Date().toISOString(),
                            metadata: {
                                ...(msg.body.missionId ? {
                                    mission_id: msg.body.missionId,
                                    mission_name: msg.body.missionName,
                                    territory_id: msg.body.territoryId,
                                    territory_name: msg.body.territoryName,
                                } : {}),
                                query: msg.body.searchQuery,
                                triggers_total: triggers.length,
                                triggers_new: newTriggers.length,
                                triggers_deduplicated: triggers.length - newTriggers.length,
                                sources_checked: 4,
                            },
                        }, 'id', msg.body.runId);

                        // Also update saved_searches results_count
                        if (msg.body.missionId) {
                            await patchRow(env, 'saved_searches', {
                                results_count: newTriggers.length,
                            }, 'id', msg.body.missionId);
                        }
                    } catch (_) { /* non-blocking */ }
                }

                if (action === 'dispatch_agent' && msg.body.agentId) {
                    await resetAgentStatus(env, msg.body.agentId);
                }

                msg.ack();
            } catch (error) {
                const errorMessage = String(error).slice(0, 500);
                console.error(`❌ Queue processing error for Campaign #${msg.body.campaignId || 0}:`, error);

                // Record the error to pipeline_runs so the UI can show what went wrong
                if (msg.body.runId) {
                    try {
                        await patchRow(env, 'pipeline_runs', {
                            status: 'failed',
                            errors: [{ message: errorMessage, timestamp: new Date().toISOString() }],
                            completed_at: new Date().toISOString(),
                        }, 'id', msg.body.runId);
                    } catch (_) { /* non-blocking */ }
                }

                if (msg.body.action === 'dispatch_agent' && msg.body.agentId) {
                    try {
                        const { patchRow: pr } = await import('./utils/supabase');
                        await pr(env, 'agents', { status: 'active' }, 'id', msg.body.agentId);
                    } catch (e) { /* non-blocking */ }
                }
                msg.retry();
            }
        }
    },

    /**
     * Scheduled handler — cron-triggered market sensing.
     * Runs at 9 AM + 3 PM EST Mon-Fri.
     */
    async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
        console.log(`⏰ Cron triggered: ${new Date(event.scheduledTime).toISOString()}`);

        try {
            // 1. Run territory briefings generator first
            await generateTerritoryBriefings(env);

            // 2. Fetch all active agents that have a schedule != 'manual'
            const url = `${env.SUPABASE_URL}/rest/v1/agents?status=eq.active&schedule=neq.manual&select=*`;
            const res = await fetch(url, {
                method: 'GET',
                headers: {
                    'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                    'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                    'Content-Type': 'application/json',
                    'Prefer': 'return=minimal',
                },
            });
            const agents = res.ok ? await res.json() as any[] : [];

            console.log(`📡 Cron found ${agents.length} active scheduled agents`);

            for (const agent of agents) {
                // Dispatch each agent immediately via its own queue message
                if (env.STRIKE_QUEUE) {
                    const { patchRow } = await import('./utils/supabase');
                    await patchRow(env, 'agents', { status: 'running' }, 'id', agent.id);

                    await env.STRIKE_QUEUE.send({
                        campaignId: 0,
                        persona: agent.persona || "Rob O'Neill",
                        action: 'dispatch_agent',
                        agentId: agent.id,
                    });
                    console.log(`📤 Queued auto-dispatch for Agent #${agent.id} (${agent.name})`);
                }
            }

            // 3. Run additional free sources in parallel (non-blocking)
            ctx.waitUntil(runAdditionalSources(env));

            // 4. Auto-run saved search missions (frequency = 'daily')
            ctx.waitUntil(runSavedSearchMissions(env));

            console.log(`✅ Cron complete: ${agents.length} agents dispatched + additional sources + missions launched`);
        } catch (error) {
            console.error('❌ Cron error:', error);
        }
    },
};

// ---------------------------------------------------------------------------
// Additional free sources — runs in parallel during cron, non-blocking
// ---------------------------------------------------------------------------

async function runAdditionalSources(env: Env): Promise<void> {
    try {
        console.log('📡 Running additional free data sources (SEC, Court, News)...');

        const [secResult, courtResult, newsResult] = await Promise.allSettled([
            senseSecFilings(env),
            senseCourtFilings(env),
            senseNews(env),
        ]);

        const allTriggers: MarketTrigger[] = [];
        const sourceStats: Record<string, number> = {};

        for (const [label, result] of [
            ['SEC EDGAR', secResult],
            ['CourtListener', courtResult],
            ['NewsData.io', newsResult],
        ] as [string, PromiseSettledResult<MarketTrigger[]>][]) {
            if (result.status === 'fulfilled') {
                allTriggers.push(...result.value);
                sourceStats[label] = result.value.length;
            } else {
                sourceStats[label] = 0;
                console.warn(`⚠️ ${label} failed:`, result.reason);
            }
        }

        if (allTriggers.length === 0) {
            console.log('📡 No triggers from additional sources.');
            return;
        }

        // Deduplicate across sources
        const unique = deduplicateTriggers(allTriggers);
        console.log(`📡 Additional sources: ${allTriggers.length} raw → ${unique.length} unique (SEC: ${sourceStats['SEC EDGAR']}, Court: ${sourceStats['CourtListener']}, News: ${sourceStats['NewsData.io']})`);

        // Queue each trigger for processing through the standard pipeline
        // Limit to top 10 to avoid overwhelming the queue
        for (const trigger of unique.slice(0, 10)) {
            if (env.STRIKE_QUEUE) {
                await env.STRIKE_QUEUE.send({
                    campaignId: 0,
                    persona: "Rob O'Neill",
                    action: 'process_external_trigger',
                    trigger,
                });
            }
        }

        console.log(`✅ Queued ${Math.min(unique.length, 10)} triggers from additional sources`);
    } catch (err) {
        console.error('❌ Additional sources error:', err);
    }
}

// ---------------------------------------------------------------------------
// Auto-run saved search missions — dispatches all daily-frequency missions
// ---------------------------------------------------------------------------

async function runSavedSearchMissions(env: Env): Promise<void> {
    try {
        console.log('🎯 Checking for saved search missions to auto-run...');

        // Fetch all saved searches with frequency = 'daily' + their territory info
        const ssUrl = `${env.SUPABASE_URL}/rest/v1/saved_searches?frequency=eq.daily&select=*,territories(id,name)`;
        const ssRes = await fetch(ssUrl, {
            headers: {
                'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                'Accept': 'application/json',
            },
        });

        if (!ssRes.ok) {
            console.warn(`⚠️ Failed to fetch saved searches: ${ssRes.status}`);
            return;
        }

        const searches = await ssRes.json() as any[];
        if (!searches || searches.length === 0) {
            console.log('🎯 No daily search missions configured.');
            return;
        }

        console.log(`🎯 Found ${searches.length} daily search missions to dispatch`);

        for (const search of searches) {
            try {
                const territoryName = search.territories?.name || 'Unknown Territory';

                // Create a pipeline_runs record for this mission
                const { insertRow, patchRow } = await import('./utils/supabase');
                const runRes = await insertRow(env, 'pipeline_runs', {
                    run_type: 'search_mission',
                    agent_name: `Mission: ${search.name}`,
                    status: 'running',
                    triggered_by: 'cron',
                    metadata: {
                        mission_id: search.id,
                        mission_name: search.name,
                        territory_id: search.territory_id,
                        territory_name: territoryName,
                        query: search.exa_query,
                    },
                    started_at: new Date().toISOString(),
                });

                const runId = runRes.data?.[0]?.id;

                // Dispatch the mission to the queue with metadata
                if (env.STRIKE_QUEUE) {
                    await env.STRIKE_QUEUE.send({
                        campaignId: 0,
                        persona: "Rob O'Neill",
                        action: 'search_mission',
                        searchQuery: search.exa_query,
                        maxResults: 5,
                        runId,
                        // Mission metadata — carried through to strikes
                        missionId: search.id,
                        missionName: search.name,
                        territoryId: search.territory_id,
                        territoryName: territoryName,
                    });
                    console.log(`📤 Dispatched mission: "${search.name}" (territory: ${territoryName})`);
                }

                // Update last_run_at
                await patchRow(env, 'saved_searches', {
                    last_run_at: new Date().toISOString(),
                }, 'id', search.id);
            } catch (missionErr) {
                console.warn(`⚠️ Failed to dispatch mission "${search.name}":`, missionErr);
            }
        }

        console.log(`✅ Dispatched ${searches.length} search missions`);
    } catch (err) {
        console.error('❌ Saved search missions error:', err);
    }
}
