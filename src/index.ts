/**
 * PPA+ APEX — Strike Engine Worker (Entry Point)
 *
 * Cloudflare Worker handling:
 * 1. HTTP requests (manual triggers, health checks, webhook receivers)
 * 2. Queue consumption (strike pipeline processing)
 * 3. Durable Object exports (HITL gate)
 */
import { HitlGateDurableObject } from './durable-objects/hitl-gate';
import { senseTriggers, senseTriggersForAgent } from './activities/sense-triggers';
import { enrichLead, type EnrichedLead } from './activities/enrich-lead';
import type { DraftInput } from './activities/generate-draft';
import { generateDraft } from './activities/generate-draft';
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
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-worker-secret',
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
                const body = await request.json() as { workflowId: string; action: string };
                const result = await executeCampaign(env, {
                    campaignId: 0,
                    workflowId: body.workflowId,
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
                    patternEmails: result.patternEmails || [],
                    otherContacts: (result.otherContacts || []).map((c: any) => ({
                        name: c.name,
                        title: c.title,
                        email: c.email,
                        phone: c.phone,
                        linkedinUrl: c.linkedinUrl || c.linkedin_url,
                        seniority: c.seniority,
                    })),
                });
            } catch (error) {
                console.error('[re-enrich] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Regenerate or Deep Research trigger
        if (url.pathname === '/api/regenerate' && request.method === 'POST') {
            try {
                const body = await request.json() as { workflowId: string; action: 'regenerate' | 'research' };
                await env.STRIKE_QUEUE.send({
                    campaignId: 0,
                    workflowId: body.workflowId,
                    action: body.action,
                    persona: "Rob O'Neill",
                });
                return jsonWithCors({ status: 'queued' });
            } catch (error) {
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

                        // Write to triage_replies table
                        const insertResult = await insertRow(env, 'triage_replies', {
                            campaign_id: campaignId > 0 ? campaignId : null,
                            sender_name: senderName,
                            sender_company: senderCompany,
                            subject,
                            body: replyBody,
                            category: triageResult.category,
                            confidence: triageResult.confidence,
                            preview: replyBody.slice(0, 200),
                        });

                        if (insertResult.ok) {
                            console.log(`✅ Webhook processed: ${senderName} → ${triageResult.category}`);
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

                const { campaignId, persona, workflowId: regenerateWorkflowId, action } = msg.body;
                console.log(`📨 Queue processing: Campaign #${campaignId} | Persona: ${persona} | Action: ${action || 'new'}`);

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

                    // Always re-enrich the lead to get the latest data and real contact names
                    const enrichedLead = await enrichLead(env, {
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

                    // Fetch system settings for the sender persona
                    const { getRow } = await import('./utils/supabase');
                    const { data: settings } = await getRow(env, 'system_settings', 1);
                    const senderName = settings?.default_sender_name || persona || "Rob O'Neill";

                    // Fetch Partner Profiles (M:N) if this strike belongs to a strategic campaign
                    let partnerProfiles: any[] = [];
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
                    }

                    const draft = await generateDraft(env, {
                        lead: enrichedLead,
                        persona: senderName,
                        triggerHeadline: lead.trigger_event,
                        partnerProfiles,
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

                if (action === 'dispatch_agent' && msg.body.agentId) {
                    const { fetchRow, patchRow } = await import('./utils/supabase');
                    const agentRows = await fetchRow(env, 'agents', 'id', msg.body.agentId);
                    if (!agentRows || agentRows.length === 0) {
                        console.error(`Agent ${msg.body.agentId} not found for dispatch`);
                        msg.ack();
                        continue;
                    }
                    const agent = agentRows[0];

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
                        console.log(`📋 Agent #${msg.body.agentId} → Campaign ${strategicCampaignId} with ${partnerProfiles.length} partners`);
                    }

                    triggers = await senseTriggersForAgent(env, agent);

                    if (triggers.length === 0) {
                        await patchRow(env, 'agents', { status: 'active' }, 'id', agent.id);
                        console.log(`⚠️ No triggers found for Agent #${agent.id}`);
                        msg.ack();
                        continue;
                    }
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

                        // Step 3: Generate personalized email draft via Gemini
                        const draft = await generateDraft(env, {
                            lead: enrichedLead,
                            persona: persona || "Rob O'Neill",
                            triggerHeadline: selectedTrigger.headline,
                            triggerArticleText: selectedTrigger.articleText || '',
                            partnerProfiles,
                        });

                        // Step 4: Save lead + campaign to Supabase
                        const workflowId = `wf-${crypto.randomUUID().slice(0, 12)}`;

                        const targetRes = await insertRow(env, 'lead_targets', {
                            company: enrichedLead.company || selectedTrigger.company,
                            executive_name: enrichedLead.executiveName || selectedTrigger.executiveName,
                            executive_title: enrichedLead.executiveTitle || selectedTrigger.executiveTitle,
                            trigger_event: selectedTrigger.headline,
                            trigger_source: selectedTrigger.sourceUrl || null,
                            trigger_relevance: selectedTrigger.relevanceScore || 95,
                            enrichment_data: {
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
                            },
                        });

                        if (targetRes.ok && targetRes.data && targetRes.data.length > 0) {
                            const targetId = targetRes.data[0].id;
                            const campaignRes = await insertRow(env, 'strike_campaigns', {
                                target_id: targetId,
                                status: 'pending_hitl',
                                persona_used: persona,
                                email_subject: draft.subject,
                                drafted_body: draft.body,
                                workflow_id: workflowId,
                                campaign_id: strategicCampaignId,
                            });

                            // Initialize HITL Gate
                            if (campaignRes.ok && campaignRes.data?.[0]?.id) {
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
                            const { patchRow } = await import('./utils/supabase');
                            await patchRow(env, 'agents', { status: 'active' }, 'id', (selectedTrigger as any).agentId);
                        }
                    } catch (triggerErr) {
                        console.error(`❌ Failed to process trigger for ${selectedTrigger.company}:`, triggerErr);
                    }
                }

                console.log(`✅ Pipeline complete: ${newTriggers.length} triggers processed`);

                msg.ack();
            } catch (error) {
                console.error(`❌ Queue processing error for Campaign #${msg.body.campaignId || 0}:`, error);
                msg.retry();
            }
        }
    },

    /**
     * Scheduled handler — cron-triggered market sensing.
     * Runs at 9 AM EST (14:00 UTC) Mon-Fri.
     */
    async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
        console.log(`⏰ Cron triggered: ${new Date(event.scheduledTime).toISOString()}`);

        try {
            // Fetch all active agents that have a schedule != 'manual'
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

            console.log(`✅ Cron complete: ${agents.length} agents dispatched`);
        } catch (error) {
            console.error('❌ Cron error:', error);
        }
    },
};
