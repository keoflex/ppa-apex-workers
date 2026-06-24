/**
 * PPA+ APEX — Strike Engine Worker (Entry Point)
 *
 * Cloudflare Worker handling:
 * 1. HTTP requests (manual triggers, health checks, webhook receivers)
 * 2. Queue consumption (strike pipeline processing)
 * 2. Queue consumption (strike pipeline processing)
 */
import { senseTriggers, senseTriggersForAgent, type MarketTrigger } from './activities/sense-triggers';
import { senseSecFilings, senseSecFilingsForQuery } from './activities/sense-sec-filings';
import { senseCourtFilings, senseCourtFilingsForQuery } from './activities/sense-court-filings';
import { senseNews, senseNewsForQuery } from './activities/sense-news';
import { senseWarnNotices, senseWarnNoticesForQuery } from './activities/sense-warn';
import { senseUsptoNotices, senseUsptoNoticesForQuery } from './activities/sense-uspto';
import { senseSecBulk, senseSecBulkForQuery } from './activities/sense-sec-bulk';
import { senseFederalRegister, senseFederalRegisterForQuery } from './activities/sense-federal-register';
import { senseWarnDirect, senseWarnDirectForQuery } from './activities/sense-warn-direct';
import { senseUsptoDirect, senseUsptoDirectForQuery } from './activities/sense-uspto-direct';
import { deduplicateTriggers } from './utils/dedup';
import { enrichLead, type EnrichedLead } from './activities/enrich-lead';
import type { DraftInput } from './activities/generate-draft';
import { generateDraft } from './activities/generate-draft';
import { generateMobileDraft } from './activities/generate-mobile-draft';
import { synthesizeMobileResearch } from './activities/synthesize-mobile-research';
import { queueTerritoryBriefings } from './tasks/generate-briefing';
import { fetchGemini } from './utils/gemini-fetch';
import { logGeminiError } from './utils/gemini-logger';
import { safeGeminiResponseParse } from './utils/gemini-parse';
import { safeJsonParse } from './utils/json-repair';
import { executeCampaign } from './activities/execute-campaign';
import { executeCampaignSes } from './activities/execute-campaign-ses';
import { triageReply } from './activities/triage-reply';
import { copilotChat, type CopilotRequest } from './activities/copilot-chat';
import { insertRow, patchRow, fetchRow } from './utils/supabase';
import { Webhook, WebhookVerificationError } from 'standardwebhooks';

/**
 * Best-effort discovery of a REAL published contact from a company's own website.
 * Fetches the homepage and common contact pages, then extracts mailto:/tel: and on-page
 * email/phone matches. Returns only contacts that actually appear on the site — we never
 * fabricate a pattern like info@domain — so a delivered contact is always verifiable.
 * Used by the contact-critical strike gate.
 */
async function discoverPublicContact(domain: string): Promise<{ email: string; phone: string; source: string }> {
    const clean = domain.replace(/^https?:\/\//, '').replace(/\/.*$/, '').replace(/^www\./, '');
    if (!clean || !clean.includes('.')) return { email: '', phone: '', source: '' };
    const candidates = [
        `https://${clean}/contact`,
        `https://${clean}/contact-us`,
        `https://${clean}/about`,
        `https://${clean}/`,
    ];
    const rootDomain = clean.split('.').slice(-2).join('.');
    const skipLocal = /^(?:no-?reply|noreply|donotreply|example|email|your)/i;
    for (const url of candidates) {
        try {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), 6000);
            const res = await fetch(url, {
                signal: controller.signal,
                headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ApexResearch/1.0; +https://polsinellipa.com)' },
                redirect: 'follow',
            });
            clearTimeout(timer);
            if (!res.ok) continue;
            const html = (await res.text()).slice(0, 500_000);

            // Emails: prefer mailto:, then plain on-page text. Drop image filenames & no-reply.
            const emails = new Set<string>();
            for (const m of html.matchAll(/mailto:([^"'?>\s]+@[^"'?>\s]+)/gi)) emails.add(m[1].toLowerCase());
            for (const m of html.matchAll(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g)) emails.add(m[0].toLowerCase());
            const ranked = [...emails].filter(e => !skipLocal.test(e) && !/\.(png|jpe?g|gif|webp|svg|css|js)$/i.test(e));
            const email = ranked.find(e => e.endsWith('@' + rootDomain) || e.includes(rootDomain)) || ranked[0] || '';

            // Phone: tel: link first, then a loose North-America-friendly pattern.
            let phone = '';
            const tel = html.match(/tel:([+0-9().\-\s]{7,})/i);
            if (tel) phone = tel[1].replace(/\s+/g, ' ').trim();
            else {
                const p = html.match(/\+?\d[\d().\-\s]{8,}\d/);
                if (p) phone = p[0].replace(/\s+/g, ' ').trim();
            }

            if (email || phone) return { email, phone, source: url };
        } catch (_) { /* try next candidate */ }
    }
    return { email: '', phone: '', source: '' };
}

/** Small stable string hash (djb2) → base36, for compact cache keys. */
function cacheHash(input: string): string {
    let h = 5381;
    for (let i = 0; i < input.length; i++) h = ((h << 5) + h + input.charCodeAt(i)) >>> 0;
    return h.toString(36);
}

/**
 * Reads a non-expired entry from the mobile_research_cache table. Used to avoid re-paying Exa /
 * Apollo / Gemini for research we've already done. Returns the stored payload or null.
 */
async function getResearchCache(env: Env, key: string): Promise<any | null> {
    try {
        const res = await fetch(
            `${env.SUPABASE_URL}/rest/v1/mobile_research_cache?cache_key=eq.${encodeURIComponent(key)}&expires_at=gt.${encodeURIComponent(new Date().toISOString())}&select=payload&limit=1`,
            { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
        );
        if (!res.ok) return null;
        const rows = await res.json() as any[];
        return rows[0]?.payload ?? null;
    } catch { return null; }
}

/** Upserts a cache entry with a TTL (days). Requires a UNIQUE constraint on cache_key. */
async function setResearchCache(env: Env, key: string, payload: any, ttlDays: number): Promise<void> {
    try {
        const expires = new Date(Date.now() + ttlDays * 86400000).toISOString();
        await fetch(`${env.SUPABASE_URL}/rest/v1/mobile_research_cache`, {
            method: 'POST',
            headers: {
                apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                'Content-Type': 'application/json',
                Prefer: 'resolution=merge-duplicates',
            },
            body: JSON.stringify({ cache_key: key, payload, expires_at: expires }),
        });
    } catch { /* non-fatal: caching is best-effort */ }
}

export function normalizeDomain(companyName: string, enrichmentData?: any, sourceUrl?: string | null): string {
    // 1. Check if enrichment data has a domain
    let domain = enrichmentData?.company_domain || enrichmentData?.companyDomain || enrichmentData?.domain;
    if (domain) {
        domain = domain.trim().toLowerCase().replace(/^(https?:\/\/)?(www\.)?/, '');
        return domain;
    }
    
    // 2. Try parsing source URL if provided
    const urlToCheck = sourceUrl || enrichmentData?.trigger_source || enrichmentData?.source_url || enrichmentData?.url;
    if (urlToCheck) {
        try {
            const parsed = new URL(urlToCheck).hostname.replace('www.', '').toLowerCase();
            if (parsed) return parsed;
        } catch (_) {}
    }
    
    // 3. Fallback to company name normalization
    let name = companyName || '';
    name = name.trim().toLowerCase();
    
    // Remove trailing dots
    while (name.endsWith('.')) {
        name = name.slice(0, -1);
    }
    
    // Replace all spaces and non-alphanumeric characters, except dots
    let normalized = name.replace(/[^a-z0-9.]/g, '');
    
    // Remove any remaining trailing/double dots
    normalized = normalized.replace(/\.+$/, '');
    normalized = normalized.replace(/\.+/g, '.');
    
    // Check if it should end with .gov or .com
    if (normalized.endsWith('.gov') || normalized.endsWith('gov')) {
        return normalized.replace(/gov$/, '') + '.gov';
    }
    
    if (normalized.endsWith('.com')) {
        return normalized;
    }
    
    return normalized + '.com';
}

export interface Env {
    STRIKE_QUEUE: Queue;
    MOBILE_QUEUE: Queue;
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
    // Amazon SES credentials
    AWS_ACCESS_KEY_ID: string;
    AWS_SECRET_ACCESS_KEY: string;
    AWS_REGION: string;           // e.g. 'us-east-1'
    SES_CONFIGURATION_SET?: string;  // optional: for event tracking
    WEBHOOK_SIGNING_SECRET?: string; // Gemini Webhook signing secret
    FORCE_EXA_RESEARCH?: string;     // override to force Exa deep research
    THROTTLE_DELAY_MS?: string;      // dynamic queue delivery sleep delay in ms
    SOCRATA_APP_TOKEN?: string;      // Socrata open data app token
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

        // Service health — live credit check for external APIs (used by platform alert banner)
        if (url.pathname === '/api/service-health') {
            const services: Record<string, { status: string; details: string }> = {};

            // Exa.ai credit check
            try {
                const exaRes = await fetch('https://api.exa.ai/search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': env.EXA_API_KEY },
                    body: JSON.stringify({ query: 'test', numResults: 1, type: 'neural', contents: { text: { maxCharacters: 10 } } }),
                });
                if (exaRes.status === 402) {
                    services['exa'] = { status: 'credits_exhausted', details: 'Top up at dashboard.exa.ai' };
                } else if (exaRes.ok) {
                    services['exa'] = { status: 'operational', details: 'Credits available' };
                } else {
                    services['exa'] = { status: 'degraded', details: `HTTP ${exaRes.status}` };
                }
            } catch (err) {
                services['exa'] = { status: 'error', details: String(err).slice(0, 100) };
            }

            return jsonWithCors({ services });
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

        // ── Gemini Email Guesser ──
        if (url.pathname === '/api/guess-email' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    company: string;
                    executiveName: string;
                    domain?: string;
                };

                console.log(`🧠 Gemini predicting emails for ${body.executiveName} at ${body.company}`);

                const prompt = `You are an expert B2B data researcher and sales intelligence AI with access to Google Search.
Your task is to identify and generate the most likely email addresses for an executive based on standard corporate syntax or live Google web searches.

Company Name: ${body.company}
Executive Name: ${body.executiveName}
${body.domain ? `Company Domain: ${body.domain}` : ''}

Instructions:
1. CRITICAL: If the Executive Name is "Unknown", "N/A", or "None":
   - You MUST execute a live Google Search using your internal tools to search for the ACTUAL current CEO, Founder, or President of ${body.company}. Search SEC filings, press releases, and news.
   - If you can successfully identify the key executive's name (e.g., John Smith), generate the 3-4 most statistically common permutations FOR THAT SPECIFIC PERSON (e.g. john.smith@, jsmith@).
   - If you find an explicit email address for them in the search results, use it immediately in your guesses.
   - If your Google Search turns up empty and you cannot identify a specific human executive, you MUST return an empty array: \`{ "guesses": [] }\`. NEVER return generic role-based emails like ceo@ or info@.
4. You must output ONLY valid JSON matching this exact structure, with NO MARKDOWN CODE BLOCKS:
{
  "guesses": [
    {
      "email": "string",
      "rationale": "string indicating WHY you guessed this (e.g. 'Found John Smith as CEO via Google Search. Guessing firstname.lastname format.')"
    }
  ],
  "executiveFound": "string (the actual name of the person you found, e.g. 'John Smith', or null if you did not find anyone)"
}`;

                const res = await fetchGemini(env, 'lite', {
                    activityName: 'guess-email',
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{ role: 'user', parts: [{ text: prompt }] }],
                        generationConfig: {
                            temperature: 0.1,
                            maxOutputTokens: 256,
                            responseMimeType: 'application/json',
                            responseSchema: {
                                type: "OBJECT",
                                properties: {
                                    guesses: {
                                        type: "ARRAY",
                                        items: {
                                            type: "OBJECT",
                                            properties: {
                                                email: { type: "STRING" },
                                                rationale: { type: "STRING" }
                                            },
                                            required: ["email", "rationale"]
                                        }
                                    },
                                    executiveFound: { type: "STRING", description: "The actual name of the person you found, or 'Unknown' if none" }
                                },
                                required: ["guesses", "executiveFound"]
                            }
                        },
                        tools: [{ googleSearch: {} }]
                    })
                });

                if (!res.ok) {
                    throw new Error(`Gemini API error ${res.status}`);
                }

                const data = await res.json() as any;
                const text = data.candidates?.[0]?.content?.parts?.[0]?.text || '{}';
                
                const parsed = safeJsonParse<any>(text, null);
                if (parsed && typeof parsed === 'object') {
                    return jsonWithCors({ status: 'success', guesses: parsed.guesses || [], executiveFound: parsed.executiveFound || null });
                } else {
                    return jsonWithCors({ status: 'error', error: 'Failed to parse Gemini response' });
                }
            } catch (err: any) {
                console.error('[guess-email] Error:', err);
                await logGeminiError(env, 'pro-guess-email', '/api/guess-email', err);
                return jsonWithCors({ error: String(err) }, { status: 500 });
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

                const geminiRes = await fetchGemini(env, 'lite', {
                    activityName: 'ai-resync',
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
                                type: "OBJECT",
                                properties: {
                                    summary: { type: "STRING" },
                                    industry: { type: "STRING" },
                                    revenue: { type: "STRING" },
                                    headcount: { type: "STRING" }
                                },
                                required: ["summary", "industry"]
                            }
                        },
                    }),
                });

                if (!geminiRes.ok) {
                    const errText = await geminiRes.text();
                    console.error(`[ai-resync] Gemini error ${geminiRes.status}: ${errText}`);
                    return jsonWithCors({ error: `Gemini API error ${geminiRes.status}: ${errText.slice(0, 200)}` }, { status: 502 });
                }

                const { text: rawText } = await safeGeminiResponseParse(geminiRes);

                if (!rawText) {
                    console.error('[ai-resync] Gemini empty response');
                    return jsonWithCors({ error: 'Gemini returned empty response' }, { status: 502 });
                }

                const parsed = safeJsonParse<any>(rawText, null);
                if (!parsed || typeof parsed !== 'object') {
                    console.error('[ai-resync] Failed to parse Gemini output:', rawText.slice(0, 300));
                    return jsonWithCors({ summary: rawText.slice(0, 500), industry: null, revenue: null, headcount: null });
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
        // Everything here can drive the pipeline (burn Exa/Apollo/Gemini credits) or, via
        // regenerate→Tier-1 auto-clearance, send real email — so all of it requires the secret.
        // The dashboard reaches these through its server-side /api/v1/* proxy routes, which
        // attach the secret from server env.
        const protectedPaths = [
            '/api/execute',
            '/api/copilot/chat',
            '/api/send-email',
            '/api/regenerate',
            '/api/re-enrich',
            '/api/trigger-strike',
            '/api/trigger-sweep',
            '/api/search-mission',
        ];
        const protectedPrefixes = ['/api/dispatch-agent/'];
        const isProtected = protectedPaths.includes(url.pathname)
            || protectedPrefixes.some((p) => url.pathname.startsWith(p));
        if (isProtected && request.method === 'POST') {
            const secret = request.headers.get('x-worker-secret');
            if (!secret || secret !== env.WORKER_SECRET) {
                return jsonWithCors(
                    { error: 'Unauthorized — invalid or missing x-worker-secret header' },
                    { status: 401 },
                );
            }
        }

        // ── Gemini API Webhook Receiver ──
        if (url.pathname === '/api/gemini-webhook' && request.method === 'POST') {
            try {
                if (!env.WEBHOOK_SIGNING_SECRET) {
                    console.error('[gemini-webhook] Missing WEBHOOK_SIGNING_SECRET');
                    return jsonWithCors({ error: 'Webhook secret not configured' }, { status: 500 });
                }

                const payload = await request.text();
                const headers = Object.fromEntries(request.headers.entries());

                const wh = new Webhook(env.WEBHOOK_SIGNING_SECRET);
                const event = wh.verify(payload, headers) as Record<string, any>;

                console.log(`🤖 Gemini Webhook Event: ${event.type} | ID: ${event.data?.id}`);

                if (event.type === 'batch.succeeded') {
                    console.log(`✅ Batch completed: ${event.data?.output_file_uri}`);
                    // Future: Trigger queue to download and process output_file_uri
                } else if (event.type === 'batch.failed') {
                    console.error(`❌ Batch failed: ${event.data?.id}`);
                }

                return jsonWithCors({ status: 'received' }, { status: 200 });
            } catch (e: any) {
                console.error('[gemini-webhook] Verification failed:', e.message);
                return jsonWithCors({ error: 'Signature invalid' }, { status: 400 });
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

        // Manual free sensor sweep trigger
        if (url.pathname === '/api/trigger-sweep' && request.method === 'POST') {
            try {
                if (env.STRIKE_QUEUE) {
                    await env.STRIKE_QUEUE.send({
                        campaignId: 0,
                        persona: 'system',
                        action: 'free_sensor_sweep',
                    });
                    return new Response(JSON.stringify({ ok: true, message: 'Free sensor sweep queued' }), {
                        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
                    });
                }
                return new Response(JSON.stringify({ error: 'Queue not available' }), { status: 500, headers: corsHeaders });
            } catch (e: any) {
                return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
            }
        }

        // Manual strike trigger → push to Queue for async processing
        if (url.pathname === '/api/trigger-strike' && request.method === 'POST') {
            try {
                const body = await request.json() as { campaignId: number; persona: string; runId?: number | string };

                // ── Auto-reset stale agent locks before every pipeline run ──
                try {
                    const { patchRow: patchStale } = await import('./utils/supabase');
                    const staleThresholdTime = new Date(Date.now() - 120 * 60 * 1000).toISOString();
                    const staleRes = await fetch(
                        `${env.SUPABASE_URL}/rest/v1/agents?active_pipelines=gt.0&last_activity=lt.${staleThresholdTime}&select=id`,
                        { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                    );
                    if (staleRes.ok) {
                        const staleAgents = await staleRes.json() as any[];
                        if (staleAgents.length > 0) {
                            const staleIds = staleAgents.map(a => a.id).join(',');
                            await fetch(
                                `${env.SUPABASE_URL}/rest/v1/agents?id=in.(${staleIds})`,
                                {
                                    method: 'PATCH',
                                    headers: {
                                        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                                        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                        'Content-Type': 'application/json',
                                        Prefer: 'return=minimal',
                                    },
                                    body: JSON.stringify({ active_pipelines: 0 }),
                                }
                            );
                            console.log(`🔓 Auto-reset ${staleAgents.length} stale agent locks before pipeline run`);
                        }
                    }
                } catch (e) { console.warn('⚠️ Stale lock reset failed (non-blocking):', e); }

                if (env.STRIKE_QUEUE && body.campaignId) {
                    const caRes = await fetch(`${env.SUPABASE_URL}/rest/v1/campaign_agents?campaign_id=eq.${body.campaignId}&select=agent_id`, {
                        headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` }
                    });
                    if (caRes.ok) {
                        const caRows = await caRes.json() as any[];
                        if (caRows.length > 0) {
                            const messages = caRows.map(r => ({
                                body: {
                                    campaignId: body.campaignId,
                                    agentId: r.agent_id,
                                    persona: body.persona || "Rob O'Neill",
                                    source: 'manual',
                                    action: 'dispatch_agent',
                                    runId: body.runId,
                                    retryCount: 0
                                }
                            }));
                            await safeQueueSendBatch(env.STRIKE_QUEUE, messages);
                            console.log(`🚀 Manually pushed ${caRows.length} agents for Campaign #${body.campaignId} into queue.`);
                            return jsonWithCors({ status: 'queued', campaignId: body.campaignId, agentsQueued: caRows.length }, { status: 202 });
                        }
                    }
                    return jsonWithCors({ error: 'No agents found for campaign' }, { status: 400 });
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

        // ── SES Event Webhook (SNS → Worker) ──
        // Receives bounce, complaint, and delivery notifications from Amazon SES via SNS
        if (url.pathname === '/api/ses-webhook' && request.method === 'POST') {
            try {
                const rawBody = await request.text();
                const payload = JSON.parse(rawBody);

                // Handle SNS subscription confirmation
                if (payload.Type === 'SubscriptionConfirmation') {
                    console.log('📬 SNS subscription confirmation received, confirming...');
                    if (payload.SubscribeURL) {
                        await fetch(payload.SubscribeURL);
                        console.log('✅ SNS subscription confirmed');
                    }
                    return jsonWithCors({ status: 'subscription_confirmed' });
                }

                // Handle actual notification
                if (payload.Type === 'Notification') {
                    const message = typeof payload.Message === 'string'
                        ? JSON.parse(payload.Message) : payload.Message;

                    const eventType = message.eventType || message.notificationType;
                    const mail = message.mail || {};
                    const sesMessageId = mail.messageId || '';

                    console.log(`📬 SES event: ${eventType} | MessageId: ${sesMessageId}`);

                    if (!sesMessageId) {
                        return jsonWithCors({ status: 'ignored', reason: 'no messageId' });
                    }

                    // Find the campaign by SES message ID
                    const campaignRes = await fetch(
                        `${env.SUPABASE_URL}/rest/v1/strike_campaigns?ses_message_id=eq.${encodeURIComponent(sesMessageId)}&select=id,status,workflow_id,funnel_id`,
                        {
                            headers: {
                                'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                                'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                            },
                        }
                    );

                    if (!campaignRes.ok) {
                        console.error(`[ses-webhook] Supabase lookup failed: ${campaignRes.status}`);
                        return jsonWithCors({ status: 'error', reason: 'db_lookup_failed' }, { status: 500 });
                    }

                    const campaigns = await campaignRes.json() as any[];
                    if (campaigns.length === 0) {
                        console.log(`[ses-webhook] No campaign found for MessageId: ${sesMessageId}`);
                        return jsonWithCors({ status: 'ignored', reason: 'campaign_not_found' });
                    }

                    const campaign = campaigns[0];
                    const now = new Date().toISOString();
                    const updateData: Record<string, any> = {};

                    if (eventType === 'Bounce') {
                        const bounce = message.bounce || {};
                        const bounceType = bounce.bounceType === 'Permanent' ? 'hard_bounce' : 'soft_bounce';
                        const bouncedRecipients = (bounce.bouncedRecipients || [])
                            .map((r: any) => `${r.emailAddress}: ${r.diagnosticCode || r.status || 'unknown'}`)
                            .join('; ');

                        updateData.status = 'failed';
                        updateData.bounced_at = now;
                        updateData.bounce_type = bounceType;
                        console.log(`🚫 BOUNCE (${bounceType}) for campaign #${campaign.id}: ${bouncedRecipients}`);

                    } else if (eventType === 'Complaint') {
                        const complaint = message.complaint || {};
                        updateData.status = 'failed';
                        updateData.bounced_at = now;
                        updateData.bounce_type = 'complaint';
                        console.log(`⚠️ COMPLAINT for campaign #${campaign.id}: ${complaint.complaintFeedbackType || 'unknown'}`);

                    } else if (eventType === 'Delivery') {
                        updateData.delivered_at = now;
                        console.log(`✅ DELIVERED campaign #${campaign.id}`);

                    } else if (eventType === 'Open') {
                        updateData.opened_at = now;
                        console.log(`👁️ OPENED campaign #${campaign.id}`);

                    } else if (eventType === 'Click') {
                        updateData.clicked_at = now;
                        console.log(`🔗 CLICKED campaign #${campaign.id}`);

                    } else {
                        console.log(`[ses-webhook] Unhandled event type: ${eventType}`);
                        return jsonWithCors({ status: 'ignored', reason: `unhandled_event: ${eventType}` });
                    }

                    // Update the campaign
                    if (Object.keys(updateData).length > 0) {
                        const patchRes = await fetch(
                            `${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=eq.${campaign.id}`,
                            {
                                method: 'PATCH',
                                headers: {
                                    'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                                    'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                    'Content-Type': 'application/json',
                                    'Prefer': 'return=minimal',
                                },
                                body: JSON.stringify(updateData),
                            }
                        );

                        if (!patchRes.ok) {
                            console.error(`[ses-webhook] Failed to update campaign #${campaign.id}: ${patchRes.status}`);
                        } else {
                            console.log(`💾 Campaign #${campaign.id} updated: ${JSON.stringify(updateData)}`);
                            
                            // Safety Catch / Kill Switch: Halt Funnel if this is a Bounce or Complaint
                            if (campaign.funnel_id && (eventType === 'Bounce' || eventType === 'Complaint')) {
                                await fetch(`${env.SUPABASE_URL}/rest/v1/strike_funnels?id=eq.${campaign.funnel_id}`, {
                                    method: 'PATCH',
                                    headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json', 'Prefer': 'return=minimal' },
                                    body: JSON.stringify({ status: eventType === 'Bounce' ? 'halted_bounced' : 'halted_complaint' })
                                });
                                console.log(`🛑 Funnel #${campaign.funnel_id} Kill Switch triggered by ${eventType}`);
                            }
                        }
                    }

                    return jsonWithCors({ status: 'processed', eventType, campaignId: campaign.id });
                }

                return jsonWithCors({ status: 'ignored', reason: 'unknown_type' });
            } catch (error) {
                console.error('[ses-webhook] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Execute delivery (called by Next.js approve endpoint)
        if (url.pathname === '/api/execute' && request.method === 'POST') {
            try {
                const body = await request.json() as { workflowId: string; action: string; senderAccounts?: string[]; forceInline?: boolean };

                // Look up the strike campaign from Supabase to get email content + recipient
                const campaignRows = await fetchRow(env, 'strike_campaigns', 'workflow_id', body.workflowId);
                const campaign = campaignRows?.[0];

                if (!campaign) {
                    return Response.json({ error: `Strike campaign not found: ${body.workflowId}` }, { status: 404 });
                }

                // Get the target lead's email
                let recipientEmail = '';
                if (campaign.target_id) {
                    const targetRows = await fetchRow(env, 'lead_targets', 'id', campaign.target_id);
                    if (targetRows?.[0]) {
                        const target = targetRows[0];
                        recipientEmail = target.email || target.enrichment_data?.email || '';
                    }
                }

                if (!recipientEmail) {
                    return Response.json({ error: 'No recipient email found for this strike' }, { status: 400 });
                }

                // Send via Amazon SES
                const senderEmail = body.senderAccounts?.[0] || 'fred@polsinellimgmt.com';

                // Queue the delivery via STRIKE_QUEUE for throttled sending
                if (env.STRIKE_QUEUE && !body.forceInline) {
                    await env.STRIKE_QUEUE.send({
                        action: 'deliver',
                        campaignId: Number(campaign.id) || 0,
                        workflowId: body.workflowId,
                        emailSubject: campaign.email_subject || `Introduction — ${campaign.workflow_id}`,
                        emailSubjectB: campaign.email_subject_b || null,
                        emailBody: campaign.drafted_body || campaign.email_body || '',
                        recipientEmail,
                        senderEmail,
                        senderName: 'Fred Posinelli',
                    });
                    console.log(`📤 Queued throttled delivery for Workflow #${body.workflowId}`);
                    return Response.json({ status: 'queued', message: 'Delivery queued for throttled sending' });
                } else {
                    if (!env.STRIKE_QUEUE) console.warn(`⚠️ STRIKE_QUEUE not bound, unable to queue delivery for Workflow #${body.workflowId}`);
                    console.log(`🚀 Forcing inline delivery for Workflow #${body.workflowId}...`);
                    try {
                        let selectedSubject = campaign.email_subject || `Introduction — ${campaign.workflow_id}`;
                        let selectedVersion = 'A';
                        if (campaign.email_subject_b) {
                            if (Number(campaign.id) % 2 === 0) {
                                selectedSubject = campaign.email_subject;
                                selectedVersion = 'A';
                            } else {
                                selectedSubject = campaign.email_subject_b;
                                selectedVersion = 'B';
                            }
                            console.log(`⚖️ [A/B Testing] Inline selected Version ${selectedVersion}: "${selectedSubject}"`);
                        }

                        const { executeCampaignSes } = await import('./activities/execute-campaign-ses');
                        const result = await executeCampaignSes(env, {
                            campaignId: Number(campaign.id) || 0,
                            workflowId: body.workflowId,
                            emailSubject: selectedSubject,
                            emailBody: campaign.drafted_body || campaign.email_body || '',
                            recipientEmail,
                            senderEmail,
                            senderName: 'Fred Posinelli',
                            subjectVersion: selectedVersion,
                        });
                        return Response.json({ status: 'inline_delivered', result });
                    } catch (e) {
                         console.error('❌ Inline SES execution failed:', e);
                         return Response.json({ error: `Inline delivery failed: ${e}` }, { status: 500 });
                    }
                }
            } catch (error) {
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Send-email (called by Next.js platform or internally for alerts/digests)
        if (url.pathname === '/api/send-email' && request.method === 'POST') {
            try {
                const body = await request.json() as { to: string; subject: string; html: string; senderEmail?: string; senderName?: string };
                if (!body.to || !body.subject || !body.html) {
                    return jsonWithCors({ error: 'Missing required fields: to, subject, html' }, { status: 400 });
                }

                // Send via AWS SES directly
                const result = await executeCampaignSes(env, {
                    campaignId: 0, // dummy id to skip database update
                    workflowId: 'system-digest',
                    emailSubject: body.subject,
                    emailBody: body.html,
                    recipientEmail: body.to,
                    senderEmail: body.senderEmail || 'fred@polsinellimgmt.com',
                    senderName: body.senderName || 'PPA APEX Platform',
                });

                if (result.error) {
                    return jsonWithCors({ error: result.error }, { status: 500 });
                }

                return jsonWithCors({ status: 'success', sesMessageId: result.sesMessageId });
            } catch (error: any) {
                console.error('[send-email] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
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

                const geminiRes = await fetchGemini(
                    env, 'pro',
                    {
                        activityName: 'test-pipeline',
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            system_instruction: { parts: [{ text: 'Extract company and executive. Return JSON array: [{"index":0,"company":"Name","executiveName":"John","executiveTitle":"CEO"}]. If no executive named, use "Unknown".' }] },
                            contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                            generationConfig: { temperature: 0.1, responseMimeType: 'application/json' },
                        }),
                    }
                );
                const { text: rawText, raw: geminiBody } = await safeGeminiResponseParse(geminiRes);
                const parts = geminiBody?.candidates?.[0]?.content?.parts || [];
                steps.gemini = { status: geminiRes.status, partsCount: parts.length, rawText: rawText?.slice(0, 500) };

                if (rawText) {
                    const extracted = safeJsonParse<any[]>(rawText, []);
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

                let requestBody: any = {};
                try {
                    requestBody = await request.json();
                } catch (e) {
                    // Body might be missing or empty
                }

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
                        runId: requestBody.runId,
                    });
                }
                return jsonWithCors({ status: 'queued', agentId: agent.id });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Generate Client Intelligence Brief (called by platform)
        if (url.pathname === '/api/generate-brief' && request.method === 'POST') {
            try {
                const secret = request.headers.get('x-worker-secret');
                if (!secret || secret !== env.WORKER_SECRET) {
                    return jsonWithCors({ error: 'Unauthorized' }, { status: 401 });
                }

                const body = await request.json() as {
                    companyName: string;
                    domain?: string;
                    industry?: string;
                    existingSummary?: string;
                    watchDomains?: string[];
                    watchTopics?: string[];
                };

                console.log(`📊 Generating intelligence brief for ${body.companyName}`);

                // Step 1: Exa search for recent news about the company
                const searchQueries = [
                    `${body.companyName} ${new Date().getFullYear()} news announcement`,
                    ...(body.watchTopics?.length ? [`${body.companyName} ${body.watchTopics.join(' ')}`] : []),
                ];

                const allResults: Array<{ title: string; text: string; url: string }> = [];

                for (const query of searchQueries.slice(0, 2)) {
                    try {
                        const exaRes = await fetch('https://api.exa.ai/search', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json', 'x-api-key': env.EXA_API_KEY },
                            body: JSON.stringify({
                                query,
                                numResults: 5,
                                type: 'neural',
                                useAutoprompt: true,
                                contents: { text: { maxCharacters: 500 }, highlights: { numSentences: 2, highlightsPerUrl: 1 } },
                            }),
                        });
                        if (exaRes.ok) {
                            const exaData = await exaRes.json() as any;
                            for (const r of (exaData.results || [])) {
                                allResults.push({ title: r.title || '', text: r.text || r.highlights?.join(' ') || '', url: r.url || '' });
                            }
                        }
                    } catch (err) {
                        console.warn(`[generate-brief] Exa search failed for query: ${query}`, err);
                    }
                }

                // Step 2: Gemini synthesis

                const systemPrompt = `You are a strategic intelligence analyst for Polsinelli, a major Am Law 100 law firm.
Your job is to create a concise intelligence brief about a specific company that helps attorneys understand what is happening
with their client or prospect, and spot cross-sell opportunities.

OUTPUT FORMAT: Respond with a JSON object ONLY (no markdown):
{
  "title": "Brief title (e.g., 'Weekly Intelligence: Acme Corp')",
  "content": "2-4 paragraph markdown brief covering key developments, market movements, and strategic implications. Be specific and actionable.",
  "keyFindings": ["Finding 1", "Finding 2", "Finding 3"],
  "crossSellOpportunities": ["Opportunity 1 - which practice area could help", "Opportunity 2"],
  "sourceUrls": ["url1", "url2"]
}`;

                const newsContext = allResults.length > 0
                    ? allResults.map((r, i) => `[${i + 1}] ${r.title}\n${r.text}\nURL: ${r.url}`).join('\n\n')
                    : 'No recent news articles found. Generate brief from known information.';

                const userPrompt = `Company: ${body.companyName}
${body.domain ? `Domain: ${body.domain}` : ''}
${body.industry ? `Industry: ${body.industry}` : ''}
${body.existingSummary ? `Existing Intel: ${body.existingSummary}` : ''}
${body.watchTopics?.length ? `Watch Topics: ${body.watchTopics.join(', ')}` : ''}

Recent News:
${newsContext}

Generate a strategic intelligence brief for this company. Focus on actionable insights and cross-sell opportunities for a law firm.`;

                const geminiRes = await fetchGemini(env, 'lite', {
                    activityName: 'generate-brief',
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        system_instruction: { parts: [{ text: systemPrompt }] },
                        contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
                        generationConfig: {
                            temperature: 0.7,
                            maxOutputTokens: 2048,
                            responseMimeType: 'application/json',
                            responseSchema: {
                                type: "OBJECT",
                                properties: {
                                    title: { type: "STRING" },
                                    content: { type: "STRING" },
                                    keyFindings: { type: "ARRAY", items: { type: "STRING" } },
                                    crossSellOpportunities: { type: "ARRAY", items: { type: "STRING" } },
                                    sourceUrls: { type: "ARRAY", items: { type: "STRING" } }
                                },
                                required: ["title", "content", "keyFindings"]
                            }
                        },
                    }),
                });

                if (!geminiRes.ok) {
                    const errText = await geminiRes.text();
                    console.error(`[generate-brief] Gemini error ${geminiRes.status}: ${errText}`);
                    return jsonWithCors({ error: `Gemini API error` }, { status: 502 });
                }

                const { text: rawText } = await safeGeminiResponseParse(geminiRes);

                if (!rawText) {
                    return jsonWithCors({ error: 'Gemini returned empty response' }, { status: 502 });
                }

                const parsed = safeJsonParse<any>(rawText, null);
                const brief = {
                    title: parsed?.title || `Intelligence Brief: ${body.companyName}`,
                    content: parsed?.content || rawText || '',
                    keyFindings: parsed?.keyFindings || [],
                    crossSellOpportunities: parsed?.crossSellOpportunities || [],
                    sourceUrls: parsed?.sourceUrls || allResults.map(r => r.url).filter(Boolean),
                };

                console.log(`✅ Brief generated for ${body.companyName}: ${brief.title}`);

                return jsonWithCors({
                    title: brief.title,
                    content: brief.content,
                    keyFindings: brief.keyFindings,
                    crossSellOpportunities: brief.crossSellOpportunities,
                    sourceUrls: brief.sourceUrls,
                });
            } catch (error: any) {
                console.error('[generate-brief] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // ── Classify Reply — Gemini-powered reply intent classification ──
        if (url.pathname === '/api/classify-reply' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    replyBody: string;
                    originalSubject?: string;
                    originalBody?: string;
                    senderName?: string;
                    senderCompany?: string;
                };

                if (!body.replyBody) {
                    return jsonWithCors({ error: 'replyBody required' }, { status: 400 });
                }

                const classifyPrompt = `You are an expert classifier for legal business development email replies.

Classify this reply into exactly ONE category:
- "interested" — They want to learn more, asked a question, or showed positive intent
- "objection" — They pushed back, said timing is bad, or need convincing  
- "meeting_request" — They explicitly requested a call/meeting or suggested times
- "not_now" — Polite decline, not interested at this time
- "out_of_office" — Auto-reply or OOO message
- "unsubscribe" — Wants to be removed from communications

Also provide:
- confidence: float 0-1
- summary: 1 sentence summary of their intent
- suggested_action: what the sales team should do next

Original email subject: ${body.originalSubject || '(unknown)'}
Original email context: ${(body.originalBody || '').slice(0, 500)}
Reply from: ${body.senderName || 'Unknown'} at ${body.senderCompany || 'Unknown'}

REPLY TO CLASSIFY:
${body.replyBody.slice(0, 2000)}

Return ONLY valid JSON:
{
  "classification": "interested|objection|meeting_request|not_now|out_of_office|unsubscribe",
  "confidence": 0.95,
  "summary": "...",
  "suggested_action": "..."
}`;

                const geminiRes = await fetchGemini(env, 'lite', {
                    activityName: 'classify-reply',
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{ parts: [{ text: classifyPrompt }] }],
                        generationConfig: {
                            temperature: 0.1,
                            maxOutputTokens: 500,
                            responseMimeType: 'application/json',
                            responseSchema: {
                                type: "OBJECT",
                                properties: {
                                    classification: { type: "STRING", enum: ["interested", "objection", "meeting_request", "not_now", "out_of_office", "unsubscribe"] },
                                    confidence: { type: "NUMBER" },
                                    summary: { type: "STRING" },
                                    suggested_action: { type: "STRING" }
                                },
                                required: ["classification", "confidence", "summary", "suggested_action"]
                            }
                        },
                    }),
                });

                if (!geminiRes.ok) {
                    return jsonWithCors({ error: 'Gemini classification failed' }, { status: 502 });
                }

                const { text: rawText } = await safeGeminiResponseParse(geminiRes);

                const result = safeJsonParse(rawText, null) || { classification: 'not_now', confidence: 0.5, summary: 'Could not parse reply', suggested_action: 'Manual review needed' };

                console.log(`🧠 Reply classified: ${result.classification} (${result.confidence}) — ${result.summary}`);
                return jsonWithCors(result);
            } catch (error: any) {
                console.error('[classify-reply] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // ── Generate Follow-up — AI-crafted contextual follow-up email ──
        if (url.pathname === '/api/generate-followup' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    strikeId: number;
                    stepNumber: number;
                    replyClassification?: string;
                    replySummary?: string;
                    originalSubject?: string;
                    originalBody?: string;
                    company?: string;
                    executive?: string;
                    persona?: string;
                };

                if (!body.strikeId) {
                    return jsonWithCors({ error: 'strikeId required' }, { status: 400 });
                }

                // Strategy based on classification
                const strategies: Record<string, string> = {
                    interested: 'They showed interest. Provide a specific value proposition and suggest a concrete next step like a 15-minute call.',
                    objection: 'They had concerns. Address their specific objection empathetically, provide social proof or a case study, and offer a low-commitment next step.',
                    not_now: 'They declined for now. Respect their timing, add value with a relevant insight, and leave the door open for future contact.',
                    meeting_request: 'They want to meet! Confirm the meeting request, suggest 2-3 specific time slots, and briefly outline what you will cover.',
                    out_of_office: 'They are out of office. This is a timed follow-up to reconnect when they return.',
                };

                const strategy = strategies[body.replyClassification || 'not_now'] || strategies.not_now;

                const followUpPrompt = `You are a senior business development strategist at a top-100 law firm (Posinelli).
Write a follow-up email (step ${body.stepNumber} in the sequence).

Context:
- Company: ${body.company || 'Unknown'}
- Executive: ${body.executive || 'Unknown'}
- Original subject: ${body.originalSubject || '(unknown)'}
- Original email: ${(body.originalBody || '').slice(0, 800)}
- Reply classification: ${body.replyClassification || 'no reply yet'}
- Reply summary: ${body.replySummary || 'No reply received'}
- Persona: ${body.persona || 'Trusted Advisor'}

Strategy: ${strategy}

Rules:
- Be concise (3-4 paragraphs max)
- Sound natural and human, not templated
- Reference specific details from the original outreach
- ${body.stepNumber > 2 ? 'This is a later follow-up. Be brief and add fresh value.' : 'This is the first follow-up. Reference the original email naturally.'}
- Sign off as a Polsinelli attorney

Return ONLY valid JSON:
{
  "subject": "Re: ...",
  "body": "...",
  "reasoning": "Brief note on why this approach was chosen"
}`;

                const geminiRes = await fetchGemini(env, 'pro', {
                    activityName: 'generate-followup',
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        contents: [{ parts: [{ text: followUpPrompt }] }],
                        generationConfig: {
                            temperature: 0.7,
                            maxOutputTokens: 1500,
                            responseMimeType: 'application/json',
                            responseSchema: {
                                type: "OBJECT",
                                properties: {
                                    subject: { type: "STRING" },
                                    body: { type: "STRING" },
                                    reasoning: { type: "STRING" }
                                },
                                required: ["subject", "body", "reasoning"]
                            }
                        },
                    }),
                });

                if (!geminiRes.ok) {
                    return jsonWithCors({ error: 'Gemini follow-up generation failed' }, { status: 502 });
                }

                const { text: rawText } = await safeGeminiResponseParse(geminiRes);
                const rawTextSafe = rawText || '';

                const result = safeJsonParse(rawTextSafe, null) || { subject: `Follow-up: ${body.originalSubject || 'Our conversation'}`, body: rawTextSafe, reasoning: 'Raw Gemini output' };

                console.log(`📧 Follow-up generated for strike ${body.strikeId} step ${body.stepNumber}: ${result.subject}`);
                return jsonWithCors(result);
            } catch (error: any) {
                console.error('[generate-followup] Error:', error);
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

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
                                const { fetchGemini } = await import('./utils/gemini-fetch');
                                const geminiRes = await fetchGemini(env, 'lite', {
                                    activityName: 'evaluate-sequence-condition',
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
                                        generationConfig: {
                                            temperature: 0.2,
                                            maxOutputTokens: 1024,
                                            responseMimeType: 'application/json',
                                            responseSchema: {
                                                type: "OBJECT",
                                                properties: {
                                                    website: { type: "STRING" },
                                                    description: { type: "STRING" },
                                                    keyFacts: { type: "ARRAY", items: { type: "STRING" } },
                                                    recentNews: { type: "ARRAY", items: { type: "STRING" } },
                                                    industry: { type: "STRING" },
                                                    headquarters: { type: "STRING" }
                                                },
                                                required: ["website", "description", "industry"]
                                            }
                                        },
                                    }),
                                });

                                if (geminiRes.ok) {
                                    const { text: rawText } = await safeGeminiResponseParse(geminiRes);
                                    if (rawText) {
                                        companyIntel = safeJsonParse(rawText, null);
                                        if (companyIntel && companyIntel.description) {
                                            console.log(`✅ Company intel summarized: ${companyIntel.description.slice(0, 60)}...`);
                                        } else {
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

        // Mobile Strike Dispatch Endpoint
        if (url.pathname === '/api/mobile-dispatch' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    userId: string;
                    targetAudience: string;
                    market: string;
                    desiredOutcome: string;
                    informationToGather?: string;
                    sector?: string;
                    contactRequired?: boolean;
                    remainingCount: number;
                    watchlistDomain?: string;
                    watchlistTrigger?: any;
                };

                if (!body.userId || !body.targetAudience || !body.market || !body.desiredOutcome) {
                    return jsonWithCors({ error: 'Missing parameters: userId, targetAudience, market, desiredOutcome' }, { status: 400 });
                }

                const queueToUse = env.MOBILE_QUEUE || env.STRIKE_QUEUE;
                if (queueToUse) {
                    await queueToUse.send({
                        action: 'mobile_strike_generation',
                        userId: body.userId,
                        targetAudience: body.targetAudience,
                        market: body.market,
                        desiredOutcome: body.desiredOutcome,
                        informationToGather: body.informationToGather || '',
                        sector: body.sector || '',
                        contactRequired: !!body.contactRequired,
                        remainingCount: body.remainingCount || 1,
                        watchlistDomain: body.watchlistDomain || null,
                        watchlistTrigger: body.watchlistTrigger || null,
                    });
                    return jsonWithCors({ status: 'queued', userId: body.userId, count: body.remainingCount });
                }

                return jsonWithCors({ error: 'Queue not available' }, { status: 503 });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Deep-dive a single existing mobile strike (richer research, costs 1 credit).
        if (url.pathname === '/api/mobile-deep-dive' && request.method === 'POST') {
            try {
                const body = await request.json() as { userId: string; strikeId: number };
                if (!body.userId || !body.strikeId) {
                    return jsonWithCors({ error: 'Missing userId or strikeId' }, { status: 400 });
                }
                const queueToUse = env.MOBILE_QUEUE || env.STRIKE_QUEUE;
                if (!queueToUse) return jsonWithCors({ error: 'Queue not available' }, { status: 503 });
                await queueToUse.send({ action: 'mobile_deep_dive', userId: body.userId, strikeId: body.strikeId });
                return jsonWithCors({ status: 'queued', strikeId: body.strikeId });
            } catch (error) {
                return jsonWithCors({ error: String(error) }, { status: 500 });
            }
        }

        // Tailor pushed campaign strikes for a client (research + client-tailored draft).
        if (url.pathname === '/api/mobile-campaign-tailor' && request.method === 'POST') {
            try {
                const body = await request.json() as { userId: string; strikeIds: number[] };
                if (!body.userId || !Array.isArray(body.strikeIds) || body.strikeIds.length === 0) {
                    return jsonWithCors({ error: 'Missing userId or strikeIds' }, { status: 400 });
                }
                const queueToUse = env.MOBILE_QUEUE || env.STRIKE_QUEUE;
                if (!queueToUse) return jsonWithCors({ error: 'Queue not available' }, { status: 503 });
                await queueToUse.send({ action: 'mobile_campaign_tailor', userId: body.userId, strikeIds: body.strikeIds });
                return jsonWithCors({ status: 'queued', count: body.strikeIds.length });
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

                const { fetchGemini } = await import('./utils/gemini-fetch');

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

                        const geminiRes = await fetchGemini(env, 'lite', {
                            activityName: 'generate-variant',
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                contents: [{ parts: [{ text: prompt }] }],
                                generationConfig: {
                                    temperature: 0.8,
                                    maxOutputTokens: 2000,
                                    responseMimeType: 'application/json',
                                    responseSchema: {
                                        type: "OBJECT",
                                        properties: {
                                            subject: { type: "STRING" },
                                            body: { type: "STRING" }
                                        },
                                        required: ["subject", "body"]
                                    }
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

                        const { text: rawText } = await safeGeminiResponseParse(geminiRes);
                        const rawTextSafe = rawText || '';
                        let parsed: { subject: string; body: string };

                        parsed = safeJsonParse(rawTextSafe, null) || { subject: body.goldDraft.subject, body: rawTextSafe };

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

                            // Record engagement event for Intelligence closed-loop tracking
                            await insertRow(env, 'engagement_events', {
                                strike_id: campaignId,
                                event_type: 'reply',
                                metadata: {
                                    sender_name: senderName,
                                    sender_company: senderCompany,
                                    category: triageResult.category,
                                    confidence: triageResult.confidence,
                                },
                            });

                            // Update replied_at convenience column (only if not already set)
                            await patchRow(env, 'strike_campaigns', { replied_at: new Date().toISOString() }, 'id', campaignId);

                            // Bump conversion score (+5 for reply)
                            const scData = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                            if (scData && scData.length > 0) {
                                const currentScore = Number(scData[0].conversion_score) || 0;
                                await patchRow(env, 'strike_campaigns', { conversion_score: currentScore + 5 }, 'id', campaignId);
                            }

                            // If category is 'direct_strike' (high intent), auto-record meeting_booked engagement
                            if (triageResult.category === 'direct_strike') {
                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'meeting_booked',
                                    metadata: { auto_detected: true, source: 'reply_triage' },
                                });
                                await patchRow(env, 'strike_campaigns', { meeting_booked_at: new Date().toISOString() }, 'id', campaignId);
                                // +10 for meeting
                                const scData2 = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                                if (scData2 && scData2.length > 0) {
                                    const cs = Number(scData2[0].conversion_score) || 0;
                                    await patchRow(env, 'strike_campaigns', { conversion_score: cs + 10 }, 'id', campaignId);
                                }
                            }

                            console.log(`📊 Engagement event recorded: reply for strike ${campaignId}`);
                        }
                    } catch (err) {
                        console.error('❌ Webhook background processing error:', err);
                    }
                })()
            );

            // Smartlead gets 200 instantly — no timeout risk
            return Response.json({ status: 'accepted' });
        }

        // ── Amazon SES Inbound Reply Webhook (via SNS) ──
        if (url.pathname === '/api/webhook/ses' && request.method === 'POST') {
            const rawBody = await request.text();
            let snsMessage: any;

            try {
                snsMessage = JSON.parse(rawBody);
            } catch {
                return Response.json({ error: 'Invalid JSON' }, { status: 400 });
            }

            // Handle SNS SubscriptionConfirmation (auto-confirm)
            if (snsMessage.Type === 'SubscriptionConfirmation' && snsMessage.SubscribeURL) {
                console.log('[ses-webhook] Auto-confirming SNS subscription...');
                await fetch(snsMessage.SubscribeURL);
                return Response.json({ status: 'subscription_confirmed' });
            }

            // Handle SNS Notification (inbound email)
            if (snsMessage.Type === 'Notification') {
                ctx.waitUntil(
                    (async () => {
                        try {
                            const message = JSON.parse(snsMessage.Message);
                            const mailContent = message.mail || message.content || message;
                            const receipt = message.receipt || {};

                            // Extract email fields from SES inbound format
                            const fromHeader = mailContent.commonHeaders?.from?.[0] || mailContent.source || '';
                            const fromMatch = fromHeader.match(/^(?:"?([^"<]+)"?\s*)?<?([^>]+@[^>]+)>?$/);
                            const senderName = fromMatch?.[1]?.trim() || fromMatch?.[2] || 'Unknown';
                            const senderEmail = fromMatch?.[2] || fromHeader;
                            const senderCompany = senderEmail.split('@')[1]?.split('.')[0] || 'Unknown';
                            const subject = mailContent.commonHeaders?.subject || mailContent.subject || '(no subject)';
                            let replyBody = message.content || mailContent.content || '';

                            // Decode base64 Raw format if it comes from SES (Raw format is base64 encoded by SNS)
                            if (replyBody && /^[A-Za-z0-9+/=\s]+$/.test(replyBody) && replyBody.length > 20) {
                                try {
                                    // Use TextDecoder for proper UTF-8 handling from base64
                                    const rawStr = atob(replyBody.replace(/\s+/g, ''));
                                    const bytes = new Uint8Array(rawStr.length);
                                    for (let i = 0; i < rawStr.length; i++) bytes[i] = rawStr.charCodeAt(i);
                                    const decoded = new TextDecoder('utf-8').decode(bytes);
                                    
                                    // Simple extraction of the plain text part if it's multipart MIME
                                    const textPlainIndex = decoded.indexOf('Content-Type: text/plain');
                                    if (textPlainIndex !== -1) {
                                        let start = decoded.indexOf('\r\n\r\n', textPlainIndex);
                                        if (start === -1) start = decoded.indexOf('\n\n', textPlainIndex);
                                        start = start !== -1 ? start + (decoded[start] === '\r' ? 4 : 2) : textPlainIndex + 30;
                                        
                                        let textChunk = decoded.substring(start);
                                        const boundaryMatch = decoded.match(/boundary="?([^"\r\n]+)"?/);
                                        if (boundaryMatch) {
                                            const boundaryIndex = textChunk.indexOf('--' + boundaryMatch[1]);
                                            if (boundaryIndex !== -1) textChunk = textChunk.substring(0, boundaryIndex);
                                        }
                                        replyBody = textChunk.trim();
                                    } else {
                                        // Not clearly tagged, strip standard headers block
                                        const doubleNewline = decoded.indexOf('\r\n\r\n');
                                        if (doubleNewline !== -1 && doubleNewline < 3000) {
                                            replyBody = decoded.substring(doubleNewline + 4).trim();
                                        } else {
                                            replyBody = decoded.trim();
                                        }
                                    }
                                } catch (err) {
                                    console.warn('⚠️ Failed to decode SES base64 message content', err);
                                }
                            }

                            // Filter out automated bounces and delivery failures from hitting the Triage Inbox
                            const lowerSubject = subject.toLowerCase();
                            const isBounce = senderEmail.toLowerCase().startsWith('mailer-daemon') ||
                                             senderEmail.toLowerCase().startsWith('postmaster') ||
                                             lowerSubject.includes('delivery status notification') ||
                                             lowerSubject.includes('undeliverable') ||
                                             lowerSubject.includes('returned mail');
                            
                            if (isBounce) {
                                console.log(`🛑 Ignoring automated bounce in Triage Inbox: ${subject} from ${senderEmail}`);
                                return;
                            }

                            // Try to match to originating campaign via In-Reply-To header or subject
                            let campaignId = 0;
                            const inReplyTo = mailContent.commonHeaders?.inReplyTo || '';
                            if (inReplyTo) {
                                // Extract the core SES message ID by removing <>, and stripping the `@email.amazonses.com` domain
                                let cleanInReplyTo = inReplyTo.replace(/[<>]/g, '');
                                const atIndex = cleanInReplyTo.indexOf('@');
                                if (atIndex !== -1) {
                                    cleanInReplyTo = cleanInReplyTo.substring(0, atIndex);
                                }
                                
                                // Look up the exact core SES message ID in our campaigns
                                const rows = await fetchRow(env, 'strike_campaigns', 'ses_message_id', cleanInReplyTo);
                                if (rows?.[0]) {
                                    campaignId = Number(rows[0].id) || 0;
                                }
                            }

                            // Run through existing AI triage pipeline
                            const triageResult = await triageReply(env, {
                                senderName,
                                senderCompany,
                                subject,
                                body: replyBody,
                            });

                            // Look up strategic campaign for bucketing
                            let strategicCampaignId: string | null = null;
                            let assignedTo: string | null = null;
                            if (campaignId > 0) {
                                const scRows = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                                if (scRows?.[0]) {
                                    strategicCampaignId = scRows[0].campaign_id || null;
                                    assignedTo = scRows[0].assigned_to || null;
                                }
                            }

                            // Write to triage_replies (same as SmartLead webhook)
                            await insertRow(env, 'triage_replies', {
                                campaign_id: campaignId > 0 ? campaignId : null,
                                strategic_campaign_id: strategicCampaignId,
                                assigned_to: assignedTo,
                                sender_name: senderName,
                                sender_company: senderCompany,
                                sender_email: senderEmail,
                                subject,
                                body: replyBody,
                                category: triageResult.category,
                                confidence: triageResult.confidence,
                                preview: replyBody.slice(0, 200),
                                source: 'ses',
                            });

                            // Update campaign status + engagement tracking
                            if (campaignId > 0) {
                                await patchRow(env, 'strike_campaigns', { status: 'replied', replied_at: new Date().toISOString() }, 'id', campaignId);

                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'reply',
                                    metadata: {
                                        sender_name: senderName,
                                        sender_email: senderEmail,
                                        category: triageResult.category,
                                        confidence: triageResult.confidence,
                                        source: 'ses',
                                    },
                                });

                                // Bump conversion score (+5 for reply)
                                const scData = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                                if (scData?.[0]) {
                                    const currentScore = Number(scData[0].conversion_score) || 0;
                                    await patchRow(env, 'strike_campaigns', { conversion_score: currentScore + 5 }, 'id', campaignId);
                                    
                                    // Kill Switch: Reply received, halt all future sequence steps
                                    if (scData[0].funnel_id) {
                                        const isOptOut = triageResult.category === 'not_now';
                                        await patchRow(env, 'strike_funnels', { status: isOptOut ? 'halted_opt_out' : 'halted_replied' }, 'id', scData[0].funnel_id);
                                        console.log(`🛑 Funnel #${scData[0].funnel_id} Kill Switch triggered by Inbound Reply (Category: ${triageResult.category})`);
                                    }
                                }

                                // Auto-detect high intent
                                if (triageResult.category === 'direct_strike') {
                                    await insertRow(env, 'engagement_events', {
                                        strike_id: campaignId,
                                        event_type: 'meeting_booked',
                                        metadata: { auto_detected: true, source: 'ses_reply_triage' },
                                    });
                                    await patchRow(env, 'strike_campaigns', { meeting_booked_at: new Date().toISOString() }, 'id', campaignId);
                                }
                            }

                            console.log(`✅ SES inbound processed: ${senderName} → ${triageResult.category}`);
                        } catch (err) {
                            console.error('❌ SES webhook processing error:', err);
                        }
                    })()
                );

                return Response.json({ status: 'accepted' });
            }

            return Response.json({ status: 'ignored' });
        }

        // ── Amazon SES Event Webhook (bounces, complaints, opens, clicks via SNS) ──
        if (url.pathname === '/api/webhook/ses-events' && request.method === 'POST') {
            const rawBody = await request.text();
            let snsMessage: any;

            try {
                snsMessage = JSON.parse(rawBody);
            } catch {
                return Response.json({ error: 'Invalid JSON' }, { status: 400 });
            }

            // Auto-confirm SNS subscription
            if (snsMessage.Type === 'SubscriptionConfirmation' && snsMessage.SubscribeURL) {
                await fetch(snsMessage.SubscribeURL);
                return Response.json({ status: 'subscription_confirmed' });
            }

            if (snsMessage.Type === 'Notification') {
                ctx.waitUntil(
                    (async () => {
                        try {
                            const event = JSON.parse(snsMessage.Message);
                            const eventType = event.eventType || event.notificationType || '';
                            const sesMessageId = event.mail?.messageId || '';

                            // Find the campaign by SES message ID
                            let campaignId = 0;
                            if (sesMessageId) {
                                const rows = await fetchRow(env, 'strike_campaigns', 'ses_message_id', sesMessageId);
                                if (rows?.[0]) {
                                    campaignId = Number(rows[0].id) || 0;
                                }
                            }

                            if (campaignId <= 0) {
                                console.warn(`[ses-events] No campaign found for SES message: ${sesMessageId}`);
                                return;
                            }

                            // Handle each event type
                            if (eventType === 'Bounce' || eventType === 'bounce') {
                                const bounceType = event.bounce?.bounceType === 'Permanent' ? 'hard' : 'soft';
                                const bouncedRecipients = event.bounce?.bouncedRecipients || [];

                                await patchRow(env, 'strike_campaigns', {
                                    bounced_at: new Date().toISOString(),
                                    bounce_type: bounceType,
                                    status: 'bounced',
                                }, 'id', campaignId);

                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'bounce',
                                    metadata: { bounce_type: bounceType, recipients: bouncedRecipients },
                                });

                                // Flag bounced emails in lead_targets
                                for (const recipient of bouncedRecipients) {
                                    const email = recipient.emailAddress;
                                    if (email) {
                                        // Find lead_target by email and flag it
                                        const lookupRes = await fetch(
                                            `${env.SUPABASE_URL}/rest/v1/lead_targets?enrichment_data->>email=eq.${encodeURIComponent(email)}&select=id`,
                                            {
                                                headers: {
                                                    'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                                                    'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                                },
                                            }
                                        );
                                        if (lookupRes.ok) {
                                            const leads = await lookupRes.json() as any[];
                                            for (const lead of leads) {
                                                await patchRow(env, 'lead_targets', {
                                                    email_status: bounceType === 'hard' ? 'bounced' : 'soft_bounce',
                                                    email_status_updated_at: new Date().toISOString(),
                                                }, 'id', lead.id);
                                            }
                                        }
                                        
                                        // Flag in suppression list to prevent future sends
                                        await insertRow(env, 'suppression_list', {
                                            email,
                                            reason: bounceType === 'hard' ? 'SES Hard Bounce' : 'SES Soft Bounce',
                                        });
                                    }
                                }

                                console.log(`📮 Bounce (${bounceType}) recorded for campaign ${campaignId}`);

                            } else if (eventType === 'Complaint' || eventType === 'complaint') {
                                const complainedRecipients = event.complaint?.complainedRecipients || [];

                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'complaint',
                                    metadata: { recipients: complainedRecipients },
                                });

                                // Flag complained emails
                                for (const recipient of complainedRecipients) {
                                    const email = recipient.emailAddress;
                                    if (email) {
                                        const lookupRes = await fetch(
                                            `${env.SUPABASE_URL}/rest/v1/lead_targets?enrichment_data->>email=eq.${encodeURIComponent(email)}&select=id`,
                                            {
                                                headers: {
                                                    'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
                                                    'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                                },
                                            }
                                        );
                                        if (lookupRes.ok) {
                                            const leads = await lookupRes.json() as any[];
                                            for (const lead of leads) {
                                                await patchRow(env, 'lead_targets', {
                                                    email_status: 'complained',
                                                    email_status_updated_at: new Date().toISOString(),
                                                }, 'id', lead.id);
                                            }
                                        }
                                        
                                        // Add explicit suppression
                                        await insertRow(env, 'suppression_list', {
                                            email,
                                            reason: 'SES SPAM Complaint',
                                        });
                                    }
                                }

                                console.log(`🚫 Complaint recorded for campaign ${campaignId}`);

                            } else if (eventType === 'Delivery' || eventType === 'delivery') {
                                await patchRow(env, 'strike_campaigns', {
                                    delivered_at: new Date().toISOString(),
                                }, 'id', campaignId);

                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'delivery',
                                    metadata: {},
                                });

                                console.log(`✉️ Delivery confirmed for campaign ${campaignId}`);

                            } else if (eventType === 'Open' || eventType === 'open') {
                                await patchRow(env, 'strike_campaigns', {
                                    opened_at: new Date().toISOString(),
                                }, 'id', campaignId);

                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'open',
                                    metadata: {},
                                });

                                // Bump conversion score (+1 for open)
                                const scData = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                                if (scData?.[0]) {
                                    const cs = Number(scData[0].conversion_score) || 0;
                                    await patchRow(env, 'strike_campaigns', { conversion_score: cs + 1 }, 'id', campaignId);
                                }

                                console.log(`👁️ Open tracked for campaign ${campaignId}`);

                            } else if (eventType === 'Click' || eventType === 'click') {
                                await patchRow(env, 'strike_campaigns', {
                                    clicked_at: new Date().toISOString(),
                                }, 'id', campaignId);

                                await insertRow(env, 'engagement_events', {
                                    strike_id: campaignId,
                                    event_type: 'click',
                                    metadata: { link: event.click?.link || '' },
                                });

                                // Bump conversion score (+2 for click)
                                const scData = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                                if (scData?.[0]) {
                                    const cs = Number(scData[0].conversion_score) || 0;
                                    await patchRow(env, 'strike_campaigns', { conversion_score: cs + 2 }, 'id', campaignId);
                                }

                                console.log(`🔗 Click tracked for campaign ${campaignId}`);
                            }
                        } catch (err) {
                            console.error('❌ SES event processing error:', err);
                        }
                    })()
                );

                return Response.json({ status: 'accepted' });
            }

            return Response.json({ status: 'ignored' });
        }

        // Engagement tracking webhook — handles opens, clicks, bounces from SmartLead or any source
        if (url.pathname === '/api/webhook/engagement' && request.method === 'POST') {
            const body = await request.json() as {
                campaign_id?: number | string;
                event_type?: string;        // 'open', 'click', 'bounce', 'unsubscribe'
                lead_email?: string;
                metadata?: Record<string, any>;
            };

            const campaignId = typeof body.campaign_id === 'string'
                ? parseInt(body.campaign_id, 10)
                : body.campaign_id ?? 0;

            const eventType = body.event_type || 'open';

            if (campaignId <= 0) {
                return jsonWithCors({ error: 'Invalid campaign_id' }, { status: 400 });
            }

            ctx.waitUntil(
                (async () => {
                    try {
                        // Record engagement event
                        await insertRow(env, 'engagement_events', {
                            strike_id: campaignId,
                            event_type: eventType,
                            metadata: {
                                lead_email: body.lead_email || null,
                                ...(body.metadata || {}),
                            },
                        });

                        // Update convenience columns  
                        const { patchRow } = await import('./utils/supabase');
                        const colMap: Record<string, string> = {
                            open: 'opened_at',
                            click: 'clicked_at',
                        };
                        const col = colMap[eventType];
                        if (col) {
                            await patchRow(env, 'strike_campaigns', { [col]: new Date().toISOString() }, 'id', campaignId);
                        }

                        // Bump conversion score
                        const scoreMap: Record<string, number> = { open: 1, click: 2, bounce: 0 };
                        const addScore = scoreMap[eventType] ?? 0;
                        if (addScore > 0) {
                            const scData = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                            if (scData && scData.length > 0) {
                                const currentScore = Number(scData[0].conversion_score) || 0;
                                await patchRow(env, 'strike_campaigns', { conversion_score: currentScore + addScore }, 'id', campaignId);
                            }
                        }

                        console.log(`📊 Engagement ${eventType} recorded for strike ${campaignId}`);
                    } catch (err) {
                        console.error(`❌ Engagement webhook error:`, err);
                    }
                })()
            );

            return Response.json({ status: 'accepted', event_type: eventType, campaign_id: campaignId });
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

                // ── Handle mobile_deep_dive (richer research on one existing strike) ──
                if (msg.body?.action === 'mobile_deep_dive') {
                    const { userId, strikeId } = msg.body;
                    try {
                        const hdr = { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` };
                        const scRes = await fetch(
                            `${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=eq.${strikeId}&mobile_user_id=eq.${userId}&select=id,mobile_search_label,lead_targets(company,executive_name,executive_title,trigger_event,enrichment_data)`,
                            { headers: hdr }
                        );
                        const scRows = scRes.ok ? await scRes.json() as any[] : [];
                        if (!scRows.length) { console.warn(`Deep-dive: strike ${strikeId} not found for user ${userId}`); msg.ack(); continue; }
                        const lt = scRows[0].lead_targets || {};
                        const ed = lt.enrichment_data || {};
                        const profileRows = await fetchRow(env, 'mobile_user_profiles', 'id', userId);
                        const prof = (profileRows && profileRows[0]) || {};
                        const [aud, mkt] = String(scRows[0].mobile_search_label || ' · ').split(' · ');

                        const research = await synthesizeMobileResearch(env, {
                            company: lt.company,
                            industry: ed.industry,
                            trigger: lt.trigger_event,
                            enrichmentContext: [
                                ed.revenue ? `Revenue: ${ed.revenue}` : '',
                                ed.employees ? `Employees: ${ed.employees}` : '',
                                Array.isArray(ed.signals) && ed.signals.length ? `Signals: ${ed.signals.join('; ')}` : '',
                                ed.executive_research ? `Exec background: ${ed.executive_research}` : '',
                            ].filter(Boolean).join(' | '),
                            primaryContactName: lt.executive_name,
                            primaryContactTitle: lt.executive_title,
                            senderCompany: prof.company_name || '',
                            senderNarrative: prof.core_services_narrative || '',
                            targetAudience: aud || '',
                            market: mkt || '',
                            desiredOutcome: '',
                            informationToGather: 'DEEP DIVE — surface the most useful intel available: recent funding/M&A, leadership changes, expansion, hiring, partnerships, and risks; and identify the full buying committee.',
                            idealCustomerProfile: prof.ideal_customer_profile || '',
                            exclusions: prof.exclusions || '',
                        });

                        const contacts = [
                            { name: lt.executive_name || 'Unknown', title: lt.executive_title || 'Executive', email: ed.email || '', phone: ed.phone || '', linkedin: ed.linkedin_url || '', verified: !!ed.email },
                            ...research.additionalContacts.map(c => ({ name: c.name, title: c.title, email: '', phone: '', linkedin: '', verified: false })),
                        ];
                        await patchRow(env, 'strike_campaigns', {
                            mobile_relevance_score: research.relevanceScore,
                            mobile_relevance_reason: research.relevanceReason,
                            mobile_research_findings: JSON.stringify(research.researchFindings),
                            mobile_contacts: JSON.stringify(contacts),
                        }, 'id', strikeId);

                        const balRows = await fetchRow(env, 'mobile_strike_balances', 'user_id', userId);
                        const cur = balRows && balRows[0] ? balRows[0] : null;
                        if (cur) await patchRow(env, 'mobile_strike_balances', { credits_used: (cur.credits_used || 0) + 1, updated_at: new Date().toISOString() }, 'user_id', userId);

                        console.log(`🔬 Deep-dive complete for strike #${strikeId} (user ${userId}).`);
                        msg.ack();
                    } catch (deepErr) {
                        console.error('❌ Deep-dive failed:', deepErr);
                        msg.retry();
                    }
                    continue;
                }

                // ── Handle mobile_campaign_tailor (research + client-tailored draft for pushed strikes) ──
                if (msg.body?.action === 'mobile_campaign_tailor') {
                    const { userId, strikeIds } = msg.body;
                    try {
                        const hdr = { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` };
                        const profileRows = await fetchRow(env, 'mobile_user_profiles', 'id', userId);
                        const prof = (profileRows && profileRows[0]) || {};
                        for (const strikeId of (strikeIds || [])) {
                            try {
                                const scRes = await fetch(
                                    `${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=eq.${strikeId}&mobile_user_id=eq.${userId}&select=id,mobile_search_label,lead_targets(company,executive_name,executive_title,trigger_event,enrichment_data)`,
                                    { headers: hdr }
                                );
                                const scRows = scRes.ok ? await scRes.json() as any[] : [];
                                if (!scRows.length) continue;
                                const lt = scRows[0].lead_targets || {};
                                const ed = lt.enrichment_data || {};
                                const [aud, mkt] = String(scRows[0].mobile_search_label || ' · ').split(' · ');
                                const enrichmentContext = [
                                    ed.revenue ? `Revenue: ${ed.revenue}` : '',
                                    ed.employees ? `Employees: ${ed.employees}` : '',
                                    Array.isArray(ed.signals) && ed.signals.length ? `Signals: ${ed.signals.join('; ')}` : '',
                                    ed.executive_research ? `Exec background: ${ed.executive_research}` : '',
                                ].filter(Boolean).join(' | ');

                                const research = await synthesizeMobileResearch(env, {
                                    company: lt.company, industry: ed.industry, trigger: lt.trigger_event,
                                    enrichmentContext, primaryContactName: lt.executive_name, primaryContactTitle: lt.executive_title,
                                    senderCompany: prof.company_name || '', senderNarrative: prof.core_services_narrative || '',
                                    targetAudience: aud || '', market: mkt || '', desiredOutcome: '',
                                    informationToGather: 'Surface the most useful intel and the full buying committee for this shared opportunity.',
                                    idealCustomerProfile: prof.ideal_customer_profile || '', exclusions: prof.exclusions || '',
                                });
                                const draft = await generateMobileDraft(env, {
                                    lead: { executiveName: lt.executive_name, executiveTitle: lt.executive_title } as any,
                                    senderName: prof.full_name || '', senderCompany: prof.company_name || '',
                                    senderNarrative: prof.core_services_narrative || '', desiredOutcome: '',
                                    targetCompany: lt.company, triggerContext: lt.trigger_event || '',
                                });
                                const contacts = [
                                    { name: lt.executive_name || 'Unknown', title: lt.executive_title || 'Executive', email: ed.email || '', phone: ed.phone || '', linkedin: ed.linkedin_url || '', verified: !!ed.email },
                                    ...research.additionalContacts.map(c => ({ name: c.name, title: c.title, email: '', phone: '', linkedin: '', verified: false })),
                                ];
                                await patchRow(env, 'strike_campaigns', {
                                    email_subject: draft.subject,
                                    drafted_body: draft.body,
                                    mobile_relevance_score: research.relevanceScore,
                                    mobile_relevance_reason: research.relevanceReason,
                                    mobile_research_findings: JSON.stringify(research.researchFindings),
                                    mobile_contacts: JSON.stringify(contacts),
                                }, 'id', strikeId);
                            } catch (oneErr) {
                                console.error(`Campaign-tailor strike ${strikeId} failed:`, oneErr);
                            }
                        }
                        console.log(`🎁 Campaign push tailored ${(strikeIds || []).length} strikes for user ${userId}.`);
                        msg.ack();
                    } catch (tailErr) {
                        console.error('❌ Campaign tailor failed:', tailErr);
                        msg.retry();
                    }
                    continue;
                }

                // ── Handle mobile_strike_generation ──
                if (msg.body?.action === 'mobile_strike_generation') {
                    const { userId, targetAudience, market, desiredOutcome, informationToGather, sector, contactRequired, remainingCount, watchlistDomain, watchlistTrigger } = msg.body;
                    console.log(`📱 Priority Mobile Strike Generation: User #${userId} | Audience: ${targetAudience} | Market: ${market} | Limit: ${remainingCount}`);

                    try {
                        // 1. Fetch user onboarding profile for service narrative & details
                        const profileRows = await fetchRow(env, 'mobile_user_profiles', 'id', userId);
                        if (!profileRows || profileRows.length === 0) {
                            console.error(`❌ Mobile profile not found for user ${userId}`);
                            msg.ack();
                            continue;
                        }
                        const profile = profileRows[0];
                        const servicesNarrative = profile.core_services_narrative;
                        const companyName = profile.company_name;

                        // LEARNING LOOP: bias discovery toward companies the user has previously SAVED.
                        let learningHint = '';
                        try {
                            const savedRes = await fetch(
                                `${env.SUPABASE_URL}/rest/v1/strike_campaigns?mobile_user_id=eq.${userId}&mobile_feedback_status=eq.approved&select=lead_targets(company)&order=mobile_delivered_at.desc.nullslast&limit=12`,
                                { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                            );
                            if (savedRes.ok) {
                                const saved = await savedRes.json() as any[];
                                const names = saved.map(r => r.lead_targets?.company).filter(Boolean).slice(0, 12);
                                if (names.length) {
                                    learningHint = `The user has previously SAVED these as strong matches — strongly prefer companies similar in sector/size/type: ${names.join(', ')}.`;
                                    console.log(`🧠 Learning loop: ${names.length} saved examples informing discovery for User #${userId}.`);
                                }
                            }
                        } catch (learnErr) {
                            console.warn('⚠️ Learning-loop fetch failed (non-fatal):', learnErr);
                        }

                        const uniqueResults: any[] = [];

                        if (watchlistDomain && watchlistTrigger) {
                            console.log(`🎯 Watchlist triggered for User #${userId} on domain: ${watchlistDomain}`);
                            uniqueResults.push({
                                companyName: watchlistTrigger.company || watchlistDomain.split('.')[0],
                                companyDomain: watchlistDomain,
                                executiveName: watchlistTrigger.executiveName || 'Unknown',
                                executiveTitle: watchlistTrigger.executiveTitle || 'Key Decision-Maker',
                                triggerEvent: watchlistTrigger.headline || watchlistTrigger.articleText || `Watchlist signal detected for ${watchlistDomain}.`,
                                triggerSource: watchlistTrigger.sourceUrl || null,
                                dataSource: watchlistTrigger.source || 'Watchlist Monitor'
                            });
                        } else {
                            // ── Exa discovery cache (cost saver) ──
                            // The candidate company list for a given request is user-independent, so we
                            // cache it for 14 days. Identical or recurring searches reuse it and skip BOTH
                            // the Exa search calls AND the Gemini query-gen/discovery calls entirely.
                            const discKey = 'disc:' + cacheHash([targetAudience, market, sector || '', desiredOutcome, informationToGather || ''].join('|').toLowerCase().trim());
                            const cachedDisc = await getResearchCache(env, discKey);
                            if (Array.isArray(cachedDisc) && cachedDisc.length > 0) {
                                uniqueResults.push(...cachedDisc);
                                console.log(`♻️ Exa discovery cache HIT for "${targetAudience} · ${market}" (${cachedDisc.length} candidates) — skipped Exa + Gemini discovery (no API spend).`);
                            } else {
                            // 2. Generate search queries optimized for Exa
                            const queryGenSystem = `You are a search query engineering expert. Given a target audience and a geographic market, generate 2 optimized search queries for Exa.ai (neural search engine) to find target companies and their websites.
Keep them simple, using natural language or neural-search friendly phrases.
Return EXACTLY a JSON object with a "queries" array:
{
  "queries": ["query 1", "query 2"]
}`;
                            const queryGenPrompt = `Target Audience: ${targetAudience}\nMarket: ${market}`;
                            
                            let queries = [`companies matching ${targetAudience} in ${market}`];
                            try {
                                const geminiRes = await fetchGemini(env, 'lite', {
                                    activityName: 'mobile-query-generation',
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        system_instruction: { parts: [{ text: queryGenSystem }] },
                                        contents: [{ role: 'user', parts: [{ text: queryGenPrompt }] }],
                                        generationConfig: {
                                            temperature: 0.2,
                                            maxOutputTokens: 200,
                                            responseMimeType: 'application/json',
                                        }
                                    })
                                });
                                if (geminiRes.ok) {
                                    const parsed = await safeGeminiResponseParse(geminiRes);
                                    const parsedJson = safeJsonParse<any>(parsed.text, {});
                                    if (Array.isArray(parsedJson.queries) && parsedJson.queries.length > 0) {
                                        queries = parsedJson.queries;
                                    }
                                }
                            } catch (err) {
                                console.warn('⚠️ Gemini query generation failed, using fallback query:', err);
                            }

                            console.log(`📡 Exa search queries generated: ${JSON.stringify(queries)}`);

                            // 3. Search Exa & Free Resources for matching companies in parallel
                            const allExaResults: any[] = [];
                            const freeTriggers: any[] = [];

                            // Over-fetch candidates so that after dedup + enrichment failures we still
                            // have enough fresh targets to fill the requested count — but cap it tightly,
                            // since Exa bills per page of text returned. 2× the order with a 15–50 band
                            // (was 3×, min 30, uncapped) meaningfully cuts Exa spend; Gemini discovery and
                            // the free gov sensors also contribute candidates to fill the order.
                            const exaPerQuery = Math.min(Math.max(remainingCount * 2, 15), 50);
                            console.log(`🔑 EXA_API_KEY ${env.EXA_API_KEY ? 'present' : 'MISSING — Exa will return nothing'}; requesting ${exaPerQuery} results/query for ${remainingCount} target(s)`);

                            for (const query of queries) {
                                try {
                                    const exaPromise = fetch('https://api.exa.ai/search', {
                                        method: 'POST',
                                        headers: {
                                            'Content-Type': 'application/json',
                                            'x-api-key': env.EXA_API_KEY || '',
                                        },
                                        body: JSON.stringify({
                                            query,
                                            numResults: exaPerQuery,
                                            type: 'neural',
                                            useAutoprompt: true,
                                            excludeDomains: ['wikipedia.org', 'reddit.com', 'quora.com', 'investopedia.com'],
                                            contents: {
                                                text: { maxCharacters: 1000 }
                                            }
                                        })
                                    });

                                    const freePromise = Promise.allSettled([
                                        senseSecBulkForQuery(env, query),
                                        senseFederalRegisterForQuery(env, query),
                                        senseWarnDirectForQuery(env, query),
                                        senseUsptoDirectForQuery(env, query),
                                    ]);

                                    const [exaRes, freeRes] = await Promise.all([exaPromise, freePromise]);

                                    if (exaRes.ok) {
                                        const data = await exaRes.json() as any;
                                        const got = data?.results?.length || 0;
                                        console.log(`📡 Exa "${query}" → ${got} results`);
                                        if (data?.results) {
                                            allExaResults.push(...data.results);
                                        }
                                    } else {
                                        const errBody = await exaRes.text().catch(() => '');
                                        console.error(`❌ Exa search "${query}" failed: HTTP ${exaRes.status} ${errBody.slice(0, 300)}`);
                                    }

                                    let freeCount = 0;
                                    freeRes.forEach(r => {
                                        if (r.status === 'fulfilled' && Array.isArray(r.value)) {
                                            freeCount += r.value.length;
                                            freeTriggers.push(...r.value);
                                        }
                                    });
                                    console.log(`🏛️ Free sensors "${query}" → ${freeCount} triggers`);
                                } catch (err) {
                                    console.warn(`⚠️ Search failed for query "${query}":`, err);
                                }
                            }
                            console.log(`📊 Mobile search totals: ${allExaResults.length} Exa + ${freeTriggers.length} free triggers (before dedup)`);

                            // Gemini company discovery — audience-matched REAL companies. This is the
                            // relevance backstop: when Exa is unavailable/insufficient, government-filing
                            // sensors alone return off-target entities (funds, SPACs) that don't match the
                            // requested audience. Gemini supplies actual companies that fit the brief.
                            const geminiCompanies: any[] = [];
                            try {
                                const discSystem = `You are a B2B prospecting engine. Given a target audience and a market/geography, list REAL, currently-operating companies that genuinely match. Return ONLY a JSON object: {"companies":[{"name":"Company Name","domain":"company.com","reason":"one short reason they fit"}]}. Use real companies and their real primary website domain. Do NOT return SEC filing entities, investment funds, or SPACs unless the audience explicitly asks for them.`;
                                // Cap the count so the JSON response can't truncate; over-fill modestly.
                                const discCount = Math.min(Math.max(remainingCount + 5, 12), 25);
                                const discPrompt = `Target audience: ${targetAudience}\nMarket / geography: ${market}\n${sector ? `Industry sector (companies MUST operate in this sector): ${sector}\n` : ''}Desired outcome: ${desiredOutcome}\nInformation focus: ${informationToGather || 'general fit'}\n${profile.ideal_customer_profile ? `Ideal customer profile: ${profile.ideal_customer_profile}\n` : ''}${profile.exclusions ? `AVOID: ${profile.exclusions}\n` : ''}${learningHint ? learningHint + '\n' : ''}Return up to ${discCount} companies.`;
                                const discRes = await fetchGemini(env, 'lite', {
                                    activityName: 'mobile-company-discovery',
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({
                                        system_instruction: { parts: [{ text: discSystem }] },
                                        contents: [{ role: 'user', parts: [{ text: discPrompt }] }],
                                        generationConfig: {
                                            temperature: 0.4,
                                            maxOutputTokens: 4096,
                                            responseMimeType: 'application/json',
                                            responseSchema: {
                                                type: 'OBJECT',
                                                properties: {
                                                    companies: {
                                                        type: 'ARRAY',
                                                        items: {
                                                            type: 'OBJECT',
                                                            properties: {
                                                                name: { type: 'STRING' },
                                                                domain: { type: 'STRING' },
                                                                reason: { type: 'STRING' },
                                                            },
                                                            required: ['name'],
                                                        },
                                                    },
                                                },
                                                required: ['companies'],
                                            },
                                        },
                                    }),
                                });
                                if (discRes.ok) {
                                    const { text: discText, finishReason } = await safeGeminiResponseParse(discRes);
                                    // safeJsonParse repairs minor truncation; log if the model hit the token ceiling.
                                    if (finishReason === 'MAX_TOKENS') console.warn('⚠️ Gemini discovery hit MAX_TOKENS — parsing what we got.');
                                    const json = safeJsonParse<{ companies?: any[] }>(discText || '', { companies: [] });
                                    if (Array.isArray(json?.companies)) geminiCompanies.push(...json.companies);
                                } else {
                                    console.error(`❌ Gemini discovery failed: HTTP ${discRes.status}`);
                                }
                                console.log(`🤖 Gemini discovery → ${geminiCompanies.length} candidate companies`);
                            } catch (discErr) {
                                console.warn('⚠️ Gemini company discovery failed:', discErr);
                            }

                            // Deduplicate and merge targets. Priority order = Exa → Gemini discovery →
                            // free government sensors, so audience-relevant sources fill the requested
                            // count first and noisy filing data is only used to top up leftover slots.
                            const seenDomains = new Set<string>();

                            // Parse Exa search results
                            for (const result of allExaResults) {
                                try {
                                    const domain = normalizeDomain(result.title || '', null, result.url);
                                    if (!seenDomains.has(domain)) {
                                        seenDomains.add(domain);
                                        uniqueResults.push({
                                            companyName: result.title ? result.title.split('-')[0].split('|')[0].trim() : domain.split('.')[0],
                                            companyDomain: domain,
                                            executiveName: 'Unknown',
                                            executiveTitle: 'Key Decision-Maker',
                                            triggerEvent: result.text || `Target matching ${targetAudience} located via Exa.ai.`,
                                            triggerSource: result.url || null,
                                            dataSource: 'Exa.ai Mobile Search'
                                        });
                                    }
                                } catch (_) {}
                            }

                            // Parse Gemini-discovered companies (audience-matched, second priority)
                            for (const g of geminiCompanies) {
                                try {
                                    const company = (g.name || '').trim();
                                    if (!company) continue;
                                    const domain = normalizeDomain(company, g.domain ? { company_domain: g.domain } : null);
                                    if (!seenDomains.has(domain)) {
                                        seenDomains.add(domain);
                                        uniqueResults.push({
                                            companyName: company,
                                            companyDomain: domain,
                                            executiveName: 'Unknown',
                                            executiveTitle: 'Key Decision-Maker',
                                            triggerEvent: g.reason || `Strong fit for target audience "${targetAudience}" in ${market}.`,
                                            triggerSource: null,
                                            dataSource: 'Gemini Discovery'
                                        });
                                    }
                                } catch (_) {}
                            }

                            // Parse free triggers
                            for (const trg of freeTriggers) {
                                try {
                                    // Derive the TARGET company's domain from its name — never from the
                                    // data-source URL (that yielded sec.gov / federalregister.gov). The
                                    // enrichment step below can still upgrade this to a verified domain.
                                    let domain = normalizeDomain(trg.company, null, null);
                                    if (!seenDomains.has(domain)) {
                                        seenDomains.add(domain);
                                        uniqueResults.push({
                                            companyName: trg.company,
                                            companyDomain: domain,
                                            executiveName: trg.executiveName || 'Unknown',
                                            executiveTitle: trg.executiveTitle || 'Key Decision-Maker',
                                            triggerEvent: trg.headline || trg.articleText || `Opportunity discovered in public records: ${trg.source}`,
                                            triggerSource: trg.sourceUrl || null,
                                            dataSource: trg.source || 'Free Register Scrape'
                                        });
                                    }
                                } catch (_) {}
                            }

                            // Cache the merged candidate list (user-independent) so repeat/similar
                            // searches reuse it for 14 days instead of re-paying Exa + Gemini.
                            if (uniqueResults.length > 0) {
                                await setResearchCache(env, discKey, uniqueResults, 14);
                                console.log(`💾 Cached ${uniqueResults.length} discovery candidates for 14 days (key ${discKey}).`);
                            }
                            } // end discovery cache-miss branch
                        }

                        console.log(`🧹 Unified search found ${uniqueResults.length} unique domains (Exa → Gemini discovery → Free Government/State Sensors, in priority order).`);

                        // 4. Fetch already delivered domains for this user to deduplicate
                        const deliveredRes = await fetch(
                            `${env.SUPABASE_URL}/rest/v1/mobile_delivered_strikes_log?user_id=eq.${userId}&select=company_domain`,
                            { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                        );
                        const deliveredLogs = deliveredRes.ok ? await deliveredRes.json() as any[] : [];
                        const deliveredDomains = new Set(deliveredLogs.map(l => l.company_domain.toLowerCase()));

                        // Filter out already delivered domains
                        const freshResults = uniqueResults.filter(r => !deliveredDomains.has(r.companyDomain));
                        console.log(`🆕 ${freshResults.length} targets are fresh for User #${userId}`);

                        // 5. Generate strikes up to remainingCount.
                        // Every DELIVERED strike is billed (1 credit). For normal searches we only
                        // deliver HIGH-CONFIDENCE (audience-matched: Exa / Gemini) results — no free
                        // off-audience filler. Watchlist alerts are a single targeted hit and are billed too.
                        const isWatchlist = !!watchlistDomain;
                        let generatedCount = 0;
                        for (const target of freshResults) {
                            if (generatedCount >= remainingCount) break;

                            const isHighConfidence = target.dataSource === 'Exa.ai Mobile Search'
                                || target.dataSource === 'Gemini Discovery';

                            // Skip low-confidence filler for normal searches BEFORE spending any money
                            // on enrichment/synthesis. (Watchlist hits always proceed.)
                            if (!isWatchlist && !isHighConfidence) continue;

                            const companyDomain = target.companyDomain;
                            const companyCleanName = target.companyName;

                            console.log(`⚡ Processing fresh target company: ${companyCleanName} (${companyDomain})`);

                            // Check if company exists in lead_targets already so we can REUSE its
                            // enrichment (Apollo contact, revenue, signals) and skip the paid enrichLead()
                            // call. We match by DOMAIN first (reliable across name variants like
                            // "Acme" / "Acme Corp" / "Acme, Inc."), then fall back to an exact name match.
                            const leadSelect = 'select=id,executive_name,executive_title,enrichment_data&limit=1';
                            const leadHeaders = { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } };
                            let existingLeads: any[] = [];
                            if (companyDomain) {
                                const encDomain = encodeURIComponent(companyDomain);
                                const byDomainRes = await fetch(
                                    `${env.SUPABASE_URL}/rest/v1/lead_targets?or=(enrichment_data->>company_domain.eq.${encDomain},enrichment_data->>companyDomain.eq.${encDomain})&${leadSelect}`,
                                    leadHeaders
                                );
                                if (byDomainRes.ok) existingLeads = await byDomainRes.json() as any[];
                            }
                            if (existingLeads.length === 0) {
                                const byNameRes = await fetch(
                                    `${env.SUPABASE_URL}/rest/v1/lead_targets?company=ilike.${encodeURIComponent(companyCleanName)}&${leadSelect}`,
                                    leadHeaders
                                );
                                if (byNameRes.ok) existingLeads = await byNameRes.json() as any[];
                            }
                            if (existingLeads.length > 0) {
                                console.log(`♻️ Reusing cached enrichment for ${companyCleanName} (${companyDomain}) — skipped Apollo/enrich spend.`);
                            }
                            
                            let leadId: number | null = null;
                            let enrichedLead: any = null;

                            if (existingLeads.length > 0) {
                                const lead = existingLeads[0];
                                leadId = lead.id;
                                const ed = lead.enrichment_data || {};
                                enrichedLead = {
                                    company: lead.company,
                                    executiveName: lead.executive_name,
                                    executiveTitle: lead.executive_title,
                                    companyRevenue: ed.revenue || 'Unknown',
                                    employeeCount: ed.employees || 'Unknown',
                                    companyDomain: ed.companyDomain || ed.company_domain || companyDomain,
                                    signals: ed.signals || [],
                                    linkedinUrl: ed.linkedin_url || '',
                                    email: ed.email || '',
                                    phone: ed.phone || '',
                                    emailConfidence: ed.emailConfidence || 'none',
                                    emailSource: ed.emailSource || '',
                                    executiveResearch: ed.executive_research || '',
                                };
                            } else {
                                // Run full multi-layer enrichment pipeline
                                try {
                                    enrichedLead = await enrichLead(env, {
                                        company: companyCleanName,
                                        executiveName: target.executiveName || 'Unknown',
                                        executiveTitle: target.executiveTitle || 'Key Decision-Maker',
                                    });
                                } catch (enrichErr) {
                                    console.warn(`⚠️ Enrichment failed for ${companyCleanName}:`, enrichErr);
                                    enrichedLead = {
                                        company: companyCleanName,
                                        executiveName: target.executiveName || 'Key Decision-Maker',
                                        executiveTitle: target.executiveTitle || 'Executive',
                                        companyRevenue: 'Unknown',
                                        employeeCount: 'Unknown',
                                        companyDomain,
                                        signals: [],
                                        linkedinUrl: '',
                                    };
                                }
                            }

                            // Resolve the best real company domain: prefer the enrichment-verified
                            // domain, then the candidate's own domain (real for Exa, name-derived for
                            // free-register hits). Never the data-source domain.
                            const resolvedDomain = (
                                (enrichedLead?.companyDomain && enrichedLead.companyDomain.trim())
                                || (companyDomain && companyDomain.trim())
                                || normalizeDomain(companyCleanName, null)
                            );

                            // Contact-critical (#1): when the user flags contact info as required and
                            // enrichment didn't surface an email/phone, try to discover a REAL published
                            // contact from the company's own website before deciding. We extract only
                            // addresses that actually appear on the site — never a fabricated pattern —
                            // so a delivered contact is always verifiable.
                            let hasRealContact = !!(enrichedLead.email || enrichedLead.phone);
                            if (contactRequired && !hasRealContact && resolvedDomain) {
                                try {
                                    const discovered = await discoverPublicContact(resolvedDomain);
                                    if (discovered.email) {
                                        enrichedLead.email = discovered.email;
                                        enrichedLead.emailConfidence = 'website';
                                        enrichedLead.emailSource = discovered.source;
                                        hasRealContact = true;
                                    } else if (discovered.phone) {
                                        enrichedLead.phone = discovered.phone;
                                        hasRealContact = true;
                                    }
                                    if (hasRealContact) console.log(`🔎 Discovered website contact for ${companyCleanName}: ${enrichedLead.email || enrichedLead.phone}`);
                                } catch (e) {
                                    console.warn(`⚠️ Contact discovery failed for ${resolvedDomain}:`, e);
                                }
                            }

                            // Hard gate: never deliver (or bill for) a contactless strike when the user
                            // marked contact info critical. Skipping here means no synthesis, no draft,
                            // no insert, and no credit charged for this company.
                            if (contactRequired && !hasRealContact) {
                                console.log(`⏭️ Skipping ${companyCleanName} — no verified contact and contactRequired=true (not billed).`);
                                continue;
                            }

                            // Save to lead_targets if it's new
                            if (!leadId) {
                                const targetRes = await insertRow(env, 'lead_targets', {
                                    company: enrichedLead.company || companyCleanName,
                                    executive_name: enrichedLead.executiveName || 'Key Decision-Maker',
                                    executive_title: enrichedLead.executiveTitle || 'Executive',
                                    trigger_event: target.triggerEvent || `Target matching ${targetAudience} located via Exa.ai.`,
                                    trigger_source: target.triggerSource || null,
                                    trigger_relevance: 90,
                                    enrichment_data: {
                                        data_source: target.dataSource,
                                        revenue: enrichedLead.companyRevenue,
                                        employees: enrichedLead.employeeCount,
                                        signals: enrichedLead.signals,
                                        linkedin_url: enrichedLead.linkedinUrl,
                                        email: enrichedLead.email || '',
                                        phone: enrichedLead.phone || '',
                                        company_domain: resolvedDomain,
                                        companyDomain: resolvedDomain,
                                        email_confidence: enrichedLead.emailConfidence || 'none',
                                        emailConfidence: enrichedLead.emailConfidence || 'none',
                                        email_source: enrichedLead.emailSource || '',
                                        emailSource: enrichedLead.emailSource || '',
                                        executive_research: enrichedLead.executiveResearch || '',
                                        trigger_summary: target.triggerEvent || '',
                                        source: 'gemini+apollo+exa',
                                        enriched_at: new Date().toISOString(),
                                    },
                                });
                                if (targetRes.ok && targetRes.data?.[0]) {
                                    leadId = targetRes.data[0].id;
                                }
                            }

                            if (!leadId) {
                                console.error(`❌ Failed to resolve lead target ID for ${companyCleanName}`);
                                continue;
                            }

                            // Research synthesis: real relevance score, findings that answer the user's
                            // information request, and the wider buying committee. This is what makes the
                            // strike a research deliverable rather than a single lead.
                            const enrichmentContext = [
                                enrichedLead.companyRevenue && enrichedLead.companyRevenue !== 'Unknown' ? `Revenue: ${enrichedLead.companyRevenue}` : '',
                                enrichedLead.employeeCount && enrichedLead.employeeCount !== 'Unknown' ? `Employees: ${enrichedLead.employeeCount}` : '',
                                Array.isArray(enrichedLead.signals) && enrichedLead.signals.length ? `Signals: ${enrichedLead.signals.join('; ')}` : '',
                                enrichedLead.executiveResearch ? `Exec background: ${enrichedLead.executiveResearch}` : '',
                            ].filter(Boolean).join(' | ');

                            // Synthesis cache (cost saver): the findings are tailored to THIS user's
                            // request, so we key by user + company domain + a hash of the request inputs.
                            // An exact repeat (e.g. a recurring saved search hitting the same company with
                            // no new angle) reuses the result instead of re-paying Gemini. Different
                            // requests still synthesize fresh, so quality is unaffected.
                            const synKey = 'syn:' + userId + ':' + resolvedDomain + ':' + cacheHash([
                                informationToGather || '', desiredOutcome, targetAudience,
                                profile.ideal_customer_profile || '', profile.exclusions || '',
                            ].join('|').toLowerCase().trim());
                            let research = await getResearchCache(env, synKey);
                            if (research) {
                                console.log(`♻️ Synthesis cache HIT for ${companyCleanName} — skipped Gemini synthesis.`);
                            } else {
                                research = await synthesizeMobileResearch(env, {
                                    company: companyCleanName,
                                    industry: (enrichedLead as any).industry,
                                    trigger: target.triggerEvent || '',
                                    enrichmentContext,
                                    primaryContactName: enrichedLead.executiveName,
                                    primaryContactTitle: enrichedLead.executiveTitle,
                                    senderCompany: companyName,
                                    senderNarrative: servicesNarrative || '',
                                    targetAudience,
                                    market,
                                    desiredOutcome,
                                    informationToGather: informationToGather || '',
                                    idealCustomerProfile: profile.ideal_customer_profile || '',
                                    exclusions: profile.exclusions || '',
                                });
                                await setResearchCache(env, synKey, research, 14);
                            }

                            // Build the contact list: the Apollo-enriched primary (verified) plus the
                            // additional decision-makers from synthesis (suggested, not verified).
                            const contacts = [
                                {
                                    name: enrichedLead.executiveName || 'Unknown',
                                    title: enrichedLead.executiveTitle || 'Executive',
                                    email: enrichedLead.email || '',
                                    phone: enrichedLead.phone || '',
                                    linkedin: enrichedLead.linkedinUrl || '',
                                    verified: !!enrichedLead.email,
                                },
                                ...(research.additionalContacts || []).map((c: { name: string; title: string }) => ({
                                    name: c.name, title: c.title, email: '', phone: '', linkedin: '', verified: false,
                                })),
                            ];

                            // Generate the outreach draft with the MOBILE-ONLY generator. This is fully
                            // isolated from the Polsinelli web engine: the sender is the mobile user and
                            // the email must never reference Polsinelli or any third party.
                            const draft = await generateMobileDraft(env, {
                                lead: enrichedLead,
                                senderName: profile.full_name || '', // person's name only — never the company
                                senderCompany: companyName,
                                senderNarrative: servicesNarrative || '',
                                desiredOutcome,
                                targetCompany: companyCleanName,
                                triggerContext: target.triggerEvent || '',
                            });

                            // Save to strike_campaigns. mobile_user_id is the durable, exact ownership
                            // key the mobile results endpoint filters on (no more domain-string guessing).
                            const workflowId = `wf-mob-${crypto.randomUUID().slice(0, 12)}`;
                            const strikeCampaignRes = await insertRow(env, 'strike_campaigns', {
                                target_id: leadId,
                                status: 'pending_hitl', // Set to pending review so it's completed and ready
                                persona_used: profile.full_name || companyName,
                                email_subject: draft.subject,
                                email_subject_b: (draft as any).subjectB || null,
                                drafted_body: draft.body,
                                workflow_id: workflowId,
                                campaign_id: null, // Mobile independent strike
                                mobile_user_id: userId,
                                mobile_delivered_at: new Date().toISOString(),
                                // Human-readable label of the search that produced this strike, so the
                                // app can group results by request ("audience · market").
                                mobile_search_label: [targetAudience, market].filter(Boolean).join(' · '),
                                // Research deliverable: real relevance, findings, and the buying committee.
                                mobile_relevance_score: research.relevanceScore,
                                mobile_relevance_reason: research.relevanceReason,
                                mobile_research_findings: JSON.stringify(research.researchFindings),
                                mobile_contacts: JSON.stringify(contacts),
                            });

                            let strikeCampaignId: number | null = null;
                            if (strikeCampaignRes.ok && strikeCampaignRes.data?.[0]) {
                                strikeCampaignId = strikeCampaignRes.data[0].id;
                            }

                            // Record the resolved real domain for per-user dedup on future runs.
                            await insertRow(env, 'mobile_delivered_strikes_log', {
                                user_id: userId,
                                company_domain: resolvedDomain,
                                delivered_at: new Date().toISOString()
                            });

                            generatedCount++;
                            console.log(`✅ Priority strike generated for ${companyCleanName} (${companyDomain})${isWatchlist ? ' [watchlist]' : ''} → User #${userId}`);

                            // Dispatch push notification if APNS device token is configured
                            if (profile.apns_device_token) {
                                try {
                                    const { sendApnsNotification } = await import('./utils/apns');
                                    await sendApnsNotification(
                                        env,
                                        profile.apns_device_token,
                                        `New Opportunity: ${companyCleanName}`,
                                        `We identified a high relevance business opportunity with ${companyCleanName}. Tap to draft outreach.`,
                                        {
                                            strikeId: strikeCampaignId,
                                            companyDomain: resolvedDomain
                                        }
                                    );
                                } catch (apnsErr) {
                                    console.error('❌ Failed to trigger APNS push notification:', apnsErr);
                                }
                            }
                        }

                        // ── Partner Network injection (#2) ──
                        // If any strategic partners genuinely align with this research request, surface
                        // them as highlighted "Strategic Partner" strikes with their real contact info.
                        // These are a FREE value-add (not billed) and skipped for watchlist alerts.
                        // Flip PARTNER_STRIKES_BILLED to charge a credit for them instead.
                        const PARTNER_STRIKES_BILLED = false;
                        if (!isWatchlist) {
                            try {
                                const partnersRes = await fetch(
                                    `${env.SUPABASE_URL}/rest/v1/partner_profiles?select=id,name,website,value_proposition,key_offerings,categories,ideal_customer_profile,ai_alignment_rules,contact_name,contact_email,contact_phone`,
                                    { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                                );
                                const partners = partnersRes.ok ? await partnersRes.json() as any[] : [];
                                if (partners.length > 0) {
                                    const partnerList = partners.map(p =>
                                        `#${p.id} ${p.name} — categories: ${(p.categories || []).join(', ') || 'n/a'}; offerings: ${(p.key_offerings || []).join(', ') || 'n/a'}; ICP: ${p.ideal_customer_profile || 'n/a'}; rules: ${p.ai_alignment_rules || 'n/a'}`
                                    ).join('\n');
                                    const matchPrompt = `A client made this research request:\nAudience: ${targetAudience}\nMarket: ${market}\n${sector ? `Sector: ${sector}\n` : ''}Desired outcome: ${desiredOutcome}\nInfo needed: ${informationToGather || 'general fit'}\n\nOur strategic partners:\n${partnerList}\n\nReturn ONLY partners that GENUINELY align with this request (respect each partner's alignment rules). Give a one-sentence reason tailored to the request. Return at most 2. If none truly fit, return an empty array.`;
                                    const matchRes = await fetchGemini(env, 'lite', {
                                        activityName: 'mobile-partner-match',
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({
                                            contents: [{ role: 'user', parts: [{ text: matchPrompt }] }],
                                            generationConfig: {
                                                temperature: 0.3, maxOutputTokens: 1024, responseMimeType: 'application/json',
                                                responseSchema: { type: 'OBJECT', properties: { matches: { type: 'ARRAY', items: { type: 'OBJECT', properties: { id: { type: 'NUMBER' }, reason: { type: 'STRING' } }, required: ['id', 'reason'] } } }, required: ['matches'] },
                                            },
                                        }),
                                    });
                                    let matches: any[] = [];
                                    if (matchRes.ok) {
                                        const { text: mText } = await safeGeminiResponseParse(matchRes);
                                        matches = safeJsonParse<{ matches?: any[] }>(mText || '', { matches: [] }).matches || [];
                                    }
                                    for (const m of matches.slice(0, 2)) {
                                        const partner = partners.find(p => p.id === m.id);
                                        if (!partner) continue;
                                        // Dedup: don't re-deliver the same partner to the same user.
                                        const dupRes = await fetch(
                                            `${env.SUPABASE_URL}/rest/v1/strike_campaigns?select=id&mobile_user_id=eq.${userId}&mobile_partner_id=eq.${partner.id}&limit=1`,
                                            { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                                        );
                                        const dup = dupRes.ok ? await dupRes.json() as any[] : [];
                                        if (dup.length > 0) continue;

                                        const partnerContacts = [{
                                            name: partner.contact_name || partner.name,
                                            title: 'Strategic Partner',
                                            email: partner.contact_email || '',
                                            phone: partner.contact_phone || '',
                                            linkedin: '',
                                            verified: !!partner.contact_email,
                                        }];

                                        // Resolve (reuse-or-create) a lead_target for the partner so the strike
                                        // joins cleanly in the results endpoint and shows the partner's name.
                                        const partnerDomain = (partner.website || '').replace(/^https?:\/\//, '').replace(/^www\./, '').replace(/\/.*$/, '');
                                        let partnerLeadId: number | null = null;
                                        const existingPartnerLeadRes = await fetch(
                                            `${env.SUPABASE_URL}/rest/v1/lead_targets?company=ilike.${encodeURIComponent(partner.name)}&select=id&limit=1`,
                                            { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                                        );
                                        const existingPartnerLead = existingPartnerLeadRes.ok ? await existingPartnerLeadRes.json() as any[] : [];
                                        if (existingPartnerLead.length > 0) {
                                            partnerLeadId = existingPartnerLead[0].id;
                                        } else {
                                            const pLeadRes = await insertRow(env, 'lead_targets', {
                                                company: partner.name,
                                                executive_name: partner.contact_name || 'Partner Contact',
                                                executive_title: 'Strategic Partner',
                                                trigger_event: m.reason || partner.value_proposition || 'Strategic partner in our network.',
                                                trigger_source: partner.website || null,
                                                trigger_relevance: 95,
                                                enrichment_data: {
                                                    data_source: 'Partner Network',
                                                    company_domain: partnerDomain,
                                                    companyDomain: partnerDomain,
                                                    email: partner.contact_email || '',
                                                    phone: partner.contact_phone || '',
                                                    is_partner: true,
                                                    source: 'partner_profiles',
                                                },
                                            });
                                            partnerLeadId = (pLeadRes.ok && pLeadRes.data?.[0]) ? pLeadRes.data[0].id : null;
                                        }
                                        const pDraft = await generateMobileDraft(env, {
                                            lead: { company: partner.name, executiveName: partner.contact_name || 'Unknown', executiveTitle: 'Strategic Partner' } as any,
                                            senderName: profile.full_name || '',
                                            senderCompany: companyName,
                                            senderNarrative: servicesNarrative || '',
                                            desiredOutcome,
                                            targetCompany: partner.name,
                                            triggerContext: m.reason || partner.value_proposition || '',
                                        });
                                        const pWorkflowId = `wf-prt-${crypto.randomUUID().slice(0, 12)}`;
                                        const pRes = await insertRow(env, 'strike_campaigns', {
                                            target_id: partnerLeadId,
                                            status: 'pending_hitl',
                                            persona_used: profile.full_name || companyName,
                                            email_subject: pDraft.subject,
                                            drafted_body: pDraft.body,
                                            workflow_id: pWorkflowId,
                                            campaign_id: null,
                                            mobile_user_id: userId,
                                            mobile_delivered_at: new Date().toISOString(),
                                            mobile_search_label: [targetAudience, market].filter(Boolean).join(' · '),
                                            mobile_relevance_score: 95,
                                            mobile_relevance_reason: m.reason || 'Strategic partner aligned with your request.',
                                            mobile_research_findings: JSON.stringify([partner.value_proposition || m.reason || 'Strategic partner in our network.'].filter(Boolean)),
                                            mobile_contacts: JSON.stringify(partnerContacts),
                                            mobile_is_partner: true,
                                            mobile_partner_id: partner.id,
                                        });
                                        if (pRes.ok) {
                                            if (PARTNER_STRIKES_BILLED) generatedCount++;
                                            console.log(`🤝 Injected partner strike: ${partner.name} → User #${userId}`);
                                        }
                                    }
                                }
                            } catch (partnerErr) {
                                console.warn('⚠️ Partner injection failed:', partnerErr);
                            }
                        }

                        // Charge 1 credit per DELIVERED strike (only on-audience strikes are delivered now,
                        // and watchlist alerts are billed too). Dispatch deducts nothing up front.
                        if (generatedCount > 0) {
                            try {
                                const balRows = await fetchRow(env, 'mobile_strike_balances', 'user_id', userId);
                                const current = balRows && balRows[0] ? balRows[0] : null;
                                if (current) {
                                    await patchRow(env, 'mobile_strike_balances', {
                                        credits_used: (current.credits_used || 0) + generatedCount,
                                        updated_at: new Date().toISOString(),
                                    }, 'user_id', userId);
                                }
                            } catch (creditErr) {
                                console.error('❌ Failed to update mobile credit balance:', creditErr);
                            }
                        }

                        // Log a MISSED opportunity when nothing on-audience was found (skip for watchlist
                        // alerts), so the team can source options from the web dashboard later.
                        if (generatedCount === 0 && !isWatchlist) {
                            try {
                                await insertRow(env, 'mobile_missed_strikes', {
                                    user_id: userId,
                                    target_audience: targetAudience,
                                    market,
                                    desired_outcome: desiredOutcome,
                                    information_requested: informationToGather || null,
                                    requested_count: remainingCount,
                                    candidates_found: 0,
                                    created_at: new Date().toISOString(),
                                });
                                console.log(`📝 Logged missed strike request for User #${userId} (audience="${targetAudience}", market="${market}").`);
                            } catch (missErr) {
                                console.error('❌ Failed to log missed strike:', missErr);
                            }
                        }

                        console.log(`📱 Priority Mobile Strike Generation complete: ${generatedCount}/${remainingCount} delivered & billed.`);
                        msg.ack();
                    } catch (queueErr) {
                        console.error('❌ Priority Mobile Strike Generation queue failed:', queueErr);
                        msg.retry();
                    }
                    continue;
                }

                const { campaignId, persona, workflowId: regenerateWorkflowId, action, steeringNotes } = msg.body;
                console.log(`📨 Queue processing: Campaign #${campaignId} | Persona: ${persona} | Action: ${action || 'new'}`);

                // Helper to gracefully reset agent status at the end of the pipeline run
                const resetAgentStatus = async (env: Env, agentId: number) => {
                    try {
                        const { patchRow } = await import('./utils/supabase');
                        await patchRow(env, 'agents', { status: 'active', active_pipelines: 0, last_activity: new Date().toISOString() }, 'id', agentId);
                        console.log(`🔄 Reset Agent #${agentId} to 'active' state (pipeline lock cleared).`);
                    } catch (e) {
                        console.error(`❌ Failed to reset agent state for #${agentId}`, e);
                    }
                };

                if (action === 'generate_briefing') {
                    console.log(`🌍 Generating territory briefing for [ID: ${msg.body.territoryId}] ${msg.body.territoryName}`);
                    try {
                        const { processTerritoryBriefing } = await import('./tasks/generate-briefing');
                        await processTerritoryBriefing(env, msg.body.territoryId, msg.body.territoryName);
                        msg.ack();
                    } catch (err) {
                        console.error(`❌ Briefing generation failed:`, err);
                        msg.retry();
                    }
                    continue;
                }

                // ═══════════════════════════════════════════════════════
                // FREE SENSOR SWEEP — runs ONCE per cron cycle
                // High-volume government data → dedup → enrich → draft
                // ═══════════════════════════════════════════════════════
                if (action === 'free_sensor_sweep') {
                    console.log('🏛️ ═══ FREE SENSOR SWEEP — HIGH VOLUME INDEPENDENT RUN ═══');
                    const sweepStart = Date.now();
                    try {
                        const { senseSecBulk } = await import('./activities/sense-sec-bulk');
                        const { senseFederalRegister } = await import('./activities/sense-federal-register');
                        const { senseWarnDirect } = await import('./activities/sense-warn-direct');
                        const { senseUsptoDirect } = await import('./activities/sense-uspto-direct');
                        const { senseStateWarn } = await import('./activities/sense-state-warn');
                        const { senseStateRegistrations } = await import('./activities/sense-state-registrations');
                        const { senseCourtFilings } = await import('./activities/sense-court-filings');
                        const { patchRow, fetchRow } = await import('./utils/supabase');

                        // Fetch all agents to map agent_id to persona dynamically!
                        let agentPersonaMap = new Map<number, string>();
                        try {
                            const agentsRes = await fetch(`${env.SUPABASE_URL}/rest/v1/agents?select=id,persona`, {
                                headers: {
                                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
                                }
                            });
                            if (agentsRes.ok) {
                                const systemAgents = await agentsRes.json() as { id: number, persona: string }[];
                                for (const sa of systemAgents) {
                                    agentPersonaMap.set(sa.id, sa.persona);
                                }
                            }
                        } catch (agentMapErr) {
                            console.warn('⚠️ Could not fetch agents for dynamic persona mapping, using fallbacks:', agentMapErr);
                        }

                        const fallbackPersonaMap: Record<number, string> = {
                            1: "Rob O'Neill",
                            2: "Fred Posinelli",
                            3: "Fred Posinelli",
                            4: "Fred Posinelli",
                            5: "Rob O'Neill"
                        };

                        // Run all 7 sensors in parallel
                        const [secResult, fedRegResult, warnResult, usptoResult, stateWarnResult, sosRegResult, courtResult] = await Promise.allSettled([
                            senseSecBulk(env),
                            senseFederalRegister(env),
                            senseWarnDirect(env),
                            senseUsptoDirect(env),
                            senseStateWarn(env),
                            senseStateRegistrations(env),
                            senseCourtFilings(env),
                        ]);

                        // Merge all results
                        const allTriggers: MarketTrigger[] = [];
                        const sourceStats: Record<string, number> = {};

                        for (const [label, result] of [
                            ['SEC EDGAR (Bulk)', secResult],
                            ['Federal Register', fedRegResult],
                            ['WARN Direct (Free)', warnResult],
                            ['USPTO Direct (Free)', usptoResult],
                            ['WARN 50-State (Stanford)', stateWarnResult],
                            ['State Corporate SOS', sosRegResult],
                            ['CourtListener Federal cases', courtResult],
                        ] as [string, PromiseSettledResult<MarketTrigger[]>][]) {
                            if (result.status === 'fulfilled') {
                                allTriggers.push(...result.value);
                                sourceStats[label] = result.value.length;
                                console.log(`✅ ${label}: ${result.value.length} triggers`);
                            } else {
                                sourceStats[label] = 0;
                                console.warn(`⚠️ ${label} failed:`, result.reason);
                                const reasonStr = String(result.reason || '');
                                if (reasonStr.includes('location is not supported') || reasonStr.includes('LocationNotSupported')) {
                                    throw result.reason;
                                }
                            }
                        }

                        console.log(`📊 Total raw triggers from free sensors: ${allTriggers.length}`);

                        // Deduplicate across all sources
                        const dedupedTriggers = deduplicateTriggers(allTriggers);
                        console.log(`🧹 After cross-source dedup: ${dedupedTriggers.length} unique`);

                        // ── Check Watchlist Monitored Domains ──
                        try {
                            const monitoredRes = await fetch(
                                `${env.SUPABASE_URL}/rest/v1/mobile_monitored_domains?select=user_id,domain&limit=10000`,
                                { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                            );
                            if (monitoredRes.ok) {
                                const monitored = await monitoredRes.json() as any[];
                                if (monitored && monitored.length > 0) {
                                    console.log(`👁️ Checking ${dedupedTriggers.length} unique triggers against ${monitored.length} watchlisted domains...`);
                                    
                                    // Build map of domain -> Set of userIds
                                    const domainUserMap = new Map<string, Set<string>>();
                                    for (const m of monitored) {
                                        const cleanDomain = (m.domain || '').trim().toLowerCase().replace(/^(https?:\/\/)?(www\.)?/, '');
                                        if (cleanDomain) {
                                            if (!domainUserMap.has(cleanDomain)) {
                                                domainUserMap.set(cleanDomain, new Set());
                                            }
                                            domainUserMap.get(cleanDomain)!.add(m.user_id);
                                        }
                                    }

                                    // Check triggers — BATCHED per matched domain so this scales to
                                    // thousands of watchers (2 queries per matched domain, not per user).
                                    let watchlistMatches = 0;
                                    const hdr = { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` };
                                    const handledDomains = new Set<string>();
                                    for (const trigger of dedupedTriggers) {
                                        const domain = normalizeDomain(trigger.company || '', null, trigger.sourceUrl);
                                        const matchingUserIds = domainUserMap.get(domain);
                                        if (!matchingUserIds || matchingUserIds.size === 0) continue;
                                        if (handledDomains.has(domain)) continue; // one alert per domain per sweep
                                        handledDomains.add(domain);
                                        const candidates = [...matchingUserIds];

                                        // Batch 1: which of these users already received this domain?
                                        const delRes = await fetch(
                                            `${env.SUPABASE_URL}/rest/v1/mobile_delivered_strikes_log?company_domain=eq.${encodeURIComponent(domain)}&select=user_id&limit=10000`,
                                            { headers: hdr }
                                        );
                                        const deliveredSet = new Set((delRes.ok ? await delRes.json() as any[] : []).map(r => r.user_id));
                                        const fresh = candidates.filter(uid => !deliveredSet.has(uid));
                                        if (fresh.length === 0) continue;

                                        // Batch 2: which fresh users have at least 1 credit (alerts cost 1 credit)?
                                        const balRes = await fetch(
                                            `${env.SUPABASE_URL}/rest/v1/mobile_strike_balances?user_id=in.(${fresh.join(',')})&select=user_id,credits_purchased,credits_used`,
                                            { headers: hdr }
                                        );
                                        const balRows = balRes.ok ? await balRes.json() as any[] : [];
                                        const payable = new Set(
                                            balRows.filter(b => (b.credits_purchased || 0) - (b.credits_used || 0) >= 1).map(b => b.user_id)
                                        );

                                        for (const userId of fresh) {
                                            if (!payable.has(userId) || !env.MOBILE_QUEUE) continue;
                                            await env.MOBILE_QUEUE.send({
                                                userId,
                                                targetAudience: 'Watchlist Alert',
                                                market: 'Global',
                                                desiredOutcome: 'Alert',
                                                informationToGather: 'Watchlist Signal',
                                                remainingCount: 1,
                                                watchlistDomain: domain,
                                                watchlistTrigger: trigger,
                                            });
                                            watchlistMatches++;
                                        }
                                        console.log(`🎯 Watchlist "${domain}": ${fresh.length} fresh, ${payable.size} payable → queued.`);
                                    }
                                    console.log(`👁️ Watchlist sweep complete. Queued ${watchlistMatches} alert(s).`);
                                }
                            }
                        } catch (watchlistErr) {
                            console.error('❌ Watchlist matching failed:', watchlistErr);
                        }

                        // Check against existing DB to skip known companies
                        let newTriggers: MarketTrigger[] = dedupedTriggers;
                        if (dedupedTriggers.length > 0) {
                            const companyNames = dedupedTriggers.map(t => t.company).filter(Boolean);
                            if (companyNames.length > 0) {
                                const { fetchRowsIn } = await import('./utils/supabase');
                                const existingRows = await fetchRowsIn(env, 'lead_targets', 'company', companyNames);
                                const existingSet = new Set(existingRows.map((r: any) => (r.company || '').toLowerCase()));
                                newTriggers = dedupedTriggers.filter(t => !existingSet.has((t.company || '').toLowerCase()));
                                console.log(`🆕 ${newTriggers.length} genuinely NEW companies (${existingSet.size} already in DB)`);
                            }
                        }

                        // Process new triggers: FAST BULK INSERT + queue enrichment
                        // Key insight: Cloudflare Workers have 30s CPU limit.
                        // We can't enrich 100+ leads sequentially. Instead:
                        // 1. Insert raw lead_targets immediately (fast)
                        // 2. Queue individual enrichment messages (async)
                        let inserted = 0;
                        const queueMessages: any[] = [];
                        if (newTriggers.length > 0) {
                            const batch = newTriggers.slice(0, 1500);
                            console.log(`📥 Fast-inserting ${batch.length} new leads (enrichment queued separately)...`);

                            for (const trigger of batch) {
                                try {
                                    const workflowId = `wf-${crypto.randomUUID().slice(0, 12)}`;
                                    const agentId = trigger.agentId || 1;
                                    const personaUsed = agentPersonaMap.get(agentId) || fallbackPersonaMap[agentId as keyof typeof fallbackPersonaMap] || "Rob O'Neill";

                                    // Fast insert: raw lead data without enrichment
                                    const targetRes = await insertRow(env, 'lead_targets', {
                                        company: trigger.company,
                                        executive_name: trigger.executiveName || 'Key Decision-Maker',
                                        executive_title: trigger.executiveTitle || 'General Counsel',
                                        trigger_event: trigger.headline,
                                        trigger_source: trigger.sourceUrl || null,
                                        trigger_relevance: trigger.relevanceScore || 70,
                                        discovered_by_agent: agentId,
                                        enrichment_data: {
                                            data_source: trigger.source || 'SEC EDGAR (Bulk)',
                                            trigger_summary: trigger.articleText || '',
                                            sources: trigger.sourceUrl ? [{ label: 'Trigger Source', url: trigger.sourceUrl }] : [],
                                            source: 'free-sensor-sweep',
                                            enriched_at: new Date().toISOString(),
                                            enrichment_status: 'pending',
                                        },
                                    });

                                    if (targetRes.ok && targetRes.data?.[0]) {
                                        const targetId = targetRes.data[0].id;
                                        if (!targetId) {
                                            console.error(`❌ targetId is missing from lead_targets insert response (fast-insert) for ${trigger.company}`, targetRes.data);
                                        } else {
                                            // Insert a placeholder campaign for visibility in Strike Queue
                                            const campaignRes = await insertRow(env, 'strike_campaigns', {
                                                target_id: targetId,
                                                status: 'pending_hitl',
                                                persona_used: personaUsed,
                                                email_subject: `${trigger.company} — ${trigger.headline?.slice(0, 60) || 'New Opportunity'}`,
                                                drafted_body: `[Pending enrichment] ${trigger.articleText || trigger.headline || ''}`,
                                                workflow_id: workflowId,
                                                agent_id: agentId,
                                            });

                                            if (campaignRes.ok && campaignRes.data?.[0]) {
                                                const campaignId = campaignRes.data[0].id;
                                                queueMessages.push({
                                                    body: {
                                                        campaignId: campaignId,
                                                        workflowId: workflowId,
                                                        persona: personaUsed,
                                                        action: 'regenerate'
                                                    }
                                                });
                                            }

                                            inserted++;
                                            if (inserted <= 5) {
                                                console.log(`✅ Lead #${targetId}: ${trigger.company} [${trigger.source}]`);
                                            }
                                        }
                                    }
                                } catch (insertErr) {
                                    console.warn(`⚠️ Insert failed for ${trigger.company}:`, insertErr);
                                }
                            }

                            if (env.STRIKE_QUEUE && queueMessages.length > 0) {
                                console.log(`🚀 Dispatching ${queueMessages.length} enrichment messages to STRIKE_QUEUE...`);
                                await safeQueueSendBatch(env.STRIKE_QUEUE, queueMessages);
                            }
                        }

                        const elapsed = ((Date.now() - sweepStart) / 1000).toFixed(1);
                        console.log(`🏛️ ═══ FREE SENSOR SWEEP COMPLETE ═══`);
                        console.log(`   📊 Sources: ${JSON.stringify(sourceStats)}`);
                        console.log(`   🧹 Deduped: ${dedupedTriggers.length} unique`);
                        console.log(`   🆕 New: ${newTriggers.length} companies`);
                        console.log(`   📥 Inserted: ${inserted} lead_targets + strike_campaigns`);
                        console.log(`   ⏱️ Elapsed: ${elapsed}s`);

                        msg.ack();
                    } catch (sweepErr) {
                        console.error('❌ Free sensor sweep failed:', sweepErr);
                        msg.retry();
                    }
                    continue;
                }

                // 'dispatch' is the action used by cron 2b for due scheduled funnel steps — it is an
                // alias of 'deliver'. (Previously it fell through to the global sense pipeline and
                // scheduled emails were never sent.)
                if (action === 'deliver' || action === 'dispatch') {
                    console.log(`🚀 Throttled delivery starting for Workflow #${msg.body.workflowId}...`);
                    try {
                        // Send-Time Optimization & A/B testing variables
                        let selectedSubject = msg.body.emailSubject;
                        let selectedVersion = 'A';
                        let targetState = '';
                        let campaignId = msg.body.campaignId || 0;
                        let campaign = null;
                        
                        // 1. Dynamic lookup to resolve campaign details
                        try {
                            const { fetchRow } = await import('./utils/supabase');
                            let campaignRows = [];
                            if (campaignId > 0) {
                                campaignRows = await fetchRow(env, 'strike_campaigns', 'id', campaignId);
                            } else if (msg.body.workflowId) {
                                campaignRows = await fetchRow(env, 'strike_campaigns', 'workflow_id', msg.body.workflowId);
                            }
                            
                            if (campaignRows && campaignRows.length > 0) {
                                campaign = campaignRows[0];
                                campaignId = campaign.id;
                            }
                        } catch (e) {
                            console.warn(`⚠️ Failed to fetch campaign for delivery:`, e);
                        }

                        // Idempotency guards: never re-send a campaign that already went out,
                        // and never send one a human discarded/suppressed or that already got a reply.
                        const terminalStatuses = ['sent', 'delivered', 'discarded', 'suppressed', 'replied'];
                        if (campaign && (terminalStatuses.includes(campaign.status) || (campaign.sent_at && campaign.status !== 'failed'))) {
                            console.log(`⏭️ Skipping delivery for Campaign #${campaignId} — status '${campaign.status}'${campaign.sent_at ? ` (sent_at ${campaign.sent_at})` : ''}`);
                            msg.ack();
                            continue;
                        }
                        if (!campaign && ((msg.body.campaignId || 0) > 0 || msg.body.workflowId)) {
                            // Campaign lookup failed (transient DB error or row gone) — retry instead of sending unguarded
                            console.warn(`⚠️ Could not resolve campaign for delivery (id=${msg.body.campaignId}, wf=${msg.body.workflowId}) — retrying`);
                            msg.retry();
                            continue;
                        }

                        // 2. Resolve delivery fields using DB values if missing in payload
                        let recipientEmail = msg.body.recipientEmail;
                        let emailBody = msg.body.emailBody;
                        let senderEmail = msg.body.senderEmail;
                        let senderName = msg.body.senderName || 'Fred Posinelli';

                        if (campaign) {
                            try {
                                const { fetchRow, fetchRows, patchRow } = await import('./utils/supabase');
                                const targetId = campaign.target_id;
                                
                                // A/B Subject Line selection
                                const subjectA = campaign.email_subject || msg.body.emailSubject;
                                const subjectB = campaign.email_subject_b || msg.body.emailSubjectB;
                                if (subjectB) {
                                    if (campaignId % 2 === 0) {
                                        selectedSubject = subjectA;
                                        selectedVersion = 'A';
                                    } else {
                                        selectedSubject = subjectB;
                                        selectedVersion = 'B';
                                    }
                                    console.log(`⚖️ [A/B Testing] Selected Version ${selectedVersion} for Campaign #${campaignId}: "${selectedSubject}"`);
                                } else {
                                    selectedSubject = subjectA;
                                }

                                const trgRows = await fetchRow(env, 'lead_targets', 'id', targetId);
                                if (trgRows && trgRows.length > 0) {
                                    const trg = trgRows[0];
                                    if (!recipientEmail) {
                                        recipientEmail = trg.email || trg.enrichment_data?.email || '';
                                    }
                                    const ed = trg.enrichment_data || {};
                                    targetState = ed.state || ed.state_code || ed.location?.split(',')?.pop()?.trim() || '';
                                    if (!targetState && trg.crm_company_id) {
                                        const compRows = await fetchRow(env, 'crm_companies', 'id', trg.crm_company_id);
                                        if (compRows && compRows.length > 0) {
                                            const comp = compRows[0];
                                            targetState = comp.state || comp.hq_location?.split(',')?.pop()?.trim() || '';
                                        }
                                    }
                                }

                                if (!emailBody) {
                                    emailBody = campaign.drafted_body || campaign.email_body || '';
                                }

                                // Resolve and dynamically rotate sender email if not set in DB
                                const dbSender = campaign.sender_email;
                                if (dbSender) {
                                    senderEmail = dbSender;
                                } else {
                                    try {
                                        const senders = await fetchRows(env, 'crm_settings_senders?status=eq.active&order=sent_count.asc&limit=1');
                                        if (senders && senders.length > 0) {
                                            const picked = senders[0];
                                            senderEmail = picked.email;
                                            
                                            // Increment sent_count on the picked sender
                                            await patchRow(env, 'crm_settings_senders', { sent_count: (picked.sent_count || 0) + 1 }, 'id', picked.id);
                                            
                                            // Save the assigned sender to the campaign in the database
                                            await patchRow(env, 'strike_campaigns', { sender_email: senderEmail }, 'id', campaignId);
                                            
                                            console.log(`🔄 [Auto-Clearance Rotation] Rotated and assigned sender: ${senderEmail} (sent_count: ${picked.sent_count} → ${(picked.sent_count || 0) + 1}) for Campaign #${campaignId}`);
                                        } else {
                                            senderEmail = 'fred@polsinellimgmt.com';
                                        }
                                    } catch (err) {
                                        console.warn(`⚠️ [Auto-Clearance Rotation] Failed to rotate sender, falling back to default:`, err);
                                        senderEmail = 'fred@polsinellimgmt.com';
                                    }
                                }
                            } catch (e) {
                                console.warn(`⚠️ Failed to resolve database target info:`, e);
                            }
                        }

                        // Fail safe if recipient email is still not resolved
                        if (!recipientEmail) {
                            throw new Error('No recipient email found for delivery');
                        }

                        const tz = getTimezoneFromState(targetState);
                        const formatter = new Intl.DateTimeFormat('en-US', {
                            timeZone: tz,
                            hour: 'numeric',
                            weekday: 'short',
                            hour12: false
                        });
                        const parts = formatter.formatToParts(new Date());
                        const partMap = Object.fromEntries(parts.map(p => [p.type, p.value]));
                        const localHour = parseInt(partMap.hour, 10);
                        const localWeekday = partMap.weekday;

                        // Standard business hours: 9 AM - 5 PM, Mon-Fri
                        if (localWeekday === 'Sat' || localWeekday === 'Sun' || localHour < 9 || localHour >= 17) {
                            const delay = calculateDelayToNextBusinessHour(tz);
                            if (campaignId > 0) {
                                // Persist the schedule on the row and ack — queue retries are capped at 12h
                                // and a limited retry budget, so weekend deferrals via msg.retry() would be
                                // silently dropped. Cron 2b re-dispatches due 'scheduled' rows.
                                const sendAtIso = new Date(Date.now() + delay * 1000).toISOString();
                                const { patchRow } = await import('./utils/supabase');
                                await patchRow(env, 'strike_campaigns', { status: 'scheduled', scheduled_send_at: sendAtIso }, 'id', campaignId);
                                console.log(`⏳ [Send-Time Optimization] Campaign #${campaignId} rescheduled for ${sendAtIso}. Recipient Local Time: ${localWeekday} ${localHour}:00 (TZ: ${tz}, State: ${targetState || 'default'})`);
                                msg.ack();
                            } else {
                                console.log(`⏳ [Send-Time Optimization] Deferring ad-hoc delivery by ${Math.min(delay, 43200)}s (no campaign row to reschedule).`);
                                msg.retry({ delaySeconds: Math.min(delay, 43200) });
                            }
                            continue;
                        }

                        // Atomically claim the row so a concurrent deliver message (cron auto-recovery,
                        // double approval click, queue redelivery) cannot double-send. Only one consumer
                        // wins the conditional PATCH on sent_at IS NULL.
                        if (campaignId > 0) {
                            const claimRes = await fetch(
                                `${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=eq.${campaignId}&sent_at=is.null&status=not.in.(sent,delivered,discarded,suppressed,replied)`,
                                {
                                    method: 'PATCH',
                                    headers: {
                                        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                                        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                        'Content-Type': 'application/json',
                                        Prefer: 'return=representation',
                                    },
                                    body: JSON.stringify({ sent_at: new Date().toISOString() }),
                                },
                            );
                            if (!claimRes.ok) {
                                console.warn(`⚠️ Delivery claim failed for Campaign #${campaignId} (HTTP ${claimRes.status}) — retrying`);
                                msg.retry();
                                continue;
                            }
                            const claimedRows = await claimRes.json() as any[];
                            if (!Array.isArray(claimedRows) || claimedRows.length === 0) {
                                console.log(`⏭️ Campaign #${campaignId} already claimed or sent by another consumer — skipping duplicate delivery.`);
                                msg.ack();
                                continue;
                            }
                        }

                        const result = await executeCampaignSes(env, {
                            campaignId: campaignId,
                            workflowId: msg.body.workflowId,
                            emailSubject: selectedSubject,
                            emailBody: emailBody,
                            recipientEmail: recipientEmail,
                            senderEmail: senderEmail,
                            senderName: senderName,
                            subjectVersion: selectedVersion,
                        });

                        if (result.error || result.status !== 'sent') {
                            // executeCampaignSes never throws — it returns {status:'failed', error}.
                            // Release the sent_at claim so a queue retry can re-attempt the send.
                            console.error(`❌ SES delivery failed for Campaign #${campaignId}: ${result.error || `status=${result.status}`}`);
                            if (campaignId > 0) {
                                try {
                                    const { patchRow } = await import('./utils/supabase');
                                    await patchRow(env, 'strike_campaigns', { sent_at: null }, 'id', campaignId);
                                } catch (unclaimErr) {
                                    console.warn(`⚠️ Failed to release delivery claim for Campaign #${campaignId}:`, unclaimErr);
                                }
                            }
                            msg.retry();
                            continue;
                        }

                        console.log(`✅ Email sent via SES: ${result.sesMessageId}`);
                        // Ack BEFORE throttling — sleeping first widens the redelivery window after a successful send.
                        msg.ack();

                        // Enforce a safe delay to throttle batch campaigns and protect IP reputation.
                        // Default to 8 seconds to achieve up to 10,800 daily sends while safely distributing across rotated domains.
                        const delayMs = env.THROTTLE_DELAY_MS ? Number(env.THROTTLE_DELAY_MS) : 8000;
                        await new Promise(resolve => setTimeout(resolve, delayMs));
                    } catch (err) {
                        console.error(`❌ Failed to send throttled email for Workflow #${msg.body.workflowId}:`, err);
                        msg.retry();
                    }
                    continue;
                }

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
                        const existingEnrichment = lead.enrichment_data || {};
                        const { patchRow: patchLead } = await import('./utils/supabase');
                        await patchLead(env, 'lead_targets', {
                            executive_name: enrichedLead.executiveName || lead.executive_name,
                            executive_title: enrichedLead.executiveTitle || lead.executive_title,
                            enrichment_data: {
                                ...existingEnrichment,
                                revenue: enrichedLead.companyRevenue,
                                employees: enrichedLead.employeeCount,
                                signals: enrichedLead.signals,
                                linkedin_url: enrichedLead.linkedinUrl,
                                email: enrichedLead.email || '',
                                phone: enrichedLead.phone || '',
                                companyDomain: enrichedLead.companyDomain,
                                emailConfidence: enrichedLead.emailConfidence,
                                phoneConfidence: enrichedLead.phoneConfidence,
                                executive_research: enrichedLead.executiveResearch || '',
                                enriched_at: new Date().toISOString(),
                                enrichment_status: 'completed',
                            }
                        }, 'id', lead.id);
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
                            emailConfidence: existingEnrichment.emailConfidence || existingEnrichment.email_confidence || 'none',
                            phone: existingEnrichment.phone || '',
                            phoneConfidence: existingEnrichment.phoneConfidence || existingEnrichment.phone_confidence || 'none',
                            companyDomain: existingEnrichment.companyDomain || existingEnrichment.company_domain || null,
                            executiveResearch: existingEnrichment.executive_research || '',
                        };
                    }

                    // Robust Domain-Level Deduplication (Post-Apollo/Enrichment)
                    let isDuplicate = false;
                    if (enrichedLead.companyDomain) {
                        const domainRes = await fetch(
                            `${env.SUPABASE_URL}/rest/v1/lead_targets?enrichment_data->>companyDomain=eq.${encodeURIComponent(enrichedLead.companyDomain)}&id=neq.${lead.id}&select=id`,
                            { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                        );
                        if (domainRes.ok) {
                            const matchedDomains = await domainRes.json() as any[];
                            if (matchedDomains.length > 0) {
                                console.log(`🔄 Skipping duplicate (Domain match via Apollo): ${enrichedLead.companyDomain}`);
                                isDuplicate = true;
                            }
                        }
                    }

                    if (isDuplicate) {
                        await patchRow(env, 'strike_campaigns', {
                            status: 'suppressed',
                            email_subject: 'Duplicate Domain Skipping',
                            drafted_body: `This company domain (${enrichedLead.companyDomain || 'unknown'}) already exists in database. Skipping duplicate targeting.`,
                            persona_used: persona || "Rob O'Neill",
                        }, 'workflow_id', regenerateWorkflowId);
                        msg.ack();
                        continue;
                    }

                    // Compliance & Suppression Check
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

                    let draft = { subject: 'Suppressed Contact', body: `This contact was flagged against the suppression list. Reason: ${suppressionReason || 'Unknown'}` };
                    if (!isSuppressed) {
                        draft = await generateDraft(env, {
                            lead: enrichedLead,
                            persona: senderName,
                            triggerHeadline: lead.trigger_event,
                            triggerArticleText,
                            partnerProfiles,
                            steeringNotes: steeringNotes || campaignObjective || undefined,
                        });
                    }

                    const verifiedSource = enrichedLead.emailSource === 'apollo_match' || enrichedLead.emailSource === 'apollo_search' || enrichedLead.emailSource === 'apollo_org_search';
                    const isHighConfidenceEmail = verifiedSource && (enrichedLead.emailConfidence === 'high' || enrichedLead.emailConfidence === 'verified');
                    const isMediumConfidenceEmail = verifiedSource && enrichedLead.emailConfidence === 'medium';
                    const isFallbackDraft = (draft as any).modelUsed === 'fallback-template';
                    const relevanceScore = lead.trigger_relevance || 50;
                    const isTierOne = !isFallbackDraft && (
                        (relevanceScore >= 75 && isHighConfidenceEmail) ||
                        (relevanceScore >= 85 && isMediumConfidenceEmail)
                    );
                    const targetStatus = isSuppressed ? 'suppressed' : (isTierOne ? 'approved' : 'pending_hitl');

                    await patchRow(env, 'strike_campaigns', {
                        email_subject: draft.subject,
                        email_subject_b: (draft as any).subjectB || null,
                        drafted_body: draft.body,
                        persona_used: senderName,
                        status: targetStatus,
                        ...(isTierOne && !isSuppressed ? { approved_at: new Date().toISOString() } : {})
                    }, 'workflow_id', regenerateWorkflowId);

                    // Instantly dispatch Tier 1 leads to SES delivery queue
                    if (isTierOne && !isSuppressed && env.STRIKE_QUEUE) {
                        console.log(`🚀 Tier 1 Auto-Clearance: Instantly dispatching ${enrichedLead.email} to SES`);
                        // Update status to active (processing) instantly to prevent auto-recovery collision
                        await patchRow(env, 'strike_campaigns', { status: 'active' }, 'workflow_id', regenerateWorkflowId);
                        
                        await env.STRIKE_QUEUE.send({
                            action: 'deliver',
                            campaignId: Number(campaign.id) || 0,
                            workflowId: regenerateWorkflowId,
                            emailSubject: draft.subject,
                            emailSubjectB: (draft as any).subjectB || null,
                            emailBody: draft.body,
                            recipientEmail: enrichedLead.email,
                            senderEmail: campaign.sender_email || 'fred@polsinellimgmt.com',
                            senderName: senderName || 'Fred Posinelli',
                        });
                    }

                    console.log(`✅ Regenerated and Enriched Draft for Workflow #${regenerateWorkflowId} (Status: ${targetStatus})`);
                    msg.ack();
                    continue;
                }

                let triggers = [];
                let strategicCampaignId: string | null = null;
                let partnerProfiles: any[] = [];
                let campaignObjective: string | undefined = undefined;
                let agentCampaigns: Array<{ id: string, objective?: string, partners: any[] }> = [];
                let maxLeads = 5;

                if (action === 'dispatch_agent' && msg.body.agentId) {
                    const { fetchRow, patchRow } = await import('./utils/supabase');
                    const agentRows = await fetchRow(env, 'agents', 'id', msg.body.agentId);
                    if (!agentRows || agentRows.length === 0) {
                        console.error(`Agent ${msg.body.agentId} not found for dispatch`);
                        msg.ack();
                        continue;
                    }
                    const agent = agentRows[0];
                    maxLeads = agent.max_leads_per_run || 5;

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

                    // agentCampaigns is defined in outer scope
                    agentCampaigns = [];

                    if (caRows.length > 0) {
                        // Use first campaign assignment for legacy fallback
                        strategicCampaignId = caRows[0].campaign_id;

                        const { fetchRow } = await import('./utils/supabase');

                        for (const row of caRows) {
                            const cid = row.campaign_id;
                            let obj = undefined;
                            let partners: any[] = [];

                            // Fetch all partners for this campaign via campaign_partners
                            const cpUrl = `${env.SUPABASE_URL}/rest/v1/campaign_partners?campaign_id=eq.${cid}&select=company_id`;
                            const cpRes = await fetch(cpUrl, {
                                headers: { 'apikey': env.SUPABASE_SERVICE_ROLE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` },
                            });
                            const cpRows = cpRes.ok ? await cpRes.json() as any[] : [];
                            for (const cp of cpRows) {
                                const pRows = await fetchRow(env, 'crm_companies', 'id', cp.company_id);
                                if (pRows && pRows.length > 0) partners.push(pRows[0]);
                            }

                            // Fetch the campaign objective
                            const campRows = await fetchRow(env, 'campaigns', 'id', cid);
                            if (campRows && campRows.length > 0) obj = campRows[0].objective || undefined;

                            agentCampaigns.push({ id: cid, objective: obj, partners });

                            // Populate legacy partnerProfiles with all partners from all campaigns just in case something else relies on it
                            partnerProfiles.push(...partners);
                        }

                        console.log(`📋 Agent #${msg.body.agentId} → mapped to ${agentCampaigns.length} campaigns for round-robin dispatch.`);
                    }

                    // ── Run Exa (paid) — agents focus on neural search ──
                    // Free sensors now run ONCE per cron cycle via independent sweep
                    // (not per-agent, which was causing 150x duplicate API calls).
                    // When the cron circuit breaker reports Exa credits exhausted, skip the paid
                    // call entirely and rely on the secondary/free sources below.
                    const agentQuery = agent.exa_query || agent.name;
                    let exaResults: MarketTrigger[] = [];
                    if (msg.body.skipExa) {
                        console.log(`⛔ Skipping Exa for Agent #${agent.id} — credit breaker tripped this cron cycle.`);
                    } else {
                        exaResults = await senseTriggersForAgent(env, agent, msg.body.runId);
                    }
                    triggers = deduplicateTriggers(exaResults);
                    console.log(`📡 Agent #${agent.id}: ${exaResults.length} raw → ${triggers.length} unique from Exa`);

                    // Quick deduplication to see if we found anything genuinely new
                    let hasNewTriggers = false;
                    if (triggers.length > 0) {
                        const companyNames = triggers.map(t => t.company).filter(Boolean);
                        if (companyNames.length > 0) {
                            const { fetchRowsIn } = await import('./utils/supabase');
                            const existingRows = await fetchRowsIn(env, 'lead_targets', 'company', companyNames);
                            const existingCompanies = new Set(existingRows.map((r: any) => r.company));
                            hasNewTriggers = triggers.some(t => !existingCompanies.has(t.company));
                        } else {
                            hasNewTriggers = true;
                        }
                    }

                    if (!hasNewTriggers) {
                        console.log(`⚠️ Exa + Free sensors returned 0 NEW leads for Agent #${agent.id}, trying paid secondary sources...`);
                        const [secResult, courtResult, newsResult, warnResult, usptoResult] = await Promise.allSettled([
                            senseSecFilingsForQuery(env, agentQuery),
                            senseCourtFilingsForQuery(env, agentQuery),
                            senseNewsForQuery(env, agentQuery),
                            senseWarnNoticesForQuery(env, agentQuery),
                            senseUsptoNoticesForQuery(env, agentQuery),
                        ]);
                        const fallbackTriggers: MarketTrigger[] = [];
                        const sourceStatus: Record<string, string> = {};
                        for (const [label, result] of [
                            ['SEC', secResult],
                            ['Court', courtResult],
                            ['News', newsResult],
                            ['WARN', warnResult],
                            ['USPTO', usptoResult],
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
                        console.log(`📡 Fallback: ${fallbackTriggers.length} raw → ${triggers.length} unique from paid secondary sources`);

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
                    maxLeads = msg.body.maxResults || 5;
                    const virtualAgent = {
                        id: 0,
                        name: 'Search Mission',
                        exa_query: searchQuery,
                        max_leads_per_run: maxLeads,
                    };

                    // Fan out to ALL sources in parallel — paid + FREE sensors
                    const sourceNames = ['Exa.ai', 'SEC EDGAR', 'CourtListener', 'News', 'WARN', 'USPTO', 'SEC Bulk (Free)', 'Fed Register (Free)', 'WARN Direct (Free)', 'USPTO Direct (Free)'];
                    const [exaResult, secResult, courtResult, newsResult, warnResult, usptoResult, secBulkResult, fedRegResult, warnDirectResult, usptoDirectResult] = await Promise.allSettled([
                        senseTriggersForAgent(env, virtualAgent, msg.body.runId),
                        senseSecFilingsForQuery(env, searchQuery),
                        senseCourtFilingsForQuery(env, searchQuery),
                        senseNewsForQuery(env, searchQuery),
                        senseWarnNoticesForQuery(env, searchQuery),
                        senseUsptoNoticesForQuery(env, searchQuery),
                        // ── FREE SENSORS (no Exa/API credits) ──
                        senseSecBulkForQuery(env, searchQuery),
                        senseFederalRegisterForQuery(env, searchQuery),
                        senseWarnDirectForQuery(env, searchQuery),
                        senseUsptoDirectForQuery(env, searchQuery),
                    ]);

                    // Log per-source status for diagnostics
                    const sourceResults = [exaResult, secResult, courtResult, newsResult, warnResult, usptoResult, secBulkResult, fedRegResult, warnDirectResult, usptoDirectResult];
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
                    maxLeads = 1;
                } else if (action) {
                    // Unknown action: ack and drop. Never let an unrecognized action fall through to
                    // the global sense pipeline — that burns Exa credits and inserts unrelated leads.
                    console.error(`❌ Unknown queue action '${action}' for Campaign #${campaignId} — acking without processing.`);
                    msg.ack();
                    continue;
                } else {
                    // Step 1: Sense market triggers (Global fallback if no agent specified)
                    triggers = await senseTriggers(env);
                    if (triggers.length === 0) {
                        console.log(`⚠️ No global triggers found for Campaign #${campaignId}`);
                        msg.ack();
                        continue;
                    }
                    maxLeads = 5;
                }

                // Process ALL non-duplicate triggers (N+1 query fixed)
                const validTriggers = triggers.filter(t => t && t.company && t.company.trim() !== '');
                const newTriggers = [];
                const companyNames = validTriggers.map(t => t.company).filter(Boolean);
                
                let existingCompanies = new Set<string>();
                if (companyNames.length > 0) {
                    const { fetchRowsIn } = await import('./utils/supabase');
                    const existingRows = await fetchRowsIn(env, 'lead_targets', 'company', companyNames);
                    existingCompanies = new Set(existingRows.map((r: any) => r.company));
                }

                for (const t of validTriggers) {
                    if (!existingCompanies.has(t.company)) {
                        newTriggers.push(t);
                    } else {
                        console.log(`🔄 Skipping duplicate (String match): ${t.company}`);
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

                const newTriggersToProcess = newTriggers.slice(0, maxLeads);
                console.log(`🎯 Processing ${newTriggersToProcess.length} new triggers (sliced to match limit of ${maxLeads}) out of ${triggers.length} total`);

                for (const selectedTrigger of newTriggersToProcess) {
                    if (!selectedTrigger.company || !selectedTrigger.company.trim()) {
                        console.warn('⚠️ Skipping trigger without company name in execution loop:', selectedTrigger.headline);
                        continue;
                    }
                    try {
                        // Step 2: Enrich lead via Gemini + Apollo + Exa
                        const enrichedLead = await enrichLead(env, {
                            company: selectedTrigger.company,
                            executiveName: selectedTrigger.executiveName,
                            executiveTitle: selectedTrigger.executiveTitle,
                        });

                        // Robust Domain-Level Deduplication (Post-Apollo)
                        if (enrichedLead.companyDomain) {
                            const domainRes = await fetch(
                                `${env.SUPABASE_URL}/rest/v1/lead_targets?enrichment_data->>companyDomain=eq.${encodeURIComponent(enrichedLead.companyDomain)}&select=id`,
                                { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                            );
                            if (domainRes.ok) {
                                const matchedDomains = await domainRes.json() as any[];
                                if (matchedDomains.length > 0) {
                                    console.log(`🔄 Skipping duplicate (Domain match via Apollo): ${enrichedLead.companyDomain}`);
                                    continue;
                                }
                            }
                        }

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

                        // Round Robin logic for campaigns
                        let targetCampaignObj = campaignObjective;
                        let targetPartnerProfiles = partnerProfiles;
                        let targetCampaignId = strategicCampaignId;

                        if (agentCampaigns && agentCampaigns.length > 0 && action === 'dispatch_agent') {
                            const stringHash = selectedTrigger.company ? selectedTrigger.company.length : Math.floor(Math.random() * 10);
                            const selectedCamp = agentCampaigns[stringHash % agentCampaigns.length];
                            targetCampaignId = selectedCamp.id;
                            targetCampaignObj = selectedCamp.objective;
                            targetPartnerProfiles = selectedCamp.partners;
                            console.log(`🎯 Distributing ${selectedTrigger.company} to Campaign #${targetCampaignId}`);
                        }

                        // Step 3: Generate personalized email draft via Gemini
                        let draft = { subject: 'Suppressed Contact', body: `This contact was flagged against the suppression list. Reason: ${suppressionReason || 'Unknown'}` };
                        if (!isSuppressed) {
                            draft = await generateDraft(env, {
                                lead: enrichedLead,
                                persona: persona || "Rob O'Neill",
                                triggerHeadline: selectedTrigger.headline,
                                triggerArticleText: selectedTrigger.articleText || '',
                                partnerProfiles: targetPartnerProfiles,
                                steeringNotes: targetCampaignObj || undefined,
                            });
                        }

                        // Step 4: Save lead + campaign to Supabase
                        const workflowId = `wf-${crypto.randomUUID().slice(0, 12)}`;

                        const currentAgentId = (selectedTrigger as any).agentId || msg.body.agentId || null;
                        const targetRes = await insertRow(env, 'lead_targets', {
                            company: enrichedLead.company || selectedTrigger.company || 'Unknown Company',
                            executive_name: enrichedLead.executiveName || selectedTrigger.executiveName || 'Key Decision-Maker',
                            executive_title: enrichedLead.executiveTitle || selectedTrigger.executiveTitle || 'General Counsel',
                            trigger_event: selectedTrigger.headline || 'New Opportunity',
                            trigger_source: selectedTrigger.sourceUrl || null,
                            trigger_relevance: selectedTrigger.relevanceScore || 50,
                            discovered_by_agent: currentAgentId,
                            enrichment_data: {
                                data_source: selectedTrigger.source || 'Exa.ai',
                                revenue: enrichedLead.companyRevenue,
                                employees: enrichedLead.employeeCount,
                                signals: enrichedLead.signals,
                                linkedin_url: enrichedLead.linkedinUrl,
                                email: enrichedLead.email || '',
                                phone: enrichedLead.phone || '',
                                company_domain: enrichedLead.companyDomain,
                                companyDomain: enrichedLead.companyDomain,
                                email_confidence: enrichedLead.emailConfidence || 'none',
                                emailConfidence: enrichedLead.emailConfidence || 'none',
                                email_source: enrichedLead.emailSource || '',
                                emailSource: enrichedLead.emailSource || '',
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
                            if (!targetId) {
                                console.error(`❌ targetId is missing from lead_targets insert response for ${selectedTrigger.company}`, targetRes.data);
                            } else {
                                // Tier 1 Auto-Clearance Logic
                                // Only Apollo-sourced (verified-provider) emails may auto-send. Gemini-inferred
                                // and pattern-guessed addresses are never auto-cleared — they go to HITL.
                                const verifiedSource = enrichedLead.emailSource === 'apollo_match' || enrichedLead.emailSource === 'apollo_search' || enrichedLead.emailSource === 'apollo_org_search';
                                const isHighConfidenceEmail = verifiedSource && (enrichedLead.emailConfidence === 'high' || enrichedLead.emailConfidence === 'verified');
                                const isMediumConfidenceEmail = verifiedSource && enrichedLead.emailConfidence === 'medium';
                                const isFallbackDraft = (draft as any).modelUsed === 'fallback-template';
                                // Missing relevance defaults LOW (50) — never assume top tier on absent data.
                                const relevanceScore = selectedTrigger.relevanceScore || 50;
                                const isTierOne = !isFallbackDraft && (
                                    (relevanceScore >= 75 && isHighConfidenceEmail) ||
                                    (relevanceScore >= 85 && isMediumConfidenceEmail)
                                );
                                const targetStatus = isSuppressed ? 'suppressed' : (isTierOne ? 'approved' : 'pending_hitl');

                                const campaignRes = await insertRow(env, 'strike_campaigns', {
                                    target_id: targetId,
                                    status: targetStatus,
                                    persona_used: persona,
                                    email_subject: draft.subject,
                                    email_subject_b: (draft as any).subjectB || null,
                                    drafted_body: draft.body,
                                    workflow_id: workflowId,
                                    campaign_id: targetCampaignId,
                                    agent_id: currentAgentId,
                                    ...(isTierOne ? { approved_at: new Date().toISOString() } : {}),
                                });

                                // Instantly dispatch Tier 1 leads to SES delivery queue
                                if (isTierOne && !isSuppressed && env.STRIKE_QUEUE) {
                                    console.log(`🚀 Tier 1 Auto-Clearance: Instantly dispatching ${enrichedLead.email} to SES`);
                                    await env.STRIKE_QUEUE.send({
                                        workflowId,
                                        action: 'deliver',
                                        emailSubject: draft.subject,
                                        emailSubjectB: (draft as any).subjectB || null,
                                    });
                                }

                                console.log(`✅ Lead #${targetId} created: ${enrichedLead.executiveName || 'Key Decision-Maker'} @ ${enrichedLead.company || selectedTrigger.company} — "${draft.subject}"`);

                                // Update agent stats immediately
                                const agentId = (selectedTrigger as any).agentId || 1;
                                try {
                                    const agentRow = await fetchRow(env, 'agents', 'id', agentId);
                                    if (agentRow.length > 0) {
                                        const a = agentRow[0] as any;
                                        await patchRow(env, 'agents', {
                                            triggers_submitted: (a.triggers_submitted || 0) + 1,
                                            drafts_generated: (a.drafts_generated || 0) + 1,
                                            status: 'active',
                                            last_activity: new Date().toISOString(),
                                        }, 'id', agentId);
                                    }
                                } catch (_) { /* non-blocking */ }
                            }
                        }

                        // NOTE: do NOT reset the agent lock here — we're still inside the per-trigger
                        // loop. Clearing it mid-run lets the next cron double-dispatch this agent.
                        // The lock is released once after the loop completes (end-of-pipeline reset).
                    } catch (triggerErr) {
                        console.error(`❌ Failed to process trigger for ${selectedTrigger.company}:`, triggerErr);
                        try {
                            const { logGeminiError } = await import('./utils/gemini-logger');
                            // using gemini error logger just to surface the exception to supabase
                            await logGeminiError(env, 'worker-crash', 'pipeline-enrich-lead', String(triggerErr), { company: selectedTrigger.company });
                        } catch (e) { /* ignore */ }
                    }
                }

                console.log(`✅ Pipeline complete: ${newTriggers.length} triggers processed`);

                // Update pipeline_runs record
                if (msg.body.runId) {
                    try {
                        await patchRow(env, 'pipeline_runs', {
                            status: 'completed',
                            triggers_found: triggers.length,
                            leads_enriched: newTriggers.length,
                            drafts_generated: newTriggers.length,
                            completed_at: new Date().toISOString(),
                            metadata: {
                                ...(action === 'search_mission' && msg.body.missionId ? {
                                    mission_id: msg.body.missionId,
                                    mission_name: msg.body.missionName,
                                    territory_id: msg.body.territoryId,
                                    territory_name: msg.body.territoryName,
                                } : {}),
                                query: msg.body.searchQuery || msg.body.persona || 'Agent Dispatch',
                                triggers_total: triggers.length,
                                triggers_new: newTriggers.length,
                                triggers_deduplicated: triggers.length - newTriggers.length,
                                sources_checked: action === 'search_mission' ? 4 : 1,
                            },
                        }, 'id', msg.body.runId);

                        // Also update saved_searches results_count
                        if (action === 'search_mission' && msg.body.missionId) {
                            await patchRow(env, 'saved_searches', {
                                results_count: newTriggers.length,
                            }, 'id', msg.body.missionId);
                        }
                    } catch (_) { /* non-blocking */ }
                }

                // Always reset agent lock at end of pipeline processing
                if (msg.body.agentId) {
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

                // Always reset agent lock on error (not just dispatch_agent)
                if (msg.body.agentId) {
                    try {
                        const { patchRow: pr } = await import('./utils/supabase');
                        await pr(env, 'agents', { status: 'active', active_pipelines: 0 }, 'id', msg.body.agentId);
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
            // 0c. Trigger Weekly Report email digest on Mondays at 8:00 AM UTC
            try {
                const date = new Date(event.scheduledTime || Date.now());
                const isMonday = date.getUTCDay() === 1;
                // Since cron runs every 15 mins, checking if hours is 8 and minutes is under 15 is safe
                const is8AM = date.getUTCHours() === 8 && date.getUTCMinutes() < 15;
                if (isMonday && is8AM) {
                    console.log('⏰ Monday 8:00 AM UTC: Triggering Weekly Digest Performance Email Report...');
                    const platformUrl = env.ENVIRONMENT === 'development'
                        ? 'http://localhost:3000'
                        : 'https://ppa-apex-platform.fred-78e.workers.dev';
                    
                    const reportRes = await fetch(`${platformUrl}/api/v1/strikes/weekly-report`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'x-worker-secret': env.WORKER_SECRET || '',
                        }
                    });

                    if (reportRes.ok) {
                        const reportData = await reportRes.json() as any;
                        console.log('✅ Weekly digest email trigger completed:', JSON.stringify(reportData));
                    } else {
                        const reportErr = await reportRes.text();
                        console.error('❌ Failed to trigger weekly report digest:', reportErr);
                    }
                }
            } catch (digestErr) {
                console.error('❌ Weekly report digest trigger threw an error:', digestErr);
            }

            // 0b. Process due mobile SAVED / RECURRING searches — a standing research subscription.
            try {
                const savedRes = await fetch(
                    `${env.SUPABASE_URL}/rest/v1/mobile_saved_searches?active=eq.true&select=*`,
                    { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                );
                if (savedRes.ok) {
                    const searches = await savedRes.json() as any[];
                    const now = Date.now();
                    const intervalMs = (f: string) => (f === 'monthly' ? 30 : 7) * 24 * 60 * 60 * 1000;
                    const queue = env.MOBILE_QUEUE || env.STRIKE_QUEUE;

                    for (const s of searches) {
                        const lastRun = s.last_run_at ? new Date(s.last_run_at).getTime() : 0;
                        if (lastRun && (now - lastRun) < intervalMs(s.frequency)) continue; // not due yet
                        if (!queue) break;

                        // Only run what the user can afford (per-delivery billing happens in the consumer).
                        let available = 0;
                        try {
                            const balRes = await fetch(
                                `${env.SUPABASE_URL}/rest/v1/mobile_strike_balances?user_id=eq.${s.user_id}&select=credits_purchased,credits_used`,
                                { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                            );
                            if (balRes.ok) {
                                const rows = await balRes.json() as any[];
                                if (rows[0]) available = (rows[0].credits_purchased || 0) - (rows[0].credits_used || 0);
                            }
                        } catch { /* treat as 0 */ }

                        if (available <= 0) {
                            console.log(`⏭️ Recurring search #${s.id}: user out of credits, skipping (will retry next cycle).`);
                            continue;
                        }

                        const count = Math.min(s.strike_limit || 10, available);
                        await queue.send({
                            action: 'mobile_strike_generation',
                            userId: s.user_id,
                            targetAudience: s.target_audience,
                            market: s.market,
                            desiredOutcome: s.desired_outcome || '',
                            informationToGather: s.information_to_gather || '',
                            remainingCount: count,
                        });
                        await patchRow(env, 'mobile_saved_searches', { last_run_at: new Date(now).toISOString() }, 'id', s.id);
                        console.log(`🔁 Recurring search #${s.id} queued (${count} strikes) for user ${s.user_id}.`);
                    }
                }
            } catch (recurErr) {
                console.error('❌ Recurring saved-search processing failed:', recurErr);
            }

            // 0. Clean up stale agents (running >30 min) from previous runs.
            // A single agent dispatch enriches many triggers sequentially (Apollo + Gemini + Exa),
            // which can legitimately exceed several minutes — a 5-min reset would clear the lock
            // mid-run and let the next cron double-dispatch the same agent.
            try {
                const { createClient } = await import('@supabase/supabase-js');
                const supabase = createClient(env.SUPABASE_URL, env.SUPABASE_SERVICE_ROLE_KEY);

                const staleThreshold = new Date(Date.now() - 30 * 60 * 1000).toISOString();
                const { data: staleAgents, error: fetchErr } = await supabase
                    .from('agents')
                    .select('id, name')
                    .eq('status', 'running')
                    .lt('last_activity', staleThreshold);
                    
                if (fetchErr) {
                    console.error('❌ Failed to fetch stale agents:', fetchErr);
                } else if (staleAgents && staleAgents.length > 0) {
                    const staleIds = staleAgents.map(a => a.id);
                    console.log(`🧹 Resetting ${staleAgents.length} stale agents: ${staleAgents.map(a => `#${a.id}`).join(', ')}`);
                    
                    const { error: resetErr } = await supabase
                        .from('agents')
                        .update({ status: 'active', active_pipelines: 0, last_activity: new Date().toISOString() })
                        .in('id', staleIds);
                        
                    if (resetErr) {
                        console.error('❌ Failed to reset stale agents:', resetErr);
                    }
                }
            } catch (cleanupErr) {
                console.error('❌ Stale agent cleanup threw an error:', cleanupErr);
            }

            // 0b. Bulk-reset agents with STALE pipeline locks (active_pipelines > 0 AND idle >30 min).
            // The age guard is essential — clearing every lock unconditionally each cron would unlock
            // agents whose pipeline is still actively running, causing concurrent double-dispatch.
            try {
                const lockStaleThreshold = new Date(Date.now() - 30 * 60 * 1000).toISOString();
                const lockFilter = `active_pipelines=gt.0&last_activity=lt.${encodeURIComponent(lockStaleThreshold)}`;
                const lockResetRes = await fetch(
                    `${env.SUPABASE_URL}/rest/v1/agents?${lockFilter}&select=id`,
                    { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                );
                if (lockResetRes.ok) {
                    const lockedAgents = await lockResetRes.json() as any[];
                    if (lockedAgents.length > 0) {
                        await fetch(
                            `${env.SUPABASE_URL}/rest/v1/agents?${lockFilter}`,
                            {
                                method: 'PATCH',
                                headers: {
                                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                    'Content-Type': 'application/json',
                                    Prefer: 'return=minimal',
                                },
                                body: JSON.stringify({ active_pipelines: 0 }),
                            }
                        );
                        console.log(`🔓 CRON: Auto-reset ${lockedAgents.length} stale agent pipeline locks`);
                    }
                }
            } catch (lockErr) {
                console.warn('⚠️ Pipeline lock reset failed (non-blocking):', lockErr);
            }

            // 0d. Clean up stale pipeline_runs (status='running' for >30 min)
            try {
                const staleThreshold = new Date(Date.now() - 30 * 60 * 1000).toISOString();
                const staleRunQueryRes = await fetch(
                    `${env.SUPABASE_URL}/rest/v1/pipeline_runs?status=eq.running&started_at=lt.${staleThreshold}&select=id`,
                    { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } }
                );
                if (staleRunQueryRes.ok) {
                    const staleRuns = await staleRunQueryRes.json() as any[];
                    if (staleRuns.length > 0) {
                        await fetch(
                            `${env.SUPABASE_URL}/rest/v1/pipeline_runs?status=eq.running&started_at=lt.${staleThreshold}`,
                            {
                                method: 'PATCH',
                                headers: {
                                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                                    'Content-Type': 'application/json',
                                    Prefer: 'return=minimal',
                                },
                                body: JSON.stringify({
                                    status: 'failed',
                                    completed_at: new Date().toISOString()
                                }),
                            }
                        );
                        console.log(`🧹 CRON: Auto-reset ${staleRuns.length} stale pipeline runs to failed`);
                    }
                }
            } catch (staleRunErr) {
                console.warn('⚠️ Stale pipeline run cleanup failed (non-blocking):', staleRunErr);
            }

            // 1. Queue all active territory briefings to run asynchronously
            await queueTerritoryBriefings(env);

            // 1b. ── Exa.ai Credit Circuit Breaker ──────────────────────────
            // Probe Exa with a minimal request before dispatching 75 agents.
            // If credits are exhausted (HTTP 402), skip the entire agent batch
            // to avoid generating thousands of wasted pipeline_runs.
            let exaCreditsAvailable = true;
            try {
                const probeRes = await fetch('https://api.exa.ai/search', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': env.EXA_API_KEY,
                    },
                    body: JSON.stringify({
                        query: 'test',
                        numResults: 1,
                        type: 'neural',
                    }),
                });
                if (probeRes.status === 402) {
                    exaCreditsAvailable = false;
                    console.warn('🚨 EXA CIRCUIT BREAKER: Credits exhausted (HTTP 402). Skipping agent dispatch batch. Replenish credits at dashboard.exa.ai');
                } else if (probeRes.status === 429) {
                    exaCreditsAvailable = false;
                    console.warn('⚠️ EXA CIRCUIT BREAKER: Rate limited (HTTP 429). Skipping agent dispatch batch.');
                } else {
                    console.log('✅ Exa.ai credit probe OK — proceeding with agent dispatch');
                }
            } catch (probeErr) {
                console.warn('⚠️ Exa credit probe failed (network error), proceeding cautiously:', probeErr);
                // On network error, proceed anyway — might be transient
            }

            let agentsDispatched = 0;
            // 2. Fetch up to 150 active agents that haven't run recently (staggered load balancing)
            // NOTE: Agents ALWAYS dispatch regardless of Exa credit status.
            // FREE sensors (SEC Bulk, Fed Register, WARN Direct, USPTO Direct) cost $0,
            // and the queue consumer handles Exa failures gracefully via Promise.allSettled.
            if (!exaCreditsAvailable) {
                console.log('⚠️ Exa credits exhausted — agents will still dispatch using FREE sensors (SEC Bulk, Fed Register, WARN Direct, USPTO Direct).');
            }

            const agentLimit = exaCreditsAvailable ? 150 : 20;
            const url = `${env.SUPABASE_URL}/rest/v1/agents?status=eq.active&schedule=neq.manual&select=id,name,persona&order=last_activity.asc.nullsfirst&limit=${agentLimit}`;
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
            agentsDispatched = agents.length;

            console.log(`📡 Cron dispatching batched stagger of ${agents.length} active scheduled agents`);

            if (agents.length > 0 && env.STRIKE_QUEUE) {
                const agentIds = agents.map(a => a.id);
                
                // Bulk update all agents to 'running' instantly in a single query
                await fetch(`${env.SUPABASE_URL}/rest/v1/agents?id=in.(${agentIds.join(',')})`, {
                    method: 'PATCH',
                    headers: {
                        apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                        Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                        'Content-Type': 'application/json',
                        Prefer: 'return=minimal',
                    },
                    body: JSON.stringify({ status: 'running' }),
                });

                // Prepare messages for instantaneous batch queueing.
                // Propagate the Exa credit state so the consumer skips paid Exa calls when exhausted
                // (the breaker probe alone doesn't stop per-agent Exa spend).
                const messages = agents.map(agent => ({
                    body: {
                        campaignId: 0,
                        persona: agent.persona || "Rob O'Neill",
                        action: 'dispatch_agent',
                        agentId: agent.id,
                        skipExa: !exaCreditsAvailable,
                    }
                }));

                // Dispatch entire batch to Cloudflare Queue
                await safeQueueSendBatch(env.STRIKE_QUEUE, messages);
                console.log(`📤 Bulk-queued auto-dispatch for Agents: ${agentIds.join(', ')}`);

                // ── Queue FREE SENSOR SWEEP — runs ONCE per cron cycle ──
                try {
                    await env.STRIKE_QUEUE.send({
                        campaignId: 0,
                        persona: 'system',
                        action: 'free_sensor_sweep',
                    });
                    console.log('🏛️ Queued independent free sensor sweep (SEC, FedReg, WARN, USPTO)');
                } catch (sweepQueueErr) {
                    console.error('❌ Failed to queue free sensor sweep:', sweepQueueErr);
                }
            }
            // 2b. Dispatch Scheduled Funnel Sequence Steps
            const nowIso = new Date().toISOString();
            const schedUrl = `${env.SUPABASE_URL}/rest/v1/strike_campaigns?status=eq.scheduled&mobile_user_id=is.null&scheduled_send_at=lte.${nowIso}&select=id,workflow_id,funnel_id,strike_funnels(status)&limit=100`;
            const schedRes = await fetch(schedUrl, { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } });
            
            if (schedRes.ok) {
                const dueStrikes = await schedRes.json() as any[];
                if (dueStrikes.length > 0 && env.STRIKE_QUEUE) {
                    const validIds: number[] = [];
                    const messagesItems = [];

                    for (const strike of dueStrikes) {
                        const funnelStatus = strike.strike_funnels?.status || 'active';
                        // If funnel is active, push it onto the delivery queue
                        if (funnelStatus === 'active') {
                            validIds.push(strike.id);
                            messagesItems.push({
                                body: {
                                    campaignId: strike.id,
                                    workflowId: strike.workflow_id,
                                    persona: "Rob O'Neill", // Fallback persona, executor handles true persona
                                    action: 'deliver',
                                    forceInline: true // Ensure fast delivery since it's scheduled
                                }
                            });
                        } else {
                            // Suppress halted funnel strikes so they don't block the queue
                            await fetch(`${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=eq.${strike.id}`, {
                                method: 'PATCH',
                                headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ status: 'suppressed', failure_reason: `Funnel halted: ${funnelStatus}` })
                            });
                            console.log(`🛑 Suppressed delivery for #${strike.id} due to Kill Switch (Funnel Status: ${funnelStatus})`);
                        }
                    }

                    if (validIds.length > 0) {
                        // Set them immediately to 'active' (Processing)
                        await fetch(`${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=in.(${validIds.join(',')})`, {
                            method: 'PATCH',
                            headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' },
                            body: JSON.stringify({ status: 'active' })
                        });
                        
                        await safeQueueSendBatch(env.STRIKE_QUEUE, messagesItems);
                        console.log(`⏰ Queued ${validIds.length} newly activated Funnel Steps for dispatch!`);
                    }
                }
            }

            // 2c. Auto-recover and dispatch Approved campaigns that are stuck
            try {
                const appUrl = `${env.SUPABASE_URL}/rest/v1/strike_campaigns?status=eq.approved&mobile_user_id=is.null&select=id,workflow_id,email_subject,email_subject_b,drafted_body,target_id,sender_email&limit=50`;
                const appRes = await fetch(appUrl, { headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` } });
                if (appRes.ok) {
                    const approvedStrikes = await appRes.json() as any[];
                    if (approvedStrikes.length > 0 && env.STRIKE_QUEUE) {
                        console.log(`⏰ Cron auto-recovering ${approvedStrikes.length} approved campaigns for delivery...`);
                        const recoveryMessages = [];
                        const recoveredIds = [];
                        for (const strike of approvedStrikes) {
                             // Find target email — select both the column and the enrichment JSON.
                             const tr = await fetch(`${env.SUPABASE_URL}/rest/v1/lead_targets?id=eq.${strike.target_id}&select=email,enrichment_data`, {
                                 headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}` }
                             });
                             if (!tr.ok) {
                                 // Transient lookup failure — skip this tick rather than destroying the campaign.
                                 console.warn(`⚠️ Lead lookup failed (HTTP ${tr.status}) for approved campaign #${strike.id} — will retry next cron.`);
                                 continue;
                             }
                             const targets = await tr.json() as any[];
                             const target = Array.isArray(targets) ? targets[0] : undefined;
                             const recipientEmail = target ? (target.email || target.enrichment_data?.email || '') : '';

                             if (recipientEmail) {
                                 recoveredIds.push(strike.id);
                                 recoveryMessages.push({
                                     body: {
                                         action: 'deliver',
                                         campaignId: strike.id,
                                         workflowId: strike.workflow_id,
                                         emailSubject: strike.email_subject || `Introduction — ${strike.workflow_id}`,
                                         emailSubjectB: strike.email_subject_b || null,
                                         emailBody: strike.drafted_body || '',
                                         recipientEmail,
                                         senderEmail: strike.sender_email || 'fred@polsinellimgmt.com',
                                         senderName: 'Fred Posinelli',
                                     }
                                 });
                            } else {
                                // Mark as failed if no email
                                await fetch(`${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=eq.${strike.id}`, {
                                    method: 'PATCH',
                                    headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ status: 'failed', failure_reason: 'No recipient email found' })
                                });
                                console.warn(`⚠️ Marked approved campaign #${strike.id} as failed (no recipient email)`);
                            }
                        }

                        if (recoveryMessages.length > 0) {
                            // Set them immediately to 'active' (Processing)
                            await fetch(`${env.SUPABASE_URL}/rest/v1/strike_campaigns?id=in.(${recoveredIds.join(',')})`, {
                                method: 'PATCH',
                                headers: { apikey: env.SUPABASE_SERVICE_ROLE_KEY, Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`, 'Content-Type': 'application/json' },
                                body: JSON.stringify({ status: 'active' })
                            });
                            
                            await safeQueueSendBatch(env.STRIKE_QUEUE, recoveryMessages);
                            console.log(`⏰ Successfully recovered and queued ${recoveryMessages.length} approved strikes for delivery!`);
                        }
                    }
                }
            } catch (recoveryErr) {
                console.error('❌ Failed to run approved campaign auto-recovery:', recoveryErr);
            }

            // 2d. Recover stranded deliveries:
            //  - rows stuck 'active' >2h with no send (their queue message was dropped after retries)
            //  - rows with an orphaned delivery claim (sent_at set, no ses_message_id) >2h old
            // Both are reset to 'approved' so cron 2c re-dispatches them; the atomic sent_at claim
            // in the deliver consumer prevents any double-send.
            try {
                const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();
                const strandedHeaders = {
                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                    'Content-Type': 'application/json',
                    Prefer: 'return=representation',
                };
                const strandedActiveRes = await fetch(
                    `${env.SUPABASE_URL}/rest/v1/strike_campaigns?status=eq.active&mobile_user_id=is.null&sent_at=is.null&updated_at=lt.${encodeURIComponent(twoHoursAgo)}&or=(approved_at.not.is.null,funnel_id.not.is.null)&select=id`,
                    { method: 'PATCH', headers: strandedHeaders, body: JSON.stringify({ status: 'approved' }) }
                );
                if (strandedActiveRes.ok) {
                    const rows = await strandedActiveRes.json() as any[];
                    if (rows.length > 0) console.log(`🛟 Recovered ${rows.length} stranded 'active' deliveries back to 'approved': ${rows.map(r => `#${r.id}`).join(', ')}`);
                }
                const orphanedClaimRes = await fetch(
                    `${env.SUPABASE_URL}/rest/v1/strike_campaigns?sent_at=not.is.null&ses_message_id=is.null&status=in.(active,approved,scheduled)&updated_at=lt.${encodeURIComponent(twoHoursAgo)}&select=id`,
                    { method: 'PATCH', headers: strandedHeaders, body: JSON.stringify({ sent_at: null, status: 'approved' }) }
                );
                if (orphanedClaimRes.ok) {
                    const rows = await orphanedClaimRes.json() as any[];
                    if (rows.length > 0) console.log(`🛟 Released ${rows.length} orphaned delivery claims: ${rows.map(r => `#${r.id}`).join(', ')}`);
                }
            } catch (strandedErr) {
                console.warn('⚠️ Stranded delivery recovery failed (non-blocking):', strandedErr);
            }

            // 3. Run additional free sources in parallel (non-blocking)
            ctx.waitUntil(runAdditionalSources(env));

            // 4. Auto-run saved search missions (frequency = 'daily')
            ctx.waitUntil(runSavedSearchMissions(env));
            
            // 5. Archive and purge old error logs to R2
            ctx.waitUntil(archiveOldErrorLogs(env));

            // 6. Purge expired research-cache entries (Exa/Apollo/Gemini reuse cache)
            ctx.waitUntil(purgeExpiredResearchCache(env));

            console.log(`✅ Cron complete: ${agentsDispatched} agents dispatched + additional sources + missions launched`);
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

        // Fetch all saved searches with frequency = 'daily' that haven't run in the last ~23h.
        // Without this floor the */15 cron dispatches every "daily" mission 96×/day (huge Exa burn).
        const dueBefore = new Date(Date.now() - 23 * 60 * 60 * 1000).toISOString();
        const ssUrl = `${env.SUPABASE_URL}/rest/v1/saved_searches?frequency=eq.daily&or=(last_run_at.is.null,last_run_at.lt.${encodeURIComponent(dueBefore)})&select=*,territories(id,name)`;
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

        if (searches.length > 0 && env.STRIKE_QUEUE) {
            // 1. Bulk insert pipeline_runs
            const pipelineRunPayloads = searches.map(search => ({
                run_type: 'search_mission',
                agent_name: `Mission: ${search.name}`,
                status: 'running',
                triggered_by: 'cron',
                metadata: {
                    mission_id: search.id,
                    mission_name: search.name,
                    territory_id: search.territory_id,
                    territory_name: search.territories?.name || 'Unknown Territory',
                    query: search.exa_query,
                },
                started_at: new Date().toISOString(),
            }));

            const runsRes = await fetch(`${env.SUPABASE_URL}/rest/v1/pipeline_runs?select=id`, {
                method: 'POST',
                headers: {
                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                    'Content-Type': 'application/json',
                    Prefer: 'return=representation',
                },
                body: JSON.stringify(pipelineRunPayloads),
            });

            const runsData = runsRes.ok ? await runsRes.json() as any[] : [];

            // 2. Prepare batch queue messages
            const messages = searches.map((search, i) => ({
                body: {
                    campaignId: 0,
                    persona: "Rob O'Neill",
                    action: 'search_mission',
                    searchQuery: search.exa_query,
                    maxResults: 5,
                    runId: runsData[i]?.id,
                    missionId: search.id,
                    missionName: search.name,
                    territoryId: search.territory_id,
                    territoryName: search.territories?.name || 'Unknown Territory',
                }
            }));

            // 3. Batch dispatch to Queue
            await safeQueueSendBatch(env.STRIKE_QUEUE, messages);

            // 4. Bulk Patch last_run_at state
            const searchIds = searches.map(s => s.id);
            await fetch(`${env.SUPABASE_URL}/rest/v1/saved_searches?id=in.(${searchIds.join(',')})`, {
                method: 'PATCH',
                headers: {
                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                    'Content-Type': 'application/json',
                    Prefer: 'return=minimal',
                },
                body: JSON.stringify({ last_run_at: new Date().toISOString() }),
            });

            console.log(`✅ Bulk-dispatched ${searches.length} search missions instantly`);
        }
    } catch (err) {
        console.error('❌ Saved search missions error:', err);
    }
}

/**
 * Safely send a batch of messages to a Queue by chunking them to avoid Cloudflare Queues batch limit of 100 messages.
 */
async function safeQueueSendBatch(queue: Queue<any>, messages: any[]): Promise<void> {
    const CHUNK_SIZE = 50;
    for (let i = 0; i < messages.length; i += CHUNK_SIZE) {
        const chunk = messages.slice(i, i + CHUNK_SIZE);
        await queue.sendBatch(chunk);
    }
}

/**
 * ── Archival: Backup and Purge Error Logs ─────────────────────────────
 * Exports gemini_error_logs older than 5 days as a JSONL file to Cloudflare R2
 * and then deletes them from PostgreSQL to preserve audit trails while saving DB space.
 */
/** Deletes expired entries from the research reuse cache so the table stays small. Non-fatal. */
async function purgeExpiredResearchCache(env: Env): Promise<void> {
    try {
        const res = await fetch(
            `${env.SUPABASE_URL}/rest/v1/mobile_research_cache?expires_at=lt.${encodeURIComponent(new Date().toISOString())}`,
            {
                method: 'DELETE',
                headers: {
                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
                    Prefer: 'count=exact',
                },
            }
        );
        if (res.ok) {
            console.log(`🧽 Purged expired research-cache rows (${res.headers.get('content-range') || 'ok'}).`);
        }
    } catch (err) {
        console.warn('⚠️ Research-cache purge failed (non-fatal):', err);
    }
}

async function archiveOldErrorLogs(env: Env): Promise<void> {
    try {
        if (!env.CRM_ATTACHMENTS) return; // R2 binding required
        
        const fiveDaysAgo = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();
        const url = `${env.SUPABASE_URL}/rest/v1/gemini_error_logs?created_at=lt.${encodeURIComponent(fiveDaysAgo)}&select=*`;
        
        const res = await fetch(url, {
            headers: {
                apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
            }
        });
        
        if (!res.ok) return;
        const oldLogs = await res.json() as any[];
        
        if (oldLogs && oldLogs.length > 0) {
            // Convert to JSONL format. Include a full timestamp in the key so multiple runs
            // on the same day don't overwrite each other's archive (which previously destroyed
            // audit data, since the source rows are deleted right after upload).
            const jsonlContent = oldLogs.map(log => JSON.stringify(log)).join('\n');
            const stamp = new Date().toISOString().replace(/[:.]/g, '-');
            const fileName = `audit_logs/gemini_errors_${stamp}_backup.jsonl`;
            
            // Upload to R2
            await env.CRM_ATTACHMENTS.put(fileName, jsonlContent, {
                httpMetadata: { contentType: 'application/x-ndjson' }
            });
            console.log(`🗄️ Archived ${oldLogs.length} error logs to R2: ${fileName}`);
            
            // Purge the archived logs from Supabase
            const deleteUrl = `${env.SUPABASE_URL}/rest/v1/gemini_error_logs?created_at=lt.${encodeURIComponent(fiveDaysAgo)}`;
            await fetch(deleteUrl, {
                method: 'DELETE',
                headers: {
                    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
                    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
                }
            });
            console.log(`🧹 Purged ${oldLogs.length} archived error logs from Postgres`);
        }
    } catch (e) {
        console.error('❌ Error archiving old logs:', e);
    }
}

/**
 * Helper to map US states to standard timezones for Send-Time Optimization.
 */
function getTimezoneFromState(state?: string): string {
    if (!state) return 'America/Chicago'; // Default to Central
    const s = state.toUpperCase().trim();
    const eastern = ['CT', 'DE', 'DC', 'FL', 'GA', 'IN', 'KY', 'ME', 'MD', 'MA', 'MI', 'NH', 'NJ', 'NY', 'NC', 'OH', 'PA', 'RI', 'SC', 'TN', 'VT', 'VA', 'WV'];
    const central = ['AL', 'AR', 'IL', 'IA', 'KS', 'LA', 'MN', 'MS', 'MO', 'NE', 'ND', 'OK', 'SD', 'TX', 'WI'];
    const mountain = ['AZ', 'CO', 'ID', 'MT', 'NM', 'UT', 'WY'];
    const pacific = ['CA', 'NV', 'OR', 'WA'];
    if (eastern.includes(s)) return 'America/New_York';
    if (central.includes(s)) return 'America/Chicago';
    if (mountain.includes(s)) return 'America/Denver';
    if (pacific.includes(s)) return 'America/Los_Angeles';
    if (s === 'AK') return 'America/Anchorage';
    if (s === 'HI') return 'Pacific/Honolulu';
    return 'America/Chicago'; // Default
}

/**
 * Calculates delay in seconds to the next standard business hours window (9 AM - 5 PM, Mon-Fri) in the target timezone.
 */
function calculateDelayToNextBusinessHour(tz: string): number {
    const now = new Date();
    const future = new Date(now.getTime());
    // Try incrementing by 1 hour up to 120 hours (5 days) to find the next business day/hour
    for (let h = 1; h <= 120; h++) {
        future.setTime(now.getTime() + h * 60 * 60 * 1000);
        const formatter = new Intl.DateTimeFormat('en-US', {
            timeZone: tz,
            hour: 'numeric',
            weekday: 'short',
            hour12: false
        });
        const parts = formatter.formatToParts(future);
        const partMap = Object.fromEntries(parts.map(p => [p.type, p.value]));
        const localHour = parseInt(partMap.hour, 10);
        const localWeekday = partMap.weekday;

        if (localWeekday !== 'Sat' && localWeekday !== 'Sun' && localHour >= 9 && localHour < 17) {
            return Math.max(60, Math.floor((future.getTime() - now.getTime()) / 1000));
        }
    }
    return 3600; // Fallback to 1 hour
}

