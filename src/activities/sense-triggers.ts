/**
 * Activity: Sense Market Triggers
 * Scans Exa.ai for actionable market events relevant to PPA+ institutional outreach.
 * Uses native fetch — no Node.js built-ins.
 */
import type { Env } from '../index';
import { fetchRow, patchRow } from '../utils/supabase';

export interface MarketTrigger {
    triggerId: string;
    source: string;
    sourceUrl: string;
    headline: string;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    detectedAt: string;
    articleText?: string;
    agentId: number; // 1-5, maps to EXA_QUERIES index + 1
}

// ---------------------------------------------------------------------------
// Exa.ai response shapes (minimal, only the fields we consume)
// ---------------------------------------------------------------------------

interface ExaResult {
    id: string;
    url: string;
    title: string;
    publishedDate?: string;
    author?: string;
    score?: number;
    text?: string;
    highlights?: string[];
}

interface ExaSearchResponse {
    results: ExaResult[];
}

// ---------------------------------------------------------------------------
// Search queries — five signal buckets that matter to PPA+ outreach
// ---------------------------------------------------------------------------

const CURRENT_YEAR = new Date().getFullYear().toString();

const EXA_QUERIES = [
    `"acquisition" OR "merger" OR "deal closed" financial services bank ${CURRENT_YEAR} announcement`,
    `"raises" OR "closes fund" OR "launch" private equity venture capital billion million ${CURRENT_YEAR}`,
    `"appointed" OR "named" OR "hires" CEO CFO CIO "managing director" asset management financial services ${CURRENT_YEAR}`,
    `"strategic partnership" OR "joint venture" OR "advisory" investment bank deal ${CURRENT_YEAR}`,
    `"IPO" OR "SPAC" OR "public offering" OR "regulatory approval" financial institution ${CURRENT_YEAR}`,
] as const;

import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';

// ---------------------------------------------------------------------------
// Extracted Data Type
// ---------------------------------------------------------------------------

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
}

// ---------------------------------------------------------------------------
// Main activity
// ---------------------------------------------------------------------------

export async function senseTriggers(env: Env): Promise<MarketTrigger[]> {
    console.log('🔍 Sensing market triggers via Exa.ai...');

    const triggers: MarketTrigger[] = [];

    // Run all query buckets sequentially — track which agent (query) found each result
    const allExaResults: ExaResult[] = [];
    const resultAgentMap: number[] = []; // parallel array: agentId for each result
    for (let qi = 0; qi < EXA_QUERIES.length; qi++) {
        const query = EXA_QUERIES[qi];
        const agentId = qi + 1; // agents are 1-indexed in DB
        try {
            const res = await fetch('https://api.exa.ai/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': env.EXA_API_KEY,
                },
                body: JSON.stringify({
                    query,
                    numResults: 3,
                    type: 'neural',
                    useAutoprompt: true,
                    startPublishedDate: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
                    excludeDomains: [
                        'wikipedia.org',
                        'investopedia.com',
                        'britannica.com',
                        'wikiwand.com',
                        'reddit.com',
                        'quora.com',
                    ],
                    contents: {
                        text: { maxCharacters: 1500 },
                        highlights: { numSentences: 2, highlightsPerUrl: 1 },
                    },
                }),
            });

            if (!res.ok) {
                const errBody = await res.text();
                console.warn(`⚠️ Exa query ${agentId} failed (${res.status}): ${errBody}`);
                continue;
            }

            const data = await res.json() as ExaSearchResponse;
            if (Array.isArray(data.results)) {
                for (const r of data.results) {
                    allExaResults.push(r);
                    resultAgentMap.push(agentId);
                }
                console.log(`🤖 Agent ${agentId}: found ${data.results.length} results`);
            }
        } catch (err) {
            console.warn(`⚠️ Exa query ${agentId} exception:`, err);
        }
    }

    if (allExaResults.length === 0) {
        console.warn('⚠️ No Exa results found across any queries.');
        return [];
    }

    // Post-filter: remove any Wikipedia/encyclopedia URLs that slipped through
    const blockedDomains = ['wikipedia.org', 'investopedia.com', 'britannica.com', 'wikiwand.com', 'wikidata.org', 'dbpedia.org'];
    const filteredResults: ExaResult[] = [];
    const filteredAgentMap: number[] = [];
    for (let i = 0; i < allExaResults.length; i++) {
        const url = (allExaResults[i].url || '').toLowerCase();
        const blocked = blockedDomains.some(d => url.includes(d));
        if (blocked) {
            console.log(`🚫 Filtered out non-news URL: ${allExaResults[i].url}`);
        } else {
            filteredResults.push(allExaResults[i]);
            filteredAgentMap.push(resultAgentMap[i]);
        }
    }

    if (filteredResults.length === 0) {
        console.warn('⚠️ All Exa results filtered out (non-news sources).');
        return [];
    }

    console.log(`📊 ${filteredResults.length} results after domain filtering (from ${allExaResults.length} raw)`);

    // 2. Batch all results and ask Gemini to extract the names
    const itemsPrompt = filteredResults.map((r, i) =>
        `[Item ${i}]\nTitle: ${r.title}\nText: ${r.text ?? r.highlights?.join(' ') ?? ''}\nURL: ${r.url}`
    ).join('\n\n');

    const systemPrompt = `You are a precise data extraction AI for business development. Given news articles, extract:
1. The PRIMARY COMPANY involved in the deal/event (the one a consulting firm would want to contact)
2. The HIGHEST-RANKING EXECUTIVE mentioned by name in the article

CRITICAL RULES FOR EXECUTIVE NAME EXTRACTION:
- Look carefully for ANY named person in the article — CEOs, CFOs, Presidents, Chairmen, Managing Directors, Partners, Founders
- Names often appear in quotes, bylines, or phrases like "said CEO John Smith" or "led by Managing Director Jane Doe"
- If a press release mentions the company, look for the executive who signed it or is quoted
- If the article mentions multiple people, pick the most senior one
- ONLY return "Unknown" if truly NO human name appears anywhere in the text
- DO NOT use "Key Decision-Maker" or any other placeholder — either a real name or "Unknown"

Respond with ONLY a JSON array:
[
  { "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO" }
]`;

    let extractedData: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-triggers',
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: { parts: [{ text: systemPrompt }] },
                contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                generationConfig: {
                    temperature: 0.1,
                    responseMimeType: 'application/json',
                    responseSchema: {
                        type: "ARRAY",
                        description: "List of extracted companies and executives from news.",
                        items: {
                            type: "OBJECT",
                            properties: {
                                index: { type: "INTEGER", description: "The [Item X] index number from the prompt" },
                                company: { type: "STRING", description: "Primary company involved in the event" },
                                executiveName: { type: "STRING", description: "Highest ranking executive name, or 'Unknown' if none" },
                                executiveTitle: { type: "STRING" }
                            },
                            required: ["index", "company", "executiveName", "executiveTitle"]
                        }
                    }
                },
            }),
        });

        if (!geminiRes.ok) throw new Error(await geminiRes.text());

        const geminiData = await geminiRes.json() as any;
        // Gemini 3 may return multiple parts (text + thoughtSignature). Find the text part.
        const parts = geminiData?.candidates?.[0]?.content?.parts || [];
        const rawText = parts.find((p: any) => p.text)?.text;
        console.log(`🤖 Gemini extraction: ${parts.length} parts, text length: ${rawText?.length || 0}`);

        if (rawText) {
            extractedData = JSON.parse(rawText) as ExtractedMeta[];
            console.log(`📋 Extracted ${extractedData.length} meta entries from Gemini`);
        }
    } catch (err) {
        console.error('❌ Gemini extraction failed:', err);
        await logGeminiError(env, 'lite-trigger-extraction', 'sense-triggers', err, { itemsCount: filteredResults.length });
        return []; // Fail safe, don't generate garbage triggers
    }

    // 3. Re-assemble triggers — allow "Unknown" names, Apollo will resolve them later
    for (const meta of extractedData) {
        const result = filteredResults[meta.index];
        if (!result) continue;

        // Only discard explicitly bad placeholders
        const nameLower = meta.executiveName.toLowerCase();
        if (nameLower.includes('decision-maker')) {
            console.log(`🗑️ Discarding trigger (placeholder name): ${result.title}`);
            continue;
        }

        triggers.push({
            triggerId: `trg-${crypto.randomUUID().slice(0, 8)}`,
            source: 'Exa.ai',
            sourceUrl: result.url ?? '',
            headline: result.title ?? 'Market event detected',
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: Math.round((result.score ?? 0.7) * 100),
            detectedAt: result.publishedDate ?? new Date().toISOString(),
            articleText: result.text || result.highlights?.join(' ') || '',
            agentId: filteredAgentMap[meta.index] || 1,
        });
    }

    // Sort descending by relevance, keep top 10
    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 10);

    console.log(`✅ Detected ${top.length} market triggers from Exa.ai`);
    return top;
}

export async function senseTriggersForAgent(env: Env, agent: any, runId?: string): Promise<MarketTrigger[]> {
    console.log(`🔍 Sensing market triggers for custom agent: ${agent.name} (#${agent.id})...`);

    const triggers: MarketTrigger[] = [];

    // Run the specific agent query
    const allExaResults: ExaResult[] = [];
    const query = agent.exa_query;
    if (!query) {
        console.warn(`⚠️ Agent #${agent.id} has no exa_query defined.`);
        return [];
    }

    try {
        const res = await fetch('https://api.exa.ai/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.EXA_API_KEY,
            },
            body: JSON.stringify({
                query,
                numResults: agent.max_leads_per_run || 5, // use configured limit
                type: 'neural',
                useAutoprompt: true,
                startPublishedDate: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
                excludeDomains: [
                    'wikipedia.org',
                    'investopedia.com',
                    'britannica.com',
                    'wikiwand.com',
                    'reddit.com',
                    'quora.com',
                ],
                contents: {
                    text: { maxCharacters: 1500 },
                    highlights: { numSentences: 2, highlightsPerUrl: 1 },
                },
            }),
        });

        if (!res.ok) {
            const errBody = await res.text();
            if (res.status === 402 || res.status === 429) {
                console.error(`🚨 Exa.ai CREDITS EXHAUSTED (${res.status}) for Agent #${agent.id}`);
                if (runId) {
                    try {
                        const rows = await fetchRow(env, 'pipeline_runs', 'id', runId);
                        const existingMeta = rows?.[0]?.metadata || {};
                        await patchRow(env, 'pipeline_runs', { metadata: { ...existingMeta, exa_status: 'credits_exhausted', exa_error_code: res.status } }, 'id', runId);
                    } catch(e) { console.warn('Failed to tag exa bounds', e); }
                }
            } else {
                console.warn(`⚠️ Exa custom query failed (${res.status}): ${errBody}`);
            }
            return [];
        }

        if (runId) {
            try {
                const rows = await fetchRow(env, 'pipeline_runs', 'id', runId);
                const existingMeta = rows?.[0]?.metadata || {};
                await patchRow(env, 'pipeline_runs', { metadata: { ...existingMeta, exa_status: 'operational' } }, 'id', runId);
            } catch(e) { console.warn('Failed to tag exa operational', e); }
        }

        const data = await res.json() as ExaSearchResponse;
        if (Array.isArray(data.results)) {
            allExaResults.push(...data.results);
            console.log(`🤖 Custom Agent ${agent.id} found ${data.results.length} results`);
        }
    } catch (err) {
        console.warn(`⚠️ Exa custom query exception:`, err);
        return [];
    }

    if (allExaResults.length === 0) {
        console.warn(`⚠️ No Exa results found for agent #${agent.id}.`);
        return [];
    }

    // Post-filter: remove encyclopedias
    const blockedDomains = ['wikipedia.org', 'investopedia.com', 'britannica.com', 'wikiwand.com', 'wikidata.org', 'dbpedia.org'];
    const filteredResults: ExaResult[] = [];
    for (let i = 0; i < allExaResults.length; i++) {
        const url = (allExaResults[i].url || '').toLowerCase();
        const blocked = blockedDomains.some(d => url.includes(d));
        if (blocked) {
            console.log(`🚫 Filtered out non-news URL: ${allExaResults[i].url}`);
        } else {
            filteredResults.push(allExaResults[i]);
        }
    }

    if (filteredResults.length === 0) {
        console.warn('⚠️ All Exa results filtered out (non-news sources).');
        return [];
    }

    // 2. Extractor with Gemini
    const itemsPrompt = filteredResults.map((r, i) =>
        `[Item ${i}]\nTitle: ${r.title}\nText: ${r.text ?? r.highlights?.join(' ') ?? ''}\nURL: ${r.url}`
    ).join('\n\n');

    const systemPrompt = `You are a precise data extraction AI for business development. Given news articles, extract:
1. The PRIMARY COMPANY involved in the deal/event (the one a consulting firm would want to contact)
2. The HIGHEST-RANKING EXECUTIVE mentioned by name in the article

CRITICAL RULES FOR EXECUTIVE NAME EXTRACTION:
- Look carefully for ANY named person in the article — CEOs, CFOs, Presidents, Chairmen, Managing Directors, Partners, Founders
- Names often appear in quotes, bylines, or phrases like "said CEO John Smith" or "led by Managing Director Jane Doe"
- If a press release mentions the company, look for the executive who signed it or is quoted
- If the article mentions multiple people, pick the most senior one
- ONLY return "Unknown" if truly NO human name appears anywhere in the text
- DO NOT use "Key Decision-Maker" or any other placeholder — either a real name or "Unknown"

Respond with ONLY a JSON array:
[
  { "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO" }
]`;

    let extractedData: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-mission',
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: { parts: [{ text: systemPrompt }] },
                contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                generationConfig: {
                    temperature: 0.1,
                    responseMimeType: 'application/json',
                    responseSchema: {
                        type: "ARRAY",
                        description: "List of extracted companies and executives from news.",
                        items: {
                            type: "OBJECT",
                            properties: {
                                index: { type: "INTEGER", description: "The [Item X] index number from the prompt" },
                                company: { type: "STRING", description: "Primary company involved in the event" },
                                executiveName: { type: "STRING", description: "Highest ranking executive name, or 'Unknown' if none" },
                                executiveTitle: { type: "STRING" }
                            },
                            required: ["index", "company", "executiveName", "executiveTitle"]
                        }
                    }
                },
            }),
        });

        if (!geminiRes.ok) throw new Error(await geminiRes.text());

        const geminiData = await geminiRes.json() as any;
        const parts = geminiData?.candidates?.[0]?.content?.parts || [];
        const rawText = parts.find((p: any) => p.text)?.text;

        if (rawText) {
            extractedData = JSON.parse(rawText) as ExtractedMeta[];
        }
    } catch (err) {
        console.error('❌ Gemini extraction failed:', err);
        await logGeminiError(env, 'lite-agent-extraction', `sense-triggers:agent-${agent.id}`, err, { itemsCount: filteredResults.length });
        return [];
    }

    // 3. Re-assemble triggers
    for (const meta of extractedData) {
        const result = filteredResults[meta.index];
        if (!result) continue;

        const nameLower = meta.executiveName.toLowerCase();
        if (nameLower.includes('decision-maker')) {
            continue;
        }

        triggers.push({
            triggerId: `trg-${crypto.randomUUID().slice(0, 8)}`,
            source: 'Exa.ai',
            sourceUrl: result.url ?? '',
            headline: result.title ?? 'Market event detected',
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: Math.round((result.score ?? 0.7) * 100),
            detectedAt: result.publishedDate ?? new Date().toISOString(),
            articleText: result.text || result.highlights?.join(' ') || '',
            agentId: agent.id, // Set the current agent ID
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const resultCount = agent.max_leads_per_run || 5;
    return triggers.slice(0, resultCount);
}
