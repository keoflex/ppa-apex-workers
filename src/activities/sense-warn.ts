/**
 * Activity: Sense WARN Act & Layoff Notices via Exa.ai
 * Searches for mass layoff announcements, WARN act filings, and corporate restructuring.
 * Extremely relevant for Employment and Bankruptcy/Restructuring law firms.
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

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

// Queries specifically targeting WARN notices and mass layoffs
const WARN_QUERIES = [
    `"WARN act notice" OR "Worker Adjustment and Retraining Notification" layoffs`,
    `"mass layoffs" OR "workforce reduction" OR "closing facility" restructuring`,
    `"chapter 11" OR "bankruptcy" laying off employees`
] as const;

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore?: number;
}

export async function senseWarnNotices(env: Env): Promise<MarketTrigger[]> {
    console.log('🚨 Sensing WARN Act and layoff notices via Exa.ai...');

    const allResults: ExaResult[] = [];
    const queryLabels: string[] = [];

    for (const query of WARN_QUERIES) {
        try {
            const res = await fetch('https://api.exa.ai/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': env.EXA_API_KEY,
                },
                body: JSON.stringify({
                    query,
                    numResults: 5,
                    type: 'neural',
                    useAutoprompt: true,
                    startPublishedDate: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
                    excludeDomains: ['wikipedia.org', 'investopedia.com', 'reddit.com'],
                    contents: {
                        text: { maxCharacters: 1500 },
                        highlights: { numSentences: 2, highlightsPerUrl: 1 },
                    },
                }),
            });

            if (!res.ok) {
                console.warn(`⚠️ Exa WARN query failed (${res.status})`);
                continue;
            }

            const data = await res.json() as ExaSearchResponse;
            for (const r of (data?.results || [])) {
                allResults.push(r);
                queryLabels.push('Layoff/WARN Notice');
            }
            console.log(`🚨 WARN Exa: ${data?.results?.length || 0} results`);
        } catch (err) {
            console.warn(`⚠️ Exa WARN exception:`, err);
        }
    }

    if (allResults.length === 0) { console.warn('⚠️ No WARN/Layoff results.'); return []; }

    // Deduplicate by URL
    const uniqueResults = [];
    const seenUrls = new Set();
    const uniqueLabels = [];
    for (let i = 0; i < allResults.length; i++) {
        if (!seenUrls.has(allResults[i].url)) {
            seenUrls.add(allResults[i].url);
            uniqueResults.push(allResults[i]);
            uniqueLabels.push(queryLabels[i]);
        }
    }

    return extractAndBuildTriggers(env, uniqueResults, uniqueLabels);
}

export async function senseWarnNoticesForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`🚨 WARN mission: "${query}"`);

    try {
        const res = await fetch('https://api.exa.ai/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.EXA_API_KEY,
            },
            body: JSON.stringify({
                query: `${query} AND ("WARN act" OR layoff OR workforce reduction)`,
                numResults: 10,
                type: 'neural',
                useAutoprompt: true,
                startPublishedDate: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
                excludeDomains: ['wikipedia.org', 'investopedia.com', 'reddit.com'],
                contents: {
                    text: { maxCharacters: 1500 },
                    highlights: { numSentences: 2, highlightsPerUrl: 1 },
                },
            }),
        });

        if (!res.ok) { console.warn(`⚠️ Exa WARN mission failed (${res.status})`); return []; }

        const data = await res.json() as ExaSearchResponse;
        if (!data?.results?.length) return [];

        const labels = data.results.map(() => 'Mission Search: Layoff/WARN');
        return extractAndBuildTriggers(env, data.results, labels);
    } catch (err) {
        console.error('❌ Exa WARN mission error:', err);
        return [];
    }
}

async function extractAndBuildTriggers(
    env: Env,
    results: ExaResult[],
    queryLabels: string[],
): Promise<MarketTrigger[]> {
    const itemsPrompt = results.map((r, i) =>
        `[Item ${i}]\nTitle: ${r.title}\nText: ${r.text ?? r.highlights?.join(' ') ?? ''}\nURL: ${r.url}\nCategory: ${queryLabels[i]}`
    ).join('\n\n');

    const systemPrompt = `You are a data extraction AI for a law firm's business development. Given news articles about corporate layoffs and WARN act notices, extract:
1. The PRIMARY COMPANY involved in the layoffs
2. The HIGHEST-RANKING EXECUTIVE mentioned (CEO, Founder, HR Director, etc.)
3. A relevance score 0-100 for how actionable this is for law firm outreach (e.g. higher score if it's a large mass layoff)

Rules:
- If no executive name found, use "Unknown" with likely title
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO", "relevanceScore": 80 }]`;

    let extracted: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-warn',
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: { parts: [{ text: systemPrompt }] },
                contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                generationConfig: {
                    temperature: 0.1,
                    maxOutputTokens: 2048,
                    responseMimeType: 'application/json',
                    responseSchema: {
                        type: "ARRAY",
                        items: {
                            type: "OBJECT",
                            properties: {
                                index: { type: "INTEGER" },
                                company: { type: "STRING" },
                                executiveName: { type: "STRING" },
                                executiveTitle: { type: "STRING" },
                                relevanceScore: { type: "INTEGER" }
                            },
                            required: ["index", "company", "executiveName", "executiveTitle", "relevanceScore"]
                        }
                    }
                },
            }),
        });
        if (!geminiRes.ok) throw new Error(await geminiRes.text());

        const { text: rawText } = await safeGeminiResponseParse(geminiRes);
        if (rawText) {
            let jsonStr = rawText;
            const match = rawText.match(/\[[\s\S]*\]/);
            if (match) jsonStr = match[0];
            extracted = safeJsonParse<ExtractedMeta[]>(jsonStr, []);
            console.log(`📋 Gemini extracted ${extracted.length} WARN entities`);
        }
    } catch (err) {
        console.error('❌ Gemini WARN extraction failed:', err);
        await logGeminiError(env, 'lite-warn-extraction', 'sense-warn', err, { itemsCount: results.length });
        return [];
    }

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const r = results[meta.index];
        if (!r) continue;

        triggers.push({
            triggerId: `warn-${crypto.randomUUID().slice(0, 8)}`,
            source: 'Exa.ai WARN Notice',
            sourceUrl: r.url || '',
            headline: r.title || 'Layoff event detected',
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 75,
            detectedAt: r.publishedDate || new Date().toISOString(),
            articleText: r.text || r.highlights?.join(' ') || '',
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 10);
    console.log(`✅ ${top.length} triggers from WARN/Layoffs`);
    return top;
}
