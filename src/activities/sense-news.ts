/**
 * Activity: Sense News via NewsData.io
 * Searches for legal-adjacent business news to supplement Exa.ai coverage.
 * FREE TIER — 200 calls/day, no credit card needed.
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { GEMINI_REST_URL } from '../config/gemini';

// ---------------------------------------------------------------------------
// NewsData.io response shape
// ---------------------------------------------------------------------------

interface NewsArticle {
    article_id: string;
    title: string;
    link: string;
    description: string;
    content?: string;
    pubDate: string;
    creator?: string[];
    source_name: string;
    category?: string[];
    country?: string[];
}

interface NewsDataResponse {
    status: string;
    totalResults: number;
    results: NewsArticle[];
}

// ---------------------------------------------------------------------------
// Queries designed to COMPLEMENT Exa, not duplicate it
// ---------------------------------------------------------------------------

const NEWS_QUERIES = [
    { q: '"general counsel" OR "chief legal officer" appointed hired', label: 'GC Appointments' },
    { q: '"regulatory action" OR "consent decree" OR "enforcement"', label: 'Regulatory Events' },
    { q: '"corporate restructuring" OR "layoffs" OR "chapter 11 filing"', label: 'Distress Signals' },
] as const;

// ---------------------------------------------------------------------------
// Gemini-extracted metadata
// ---------------------------------------------------------------------------

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore?: number;
}

// ---------------------------------------------------------------------------
// Main activity — runs on cron / agent dispatch
// ---------------------------------------------------------------------------

export async function senseNews(env: Env): Promise<MarketTrigger[]> {
    const apiKey = (env as any).NEWSDATA_API_KEY;
    if (!apiKey) {
        console.warn('⚠️ NEWSDATA_API_KEY not set, skipping news sensor.');
        return [];
    }

    console.log('📰 Sensing news via NewsData.io...');

    const allResults: NewsArticle[] = [];
    const queryLabels: string[] = [];

    for (const query of NEWS_QUERIES) {
        try {
            const url = `https://newsdata.io/api/1/latest?apikey=${apiKey}&q=${encodeURIComponent(query.q)}&language=en&category=business&size=5`;
            const res = await fetch(url);

            if (!res.ok) { console.warn(`⚠️ News "${query.label}" failed (${res.status})`); continue; }

            const data = await res.json() as NewsDataResponse;
            for (const article of (data?.results || [])) {
                allResults.push(article);
                queryLabels.push(query.label);
            }
            console.log(`📰 News ${query.label}: ${data?.results?.length || 0} articles`);
        } catch (err) {
            console.warn(`⚠️ News "${query.label}" exception:`, err);
        }
    }

    if (allResults.length === 0) { console.warn('⚠️ No news results.'); return []; }

    return extractAndBuildTriggers(env, allResults, queryLabels);
}

// ---------------------------------------------------------------------------
// Query-specific variant for Search Missions
// ---------------------------------------------------------------------------

export async function senseNewsForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    const apiKey = (env as any).NEWSDATA_API_KEY;
    if (!apiKey) {
        console.warn('⚠️ NEWSDATA_API_KEY not set, skipping news mission.');
        return [];
    }

    console.log(`📰 News mission: "${query}"`);

    try {
        const url = `https://newsdata.io/api/1/latest?apikey=${apiKey}&q=${encodeURIComponent(query)}&language=en&category=business&size=10`;
        const res = await fetch(url);

        if (!res.ok) { console.warn(`⚠️ News mission failed (${res.status})`); return []; }

        const data = await res.json() as NewsDataResponse;
        if (!data?.results?.length) return [];

        const labels = data.results.map(() => 'Mission Search');
        return extractAndBuildTriggers(env, data.results, labels);
    } catch (err) {
        console.error('❌ News mission error:', err);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Shared: Gemini extraction + MarketTrigger assembly
// ---------------------------------------------------------------------------

async function extractAndBuildTriggers(
    env: Env,
    articles: NewsArticle[],
    queryLabels: string[],
): Promise<MarketTrigger[]> {
    const itemsPrompt = articles.map((a, i) => {
        return `[Article ${i}]
Title: ${a.title}
Source: ${a.source_name}
Published: ${a.pubDate}
Description: ${a.description || 'N/A'}
Content: ${(a.content || '').slice(0, 800)}
Category: ${queryLabels[i]}`;
    }).join('\n\n');

    const systemPrompt = `You are a data extraction AI for a law firm's business development. Given news articles, extract:
1. The PRIMARY COMPANY involved in the news event
2. The HIGHEST-RANKING EXECUTIVE mentioned — look for names in quotes, bylines, or mentions
3. A relevance score 0-100 for how actionable this is for law firm outreach

Rules:
- Focus on the company most likely to need legal services
- For GC appointments: the company and the new General Counsel
- For regulatory events: the company facing regulation and its CEO or GC
- For restructuring: the distressed company and its CEO
- If no executive name found, use "Unknown" with likely title
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO", "relevanceScore": 80 }]`;

    let extracted: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetch(`${GEMINI_REST_URL}?key=${env.GEMINI_API_KEY}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: { parts: [{ text: systemPrompt }] },
                contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                generationConfig: { temperature: 0.1, responseMimeType: 'application/json' },
            }),
        });
        if (!geminiRes.ok) throw new Error(await geminiRes.text());

        const gd = await geminiRes.json() as any;
        const rawText = gd?.candidates?.[0]?.content?.parts?.find((p: any) => p.text)?.text;
        if (rawText) {
            extracted = JSON.parse(rawText);
            console.log(`📋 Gemini extracted ${extracted.length} news entities`);
        }
    } catch (err) {
        console.error('❌ Gemini news extraction failed:', err);
        return [];
    }

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const a = articles[meta.index];
        if (!a) continue;

        triggers.push({
            triggerId: `news-${crypto.randomUUID().slice(0, 8)}`,
            source: 'NewsData.io',
            sourceUrl: a.link || '',
            headline: a.title || 'News event detected',
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 60,
            detectedAt: a.pubDate || new Date().toISOString(),
            articleText: a.description || a.content?.slice(0, 500) || '',
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 10);
    console.log(`✅ ${top.length} triggers from NewsData.io`);
    return top;
}
