/**
 * Activity: Sense Court Filings via CourtListener (RECAP)
 * Searches federal court filings for litigation triggers relevant to PPA+ outreach.
 * FREE — API works without auth for basic search, but token recommended for higher limits.
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';

// ---------------------------------------------------------------------------
// CourtListener search response shape
// ---------------------------------------------------------------------------

interface CourtResult {
    caseName: string;
    dateFiled: string;
    court: string;
    court_id: string;
    docketNumber: string;
    docket_absolute_url: string;
    cause: string;
    attorney: string[];
    party: string[];
    firm: string[];
    jurisdictionType: string;
}

interface CourtSearchResponse {
    count: number;
    results: CourtResult[];
}

// ---------------------------------------------------------------------------
// Legal-relevant queries for PPA practice areas
// ---------------------------------------------------------------------------

const COURT_QUERIES = [
    { q: '"chapter 11" OR "voluntary petition" OR "bankruptcy"', label: 'Restructuring / Bankruptcy' },
    { q: '"securities fraud" OR "class action" OR "shareholder"', label: 'Securities Litigation' },
    { q: '"patent infringement" OR "trade secret"', label: 'IP Litigation' },
    { q: '"antitrust" OR "unfair competition" OR "FTC"', label: 'Regulatory / Antitrust' },
    { q: '"breach of fiduciary" OR "derivative action" OR "proxy"', label: 'Corporate Governance' },
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

export async function senseCourtFilings(env: Env): Promise<MarketTrigger[]> {
    console.log('⚖️ Sensing federal court filings via CourtListener...');

    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const allResults: CourtResult[] = [];
    const queryLabels: string[] = [];

    const headers: Record<string, string> = { 'Accept': 'application/json' };
    if ((env as any).COURTLISTENER_API_KEY) {
        headers['Authorization'] = `Token ${(env as any).COURTLISTENER_API_KEY}`;
    }

    for (const query of COURT_QUERIES) {
        try {
            const url = `https://www.courtlistener.com/api/rest/v4/search/?type=r&q=${encodeURIComponent(query.q)}&filed_after=${thirtyDaysAgo}&order_by=dateFiled+desc&page_size=5`;
            const res = await fetch(url, { headers });

            if (!res.ok) { console.warn(`⚠️ Court "${query.label}" failed (${res.status})`); continue; }

            const data = await res.json() as CourtSearchResponse;
            for (const r of (data?.results || [])) {
                allResults.push(r);
                queryLabels.push(query.label);
            }
            console.log(`⚖️ Court ${query.label}: ${data?.results?.length || 0} cases`);
            await new Promise(r => setTimeout(r, 500)); // polite delay
        } catch (err) {
            console.warn(`⚠️ Court "${query.label}" exception:`, err);
        }
    }

    if (allResults.length === 0) { console.warn('⚠️ No court results.'); return []; }

    return extractAndBuildTriggers(env, allResults, queryLabels);
}

// ---------------------------------------------------------------------------
// Query-specific variant for Search Missions
// ---------------------------------------------------------------------------

export async function senseCourtFilingsForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`⚖️ Court mission: "${query}"`);

    const sixtyDaysAgo = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const headers: Record<string, string> = { 'Accept': 'application/json' };
    if ((env as any).COURTLISTENER_API_KEY) {
        headers['Authorization'] = `Token ${(env as any).COURTLISTENER_API_KEY}`;
    }

    try {
        const url = `https://www.courtlistener.com/api/rest/v4/search/?type=r&q=${encodeURIComponent(query)}&filed_after=${sixtyDaysAgo}&order_by=dateFiled+desc&page_size=10`;
        const res = await fetch(url, { headers });

        if (!res.ok) { console.warn(`⚠️ Court mission failed (${res.status})`); return []; }

        const data = await res.json() as CourtSearchResponse;
        if (!data?.results?.length) return [];

        const labels = data.results.map(() => 'Mission Search');
        return extractAndBuildTriggers(env, data.results, labels);
    } catch (err) {
        console.error('❌ Court mission error:', err);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Shared: Gemini extraction + MarketTrigger assembly
// ---------------------------------------------------------------------------

async function extractAndBuildTriggers(
    env: Env,
    results: CourtResult[],
    queryLabels: string[],
): Promise<MarketTrigger[]> {
    const itemsPrompt = results.map((r, i) => {
        return `[Case ${i}]
Case: ${r.caseName}
Court: ${r.court}
Filed: ${r.dateFiled}
Docket: ${r.docketNumber}
Cause: ${r.cause || 'N/A'}
Parties: ${r.party?.join(', ') || 'N/A'}
Attorneys: ${r.attorney?.join(', ') || 'N/A'}
Firms: ${r.firm?.join(', ') || 'N/A'}
Category: ${queryLabels[i]}`;
    }).join('\n\n');

    const systemPrompt = `You are a data extraction AI for a law firm's business development. Given federal court filings, extract:
1. The PRIMARY COMPANY or ORGANIZATION that is a defendant or major party (the one a law firm would want to represent or know about)
2. The HIGHEST-RANKING EXECUTIVE or decision-maker at that company — look for named parties who are officers, or infer from the company name
3. A relevance score 0-100 for how actionable this is for law firm outreach

Rules:
- Focus on CORPORATE parties, not individual plaintiffs in consumer cases
- For bankruptcy cases: the debtor company and its CEO/CFO
- For securities fraud: the defendant company and its General Counsel
- For IP cases: the defendant company and its CTO or General Counsel
- If no specific executive name, use "Unknown" but provide the likely title
- CRITICAL: DO NOT output every filing! Only output filings with a relevance score of 50 or higher. Ignore low-relevance filings entirely to keep the JSON array extremely small and fast to generate.
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "General Counsel", "relevanceScore": 75 }]`;

    let extracted: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-court-filings',
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

        const gd = await geminiRes.json() as any;
        const rawText = gd?.candidates?.[0]?.content?.parts?.find((p: any) => p.text)?.text;
        if (rawText) {
            extracted = JSON.parse(rawText);
            console.log(`📋 Gemini extracted ${extracted.length} court entities`);
        }
    } catch (err) {
        console.error('❌ Gemini court extraction failed:', err);
        await logGeminiError(env, 'lite-court-extraction', 'sense-court-filings', err, { itemsCount: results.length });
        return [];
    }

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const r = results[meta.index];
        if (!r) continue;

        triggers.push({
            triggerId: `court-${crypto.randomUUID().slice(0, 8)}`,
            source: 'CourtListener',
            sourceUrl: `https://www.courtlistener.com${r.docket_absolute_url}`,
            headline: `${r.caseName} — ${queryLabels[meta.index]}`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 65,
            detectedAt: r.dateFiled || new Date().toISOString(),
            articleText: `Federal case: ${r.caseName}. Court: ${r.court}. Filed ${r.dateFiled}. Cause: ${r.cause || 'N/A'}. Parties: ${r.party?.join(', ') || 'N/A'}`,
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 10);
    console.log(`✅ ${top.length} triggers from CourtListener`);
    return top;
}
