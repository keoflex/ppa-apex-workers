/**
 * Activity: Sense SEC EDGAR Filings
 * Scans SEC EDGAR full-text search for material events relevant to PPA+ outreach.
 * FREE — no API key needed, just a User-Agent header.
 * Rate limit: 10 req/sec (SEC guidelines).
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';

// ---------------------------------------------------------------------------
// SEC EDGAR search API response shape
// ---------------------------------------------------------------------------

interface EdgarHit {
    _id: string;
    _score: number;
    _source: {
        file_date: string;
        display_names: string[];
        file_num: string[];
        root_forms: string[];
        file_description?: string;
        period_ending?: string;
        biz_states?: string[];
    };
}

interface EdgarSearchResponse {
    hits: {
        hits: EdgarHit[];
        total: { value: number };
    };
}

// ---------------------------------------------------------------------------
// Legal-relevant queries — form types map to PPA practice areas
// ---------------------------------------------------------------------------

const SEC_QUERIES = [
    { q: '"merger" OR "acquisition" OR "definitive agreement"', forms: '8-K', label: 'M&A / Material Events' },
    { q: '"bankruptcy" OR "chapter 11" OR "restructuring"', forms: '8-K', label: 'Restructuring' },
    { q: '"regulatory approval" OR "consent order" OR "settlement"', forms: '8-K', label: 'Regulatory' },
    { q: '"initial public offering" OR "registration statement"', forms: 'S-1', label: 'IPO' },
    { q: '"activist" OR "beneficial ownership"', forms: 'SC 13D', label: 'Activist Investor' },
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

export async function senseSecFilings(env: Env): Promise<MarketTrigger[]> {
    console.log('📜 Sensing SEC EDGAR filings...');

    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    const allResults: EdgarHit[] = [];
    const queryLabels: string[] = [];

    for (const query of SEC_QUERIES) {
        try {
            const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query.q)}&forms=${query.forms}&dateRange=custom&startdt=${weekAgo}&enddt=${today}&from=0&size=5`;
            const res = await fetch(url, {
                headers: {
                    'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)',
                    'Accept': 'application/json',
                },
            });

            if (!res.ok) { console.warn(`⚠️ SEC "${query.label}" failed (${res.status})`); continue; }

            const data = await res.json() as EdgarSearchResponse;
            for (const hit of (data?.hits?.hits || [])) {
                allResults.push(hit);
                queryLabels.push(query.label);
            }
            console.log(`📜 SEC ${query.label}: ${data?.hits?.hits?.length || 0} filings`);
            await new Promise(r => setTimeout(r, 200)); // polite delay
        } catch (err) {
            console.warn(`⚠️ SEC "${query.label}" exception:`, err);
        }
    }

    if (allResults.length === 0) { console.warn('⚠️ No SEC results.'); return []; }

    return extractAndBuildTriggers(env, allResults, queryLabels);
}

// ---------------------------------------------------------------------------
// Query-specific variant for Search Missions
// ---------------------------------------------------------------------------

export async function senseSecFilingsForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`📜 SEC mission: "${query}"`);

    const today = new Date().toISOString().split('T')[0];
    const monthAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    try {
        const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query)}&dateRange=custom&startdt=${monthAgo}&enddt=${today}&from=0&size=10`;
        const res = await fetch(url, {
            headers: { 'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)', 'Accept': 'application/json' },
        });

        if (!res.ok) { console.warn(`⚠️ SEC mission failed (${res.status})`); return []; }

        const data = await res.json() as EdgarSearchResponse;
        const hits = data?.hits?.hits || [];
        if (hits.length === 0) return [];

        const labels = hits.map(() => 'Mission Search');
        return extractAndBuildTriggers(env, hits, labels);
    } catch (err) {
        console.error('❌ SEC mission error:', err);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Shared: Gemini extraction + MarketTrigger assembly
// ---------------------------------------------------------------------------

async function extractAndBuildTriggers(
    env: Env,
    hits: EdgarHit[],
    queryLabels: string[],
): Promise<MarketTrigger[]> {
    const itemsPrompt = hits.map((hit, i) => {
        const s = hit._source;
        const entityName = s.display_names?.[0] || 'Unknown Entity';
        const formType = s.root_forms?.[0] || 'Unknown';
        return `[Filing ${i}]\nEntity: ${entityName}\nForm: ${formType}\nFiled: ${s.file_date}\nDescription: ${s.file_description || 'N/A'}\nFile#: ${s.file_num?.[0] || 'N/A'}\nCategory: ${queryLabels[i]}`;
    }).join('\n\n');

    const systemPrompt = `You are a data extraction AI for a law firm's business development. Given SEC EDGAR filings, extract:
1. The PRIMARY COMPANY (clean name, e.g. "ACME CORP /DE/" → "Acme Corp")
2. The most likely EXECUTIVE involved based on filing type:
   - 8-K: CEO or General Counsel (signatory)
   - S-1: CEO or CFO
   - SC 13D: Fund manager
3. A relevance score 0-100 for how actionable this is for law firm outreach

Rules:
- If no specific name is determinable, use "Unknown" but still provide the likely title
- Clean up entity names (remove /DE/, /NV/, etc.)
- CRITICAL: DO NOT output every filing! Only output filings with a relevance score of 50 or higher. Ignore low-relevance filings entirely to keep the JSON array extremely small and fast to generate.
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO", "relevanceScore": 85 }]`;

    let extracted: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-sec-filings',
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
            console.log(`📋 Gemini extracted ${extracted.length} SEC entities`);
        }
    } catch (err) {
        console.error('❌ Gemini SEC extraction failed:', err);
        await logGeminiError(env, 'lite-sec-extraction', 'sense-sec-filings', err, { itemsCount: hits.length });
        return [];
    }

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const hit = hits[meta.index];
        if (!hit) continue;
        const s = hit._source;
        const entityName = s.display_names?.[0] || 'Unknown';
        const formType = s.root_forms?.[0] || '8-K';
        const fileNum = s.file_num?.[0] || '';

        triggers.push({
            triggerId: `sec-${crypto.randomUUID().slice(0, 8)}`,
            source: 'SEC EDGAR',
            sourceUrl: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=${encodeURIComponent(fileNum)}&type=&dateb=&owner=include&count=10`,
            headline: `${formType}: ${s.file_description || entityName} — ${queryLabels[meta.index]}`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 70,
            detectedAt: s.file_date || new Date().toISOString(),
            articleText: `SEC ${formType} filing by ${entityName}. Filed ${s.file_date}. ${s.file_description || ''}`,
            agentId: 0, // external source, not a deployed agent
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 10);
    console.log(`✅ ${top.length} triggers from SEC EDGAR`);
    return top;
}
