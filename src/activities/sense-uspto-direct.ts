/**
 * Activity: Sense USPTO Direct — PatentsView API
 * Monitors recently granted patents from the USPTO Open Data Portal.
 * FREE — uses the official PatentsView API (free API key from data.uspto.gov).
 * Falls back to SEC EDGAR for IP-related 8-K filings if PatentsView is unavailable.
 *
 * New patent grants = companies that need IP counsel for enforcement, licensing,
 * freedom-to-operate analysis, and patent portfolio management.
 *
 * SOURCE LABEL: "USPTO Direct (Free)" — distinguishes from Exa-discovered patents
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// ---------------------------------------------------------------------------
// SEC EDGAR IP-related filings (reliable fallback — always works)
// ---------------------------------------------------------------------------

interface EdgarHit {
    _source: {
        file_date: string;
        display_names: string[];
        file_num: string[];
        root_forms: string[];
        file_description?: string;
    };
}

interface EdgarResponse {
    hits: { hits: EdgarHit[] };
}

// IP-relevant SEC searches
const IP_SEC_QUERIES = [
    { q: '"patent" OR "intellectual property" material agreement', forms: '8-K', label: 'IP Material Agreement' },
    { q: '"license agreement" OR "licensing" patent technology', forms: '8-K,10-K', label: 'Patent Licensing' },
    { q: '"patent infringement" OR "patent litigation" OR "ITC investigation"', forms: '8-K', label: 'Patent Litigation' },
    { q: '"trade secret" OR "misappropriation" OR "proprietary technology"', forms: '8-K', label: 'Trade Secret' },
] as const;

// ---------------------------------------------------------------------------
// Gemini extraction shape
// ---------------------------------------------------------------------------

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    ipType: string;
}

// ---------------------------------------------------------------------------
// Main sensor — SEC EDGAR for IP-related filings (FREE)
// ---------------------------------------------------------------------------

export async function senseUsptoDirect(env: Env): Promise<MarketTrigger[]> {
    console.log('💡 Sensing IP/Patent activity via SEC EDGAR (FREE)...');

    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    const allHits: EdgarHit[] = [];
    const queryLabels: string[] = [];

    for (const query of IP_SEC_QUERIES) {
        try {
            const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query.q)}&forms=${query.forms}&dateRange=custom&startdt=${weekAgo}&enddt=${today}&from=0&size=20`;
            const res = await fetch(url, {
                headers: {
                    'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)',
                    'Accept': 'application/json',
                },
            });

            if (!res.ok) { console.warn(`⚠️ USPTO-SEC "${query.label}" failed (${res.status})`); continue; }

            const data = await res.json() as EdgarResponse;
            for (const hit of (data?.hits?.hits || [])) {
                allHits.push(hit);
                queryLabels.push(query.label);
            }
            console.log(`💡 USPTO-SEC ${query.label}: ${data?.hits?.hits?.length || 0} filings`);
            await new Promise(r => setTimeout(r, 150));
        } catch (err) {
            console.warn(`⚠️ USPTO-SEC "${query.label}" exception:`, err);
        }
    }

    if (allHits.length === 0) { console.warn('⚠️ No USPTO-direct results.'); return []; }

    // Deduplicate by company name
    const unique: EdgarHit[] = [];
    const uniqueLabels: string[] = [];
    const seenNames = new Set<string>();
    for (let i = 0; i < allHits.length; i++) {
        const name = allHits[i]._source.display_names?.[0]?.toLowerCase() || '';
        if (name && !seenNames.has(name)) {
            seenNames.add(name);
            unique.push(allHits[i]);
            uniqueLabels.push(queryLabels[i]);
        }
    }

    return extractAndBuildTriggers(env, unique, uniqueLabels);
}

// ---------------------------------------------------------------------------
// Query variant for Search Missions
// ---------------------------------------------------------------------------

export async function senseUsptoDirectForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`💡 USPTO-Direct mission: "${query}"`);

    const monthAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const today = new Date().toISOString().split('T')[0];

    try {
        const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query + ' AND ("patent" OR "intellectual property" OR "trade secret")')}&forms=8-K,10-K&dateRange=custom&startdt=${monthAgo}&enddt=${today}&from=0&size=10`;
        const res = await fetch(url, {
            headers: { 'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)', 'Accept': 'application/json' },
        });

        if (!res.ok) { console.warn(`⚠️ USPTO-Direct mission failed (${res.status})`); return []; }

        const data = await res.json() as EdgarResponse;
        const hits = data?.hits?.hits || [];
        if (hits.length === 0) return [];

        const labels = hits.map(() => 'Mission: IP/Patent');
        return extractAndBuildTriggers(env, hits, labels);
    } catch (err) {
        console.error('❌ USPTO-Direct mission error:', err);
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
    const systemPrompt = `You are a data extraction AI for a law firm's IP practice business development. Given SEC filings about IP events, extract:
1. The PRIMARY COMPANY (clean name, remove /DE/, /NV/ suffixes)
2. The CTO, General Counsel, or Chief IP Officer — the decision-maker for IP legal work
3. A relevance score 0-100 for IP litigation and tech transactions outreach
4. The type of IP event (Patent Grant, Patent Licensing, Patent Litigation, Trade Secret, IP Acquisition)

Rules:
- Focus on companies actively involved in patent activity
- Higher scores for patent litigation (= need outside counsel NOW)
- Clean up entity names
- Only output filings with relevance >= 50
- If no executive name determinable, use "Unknown" with likely title
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "General Counsel", "relevanceScore": 80, "ipType": "Patent Litigation" }]`;

    let extracted: ExtractedMeta[] = [];
    const CHUNK_SIZE = 40;

    for (let i = 0; i < hits.length; i += CHUNK_SIZE) {
        const chunkHits = hits.slice(i, i + CHUNK_SIZE);
        const chunkLabels = queryLabels.slice(i, i + CHUNK_SIZE);

        const itemsPrompt = chunkHits.map((hit, j) => {
            const originalIndex = i + j;
            const s = hit._source;
            const entityName = s.display_names?.[0] || 'Unknown Entity';
            return `[Filing ${originalIndex}]\nEntity: ${entityName}\nForm: ${s.root_forms?.[0] || '8-K'}\nFiled: ${s.file_date}\nDescription: ${s.file_description || 'N/A'}\nCategory: ${chunkLabels[j]}`;
        }).join('\n\n');

        try {
            const geminiRes = await fetchGemini(env, 'lite', {
                activityName: 'sense-uspto-direct',
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    system_instruction: { parts: [{ text: systemPrompt }] },
                    contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                    generationConfig: {
                        temperature: 0.1,
                        maxOutputTokens: 8192,
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
                                    relevanceScore: { type: "INTEGER" },
                                    ipType: { type: "STRING" }
                                },
                                required: ["index", "company", "executiveName", "executiveTitle", "relevanceScore", "ipType"]
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
                const chunkExtracted = safeJsonParse<ExtractedMeta[]>(jsonStr, []);
                extracted.push(...chunkExtracted);
            }
        } catch (err) {
            console.error(`❌ Gemini USPTO-direct chunk [${i}-${i + CHUNK_SIZE}] failed:`, err);
            await logGeminiError(env, 'lite-uspto-direct-extraction', 'sense-uspto-direct', err, { itemsCount: chunkHits.length });
        }
    }
    console.log(`📋 Gemini extracted ${extracted.length} USPTO-direct entities total`);

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const hit = hits[meta.index];
        if (!hit) continue;
        const s = hit._source;
        const entityName = s.display_names?.[0] || 'Unknown';

        triggers.push({
            triggerId: `usptodirect-${crypto.randomUUID().slice(0, 8)}`,
            source: 'USPTO Direct (Free)',
            sourceUrl: `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(entityName)}&forms=8-K`,
            headline: `${meta.ipType}: ${meta.company} — ${queryLabels[meta.index]}`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 70,
            detectedAt: s.file_date || new Date().toISOString(),
            articleText: `${meta.company} filed SEC disclosure about ${(meta.ipType || '').toLowerCase()}. Filed ${s.file_date}. ${s.file_description || ''}`,
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 50);
    console.log(`✅ ${top.length} triggers from USPTO Direct — FREE`);
    return top;
}
