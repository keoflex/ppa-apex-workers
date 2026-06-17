/**
 * Activity: Sense SEC EDGAR Bulk — Company-level 8-K Monitoring
 * Proactively monitors recently filed 8-Ks across all public companies.
 * FREE — no API key needed, just a User-Agent header.
 * Rate limit: 10 req/sec (SEC guidelines).
 *
 * Unlike sense-sec-filings.ts (keyword search), this sensor uses the SEC's
 * full-text search for high-volume bulk discovery of material events.
 *
 * SOURCE LABEL: "SEC EDGAR (Bulk)" — distinguishes from Exa-discovered leads
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// ---------------------------------------------------------------------------
// SEC EDGAR full-text search response shape
// ---------------------------------------------------------------------------

interface EdgarFiling {
    _id: string;
    _source: {
        file_date: string;
        display_names: string[];
        file_num: string[];
        root_forms: string[];
        file_description?: string;
        period_ending?: string;
        biz_locations?: string[];
    };
}

interface EdgarSearchResponse {
    hits: {
        hits: EdgarFiling[];
        total: { value: number };
    };
}

// ---------------------------------------------------------------------------
// Bulk queries — high-volume material event signals
// ---------------------------------------------------------------------------

const BULK_QUERIES = [
    // ── Core 8-K Event Categories (highest value) ──
    { q: '"entry into a material definitive agreement"', forms: '8-K', label: 'M&A / Material Agreement' },
    { q: '"completion of acquisition" OR "consummation of merger"', forms: '8-K', label: 'Closed Deal' },
    { q: '"departure of directors" OR "appointment of" officer', forms: '8-K', label: 'Leadership Change' },
    { q: '"notice of delisting" OR "failure to satisfy" listing', forms: '8-K', label: 'Compliance/Delisting' },
    { q: '"change in control" OR "going private"', forms: '8-K', label: 'Control Change' },
    { q: '"voluntary petition" OR "chapter 11"', forms: '8-K', label: 'Bankruptcy Filing' },
    // ── Expanded 8-K Triggers ──
    { q: '"definitive merger agreement" OR "asset purchase agreement"', forms: '8-K', label: 'Merger/Asset Purchase' },
    { q: '"securities purchase agreement" OR "private placement"', forms: '8-K', label: 'Capital Raise' },
    { q: '"workforce reduction" OR "restructuring plan"', forms: '8-K', label: 'Restructuring' },
    { q: '"restatement" OR "material weakness" internal controls', forms: '8-K', label: 'Compliance/Restatement' },
    { q: '"cybersecurity incident" OR "data breach"', forms: '8-K', label: 'Cyber Incident' },
    { q: '"joint venture" OR "strategic alliance" OR "partnership agreement"', forms: '8-K', label: 'Strategic Partnership' },
    { q: '"spin-off" OR "divestiture" OR "sale of subsidiary"', forms: '8-K', label: 'Divestiture/Spin-off' },
    { q: '"consent decree" OR "settlement agreement" government', forms: '8-K', label: 'Government Settlement' },
    // ── IPO & Capital Markets ──
    { q: '"initial public offering" OR "proposed maximum aggregate"', forms: 'S-1,S-1/A', label: 'IPO Filing' },
    { q: '"shelf registration" OR "securities to be offered"', forms: 'S-3,S-3/A', label: 'Shelf Registration' },
    // ── Proxy & Governance ──
    { q: '"merger proposal" OR "special meeting" OR "shareholder vote"', forms: 'DEF 14A,DEFA14A', label: 'Proxy/M&A Vote' },
    // ── Large Acquisitions (Hart-Scott-Rodino) ──
    { q: '"Hart-Scott-Rodino" OR "HSR" antitrust', forms: '8-K,SC TO-T', label: 'HSR/Antitrust Filing' },
    // ── Real Estate / REIT Activity ──
    { q: '"property acquisition" OR "real estate portfolio"', forms: '8-K', label: 'Real Estate Deal' },
    // ── Executive Compensation ──
    { q: '"employment agreement" OR "severance" OR "golden parachute"', forms: '8-K', label: 'Executive Compensation' },
    // ── Financing & Transactions ──
    { q: '"credit agreement" OR "term loan" OR "revolving credit"', forms: '8-K', label: 'Debt Financing' },
    { q: '"license agreement" OR "collaboration agreement" OR "development agreement"', forms: '8-K', label: 'IP/License Agreement' },
    { q: '"asset sale" OR "asset purchase" OR "purchase and sale agreement"', forms: '8-K', label: 'Asset Deal' },
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
    practiceAreas: string[];
}

// ---------------------------------------------------------------------------
// Main sensor — HIGH VOLUME mode for independent cron sweep
// ---------------------------------------------------------------------------

export async function senseSecBulk(env: Env): Promise<MarketTrigger[]> {
    console.log('🏛️ Sensing SEC EDGAR bulk filings — HIGH VOLUME (FREE)...');

    const today = new Date().toISOString().split('T')[0];
    const lookbackDays = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    const allFilings: EdgarFiling[] = [];
    const queryLabels: string[] = [];

    for (const query of BULK_QUERIES) {
        try {
            const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query.q)}&forms=${query.forms}&dateRange=custom&startdt=${lookbackDays}&enddt=${today}&from=0&size=80`;
            const res = await fetch(url, {
                headers: {
                    'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)',
                    'Accept': 'application/json',
                },
            });

            if (!res.ok) { console.warn(`⚠️ SEC Bulk "${query.label}" failed (${res.status})`); continue; }

            const data = await res.json() as EdgarSearchResponse;
            for (const hit of (data?.hits?.hits || [])) {
                allFilings.push(hit);
                queryLabels.push(query.label);
            }
            console.log(`🏛️ SEC Bulk ${query.label}: ${data?.hits?.hits?.length || 0} filings`);
            await new Promise(r => setTimeout(r, 120)); // polite rate limit (10 req/sec)
        } catch (err) {
            console.warn(`⚠️ SEC Bulk "${query.label}" exception:`, err);
        }
    }

    if (allFilings.length === 0) { console.warn('⚠️ No SEC bulk results.'); return []; }
    console.log(`🏛️ Total raw SEC filings: ${allFilings.length}`);

    // Deduplicate by company name
    const unique: EdgarFiling[] = [];
    const uniqueLabels: string[] = [];
    const seenNames = new Set<string>();
    for (let i = 0; i < allFilings.length; i++) {
        const name = allFilings[i]._source.display_names?.[0]?.toLowerCase() || '';
        if (name && !seenNames.has(name)) {
            seenNames.add(name);
            unique.push(allFilings[i]);
            uniqueLabels.push(queryLabels[i]);
        }
    }

    return extractAndBuildTriggers(env, unique, uniqueLabels);
}

// ---------------------------------------------------------------------------
// Query variant for Search Missions
// ---------------------------------------------------------------------------

export async function senseSecBulkForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`🏛️ SEC Bulk mission: "${query}"`);

    const today = new Date().toISOString().split('T')[0];
    const monthAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    try {
        const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query)}&dateRange=custom&startdt=${monthAgo}&enddt=${today}&from=0&size=10`;
        const res = await fetch(url, {
            headers: { 'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)', 'Accept': 'application/json' },
        });

        if (!res.ok) { console.warn(`⚠️ SEC Bulk mission failed (${res.status})`); return []; }

        const data = await res.json() as EdgarSearchResponse;
        const hits = data?.hits?.hits || [];
        if (hits.length === 0) return [];

        const labels = hits.map(() => 'Mission Search');
        return extractAndBuildTriggers(env, hits, labels);
    } catch (err) {
        console.error('❌ SEC Bulk mission error:', err);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Shared: Gemini extraction + MarketTrigger assembly with practice area AI
// ---------------------------------------------------------------------------

async function extractAndBuildTriggers(
    env: Env,
    hits: EdgarFiling[],
    queryLabels: string[],
): Promise<MarketTrigger[]> {
    const systemPrompt = `You are a data extraction AI for a law firm's business development. Given SEC EDGAR filings, extract:
1. The PRIMARY COMPANY (clean name, e.g. "ACME CORP /DE/" → "Acme Corp")
2. The most likely EXECUTIVE involved based on filing type:
   - 8-K material agreements: CEO or General Counsel
   - Leadership changes: the incoming/outgoing officer
   - Bankruptcy: CEO or CFO
   - Delisting: CFO or General Counsel
3. A relevance score 0-100 for how actionable this is for law firm outreach
4. Practice areas that align (choose from: "Corporate/M&A", "IP Litigation", "Bankruptcy/Restructuring", "Employment", "Securities", "Real Estate", "Financial Services", "Regulatory/Compliance", "Tax")

Rules:
- Clean up entity names (remove /DE/, /NV/, state suffixes)
- Only output filings with relevance >= 50
- If no specific executive name, use "Unknown" with likely title
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO", "relevanceScore": 85, "practiceAreas": ["Corporate/M&A", "Securities"] }]`;

    let extracted: ExtractedMeta[] = [];
    const CHUNK_SIZE = 40;

    for (let i = 0; i < hits.length; i += CHUNK_SIZE) {
        const chunkHits = hits.slice(i, i + CHUNK_SIZE);
        const chunkLabels = queryLabels.slice(i, i + CHUNK_SIZE);

        const itemsPrompt = chunkHits.map((hit, j) => {
            const originalIndex = i + j;
            const s = hit._source;
            const entityName = s.display_names?.[0] || 'Unknown Entity';
            const formType = s.root_forms?.[0] || 'Unknown';
            return `[Filing ${originalIndex}]\nEntity: ${entityName}\nForm: ${formType}\nFiled: ${s.file_date}\nDescription: ${s.file_description || 'N/A'}\nCategory: ${chunkLabels[j]}`;
        }).join('\n\n');

        try {
            const geminiRes = await fetchGemini(env, 'lite', {
                activityName: 'sense-sec-bulk',
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
                                    practiceAreas: { type: "ARRAY", items: { type: "STRING" } }
                                },
                                required: ["index", "company", "executiveName", "executiveTitle", "relevanceScore", "practiceAreas"]
                            }
                        }
                    },
                }),
            });
            if (!geminiRes.ok) throw new Error(await geminiRes.text());

            const { text: rawText, finishReason, wasEmpty } = await safeGeminiResponseParse(geminiRes);
            if (wasEmpty) {
                console.warn(`⚠️ Gemini returned empty body for SEC bulk chunk [${i}-${i + CHUNK_SIZE}]`);
            } else if (rawText) {
                let jsonStr = rawText;
                const match = rawText.match(/\[[\s\S]*\]/);
                if (match) jsonStr = match[0];
                const chunkExtracted = safeJsonParse<ExtractedMeta[]>(jsonStr, []);
                extracted.push(...chunkExtracted);
            }
        } catch (err) {
            console.error(`❌ Gemini SEC bulk chunk [${i}-${i + CHUNK_SIZE}] failed:`, err);
            await logGeminiError(env, 'lite-sec-bulk-extraction', 'sense-sec-bulk', err, { itemsCount: chunkHits.length });
        }
    }
    console.log(`📋 Gemini extracted ${extracted.length} SEC bulk entities total`);

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const hit = hits[meta.index];
        if (!hit) continue;
        const s = hit._source;
        const entityName = s.display_names?.[0] || 'Unknown';
        const formType = s.root_forms?.[0] || '8-K';
        const fileNum = s.file_num?.[0] || '';

        triggers.push({
            triggerId: `secbulk-${crypto.randomUUID().slice(0, 8)}`,
            source: 'SEC EDGAR (Bulk)',
            sourceUrl: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum=${encodeURIComponent(fileNum)}&type=&dateb=&owner=include&count=10`,
            headline: `${formType}: ${s.file_description || entityName} — ${queryLabels[meta.index]}`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 70,
            detectedAt: s.file_date || new Date().toISOString(),
            articleText: `SEC ${formType} filing by ${entityName}. Filed ${s.file_date}. ${s.file_description || ''}. Practice areas: ${meta.practiceAreas?.join(', ') || 'General'}`,
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 250);
    console.log(`✅ ${top.length} triggers from SEC EDGAR (Bulk) — FREE — HIGH VOLUME`);
    return top;
}
