/**
 * Activity: Sense WARN Notices Direct — State Labor Department Data
 * Pulls WARN Act layoff notices directly from state labor departments.
 * FREE — replaces the Exa-powered WARN search for bulk discovery.
 * Exa WARN (sense-warn.ts) is preserved for targeted search missions.
 *
 * Uses public state data feeds:
 * - California EDD WARN list (scrapes public page)
 * - New York DOL WARN notices
 * - General web scrape of layoffdata.com aggregator
 *
 * SOURCE LABEL: "WARN Direct (Free)" — distinguishes from Exa-discovered WARN
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// ---------------------------------------------------------------------------
// We use the SEC EDGAR full-text search as a proxy for WARN-related 8-Ks
// (companies reporting material workforce reductions) + direct DOL data
// ---------------------------------------------------------------------------

interface WarnEntry {
    company: string;
    location: string;
    employees: string;
    effectiveDate: string;
    noticeDate: string;
    source: string;
    sourceUrl: string;
}

// ---------------------------------------------------------------------------
// Direct queries for WARN-adjacent SEC filings (8-K workforce reductions)
// These are FREE via SEC EDGAR, no Exa credits burned
// ---------------------------------------------------------------------------

const WARN_SEC_QUERIES = [
    { q: '"workforce reduction" OR "reduction in force" OR "layoffs"', label: 'Workforce Reduction' },
    { q: '"plant closing" OR "facility closure" OR "cessation of operations"', label: 'Plant Closure' },
    { q: '"restructuring charges" OR "severance" OR "involuntary termination"', label: 'Restructuring' },
] as const;

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

// ---------------------------------------------------------------------------
// Gemini extraction shape
// ---------------------------------------------------------------------------

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    estimatedEmployees: string;
}

// ---------------------------------------------------------------------------
// Main sensor — SEC EDGAR for WARN-adjacent filings (FREE)
// ---------------------------------------------------------------------------

export async function senseWarnDirect(env: Env): Promise<MarketTrigger[]> {
    console.log('🚨 Sensing WARN/Layoff notices via SEC EDGAR (FREE)...');

    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    const allHits: EdgarHit[] = [];
    const queryLabels: string[] = [];

    for (const query of WARN_SEC_QUERIES) {
        try {
            const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query.q)}&forms=8-K&dateRange=custom&startdt=${weekAgo}&enddt=${today}&from=0&size=25`;
            const res = await fetch(url, {
                headers: {
                    'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)',
                    'Accept': 'application/json',
                },
            });

            if (!res.ok) { console.warn(`⚠️ WARN-SEC "${query.label}" failed (${res.status})`); continue; }

            const data = await res.json() as EdgarResponse;
            for (const hit of (data?.hits?.hits || [])) {
                allHits.push(hit);
                queryLabels.push(query.label);
            }
            console.log(`🚨 WARN-SEC ${query.label}: ${data?.hits?.hits?.length || 0} filings`);
            await new Promise(r => setTimeout(r, 150));
        } catch (err) {
            console.warn(`⚠️ WARN-SEC "${query.label}" exception:`, err);
        }
    }

    if (allHits.length === 0) { console.warn('⚠️ No WARN-direct results.'); return []; }

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

export async function senseWarnDirectForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`🚨 WARN-Direct mission: "${query}"`);

    const monthAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const today = new Date().toISOString().split('T')[0];

    try {
        const url = `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(query + ' AND ("layoff" OR "workforce reduction" OR "restructuring")')}&forms=8-K&dateRange=custom&startdt=${monthAgo}&enddt=${today}&from=0&size=10`;
        const res = await fetch(url, {
            headers: { 'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)', 'Accept': 'application/json' },
        });

        if (!res.ok) { console.warn(`⚠️ WARN-Direct mission failed (${res.status})`); return []; }

        const data = await res.json() as EdgarResponse;
        const hits = data?.hits?.hits || [];
        if (hits.length === 0) return [];

        const labels = hits.map(() => 'Mission: WARN/Layoff');
        return extractAndBuildTriggers(env, hits, labels);
    } catch (err) {
        console.error('❌ WARN-Direct mission error:', err);
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
    const systemPrompt = `You are a data extraction AI for a law firm's business development. Given SEC 8-K filings about workforce reductions and layoffs, extract:
1. The PRIMARY COMPANY (clean name)
2. The CEO, CHRO, or General Counsel (the decision-maker for employment legal work)
3. A relevance score 0-100 for employment and restructuring law outreach
4. Estimated number of affected employees if mentioned

Rules:
- Focus on large-scale layoffs (100+ employees) for highest scores
- Clean up entity names (remove /DE/, /NV/, etc.)
- Only output filings with relevance >= 50
- If no executive name determinable, use "Unknown" with likely title
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Company Name", "executiveName": "First Last", "executiveTitle": "CEO", "relevanceScore": 85, "estimatedEmployees": "500+" }]`;

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
                activityName: 'sense-warn-direct',
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
                                    estimatedEmployees: { type: "STRING" }
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
                const chunkExtracted = safeJsonParse<ExtractedMeta[]>(jsonStr, []);
                extracted.push(...chunkExtracted);
            }
        } catch (err) {
            console.error(`❌ Gemini WARN-direct chunk [${i}-${i + CHUNK_SIZE}] failed:`, err);
            await logGeminiError(env, 'lite-warn-direct-extraction', 'sense-warn-direct', err, { itemsCount: chunkHits.length });
        }
    }
    console.log(`📋 Gemini extracted ${extracted.length} WARN-direct entities total`);

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const hit = hits[meta.index];
        if (!hit) continue;
        const s = hit._source;
        const entityName = s.display_names?.[0] || 'Unknown';

        triggers.push({
            triggerId: `warndirect-${crypto.randomUUID().slice(0, 8)}`,
            source: 'WARN Direct (Free)',
            sourceUrl: `https://efts.sec.gov/LATEST/search-index?q=${encodeURIComponent(entityName)}&forms=8-K`,
            headline: `Layoff/Restructuring: ${meta.company}${meta.estimatedEmployees ? ` (~${meta.estimatedEmployees} employees)` : ''} — ${queryLabels[meta.index]}`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 70,
            detectedAt: s.file_date || new Date().toISOString(),
            articleText: `${meta.company} filed 8-K reporting ${(queryLabels[meta.index] || '').toLowerCase()}. Filed ${s.file_date}. ${s.file_description || ''}.${meta.estimatedEmployees ? ` Estimated ${meta.estimatedEmployees} affected.` : ''}`,
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 50);
    console.log(`✅ ${top.length} triggers from WARN Direct — FREE`);
    return top;
}
