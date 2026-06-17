/**
 * Activity: Sense Federal Register — Regulatory Watch
 * Monitors proposed rules and final rules from federal agencies.
 * FREE — no API key needed, JSON responses, no authentication.
 *
 * Every significant regulation = companies needing legal counsel.
 * FDA, EPA, FTC, DOL, SEC rules are gold for practice group targeting.
 *
 * SOURCE LABEL: "Federal Register" — clearly free, government data
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// ---------------------------------------------------------------------------
// Federal Register API response shape
// ---------------------------------------------------------------------------

interface FedRegDocument {
    document_number: string;
    title: string;
    type: string;           // "Rule", "Proposed Rule", "Notice"
    abstract?: string;
    agencies: Array<{ name: string; id: number }>;
    publication_date: string;
    effective_on?: string;
    comment_end_date?: string;
    html_url: string;
    pdf_url?: string;
    significant?: boolean;
    action?: string;
    dates?: string;
    regulation_id_numbers?: Array<{ regulation_id_number: string }>;
}

interface FedRegSearchResponse {
    count: number;
    results: FedRegDocument[];
}

// ---------------------------------------------------------------------------
// Agency filters — agencies that generate work for law firms
// ---------------------------------------------------------------------------

const AGENCY_WATCHES = [
    // ── Tier 1 — Highest legal-services demand ──
    { agencies: ['securities-and-exchange-commission'], label: 'SEC Regulation' },
    { agencies: ['environmental-protection-agency'], label: 'EPA Regulation' },
    { agencies: ['food-and-drug-administration'], label: 'FDA Regulation' },
    { agencies: ['federal-trade-commission'], label: 'FTC Regulation' },
    { agencies: ['department-of-labor'], label: 'DOL Regulation' },
    { agencies: ['department-of-justice'], label: 'DOJ Action' },
    { agencies: ['consumer-financial-protection-bureau'], label: 'CFPB Regulation' },
    { agencies: ['federal-communications-commission'], label: 'FCC Regulation' },
    { agencies: ['internal-revenue-service'], label: 'IRS Regulation' },
    // ── Tier 2 — High-value regulatory activity ──
    { agencies: ['department-of-energy'], label: 'DOE Regulation' },
    { agencies: ['department-of-health-and-human-services'], label: 'HHS Regulation' },
    { agencies: ['department-of-homeland-security'], label: 'DHS Regulation' },
    { agencies: ['department-of-defense'], label: 'DoD Procurement' },
    { agencies: ['patent-and-trademark-office'], label: 'USPTO Rule' },
    { agencies: ['federal-reserve-system'], label: 'Fed Reserve Rule' },
] as const;

// ---------------------------------------------------------------------------
// Gemini extraction shape
// ---------------------------------------------------------------------------

interface ExtractedMeta {
    index: number;
    company: string;
    isIndustry?: boolean;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    practiceAreas: string[];
    affectedIndustries: string[];
}

// ---------------------------------------------------------------------------
// Main sensor — runs on cron / pipeline dispatch
// ---------------------------------------------------------------------------

export async function senseFederalRegister(env: Env): Promise<MarketTrigger[]> {
    console.log('📋 Sensing Federal Register regulations (FREE)...');

    const lookbackDate = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const allDocs: FedRegDocument[] = [];
    const queryLabels: string[] = [];

    for (const watch of AGENCY_WATCHES) {
        try {
            // Federal Register API — completely free, no auth needed
            const agencySlug = watch.agencies[0];
            const url = `https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=${agencySlug}&conditions[type][]=RULE&conditions[type][]=PRORULE&conditions[type][]=NOTICE&conditions[publication_date][gte]=${lookbackDate}&per_page=60&order=newest`;

            const res = await fetch(url, {
                headers: { 'Accept': 'application/json' },
            });

            if (!res.ok) { console.warn(`⚠️ FedReg "${watch.label}" failed (${res.status})`); continue; }

            const data = await res.json() as FedRegSearchResponse;
            for (const doc of (data?.results || [])) {
                allDocs.push(doc);
                queryLabels.push(watch.label);
            }
            console.log(`📋 FedReg ${watch.label}: ${data?.results?.length || 0} docs`);
            await new Promise(r => setTimeout(r, 200)); // polite delay
        } catch (err) {
            console.warn(`⚠️ FedReg "${watch.label}" exception:`, err);
        }
    }

    if (allDocs.length === 0) { console.warn('⚠️ No Federal Register results.'); return []; }

    return extractAndBuildTriggers(env, allDocs, queryLabels);
}

// ---------------------------------------------------------------------------
// Query variant for Search Missions
// ---------------------------------------------------------------------------

export async function senseFederalRegisterForQuery(env: Env, query: string): Promise<MarketTrigger[]> {
    console.log(`📋 FedReg mission: "${query}"`);

    const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    try {
        const url = `https://www.federalregister.gov/api/v1/documents.json?conditions[term]=${encodeURIComponent(query)}&conditions[type][]=RULE&conditions[type][]=PRORULE&conditions[type][]=NOTICE&conditions[publication_date][gte]=${thirtyDaysAgo}&per_page=10&order=newest`;
        const res = await fetch(url, { headers: { 'Accept': 'application/json' } });

        if (!res.ok) { console.warn(`⚠️ FedReg mission failed (${res.status})`); return []; }

        const data = await res.json() as FedRegSearchResponse;
        if (!data?.results?.length) return [];

        const labels = data.results.map(d => `Mission: ${d.type}`);
        return extractAndBuildTriggers(env, data.results, labels);
    } catch (err) {
        console.error('❌ FedReg mission error:', err);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Shared: Gemini extraction — identifies affected companies + practice areas
// ---------------------------------------------------------------------------

async function extractAndBuildTriggers(
    env: Env,
    docs: FedRegDocument[],
    queryLabels: string[],
): Promise<MarketTrigger[]> {
    const systemPrompt = `You are a strategic intelligence AI for a law firm's business development. Given federal regulations, extract:
1. The PRIMARY affected ENTITY. CRITICAL: only output a specific company name if that company is LITERALLY NAMED in the regulation's title or abstract. Do NOT guess, infer, or invent a company that "would be affected" — if no company is explicitly named, output the affected INDUSTRY instead (e.g. "Banking", "Pharmaceuticals") and set "isIndustry": true.
2. The most likely DECISION-MAKER title at affected organizations (General Counsel, Chief Compliance Officer). Use executiveName "Unknown" unless a person is literally named in the text.
3. A relevance score 0-100 for how actionable this is for law firm outreach
4. Practice areas that align (choose from: "Corporate/M&A", "IP Litigation", "Bankruptcy/Restructuring", "Employment", "Securities", "Real Estate", "Financial Services", "Regulatory/Compliance", "Tax", "Healthcare", "Environmental")
5. Industries affected (e.g. "Banking", "Pharmaceuticals", "Technology")

Rules:
- NEVER fabricate a company-to-regulation association. A named company must appear verbatim in the title/abstract.
- When no company is named, "company" must be the industry name and "isIndustry" must be true.
- Only output regulations with relevance >= 50
- Respond with ONLY a JSON array:
[{ "index": 0, "company": "Banking", "isIndustry": true, "executiveName": "Unknown", "executiveTitle": "General Counsel", "relevanceScore": 80, "practiceAreas": ["Regulatory/Compliance"], "affectedIndustries": ["Banking"] }]`;

    let extracted: ExtractedMeta[] = [];
    const CHUNK_SIZE = 30;

    for (let i = 0; i < docs.length; i += CHUNK_SIZE) {
        const chunkDocs = docs.slice(i, i + CHUNK_SIZE);
        const chunkLabels = queryLabels.slice(i, i + CHUNK_SIZE);

        const itemsPrompt = chunkDocs.map((d, j) => {
            const originalIndex = i + j;
            const agencies = d.agencies?.map(a => a.name).join(', ') || 'Unknown Agency';
            return `[Regulation ${originalIndex}]\nTitle: ${d.title}\nAgency: ${agencies}\nType: ${d.type}\nPublished: ${d.publication_date}\nEffective: ${d.effective_on || 'N/A'}\nComment Deadline: ${d.comment_end_date || 'N/A'}\nAbstract: ${(d.abstract || 'N/A').slice(0, 800)}\nCategory: ${chunkLabels[j]}`;
        }).join('\n\n');

        try {
            const geminiRes = await fetchGemini(env, 'lite', {
                activityName: 'sense-federal-register',
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    system_instruction: { parts: [{ text: systemPrompt }] },
                    contents: [{ role: 'user', parts: [{ text: itemsPrompt }] }],
                    generationConfig: {
                        temperature: 0.2,
                        maxOutputTokens: 8192,
                        responseMimeType: 'application/json',
                        responseSchema: {
                            type: "ARRAY",
                            items: {
                                type: "OBJECT",
                                properties: {
                                    index: { type: "INTEGER" },
                                    company: { type: "STRING" },
                                    isIndustry: { type: "BOOLEAN" },
                                    executiveName: { type: "STRING" },
                                    executiveTitle: { type: "STRING" },
                                    relevanceScore: { type: "INTEGER" },
                                    practiceAreas: { type: "ARRAY", items: { type: "STRING" } },
                                    affectedIndustries: { type: "ARRAY", items: { type: "STRING" } }
                                },
                                required: ["index", "company", "executiveName", "executiveTitle", "relevanceScore", "practiceAreas", "affectedIndustries"]
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
            console.error(`❌ Gemini FedReg chunk [${i}-${i + CHUNK_SIZE}] failed:`, err);
            await logGeminiError(env, 'lite-fedreg-extraction', 'sense-federal-register', err, { itemsCount: chunkDocs.length });
        }
    }
    console.log(`📋 Gemini extracted ${extracted.length} regulatory entities total`);

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const d = docs[meta.index];
        if (!d) continue;
        // Industry-only extractions (no company literally named in the regulation) are macro/briefing
        // context, not a contactable lead. Skip them so we never enrich/email "Banking" as a company.
        if (meta.isIndustry) {
            console.log(`ℹ️ FedReg: skipping industry-level signal "${meta.company}" (no named company) — not a strike lead.`);
            continue;
        }
        const agencyName = d.agencies?.[0]?.name || 'Federal Agency';

        triggers.push({
            triggerId: `fedreg-${crypto.randomUUID().slice(0, 8)}`,
            source: 'Federal Register',
            sourceUrl: d.html_url || '',
            headline: `${d.type}: ${d.title.slice(0, 120)} — ${agencyName}`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore ?? 65,
            detectedAt: d.publication_date || new Date().toISOString(),
            articleText: `${d.type} by ${agencyName}. ${d.abstract?.slice(0, 400) || d.title}. Practice areas: ${meta.practiceAreas?.join(', ')}. Industries: ${meta.affectedIndustries?.join(', ')}`,
            agentId: 0,
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 200);
    console.log(`✅ ${top.length} triggers from Federal Register — FREE — HIGH VOLUME`);
    return top;
}
