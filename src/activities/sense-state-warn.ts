/**
 * Activity: Sense State WARN notices (Free, 50-State Network)
 * Pulls mass layoff notices from all 50 states using the unified Stanford Big Local News dataset,
 * with direct HTML scrapers for CA, TX, and NY as high-frequency fallbacks.
 * Uses Gemini LITE for extraction and campaign agent routing.
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// 50-State WARN dataset URL from Stanford Big Local News
const UNIFIED_WARN_CSV_URL = 'https://raw.githubusercontent.com/biglocalnews/warn-github-flow/transformer/data/warn-transformer/processed/consolidated.csv';

interface UnifiedWarnRow {
    postal_code: string;
    company: string;
    location: string;
    notice_date: string;
    effective_date: string;
    jobs: string;
    is_temporary: string;
    is_closure: string;
    is_amendment: string;
}

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    estimatedEmployees: string;
    agentId: number;
    rationale: string;
}

// Simple linear quote-aware CSV line parser
function parseCSVLine(line: string): string[] {
    const result: string[] = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            result.push(current.trim());
            current = '';
        } else {
            current += char;
        }
    }
    result.push(current.trim());
    return result;
}

// Safe page fetcher with timeout
async function fetchStatePage(url: string): Promise<string> {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), 6000);
    try {
        const res = await fetch(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 PPA-APEX/1.0 (apex@posinelli.com)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            },
            signal: controller.signal
        });
        clearTimeout(id);
        if (!res.ok) return '';
        return await res.text();
    } catch (e) {
        clearTimeout(id);
        return '';
    }
}

function cleanHTML(html: string): string {
    return html
        .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
        .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
        .replace(/<[^>]+>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 10000); // keep context length modest
}

export async function senseStateWarn(env: Env): Promise<MarketTrigger[]> {
    console.log('🚨 Starting 50-State Free WARN Sensor Sweep...');

    const rawLayoffs: Array<{
        company: string;
        state: string;
        location: string;
        noticeDate: string;
        effectiveDate: string;
        jobs: string;
        source: string;
        detailsUrl: string;
    }> = [];

    // 1. Direct State Crawlers (CA, TX, NY) - fast, high-frequency signals
    console.log('🌐 Polling direct high-yield state pages (CA, TX, NY)...');
    
    // CA EDD
    try {
        const caHtml = await fetchStatePage('https://edd.ca.gov/en/Jobs_and_Training/Layoff_Services_WARN');
        if (caHtml) {
            const cleanText = cleanHTML(caHtml);
            rawLayoffs.push({
                company: 'California DOL / EDD Page Extract',
                state: 'CA',
                location: 'Multiple Locations',
                noticeDate: new Date().toISOString().split('T')[0],
                effectiveDate: '',
                jobs: 'unknown',
                source: 'California EDD Page Scrape',
                detailsUrl: 'https://edd.ca.gov/en/Jobs_and_Training/Layoff_Services_WARN',
                // Keep raw snippet in the company field so Gemini can parse it below
                detailsTextSnippet: cleanText.slice(0, 3000)
            } as any);
            console.log('✅ California EDD page fetched successfully');
        }
    } catch (e) {
        console.warn('⚠️ California EDD direct fetch failed, falling back to unified dataset:', e);
    }

    // TX TWC
    try {
        const txHtml = await fetchStatePage('https://www.twc.texas.gov/businesses/worker-adjustment-and-retraining-notification-warn-notices');
        if (txHtml) {
            const cleanText = cleanHTML(txHtml);
            rawLayoffs.push({
                company: 'Texas TWC Page Extract',
                state: 'TX',
                location: 'Multiple Locations',
                noticeDate: new Date().toISOString().split('T')[0],
                effectiveDate: '',
                jobs: 'unknown',
                source: 'Texas TWC Page Scrape',
                detailsUrl: 'https://www.twc.texas.gov/businesses/worker-adjustment-and-retraining-notification-warn-notices',
                detailsTextSnippet: cleanText.slice(0, 3000)
            } as any);
            console.log('✅ Texas TWC page fetched successfully');
        }
    } catch (e) {
        console.warn('⚠️ Texas TWC direct fetch failed, falling back to unified dataset:', e);
    }

    // NY DOL
    try {
        const nyHtml = await fetchStatePage('https://dol.ny.gov/warn-notices');
        if (nyHtml) {
            const cleanText = cleanHTML(nyHtml);
            rawLayoffs.push({
                company: 'New York DOL Page Extract',
                state: 'NY',
                location: 'Multiple Locations',
                noticeDate: new Date().toISOString().split('T')[0],
                effectiveDate: '',
                jobs: 'unknown',
                source: 'New York DOL Page Scrape',
                detailsUrl: 'https://dol.ny.gov/warn-notices',
                detailsTextSnippet: cleanText.slice(0, 3000)
            } as any);
            console.log('✅ New York DOL page fetched successfully');
        }
    } catch (e) {
        console.warn('⚠️ New York DOL direct fetch failed, falling back to unified dataset:', e);
    }

    // 2. Unified 50-State Dataset Fetch (Stanford Big Local News)
    console.log('🏛️ Fetching unified 50-state WARN dataset...');
    try {
        const res = await fetch(UNIFIED_WARN_CSV_URL);
        if (res.ok) {
            const csvText = await res.text();
            const lines = csvText.split('\n');
            const headers = parseCSVLine(lines[0] || '');
            
            // Map headers to indices
            const hIdx = {
                postal_code: headers.indexOf('postal_code'),
                company: headers.indexOf('company'),
                location: headers.indexOf('location'),
                notice_date: headers.indexOf('notice_date'),
                effective_date: headers.indexOf('effective_date'),
                jobs: headers.indexOf('jobs')
            };

            // Scan the WHOLE file and filter on a rolling recency window. The file's row order is
            // not guaranteed chronological, so a fixed tail slice can miss new notices; and a
            // hardcoded year literal goes blind at each new year. Window: notices ≤120 days old.
            const RECENCY_WINDOW_MS = 120 * 24 * 60 * 60 * 1000;
            const recencyFloor = Date.now() - RECENCY_WINDOW_MS;
            const dataLines = lines.slice(1); // skip header
            let unifiedCount = 0;

            for (const line of dataLines) {
                if (!line.trim()) continue;
                const cols = parseCSVLine(line);
                const company = cols[hIdx.company] || '';
                const state = cols[hIdx.postal_code] || '';
                const noticeDate = cols[hIdx.notice_date] || '';
                const effectiveDate = cols[hIdx.effective_date] || '';
                const jobs = cols[hIdx.jobs] || '';
                const location = cols[hIdx.location] || '';

                if (!company || company.toLowerCase() === 'company') continue;

                // Keep notices whose parsed date falls within the rolling window.
                const noticeMs = Date.parse(noticeDate);
                const isRecent = !Number.isNaN(noticeMs) && noticeMs >= recencyFloor;
                if (isRecent) {
                    rawLayoffs.push({
                        company,
                        state,
                        location,
                        noticeDate,
                        effectiveDate,
                        jobs,
                        source: `WARN State Feed (${state})`,
                        detailsUrl: `https://raw.githubusercontent.com/biglocalnews/warn-github-flow/transformer/data/warn-transformer/processed/consolidated.csv`
                    });
                    unifiedCount++;
                }
            }
            console.log(`✅ Loaded ${unifiedCount} recent layoffs from 50-state unified dataset`);
        } else {
            console.warn(`⚠️ Stanford unified WARN fetch failed with status ${res.status}`);
        }
    } catch (e) {
        console.error('❌ Unified 50-state dataset fetch failed:', e);
    }

    if (rawLayoffs.length === 0) {
        console.warn('⚠️ No state WARN/Layoff data collected.');
        return [];
    }

    // Deduplicate by company name to avoid redundancy
    const uniqueRaw: typeof rawLayoffs = [];
    const seenCompanies = new Set<string>();
    for (const item of rawLayoffs) {
        const key = `${item.company.toLowerCase().trim()}_${item.state.toLowerCase().trim()}`;
        if (!seenCompanies.has(key)) {
            seenCompanies.add(key);
            uniqueRaw.push(item);
        }
    }
    console.log(`🧹 Deduplicated raw layoff entries to ${uniqueRaw.length} unique items`);

    // Sort by notice date (newest first) so the 60-item cap keeps the freshest notices,
    // not whatever happened to appear first in the file.
    uniqueRaw.sort((a, b) => (Date.parse(b.noticeDate) || 0) - (Date.parse(a.noticeDate) || 0));

    // Keep top 60 items to stay safely within Gemini's response limit
    const chunk = uniqueRaw.slice(0, 60);
    return extractAndBuildTriggers(env, chunk);
}

async function extractAndBuildTriggers(
    env: Env,
    items: Array<{
        company: string;
        state: string;
        location: string;
        noticeDate: string;
        effectiveDate: string;
        jobs: string;
        source: string;
        detailsUrl: string;
        detailsTextSnippet?: string;
    }>
): Promise<MarketTrigger[]> {
    const itemsPrompt = items.map((item, i) => {
        return `[Item ${i}]\nCompany/Source: ${item.company}\nState: ${item.state}\nLocation: ${item.location}\nNotice Date: ${item.noticeDate}\nEffective Date: ${item.effectiveDate}\nJobs Affected: ${item.jobs}\nDetails Snippet: ${item.detailsTextSnippet || 'N/A'}`;
    }).join('\n\n');

    const systemPrompt = `You are a strategic intelligence AI for a premium corporate advisory law firm. Given a list of corporate mass layoffs and WARN notices, extract details and assign each to the most appropriate Partner Agent campaign:

Campaign/Agent Focus Areas:
- Agent 1 (M&A / Restructuring): For massive facility closures, plant shutdowns, business liquidations, large-scale layoffs (100+ employees), or companies in distress/bankruptcy.
- Agent 3 (Leadership Hires / Transitions): For notices mentioning a change in leadership, newly appointed CEOs, transition executives, or when high-level executive search/HR is involved.
- Other Agents: Only map to Agents 2, 4, or 5 if there is a highly specific signal (e.g. strategic partnership or IPO mentioned), otherwise default to Agent 1 for M&A/Restructuring or Agent 3 for leadership moves.

Extraction Rules:
1. Extract the PRIMARY COMPANY (clean business name, e.g. "SpaceX" instead of "Space Exploration Technologies Corp /DE/").
2. Identify the HIGHEST-RANKING DECISION MAKER mentioned, or specify a likely corporate title (e.g. "CEO", "General Counsel", "Chief Human Resources Officer") if not named.
3. Determine a relevance score 0-100 based on scale (higher score for 100+ employees, Fortune 1000, or notable tech/financial firms). Discard low-value local events (e.g., small local retail, restaurants, daycare closures) by giving them relevance < 50.
4. Set the most appropriate agentId (1 to 5). High-relevance layoffs and restructurings MUST map to agentId 1 (M&A / Restructuring) or agentId 3 (Leadership).

Respond with ONLY a JSON array of objects:
[{
  "index": 0,
  "company": "Company Name",
  "executiveName": "First Last or Unknown",
  "executiveTitle": "CEO / General Counsel / CHRO",
  "relevanceScore": 85,
  "estimatedEmployees": "250",
  "agentId": 1,
  "rationale": "Large manufacturing plant closure with 250 employees, highly actionable for corporate restructuring."
}]`;

    let extracted: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-state-warn',
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
                        type: 'ARRAY',
                        items: {
                            type: 'OBJECT',
                            properties: {
                                index: { type: 'INTEGER' },
                                company: { type: 'STRING' },
                                executiveName: { type: 'STRING' },
                                executiveTitle: { type: 'STRING' },
                                relevanceScore: { type: 'INTEGER' },
                                estimatedEmployees: { type: 'STRING' },
                                agentId: { type: 'INTEGER' },
                                rationale: { type: 'STRING' }
                            },
                            required: ['index', 'company', 'executiveName', 'executiveTitle', 'relevanceScore', 'agentId']
                        }
                    }
                }
            })
        });

        if (!geminiRes.ok) throw new Error(await geminiRes.text());

        const { text: rawText } = await safeGeminiResponseParse(geminiRes);
        if (rawText) {
            let jsonStr = rawText;
            const match = rawText.match(/\[[\s\S]*\]/);
            if (match) jsonStr = match[0];
            extracted = safeJsonParse<ExtractedMeta[]>(jsonStr, []);
            console.log(`📋 Gemini extracted ${extracted.length} state WARN entities`);
        }
    } catch (err) {
        console.error('❌ Gemini WARN extraction failed:', err);
        await logGeminiError(env, 'lite-state-warn-extraction', 'sense-state-warn', err, { itemsCount: items.length });
        return [];
    }

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const originalItem = items[meta.index];
        if (!originalItem) continue;

        // Skip relevance < 50
        if (meta.relevanceScore < 50) {
            console.log(`🗑️ Discarded low-relevance lead: ${meta.company} (${meta.relevanceScore}) - ${meta.rationale}`);
            continue;
        }

        const effectiveText = originalItem.effectiveDate ? ` (effective ${originalItem.effectiveDate})` : '';
        const empText = meta.estimatedEmployees ? ` affecting ~${meta.estimatedEmployees} employees` : '';

        triggers.push({
            triggerId: `statewarn-${crypto.randomUUID().slice(0, 8)}`,
            source: originalItem.source,
            sourceUrl: originalItem.detailsUrl,
            headline: `Mass Layoff/Closure: ${meta.company}${empText}${effectiveText} [${originalItem.state}]`,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore,
            detectedAt: originalItem.noticeDate || new Date().toISOString(),
            articleText: `WARN Notice filed in ${originalItem.state} on ${originalItem.noticeDate || 'recent date'}. Location: ${originalItem.location || 'Multiple'}.${empText}${effectiveText}. Analysis: ${meta.rationale}`,
            agentId: meta.agentId || 1
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    console.log(`✅ Emitted ${triggers.length} premium WARN triggers to pipeline`);
    return triggers;
}
