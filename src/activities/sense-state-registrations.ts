/**
 * Activity: Sense State Corporate Registrations (Free, Secretary of State Network)
 * Polls state registries via Socrata Open Data (e.g. New York State) to find newly qualified "Foreign Entities".
 * These indicate cross-state geographical expansion - a premium signal for Strategic Partnerships and corporate advisory.
 * Re-maps signals to Agent 4 (Strategic Partnerships & Geographical Expansion) with $0 data acquisition cost.
 */
import type { Env } from '../index';
import type { MarketTrigger } from './sense-triggers';
import { fetchGemini } from '../utils/gemini-fetch';
import { logGeminiError } from '../utils/gemini-logger';
import { safeJsonParse } from '../utils/json-repair';
import { safeGeminiResponseParse } from '../utils/gemini-parse';

// Socrata Open Data Endpoint for NYS Active Corporations (updated daily)
const NYS_CORPS_JSON_URL = 'https://data.ny.gov/resource/n9v6-gdp6.json';

interface SocrataCorpRecord {
    dos_id: string;
    current_entity_name: string;
    initial_dos_filing_date: string;
    county: string;
    jurisdiction: string; // state of origin
    entity_type: string;
    dos_process_name?: string;
    dos_process_address_1?: string;
    dos_process_city?: string;
    dos_process_state?: string;
    dos_process_zip?: string;
    registered_agent_name?: string;
    registered_agent_address_1?: string;
}

interface ExtractedMeta {
    index: number;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    agentId: number;
    rationale: string;
}

export async function senseStateRegistrations(env: Env): Promise<MarketTrigger[]> {
    console.log('🏛️ Starting State SoS Corporate Registration Sensor...');

    const rawRegistrations: SocrataCorpRecord[] = [];

    // 1. Attempt to pull the most recent active business filings from NYS
    try {
        console.log('📡 Fetching recent NYS DOS business registrations...');
        const url = `${NYS_CORPS_JSON_URL}?$order=initial_dos_filing_date DESC&$limit=100`;
        const nysHeaders: Record<string, string> = {
            'Accept': 'application/json',
            'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)'
        };
        if (env.SOCRATA_APP_TOKEN) {
            nysHeaders['X-App-Token'] = env.SOCRATA_APP_TOKEN;
        }
        const res = await fetch(url, {
            headers: nysHeaders
        });

        if (res.ok) {
            const data = await res.json() as SocrataCorpRecord[];
            if (Array.isArray(data)) {
                let nyCount = 0;
                for (const record of data) {
                    const type = (record.entity_type || '').toUpperCase();
                    const origin = (record.jurisdiction || '').toUpperCase();
                    
                    const isForeign = type.includes('FOREIGN') || (origin && origin !== 'NEW YORK' && origin !== 'NY');
                    if (isForeign && record.current_entity_name) {
                        rawRegistrations.push({
                            ...record,
                            county: record.county || 'NEW YORK'
                        });
                        nyCount++;
                    }
                }
                console.log(`✅ Loaded ${nyCount} foreign entity qualifications from NYS registry`);
            }
        } else {
            console.warn(`⚠️ Socrata NYS registry fetch returned status: ${res.status}`);
        }
    } catch (e) {
        console.error('❌ Failed to fetch live state registrations from NYS Open Data:', e);
    }

    // 2. SF Socrata active business location feed: ownership_name (Company), location_start_date (Filing Date), state, uniqueid (UID)
    try {
        console.log('📡 Fetching recent San Francisco business registrations...');
        const url = `https://data.sfgov.org/resource/g8m3-pdis.json?$order=location_start_date DESC&$limit=100`;
        const sfHeaders: Record<string, string> = {
            'Accept': 'application/json',
            'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)'
        };
        if (env.SOCRATA_APP_TOKEN) {
            sfHeaders['X-App-Token'] = env.SOCRATA_APP_TOKEN;
        }
        const res = await fetch(url, {
            headers: sfHeaders
        });

        if (res.ok) {
            const data = await res.json() as any[];
            if (Array.isArray(data)) {
                let sfCount = 0;
                for (const record of data) {
                    const state = (record.state || '').toUpperCase();
                    // If HQ state is not CA, it's a foreign qualification/expansion in SF!
                    const isForeign = state && state !== 'CA';
                    if (isForeign && record.ownership_name) {
                        rawRegistrations.push({
                            dos_id: record.uniqueid || `SF-${crypto.randomUUID().slice(0, 8)}`,
                            current_entity_name: record.ownership_name,
                            initial_dos_filing_date: record.location_start_date || new Date().toISOString(),
                            county: 'SAN FRANCISCO',
                            jurisdiction: state,
                            entity_type: 'FOREIGN ENTITY (SF)',
                            dos_process_name: record.ownership_name,
                            registered_agent_name: 'N/A'
                        });
                        sfCount++;
                    }
                }
                console.log(`✅ Loaded ${sfCount} foreign entity qualifications from SF registry`);
            }
        } else {
            console.warn(`⚠️ Socrata SF registry fetch returned status: ${res.status}`);
        }
    } catch (e) {
        console.error('❌ Failed to fetch live registrations from SF Open Data:', e);
    }

    // 3. LA Socrata active businesses: business_name (Company), location_start_date (Filing Date), location_account (UID)
    try {
        console.log('📡 Fetching recent Los Angeles business registrations...');
        const url = `https://data.lacity.org/resource/6rrh-rzua.json?$order=location_start_date DESC&$limit=100`;
        const laHeaders: Record<string, string> = {
            'Accept': 'application/json',
            'User-Agent': 'PPA-APEX/1.0 (apex@posinelli.com)'
        };
        if (env.SOCRATA_APP_TOKEN) {
            laHeaders['X-App-Token'] = env.SOCRATA_APP_TOKEN;
        }
        const res = await fetch(url, {
            headers: laHeaders
        });

        if (res.ok) {
            const data = await res.json() as any[];
            if (Array.isArray(data)) {
                let laCount = 0;
                for (const record of data) {
                    if (record.business_name) {
                        rawRegistrations.push({
                            dos_id: record.location_account || `LA-${crypto.randomUUID().slice(0, 8)}`,
                            current_entity_name: record.business_name,
                            initial_dos_filing_date: record.location_start_date || new Date().toISOString(),
                            county: 'LOS ANGELES',
                            jurisdiction: 'UNKNOWN',
                            entity_type: 'LA BUSINESS REGISTRATION',
                            dos_process_name: record.business_name,
                            registered_agent_name: 'N/A'
                        });
                        laCount++;
                    }
                }
                console.log(`✅ Loaded ${laCount} registrations from LA registry`);
            }
        } else {
            console.warn(`⚠️ Socrata LA registry fetch returned status: ${res.status}`);
        }
    } catch (e) {
        console.error('❌ Failed to fetch live registrations from LA Open Data:', e);
    }

    // No data is a valid outcome — never fabricate registrations when feeds are empty or down.
    if (rawRegistrations.length === 0) {
        console.log('ℹ️ No state registrations returned this cycle (feeds empty or unavailable). Emitting 0 triggers.');
        return [];
    }

    // Keep top 60 items for analysis
    const chunk = rawRegistrations.slice(0, 60);
    return extractAndBuildTriggers(env, chunk);
}

async function extractAndBuildTriggers(
    env: Env,
    items: SocrataCorpRecord[]
): Promise<MarketTrigger[]> {
    const itemsPrompt = items.map((item, i) => {
        return `[Item ${i}]\nEntity Name: ${item.current_entity_name}\nOriginal Jurisdiction: ${item.jurisdiction || 'DELAWARE'}\nEntity Type: ${item.entity_type}\nFiling Date: ${item.initial_dos_filing_date}\nProcess Name/Rep: ${item.dos_process_name || 'N/A'}\nRegistered Agent: ${item.registered_agent_name || 'N/A'}`;
    }).join('\n\n');

    const systemPrompt = `You are a strategic intelligence AI for a premium corporate advisory law firm. Given a list of Secretary of State "Foreign Entity" corporate registrations, extract details and classify them for outreach:

Strategic Context:
- A "Foreign Entity" registration means a company headquartered in Delaware or another state is qualifying to do business locally. This is a massive expansion signal!
- We want to target these expanding corporations for corporate advisory, compliance, real estate, and strategic partnership counsel.

Extraction Rules:
1. Extract the PRIMARY COMPANY (clean business name, removing suffixes like LLC, CORP, INC, or FOREIGN LLC).
2. Identify the HIGHEST-RANKING DECISION MAKER mentioned in the filing process name or representative field. If no executive is explicitly named, specify the corporate title "General Counsel" or "VP of Expansion" as the default contact.
3. Assign a relevance score 0-100 based on scale (higher score for notable tech, AI, defense, finance, or pharmaceutical companies). Discard small shell entities by assigning relevance < 50.
4. Set the agentId to 4 (default for geographical expansion & strategic partnerships).

Respond with ONLY a JSON array of objects:
[{
  "index": 0,
  "company": "Company Name",
  "executiveName": "First Last or Unknown",
  "executiveTitle": "General Counsel / VP of Expansion / CEO",
  "relevanceScore": 85,
  "agentId": 4,
  "rationale": "Expanding fintech firm registering locally, highly actionable for strategic partnerships and local compliance."
}]`;

    let extracted: ExtractedMeta[] = [];
    try {
        const geminiRes = await fetchGemini(env, 'lite', {
            activityName: 'sense-state-registrations',
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
            console.log(`📋 Gemini extracted ${extracted.length} corporate expansion entities`);
        }
    } catch (err) {
        console.error('❌ Gemini Corporate Registrations extraction failed:', err);
        await logGeminiError(env, 'lite-state-registrations-extraction', 'sense-state-registrations', err, { itemsCount: items.length });
        return [];
    }

    const triggers: MarketTrigger[] = [];
    for (const meta of extracted) {
        const originalItem = items[meta.index];
        if (!originalItem) continue;

        // Skip relevance < 50
        if (meta.relevanceScore < 50) {
            console.log(`🗑️ Discarded low-relevance corporate expansion: ${meta.company} (${meta.relevanceScore})`);
            continue;
        }

        let source = 'State Corporate Registry';
        let sourceUrl = `https://data.ny.gov/resource/n9v6-gdp6.json?dos_id=${originalItem.dos_id}`;
        let headline = `Corporate Expansion: ${meta.company} qualifying as Foreign Entity in NYS (from ${originalItem.jurisdiction || 'DELAWARE'})`;
        let articleText = `Entity "${meta.company}" (Original State: ${originalItem.jurisdiction || 'DELAWARE'}) filed a Foreign Entity qualification in New York State on ${originalItem.initial_dos_filing_date || 'recent date'}. Registered Agent: ${originalItem.registered_agent_name || 'N/A'}. Processing Contact: ${originalItem.dos_process_name || 'N/A'}. Analysis: ${meta.rationale}`;

        if (originalItem.county === 'SAN FRANCISCO') {
            source = 'San Francisco Corporate Registry';
            sourceUrl = `https://data.sfgov.org/resource/g8m3-pdis.json?uniqueid=${originalItem.dos_id}`;
            headline = `Corporate Expansion: ${meta.company} qualifying as Foreign Entity in SF (from ${originalItem.jurisdiction || 'DELAWARE'})`;
            articleText = `Entity "${meta.company}" (Original State: ${originalItem.jurisdiction || 'DELAWARE'}) qualified as a Foreign Entity in San Francisco on ${originalItem.initial_dos_filing_date || 'recent date'}. Unique ID: ${originalItem.dos_id}. Analysis: ${meta.rationale}`;
        } else if (originalItem.county === 'LOS ANGELES') {
            source = 'Los Angeles Corporate Registry';
            sourceUrl = `https://data.lacity.org/resource/6rrh-rzua.json?location_account=${originalItem.dos_id}`;
            headline = `Corporate Expansion: ${meta.company} registering new location in Los Angeles`;
            articleText = `Entity "${meta.company}" registered a new active business location in Los Angeles on ${originalItem.initial_dos_filing_date || 'recent date'}. Account: ${originalItem.dos_id}. Analysis: ${meta.rationale}`;
        }

        triggers.push({
            triggerId: `statecorp-${crypto.randomUUID().slice(0, 8)}`,
            source,
            sourceUrl,
            headline,
            company: meta.company,
            executiveName: meta.executiveName,
            executiveTitle: meta.executiveTitle,
            relevanceScore: meta.relevanceScore,
            detectedAt: originalItem.initial_dos_filing_date || new Date().toISOString(),
            articleText,
            agentId: meta.agentId || 4
        });
    }

    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    console.log(`✅ Emitted ${triggers.length} premium corporate expansion triggers to pipeline`);
    return triggers;
}
