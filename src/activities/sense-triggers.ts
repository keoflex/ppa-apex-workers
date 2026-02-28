/**
 * Activity: Sense Market Triggers
 * Scans Exa.ai for actionable market events relevant to PPA+ institutional outreach.
 * Uses native fetch — no Node.js built-ins.
 */
import type { Env } from '../index';

export interface MarketTrigger {
    triggerId: string;
    source: string;
    headline: string;
    company: string;
    executiveName: string;
    executiveTitle: string;
    relevanceScore: number;
    detectedAt: string;
}

// ---------------------------------------------------------------------------
// Exa.ai response shapes (minimal, only the fields we consume)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Search queries — five signal buckets that matter to PPA+ outreach
// ---------------------------------------------------------------------------

const EXA_QUERIES = [
    'mergers and acquisitions announcement 2025 financial services',
    'new private equity fund launch capital raise 2025',
    'executive transition appointment CFO CIO managing director 2025',
    'infrastructure investment award municipal government contract 2025',
    'regulatory filing SEC CFTC alternative investment 2025',
] as const;

// ---------------------------------------------------------------------------
// Extract a rough company name and executive name from Exa result text
// ---------------------------------------------------------------------------

function extractMeta(result: ExaResult): { company: string; executiveName: string; executiveTitle: string } {
    // Best-effort extraction — Exa text may contain rich context
    const text = result.text ?? result.highlights?.join(' ') ?? result.title;

    // Company: use the domain (hostname minus www. / .com)
    let company = 'Unknown Company';
    try {
        const hostname = new URL(result.url).hostname.replace(/^www\./, '').split('.')[0];
        company = hostname.charAt(0).toUpperCase() + hostname.slice(1);
    } catch {
        /* ignore malformed URLs */
    }

    // Try to pull a person-like pattern ("FirstName LastName, Title") from text
    const personMatch = text.match(/([A-Z][a-z]+ [A-Z][a-z]+),\s*([A-Z][^.,]{3,40})/);
    const executiveName = personMatch ? personMatch[1] : 'Key Decision-Maker';
    const executiveTitle = personMatch ? personMatch[2].trim() : 'Executive';

    return { company, executiveName, executiveTitle };
}

// ---------------------------------------------------------------------------
// Main activity
// ---------------------------------------------------------------------------

export async function senseTriggers(env: Env): Promise<MarketTrigger[]> {
    console.log('🔍 Sensing market triggers via Exa.ai...');

    const triggers: MarketTrigger[] = [];

    // Fan out across all query buckets, collect results, ignore individual failures
    const queryResults = await Promise.allSettled(
        EXA_QUERIES.map((query) =>
            fetch('https://api.exa.ai/search', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': env.EXA_API_KEY,
                },
                body: JSON.stringify({
                    query,
                    numResults: 3,
                    type: 'neural',
                    useAutoprompt: true,
                    // Request text highlights for better extraction
                    contents: {
                        text: { maxCharacters: 800 },
                        highlights: { numSentences: 2, highlightsPerUrl: 1 },
                    },
                }),
            })
                .then(async (res) => {
                    if (!res.ok) {
                        const errBody = await res.text();
                        throw new Error(`Exa error ${res.status}: ${errBody}`);
                    }
                    return res.json() as Promise<ExaSearchResponse>;
                })
        )
    );

    for (const settled of queryResults) {
        if (settled.status === 'rejected') {
            console.warn('⚠️ Exa query failed:', settled.reason);
            continue;
        }

        const data = settled.value;
        if (!Array.isArray(data?.results)) continue;

        for (const result of data.results) {
            try {
                const { company, executiveName, executiveTitle } = extractMeta(result);
                triggers.push({
                    triggerId: `trg-${crypto.randomUUID().slice(0, 8)}`,
                    source: 'Exa.ai',
                    headline: result.title ?? 'Market event detected',
                    company,
                    executiveName,
                    executiveTitle,
                    // Exa scores typically 0–1; multiply to an integer 0–100
                    relevanceScore: Math.round((result.score ?? 0.7) * 100),
                    detectedAt: result.publishedDate ?? new Date().toISOString(),
                });
            } catch (mapErr) {
                console.warn('⚠️ Failed to map Exa result:', mapErr);
            }
        }
    }

    // Sort descending by relevance, keep top 10
    triggers.sort((a, b) => b.relevanceScore - a.relevanceScore);
    const top = triggers.slice(0, 10);

    console.log(`✅ Detected ${top.length} market triggers from Exa.ai`);
    return top;
}
