/**
 * Cross-source deduplication for MarketTriggers.
 * Prevents the same company/event from appearing multiple times
 * when detected by different sources (Exa, SEC, Court, News).
 */
import type { MarketTrigger } from '../activities/sense-triggers';

/**
 * Deduplicate triggers across sources. When the same company appears
 * from multiple sources, keeps the one with the highest relevance score.
 */
export function deduplicateTriggers(triggers: MarketTrigger[]): MarketTrigger[] {
    const seen = new Map<string, MarketTrigger>();

    for (const t of triggers) {
        const key = normalizeCompany(t.company);
        if (!key) continue; // skip empty company names

        const existing = seen.get(key);
        if (!existing || t.relevanceScore > existing.relevanceScore) {
            seen.set(key, t); // keep higher-scoring version
        }
    }

    return Array.from(seen.values());
}

/**
 * Normalize company names for comparison.
 * Strips common suffixes (Inc, Corp, LLC, etc.) and punctuation.
 */
function normalizeCompany(name: string): string {
    return name
        .toLowerCase()
        .replace(/\b(inc|corp|corporation|llc|ltd|limited|co|company|group|holdings|plc|lp|partners)\b\.?/g, '')
        .replace(/[^a-z0-9]/g, '')
        .trim();
}
