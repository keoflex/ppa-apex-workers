/**
 * Cross-source deduplication for MarketTriggers.
 * Prevents the same company/event from appearing multiple times
 * when detected by different sources (Exa, SEC, Court, News).
 */
import type { MarketTrigger } from '../activities/sense-triggers';

// Extend MarketTrigger with optional fields for advanced matching
export interface SmartMarketTrigger extends MarketTrigger {
    companyDomain?: string;
    linkedinUrl?: string;
}

/**
 * Deduplicate triggers across sources. Matches by:
 * 1. Normalized company name.
 * 2. Company domain (if available).
 * 3. LinkedIn URL (if available).
 * When duplicates are found, keeps the one with the highest relevance score.
 */
export function deduplicateTriggers(triggers: SmartMarketTrigger[]): SmartMarketTrigger[] {
    const seenByName = new Map<string, SmartMarketTrigger>();
    const seenByDomain = new Map<string, SmartMarketTrigger>();
    const seenByLinkedin = new Map<string, SmartMarketTrigger>();
    // Track every key each kept trigger registered, so we can fully unregister it on replacement.
    const keysOf = new Map<SmartMarketTrigger, { name: string | null; domain: string | null; linkedin: string | null }>();

    const uniqueTriggers: SmartMarketTrigger[] = [];

    const computeKeys = (t: SmartMarketTrigger) => ({
        // Fall back to the raw lowercased name when normalization collapses to '' (generic-word names),
        // otherwise such companies would never dedupe against each other.
        name: normalizeCompany(t.company) || t.company.toLowerCase().replace(/[^a-z0-9]/g, '') || null,
        domain: t.companyDomain ? (normalizeDomain(t.companyDomain) || null) : null,
        linkedin: t.linkedinUrl ? (normalizeLinkedin(t.linkedinUrl) || null) : null,
    });

    const register = (t: SmartMarketTrigger, keys: { name: string | null; domain: string | null; linkedin: string | null }) => {
        if (keys.name) seenByName.set(keys.name, t);
        if (keys.domain) seenByDomain.set(keys.domain, t);
        if (keys.linkedin) seenByLinkedin.set(keys.linkedin, t);
        keysOf.set(t, keys);
    };

    const unregister = (t: SmartMarketTrigger) => {
        const keys = keysOf.get(t);
        if (!keys) return;
        if (keys.name && seenByName.get(keys.name) === t) seenByName.delete(keys.name);
        if (keys.domain && seenByDomain.get(keys.domain) === t) seenByDomain.delete(keys.domain);
        if (keys.linkedin && seenByLinkedin.get(keys.linkedin) === t) seenByLinkedin.delete(keys.linkedin);
        keysOf.delete(t);
    };

    for (const t of triggers) {
        if (!t || !t.company || !t.company.trim()) {
            console.warn('⚠️ Skipping trigger without company name:', t?.headline || 'No Headline');
            continue;
        }
        const keys = computeKeys(t);

        let existing: SmartMarketTrigger | undefined = undefined;
        if (keys.name) existing = seenByName.get(keys.name);
        if (!existing && keys.domain) existing = seenByDomain.get(keys.domain);
        if (!existing && keys.linkedin) existing = seenByLinkedin.get(keys.linkedin);

        if (existing) {
            // Keep the higher-scoring trigger; fully swap registrations when the new one wins.
            if (t.relevanceScore > existing.relevanceScore) {
                const idx = uniqueTriggers.indexOf(existing);
                if (idx > -1) uniqueTriggers.splice(idx, 1);
                unregister(existing);
                uniqueTriggers.push(t);
                register(t, keys);
            }
            // else: t is a duplicate of a higher/equal-scoring trigger — drop it.
        } else {
            uniqueTriggers.push(t);
            register(t, keys);
        }
    }

    return uniqueTriggers;
}

/**
 * Normalize company names for comparison.
 * Strips common suffixes (Inc, Corp, LLC, etc.), common business words, and punctuation.
 */
export function normalizeCompany(name: string): string {
    return (name || '')
        .toLowerCase()
        .replace(/\b(inc|corp|corporation|llc|ltd|limited|co|company|group|holdings|plc|lp|partners|global|solutions|systems|technologies|associates|advisors)\b\.?/g, '')
        .replace(/[^a-z0-9]/g, '')
        .trim();
}

/**
 * Normalize domain names for clustering (e.g. www.google.com -> google.com)
 */
export function normalizeDomain(domain: string): string {
    if (!domain) return '';
    return domain
        .toLowerCase()
        .replace(/^(https?:\/\/)?(www\.)?/, '')
        .replace(/\/$/, '')
        .split('/')[0];
}

/**
 * Normalize LinkedIn URLs for exact comparison (e.g., handles trailing slashes, subdomains)
 */
export function normalizeLinkedin(url: string): string {
    if (!url) return '';
    return url
        .toLowerCase()
        .trim()
        .replace(/^(https?:\/\/)?(www\.)?linkedin\.com\/in\//, '')
        .replace(/\/$/, '');
}
