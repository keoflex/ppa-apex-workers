/**
 * Activity: Enrich Lead Data
 * Enriches prospect data via Apollo.io People Match API.
 * Uses native fetch — no Node.js built-ins, compatible with Cloudflare Workers.
 * Falls back to safe defaults if Apollo.io is unavailable or the key is not set.
 *
 * Write-back: PATCHes the `lead_targets.enrichment_data` JSONB column in Supabase.
 */
import type { Env } from '../index';
import { patchRow } from '../utils/supabase';

export interface EnrichInput {
    company: string;
    executiveName: string;
    executiveTitle: string;
    leadId?: number; // BIGSERIAL — for Supabase write-back
}

export interface EnrichedLead {
    company: string;
    executiveName: string;
    executiveTitle: string;
    companyRevenue: string;
    employeeCount: string;
    signals: string[];
    linkedinUrl: string;
}

const APOLLO_API_URL = 'https://api.apollo.io/v1/people/match';

/**
 * Builds a safe fallback when Apollo.io is unavailable.
 */
function buildFallback(input: EnrichInput): EnrichedLead {
    return {
        company: input.company,
        executiveName: input.executiveName,
        executiveTitle: input.executiveTitle,
        companyRevenue: 'Unknown',
        employeeCount: 'Unknown',
        signals: [],
        linkedinUrl: '',
    };
}

export async function enrichLead(env: Env, input: EnrichInput): Promise<EnrichedLead> {
    console.log(`📊 Enriching lead: ${input.executiveName} @ ${input.company}`);

    // Guard: if no Apollo key is configured, return safe fallback
    if (!env.APOLLO_API_KEY) {
        console.warn('⚠️ APOLLO_API_KEY not set — returning fallback enrichment');
        return buildFallback(input);
    }

    try {
        const [firstName, ...lastParts] = input.executiveName.split(' ');
        const lastName = lastParts.join(' ') || firstName;

        const response = await fetch(APOLLO_API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.APOLLO_API_KEY,
            },
            body: JSON.stringify({
                first_name: firstName,
                last_name: lastName,
                organization_name: input.company,
                title: input.executiveTitle || undefined,
            }),
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Apollo API error ${response.status}: ${errText}`);
        }

        const data = await response.json() as {
            person?: {
                first_name?: string;
                last_name?: string;
                title?: string;
                linkedin_url?: string;
                organization?: {
                    estimated_num_employees?: number;
                    annual_revenue?: number;
                    annual_revenue_printed?: string;
                    keywords?: string[];
                };
            };
        };

        const person = data.person;
        if (!person) {
            console.warn('⚠️ Apollo returned no person match — using fallback');
            return buildFallback(input);
        }

        const org = person.organization;

        const lead: EnrichedLead = {
            company: input.company,
            executiveName: person.first_name && person.last_name
                ? `${person.first_name} ${person.last_name}`
                : input.executiveName,
            executiveTitle: person.title || input.executiveTitle,
            companyRevenue: org?.annual_revenue_printed || (org?.annual_revenue ? `$${(org.annual_revenue / 1_000_000).toFixed(0)}M` : 'Unknown'),
            employeeCount: org?.estimated_num_employees?.toLocaleString() || 'Unknown',
            signals: org?.keywords?.slice(0, 5) || [],
            linkedinUrl: person.linkedin_url || '',
        };

        console.log(`✅ Lead enriched via Apollo: ${lead.company} (${lead.companyRevenue})`);

        // Write enrichment data back to Supabase
        if (input.leadId) {
            const writeResult = await patchRow(env, 'lead_targets', {
                enrichment_data: {
                    revenue: lead.companyRevenue,
                    employees: lead.employeeCount,
                    signals: lead.signals,
                    linkedin_url: lead.linkedinUrl,
                    source: 'apollo.io',
                    enriched_at: new Date().toISOString(),
                },
            }, 'id', input.leadId);

            if (writeResult.ok) {
                console.log(`💾 Enrichment data written to lead_targets (id: ${input.leadId})`);
            }
        }

        return lead;
    } catch (err) {
        console.error('❌ Apollo enrichment failed, returning fallback:', err);
        return buildFallback(input);
    }
}
