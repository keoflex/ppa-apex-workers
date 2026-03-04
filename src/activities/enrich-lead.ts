/**
 * Activity: Enrich Lead Data — MULTI-LAYER DISCOVERY PIPELINE
 *
 * This is the core value driver. Finding actionable contact data is critical.
 *
 * PIPELINE (each layer is a fallback if the previous one fails):
 *   1. Gemini identifies executives by company name (knowledge-based)
 *   2. Apollo People Match — exact name + company match
 *   3. Apollo People Search — organization_name + seniority search
 *   4. Apollo Organization → People — find org, then search for decision-makers
 *   5. Email Pattern Generation — infer email from name + company domain
 *   6. Exa deep research — career background, press mentions
 *
 * Write-back: PATCHes the `lead_targets.enrichment_data` JSONB column in Supabase.
 */
import type { Env } from '../index';
import { patchRow } from '../utils/supabase';

/** Fetch with timeout to avoid Cloudflare Worker 30s limit */
async function fetchWithTimeout(url: string, init: RequestInit, timeoutMs = 8000): Promise<Response> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const res = await fetch(url, { ...init, signal: controller.signal });
        return res;
    } finally {
        clearTimeout(timer);
    }
}

export interface EnrichInput {
    company: string;
    executiveName: string;
    executiveTitle: string;
    leadId?: number;
}

export interface ContactInfo {
    name: string;
    title: string;
    email: string;
    phone: string;
    linkedinUrl: string;
    seniority: string;
    source: string; // which layer found this contact
}

export interface EnrichedLead {
    company: string;
    executiveName: string;
    executiveTitle: string;
    companyRevenue: string;
    employeeCount: string;
    companyDomain: string;
    signals: string[];
    linkedinUrl: string;
    email?: string;
    phone?: string;
    emailSource?: string;
    emailConfidence?: string;
    patternEmails?: string[];
    otherContacts?: ContactInfo[];
    executiveResearch?: string;
    executiveResearchUrl?: string;
}

const APOLLO_MATCH_URL = 'https://api.apollo.io/v1/people/match';
const APOLLO_SEARCH_URL = 'https://api.apollo.io/v1/mixed_people/api_search';
const APOLLO_BULK_MATCH_URL = 'https://api.apollo.io/v1/people/bulk_match';
const APOLLO_ORG_ENRICH_URL = 'https://api.apollo.io/v1/organizations/enrich';
const APOLLO_ORG_SEARCH_URL = 'https://api.apollo.io/v1/mixed_companies/search';
import { GEMINI_REST_URL as GEMINI_URL } from '../config/gemini';

function buildFallback(input: EnrichInput): EnrichedLead {
    return {
        company: input.company,
        executiveName: input.executiveName,
        executiveTitle: input.executiveTitle,
        companyRevenue: 'Unknown',
        employeeCount: 'Unknown',
        companyDomain: '',
        signals: [],
        linkedinUrl: '',
    };
}

// ─── Step 1: Gemini Executive Identification ──────────────────────────
async function identifyExecutive(
    env: Env,
    company: string,
    existingName: string,
    existingTitle: string,
): Promise<{ name: string; title: string }> {
    const fakenames = ['key decision-maker', 'unknown', 'decision maker', 'executive', 'n/a', ''];
    if (!fakenames.includes(existingName.toLowerCase().trim())) {
        console.log(`👤 Using provided name: ${existingName} (${existingTitle})`);
        return { name: existingName, title: existingTitle };
    }

    console.log(`🧠 Asking Gemini: Who leads ${company}?`);
    try {
        const res = await fetchWithTimeout(`${GEMINI_URL}?key=${env.GEMINI_API_KEY}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                system_instruction: {
                    parts: [{
                        text: `You are a business intelligence assistant. Given a company name, return the current CEO or highest-ranking executive. Respond with ONLY a JSON object: { "name": "First Last", "title": "CEO" }. Use your best knowledge. Do NOT return "Unknown".`,
                    }],
                },
                contents: [{ role: 'user', parts: [{ text: `Who is the current CEO or top executive at ${company}?` }] }],
                generationConfig: { temperature: 0.1, responseMimeType: 'application/json' },
            }),
        });

        if (res.ok) {
            const data = await res.json() as any;
            const parts = data?.candidates?.[0]?.content?.parts || [];
            const rawText = parts.find((p: any) => p.text)?.text;
            if (rawText) {
                const parsed = JSON.parse(rawText) as { name?: string; title?: string };
                if (parsed.name && parsed.name !== 'Unknown') {
                    console.log(`✅ Gemini identified: ${parsed.name} (${parsed.title}) at ${company}`);
                    return { name: parsed.name, title: parsed.title || 'CEO' };
                }
            }
        }
    } catch (err) {
        console.warn('⚠️ Gemini executive identification failed:', err);
    }

    return { name: 'Unknown', title: 'Unknown' };
}

// ─── Step 2: Apollo People Match (exact match) ────────────────────────
async function apolloPeopleMatch(
    env: Env,
    name: string,
    company: string,
    title: string,
): Promise<{ person: any; org: any } | null> {
    if (!env.APOLLO_API_KEY) return null;

    const [firstName, ...lastParts] = name.split(' ');
    const lastName = lastParts.join(' ') || firstName;

    console.log(`🔍 Layer 2: Apollo People Match: ${firstName} ${lastName} at ${company}...`);

    try {
        const res = await fetchWithTimeout(APOLLO_MATCH_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.APOLLO_API_KEY,
            },
            body: JSON.stringify({
                first_name: firstName,
                last_name: lastName,
                organization_name: company,
                title: title || undefined,
            }),
        });

        if (res.ok) {
            const data = await res.json() as any;
            if (data.person?.email) {
                console.log(`✅ Apollo Match found email: ${data.person.email}`);
                return { person: data.person, org: data.person.organization };
            }
            if (data.person) {
                console.log(`⚠️ Apollo Match found person but no email`);
                return { person: data.person, org: data.person.organization };
            }
        }
    } catch (err) {
        console.warn('⚠️ Apollo People Match failed:', err);
    }

    return null;
}

// ─── Step 3: Apollo People Search (api_search → bulk_match) ───────────
async function apolloPeopleSearch(
    env: Env,
    company: string,
    targetName?: string,
): Promise<{ primary: any; others: any[]; org: any } | null> {
    if (!env.APOLLO_API_KEY) return null;

    console.log(`🔍 Layer 3: Apollo People Search for decision-makers at ${company}...`);

    try {
        // Step 1: api_search returns partial profiles (no emails)
        const res = await fetchWithTimeout(APOLLO_SEARCH_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.APOLLO_API_KEY,
            },
            body: JSON.stringify({
                q_organization_name: company,
                person_seniorities: ['c_suite', 'vp', 'director', 'founder', 'owner', 'partner'],
                per_page: 10,
                page: 1,
            }),
        });

        if (!res.ok) {
            console.warn(`⚠️ Apollo api_search returned ${res.status}`);
            return null;
        }

        const data = await res.json() as any;
        const partialPeople = data.people || [];
        console.log(`📥 Apollo api_search returned ${partialPeople.length} partial profiles at ${company}`);

        if (partialPeople.length === 0) return null;

        // Step 2: bulk_match to enrich with emails/phones
        const details = partialPeople.map((p: any) => ({
            first_name: p.first_name,
            last_name: p.last_name,
            organization_name: p.organization?.name || company,
            ...(p.linkedin_url ? { linkedin_url: p.linkedin_url } : {}),
        }));

        const bulkRes = await fetchWithTimeout(APOLLO_BULK_MATCH_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.APOLLO_API_KEY,
            },
            body: JSON.stringify({ details }),
        });

        let enrichedPeople = partialPeople;
        if (bulkRes.ok) {
            const bulkData = await bulkRes.json() as any;
            const matches = bulkData.matches || [];
            console.log(`📥 Bulk match returned ${matches.length} enriched profiles`);
            // Merge enriched data back
            enrichedPeople = matches.map((m: any, i: number) => ({
                ...partialPeople[i],
                ...(m || {}),
            }));
        } else {
            console.warn(`⚠️ Bulk match returned ${bulkRes.status}, using partial data`);
        }

        // Try to find the target person first
        let primary = null;
        if (targetName && targetName !== 'Unknown') {
            const targetLower = targetName.toLowerCase();
            primary = enrichedPeople.find((p: any) =>
                `${p.first_name} ${p.last_name}`.toLowerCase().includes(targetLower) ||
                targetLower.includes(p.last_name?.toLowerCase())
            );
        }

        // If target not found, pick the highest-ranking person with an email
        if (!primary) {
            primary = enrichedPeople.find((p: any) => p.email) || enrichedPeople[0];
        }

        const others = enrichedPeople.filter((p: any) => p !== primary).slice(0, 5);
        const org = primary?.organization || enrichedPeople[0]?.organization;

        if (primary) {
            console.log(`✅ Apollo Search found primary: ${primary.first_name} ${primary.last_name} (${primary.title}) | Email: ${primary.email || 'N/A'}`);
            others.forEach((p: any) => {
                console.log(`   + Also found: ${p.first_name} ${p.last_name} (${p.title}) | Email: ${p.email || 'N/A'}`);
            });
        }

        return { primary, others, org };
    } catch (err) {
        console.warn('⚠️ Apollo People Search failed:', err);
    }

    return null;
}

// ─── Step 4: Apollo Organization enrich → domain → People ─────────────
async function apolloOrgAndPeopleSearch(
    env: Env,
    company: string,
    targetName?: string,
): Promise<{ domain: string; primary: any; others: any[]; org: any } | null> {
    if (!env.APOLLO_API_KEY) return null;

    console.log(`🔍 Layer 4: Apollo Org Enrich for ${company}...`);

    try {
        // First try organizations/enrich with a domain guess
        const domainGuess = company.toLowerCase().replace(/[^a-z0-9]/g, '') + '.com';
        const orgRes = await fetchWithTimeout(APOLLO_ORG_ENRICH_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.APOLLO_API_KEY,
            },
            body: JSON.stringify({ domain: domainGuess }),
        });

        let targetOrg: any = null;
        let domain = '';

        if (orgRes.ok) {
            const orgData = await orgRes.json() as any;
            targetOrg = orgData.organization;
            domain = targetOrg?.primary_domain || targetOrg?.website_url || '';
        }

        // Fallback: try mixed_companies/search
        if (!targetOrg) {
            const searchRes = await fetchWithTimeout(APOLLO_ORG_SEARCH_URL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': env.APOLLO_API_KEY,
                },
                body: JSON.stringify({
                    q_organization_name: company,
                    per_page: 1,
                    page: 1,
                }),
            });

            if (searchRes.ok) {
                const searchData = await searchRes.json() as any;
                const organizations = searchData.organizations || searchData.accounts || [];
                targetOrg = organizations[0];
                domain = targetOrg?.primary_domain || targetOrg?.website_url || '';
            }
        }

        if (!targetOrg) {
            console.log(`⚠️ Apollo Org: no org found for ${company}`);
            return null;
        }

        console.log(`✅ Found org: ${targetOrg.name} | Domain: ${domain}`);

        // Now search for people at this org using api_search + bulk_match
        if (domain) {
            const peopleRes = await fetchWithTimeout(APOLLO_SEARCH_URL, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': env.APOLLO_API_KEY,
                },
                body: JSON.stringify({
                    q_organization_domains: [domain.replace(/^https?:\/\//, '').replace(/\/$/, '')],
                    person_seniorities: ['c_suite', 'vp', 'director', 'founder', 'owner', 'partner', 'manager'],
                    per_page: 10,
                    page: 1,
                }),
            });

            if (peopleRes.ok) {
                const pData = await peopleRes.json() as any;
                const partialPeople = pData.people || [];
                console.log(`📥 Apollo domain search returned ${partialPeople.length} contacts at ${domain}`);

                // Bulk match to get full profiles
                let enrichedPeople = partialPeople;
                if (partialPeople.length > 0) {
                    const details = partialPeople.map((p: any) => ({
                        first_name: p.first_name,
                        last_name: p.last_name,
                        organization_name: targetOrg.name || company,
                    }));
                    const bulkRes = await fetchWithTimeout(APOLLO_BULK_MATCH_URL, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'x-api-key': env.APOLLO_API_KEY,
                        },
                        body: JSON.stringify({ details }),
                    });
                    if (bulkRes.ok) {
                        const bulkData = await bulkRes.json() as any;
                        const matches = bulkData.matches || [];
                        enrichedPeople = matches.map((m: any, i: number) => ({
                            ...partialPeople[i],
                            ...(m || {}),
                        }));
                    }
                }

                let primary = null;
                if (targetName && targetName !== 'Unknown') {
                    const targetLower = targetName.toLowerCase();
                    primary = enrichedPeople.find((p: any) =>
                        `${p.first_name} ${p.last_name}`.toLowerCase().includes(targetLower) ||
                        targetLower.includes(p.last_name?.toLowerCase())
                    );
                }
                if (!primary) {
                    primary = enrichedPeople.find((p: any) => p.email) || enrichedPeople[0];
                }

                const others = enrichedPeople.filter((p: any) => p !== primary).slice(0, 5);

                return { domain, primary, others, org: targetOrg };
            }
        }

        return { domain, primary: null, others: [], org: targetOrg };
    } catch (err) {
        console.warn('⚠️ Apollo Org + People search failed:', err);
    }

    return null;
}

// ─── Step 5: Email Pattern Generation ─────────────────────────────────
function generateEmailPatterns(name: string, domain: string): string[] {
    if (!domain || !name || name === 'Unknown') return [];

    const cleanDomain = domain.replace(/^https?:\/\//, '').replace(/\/$/, '').replace(/^www\./, '');
    const parts = name.toLowerCase().split(/\s+/);
    if (parts.length < 2) return [];

    const first = parts[0];
    const last = parts[parts.length - 1];
    const firstInitial = first[0];
    const lastInitial = last[0];

    return [
        `${first}.${last}@${cleanDomain}`,
        `${first}${last}@${cleanDomain}`,
        `${firstInitial}${last}@${cleanDomain}`,
        `${first}@${cleanDomain}`,
        `${first}_${last}@${cleanDomain}`,
        `${firstInitial}.${last}@${cleanDomain}`,
        `${last}.${first}@${cleanDomain}`,
        `${last}@${cleanDomain}`,
    ];
}

// ─── Step 5b: Gemini AI Contact Discovery (fallback) ──────────────────
interface GeminiContactResult {
    email?: string;
    emailConfidence?: string;
    phone?: string;
    phoneConfidence?: string;
    linkedinUrl?: string;
    twitterUrl?: string;
    companyDomain?: string;
    reasoning?: string;
}

async function geminiContactDiscovery(
    env: Env,
    name: string,
    company: string,
    title: string,
    companyDomain?: string,
): Promise<GeminiContactResult | null> {
    if (!env.GEMINI_API_KEY || name === 'Unknown') return null;

    console.log(`🤖 Layer 5b: Gemini contact discovery for ${name} at ${company}...`);

    try {
        const prompt = `You are a business intelligence researcher. Find ALL available contact information for:

Name: ${name}
Title: ${title}
Company: ${company}
${companyDomain ? `Known Company Domain: ${companyDomain}` : ''}

Search your knowledge for:
1. **Email**: Their professional email address. If you know it, provide it. If not, determine the company's email format and construct the most likely email.
2. **Phone**: Their direct phone number, office number, or mobile if publicly known.
3. **LinkedIn**: Their LinkedIn profile URL (format: linkedin.com/in/slug).
4. **Twitter/X**: Their Twitter handle or URL.
5. **Company Domain**: The company's primary website domain.

For each piece of information, rate confidence:
- HIGH: You have directly seen this information referenced in reliable sources
- MEDIUM: Strong inference from known patterns (e.g., company email format)
- LOW: Educated guess

Reply with ONLY a JSON object:
{
  "email": "email@domain.com or empty string",
  "emailConfidence": "HIGH|MEDIUM|LOW|NONE",
  "phone": "phone number or empty string",
  "phoneConfidence": "HIGH|MEDIUM|LOW|NONE",
  "linkedinUrl": "full linkedin URL or empty string",
  "twitterUrl": "full twitter/X URL or empty string",
  "companyDomain": "company.com",
  "reasoning": "brief explanation of sources/method"
}`;

        const res = await fetchWithTimeout(`${GEMINI_URL}?key=${env.GEMINI_API_KEY}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                contents: [{ parts: [{ text: prompt }] }],
                generationConfig: { temperature: 0.1, maxOutputTokens: 400 },
            }),
        });

        if (!res.ok) return null;

        const data = await res.json() as any;
        const text = data.candidates?.[0]?.content?.parts?.[0]?.text?.trim();
        if (!text) return null;

        const jsonMatch = text.match(/\{[\s\S]*\}/);
        if (!jsonMatch) return null;

        const parsed = JSON.parse(jsonMatch[0]);

        // Map Gemini confidence to our system (AI is always lower than verified data)
        const mapConfidence = (level: string) => {
            if (level === 'HIGH') return 'medium';   // Gemini "high" → our "medium"
            if (level === 'MEDIUM') return 'low';
            return 'low';
        };

        const result: GeminiContactResult = { reasoning: parsed.reasoning };

        if (parsed.email && parsed.email.includes('@') && parsed.emailConfidence !== 'NONE') {
            result.email = parsed.email.toLowerCase();
            result.emailConfidence = mapConfidence(parsed.emailConfidence);
        }
        if (parsed.phone && parsed.phoneConfidence !== 'NONE') {
            result.phone = parsed.phone;
            result.phoneConfidence = mapConfidence(parsed.phoneConfidence);
        }
        if (parsed.linkedinUrl && parsed.linkedinUrl.includes('linkedin.com')) {
            result.linkedinUrl = parsed.linkedinUrl;
        }
        if (parsed.twitterUrl) {
            result.twitterUrl = parsed.twitterUrl;
        }
        if (parsed.companyDomain) {
            result.companyDomain = parsed.companyDomain.replace(/^https?:\/\//, '').replace(/\/$/, '');
        }

        const found = [
            result.email ? `Email: ${result.email}` : null,
            result.phone ? `Phone: ${result.phone}` : null,
            result.linkedinUrl ? 'LinkedIn' : null,
        ].filter(Boolean);

        console.log(`🤖 Gemini found: ${found.length > 0 ? found.join(', ') : 'nothing new'} — ${parsed.reasoning || ''}`);

        return found.length > 0 || result.companyDomain ? result : null;
    } catch (err) {
        console.warn('⚠️ Gemini contact discovery failed:', err);
    }

    return null;
}

// ─── Step 6: Exa Deep Research ────────────────────────────────────────
async function exaDeepResearch(
    env: Env,
    name: string,
    company: string,
): Promise<{ text: string; url: string } | null> {
    if (!env.EXA_API_KEY || name.toLowerCase() === 'unknown') return null;

    console.log(`🕵️ Layer 6: Exa deep research: ${name} at ${company}...`);

    try {
        const res = await fetchWithTimeout('https://api.exa.ai/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': env.EXA_API_KEY,
            },
            body: JSON.stringify({
                query: `"${name}" "${company}" professional background career experience`,
                numResults: 1,
                useAutoprompt: true,
                contents: {
                    text: { maxCharacters: 1500 },
                    highlights: { numSentences: 3, highlightsPerUrl: 1 },
                },
            }),
        });

        if (res.ok) {
            const data = await res.json() as any;
            const top = data.results?.[0];
            if (top) {
                const text = top.text ?? top.highlights?.join(' ') ?? '';
                console.log(`✅ Exa research found: ${text.slice(0, 60)}...`);
                return { text, url: top.url ?? '' };
            }
        }
    } catch (err) {
        console.warn('⚠️ Exa research failed:', err);
    }

    return null;
}

// Helper to extract contact info from an Apollo person object
function extractContact(person: any, source: string): ContactInfo {
    return {
        name: `${person.first_name || ''} ${person.last_name || ''}`.trim(),
        title: person.title || 'Unknown',
        email: person.email || '',
        phone: person.phone_numbers?.[0]?.sanitized_number || person.phone_number?.sanitized_number || '',
        linkedinUrl: person.linkedin_url || '',
        seniority: person.seniority || '',
        source,
    };
}

/** Verify a LinkedIn URL actually exists (not 404). Returns the URL if valid, empty string if not. */
async function verifyLinkedInUrl(url: string): Promise<string> {
    if (!url || !url.includes('linkedin.com')) return url;
    try {
        const res = await fetchWithTimeout(url, { method: 'HEAD', redirect: 'follow' }, 4000);
        if (res.status === 404 || res.status === 999) {
            console.log(`⚠️ LinkedIn URL is invalid (${res.status}): ${url}`);
            return '';
        }
        return url;
    } catch {
        // Network error or timeout — keep the URL, it might still be valid
        return url;
    }
}

// ─── Main Enrichment Pipeline ─────────────────────────────────────────
export async function enrichLead(env: Env, input: EnrichInput): Promise<EnrichedLead> {
    console.log(`\n📊 ═══ MULTI-LAYER ENRICHMENT: ${input.company} ═══`);

    try {
        // STEP 1: Identify the executive (Gemini knowledge)
        const exec = await identifyExecutive(env, input.company, input.executiveName, input.executiveTitle);

        // Build lead with initial data
        const lead: EnrichedLead = buildFallback(input);
        lead.executiveName = exec.name;
        lead.executiveTitle = exec.title;

        let emailFound = false;
        const otherContacts: ContactInfo[] = [];
        let orgData: any = null;

        // STEP 2: Apollo People Match (exact)
        if (exec.name !== 'Unknown') {
            const matchResult = await apolloPeopleMatch(env, exec.name, input.company, exec.title);
            if (matchResult) {
                const contact = extractContact(matchResult.person, 'apollo_match');
                if (contact.email) {
                    lead.email = contact.email;
                    lead.emailSource = 'apollo_match';
                    lead.emailConfidence = 'high';
                    emailFound = true;
                }
                lead.phone = contact.phone || lead.phone;
                lead.linkedinUrl = contact.linkedinUrl || lead.linkedinUrl;
                lead.executiveTitle = matchResult.person.title || lead.executiveTitle;
                orgData = matchResult.org;
            }
        }

        // STEP 3: Apollo People Search (by company + seniority) — ALWAYS run for other contacts
        {
            const searchResult = await apolloPeopleSearch(env, input.company, exec.name);
            if (searchResult) {
                if (searchResult.primary) {
                    const primary = extractContact(searchResult.primary, 'apollo_search');
                    if (primary.email && !emailFound) {
                        lead.email = primary.email;
                        lead.emailSource = 'apollo_search';
                        lead.emailConfidence = 'high';
                        emailFound = true;
                        lead.executiveName = primary.name || lead.executiveName;
                        lead.executiveTitle = primary.title || lead.executiveTitle;
                    }
                    lead.phone = lead.phone || primary.phone;
                    lead.linkedinUrl = lead.linkedinUrl || primary.linkedinUrl;
                }
                // Collect other decision-makers
                for (const p of searchResult.others) {
                    otherContacts.push(extractContact(p, 'apollo_search'));
                }
                orgData = searchResult.org || orgData;
            }
        }

        // STEP 4: Apollo Org → Domain → People Search — ALWAYS run for company data + more contacts
        if (!lead.companyDomain || otherContacts.length === 0) {
            const orgResult = await apolloOrgAndPeopleSearch(env, input.company, exec.name);
            if (orgResult) {
                lead.companyDomain = orgResult.domain || lead.companyDomain;
                if (orgResult.primary) {
                    const primary = extractContact(orgResult.primary, 'apollo_org_search');
                    if (primary.email && !emailFound) {
                        lead.email = primary.email;
                        lead.emailSource = 'apollo_org_search';
                        lead.emailConfidence = 'high';
                        emailFound = true;
                        lead.executiveName = primary.name || lead.executiveName;
                        lead.executiveTitle = primary.title || lead.executiveTitle;
                    }
                    lead.phone = lead.phone || primary.phone;
                    lead.linkedinUrl = lead.linkedinUrl || primary.linkedinUrl;
                }
                for (const p of orgResult.others) {
                    // Avoid duplicate contacts
                    const exists = otherContacts.some(oc =>
                        oc.email === extractContact(p, '').email ||
                        oc.name === extractContact(p, '').name
                    );
                    if (!exists) {
                        otherContacts.push(extractContact(p, 'apollo_org_search'));
                    }
                }
                orgData = orgResult.org || orgData;
            }
        }

        // STEP 5: Email Pattern Generation (fallback)
        if (!emailFound && lead.companyDomain) {
            const patterns = generateEmailPatterns(lead.executiveName, lead.companyDomain);
            if (patterns.length > 0) {
                lead.patternEmails = patterns;
                // Don't set as primary email yet — let Gemini try first
                console.log(`📧 Layer 5: Generated ${patterns.length} email patterns`);
            }
        }

        // STEP 5b: Gemini AI Contact Discovery (fills in any missing data)
        if (!emailFound || !lead.phone || !lead.linkedinUrl) {
            const geminiResult = await geminiContactDiscovery(
                env, lead.executiveName, lead.company, lead.executiveTitle, lead.companyDomain
            );
            if (geminiResult) {
                if (geminiResult.email && !emailFound) {
                    lead.email = geminiResult.email;
                    lead.emailSource = 'gemini_inference';
                    lead.emailConfidence = geminiResult.emailConfidence || 'low';
                    emailFound = true;
                }
                if (geminiResult.phone && !lead.phone) {
                    lead.phone = geminiResult.phone;
                }
                if (geminiResult.linkedinUrl && !lead.linkedinUrl) {
                    lead.linkedinUrl = geminiResult.linkedinUrl;
                }
                if (geminiResult.companyDomain && !lead.companyDomain) {
                    lead.companyDomain = geminiResult.companyDomain;
                }
            }
        }

        // If still no email, fall back to pattern guess
        if (!emailFound && lead.patternEmails?.length) {
            lead.email = lead.patternEmails[0];
            lead.emailSource = 'pattern_guess';
            lead.emailConfidence = 'low';
            console.log(`📧 Using pattern email as last resort: ${lead.patternEmails[0]}`);
        }

        // Extract org data
        if (orgData) {
            lead.companyRevenue = orgData.annual_revenue_printed
                || (orgData.annual_revenue ? `$${(orgData.annual_revenue / 1_000_000).toFixed(0)}M` : lead.companyRevenue);
            lead.employeeCount = orgData.estimated_num_employees?.toLocaleString() || lead.employeeCount;
            lead.signals = orgData.keywords?.slice(0, 5) || lead.signals;
            lead.companyDomain = orgData.primary_domain || orgData.website_url || lead.companyDomain;
        }

        // Deduplicate other contacts (by email or name)
        const seen = new Set<string>();
        seen.add(lead.executiveName.toLowerCase());
        lead.otherContacts = otherContacts.filter(c => {
            const key = c.email || c.name.toLowerCase();
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });

        // STEP 6: Deep executive research (Exa)
        const research = await exaDeepResearch(env, lead.executiveName, lead.company);
        if (research) {
            lead.executiveResearch = research.text;
            lead.executiveResearchUrl = research.url;
        }

        // STEP 7: Verify LinkedIn URLs (filter out 404s)
        if (lead.linkedinUrl) {
            lead.linkedinUrl = await verifyLinkedInUrl(lead.linkedinUrl);
        }
        if (lead.otherContacts?.length) {
            for (const c of lead.otherContacts) {
                if (c.linkedinUrl) {
                    c.linkedinUrl = await verifyLinkedInUrl(c.linkedinUrl);
                }
            }
        }

        // ─── Summary ──────────────────────────────────────────────
        console.log(`\n✅ ═══ ENRICHMENT COMPLETE ═══`);
        console.log(`   Name:     ${lead.executiveName} (${lead.executiveTitle})`);
        console.log(`   Company:  ${lead.company} | Domain: ${lead.companyDomain || 'N/A'}`);
        console.log(`   Email:    ${lead.email || '❌ NOT FOUND'} [${lead.emailSource || 'none'}] (${lead.emailConfidence || 'none'})`);
        console.log(`   Phone:    ${lead.phone || 'N/A'}`);
        console.log(`   LinkedIn: ${lead.linkedinUrl ? 'Yes' : 'No'}`);
        console.log(`   Others:   ${lead.otherContacts?.length || 0} additional contacts found`);
        if (lead.patternEmails?.length) {
            console.log(`   Patterns: ${lead.patternEmails.join(', ')}`);
        }

        // Write enrichment data back to Supabase
        if (input.leadId) {
            const writeResult = await patchRow(env, 'lead_targets', {
                executive_name: lead.executiveName,
                executive_title: lead.executiveTitle,
                enrichment_data: {
                    revenue: lead.companyRevenue,
                    employees: lead.employeeCount,
                    company_domain: lead.companyDomain,
                    signals: lead.signals,
                    linkedin_url: lead.linkedinUrl,
                    email: lead.email || '',
                    email_source: lead.emailSource || '',
                    email_confidence: lead.emailConfidence || 'none',
                    pattern_emails: lead.patternEmails || [],
                    phone: lead.phone || '',
                    other_contacts: lead.otherContacts || [],
                    executive_research: lead.executiveResearch,
                    executive_research_url: lead.executiveResearchUrl,
                    sources: [
                        ...(lead.linkedinUrl ? [{ label: 'LinkedIn Profile', url: lead.linkedinUrl }] : []),
                        ...(lead.executiveResearchUrl ? [{ label: 'Executive Research', url: lead.executiveResearchUrl }] : []),
                    ],
                    source: 'gemini+apollo+exa',
                    enriched_at: new Date().toISOString(),
                    discovery_layers_used: [
                        'gemini_id',
                        ...(lead.emailSource === 'apollo_match' ? ['apollo_match'] : []),
                        ...(lead.emailSource === 'apollo_search' ? ['apollo_search'] : []),
                        ...(lead.emailSource === 'apollo_org_search' ? ['apollo_org_search'] : []),
                        ...(lead.emailSource === 'pattern_guess' ? ['pattern_guess'] : []),
                        'exa_research',
                    ],
                },
            }, 'id', input.leadId);

            if (writeResult.ok) {
                console.log(`💾 Enrichment written to lead_targets (id: ${input.leadId})`);
            }
        }

        return lead;
    } catch (err) {
        console.error('❌ Enrichment pipeline failed, returning fallback:', err);
        return buildFallback(input);
    }
}
