import { createClient } from '@supabase/supabase-js';
import Exa from 'exa-js';
import { GoogleGenAI } from '@google/genai';
import { Env } from '../index';

export async function generateTerritoryBriefings(env: Env) {
    console.log('[generate-briefing] Starting territory briefing generation...');

    const supabaseUrl = env.SUPABASE_URL;
    const supabaseKey = env.SUPABASE_SERVICE_ROLE_KEY;

    if (!supabaseUrl || !supabaseKey) {
        console.error('[generate-briefing] Missing Supabase config');
        return;
    }

    const supabase = createClient(supabaseUrl, supabaseKey);
    const exa = new Exa(env.EXA_API_KEY);
    const ai = new GoogleGenAI({ apiKey: env.GEMINI_API_KEY });

    // 1. Fetch active territories
    const { data: territories, error: tErr } = await supabase
        .from('territories')
        .select('*')
        .eq('is_active', true);

    if (tErr || !territories) {
        console.error('[generate-briefing] Failed to fetch territories:', tErr);
        return { success: false, error: tErr };
    }

    let briefingsGenerated = 0;

    for (const territory of territories) {
        console.log(`[generate-briefing] Processing territory: ${territory.name}`);

        // 2. Fetch saved searches for this territory
        const { data: searches, error: sErr } = await supabase
            .from('saved_searches')
            .select('*')
            .eq('territory_id', territory.id);

        if (sErr || !searches || searches.length === 0) {
            console.log(`[generate-briefing] No active searches for ${territory.name}, skipping.`);
            continue;
        }

        let compiledResults = '';
        let totalHits = 0;

        for (const search of searches) {
            console.log(`   Running search: ${search.name}`);
            try {
                // Execute Exa search
                const results = await exa.searchAndContents(search.exa_query, {
                    numResults: 5,
                    useAutoproprompt: true,
                    text: true
                });

                if (results.results.length > 0) {
                    totalHits += results.results.length;
                    compiledResults += `\n### Search Topic: ${search.name}\n`;
                    for (const r of results.results) {
                        compiledResults += `- **${r.title || 'Unknown Webpage'}** (${r.url}): ${r.text?.substring(0, 300)}...\n`;
                    }
                }

                // Update last run time and counts
                await supabase
                    .from('saved_searches')
                    .update({ last_run_at: new Date().toISOString(), results_count: search.results_count + results.results.length })
                    .eq('id', search.id);

            } catch (err) {
                console.error(`   Exa search failed for ${search.name}:`, err);
            }
        }

        if (totalHits > 0) {
            // 3. Summarize with Gemini
            try {
                const prompt = `You are a private equity intelligence analyst. Summarize the following web search results into a concise, professional, daily briefing for a territory manager in charge of ${territory.name}.

Focus purely on:
1. Significant M&A, funding, or executive changes.
2. Market trends discovered in these results.
3. specific companies mentioned that might be good targets.

Format as a clean Markdown briefing.

Raw Search Results:
${compiledResults}`;

                const response = await ai.models.generateContent({
                    model: 'gemini-2.5-flash',
                    contents: prompt,
                });

                const summaryMd = response.text;

                // 4. Save to territory_briefings
                await supabase
                    .from('territory_briefings')
                    .insert({
                        territory_id: territory.id,
                        title: `${territory.name} Daily Briefing - ${new Date().toLocaleDateString()}`,
                        content_md: summaryMd || 'Briefing generated, but summary was empty.'
                    });

                briefingsGenerated++;
                console.log(`   Briefing saved for ${territory.name}`);

            } catch (aiErr) {
                console.error(`   Gemini summarization failed for ${territory.name}:`, aiErr);
            }
        } else {
            console.log(`   No new hits found for ${territory.name}.`);
        }
    }

    console.log(`[generate-briefing] Complete. ${briefingsGenerated} briefings generated.`);
    return { success: true, generated: briefingsGenerated };
}
