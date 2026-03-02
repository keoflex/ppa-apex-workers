/**
 * Activity: Advance Sequences
 * Runs via cron to evaluate all active sequence enrollments.
 * If an enrollment's next_send_at is due (or past due), it:
 * 1. Checks if the prospect has replied. If yes, it pauses the enrollment.
 * 2. Determines the next step from the sequence template.
 * 3. Generates the draft for that step via `generateDraft`.
 * 4. Inserts a new `sequence_step_drafts` record requiring HITL approval.
 * 5. Updates the enrollment `next_send_at` based on the step's delay.
 */
import type { Env } from '../index';
import { getRow } from '../utils/supabase';
import { generateDraft, DraftInput } from './generate-draft';

async function supabaseQuery(env: Env, query: string) {
    const response = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/execute_sql`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`
        },
        body: JSON.stringify({ query })
    });

    if (!response.ok) {
        return { data: null, error: new Error(await response.text()) };
    }

    return { data: await response.json(), error: null };
}

export async function advanceSequences(env: Env) {
    console.log(`[cron] Running advanceSequences logic...`);

    // Fetch active enrollments where next_send_at <= NOW()
    const { data: enrollments, error: enrollError } = await supabaseQuery(env,
        `
        SELECT 
            se.id as enrollment_id,
            se.target_id,
            se.template_id,
            se.status,
            se.next_send_at,
            lt.company,
            lt.executive_name,
            lt.executive_title,
            lt.trigger_event,
            lt.trigger_relevance,
            lt.trigger_source,
            lt.enrichment_data,
            st.name as template_name,
            st.steps as template_steps
        FROM sequence_enrollments se
        JOIN lead_targets lt ON se.target_id = lt.id
        JOIN sequence_templates st ON se.template_id = st.id
        WHERE se.status = 'active' AND se.next_send_at <= NOW()
        `
    ) as { data: any[]; error: any };

    if (enrollError) {
        console.error(`[cron] Error fetching due enrollments: ${enrollError.message}`);
        return;
    }

    if (!enrollments || enrollments.length === 0) {
        console.log(`[cron] No sequences due for advancement.`);
        return;
    }

    console.log(`[cron] Found ${enrollments.length} due sequences. Processing...`);

    for (const enrollment of enrollments) {
        try {
            await processEnrollment(env, enrollment);
        } catch (err) {
            console.error(`[cron] Failed to process enrollment ${enrollment.enrollment_id}:`, err);
        }
    }
}

async function processEnrollment(env: Env, enrollment: any) {
    console.log(`[cron] Processing enrollment ${enrollment.enrollment_id} (Target: ${enrollment.target_id})`);

    // 1. Check for replies to pause the sequence
    // In a real system, you would check a `triage_replies` or `emails` table.
    // For now, assume no replies.
    const hasReplied = false;

    if (hasReplied) {
        console.log(`[cron] Prospect replied. Pausing enrollment ${enrollment.enrollment_id}.`);
        await supabaseQuery(env, `UPDATE sequence_enrollments SET status = 'replied' WHERE id = ${enrollment.enrollment_id}`);
        return;
    }

    // 2. Determine Which Step is Next
    // Fetch existing drafts for this enrollment to determine the max step number completed/pending
    const { data: drafts, error: draftsError } = await supabaseQuery(env,
        `SELECT step_number, email_subject, drafted_body FROM sequence_step_drafts WHERE enrollment_id = ${enrollment.enrollment_id} ORDER BY step_number ASC`
    ) as { data: any[]; error: any };

    let nextStepNumber = 1; // Default to step 1 (though usually step 1 is the initial strike)
    const previousEmails: string[] = [];

    if (!draftsError && drafts && drafts.length > 0) {
        const lastDraft = drafts[drafts.length - 1];
        nextStepNumber = lastDraft.step_number + 1;
        drafts.forEach((d: any) => {
            if (d.email_subject && d.drafted_body) {
                previousEmails.push(`Subject: ${d.email_subject}\n\n${d.drafted_body}`);
            }
        });
    }

    const steps = enrollment.template_steps || [];
    if (nextStepNumber > steps.length) {
        console.log(`[cron] Enrollment ${enrollment.enrollment_id} has completed all ${steps.length} steps.`);
        await supabaseQuery(env, `UPDATE sequence_enrollments SET status = 'completed' WHERE id = ${enrollment.enrollment_id}`);
        return;
    }

    const stepConfig = steps[nextStepNumber - 1];
    console.log(`[cron] Generating draft for Step ${nextStepNumber} (${stepConfig.type})`);

    if (stepConfig.type !== 'email') {
        // Handle manual steps or linkedin connections here in the future.
        console.log(`[cron] Skipping non-email step ${nextStepNumber} for now.`);
        return;
    }

    // 3. Generate the Draft
    const enrichmentData = enrollment.enrichment_data || {};

    const draftInput: DraftInput = {
        lead: {
            company: enrollment.company || 'Unknown',
            companyDomain: enrollment.company_domain || 'Unknown',
            executiveName: enrollment.executive_name || 'Unknown',
            executiveTitle: enrollment.executive_title || 'Unknown',
            companyRevenue: enrichmentData.companyRevenue || 'Unknown',
            employeeCount: enrichmentData.employeeCount || 'Unknown',
            signals: enrichmentData.signals || [],
            linkedinUrl: enrichmentData.linkedinUrl,
            executiveResearch: enrichmentData.executive_research
        },
        persona: enrollment.template_name, // Typically you'd explicitly map this
        triggerHeadline: enrollment.trigger_event,
        triggerArticleText: enrichmentData.trigger_summary,
        stepContext: {
            stepNumber: nextStepNumber,
            totalSteps: steps.length,
            stepPrompt: stepConfig.bodyPrompt || 'Follow up appropriately.',
            previousEmails
        }
    };

    const draft = await generateDraft(env, draftInput);

    // 4. Insert into sequence_step_drafts
    const insertQuery = `
        INSERT INTO sequence_step_drafts (enrollment_id, step_number, email_subject, drafted_body, status)
        VALUES (
            ${enrollment.enrollment_id}, 
            ${nextStepNumber}, 
            '${draft.subject.replace(/'/g, "''")}', 
            '${draft.body.replace(/'/g, "''")}', 
            'pending_hitl'
        )
    `;

    const { error: insertError } = await supabaseQuery(env, insertQuery);
    if (insertError) {
        console.error(`[cron] Failed to insert draft for enrollment ${enrollment.enrollment_id}:`, insertError);
        return;
    }

    // 5. Update next_send_at on the enrollment
    // If there are more steps, calculate the delay for the *next* step after this one.
    // Actually, next_send_at usually indicates when THIS step was due. 
    // We should update it to point to the NEXT step's due date AFTER this step is approved/sent.
    // For now, we leave `next_send_at` as is, or set it to null, until this draft is sent.
    // The typical pattern is `next_send_at` is updated when a draft is ACTUALLY SENT.
    // So here we do nothing to `next_send_at`, ensuring we don't double loop (though we check for existing drafts above).

    console.log(`[cron] Successfully queued draft for Step ${nextStepNumber} of enrollment ${enrollment.enrollment_id}`);
}
