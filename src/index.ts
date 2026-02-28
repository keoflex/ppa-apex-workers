/**
 * PPA+ APEX — Strike Engine Worker (Entry Point)
 *
 * Cloudflare Worker handling:
 * 1. HTTP requests (manual triggers, health checks, webhook receivers)
 * 2. Queue consumption (strike pipeline processing)
 * 3. Durable Object exports (HITL gate)
 */
import { HitlGateDurableObject } from './durable-objects/hitl-gate';
import { senseTriggers } from './activities/sense-triggers';
import { enrichLead } from './activities/enrich-lead';
import { generateDraft } from './activities/generate-draft';
import { executeCampaign } from './activities/execute-campaign';
import { triageReply } from './activities/triage-reply';
import { insertRow, patchRow } from './utils/supabase';

export { HitlGateDurableObject };

export interface Env {
    HITL_GATE: DurableObjectNamespace;
    STRIKE_QUEUE: Queue;
    SUPABASE_URL: string;
    SUPABASE_SERVICE_ROLE_KEY: string;
    GEMINI_API_KEY: string;
    EXA_API_KEY: string;
    APOLLO_API_KEY: string;
    SMARTLEAD_API_KEY: string;
    WORKER_SECRET: string;
    ENVIRONMENT: string;
}

export default {
    /**
     * HTTP handler — manual triggers and health checks.
     */
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);

        // Health check
        if (url.pathname === '/health') {
            return Response.json({
                status: 'operational',
                service: 'PPA+ APEX Strike Engine',
                version: '1.0.0',
                durableObjects: true,
            });
        }

        // ── Shared-secret guard for protected routes ──
        const protectedPaths = ['/api/trigger-strike', '/api/execute'];
        if (protectedPaths.includes(url.pathname) && request.method === 'POST') {
            const secret = request.headers.get('x-worker-secret');
            if (!secret || secret !== env.WORKER_SECRET) {
                return Response.json(
                    { error: 'Unauthorized — invalid or missing x-worker-secret header' },
                    { status: 401 },
                );
            }
        }

        // Manual strike trigger → push to Queue for async processing
        if (url.pathname === '/api/trigger-strike' && request.method === 'POST') {
            try {
                const body = await request.json() as { campaignId: number; persona: string };

                if (env.STRIKE_QUEUE) {
                    await env.STRIKE_QUEUE.send({
                        campaignId: body.campaignId,
                        persona: body.persona || "Rob O'Neill",
                        source: 'manual',
                    });
                    return Response.json(
                        { status: 'queued', campaignId: body.campaignId },
                        { status: 202 },
                    );
                }

                // Fallback: run inline if Queue not available (local dev)
                console.log(`🚀 Manual strike trigger (inline) | Campaign #${body.campaignId}`);
                const triggers = await senseTriggers(env);
                if (triggers.length === 0) {
                    return Response.json({ error: 'No triggers detected' }, { status: 404 });
                }

                const selectedTrigger = triggers[0];
                const enrichedLead = await enrichLead(env, {
                    company: selectedTrigger.company,
                    executiveName: selectedTrigger.executiveName,
                    executiveTitle: selectedTrigger.executiveTitle,
                });

                const draft = await generateDraft(env, {
                    lead: enrichedLead,
                    persona: body.persona || "Rob O'Neill",
                    triggerHeadline: selectedTrigger.headline,
                });

                return Response.json({
                    status: 'draft_ready',
                    trigger: selectedTrigger,
                    lead: enrichedLead,
                    draft,
                });
            } catch (error) {
                console.error('Strike trigger error:', error);
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Execute delivery (called by Next.js approve endpoint)
        if (url.pathname === '/api/execute' && request.method === 'POST') {
            try {
                const body = await request.json() as { workflowId: string; action: string };
                const result = await executeCampaign(env, {
                    campaignId: 0,
                    workflowId: body.workflowId,
                });
                return Response.json({ status: 'delivered', result });
            } catch (error) {
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Triage a reply (for testing)
        if (url.pathname === '/api/triage' && request.method === 'POST') {
            try {
                const body = await request.json() as {
                    senderName: string;
                    senderCompany: string;
                    subject: string;
                    body: string;
                };
                const result = await triageReply(env, body);
                return Response.json(result);
            } catch (error) {
                return Response.json({ error: String(error) }, { status: 500 });
            }
        }

        // Smartlead webhook — inbound reply receiver
        if (url.pathname === '/api/webhook/smartlead' && request.method === 'POST') {
            // Parse the payload synchronously
            const body = await request.json() as {
                campaign_id?: number | string;
                from_email?: string;
                from_name?: string;
                from_company?: string;
                subject?: string;
                text_body?: string;
                html_body?: string;
            };

            // Parse campaign_id as BIGSERIAL number
            const campaignId = typeof body.campaign_id === 'string'
                ? parseInt(body.campaign_id, 10)
                : body.campaign_id ?? 0;

            const senderName = body.from_name || body.from_email || 'Unknown';
            const senderCompany = body.from_company || 'Unknown';
            const subject = body.subject || '(no subject)';
            const replyBody = body.text_body || body.html_body || '';

            // Return 200 IMMEDIATELY — defer processing via waitUntil
            ctx.waitUntil(
                (async () => {
                    try {
                        const triageResult = await triageReply(env, {
                            senderName,
                            senderCompany,
                            subject,
                            body: replyBody,
                        });

                        // Write to triage_replies table
                        const insertResult = await insertRow(env, 'triage_replies', {
                            campaign_id: campaignId > 0 ? campaignId : null,
                            sender_name: senderName,
                            sender_company: senderCompany,
                            subject,
                            body: replyBody,
                            category: triageResult.category,
                            confidence: triageResult.confidence,
                            preview: replyBody.slice(0, 200),
                        });

                        if (insertResult.ok) {
                            console.log(`✅ Webhook processed: ${senderName} → ${triageResult.category}`);
                        }

                        // Mark campaign as 'replied'
                        if (campaignId > 0) {
                            const { patchRow } = await import('./utils/supabase');
                            await patchRow(env, 'strike_campaigns', { status: 'replied' }, 'id', campaignId);
                        }
                    } catch (err) {
                        console.error('❌ Webhook background processing error:', err);
                    }
                })()
            );

            // Smartlead gets 200 instantly — no timeout risk
            return Response.json({ status: 'accepted' });
        }

        return Response.json({ error: 'Not found' }, { status: 404 });
    },

    /**
     * Queue handler — processes strike pipeline messages asynchronously.
     * This prevents HTTP request timeouts when AI APIs take 15+ seconds.
     */
    async queue(batch: MessageBatch<{ campaignId: number; persona: string; source?: string }>, env: Env): Promise<void> {
        for (const msg of batch.messages) {
            try {
                const { campaignId, persona } = msg.body;
                console.log(`📨 Queue processing: Campaign #${campaignId} | Persona: ${persona}`);

                // Step 1: Sense market triggers
                const triggers = await senseTriggers(env);
                if (triggers.length === 0) {
                    console.log(`⚠️ No triggers found for Campaign #${campaignId}`);
                    msg.ack();
                    continue;
                }

                const selectedTrigger = triggers[0];

                // Step 2: Enrich lead via Apollo
                const enrichedLead = await enrichLead(env, {
                    company: selectedTrigger.company,
                    executiveName: selectedTrigger.executiveName,
                    executiveTitle: selectedTrigger.executiveTitle,
                });

                // Step 3: Generate personalized email draft via Gemini
                const draft = await generateDraft(env, {
                    lead: enrichedLead,
                    persona: persona || "Rob O'Neill",
                    triggerHeadline: selectedTrigger.headline,
                });

                // Step 4: Save draft to Supabase and set status to pending_hitl
                if (campaignId > 0) {
                    await patchRow(env, 'strike_campaigns', {
                        status: 'pending_hitl',
                        persona_used: persona,
                        email_subject: draft.subject,
                        drafted_body: draft.body,
                    }, 'id', campaignId);
                }

                console.log(`✅ Draft ready for Campaign #${campaignId}: "${draft.subject}"`);
                msg.ack();
            } catch (error) {
                console.error(`❌ Queue processing error for Campaign #${msg.body.campaignId}:`, error);
                msg.retry();
            }
        }
    },

    /**
     * Scheduled handler — cron-triggered market sensing.
     * Runs at 9 AM EST (14:00 UTC) Mon-Fri.
     */
    async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
        console.log(`⏰ Cron triggered: ${new Date(event.scheduledTime).toISOString()}`);

        try {
            // Sense market triggers
            const triggers = await senseTriggers(env);
            console.log(`📡 Cron found ${triggers.length} market triggers`);

            // Queue each trigger for processing
            for (const trigger of triggers.slice(0, 5)) {
                // Create a lead_target entry
                const insertResult = await insertRow(env, 'lead_targets', {
                    company: trigger.company,
                    executive_name: trigger.executiveName,
                    executive_title: trigger.executiveTitle,
                    trigger_event: trigger.headline,
                });

                if (insertResult.ok && env.STRIKE_QUEUE) {
                    // Create a campaign for this lead
                    const campaignResult = await insertRow(env, 'strike_campaigns', {
                        target_id: null, // Will be linked after insert
                        persona_used: "Rob O'Neill",
                        status: 'sensing',
                    });

                    if (campaignResult.ok) {
                        console.log(`📤 Queued auto-campaign for ${trigger.company}`);
                    }
                }
            }

            console.log(`✅ Cron complete: ${triggers.length} triggers processed`);
        } catch (error) {
            console.error('❌ Cron error:', error);
        }
    },
};
