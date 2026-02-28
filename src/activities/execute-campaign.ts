/**
 * Activity: Execute Smartlead Campaign
 * Pushes approved email payload to Smartlead for delivery via REST API.
 * Uses native fetch — no Node.js built-ins, compatible with Cloudflare Workers.
 * Falls back to a safe error result if the Smartlead call fails.
 *
 * Write-back: PATCHes `strike_campaigns` → status='sent', sent_at=NOW().
 */
import type { Env } from '../index';
import { patchRow } from '../utils/supabase';

export interface ExecuteInput {
    campaignId: number;  // BIGSERIAL
    workflowId: string;
    emailSubject?: string;
    emailBody?: string;
    recipientEmail?: string;
}

export interface CampaignResult {
    campaignId: number;
    smartleadId: string;
    status: string;
    scheduledAt: string;
    error: string | null;
}

const SMARTLEAD_API_BASE = 'https://server.smartlead.ai/api/v1';

export async function executeCampaign(env: Env, input: ExecuteInput): Promise<CampaignResult> {
    console.log(`🚀 Executing Smartlead campaign | Campaign #${input.campaignId} | Workflow: ${input.workflowId}`);

    try {
        // Build the Smartlead campaign payload
        const payload = {
            name: `APEX Strike — ${input.workflowId}`,
            ...(input.emailSubject ? { subject: input.emailSubject } : {}),
            ...(input.emailBody ? { body: input.emailBody } : {}),
            ...(input.recipientEmail ? {
                leads: [{ email: input.recipientEmail }],
            } : {}),
        };

        const response = await fetch(`${SMARTLEAD_API_BASE}/campaigns/?api_key=${env.SMARTLEAD_API_KEY}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Smartlead API error ${response.status}: ${errText}`);
        }

        const data = await response.json() as {
            id?: number;
            name?: string;
            status?: string;
        };

        const smartleadId = data.id ? `sl-${data.id}` : `sl-${crypto.randomUUID().slice(0, 12)}`;
        const now = new Date().toISOString();

        const result: CampaignResult = {
            campaignId: input.campaignId,
            smartleadId,
            status: 'sent',
            scheduledAt: now,
            error: null,
        };

        console.log(`✅ Campaign delivered via Smartlead: ${result.smartleadId}`);

        // Write-back: mark campaign as sent in Supabase
        if (input.campaignId > 0) {
            const writeResult = await patchRow(env, 'strike_campaigns', {
                status: 'sent',
                sent_at: now,
            }, 'id', input.campaignId);

            if (writeResult.ok) {
                console.log(`💾 Campaign status updated to 'sent' (id: ${input.campaignId})`);
            }
        }

        return result;
    } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        console.error(`❌ Smartlead execution failed: ${errorMsg}`);

        // Mark campaign as failed in Supabase
        if (input.campaignId > 0) {
            await patchRow(env, 'strike_campaigns', {
                status: 'failed',
            }, 'id', input.campaignId);
        }

        return {
            campaignId: input.campaignId,
            smartleadId: '',
            status: 'failed',
            scheduledAt: new Date().toISOString(),
            error: errorMsg,
        };
    }
}
