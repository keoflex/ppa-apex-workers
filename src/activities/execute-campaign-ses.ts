/**
 * Activity: Execute Campaign via Amazon SES
 * Sends approved email payload directly via AWS SES v2 API.
 * Uses native fetch + AWS Signature v4 — no Node.js built-ins, compatible with Cloudflare Workers.
 * Falls back to a safe error result if the SES call fails.
 *
 * Write-back: PATCHes `strike_campaigns` → status='sent', sent_at=NOW(), ses_message_id.
 */
import type { Env } from '../index';
import { patchRow } from '../utils/supabase';
import { signAwsRequest, type AwsCredentials } from '../utils/aws-signer';

export interface SesExecuteInput {
    campaignId: number;  // BIGSERIAL
    workflowId: string;
    emailSubject: string;
    emailBody: string;
    recipientEmail: string;
    senderEmail: string;      // The rotated sender address (e.g. fred@ppalink.com)
    senderName?: string;       // Display name (e.g. "Fred Posinelli")
    replyToEmail?: string;     // Optional reply-to override
}

export interface SesCampaignResult {
    campaignId: number;
    sesMessageId: string;
    status: string;
    scheduledAt: string;
    error: string | null;
}

export async function executeCampaignSes(env: Env, input: SesExecuteInput): Promise<SesCampaignResult> {
    console.log(`🚀 Executing SES campaign | Campaign #${input.campaignId} | Workflow: ${input.workflowId} | Sender: ${input.senderEmail}`);

    try {
        const credentials: AwsCredentials = {
            accessKeyId: env.AWS_ACCESS_KEY_ID,
            secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
            region: env.AWS_REGION || 'us-east-1',
        };

        const region = credentials.region;
        const sesUrl = `https://email.${region}.amazonaws.com/v2/email/outbound-emails`;

        // Build SES v2 SendEmail request body
        const senderDisplay = input.senderName
            ? `${input.senderName} <${input.senderEmail}>`
            : input.senderEmail;

        const requestBody = JSON.stringify({
            Content: {
                Simple: {
                    Subject: {
                        Data: input.emailSubject,
                        Charset: 'UTF-8',
                    },
                    Body: {
                        Html: {
                            Data: input.emailBody,
                            Charset: 'UTF-8',
                        },
                        Text: {
                            Data: input.emailBody.replace(/<[^>]*>/g, ''), // Strip HTML for plaintext version
                            Charset: 'UTF-8',
                        },
                    },
                    Headers: [
                        {
                            Name: 'List-Unsubscribe',
                            Value: `<https://ppa-apex-platform.fred-78e.workers.dev/api/v1/unsubscribe?email=${encodeURIComponent(input.recipientEmail)}&c=${input.campaignId}>, <mailto:unsubscribe@${input.senderEmail.split('@')[1]}?subject=Unsubscribe>`,
                        },
                        {
                            Name: 'List-Unsubscribe-Post',
                            Value: 'List-Unsubscribe=One-Click',
                        },
                    ],
                },
            },
            Destination: {
                ToAddresses: input.recipientEmail.split(',').map(e => e.trim()).filter(Boolean),
            },
            FromEmailAddress: senderDisplay,
            ReplyToAddresses: [input.replyToEmail || input.senderEmail],
            // Custom headers for reply threading
            EmailTags: [
                { Name: 'campaign_id', Value: String(input.campaignId) },
                { Name: 'workflow_id', Value: input.workflowId },
            ],
            // Enable SES configuration set for event tracking (bounces, opens, clicks)
            ...(env.SES_CONFIGURATION_SET ? { ConfigurationSetName: env.SES_CONFIGURATION_SET } : {}),
        });

        const headers: Record<string, string> = {
            'Content-Type': 'application/json',
        };

        const signedHeaders = await signAwsRequest(
            'POST',
            sesUrl,
            headers,
            requestBody,
            credentials,
            'ses'
        );

        const response = await fetch(sesUrl, {
            method: 'POST',
            headers: signedHeaders,
            body: requestBody,
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`SES API error ${response.status}: ${errText}`);
        }

        const data = await response.json() as {
            MessageId?: string;
        };

        const sesMessageId = data.MessageId || `ses-${crypto.randomUUID().slice(0, 12)}`;
        const now = new Date().toISOString();

        const result: SesCampaignResult = {
            campaignId: input.campaignId,
            sesMessageId,
            status: 'sent',
            scheduledAt: now,
            error: null,
        };

        console.log(`✅ Campaign delivered via SES: ${sesMessageId} → ${input.recipientEmail}`);

        // Write-back: mark campaign as sent in Supabase
        if (input.campaignId > 0) {
            const writeResult = await patchRow(env, 'strike_campaigns', {
                status: 'sent',
                sent_at: now,
                ses_message_id: sesMessageId,
                sender_email: input.senderEmail,
            }, 'id', input.campaignId);

            if (writeResult.ok) {
                console.log(`💾 Campaign status updated to 'sent' (id: ${input.campaignId})`);
            }
        }

        return result;
    } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        console.error(`❌ SES execution failed: ${errorMsg}`);

        // Mark campaign as failed in Supabase
        if (input.campaignId > 0) {
            await patchRow(env, 'strike_campaigns', {
                status: 'failed',
                failure_reason: errorMsg,
            }, 'id', input.campaignId);
        }

        return {
            campaignId: input.campaignId,
            sesMessageId: '',
            status: 'failed',
            scheduledAt: new Date().toISOString(),
            error: errorMsg,
        };
    }
}
