/**
 * PPA+ APEX — HITL Gate Durable Object
 *
 * Holds the Human-in-the-Loop approval state for a strike campaign.
 * Replaces Temporal's `workflow.wait_condition()` pattern.
 *
 * Lifecycle:
 *   1. Created when a draft is ready (status = pending_hitl)
 *   2. Holds state indefinitely until approval
 *   3. On approval, triggers campaign delivery
 *   4. Self-destructs after completion
 */
import type { Env } from '../index';

interface GateState {
    campaignId: number;
    workflowId: string;
    persona: string;
    status: 'pending_hitl' | 'approved' | 'sent';
    draftSubject: string;
    draftBody: string;
    createdAt: string;
    approvedAt?: string;
}

export class HitlGateDurableObject {
    private state: DurableObjectState;
    private env: Env;

    constructor(state: DurableObjectState, env: Env) {
        this.state = state;
        this.env = env;
    }

    async fetch(request: Request): Promise<Response> {
        const url = new URL(request.url);

        // ── POST /init — Initialize gate for a new campaign
        if (url.pathname === '/init' && request.method === 'POST') {
            const body = await request.json() as Omit<GateState, 'status' | 'createdAt'>;

            const gate: GateState = {
                ...body,
                status: 'pending_hitl',
                createdAt: new Date().toISOString(),
            };

            await this.state.storage.put('gate', gate);
            console.log(`🔒 HITL Gate initialized | Campaign #${gate.campaignId} | Workflow: ${gate.workflowId}`);

            return Response.json({ status: 'gate_created', gate });
        }

        // ── POST /approve — Unlock the gate
        if (url.pathname === '/approve' && request.method === 'POST') {
            const gate = await this.state.storage.get<GateState>('gate');
            if (!gate) {
                return Response.json({ error: 'No active gate found' }, { status: 404 });
            }

            const body = await request.json() as {
                editedSubject?: string;
                editedBody?: string;
            };

            gate.status = 'approved';
            gate.approvedAt = new Date().toISOString();
            if (body.editedSubject) gate.draftSubject = body.editedSubject;
            if (body.editedBody) gate.draftBody = body.editedBody;

            await this.state.storage.put('gate', gate);
            console.log(`✅ HITL Gate APPROVED | Campaign #${gate.campaignId}`);

            // In production: trigger delivery via Queue or direct API call
            // await this.env.STRIKE_QUEUE?.send({ campaignId: gate.campaignId, action: 'deliver' });

            return Response.json({ status: 'approved', gate });
        }

        // ── GET /status — Check gate state
        if (url.pathname === '/status' && request.method === 'GET') {
            const gate = await this.state.storage.get<GateState>('gate');
            if (!gate) {
                return Response.json({ status: 'no_gate' });
            }
            return Response.json(gate);
        }

        return Response.json({ error: 'Unknown action' }, { status: 400 });
    }
}
