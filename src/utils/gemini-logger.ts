import type { Env } from '../index';
import { insertRow } from './supabase';

/**
 * Logs a Gemini API failure to the `gemini_error_logs` table.
 * Automatically purges any logs older than 5 days using the PostgREST API.
 */
export async function logGeminiError(
    env: Env,
    modelUsed: string,
    activityName: string,
    err: unknown,
    contextPreview?: any
): Promise<void> {
    try {
        const errorMsg = err instanceof Error ? err.message : String(err);
        
        // 1. Insert the new error log
        await insertRow(env, 'gemini_error_logs', {
            model: modelUsed,
            activity: activityName,
            error_message: errorMsg,
            payload_preview: contextPreview || {}
        });

        // Archival of logs older than 5 days is now handled securely by the Worker Cron.
    } catch (loggingErr) {
        console.error('Failed to log Gemini error:', loggingErr);
    }
}
