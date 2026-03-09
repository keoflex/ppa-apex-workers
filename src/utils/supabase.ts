/**
 * Supabase REST Utility
 * Lightweight native-fetch wrapper for Cloudflare Workers edge.
 * No @supabase/supabase-js — zero Node dependencies.
 *
 * Uses PostgREST API format:
 *   Base URL: {SUPABASE_URL}/rest/v1/{table}
 *   Auth: apikey + Bearer token via SUPABASE_SERVICE_ROLE_KEY
 */

interface SupabaseEnv {
    SUPABASE_URL: string;
    SUPABASE_SERVICE_ROLE_KEY: string;
}

function headers(env: SupabaseEnv): Record<string, string> {
    return {
        'apikey': env.SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal',
    };
}

/**
 * PATCH a row in a Supabase table by column match.
 * Example: patchRow(env, 'strike_campaigns', { status: 'sent' }, 'id', 42)
 */
export async function patchRow(
    env: SupabaseEnv,
    table: string,
    data: Record<string, unknown>,
    filterColumn: string,
    filterValue: string | number,
): Promise<{ ok: boolean; error: string | null }> {
    const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filterColumn}=eq.${filterValue}`;
    try {
        const res = await fetch(url, {
            method: 'PATCH',
            headers: headers(env),
            body: JSON.stringify(data),
        });
        if (!res.ok) {
            const errText = await res.text();
            console.error(`[supabase] PATCH ${table} failed (${res.status}): ${errText}`);
            return { ok: false, error: errText };
        }
        return { ok: true, error: null };
    } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[supabase] PATCH ${table} exception: ${msg}`);
        return { ok: false, error: msg };
    }
}

/**
 * INSERT a row into a Supabase table.
 * Returns the raw PostgREST response status.
 */
export async function insertRow(
    env: SupabaseEnv,
    table: string,
    data: Record<string, unknown>,
): Promise<{ ok: boolean; error: string | null; data?: any[] }> {
    const url = `${env.SUPABASE_URL}/rest/v1/${table}`;
    try {
        const res = await fetch(url, {
            method: 'POST',
            headers: {
                ...headers(env),
                'Prefer': 'return=representation',
            },
            body: JSON.stringify(data),
        });
        if (!res.ok) {
            const errText = await res.text();
            console.error(`[supabase] INSERT ${table} failed (${res.status}): ${errText}`);
            return { ok: false, error: errText };
        }
        const resData = await res.json();
        return { ok: true, error: null, data: resData as any[] };
    } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[supabase] INSERT ${table} exception: ${msg}`);
        return { ok: false, error: msg };
    }
}

/**
 * GET a row from a Supabase table by ID.
 * Returns the first matched row.
 */
export async function getRow(
    env: SupabaseEnv,
    table: string,
    id: number | string
): Promise<{ ok: boolean; error: string | null; data?: any }> {
    const url = `${env.SUPABASE_URL}/rest/v1/${table}?id=eq.${id}&select=*`;
    try {
        const res = await fetch(url, {
            method: 'GET',
            headers: headers(env),
        });
        if (!res.ok) {
            const errText = await res.text();
            console.error(`[supabase] GET ${table} failed (${res.status}): ${errText}`);
            return { ok: false, error: errText };
        }
        const resData = await res.json() as any[];
        return { ok: true, error: null, data: resData[0] };
    } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[supabase] GET ${table} exception: ${msg}`);
        return { ok: false, error: msg };
    }
}

/**
 * FETCH a row from a Supabase table by a specific column.
 * Returns the first matched row array.
 */
export async function fetchRow(
    env: SupabaseEnv,
    table: string,
    filterColumn: string,
    filterValue: string | number
): Promise<any[]> {
    const url = `${env.SUPABASE_URL}/rest/v1/${table}?${filterColumn}=eq.${encodeURIComponent(String(filterValue))}&select=*`;
    try {
        const res = await fetch(url, {
            method: 'GET',
            headers: headers(env),
        });
        if (!res.ok) {
            console.error(`[supabase] FETCH ${table} failed (${res.status})`);
            return [];
        }
        const resData = await res.json() as any[];
        return resData;
    } catch (err) {
        console.error(`[supabase] FETCH ${table} exception:`, err);
        return [];
    }
}

/**
 * FETCH rows from a Supabase table via raw PostgREST query string.
 * Example: fetchRows(env, 'gold_drafts?is_active=eq.true&order=usage_count.desc&limit=3')
 */
export async function fetchRows(
    env: SupabaseEnv,
    queryPath: string,
): Promise<any[]> {
    const url = `${env.SUPABASE_URL}/rest/v1/${queryPath}`;
    try {
        const res = await fetch(url, {
            method: 'GET',
            headers: headers(env),
        });
        if (!res.ok) {
            console.error(`[supabase] FETCH ${queryPath} failed (${res.status})`);
            return [];
        }
        return await res.json() as any[];
    } catch (err) {
        console.error(`[supabase] FETCH ${queryPath} exception:`, err);
        return [];
    }
}
