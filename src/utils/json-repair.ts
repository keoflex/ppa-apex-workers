/**
 * Utility: Robust JSON Repair and Safe Parsing
 * Gracefully parses malformed or truncated JSON strings from AI models.
 */

/**
 * Safely parses a JSON string, with aggressive repair logic for truncation,
 * markdown wrapping, and syntax anomalies.
 * 
 * @param rawText The raw JSON or string output from Gemini
 * @param fallback The fallback value if parsing is completely impossible
 */
export function safeJsonParse<T = any>(rawText: string | null | undefined, fallback: T): T {
    if (!rawText || typeof rawText !== 'string') return fallback;

    let cleaned = rawText.trim();

    // 1. Strip markdown code fences if present (e.g., ```json ... ```)
    cleaned = cleaned.replace(/^```json\s*/i, '');
    cleaned = cleaned.replace(/^```\s*/, '');
    cleaned = cleaned.replace(/```$/, '');
    cleaned = cleaned.trim();

    if (!cleaned) return fallback;

    // 2. Try standard JSON parse first for clean paths
    try {
        return JSON.parse(cleaned) as T;
    } catch (e) {
        // Continue to recovery strategies
    }

    // 3. Extract the array content if we expect an array and have surrounding brackets with trailing garbage
    if (Array.isArray(fallback)) {
        const arrayMatch = cleaned.match(/\[[\s\S]*\]/);
        if (arrayMatch) {
            try {
                return JSON.parse(arrayMatch[0]) as T;
            } catch (e) {
                // Array itself is malformed/truncated, fall through to item extraction
            }
        }

        // 4. Aggressive brace-matching parser for truncated arrays of objects
        const objects: any[] = [];
        let braceCount = 0;
        let inString = false;
        let stringChar = '';
        let isEscaped = false;
        let objectStart = -1;

        for (let i = 0; i < cleaned.length; i++) {
            const char = cleaned[i];

            if (isEscaped) {
                isEscaped = false;
                continue;
            }

            if (char === '\\') {
                isEscaped = true;
                continue;
            }

            if (inString) {
                if (char === stringChar) {
                    inString = false;
                }
                continue;
            }

            if (char === '"' || char === "'") {
                inString = true;
                stringChar = char;
                continue;
            }

            if (char === '{') {
                if (braceCount === 0) {
                    objectStart = i;
                }
                braceCount++;
            } else if (char === '}') {
                braceCount--;
                if (braceCount === 0 && objectStart !== -1) {
                    const objStr = cleaned.slice(objectStart, i + 1);
                    try {
                        const parsed = JSON.parse(objStr);
                        if (parsed && typeof parsed === 'object') {
                            objects.push(parsed);
                        }
                    } catch (err) {
                        // Attempt to repair the individual object
                        const repaired = repairTruncatedObject(objStr);
                        if (repaired) {
                            objects.push(repaired);
                        }
                    }
                    objectStart = -1;
                }
            }
        }

        // If the entire array was truncated and we have a trailing partial object, try to repair it
        if (braceCount > 0 && objectStart !== -1) {
            const partialStr = cleaned.slice(objectStart);
            const repaired = repairTruncatedObject(partialStr);
            if (repaired) {
                objects.push(repaired);
            }
        }

        if (objects.length > 0) {
            return objects as unknown as T;
        }
    } else {
        // We expect a single object
        const objectMatch = cleaned.match(/\{[\s\S]*\}/);
        if (objectMatch) {
            try {
                return JSON.parse(objectMatch[0]) as T;
            } catch (e) {
                // Object itself is malformed, fall through to object repair
            }
        }

        const repaired = repairTruncatedObject(cleaned);
        if (repaired) {
            return repaired as unknown as T;
        }
    }

    if (cleaned) {
        console.warn(`⚠️ safeJsonParse failed to parse or repair JSON. Returning fallback. Raw text length: ${cleaned.length}. Preview: ${cleaned.slice(0, 300)}`);
    }
    return fallback;
}

/**
 * Attempts to repair a truncated JSON object string by:
 * - Adding missing closing quotes for unclosed strings
 * - Removing trailing commas and unclosed key-value/colon prefixes
 * - Appending matching closing braces
 */
function repairTruncatedObject(str: string): any {
    let cleaned = str.trim();
    if (!cleaned.startsWith('{')) {
        const firstBrace = cleaned.indexOf('{');
        if (firstBrace === -1) return null;
        cleaned = cleaned.slice(firstBrace);
    }

    // Pre-clean trailing partial properties or colons, e.g., `, "score":` or `, "score"`
    cleaned = cleaned.replace(/,\s*["'][^"']*["']?\s*:\s*$/, '');
    cleaned = cleaned.replace(/{\s*["'][^"']*["']?\s*:\s*$/, '{');
    cleaned = cleaned.replace(/,\s*["'][^"']*$/, '');
    cleaned = cleaned.replace(/{\s*["'][^"']*$/, '{');
    
    // Clean trailing partial/truncated literals like true, false, null by replacing with null
    cleaned = cleaned.replace(/:\s*(?:t|tr|tru|f|fa|fal|fals|n|nu|nul|null)\s*$/, ': null');
    
    cleaned = cleaned.replace(/,\s*$/, '');

    const stack: string[] = [];
    let inString = false;
    let stringChar = '';
    let isEscaped = false;
    let repairedStr = '';

    for (let i = 0; i < cleaned.length; i++) {
        const char = cleaned[i];
        repairedStr += char;

        if (isEscaped) {
            isEscaped = false;
            continue;
        }

        if (char === '\\') {
            isEscaped = true;
            continue;
        }

        if (inString) {
            if (char === stringChar) {
                inString = false;
                stack.pop(); // Pop corresponding quote
            }
            continue;
        }

        if (char === '"' || char === "'") {
            inString = true;
            stringChar = char;
            stack.push(char);
            continue;
        }

        if (char === '{' || char === '[') {
            stack.push(char);
        } else if (char === '}') {
            if (stack[stack.length - 1] === '{') {
                stack.pop();
            }
        } else if (char === ']') {
            if (stack[stack.length - 1] === '[') {
                stack.pop();
            }
        }
    }

    // Complete any unclosed structures in the stack
    while (stack.length > 0) {
        const open = stack.pop();
        if (open === '"' || open === "'") {
            repairedStr += open; // Close string literal
        } else if (open === '{') {
            repairedStr = repairedStr.trim();
            if (repairedStr.endsWith(',')) {
                repairedStr = repairedStr.slice(0, -1);
            }
            repairedStr += '}';
        } else if (open === '[') {
            repairedStr = repairedStr.trim();
            if (repairedStr.endsWith(',')) {
                repairedStr = repairedStr.slice(0, -1);
            }
            repairedStr += ']';
        }
    }

    try {
        return JSON.parse(repairedStr);
    } catch (e) {
        return null;
    }
}
