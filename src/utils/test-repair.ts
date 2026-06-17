import { safeJsonParse } from './json-repair.js';

function runTests() {
    console.log('🧪 Running safeJsonParse unit tests...\n');

    let passed = 0;
    let failed = 0;

    const assert = (name: string, condition: boolean, details?: string) => {
        if (condition) {
            console.log(`✅ [PASS] ${name}`);
            passed++;
        } else {
            console.error(`❌ [FAIL] ${name}${details ? ` - ${details}` : ''}`);
            failed++;
        }
    };

    // Test 1: Clean JSON Array
    try {
        const input = '[{"index": 0, "company": "Acme Corp", "score": 90}]';
        const parsed = safeJsonParse<any[]>(input, []);
        assert('Clean JSON Array', Array.isArray(parsed) && parsed.length === 1 && parsed[0].company === 'Acme Corp');
    } catch (e: any) {
        assert('Clean JSON Array', false, e.message);
    }

    // Test 2: Markdown Wrapped JSON Array
    try {
        const input = '```json\n[{"index": 0, "company": "Acme Corp", "score": 90}]\n```';
        const parsed = safeJsonParse<any[]>(input, []);
        assert('Markdown Wrapped Array', Array.isArray(parsed) && parsed.length === 1 && parsed[0].company === 'Acme Corp');
    } catch (e: any) {
        assert('Markdown Wrapped Array', false, e.message);
    }

    // Test 3: Truncated JSON Array (mid-item string cutoff)
    try {
        const input = '[{"index": 0, "company": "Acme Corp", "score": 90}, {"index": 1, "company": "Beta';
        const parsed = safeJsonParse<any[]>(input, []);
        assert('Truncated Array (mid-string)', Array.isArray(parsed) && parsed.length === 2 && parsed[0].company === 'Acme Corp' && parsed[1].index === 1);
    } catch (e: any) {
        assert('Truncated Array (mid-string)', false, e.message);
    }

    // Test 4: Truncated JSON Array (mid-item structure cutoff with partial repair)
    try {
        const input = '[{"index": 0, "company": "Acme Corp", "score": 90}, {"index": 1, "company": "Beta Inc", "score": ';
        const parsed = safeJsonParse<any[]>(input, []);
        assert('Truncated Array (partial object repair)', Array.isArray(parsed) && parsed.length === 2 && parsed[1].company === 'Beta Inc' && parsed[0].company === 'Acme Corp');
    } catch (e: any) {
        assert('Truncated Array (partial object repair)', false, e.message);
    }

    // Test 5: Single Object Truncation
    try {
        const input = '{"company": "Acme Corp", "executive": "John Doe", "score":';
        const parsed = safeJsonParse<any>(input, {});
        assert('Single Object Truncation', parsed.company === 'Acme Corp' && parsed.executive === 'John Doe' && !('score' in parsed));
    } catch (e: any) {
        assert('Single Object Truncation', false, e.message);
    }

    // Test 6: Extremely Malformed Array
    try {
        const input = 'some random text and then [{"index": 5}] and some trailing junk';
        const parsed = safeJsonParse<any[]>(input, []);
        assert('Extremely Malformed Array Extraction', Array.isArray(parsed) && parsed.length === 1 && parsed[0].index === 5);
    } catch (e: any) {
        assert('Extremely Malformed Array Extraction', false, e.message);
    }

    console.log(`\n📊 Test Summary: ${passed} passed, ${failed} failed`);
    if (failed > 0) {
        process.exit(1);
    } else {
        console.log('🎉 All tests passed successfully!');
    }
}

runTests();
