import dotenv from 'dotenv';
dotenv.config({ path: '.dev.vars' });

async function testGemini() {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=${process.env.GEMINI_API_KEY}`;
    const payload = {
        contents: [{ role: 'user', parts: [{ text: "Who is the CEO of Apple? and what is his email? Return strict JSON format: { guesses: [{email, rationale}]}." }] }],
        generationConfig: {
            temperature: 0.1,
            responseMimeType: 'application/json',
        },
        tools: [{ googleSearch: {} }]
    };

    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });

    console.log(res.status);
    console.log(await res.text());
}
testGemini();
