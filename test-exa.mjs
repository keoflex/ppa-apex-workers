import dotenv from 'dotenv';
dotenv.config({ path: '.dev.vars' });

async function testExa() {
    const company = "Arxis Inc";
    const exaQuery = `"${company}" ("CEO" OR "Founder" OR "President" OR "Leadership Team" OR "email" OR "contact")`;
    const exaRes = await fetch('https://api.exa.ai/search', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'x-api-key': process.env.EXA_API_KEY,
        },
        body: JSON.stringify({
            query: exaQuery,
            useAutoprompt: true,
            type: 'neural',
            numResults: 5,
            contents: {
                text: { maxCharacters: 1000 },
                highlights: { highlightsPerUrl: 2, numSentences: 2, query: `Who is the CEO or Founder of ${company} and what is their email address?` },
            },
        }),
    });
    
    if (exaRes.ok) {
        console.log(await exaRes.json());
    } else {
        console.error(exaRes.status, await exaRes.text());
    }
}
testExa();
