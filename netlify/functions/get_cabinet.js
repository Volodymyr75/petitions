export default async (req, context) => {
    console.log("Function invoked: get_cabinet (Node.js)");

    const apiUrl = "https://petition.kmu.gov.ua/api/petitions";
    const headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    };

    try {
        const response = await fetch(apiUrl, { headers });

        if (!response.ok) {
            return new Response(JSON.stringify({ error: `Cabinet API returned ${response.status}` }), {
                status: 502,
                headers: { "Content-Type": "application/json" }
            });
        }

        const jsonResp = await response.json();
        // The API returns { count: N, rows: [...] }
        const dataList = jsonResp.rows || [];

        const petitions = dataList.map(item => ({
            id: String(item.id),
            number: item.code,
            title: item.title,
            date: item.createdAt,
            status: item.status,
            votes: item.signaturesNumber,
            url: `https://petition.kmu.gov.ua/kmu/petition/${item.id}`
        }));

        return new Response(JSON.stringify({ source: 'Cabinet of Ministers', data: petitions }), {
            headers: { "Content-Type": "application/json" }
        });

    } catch (error) {
        console.error(error);
        return new Response(JSON.stringify({ error: error.message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
        });
    }
};
