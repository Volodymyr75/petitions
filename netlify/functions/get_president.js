import * as cheerio from 'cheerio';

export default async (req, context) => {
    console.log("Function invoked: get_president (Node.js)");

    const headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    };

    const startUrl = "https://petition.president.gov.ua/";
    try {
        const response = await fetch(startUrl, { headers });

        if (!response.ok) {
            return new Response(JSON.stringify({ error: `Upstream returned ${response.status}` }), {
                status: 502,
                headers: { "Content-Type": "application/json" }
            });
        }

        const html = await response.text();
        const $ = cheerio.load(html);
        const petitions = [];
        const items = $(".pet_item");

        items.each((i, el) => {
            try {
                const linkTag = $(el).find(".pet_link");
                if (linkTag.length === 0) return;

                const href = linkTag.attr('href');
                const petId = href.split("/").pop();
                const title = linkTag.text().trim();
                const number = $(el).find(".pet_number").text().trim() || "N/A";

                let dateText = $(el).find(".pet_date").text().trim();
                dateText = dateText.replace("Дата оприлюднення:", "").trim();

                const status = $(el).find(".pet_status").text().trim() || "Unknown";

                const rawVotes = $(el).find(".pet_counts").text().trim() || "0";
                const votes = parseInt(rawVotes.replace(/\D/g, '')) || 0;

                petitions.push({
                    id: petId,
                    number: number,
                    title: title,
                    date: dateText,
                    status: status,
                    votes: votes,
                    url: "https://petition.president.gov.ua" + href
                });
            } catch (err) {
                console.error("Error parsing item:", err);
            }
        });

        return new Response(JSON.stringify({ source: 'President of Ukraine', data: petitions }), {
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
