export default async (req, context) => {
  try {
    // 1. Search for a dataset on data.gov.ua
    const searchParams = new URLSearchParams({ q: 'петиції', rows: 10 });
    const searchUrl = `https://data.gov.ua/api/3/action/package_search?${searchParams}`;

    console.log("Searching:", searchUrl);
    const searchRes = await fetch(searchUrl);
    const searchData = await searchRes.json();

    // Check if we found anything
    if (!searchData.success || !searchData.result || searchData.result.results.length === 0) {
      return new Response(JSON.stringify({ error: "No datasets found for 'петиції'" }), {
        status: 404,
        headers: { "Content-Type": "application/json" }
      });
    }

    // 2. Iterate through results to find a readable resource (JSON/CSV)
    let packageData = null;
    let resource = null;

    for (const result of searchData.result.results) {
      const foundResource = result.resources.find(r =>
        r.format.toLowerCase() === 'json' ||
        r.format.toLowerCase() === 'csv' ||
        (r.mimetype && (r.mimetype.includes('json') || r.mimetype.includes('csv')))
      );

      if (foundResource) {
        packageData = result;
        resource = foundResource;
        break;
      }
    }

    if (!resource || !packageData) {
      return new Response(JSON.stringify({ error: "No JSON/CSV resource found in the top 10 datasets. Please try a different query." }), {
        status: 404,
        headers: { "Content-Type": "application/json" }
      });
    }

    console.log(`Found package: ${packageData.title}, Resource: ${resource.format}`);

    console.log("Fetching resource URL:", resource.url);

    // 3. Fetch the actual data
    const resourceRes = await fetch(resource.url);

    if (!resourceRes.ok) {
      return new Response(JSON.stringify({ error: `Failed to fetch resource: ${resourceRes.statusText}` }), {
        status: 502,
        headers: { "Content-Type": "application/json" }
      });
    }

    let resultData;
    const contentType = resourceRes.headers.get("content-type") || "";

    if (resource.format.toLowerCase() === 'json' || contentType.includes('json')) {
      const fullJson = await resourceRes.json();
      // If it's an array, slice it. If it's an object with a data key, handle that.
      // Assuming array for simplicity based on standard
      if (Array.isArray(fullJson)) {
        resultData = fullJson.slice(0, 10);
      } else {
        resultData = fullJson; // Just return the object if it's not an array
      }
    } else {
      // Simple CSV handling
      const text = await resourceRes.text();
      const lines = text.split('\n');
      // Get header and first 10 rows
      resultData = lines.slice(0, 11);
    }

    return new Response(JSON.stringify({
      source_package: packageData.title,
      resource_url: resource.url,
      data: resultData
    }), {
      headers: { "Content-Type": "application/json" }
    });

  } catch (error) {
    console.error("Function error:", error);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    });
  }
};
