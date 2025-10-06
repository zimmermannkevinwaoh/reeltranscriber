import express from "express";
import axios from "axios";

const app = express();
app.use(express.json());

const APIFY_TOKEN = process.env.APIFY_TOKEN;
const SCRAPER_ACTOR = process.env.SCRAPER_ACTOR || "hpix/ig-reels-scraper";
const TRANSCRIBER_ACTOR = process.env.TRANSCRIBER_ACTOR || "tictechid/anoxvanzi-Transcriber";
const APIFY = axios.create({ baseURL: "https://api.apify.com/v2" });

async function runActor(actorId, input) {
  const { data } = await APIFY.post(
    `/acts/${actorId}/runs?token=${APIFY_TOKEN}`,
    input,
    { headers: { "Content-Type": "application/json" } }
  );
  return data.data;
}

async function waitForRun(runId, send) {
  while (true) {
    const { data } = await APIFY.get(`/actor-runs/${runId}`);
    const run = data.data;
    send?.(`event: status\ndata: ${JSON.stringify({ status: run.status })}\n\n`);
    if (["SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"].includes(run.status)) return run;
    await new Promise(r => setTimeout(r, 2000));
  }
}

async function fetchDataset(datasetId) {
  const { data } = await APIFY.get(`/datasets/${datasetId}/items`, {
    params: { clean: true, format: "json" }
  });
  return data;
}

function pickUrl(item) {
  return item.videoUrl || item.mediaUrl || item.url || item.downloadUrl || null;
}

// -------- Core pipeline --------
async function pipeline({ handle, urls = [], limit = 100 }, send) {
  let reelUrls = urls;

  if (handle) {
    send?.(`event: step\ndata: ${JSON.stringify({ step: "scrape_reels", handle, limit })}\n\n`);
    const run = await runActor(SCRAPER_ACTOR, { handle, limit });
    const finished = await waitForRun(run.id, send);
    if (finished.status !== "SUCCEEDED") throw new Error("Scraper failed");
    const items = await fetchDataset(finished.defaultDatasetId);
    reelUrls = items.map(pickUrl).filter(Boolean);
  }

  if (!reelUrls.length) throw new Error("No reel URLs found.");

  // Transcribe each URL (parallel with small concurrency)
  send?.(`event: step\ndata: ${JSON.stringify({ step: "transcribe_start", count: reelUrls.length })}\n\n`);
  const results = [];
  const MAX_PAR = 5;
  let i = 0;

  async function transcribeOne(url) {
    const run = await runActor(TRANSCRIBER_ACTOR, { videoUrl: url });
    const finished = await waitForRun(run.id, send);
    if (finished.status !== "SUCCEEDED") return { url, error: "transcriber_failed" };
    const [item] = await fetchDataset(finished.defaultDatasetId);
    return { url, transcript: item?.transcript || item?.text || item || null };
  }

  const queue = [...reelUrls];
  const workers = Array(Math.min(MAX_PAR, queue.length)).fill(0).map(async () => {
    while (queue.length) {
      const url = queue.shift();
      i += 1;
      send?.(`event: progress\ndata: ${JSON.stringify({ index: i, url })}\n\n`);
      const r = await transcribeOne(url);
      results.push(r);
    }
  });
  await Promise.all(workers);

  return { handle, count: results.length, items: results };
}

// -------- HTTP endpoints --------

// JSON mode (good for manual testing)
app.post("/transcribe", async (req, res) => {
  try {
    const { handle, urls, limit } = req.body || {};
    const data = await pipeline({ handle, urls, limit });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// SSE mode (for the ChatGPT Custom Connector)
app.get("/sse", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const send = (chunk) => res.write(chunk);

  try {
    const handle = req.query.handle || undefined;
    const limit = req.query.limit ? Number(req.query.limit) : 100;
    const urls = req.query.urls ? JSON.parse(req.query.urls) : undefined;

    send(`event: hello\ndata: ${JSON.stringify({ ok: true, handle, limit })}\n\n`);
    const payload = await pipeline({ handle, urls, limit }, send);
    send(`event: done\ndata: ${JSON.stringify(payload)}\n\n`);
    res.end();
  } catch (e) {
    send(`event: error\ndata: ${JSON.stringify({ message: e.message })}\n\n`);
    res.end();
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => console.log(`Server running on :${PORT}`));
