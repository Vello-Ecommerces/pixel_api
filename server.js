import express from "express";
import cors from "cors";
import pkg from "pg";
import dotenv from "dotenv";
dotenv.config();
const { Pool } = pkg;

const app = express();
app.use(cors());
app.use(express.json());

app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// ---------------- Users ----------------
app.get("/users", async (req, res) => {
  const { rows } = await pool.query("SELECT * FROM pixel_users");
  res.json(rows);
});

app.get("/users/:client_id", async (req, res) => {
  const { client_id } = req.params;
  const { rows } = await pool.query(
    "SELECT * FROM pixel_users WHERE client_id=$1",
    [client_id]
  );
  res.json(rows[0] || null);
});

app.post("/users", async (req, res) => {
  const { client_id, traits, last_seen } = req.body;
  const { rows } = await pool.query(
    "INSERT INTO pixel_users(client_id, traits, last_seen) VALUES($1,$2,$3) ON CONFLICT (client_id) DO UPDATE SET traits=EXCLUDED.traits, last_seen=EXCLUDED.last_seen RETURNING *",
    [client_id, traits || {}, last_seen || new Date()]
  );
  res.json(rows[0]);
});

// ---------------- Sessions ----------------
app.get("/sessions", async (req, res) => {
  const { rows } = await pool.query("SELECT * FROM pixel_sessions");
  res.json(rows);
});

app.post("/sessions", async (req, res) => {
  const { session_id, client_id, started_at } = req.body;
  const { rows } = await pool.query(
    "INSERT INTO pixel_sessions(session_id, client_id, started_at) VALUES($1,$2,$3) ON CONFLICT (session_id) DO UPDATE SET client_id=EXCLUDED.client_id RETURNING *",
    [session_id, client_id, started_at || new Date()]
  );
  res.json(rows[0]);
});

// ---------------- Events ----------------
app.post("/events", async (req, res) => {
  try {
    const ev = req.body;

    // Garante que usuário existe
    if (ev.client_id) {
      await pool.query(
        `INSERT INTO pixel_users(client_id, traits, last_seen)
         VALUES($1, $2, $3)
         ON CONFLICT (client_id) DO UPDATE
         SET last_seen = EXCLUDED.last_seen`,
        [ev.client_id, ev.traits || {}, new Date()]
      );
    }

    // Garante que a sessão existe
    if (ev.session_id) {
      await pool.query(
        `INSERT INTO pixel_sessions(session_id, client_id, started_at)
         VALUES($1, $2, $3)
         ON CONFLICT (session_id) DO UPDATE
         SET client_id = EXCLUDED.client_id`,
        [ev.session_id, ev.client_id, ev.started_at || new Date()]
      );
    }

    // Inserir o evento
    const { rows } = await pool.query(
      `INSERT INTO pixel_events (
        event_id, event_name, timestamp, client_id, session_id,
        page_location, page_referrer, user_agent,
        viewport_width, viewport_height,
        utm_source, utm_medium, utm_campaign, utm_content, utm_term,
        message
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
      RETURNING id`,
      [
        ev.event_id,
        ev.event_name,
        ev.timestamp,
        ev.client_id,
        ev.session_id,
        ev.page_location,
        ev.page_referrer,
        ev.user_agent,
        ev.viewport?.width || null,
        ev.viewport?.height || null,
        ev.utm_source,
        ev.utm_medium,
        ev.utm_campaign,
        ev.utm_content,
        ev.utm_term,
        ev.message || null,
      ]
    );

    const eventDbId = rows[0].id;

    //  Inserir metadata
    const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
    await pool.query(
      `INSERT INTO pixel_metadata(pixel_event_id, ip_address, headers, geo_location)
       VALUES($1, $2, $3, $4)`,
      [eventDbId, ip, JSON.stringify(req.headers), null]
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Erro ao salvar evento:", err);
    res.status(500).json({ error: "Erro ao salvar evento" });
  }
});

// ---------------- Purchases ----------------
app.post("/purchases", async (req, res) => {
  try {
    const {
      client_id,
      session_id,
      purchase_id,
      value,
      currency,
      items,
      timestamp,
    } = req.body;

    const { rows } = await pool.query(
      `INSERT INTO pixel_purchases(client_id, session_id, purchase_id, value, currency, items, timestamp)
       VALUES($1,$2,$3,$4,$5,$6,$7)
       RETURNING id`,
      [client_id, session_id, purchase_id, value, currency, items, timestamp]
    );

    const purchaseDbId = rows[0].id;

    // Metadata para compra
    const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
    await pool.query(
      `INSERT INTO pixel_metadata(pixel_event_id, ip_address, headers, geo_location)
       VALUES($1, $2, $3, $4)`,
      [purchaseDbId, ip, JSON.stringify(req.headers), null]
    );

    res.json({ ok: true });
  } catch (err) {
    console.error("Erro ao salvar compra:", err);
    res.status(500).json({ error: "Erro ao salvar compra" });
  }
});

// ---------------- Server ----------------
const PORT = process.env.PORT || 3001;
app.listen(PORT, () =>
  console.log(`🚀 API rodando em http://localhost:${PORT}`)
);
