// FILE: api/index.ts
import express from "express";
import cors from "cors";
import pkg from "pg";
import dotenv from "dotenv";
dotenv.config();

const { Pool } = pkg;
const app = express();

// -------- Middlewares --------
app.set("trust proxy", true);
app.use(cors());
app.use(express.json({ limit: "1mb" }));
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// -------- Helpers --------
const toTimestampTz = (ms?: number) =>
  ms && Number.isFinite(ms) ? new Date(ms) : new Date();

function pick<T extends object>(obj: any, keys: (keyof T)[]): Partial<T> {
  const out: any = {};
  keys.forEach((k) => {
    if (obj?.[k as string] !== undefined) out[k] = obj[k as string];
  });
  return out;
}

// validação simples inspirada no SDK (pode evoluir pra AJV se quiser)
function validateEvent(ev: any) {
  const errs: string[] = [];
  const warns: string[] = [];

  if (!ev.event_id) errs.push("missing:event_id");
  if (!ev.event_name) errs.push("missing:event_name");
  if (!ev.client_id) errs.push("missing:client_id");
  if (!ev.session_id) warns.push("recommend:session_id");

  if (ev.event_name === "purchase") {
    if (typeof ev?.ecommerce?.value !== "number")
      errs.push("missing:ecommerce.value");
    if (typeof ev?.ecommerce?.currency !== "string")
      errs.push("missing:ecommerce.currency");
  }
  return { errs, warns };
}

// Dedupe in-memory simples (1 minuto)
const dedupe = new Map<string, number>();
function shouldStore(ev: any) {
  const key = `${ev.event_name}:${ev.event_id}`;
  const now = Date.now();
  for (const [k, ts] of dedupe.entries())
    if (now - ts > 60000) dedupe.delete(k);
  if (dedupe.has(key)) return false;
  dedupe.set(key, now);
  return true;
}

// -------- Users --------
app.get("/users", async (_req, res) => {
  const { rows } = await pool.query("SELECT * FROM public.pixel_users");
  res.json(rows);
});

app.get("/users/:client_id", async (req, res) => {
  const { client_id } = req.params;
  const { rows } = await pool.query(
    "SELECT * FROM public.pixel_users WHERE client_id=$1",
    [client_id]
  );
  res.json(rows[0] || null);
});

app.post("/users", async (req, res) => {
  const { client_id, traits, last_seen, user_id, email_sha256, phone_sha256 } =
    req.body || {};
  const { rows } = await pool.query(
    `INSERT INTO public.pixel_users(client_id, traits, last_seen, user_id, email_sha256, phone_sha256)
     VALUES($1,$2,$3,$4,$5,$6)
     ON CONFLICT (client_id) DO UPDATE
     SET traits = EXCLUDED.traits,
         last_seen = EXCLUDED.last_seen,
         user_id = COALESCE(EXCLUDED.user_id, public.pixel_users.user_id),
         email_sha256 = COALESCE(EXCLUDED.email_sha256, public.pixel_users.email_sha256),
         phone_sha256 = COALESCE(EXCLUDED.phone_sha256, public.pixel_users.phone_sha256)
     RETURNING *`,
    [
      client_id,
      traits || {},
      last_seen || new Date(),
      user_id || null,
      email_sha256 || null,
      phone_sha256 || null,
    ]
  );
  res.json(rows[0]);
});

// -------- Sessions --------
app.get("/sessions", async (_req, res) => {
  const { rows } = await pool.query("SELECT * FROM public.pixel_sessions");
  res.json(rows);
});

app.post("/sessions", async (req, res) => {
  const { session_id, client_id, started_at, first_page, last_page } =
    req.body || {};
  const { rows } = await pool.query(
    `INSERT INTO public.pixel_sessions(session_id, client_id, started_at, first_page, last_page)
     VALUES($1,$2,$3,$4,$5)
     ON CONFLICT (session_id) DO UPDATE
     SET client_id = EXCLUDED.client_id,
         first_page = COALESCE(public.pixel_sessions.first_page, EXCLUDED.first_page),
         last_page = COALESCE(EXCLUDED.last_page, public.pixel_sessions.last_page)
     RETURNING *`,
    [
      session_id,
      client_id,
      started_at || new Date(),
      first_page || null,
      last_page || null,
    ]
  );
  res.json(rows[0]);
});

// -------- Events (single) --------
app.post("/events", async (req, res) => {
  try {
    const ev = req.body || {};

    // validação + dedupe
    const { errs, warns } = validateEvent(ev);
    if (errs.length)
      return res.status(400).json({ error: "invalid_event", errs, warns });
    if (!shouldStore(ev)) return res.json({ ok: true, deduped: true });

    // garantir usuário (com novos campos)
    if (ev.client_id) {
      await pool.query(
        `INSERT INTO public.pixel_users(client_id, traits, last_seen, user_id, email_sha256, phone_sha256)
         VALUES($1, $2, $3, $4, $5, $6)
         ON CONFLICT (client_id) DO UPDATE
         SET last_seen = EXCLUDED.last_seen,
             user_id = COALESCE(EXCLUDED.user_id, public.pixel_users.user_id),
             email_sha256 = COALESCE(EXCLUDED.email_sha256, public.pixel_users.email_sha256),
             phone_sha256 = COALESCE(EXCLUDED.phone_sha256, public.pixel_users.phone_sha256)`,
        [
          ev.client_id,
          ev.traits || {},
          new Date(),
          ev.identify?.user_id || null,
          ev.identify?.email_sha256 || null,
          ev.identify?.phone_sha256 || null,
        ]
      );
    }

    // garantir sessão
    if (ev.session_id) {
      await pool.query(
        `INSERT INTO public.pixel_sessions(session_id, client_id, started_at, first_page, last_page)
         VALUES($1, $2, $3, $4, $5)
         ON CONFLICT (session_id) DO UPDATE
         SET client_id = EXCLUDED.client_id`,
        [
          ev.session_id,
          ev.client_id,
          ev.started_at ? new Date(ev.started_at) : new Date(),
          ev.page_location || null,
          ev.page_location || null,
        ]
      );
    }

    // montar colunas novas
    const occurredAt = ev.occurred_at
      ? new Date(ev.occurred_at)
      : toTimestampTz(ev.timestamp);

    // compatibilidade legada: utm_* planos → campaign jsonb
    const legacyCampaign = pick<any>(ev, [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_content",
      "utm_term",
      "gclid",
      "fbclid",
      "wbraid",
      "gbraid",
      "msclkid",
      "ttclid",
      "yclid",
    ]);
    const campaign = ev.campaign || legacyCampaign || null;

    const insertSql = `
      INSERT INTO public.pixel_events (
        event_id, event_name, timestamp, client_id, session_id,
        page_location, page_referrer, user_agent,
        viewport_width, viewport_height,
        utm_source, utm_medium, utm_campaign, utm_content, utm_term,
        message,
        occurred_at, page_title, language, timezone_offset,
        screen, viewport, network, performance,
        campaign, attribution, referrer_chain, navigation,
        click, form, engagement, ecommerce,
        browser_hints, fbp, fbc,
        validation_warnings, validation_errors,
        bot_score, experiment, raw_payload
      ) VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,
        $9,$10,
        $11,$12,$13,$14,$15,
        $16,
        $17,$18,$19,$20,
        $21,$22,$23,$24,
        $25,$26,$27,$28,
        $29,$30,$31,$32,
        $33,$34,$35,
        $36,$37,
        $38,$39,$40
      )
      RETURNING id
    `;

    const params = [
      ev.event_id,
      ev.event_name,
      ev.timestamp ?? occurredAt.getTime(),
      ev.client_id,
      ev.session_id,
      ev.page_location || null,
      ev.page_referrer || null,
      ev.user_agent || (req.headers["user-agent"] as string) || null,
      ev.viewport?.width || null,
      ev.viewport?.height || null,
      ev.utm_source || null,
      ev.utm_medium || null,
      ev.utm_campaign || null,
      ev.utm_content || null,
      ev.utm_term || null,
      ev.message || null,
      occurredAt,
      ev.page_title || null,
      ev.language || null,
      Number.isFinite(ev.timezone_offset) ? ev.timezone_offset : null,
      ev.screen || null,
      ev.viewport || null,
      ev.network || null,
      ev.performance || null,
      campaign || null,
      ev.attribution || null,
      Array.isArray(ev.referrer_chain) ? ev.referrer_chain : null,
      ev.navigation || null,
      ev.click || null,
      ev.form || null,
      ev.engagement || null,
      ev.ecommerce || null,
      ev.browser_hints || null,
      ev.fbp || null,
      ev.fbc || null,
      ev.validation_warnings || null,
      ev.validation_errors || null,
      Number.isFinite(ev.bot_score) ? ev.bot_score : 0,
      ev.experiment || null,
      ev, // raw_payload
    ];

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // evento
      const { rows } = await client.query(insertSql, params);
      const eventDbId = rows[0].id;

      // metadata
      const ip =
        (req.headers["x-forwarded-for"] as string)?.split(",")[0]?.trim() ||
        req.socket.remoteAddress ||
        null;
      await client.query(
        `INSERT INTO public.pixel_metadata(pixel_event_id, ip_address, headers, geo_location, user_agent, request_id)
         VALUES($1,$2,$3,$4,$5,$6)`,
        [
          eventDbId,
          ip,
          JSON.stringify(req.headers || {}),
          null,
          (req.headers["user-agent"] as string) || null,
          (req.headers["x-request-id"] as string) || null,
        ]
      );

      await client.query("COMMIT");
      res.json({ ok: true, id: eventDbId, warns });
    } catch (e) {
      await client.query("ROLLBACK");
      console.error("DB error /events:", e);
      res.status(500).json({ error: "db_error" });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Erro ao salvar evento:", err);
    res.status(500).json({ error: "server_error" });
  }
});

// -------- Events (bulk) --------
app.post("/events/bulk", async (req, res) => {
  try {
    const events = Array.isArray(req.body) ? req.body : [];
    if (!events.length) return res.json({ ok: true, ingested: 0 });

    const client = await pool.connect();
    let ok = 0;
    try {
      await client.query("BEGIN");
      for (const ev of events) {
        const { errs } = validateEvent(ev);
        if (errs.length) continue;
        if (!shouldStore(ev)) continue;

        // upsert user minimal
        if (ev.client_id) {
          await client.query(
            `INSERT INTO public.pixel_users(client_id, last_seen)
             VALUES($1,$2)
             ON CONFLICT (client_id) DO UPDATE
             SET last_seen = EXCLUDED.last_seen`,
            [ev.client_id, new Date()]
          );
        }
        // upsert session minimal
        if (ev.session_id) {
          await client.query(
            `INSERT INTO public.pixel_sessions(session_id, client_id, started_at)
             VALUES($1,$2,$3)
             ON CONFLICT (session_id) DO UPDATE SET client_id=EXCLUDED.client_id`,
            [ev.session_id, ev.client_id, new Date()]
          );
        }

        const occurredAt = ev.occurred_at
          ? new Date(ev.occurred_at)
          : toTimestampTz(ev.timestamp);

        const legacyCampaign = pick<any>(ev, [
          "utm_source",
          "utm_medium",
          "utm_campaign",
          "utm_content",
          "utm_term",
          "gclid",
          "fbclid",
          "wbraid",
          "gbraid",
          "msclkid",
          "ttclid",
          "yclid",
        ]);
        const campaign = ev.campaign || legacyCampaign || null;

        await client.query(
          `
          INSERT INTO public.pixel_events (
            event_id, event_name, timestamp, client_id, session_id,
            page_location, page_referrer, user_agent,
            viewport_width, viewport_height,
            utm_source, utm_medium, utm_campaign, utm_content, utm_term,
            message,
            occurred_at, page_title, language, timezone_offset,
            screen, viewport, network, performance,
            campaign, attribution, referrer_chain, navigation,
            click, form, engagement, ecommerce,
            browser_hints, fbp, fbc,
            validation_warnings, validation_errors,
            bot_score, experiment, raw_payload
          ) VALUES (
            $1,$2,$3,$4,$5,
            $6,$7,$8,
            $9,$10,
            $11,$12,$13,$14,$15,
            $16,
            $17,$18,$19,$20,
            $21,$22,$23,$24,
            $25,$26,$27,$28,
            $29,$30,$31,$32,
            $33,$34,$35,
            $36,$37,
            $38,$39,$40
          )
          `,
          [
            ev.event_id,
            ev.event_name,
            ev.timestamp ?? occurredAt.getTime(),
            ev.client_id,
            ev.session_id,
            ev.page_location || null,
            ev.page_referrer || null,
            ev.user_agent || null,
            ev.viewport?.width || null,
            ev.viewport?.height || null,
            ev.utm_source || null,
            ev.utm_medium || null,
            ev.utm_campaign || null,
            ev.utm_content || null,
            ev.utm_term || null,
            ev.message || null,
            occurredAt,
            ev.page_title || null,
            ev.language || null,
            Number.isFinite(ev.timezone_offset) ? ev.timezone_offset : null,
            ev.screen || null,
            ev.viewport || null,
            ev.network || null,
            ev.performance || null,
            campaign || null,
            ev.attribution || null,
            Array.isArray(ev.referrer_chain) ? ev.referrer_chain : null,
            ev.navigation || null,
            ev.click || null,
            ev.form || null,
            ev.engagement || null,
            ev.ecommerce || null,
            ev.browser_hints || null,
            ev.fbp || null,
            ev.fbc || null,
            ev.validation_warnings || null,
            ev.validation_errors || null,
            Number.isFinite(ev.bot_score) ? ev.bot_score : 0,
            ev.experiment || null,
            ev,
          ]
        );

        // metadata (apenas um IP/UA para o lote inteiro – opcional: p/ cada evento)
        ok++;
      }
      // metadata comum do request (uma linha, opcional)
      if (ok > 0) {
        const ip =
          (req.headers["x-forwarded-for"] as string)?.split(",")[0]?.trim() ||
          req.socket.remoteAddress ||
          null;
        await client.query(
          `INSERT INTO public.pixel_metadata(pixel_event_id, ip_address, headers, geo_location, user_agent, request_id)
           VALUES($1,$2,$3,$4,$5,$6)`,
          [
            0, // pixel_event_id opcional (0 = lote); você pode omitir essa linha se preferir só por evento
            ip,
            JSON.stringify(req.headers || {}),
            null,
            (req.headers["user-agent"] as string) || null,
            (req.headers["x-request-id"] as string) || null,
          ]
        );
      }

      await client.query("COMMIT");
      res.json({ ok: true, ingested: ok });
    } catch (e) {
      await client.query("ROLLBACK");
      console.error("DB error /events/bulk:", e);
      res.status(500).json({ error: "db_error" });
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Erro bulk:", err);
    res.status(500).json({ error: "server_error" });
  }
});

// -------- Legacy purchases (mantido por compatibilidade) --------
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
    } = req.body || {};
    // Insere um evento "purchase" no novo formato, preservando compatibilidade
    const ev = {
      event_id: purchase_id,
      event_name: "purchase",
      client_id,
      session_id,
      timestamp: timestamp || Date.now(),
      ecommerce: {
        value: Number(value || 0),
        currency: currency || null,
        items: items || [],
      },
      message: "legacy /purchases",
    };
    req.body = ev; // reusa a rota /events
    return app._router.handle(req, res, () => {}, "post", "/events");
  } catch (err) {
    console.error("Erro ao salvar compra:", err);
    res.status(500).json({ error: "Erro ao salvar compra" });
  }
});

// -------- Health --------
app.get("/health", (_req, res) => res.json({ ok: true }));

// -------- Server --------
const PORT = process.env.PORT || 3001;
app.listen(PORT, () =>
  console.log(`🚀 API rodando em http://localhost:${PORT}`)
);
