const DEFAULT_TOKEN = "Bearer YmI2OWY0OGQ4MzlhZmIyYjdiZjRhNGI4Y2U0MTE1YTg=";
const TASKS_ENDPOINT = "https://api.sensedata.io/v2/tasks";
const CUSTOMERS_ENDPOINT = "https://api.sensedata.io/v2/customers";
const DELETED_TASKS_ENDPOINT = "https://api.sensedata.io/v2/deleted_tasks";

const DEFAULT_INCREMENTAL_LOOKBACK_DAYS = 3;
const FULL_SYNC_LOOKBACK_DAYS = 120;
const MIRROR_WINDOW_DAYS = 90;
const MAX_PAGES = 50;
const TASKS_LIMIT = 1000;
const CUSTOMERS_LIMIT = 200;
const DELETED_LIMIT = 500;
const ACTIVITIES_BATCH_SIZE = 50;
const CUSTOMERS_BATCH_SIZE = 20;
const SYNC_STATE_ID = 1;

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json"
};

const sanitize = (val) => {
  if (val === null || val === undefined) return "";
  if (typeof val === "object") {
    if (Array.isArray(val)) return val.join(", ");
    return JSON.stringify(val);
  }
  return String(val).trim();
};

const toISODate = (date) => date.toISOString().split("T")[0];

const subtractDays = (days) => {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d;
};

const ensureSyncStateTable = async (env) => {
  await env.DB.prepare(`
    CREATE TABLE IF NOT EXISTS sync_state (
      id INTEGER PRIMARY KEY,
      activities_created_cursor TEXT,
      activities_updated_cursor TEXT,
      deleted_cursor TEXT,
      last_full_sync TEXT,
      last_run TEXT,
      last_error TEXT
    )
  `).run();
};

const getSyncState = async (env) => {
  await ensureSyncStateTable(env);
  return env.DB.prepare(`SELECT * FROM sync_state WHERE id = ?`).bind(SYNC_STATE_ID).first();
};

const updateSyncState = async (env, data) => {
  const columns = [
    "activities_created_cursor",
    "activities_updated_cursor",
    "deleted_cursor",
    "last_full_sync",
    "last_run",
    "last_error"
  ];
  const values = columns.map((key) => data[key] ?? null);
  await env.DB.prepare(
    `INSERT INTO sync_state (id, ${columns.join(", ")})
     VALUES (?, ${columns.map(() => "?").join(", ")})
     ON CONFLICT(id) DO UPDATE SET ${columns.map((col) => `${col}=excluded.${col}`).join(", ")}`
  ).bind(SYNC_STATE_ID, ...values).run();
};

const getAuthToken = (env) => env.SENSEDATA_TOKEN || DEFAULT_TOKEN;

const encodeQueryDate = (date) => encodeURIComponent(toISODate(date));

const pickCS = (task) => {
  return (
    task?.customer?.cs?.name ||
    task?.cs?.name ||
    task?.customer?.csm?.name ||
    task?.owner?.name ||
    "Sem CS"
  );
};

const buildTaskStatement = (env, task) => {
  const playbookName = task.parent?.description || task.playbook?.description || "";
  const statusCliente = task.customer?.status?.description || "Desconhecido";
  const notes = sanitize(task.notes).replace(/[\r\n]+/g, " ");

  return env.DB.prepare(
    `INSERT OR REPLACE INTO atividades (
        id_sensedata,
        criado_em,
        cliente,
        cs,
        playbook,
        atividade,
        status,
        previsao_conclusao,
        concluida_em,
        responsavel,
        categoria,
        tipo_atividade,
        status_cliente,
        notes,
        atualizado_em
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    task.id,
    task.created_at || null,
    task.customer?.name || "N/A",
    pickCS(task),
    playbookName,
    task.description || task.title || "Sem título",
    task.status?.description || "",
    task.due_date || null,
    task.end_date || null,
    task.owner?.name || "Sistema",
    task.group || task.type?.category || "",
    task.type?.description || "",
    statusCliente,
    notes,
    task.updated_at || task.created_at || null
  );
};

const ingestTasks = async ({ env, token, filterField, since, mode }) => {
  let page = 1;
  let hasMore = true;
  let safetyCounter = 0;
  let saved = 0;
  let maxTimestamp = since;

  while (hasMore && safetyCounter < MAX_PAGES) {
    const apiUrl = `${TASKS_ENDPOINT}?limit=${TASKS_LIMIT}&page=${page}&${filterField}:start=${encodeQueryDate(
      since
    )}`;
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: { Authorization: token, Accept: "application/json" }
    });

    if (!response.ok) break;

    const json = await response.json();
    const tasks = Array.isArray(json.tasks) ? json.tasks : [];

    if (tasks.length === 0) {
      hasMore = false;
      break;
    }

    const statements = tasks.map((task) => buildTaskStatement(env, task));
    for (let i = 0; i < statements.length; i += ACTIVITIES_BATCH_SIZE) {
      const chunk = statements.slice(i, i + ACTIVITIES_BATCH_SIZE);
      await env.DB.batch(chunk);
    }

    tasks.forEach((task) => {
      const ts = task[filterField];
      if (!ts) return;
      const tsDate = new Date(ts);
      if (!maxTimestamp || tsDate > maxTimestamp) maxTimestamp = tsDate;
    });

    saved += tasks.length;
    page++;
    safetyCounter++;
  }

  return { saved, cursor: maxTimestamp ? maxTimestamp.toISOString() : since.toISOString(), mode };
};

const syncActivitiesDoubleNet = async (request, env) => {
  const url = new URL(request.url);
  const mode = url.searchParams.get("mode") === "full" ? "full" : "incremental";
  const token = getAuthToken(env);
  const syncState = await getSyncState(env);

  const fallback = subtractDays(mode === "full" ? FULL_SYNC_LOOKBACK_DAYS : DEFAULT_INCREMENTAL_LOOKBACK_DAYS);
  const createdCursor = mode === "full" ? fallback : new Date(syncState?.activities_created_cursor || fallback);
  const updatedCursor = mode === "full" ? fallback : new Date(syncState?.activities_updated_cursor || fallback);

  const results = [];
  results.push(
    await ingestTasks({ env, token, filterField: "created_at", since: createdCursor, mode })
  );
  results.push(
    await ingestTasks({ env, token, filterField: "updated_at", since: updatedCursor, mode })
  );

  const savedTotal = results.reduce((acc, res) => acc + res.saved, 0);

  await updateSyncState(env, {
    activities_created_cursor: results[0]?.cursor,
    activities_updated_cursor: results[1]?.cursor,
    last_full_sync: mode === "full" ? new Date().toISOString() : syncState?.last_full_sync || null,
    last_run: new Date().toISOString(),
    last_error: null,
    deleted_cursor: syncState?.deleted_cursor || null
  });

  return new Response(
    JSON.stringify({ success: true, saved_count: savedTotal, mode }),
    { headers: corsHeaders, status: 200 }
  );
};

const syncClients = async (request, env) => {
  const url = new URL(request.url);
  const token = getAuthToken(env);
  let mode = url.searchParams.get("mode") || "incremental";
  const page = parseInt(url.searchParams.get("page") || "1", 10);

  let dateFilter = "";
  if (mode === "incremental") {
    const since = subtractDays(DEFAULT_INCREMENTAL_LOOKBACK_DAYS);
    dateFilter = `&updated_at:start=${encodeQueryDate(since)}`;
  } else if (mode === "full") {
    const since = subtractDays(FULL_SYNC_LOOKBACK_DAYS);
    dateFilter = `&updated_at:start=${encodeQueryDate(since)}`;
  }

  const apiUrl = `${CUSTOMERS_ENDPOINT}?limit=${CUSTOMERS_LIMIT}&page=${page}${dateFilter}`;

  try {
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: { Authorization: token, Accept: "application/json" }
    });
    if (!response.ok) {
      return new Response(JSON.stringify({ error: "Erro ao consultar clientes" }), {
        headers: corsHeaders,
        status: response.status
      });
    }

    const json = await response.json();
    const customers = Array.isArray(json.customers) ? json.customers : [];

    if (customers.length === 0) {
      return new Response(JSON.stringify({ success: true, finished: true }), {
        headers: corsHeaders,
        status: 200
      });
    }

    const activeCustomers = customers.filter(
      (c) => c.status?.description?.toLowerCase().trim() === "ativo-produto"
    );

    if (activeCustomers.length > 0) {
      const statements = activeCustomers.map((customer) => {
        const getCF = (key) =>
          customer.custom_fields && customer.custom_fields[key]
            ? customer.custom_fields[key].value
            : null;
        const csName =
          customer.cs?.name || customer.csm?.name || customer.owner?.name || "Sem CS";

        let valor = 0;
        const valorRaw = getCF("valor_total_nao_faturado") || getCF("valor_em_aberto");
        if (typeof valorRaw === "string") {
          valor = parseFloat(valorRaw.replace(/[^0-9.-]+/g, ""));
        }

        return env.DB.prepare(
          `INSERT OR REPLACE INTO clientes (
            id_sensedata,
            cliente,
            status,
            nps_onboarding,
            negocio,
            comercial,
            cs,
            situacao,
            fase,
            segmento,
            dias_sem_touch,
            ism,
            modelo_de_negocio,
            potencial,
            squad_cs,
            valor_total_em_aberto,
            atualizado_em
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        ).bind(
          customer.id,
          sanitize(customer.name),
          sanitize(customer.status?.description),
          parseFloat(getCF("nps_onboarding") || 0),
          sanitize(getCF("negocio")),
          sanitize(customer.salesperson),
          sanitize(csName),
          sanitize(getCF("situacao") || getCF("saude")),
          sanitize(customer.stage || getCF("fase_legado")),
          sanitize(customer.industry || getCF("segmento")),
          parseInt(getCF("dias_sem_touch") || "0", 10),
          sanitize(getCF("ism")),
          sanitize(getCF("modelo_de_negocio")),
          sanitize(getCF("potencial")),
          sanitize(getCF("squad_cs")),
          valor,
          customer.updated_at || null
        );
      });

      for (let i = 0; i < statements.length; i += CUSTOMERS_BATCH_SIZE) {
        const chunk = statements.slice(i, i + CUSTOMERS_BATCH_SIZE);
        await env.DB.batch(chunk);
      }
    }

    return new Response(
      JSON.stringify({
        success: true,
        saved: activeCustomers.length,
        next: page + 1,
        finished: activeCustomers.length === 0
      }),
      { headers: corsHeaders, status: 200 }
    );
  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), {
      headers: corsHeaders,
      status: 500
    });
  }
};

const syncDeletions = async (request, env) => {
  const url = new URL(request.url);
  const mode = url.searchParams.get("mode") === "full" ? "full" : "incremental";
  const token = getAuthToken(env);
  const state = await getSyncState(env);
  const fallback = subtractDays(mode === "full" ? FULL_SYNC_LOOKBACK_DAYS : DEFAULT_INCREMENTAL_LOOKBACK_DAYS);
  const deletedSince = mode === "full" ? fallback : new Date(state?.deleted_cursor || fallback);

  let page = 1;
  let hasMore = true;
  let safetyCounter = 0;
  let totalDeleted = 0;
  let maxCursor = deletedSince;

  while (hasMore && safetyCounter < MAX_PAGES) {
    const apiUrl = `${DELETED_TASKS_ENDPOINT}?limit=${DELETED_LIMIT}&page=${page}&updated_at:start=${encodeQueryDate(
      deletedSince
    )}`;
    const response = await fetch(apiUrl, {
      method: "GET",
      headers: { Authorization: token, Accept: "application/json" }
    });

    if (!response.ok) break;

    const json = await response.json();
    const deletedItems = json.deleted_tasks || json.tasks || [];

    if (deletedItems.length === 0) {
      hasMore = false;
      break;
    }

    for (const item of deletedItems) {
      const id = item.id_legacy || item.id;
      if (!id) continue;
      await env.DB.prepare(`DELETE FROM atividades WHERE id_sensedata = ?`).bind(id).run();
      totalDeleted++;

      const ts = item.updated_at || item.deleted_at;
      if (ts) {
        const tsDate = new Date(ts);
        if (!maxCursor || tsDate > maxCursor) maxCursor = tsDate;
      }
    }

    page++;
    safetyCounter++;
  }

  await updateSyncState(env, {
    deleted_cursor: maxCursor ? maxCursor.toISOString() : deletedSince.toISOString(),
    activities_created_cursor: state?.activities_created_cursor || null,
    activities_updated_cursor: state?.activities_updated_cursor || null,
    last_full_sync: mode === "full" ? new Date().toISOString() : state?.last_full_sync || null,
    last_run: new Date().toISOString(),
    last_error: null
  });

  return new Response(
    JSON.stringify({ success: true, deleted_count: totalDeleted, mode }),
    { headers: corsHeaders, status: 200 }
  );
};

const getAtividades = async (env, requestUrl) => {
  const url = new URL(requestUrl);
  const limit = parseInt(url.searchParams.get("limit") || "5000", 10);
  const sinceParam = url.searchParams.get("since");
  const since = sinceParam ? new Date(sinceParam) : subtractDays(MIRROR_WINDOW_DAYS);

  const result = await env.DB.prepare(
    `SELECT * FROM atividades
     WHERE datetime(coalesce(criado_em, atualizado_em)) >= datetime(?)
     ORDER BY datetime(coalesce(criado_em, atualizado_em)) DESC
     LIMIT ?`
  ).bind(since.toISOString(), limit).all();

  return new Response(JSON.stringify(result.results), {
    headers: corsHeaders,
    status: 200
  });
};

const getClientes = async (env) => {
  const result = await env.DB.prepare(
    `SELECT * FROM clientes ORDER BY cliente ASC`
  ).all();
  return new Response(JSON.stringify(result.results), {
    headers: corsHeaders,
    status: 200
  });
};

const handleFetch = async (request, env) => {
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  const url = new URL(request.url);

  try {
    if (url.pathname === "/api/sync-clientes") {
      return await syncClients(request, env);
    }
    if (url.pathname === "/api/sync-atividades") {
      return await syncActivitiesDoubleNet(request, env);
    }
    if (url.pathname === "/api/sync-deletados") {
      return await syncDeletions(request, env);
    }
    if (url.pathname === "/api/atividades") {
      return await getAtividades(env, request.url);
    }
    if (url.pathname === "/api/clientes") {
      return await getClientes(env);
    }

    return new Response(JSON.stringify({ status: "API espelho SenseData ativa" }), {
      headers: corsHeaders,
      status: 200
    });
  } catch (error) {
    await updateSyncState(env, {
      last_error: error.message,
      last_run: new Date().toISOString()
    });
    return new Response(JSON.stringify({ error: error.message }), {
      headers: corsHeaders,
      status: 500
    });
  }
};

const handleScheduled = async (_event, env) => {
  const mockOrigin = "http://localhost";
  const run = (path) => ({ url: `${mockOrigin}${path}` });
  await syncActivitiesDoubleNet(run("/api/sync-atividades?mode=incremental"), env);
  await syncDeletions(run("/api/sync-deletados?mode=incremental"), env);
  await syncClients(run("/api/sync-clientes?mode=incremental"), env);
};

export default {
  fetch: handleFetch,
  scheduled: handleScheduled
};
