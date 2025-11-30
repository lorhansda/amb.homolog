const DEFAULT_TOKEN = "Bearer YmI2OWY0OGQ4MzlhZmIyYjdiZjRhNGI4Y2U0MTE1YTg=";
const TASKS_ENDPOINT = "https://api.sensedata.io/v2/tasks";
const CUSTOMERS_ENDPOINT = "https://api.sensedata.io/v2/customers";
const DELETED_TASKS_ENDPOINT = "https://api.sensedata.io/v2/deleted_tasks";

const LOOKBACK_DAYS = 3;
const MIRROR_WINDOW_DAYS = 90;
const MAX_PAGES = 10;
const TASKS_LIMIT = 1000;
const CUSTOMERS_LIMIT = 200;
const DELETED_LIMIT = 500;
const ACTIVITIES_BATCH_SIZE = 50;
const CUSTOMERS_BATCH_SIZE = 20;

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

const normalizeStatus = (value) => {
  if (!value) return "";
  return String(value).trim().toLowerCase();
};

const normalizeStatusKey = (value) => {
  if (!value) return "";
  return String(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z]/gi, "")
    .toLowerCase();
};

const isActiveProductStatus = (value) => {
  const normalizedKey = normalizeStatusKey(value);
  if (!normalizedKey) return false;
  return normalizedKey.startsWith("ativoproduto");
};

const extractTagLabel = (tag) => {
  if (!tag) return "";
  if (typeof tag === "string") return tag.trim();
  if (typeof tag === "object") {
    return (
      tag.description ||
      tag.name ||
      tag.label ||
      tag.value ||
      ""
    ).toString().trim();
  }
  return "";
};

const normalizeTags = (rawTags) => {
  if (!rawTags) return [];
  if (Array.isArray(rawTags)) return rawTags;
  if (typeof rawTags === "string") {
    return rawTags
      .split(",")
      .map((tag) => tag.trim())
      .filter((tag) => tag.length > 0);
  }
  return [rawTags];
};

const parseDateParam = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return isNaN(date) ? null : date;
};

const parsePositiveInt = (value, min = 1) => {
  if (!value) return null;
  const parsed = parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < min) return null;
  return parsed;
};

const resolveCursorDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return isNaN(date) ? null : date;
};

const subtractDays = (days) => {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d;
};

const getWindowStart = () => {
  const start = subtractDays(LOOKBACK_DAYS);
  start.setUTCHours(0, 0, 0, 0);
  return start;
};

const encodeDate = (date) => encodeURIComponent(date.toISOString().split("T")[0]);

const getAuthToken = (env) => env.SENSEDATA_TOKEN || DEFAULT_TOKEN;

const pickCS = (task) =>
  task?.customer?.cs?.name ||
  task?.cs?.name ||
  task?.customer?.csm?.name ||
  task?.owner?.name ||
  "Sem CS";

const buildTaskStatement = (env, task) => {
  const playbookName = task.parent?.description || task.playbook?.description || "";
  const statusCliente = task.customer?.status?.description || "Desconhecido";
  const tagsArray = normalizeTags(task.tags);
  const categoriaTag = tagsArray
    .map(extractTagLabel)
    .filter((tag) => tag && tag.length > 0)[0] || "";
  const notes = sanitize(task.notes).replace(/[\r\n]+/g, " ");
  const activityId =
    task.id ||
    task.task_id ||
    task.id_legacy ||
    task.activity_id ||
    task.deleted_task_id ||
    null;
  if (!activityId) {
    console.log(
      `[sync-atividades] Atividade ignorada sem ID identificável: ${JSON.stringify({
        id: task.id,
        task_id: task.task_id,
        id_legacy: task.id_legacy
      })}`
    );
    return null;
  }

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
    activityId,
    task.created_at || null,
    task.customer?.name || "N/A",
    pickCS(task),
    playbookName,
    task.description || task.title || "Sem título",
    task.status?.description || "",
    task.due_date || null,
    task.end_date || null,
    task.owner?.name || "Sistema",
    categoriaTag,
    task.type?.description || "",
    statusCliente,
    notes,
    task.updated_at || task.created_at || null
  );
};

const fetchTasksWindow = async ({ env, token, filterField, since, maxPages, limit }) => {
  const pageLimit = maxPages ?? MAX_PAGES;
  const perPageLimit = limit ?? TASKS_LIMIT;
  let page = 1;
  let fetched = 0;
  let safety = 0;
  let maxTimestamp = since;
  const toDate = (value) => {
    if (!value) return null;
    const d = new Date(value);
    return isNaN(d) ? null : d;
  };

  while (safety < pageLimit) {
    const url = `${TASKS_ENDPOINT}?limit=${perPageLimit}&page=${page}&${filterField}:start=${encodeDate(
      since
    )}`;
    const response = await fetch(url, {
      method: "GET",
      headers: { Authorization: token, Accept: "application/json" }
    });
    if (!response.ok) break;

    const json = await response.json();
    const tasks = Array.isArray(json.tasks) ? json.tasks : [];
    if (tasks.length === 0) break;

    const statements = tasks
      .map((task) => {
        const stmt = buildTaskStatement(env, task);
        const ts = toDate(task[filterField]);
        if (ts && (!maxTimestamp || ts > maxTimestamp)) maxTimestamp = ts;
        return stmt;
      })
      .filter(Boolean);
    for (let i = 0; i < statements.length; i += ACTIVITIES_BATCH_SIZE) {
      const chunk = statements.slice(i, i + ACTIVITIES_BATCH_SIZE);
      if (chunk.length > 0) await env.DB.batch(chunk);
    }

    fetched += tasks.length;
    page++;
    safety++;
    if (tasks.length < perPageLimit) break;
  }

  return { fetched, cursor: maxTimestamp ? maxTimestamp.toISOString() : since.toISOString() };
};

const readSyncState = async (env) => {
  return env.DB.prepare(
    `SELECT
        activities_created_cursor,
        activities_updated_cursor,
        deleted_cursor,
        last_full_sync,
        last_run,
        last_error,
        last_activity_update,
        last_client_update
     FROM sync_state WHERE id = 1`
  ).first();
};

const updateSyncCursor = async (env, column, value) => {
  await env.DB.prepare(`UPDATE sync_state SET ${column} = ?, last_run = ? WHERE id = 1`).bind(
    value,
    new Date().toISOString()
  ).run();
};

const syncActivities = async (env, options = {}) => {
  const token = getAuthToken(env);
  const state = await readSyncState(env);
  const sinceOverride =
    options.since instanceof Date && !isNaN(options.since) ? options.since : parseDateParam(options.since);
  const cursorDate = resolveCursorDate(state?.last_activity_update);
  const since = sinceOverride || cursorDate || getWindowStart();
  const result = await fetchTasksWindow({
    env,
    token,
    filterField: "updated_at",
    since,
    maxPages: options.maxPages,
    limit: options.limit
  });
  if (result.cursor) {
    await updateSyncCursor(env, "last_activity_update", result.cursor);
  }
  return result;
};

const syncClients = async (env, options = {}) => {
  const token = getAuthToken(env);
  const state = await readSyncState(env);
  const sinceOverride =
    options.since instanceof Date && !isNaN(options.since) ? options.since : parseDateParam(options.since);
  const cursorDate = resolveCursorDate(state?.last_client_update);
  const since = sinceOverride || cursorDate || getWindowStart();
  const pageLimit = options.maxPages ?? MAX_PAGES;
  const perPageLimit = options.limit ?? CUSTOMERS_LIMIT;
  let page = 1;
  let saved = 0;
  let safety = 0;
  let maxTimestamp = since;
  const toDate = (value) => {
    if (!value) return null;
    const d = new Date(value);
    return isNaN(d) ? null : d;
  };

  while (safety < pageLimit) {
    const url = `${CUSTOMERS_ENDPOINT}?limit=${perPageLimit}&page=${page}&updated_at:start=${encodeDate(
      since
    )}`;
    const response = await fetch(url, {
      method: "GET",
      headers: { Authorization: token, Accept: "application/json" }
    });
    if (!response.ok) break;

    const json = await response.json();
    const customers = Array.isArray(json.customers) ? json.customers : [];
    if (customers.length === 0) break;

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

      saved += activeCustomers.length;
    }

    activeCustomers.forEach((customer) => {
      const ts = toDate(customer.updated_at);
      if (ts && (!maxTimestamp || ts > maxTimestamp)) maxTimestamp = ts;
    });

    page++;
    safety++;
    if (customers.length < perPageLimit) break;
  }

  if (maxTimestamp) {
    await updateSyncCursor(env, "last_client_update", maxTimestamp.toISOString());
  }

  return { fetched: saved, cursor: maxTimestamp ? maxTimestamp.toISOString() : since.toISOString() };
};

const CLEANUP_MONTHS = 4;

const purgeOldActivities = async (env) => {
  const threshold = new Date();
  threshold.setMonth(threshold.getMonth() - CLEANUP_MONTHS);
  threshold.setHours(0, 0, 0, 0);

  const result = await env.DB.prepare(
    `DELETE FROM atividades
     WHERE datetime(coalesce(atualizado_em, criado_em)) < datetime(?)
     RETURNING id_sensedata`
  ).bind(threshold.toISOString()).all();

  return result.results.length;
};

const resetDatabase = async (env, scope = "all") => {
  const normalizedScope = ["all", "atividades", "clientes"].includes(scope) ? scope : "all";
  let atividadesDeleted = 0;
  let clientesDeleted = 0;

  if (normalizedScope === "all" || normalizedScope === "atividades") {
    const result = await env.DB.prepare(`DELETE FROM atividades`).run();
    atividadesDeleted = result.meta?.changes || 0;
  }

  if (normalizedScope === "all" || normalizedScope === "clientes") {
    const result = await env.DB.prepare(`DELETE FROM clientes`).run();
    clientesDeleted = result.meta?.changes || 0;
  }

  try {
    await env.DB.prepare(
      `UPDATE sync_state
       SET activities_created_cursor = NULL,
           activities_updated_cursor = NULL,
           deleted_cursor = NULL,
           last_full_sync = NULL,
           last_run = NULL,
           last_error = NULL,
           last_activity_update = NULL,
           last_client_update = NULL
       WHERE id = 1`
    ).run();
  } catch (error) {
    console.log(`[reset-db] Falha ao resetar sync_state: ${error.message}`);
  }

  return {
    scope: normalizedScope,
    atividades_deleted: atividadesDeleted,
    clientes_deleted: clientesDeleted
  };
};

const syncDeletions = async (env, options = {}) => {
  const token = getAuthToken(env);
  const since = getWindowStart();
  const pageLimit = options.maxPages ?? MAX_PAGES;

  let page = 1;
  let removed = 0;
  let safety = 0;
  const debugSample = [];
  let rawSample = null;

  while (safety < pageLimit) {
    const url = `${DELETED_TASKS_ENDPOINT}?limit=${DELETED_LIMIT}&page=${page}&updated_at:start=${encodeDate(
      since
    )}`;
    const response = await fetch(url, {
      method: "GET",
      headers: { Authorization: token, Accept: "application/json" }
    });
    if (!response.ok) break;

    const json = await response.json();
    const deletedItems = json.deleted_tasks || json.tasks || [];
    if (deletedItems.length === 0) break;

    if (debugSample.length < 20) {
      deletedItems.slice(0, 20 - debugSample.length).forEach((item) => {
        debugSample.push({
          deleted_task_id: item.deleted_task_id,
          id: item.id,
          id_legacy: item.id_legacy,
          updated_at: item.updated_at,
          deleted_at: item.deleted_at
        });
        if (!rawSample) rawSample = item;
      });
    }

    for (const item of deletedItems) {
      const recordId =
        item.task?.id ||
        item.deleted_task_id ||
        item.id_legacy ||
        item.id ||
        null;
      if (!recordId) {
        console.log(
          `[sync-deletados] Ignorado sem ID: ${JSON.stringify({
            deleted_task_id: item.deleted_task_id,
            id: item.id,
            id_legacy: item.id_legacy
          })}`
        );
        continue;
      }
      const result = await env.DB.prepare(`DELETE FROM atividades WHERE id_sensedata = ? RETURNING id_sensedata`).bind(recordId).first();
      console.log(
        `[sync-deletados] Processado id=${recordId} - removido=${result ? "true" : "false"}`
      );
      removed += result ? 1 : 0;
    }

    page++;
    safety++;
    if (deletedItems.length < DELETED_LIMIT) break;
  }

  if (debugSample.length > 0) {
    console.log(
      `[sync-deletados] Excluídos desde ${since.toISOString()} | total=${removed} | sample=${JSON.stringify(
        debugSample
      )}`
    );
    if (rawSample) {
      console.log(`[sync-deletados] raw-sample-single=${JSON.stringify(rawSample)}`);
    }
  } else {
    console.log(`[sync-deletados] Nenhum registro retornado desde ${since.toISOString()}`);
  }

  return removed;
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
  const result = await env.DB.prepare(`SELECT * FROM clientes ORDER BY cliente ASC`).all();
  return new Response(JSON.stringify(result.results), {
    headers: corsHeaders,
    status: 200
  });
};

const getAtividadeById = async (env, id) => {
  if (!id) {
    return new Response(JSON.stringify({ error: "Parâmetro 'id' obrigatório." }), {
      headers: corsHeaders,
      status: 400
    });
  }
  const result = await env.DB.prepare(`SELECT * FROM atividades WHERE id_sensedata = ?`).bind(id).first();
  if (!result) {
    return new Response(JSON.stringify({ error: `Atividade ${id} não encontrada.` }), {
      headers: corsHeaders,
      status: 404
    });
  }
  return new Response(JSON.stringify(result), {
    headers: corsHeaders,
    status: 200
  });
};

const runDailySync = async (env, options = {}) => {
  const [activities, clients] = await Promise.all([
    syncActivities(env, {
      maxPages: options.maxPages,
      limit: options.activitiesLimit ?? options.limit,
      since: options.since
    }),
    syncClients(env, {
      maxPages: options.maxPages,
      limit: options.clientsLimit ?? options.limit,
      since: options.since
    })
  ]);

  return {
    success: true,
    activities_synced: activities.fetched,
    clients_synced: clients.fetched,
    window_days: LOOKBACK_DAYS,
    activity_cursor: activities.cursor,
    client_cursor: clients.cursor
  };
};

const handleFetch = async (request, env) => {
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  const url = new URL(request.url);
  const pagesParam = url.searchParams.get("pages");
  const maxPagesOverride = pagesParam ? Math.max(1, parseInt(pagesParam, 10) || 1) : undefined;
  const limitParam = parsePositiveInt(url.searchParams.get("limit"));
  const activitiesLimitOverride = parsePositiveInt(url.searchParams.get("activities_limit")) || limitParam;
  const clientsLimitOverride = parsePositiveInt(url.searchParams.get("clients_limit")) || limitParam;
  const sinceOverride = parseDateParam(url.searchParams.get("since"));

  try {
    if (url.pathname === "/api/delete-atividade") {
      let activityId = url.searchParams.get("id");
      if (!activityId && request.method === "POST") {
        try {
          const body = await request.json();
          activityId = body?.id || body?.activityId || null;
        } catch (err) {
          // ignore parse errors
        }
      }
      if (!activityId) {
        return new Response(JSON.stringify({ success: false, error: "Parâmetro 'id' obrigatório." }), {
          headers: corsHeaders,
          status: 400
        });
      }
      const result = await env.DB.prepare(
        `DELETE FROM atividades WHERE id_sensedata = ? RETURNING id_sensedata`
      ).bind(activityId).first();
      return new Response(JSON.stringify({
        success: true,
        id: activityId,
        deleted: Boolean(result)
      }), { headers: corsHeaders, status: 200 });
    }
    if (url.pathname === "/api/purge-old-atividades") {
      const deleted = await purgeOldActivities(env);
      return new Response(JSON.stringify({ success: true, deleted }), {
        headers: corsHeaders,
        status: 200
      });
    }
    if (url.pathname === "/api/reset-db") {
      const scopeParam = (url.searchParams.get("scope") || "all").toLowerCase();
      const summary = await resetDatabase(env, scopeParam);
      return new Response(JSON.stringify({ success: true, ...summary }), {
        headers: corsHeaders,
        status: 200
      });
    }
    if (url.pathname === "/api/sync") {
      const summary = await runDailySync(env, {
        maxPages: maxPagesOverride,
        activitiesLimit: activitiesLimitOverride,
        clientsLimit: clientsLimitOverride,
        since: sinceOverride
      });
      return new Response(JSON.stringify(summary), { headers: corsHeaders, status: 200 });
    }
    if (url.pathname === "/api/sync-atividades") {
      const summary = await syncActivities(env, {
        maxPages: maxPagesOverride,
        limit: activitiesLimitOverride,
        since: sinceOverride
      });
      return new Response(JSON.stringify(summary), { headers: corsHeaders, status: 200 });
    }
    if (url.pathname === "/api/sync-deletados") {
      const removed = await syncDeletions(env, { maxPages: maxPagesOverride });
      return new Response(JSON.stringify({ success: true, deletions: removed }), {
        headers: corsHeaders,
        status: 200
      });
    }
    if (url.pathname === "/api/sync-clientes") {
      const summary = await syncClients(env, {
        maxPages: maxPagesOverride,
        limit: clientsLimitOverride,
        since: sinceOverride
      });
      return new Response(JSON.stringify({ success: true, clients_synced: summary }), {
        headers: corsHeaders,
        status: 200
      });
    }
    if (url.pathname === "/api/atividade") {
      const id = url.searchParams.get("id");
      return await getAtividadeById(env, id);
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
    return new Response(JSON.stringify({ error: error.message }), {
      headers: corsHeaders,
      status: 500
    });
  }
};

const handleScheduled = async (event, env) => {
  if (event.cron === "*/15 * * * *") {
    await runDailySync(env, { maxPages: 1 });
  } else if (event.cron === "0 2 1 * *") {
    await purgeOldActivities(env);
  }
};

export default {
  fetch: handleFetch,
  scheduled: handleScheduled
};
