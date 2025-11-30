const DEFAULT_TOKEN = "Bearer YmI2OWY0OGQ4MzlhZmIyYjdiZjRhNGI4Y2U0MTE1YTg=";
const TASKS_ENDPOINT = "https://api.sensedata.io/v2/tasks";
const CUSTOMERS_ENDPOINT = "https://api.sensedata.io/v2/customers";
const DELETED_TASKS_ENDPOINT = "https://api.sensedata.io/v2/deleted_tasks";

const LOOKBACK_DAYS = 5;
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
    task.group || task.type?.category || "",
    task.type?.description || "",
    statusCliente,
    notes,
    task.updated_at || task.created_at || null
  );
};

const fetchTasksWindow = async ({ env, token, filterField, since, maxPages }) => {
  const pageLimit = maxPages ?? MAX_PAGES;
  let page = 1;
  let fetched = 0;
  let safety = 0;

  while (safety < pageLimit) {
    const url = `${TASKS_ENDPOINT}?limit=${TASKS_LIMIT}&page=${page}&${filterField}:start=${encodeDate(
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
      .map((task) => buildTaskStatement(env, task))
      .filter(Boolean);
    for (let i = 0; i < statements.length; i += ACTIVITIES_BATCH_SIZE) {
      const chunk = statements.slice(i, i + ACTIVITIES_BATCH_SIZE);
      await env.DB.batch(chunk);
    }

    fetched += tasks.length;
    page++;
    safety++;
    if (tasks.length < TASKS_LIMIT) break;
  }

  return fetched;
};

const syncActivities = async (env, options = {}) => {
  const token = getAuthToken(env);
  const since = getWindowStart();
  const created = await fetchTasksWindow({
    env,
    token,
    filterField: "created_at",
    since,
    maxPages: options.maxPages
  });
  const updated = await fetchTasksWindow({
    env,
    token,
    filterField: "updated_at",
    since,
    maxPages: options.maxPages
  });
  return { created, updated };
};

const syncClients = async (env, options = {}) => {
  const token = getAuthToken(env);
  const since = getWindowStart();
  const pageLimit = options.maxPages ?? MAX_PAGES;
  let page = 1;
  let saved = 0;
  let safety = 0;

  while (safety < pageLimit) {
    const url = `${CUSTOMERS_ENDPOINT}?limit=${CUSTOMERS_LIMIT}&page=${page}&updated_at:start=${encodeDate(
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

    page++;
    safety++;
    if (customers.length < CUSTOMERS_LIMIT) break;
  }

  return saved;
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

const runDailySync = async (env, options = {}) => {
  const [activities, removed, clients] = await Promise.all([
    syncActivities(env, options),
    syncDeletions(env, options),
    syncClients(env, options)
  ]);

  return {
    success: true,
    activities_created: activities.created,
    activities_updated: activities.updated,
    deletions: removed,
    clients_synced: clients,
    window_days: LOOKBACK_DAYS
  };
};

const handleFetch = async (request, env) => {
  if (request.method === "OPTIONS") {
    return new Response(null, { headers: corsHeaders, status: 204 });
  }

  const url = new URL(request.url);
  const pagesParam = url.searchParams.get("pages");
  const maxPagesOverride = pagesParam ? Math.max(1, parseInt(pagesParam, 10) || 1) : undefined;

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
        `DELETE FROM atividades WHERE id_sensedata = ? OR id = ? RETURNING id_sensedata`
      ).bind(activityId, activityId).first();
      return new Response(JSON.stringify({
        success: true,
        id: activityId,
        deleted: Boolean(result)
      }), { headers: corsHeaders, status: 200 });
    }
    if (url.pathname === "/api/sync") {
      const summary = await runDailySync(env, { maxPages: maxPagesOverride });
      return new Response(JSON.stringify(summary), { headers: corsHeaders, status: 200 });
    }
    if (url.pathname === "/api/sync-atividades") {
      const summary = await syncActivities(env, { maxPages: maxPagesOverride });
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
      const summary = await syncClients(env, { maxPages: maxPagesOverride });
      return new Response(JSON.stringify({ success: true, clients_synced: summary }), {
        headers: corsHeaders,
        status: 200
      });
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

const handleScheduled = async (_event, env) => {
  await runDailySync(env);
};

export default {
  fetch: handleFetch,
  scheduled: handleScheduled
};
