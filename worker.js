/**

===================================================================

WORKER DE PROCESSAMENTO DE OKRs

===================================================================

Este script é executado em uma thread separada.

Ele não pode acessar o DOM (window, document).

Ele se comunica com a thread principal através de postMessage e onmessage. */

// --- VARIÁVEIS GLOBAIS DO WORKER --- let rawActivities = [], rawClients = [], clientOnboardingStartDate = new Map(), csToSquadMap = new Map(), currentUser = {}, manualEmailToCsMap = {}, processedClientsMap = new Map(); // Mapa para consulta rápida de dados de clientes (inclui FaseNorm)

// --- FUNÇÕES AUXILIARES DE PROCESSAMENTO ---

const normalizeText = (str = '') => String(str).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

const getQuarter = (date) => Math.floor(date.getUTCMonth() / 3);

// Função auxiliar para calcular diferença em dias (UTC) const daysBetween = (date1, date2) => { if (!date1 || !date2) return null; // Garante que estamos comparando apenas a data, zerando horas/minutos/segundos em UTC const utcDate1 = new Date(Date.UTC(date1.getUTCFullYear(), date1.getUTCMonth(), date1.getUTCDate())); const utcDate2 = new Date(Date.UTC(date2.getUTCFullYear(), date2.getUTCMonth(), date2.getUTCDate())); return Math.floor((utcDate2 - utcDate1) / (1000 * 60 * 60 * 24)); };

const formatDate = (date) => { if (!(date instanceof Date) || isNaN(date)) return ''; const day = String(date.getUTCDate()).padStart(2, '0'); const month = String(date.getUTCMonth() + 1).padStart(2, '0'); const year = date.getUTCFullYear(); return ${day}/${month}/${year}; };

const parseDate = (dateInput) => { if (!dateInput) return null; if (dateInput instanceof Date && !isNaN(dateInput)) return dateInput; // Retorna se já for Date válido

const dateStr = String(dateInput).trim();
const dateOnlyStr = dateStr.split(' ')[0]; // Pega só a parte da data
let parts;

// Tenta formato DD/MM/YYYY ou DD-MM-YYYY
parts = dateOnlyStr.split(/[-/]/);
if (parts.length === 3 && parts[2]?.length === 4) {
    const day = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10);
    const year = parseInt(parts[2], 10);
    // Valida se os números formam uma data válida
    const date = new Date(Date.UTC(year, month - 1, day));
    if (!isNaN(date) && date.getUTCDate() === day && date.getUTCMonth() === month - 1 && date.getUTCFullYear() === year) {
         return date;
    }
}
// Tenta formato YYYY/MM/DD ou YYYY-MM-DD
 if (parts.length === 3 && parts[0]?.length === 4) {
    const year = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10);
    const day = parseInt(parts[2], 10);
    const date = new Date(Date.UTC(year, month - 1, day));
     if (!isNaN(date) && date.getUTCDate() === day && date.getUTCMonth() === month - 1 && date.getUTCFullYear() === year) {
         return date;
     }
}

// Tenta converter número serial do Excel (se aplicável, mas já deveria vir como Date)
if (typeof dateInput === 'number' && dateInput > 10000) { // Suposição básica de número serial
     const excelTimestamp = (dateInput - 25569) * 86400 * 1000;
     const date = new Date(excelTimestamp);
     // Ajusta para UTC
     return new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
}


// Se nada funcionou, retorna null
// console.warn("Falha ao parsear data:", dateInput); // Descomente para depurar datas problemáticas
return null;
};

// --- FUNÇÕES DE INICIALIZAÇÃO DE DADOS (Executadas uma vez) ---

const buildCsToSquadMap = () => { csToSquadMap.clear(); // Limpa o mapa const allCSs = [...new Set(rawClients.map(c => c.CS).filter(Boolean))];

allCSs.forEach(csName => {
    const clientsOfCS = rawClients.filter(c => c.CS === csName);
    if (!clientsOfCS.some(c => c['Squad CS'])) return; // Pula se nenhum cliente do CS tem Squad

    const squadCounts = clientsOfCS.reduce((acc, client) => {
        const squad = client['Squad CS'];
        if (squad) {
            acc[squad] = (acc[squad] || 0) + 1;
        }
        return acc;
    }, {});

    let majoritySquad = '';
    let maxCount = 0;
    for (const squad in squadCounts) {
        if (squadCounts[squad] > maxCount) {
            maxCount = squadCounts[squad];
            majoritySquad = squad;
        }
    }
    if (majoritySquad) {
        csToSquadMap.set(csName, majoritySquad);
    }
});
};

const processInitialData = () => { const clienteMap = new Map(rawClients.map(cli => [(cli.Cliente || '').trim().toLowerCase(), cli])); const onboardingPlaybooks = [ // Playbooks que definem o início do onboarding 'onboarding lantek', 'onboarding elétrica', 'onboarding altium', 'onboarding solidworks', 'onboarding usinagem', 'onboarding hp', 'onboarding alphacam', 'onboarding mkf', 'onboarding formlabs', 'tech journey' ];

clientOnboardingStartDate.clear(); // Limpa o mapa de datas de início

// Processa as atividades para adicionar dados do cliente e parsear datas
rawActivities = rawActivities.map(ativ => {
    const clienteKey = (ativ.Cliente || '').trim().toLowerCase();
    const clienteInfo = clienteMap.get(clienteKey) || {}; // Busca dados do cliente
    const criadoEmDate = parseDate(ativ['Criado em']); // Parseia data de criação
    const playbookNormalized = normalizeText(ativ.Playbook);

    // Encontra a data mais antiga de criação de um playbook de onboarding para cada cliente
    if (onboardingPlaybooks.includes(playbookNormalized) && criadoEmDate) {
        const existingStartDate = clientOnboardingStartDate.get(clienteKey);
        if (!existingStartDate || criadoEmDate < existingStartDate) {
            clientOnboardingStartDate.set(clienteKey, criadoEmDate);
        }
    }

    // Retorna a atividade enriquecida
    return {
        ...ativ,
        ClienteCompleto: clienteKey, // Nome normalizado para join
        ClienteOriginal: ativ.Cliente, // Mantém o nome original se necessário
        Segmento: clienteInfo.Segmento,
        CS: clienteInfo.CS,
        Squad: clienteInfo['Squad CS'],
        Fase: clienteInfo.Fase,
        ISM: clienteInfo.ISM,
        Negocio: clienteInfo['Negócio'], // Usa 'Negócio' com acento
        Comercial: clienteInfo['Comercial'],
        CriadoEm: criadoEmDate,
        PrevisaoConclusao: parseDate(ativ['Previsão de conclusão']), // Parseia previsão
        ConcluidaEm: parseDate(ativ['Concluída em']), // Parseia conclusão
        NPSOnboarding: clienteInfo['NPS onboarding'],
        ValorNaoFaturado: parseFloat(String(clienteInfo['Valor total não faturado'] || '0').replace(/[^0-9,-]+/g, "").replace(",", ".")) || 0 // Trata valor não faturado
    };
});
};

// --- FUNÇÕES DE CÁLCULO PRINCIPAIS ---

const calculateOverdueMetrics = (activities, selectedCS) => { // ... (código existente da função calculateOverdueMetrics) ... const metrics = {         overdueActivities: {}     };     const now = new Date();     const today = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()));

    const csActivities = selectedCS === 'Todos' ?         activities :         activities.filter(a => (a['Responsável'] || '').trim() === selectedCS);

    const overdue = csActivities.filter(a => {         const dueDate = a.PrevisaoConclusao;         return !a.ConcluidaEm && dueDate && dueDate < today;     });

    const playbookDefs = {         'plano': { p: 'plano de acao', a: 'analise do cenario do cliente' },         'adocao': { p: null, a: ["revisao da adocao - sponsor (mid/enterprise)", "revisao da adocao - ku (mid/enterprise)", "revisao da adocao - ku (smb)"] },         'reunioes-qbr': { p: null, a: "ligacao de sensibilizacao com o sponsor" },         'planos-sucesso': { p: null, a: "plano de sucesso desenvolvido" },         'followup': { p: 'pos fechamento de loop cs', a: 'follow-up 1' },         'discovery': { p: 'discovery potencial', a: 'contato com o cliente' },         'engajamento-smb': { p: 'contato consolidado', a: 'contato com o cliente' },         'baixo-engajamento-mid': { p: null, a: "cliente com baixo engajamento - mid/enter" }     };

    for (const [key, def] of Object.entries(playbookDefs)) {         let filterFn = (item) => def.p ? (normalizeText(item.Playbook).includes(def.p) && normalizeText(item.Atividade).includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => normalizeText(item.Atividade).includes(term)) : normalizeText(item.Atividade).includes(def.a));         if (key === 'plano') {             filterFn = (item) => {                 const atividadeNormalizada = normalizeText(item.Atividade);                 const playbookNormalizado = normalizeText(item.Playbook);                 const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);                 const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');                 return isPlanoOriginal || isAlertaSuporte;             };         }         metrics.overdueActivities[key] = overdue.filter(filterFn);     }     return metrics; };

const calculateMetricsForPeriod = (month, year, activities, clients, selectedCS, includeOnboarding, goals) => { // ... (código existente da função calculateMetricsForPeriod) ... const metrics = {};     const startOfPeriod = new Date(Date.UTC(year, month, 1));

    const isConcludedInPeriod = (item) => item.ConcluidaEm?.getUTCMonth() === month && item.ConcluidaEm?.getUTCFullYear() === year;     const isPredictedForPeriod = (activity) => {         const isDueInPeriod = activity.PrevisaoConclusao?.getUTCMonth() === month && activity.PrevisaoConclusao?.getUTCFullYear() === year;         // Considera previsto se vence no período E (não foi concluída OU foi concluída no período ou depois) // Isso evita contar atividades concluídas antes do período como previstas para o período. const notCompletedBeforePeriod = !activity.ConcluidaEm || activity.ConcluidaEm >= startOfPeriod;         return isDueInPeriod && notCompletedBeforePeriod;     };     const isOverdueButCompletedInPeriod = (activity) => activity.ConcluidaEm && isConcludedInPeriod(activity) && activity.PrevisaoConclusao && activity.PrevisaoConclusao < startOfPeriod;

// Mapa auxiliar para pegar CS e Fase do cliente rapidamente
    const clientOwnerMap = new Map(rawClients.map(cli => [(cli.Cliente || '').trim().toLowerCase(), {         cs: cli.CS,         fase: normalizeText(cli.Fase || '')     }]));

// Filtra clientes que pertencem à carteira no período (baseado no selectedCS ou Todos)
// E aplica o filtro de includeOnboarding
const clientsInPortfolio = clients.filter(c => selectedCS === 'Todos' || (c.CS || '').trim() === selectedCS);
const clientsForPeriod = includeOnboarding ? clientsInPortfolio : clientsInPortfolio.filter(c => normalizeText(c.Fase) !== 'onboarding');
const clientsForPeriodSet = new Set(clientsForPeriod.map(c => (c.Cliente || '').trim().toLowerCase())); // Set para lookup rápido

// Filtra atividades concluídas no período E que pertencem aos clientes do período
const concludedActivitiesInPeriodAndPortfolio = activities.filter(a =>
    isConcludedInPeriod(a) && clientsForPeriodSet.has(a.ClienteCompleto)
);

// Filtra atividades previstas para o período E que pertencem aos clientes do período
const predictedActivitiesInPeriodAndPortfolio = activities.filter(a =>
    isPredictedForPeriod(a) && clientsForPeriodSet.has(a.ClienteCompleto)
);

// Definições dos playbooks (mantidas)
 const playbookDefs = {
        'plano': { p: 'plano de acao', a: 'analise do cenario do cliente' },         'adocao': { p: null, a: ["revisao da adocao - sponsor (mid/enterprise)", "revisao da adocao - ku (mid/enterprise)", "revisao da adocao - ku (smb)"] },         'reunioes-qbr': { p: null, a: "ligacao de sensibilizacao com o sponsor" },         'planos-sucesso': { p: null, a: "plano de sucesso desenvolvido" },         'followup': { p: 'pos fechamento de loop cs', a: 'follow-up 1' },         'discovery': { p: 'discovery potencial', a: 'contato com o cliente' },         'engajamento-smb': { p: 'contato consolidado', a: 'contato com o cliente' },         'baixo-engajamento-mid': { p: null, a: "cliente com baixo engajamento - mid/enter" }     };

// Calcula Previsto, Realizado (no prazo) e Atrasado Concluído
for (const [key, def] of Object.entries(playbookDefs)) {
    let filterFn;
    // Lógica específica para 'plano' (mantida)
     if (key === 'plano') {
            filterFn = (item) => {                 const atividadeNormalizada = normalizeText(item.Atividade || item['Título'] || ''); // Usar Atividade ou Título                 const playbookNormalizado = normalizeText(item.Playbook);                 const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);                 const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');                 return isPlanoOriginal || isAlertaSuporte;             };         } else { filterFn = (item) => { const atividadeNormalizada = normalizeText(item.Atividade || item['Título'] || ''); // Usar Atividade ou Título const playbookNormalizado = normalizeText(item.Playbook); return def.p ? (playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => atividadeNormalizada.includes(term)) : atividadeNormalizada.includes(def.a)); }; }

    // Previsto: Filtra as atividades PREVISTAS que batem com a definição do playbook
    metrics[`${key}-previsto`] = predictedActivitiesInPeriodAndPortfolio.filter(filterFn);

    // Concluído (Geral): Filtra as atividades CONCLUÍDAS que batem com a definição
    const concludedMatchingActivities = concludedActivitiesInPeriodAndPortfolio.filter(filterFn);

    // Atrasado Concluído: Filtra as concluídas que estavam atrasadas
    metrics[`${key}-atrasado-concluido`] = concludedMatchingActivities.filter(isOverdueButCompletedInPeriod);

    // Realizado (no Prazo): Filtra as concluídas que NÃO estavam atrasadas
    metrics[`${key}-realizado`] = concludedMatchingActivities.filter(a => !isOverdueButCompletedInPeriod(a));
}


// --- Métricas Gerais e de Contato ---
const validContactKeywords = ['e-mail', 'ligacao', 'reuniao', 'whatsapp', 'call sponsor'];
const isCountableContact = (a) => {
    const activityType = normalizeText(a['Tipo de Atividade'] || a['Tipo de atividade'] || a['Tipo']);
    const activityName = normalizeText(a.Atividade || a['Título'] || ''); // Usar Atividade ou Título
    // Exclui regras e inclui tipos válidos ou nome começando com whatsapp octadesk
    return !activityName.includes('regra') && (validContactKeywords.some(type => activityType.includes(type)) || activityName.startsWith('whatsapp octadesk'));
};

// Filtra contatos concluídos que são contáveis
const countableContactsInPeriod = concludedActivitiesInPeriodAndPortfolio.filter(isCountableContact);

// Separa contatos de CS e Onboarding (se incluído)
let csContactActivities = [];
let onboardingContactActivities = [];
countableContactsInPeriod.forEach(activity => {
    const clientInfo = clientOwnerMap.get(activity.ClienteCompleto);
    if (clientInfo?.fase !== 'onboarding') {
        csContactActivities.push(activity);
    } else if (includeOnboarding) { // Só adiciona se includeOnboarding for true
        onboardingContactActivities.push(activity);
    }
});

// Cobertura: Clientes únicos contatados (CS + Onboarding se incluído)
const contactsForCoverage = [...csContactActivities, ...onboardingContactActivities];
const uniqueClientContactsMap = new Map();
contactsForCoverage.forEach(activity => {
    // Guarda a atividade mais recente (ou qualquer uma, só para ter os dados do cliente)
     if (!uniqueClientContactsMap.has(activity.ClienteCompleto)) {
         uniqueClientContactsMap.set(activity.ClienteCompleto, activity);
     }
});
metrics['cob-carteira'] = Array.from(uniqueClientContactsMap.values()); // Lista de *atividades* (uma por cliente contatado)


// Tipos de Contato (CS): Prioriza ligação/reunião sobre email/whatsapp para o mesmo cliente
const callKeywords = ['ligacao', 'reuniao', 'call sponsor'];
const emailKeywords = ['e-mail', 'whatsapp']; // Inclui 'whatsapp' aqui

const prioritizedCsContacts = new Map();
csContactActivities.forEach(activity => {
    const clientKey = activity.ClienteCompleto;
    if (!clientKey) return;

    const activityType = normalizeText(activity['Tipo de Atividade'] || activity['Tipo de atividade'] || activity['Tipo']);
    const activityName = normalizeText(activity.Atividade || activity['Título'] || '');
    let currentCategory = '';

    if (callKeywords.some(k => activityType.includes(k))) {
        currentCategory = 'call';
    } else if (emailKeywords.some(k => activityType.includes(k)) || activityName.startsWith('whatsapp octadesk')) { // Checa nome tbm
        currentCategory = 'email';
    }

    const existing = prioritizedCsContacts.get(clientKey);
    // Prioridade: Call > Email. Se já existe Call, não substitui. Se existe Email e vem Call, substitui.
    if (currentCategory === 'call' || (currentCategory === 'email' && (!existing || existing.category !== 'call'))) {
         prioritizedCsContacts.set(clientKey, { activity, category: currentCategory });
    }
});

const finalCsContactList = Array.from(prioritizedCsContacts.values());
metrics['contato-ligacao'] = finalCsContactList.filter(item => item.category === 'call').map(item => item.activity);
metrics['contato-email'] = finalCsContactList.filter(item => item.category === 'email').map(item => item.activity);

// Contatos Onboarding (apenas para exibição se incluído) - um por cliente
 const uniqueOnboardingContactsMap = new Map();
 onboardingContactActivities.forEach(activity => {
     if (!uniqueOnboardingContactsMap.has(activity.ClienteCompleto)) {
         uniqueOnboardingContactsMap.set(activity.ClienteCompleto, activity);
     }
 });
metrics['onboarding-contacts-display'] = Array.from(uniqueOnboardingContactsMap.values());


// Classificação de Ligações/Reuniões
const allCallsAndMeetings = metrics['contato-ligacao'] || [];
metrics['ligacoes-classificadas'] = allCallsAndMeetings.filter(a => {
    const categoria = normalizeText(a['Categoria']);
    return categoria === 'engajado' || categoria === 'desengajado';
});
metrics['ligacoes-engajadas'] = metrics['ligacoes-classificadas'].filter(a => normalizeText(a['Categoria']) === 'engajado');
metrics['ligacoes-desengajadas'] = metrics['ligacoes-classificadas'].filter(a => normalizeText(a['Categoria']) === 'desengajado');


// Métricas Gerais da Carteira (baseada em clientsForPeriod)
metrics['total-clientes-unicos-cs'] = clientsForPeriod.length; // Total de clientes NA carteira filtrada
metrics['total-clientes'] = clientsForPeriod; // Guarda a lista filtrada para usar no gráfico de clientes
const senseScores = clientsForPeriod
    .map(c => parseFloat(String(c['Sense Score']).replace(',', '.'))) // Trata vírgula
    .filter(s => !isNaN(s) && s !== null);
metrics['sensescore-avg'] = senseScores.length > 0 ? senseScores.reduce((acc, val) => acc + val, 0) / senseScores.length : 0;


// --- Engajamento SMB ---
const smbConcluidoList = (metrics['engajamento-smb-realizado'] || []).concat(metrics['engajamento-smb-atrasado-concluido'] || []);
const uniqueSmbContacts = new Map();
// Prioriza status mais recente ou 'engajado'/'desengajado'
smbConcluidoList.sort((a,b) => (b.ConcluidaEm || 0) - (a.ConcluidaEm || 0)).forEach(a => {
    const status = normalizeText(a['Categoria']);
    const clientKey = a.ClienteCompleto;
     if (!uniqueSmbContacts.has(clientKey) && (status === 'engajado' || status === 'desengajado')) {
         uniqueSmbContacts.set(clientKey, { activity: a, status: status });
     }
});

const finalSmbList = Array.from(uniqueSmbContacts.values());
metrics['engajamento-smb-engajado'] = finalSmbList.filter(item => item.status === 'engajado').map(item => item.activity);
metrics['engajamento-smb-desengajado'] = finalSmbList.filter(item => item.status === 'desengajado').map(item => item.activity);

// --- Modelo de Negócio e Potencial (baseado nos clientes CONTATADOS) ---
const clientesContatados = metrics['cob-carteira'] || []; // Lista de atividades (uma por cliente)
const clientesContatadosComDados = clientesContatados.map(atividade => {
    // Busca dados do cliente no MAPA GERAL (processedClientsMap) para pegar dados mais completos
    const clienteData = processedClientsMap.get(atividade.ClienteCompleto);
    return {
        ...atividade, // Mantém dados da atividade de contato
        'Modelo de negócio': clienteData ? (clienteData['Modelo de negócio'] || 'Vazio') : 'Não encontrado',
        'Potencial': clienteData ? (clienteData['Potencial'] || 'Vazio') : 'Não encontrado'
    };
});
metrics['modelo-negocio-preenchido'] = clientesContatadosComDados.filter(item => item['Modelo de negócio'] && item['Modelo de negócio'] !== 'Vazio');
metrics['modelo-negocio-faltante'] = clientesContatadosComDados.filter(item => !item['Modelo de negócio'] || item['Modelo de negócio'] === 'Vazio');
metrics['cliente-potencial-preenchido'] = clientesContatadosComDados.filter(item => item['Potencial'] && item['Potencial'] !== 'Vazio');
metrics['cliente-potencial-faltante'] = clientesContatadosComDados.filter(item => !item['Potencial'] || item['Potencial'] === 'Vazio');


// --- SKA LABS (Trimestral - baseado nos clientes DA CARTEIRA FILTRADA) ---
const selectedQuarter = Math.floor(month / 3);
const isConcludedInQuarter = (item) => item.ConcluidaEm && Math.floor(item.ConcluidaEm.getUTCMonth() / 3) === selectedQuarter && item.ConcluidaEm.getUTCFullYear() === year;

// Base são os clientes DA CARTEIRA FILTRADA no período
const eligibleClientSetForLabs = new Set(clientsForPeriod.map(c => (c.Cliente || '').trim().toLowerCase()));
metrics['total-labs-eligible'] = clientsForPeriod.length; // Base é a carteira filtrada

// Busca atividades de labs concluídas no trimestre E que pertencem aos clientes elegíveis
metrics['ska-labs-realizado-list'] = [...new Map(
    activities // Usa TODAS as atividades (já processadas)
    .filter(isConcludedInQuarter) // Filtra pelo trimestre
    .filter(a => normalizeText(a.Atividade || a['Título'] || '').includes('participou do ska labs')) // Filtra pelo nome
    .filter(a => eligibleClientSetForLabs.has(a.ClienteCompleto)) // Filtra pelos clientes da carteira
    .map(a => [a.ClienteCompleto, a]) // Cria mapa para pegar um por cliente
).values()];


// --- Gráficos de Pizza (baseados nos clientes CONTATADOS) ---
const contatosPorNegocio = {};
const contatosPorComercial = {};
(metrics['cob-carteira'] || []).forEach(activity => { // Usa a lista de atividades de cobertura
    const negocio = activity.Negocio || 'Não Definido';
    const comercial = activity.Comercial || 'Não Definido';
    if (negocio !== 'Não Definido') contatosPorNegocio[negocio] = (contatosPorNegocio[negocio] || 0) + 1;
    if (comercial !== 'Não Definido') contatosPorComercial[comercial] = (contatosPorComercial[comercial] || 0) + 1;
});
metrics['contatos-por-negocio'] = contatosPorNegocio;
metrics['contatos-por-comercial'] = contatosPorComercial;

// Gráfico de Clientes por Negócio (baseado nos clientes DA CARTEIRA FILTRADA)
const clientesPorNegocio = {};
clientsForPeriod.forEach(client => { // Usa clientsForPeriod (carteira filtrada)
    const negocio = client['Negócio'] || 'Não Definido'; // Usa 'Negócio'
    if (negocio !== 'Não Definido') {
        clientesPorNegocio[negocio] = (clientesPorNegocio[negocio] || 0) + 1;
    }
});
metrics['clientes-por-negocio'] = clientesPorNegocio;


// --- Distribuição Faltante (para card de cobertura) ---
const metaCoberturaPerc = (goals.coverage || 40) / 100;
const clientesTargetCount = Math.max(0, Math.ceil(metrics['total-clientes-unicos-cs'] * metaCoberturaPerc));
const clientesContatadosCount = metrics['cob-carteira']?.length || 0;
metrics['clientes-faltantes-meta-cobertura'] = Math.max(0, clientesTargetCount - clientesContatadosCount);

let ligacaoPerc = 0.65; // Padrão SMB
// Verifica segmento predominante na carteira filtrada
const midEntCount = clientsForPeriod.filter(c => normalizeText(c.Segmento || '').includes('mid') || normalizeText(c.Segmento || '').includes('enterprise')).length;
if (midEntCount > clientsForPeriod.length / 2) { // Se mais da metade for MID/ENT
    ligacaoPerc = 0.75;
}
const targetLigacoes = Math.ceil(clientesTargetCount * ligacaoPerc);
const targetEmails = clientesTargetCount - targetLigacoes;

const realizadoLigacoes = metrics['contato-ligacao']?.length || 0;
const realizadoEmails = metrics['contato-email']?.length || 0;

metrics['dist-faltante-ligacao'] = Math.max(0, targetLigacoes - realizadoLigacoes);
metrics['dist-faltante-email'] = Math.max(0, targetEmails - realizadoEmails);


return metrics;
};

const getPeriodDateRange = (period, year) => { // ... (código existente da função getPeriodDateRange) ... switch (period) {         case 'q1': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 2, 31, 23, 59, 59)) };         case 'q2': return { start: new Date(Date.UTC(year, 3, 1)), end: new Date(Date.UTC(year, 5, 30, 23, 59, 59)) };         case 'q3': return { start: new Date(Date.UTC(year, 6, 1)), end: new Date(Date.UTC(year, 8, 30, 23, 59, 59)) };         case 'q4': return { start: new Date(Date.UTC(year, 9, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59)) };         case 's1': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 5, 30, 23, 59, 59)) };         case 's2': return { start: new Date(Date.UTC(year, 6, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59)) };         case 'year': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59)) };         default: return null;     } };

const calculateMetricsForDateRange = (startDate, endDate, allActivities, allClients, selectedCS, includeOnboarding, goals) => { // ... (código existente da função calculateMetricsForDateRange) ... const periodMetrics = {         monthlyCoverages: [],         totalClientsInPeriod: new Set(),         playbookTotals: {}     };     const playbookKeys = ['plano', 'adocao', 'reunioes-qbr', 'planos-sucesso', 'followup', 'discovery', 'engajamento-smb', 'baixo-engajamento-mid'];     playbookKeys.forEach(key => periodMetrics.playbookTotals[key] = { previsto: 0, realizado: 0 });     const clientsForPeriodRange = (selectedCS === 'Todos') ? allClients : allClients.filter(d => d.CS?.trim() === selectedCS);

    for (let d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {         if (d > endDate) break; // Garante que não ultrapasse o fim do período         const currentMonth = d.getUTCMonth();         const currentYear = d.getUTCFullYear(); // Passa os clientes já filtrados pelo CS para a função mensal         const monthlyMetrics = calculateMetricsForPeriod(currentMonth, currentYear, allActivities, clientsForPeriodRange, selectedCS, includeOnboarding, goals);         const totalClientsUnicosCS = monthlyMetrics['total-clientes-unicos-cs'] || 0;         const clientesContatadosCount = monthlyMetrics['cob-carteira']?.length || 0;         const cobCarteiraPerc = totalClientsUnicosCS > 0 ? (clientesContatadosCount / totalClientsUnicosCS) * 100 : 0;         periodMetrics.monthlyCoverages.push(cobCarteiraPerc);

    // Adiciona clientes daquele MÊS ao Set geral do período
        (monthlyMetrics['total-clientes'] || []).forEach(client => periodMetrics.totalClientsInPeriod.add(client.Cliente));

        playbookKeys.forEach(key => {             periodMetrics.playbookTotals[key].previsto += (monthlyMetrics[${key}-previsto] || []).length;             const realizadoNoPrazo = (monthlyMetrics[${key}-realizado] || []).length;             const realizadoAtrasado = (monthlyMetrics[${key}-atrasado-concluido] || []).length;             periodMetrics.playbookTotals[key].realizado += realizadoNoPrazo + realizadoAtrasado;         });     }

    if (periodMetrics.monthlyCoverages.length > 0) {         const sum = periodMetrics.monthlyCoverages.reduce((acc, val) => acc + val, 0);         periodMetrics.averageCoverage = sum / periodMetrics.monthlyCoverages.length;     } else {         periodMetrics.averageCoverage = 0;     }     periodMetrics.totalUniqueClients = periodMetrics.totalClientsInPeriod.size;     return periodMetrics; };

// **** INÍCIO DA FUNÇÃO calculateNewOnboardingOKRs CORRIGIDA **** const calculateNewOnboardingOKRs = (month, year, activities, clients, teamView, selectedISM, processedClientsMap) => { // Objeto para armazenar os resultados const okrs = {}; // Datas de início e fim do mês para filtros const startOfMonth = new Date(Date.UTC(year, month, 1)); const endOfMonth = new Date(Date.UTC(year, month + 1, 0, 23, 59, 59)); const today = new Date(); // Para cálculo de duração de abertos

// --- 1. FILTRAGEM INICIAL POR FASE, TIME VIEW E ISM ---
// Começa com TODOS os clientes que JÁ FORAM processados no INIT e estão no MAPA
let onboardingClientsInPhase = [];
processedClientsMap.forEach(clientData => {
    if (clientData.FaseNorm === 'onboarding') {
        onboardingClientsInPhase.push(clientData); // Usa os dados do mapa
    }
});

// Filtra por Time View (Syneco/Outros/Geral)
let clientsForTeamView = [];
if (teamView === 'syneco') {
    clientsForTeamView = onboardingClientsInPhase.filter(c => normalizeText(c.Cliente).includes('syneco'));
} else if (teamView === 'outros') {
    clientsForTeamView = onboardingClientsInPhase.filter(c => !normalizeText(c.Cliente).includes('syneco'));
} else { // 'geral' ou se não especificado
    clientsForTeamView = [...onboardingClientsInPhase];
}

// Filtra por ISM selecionado (se não for 'Todos')
let filteredOnboardingClients = [];
if (selectedISM && selectedISM !== 'Todos') {
    filteredOnboardingClients = clientsForTeamView.filter(c => c.ISM === selectedISM);
} else {
    filteredOnboardingClients = [...clientsForTeamView]; // Usa todos do Time View
}
// Guarda a lista final de clientes filtrados (pode ser útil)
okrs.filteredClients = filteredOnboardingClients;
const filteredOnboardingClientNames = new Set(filteredOnboardingClients.map(c => c.ClienteNorm)); // Usa a chave normalizada

// Filtra as atividades APENAS para os clientes selecionados (usando rawActivities que já tem ClienteCompleto)
const filteredOnboardingActivities = rawActivities.filter(a => filteredOnboardingClientNames.has(a.ClienteCompleto));
// Filtra atividades concluídas DENTRO do mês selecionado
const concludedInPeriod = filteredOnboardingActivities.filter(a => a.ConcluidaEm >= startOfMonth && a.ConcluidaEm <= endOfMonth);

// --- 2. GO LIVE (TÍTULO EXATO) ---
const goLiveTitleNorm = 'call/reuniao de go live'; // Título exato normalizado

const goLiveActivitiesConcluded = concludedInPeriod.filter(a => {
    const tituloNorm = normalizeText(a['Título'] || a['Nome'] || a['Atividade'] || ''); // Tenta Título, Nome ou Atividade
    return tituloNorm === goLiveTitleNorm;
});

const goLiveActivitiesPredicted = filteredOnboardingActivities.filter(a => {
    const tituloNorm = normalizeText(a['Título'] || a['Nome'] || a['Atividade'] || '');
    return tituloNorm === goLiveTitleNorm &&
           !a.ConcluidaEm && // Não concluída ainda
           a.PrevisaoConclusao >= startOfMonth &&
           a.PrevisaoConclusao <= endOfMonth;
});
okrs.goLiveActivitiesConcluded = goLiveActivitiesConcluded;
okrs.goLiveActivitiesPredicted = goLiveActivitiesPredicted;

// --- NPS Pós Go Live (clientes da carteira filtrada) ---
const clientKeysWithGoLive = [...new Set(goLiveActivitiesConcluded.map(a => a.ClienteCompleto))];
// Filtra a lista JÁ FILTRADA de clientes (filteredOnboardingClients)
const clientsWithGoLiveAndNPSData = filteredOnboardingClients.filter(c =>
    clientKeysWithGoLive.includes(c.ClienteNorm) // Usa ClienteNorm do mapa
);
okrs.clientsWithGoLive = clientsWithGoLiveAndNPSData; // Clientes *filtrados* que tiveram Go Live no mês
okrs.npsResponded = clientsWithGoLiveAndNPSData.filter(c => c.NPSOnboarding && String(c.NPSOnboarding).trim() !== '');

// --- Clientes > R$50 mil (clientes da carteira filtrada) ---
okrs.highValueClients = filteredOnboardingClients.filter(c => c.ValorNaoFaturado > 50000);

// --- 3. TEMPO MÉDIO (APENAS CARTEIRA EM ANÁLISE + FASE != ONBOARDING) ---
const completedOnboardingsDetails = [];
const clientKeysWithCompletedGoLiveInPeriod = new Set(goLiveActivitiesConcluded.map(a => a.ClienteCompleto));

clientKeysWithCompletedGoLiveInPeriod.forEach(clientKey => {
    // Encontra a data MAIS RECENTE de Go Live para o cliente no período
    const goLiveDate = goLiveActivitiesConcluded
        .filter(a => a.ClienteCompleto === clientKey)
        .map(a => a.ConcluidaEm)
        .sort((a,b) => b - a)[0]; // Pega a data mais recente

    // Busca a data de início do onboarding para este cliente
    const playbookStartDate = clientOnboardingStartDate.get(clientKey);

    // Busca a informação MAIS ATUAL do cliente no MAPA (para checar a Fase)
    const clientInfoCurrent = processedClientsMap.get(clientKey);

    // Condição: Tem data de início, data de Go Live E a fase ATUAL NÃO é mais 'onboarding'
    if (goLiveDate && playbookStartDate && clientInfoCurrent && clientInfoCurrent.FaseNorm !== 'onboarding') {
        const duration = daysBetween(playbookStartDate, goLiveDate); // Usa a função auxiliar
        if (duration !== null && duration >= 0) { // Evita durações nulas ou negativas
            completedOnboardingsDetails.push({ client: clientInfoCurrent.Cliente, duration: duration }); // Usa nome original do mapa
        }
    }
});

okrs.completedOnboardingsList = completedOnboardingsDetails; // Lista detalhada

const durations = completedOnboardingsDetails.map(item => item.duration);
okrs.averageCompletedOnboardingTime = durations.length > 0
    ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length)
    : 0;

// --- 4. RANKING ABERTOS (FASE ONBOARDING + PLAYBOOK START vs HOJE) ---
const openOnboardingsDuration = [];

// Itera SOMENTE sobre os clientes FILTRADOS
filteredOnboardingClients.forEach(client => {
    const clientKey = client.ClienteNorm; // Já temos a chave normalizada
    // Busca a informação MAIS ATUAL do cliente (para checar a Fase)
    const clientInfoCurrent = processedClientsMap.get(clientKey);

    // Considera 'aberto' APENAS se a Fase ATUAL na planilha de Clientes AINDA é 'onboarding'
    if (clientInfoCurrent && clientInfoCurrent.FaseNorm === 'onboarding') {
        // Busca a data de início do onboarding
        const startDate = clientOnboardingStartDate.get(clientKey);

        if (startDate) {
            // Duração é da data de início até HOJE
            const duration = daysBetween(startDate, today); // Usa a função auxiliar
             if (duration !== null && duration >= 0) {
                 openOnboardingsDuration.push({ Cliente: client.Cliente, onboardingDuration: duration }); // Usa nome original
             }
        }
    }
});

// Ordena por duração descendente
openOnboardingsDuration.sort((a, b) => b.onboardingDuration - a.onboardingDuration);

okrs.top5LongestOnboardings = openOnboardingsDuration.slice(0, 5);
okrs.clientsOver120Days = openOnboardingsDuration.filter(item => item.onboardingDuration > 120);


// --- 5. MÉTRICAS DE PROCESSO (SLA e PRAZO COM CONCLUSÃO ANTECIPADA OK) ---
const processTargets = {
    // Nome normalizado da atividade : { config }
    'contato de welcome': { key: 'welcome', sla: 3 },
    'call/reuniao kickoff': { key: 'kickoff', sla: 7 },
    'planejamento finalizado': { key: 'planejamento', isDeadline: true },
    'call/reuniao de go live': { key: 'goLiveMeeting', isDeadline: true }, // Mesmo título do Go Live geral
    'mapear contatos': { key: 'mapearContatos', isDeadline: true },
    'acompanhamento / status reports': { key: 'acompanhamento', isDeadline: true }
};

// Inicializa os arrays nos OKRs
Object.values(processTargets).forEach(config => {
    okrs[`${config.key}Previsto`] = [];
    okrs[`${config.key}Realizado`] = [];
    if (config.sla) {
        okrs[`${config.key}SLAOk`] = [];
    }
});

// Itera UMA VEZ sobre as atividades filtradas do onboarding
filteredOnboardingActivities.forEach(a => {
    const atividadeNorm = normalizeText(a['Título'] || a['Nome'] || a['Atividade'] || ''); // Tenta Título, Nome ou Atividade
    const config = processTargets[atividadeNorm];

    if (config) { // Se a atividade for uma das que monitoramos
        const key = config.key;
        const previsao = a.PrevisaoConclusao;
        const conclusao = a.ConcluidaEm;

        // Verifica se a PREVISÃO está no mês
        if (previsao && previsao >= startOfMonth && previsao <= endOfMonth) {
            okrs[`${key}Previsto`].push(a);
        }

        // Verifica se foi CONCLUÍDA no mês
        if (conclusao && conclusao >= startOfMonth && conclusao <= endOfMonth) {
            okrs[`${key}Realizado`].push(a);

            // Se tem SLA, verifica se cumpriu (incluindo antes do prazo)
            if (config.sla && previsao) {
                const diffDays = daysBetween(previsao, conclusao); // Usa a função auxiliar
                // Está OK se foi concluída ATÉ o dia do SLA (negativo, zero ou até SLA dias depois)
                if (diffDays !== null && diffDays <= config.sla) {
                    okrs[`${key}SLAOk`].push(a);
                }
            }
            // Não precisamos mais checar `isDeadline` aqui,
            // a lista `Realizado` já contém todas as concluídas no mês.
        }
    }
});

// Retorna o objeto com todas as métricas calculadas
return okrs;
}; // **** FIM DA FUNÇÃO calculateNewOnboardingOKRs CORRIGIDA ****

// --- MANIPULADOR DE MENSAGENS DO WORKER ---

self.onmessage = (e) => { const { type, payload } = e.data;

try {
    if (type === 'INIT') {
        // 1. Recebe os dados brutos
        rawActivities = payload.rawActivities;
        rawClients = payload.rawClients;
        currentUser = payload.currentUser;
        manualEmailToCsMap = payload.manualEmailToCsMap;

        // 2. Processa e junta os dados das atividades
        processInitialData(); // Modifica rawActivities global

        // 3. Constrói o mapa de Squads
        buildCsToSquadMap(); // Usa rawClients global

        // 3.5. Cria o Mapa de Clientes Processados (para consulta rápida de Fase)
        processedClientsMap.clear(); // Limpa antes de popular
        rawClients.forEach(client => {
            const clientKey = (client.Cliente || '').trim().toLowerCase();
            if (clientKey) {
                processedClientsMap.set(clientKey, {
                    ...client, // Inclui todos os dados originais
                    ClienteNorm: clientKey, // Adiciona a chave normalizada
                    FaseNorm: normalizeText(client.Fase || '') // Adiciona a fase normalizada
                });
            }
        });
        console.log(`Worker: INIT - processedClientsMap populado com ${processedClientsMap.size} clientes.`);


        // 4. Extrai dados para os filtros
        const csSet = [...new Set(rawClients.map(d => d.CS && d.CS.trim()).filter(Boolean))];
        const squadSet = [...new Set(rawClients.map(d => d['Squad CS']).filter(Boolean))];
        const ismSet = [...new Set(rawClients.map(c => c.ISM).filter(Boolean))];
        const allDates = [...rawActivities.map(a => a.PrevisaoConclusao), ...rawActivities.map(a => a.ConcluidaEm)];
        const years = [...new Set(allDates.map(d => d?.getFullYear()).filter(y => y > 1900))].sort((a, b) => b - a); // Filtra anos inválidos e ordena


        // 5. Envia os dados de filtro de volta
        postMessage({
            type: 'INIT_COMPLETE',
            payload: {
                filterData: {
                    csSet,
                    squadSet,
                    ismSet,
                    years,
                    csToSquadMap: Object.fromEntries(csToSquadMap)
                }
            }
        });
    }

    else if (type === 'CALCULATE_MONTHLY') {
        console.log("Worker: Recebido CALCULATE_MONTHLY. processedClientsMap size:", processedClientsMap.size); // Log
         if (processedClientsMap.size === 0) {
             throw new Error("processedClientsMap está vazio ao calcular métricas mensais.");
         }

        // 1. Filtra clientes baseado nos filtros de CS/Squad
        let filteredClientsBase = rawClients; // Começa com todos
        let csForCalc = payload.selectedCS;

        // Aplica filtros de Admin (Squad ou CS específico)
         if (currentUser.isManager) {
             if (payload.selectedSquad && payload.selectedSquad !== 'Todos') {
                 filteredClientsBase = rawClients.filter(d => d['Squad CS'] === payload.selectedSquad);
                 csForCalc = 'Todos'; // Se filtrou por squad, cálculo geral é para 'Todos' CSs desse squad
             } else if (payload.selectedCS && payload.selectedCS !== 'Todos') {
                 filteredClientsBase = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
             }
             // Se ambos forem 'Todos', usa rawClients
         } else { // Se não for admin, filtra pelo CS do payload (que já foi definido na UI)
             filteredClientsBase = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
         }


        // 2. Calcula métricas do mês atual (Ongoing)
        // Passa a lista de clientes já filtrada por CS/Squad
        const dataStore = calculateMetricsForPeriod(
            payload.month,
            payload.year,
            rawActivities,         // Todas as atividades processadas
            filteredClientsBase,   // Clientes filtrados por CS/Squad
            csForCalc,             // 'Todos' ou CS específico
            payload.includeOnboarding,
            payload.goals
        );


        // 3. Calcula métricas de comparação (se necessário)
        if (payload.comparison !== 'none') {
             let compMes = payload.month, compAno = payload.year;
                if (payload.comparison === 'prev_month') {                     compMes--;                     if (compMes < 0) { compMes = 11; compAno--; }                 } else if (payload.comparison === 'prev_year') {                     compAno--;                 } // Usa a mesma base de clientes filtrada (filteredClientsBase) para comparação                 const comparisonMetrics = calculateMetricsForPeriod(compMes, compAno, rawActivities, filteredClientsBase, csForCalc, payload.includeOnboarding, payload.goals);

                // Adiciona dados de comparação ao dataStore (lógica mantida)                 dataStore['cob-carteira_comp_perc'] = (comparisonMetrics['cob-carteira']?.length || 0) * 100 / (comparisonMetrics['total-clientes-unicos-cs'] || 1);                 dataStore['sensescore-avg_comp'] = comparisonMetrics['sensescore-avg'];                 dataStore['ska-labs-realizado-list_comp_perc'] = ((comparisonMetrics['ska-labs-realizado-list']?.length || 0) * 100) / (comparisonMetrics['total-labs-eligible'] || 1); }

        // 4. Calcula OKRs de Onboarding (usando rawClients e filtros internos da função)
        console.log(`Worker: Chamando calculateNewOnboardingOKRs. processedClientsMap tem ${processedClientsMap.size} entradas.`);
        const onboardingDataStore = calculateNewOnboardingOKRs(
            payload.month,
            payload.year,
            rawActivities,     // Todas atividades processadas
            rawClients,        // Todos clientes brutos (função filtra internamente)
            payload.teamView,
            payload.selectedISM,
            processedClientsMap // Passa o mapa global
        );

        // 5. Calcula Atividades Atrasadas (baseado no CS selecionado para a visão geral)
        const overdueMetrics = calculateOverdueMetrics(rawActivities, payload.selectedCS); // Usa o selectedCS original do payload

        // 6. Calcula Divergência (baseado no CS selecionado para a visão geral)
        const concludedInPeriodActivities = rawActivities.filter(a => a.ConcluidaEm?.getUTCMonth() === payload.month && a.ConcluidaEm?.getUTCFullYear() === payload.year);
        const divergentActivities = concludedInPeriodActivities.filter(a => {
            const activityOwner = (a['Responsável'] || '').trim();
            // Divergente se o CS dono do CLIENTE é diferente do CS selecionado,
            // MAS o responsável pela ATIVIDADE é o CS selecionado (e não é a visão 'Todos')
            return a.CS && a.CS.trim() !== payload.selectedCS && activityOwner === payload.selectedCS && payload.selectedCS !== 'Todos';
        });
        dataStore['divergent-activities'] = divergentActivities; // Salva para o modal
        const divergentClientCount = new Set(divergentActivities.map(a => a.ClienteCompleto)).size;

        // 7. Envia tudo de volta
        postMessage({
            type: 'CALCULATION_COMPLETE',
            payload: {
                dataStore,
                onboardingDataStore,
                overdueMetrics,
                divergentClientCount
            }
        });
    }

    else if (type === 'CALCULATE_PERIOD') {
         const dateRange = getPeriodDateRange(payload.period, payload.year);
            if (dateRange) { // Para período, filtramos os clientes UMA VEZ baseado no selectedCS do payload const clientsForPeriodRange = (payload.selectedCS === 'Todos') ? rawClients : rawClients.filter(d => d.CS?.trim() === payload.selectedCS);

                const periodData = calculateMetricsForDateRange( dateRange.start, dateRange.end, rawActivities, clientsForPeriodRange, // Passa a lista de clientes já filtrada payload.selectedCS, payload.includeOnboarding, payload.goals );                 postMessage({                     type: 'PERIOD_CALCULATION_COMPLETE',                     payload: {                         periodData,                         periodName: payload.period // A thread principal mapeia o nome                     }                 });             } else { throw new Error(Período inválido recebido: ${payload.period}); } }

    else if (type === 'CALCULATE_TREND') {
         const { metricId, title, type: trendType, baseMonth, baseYear, selectedCS, selectedSquad, includeOnboarding /*, segmento (não usado diretamente aqui) */ } = payload;
            const labels = [];             const dataPoints = [];             const baseDate = new Date(Date.UTC(baseYear, baseMonth, 1)); // Usa UTC para consistência

            // Filtra clientes UMA VEZ baseado nos filtros de CS/Squad para a tendência             let filteredClientsForTrend = rawClients;             let csForTrendCalc = selectedCS; // CS a ser passado para calculateMetricsForPeriod

        // Aplica filtro de Squad ou CS específico (lógica similar a CALCULATE_MONTHLY)
         if (currentUser.isManager) {
             if (selectedSquad && selectedSquad !== 'Todos') {
                 filteredClientsForTrend = rawClients.filter(d => d['Squad CS'] === selectedSquad);
                 csForTrendCalc = 'Todos';
             } else if (selectedCS && selectedCS !== 'Todos') {
                 filteredClientsForTrend = rawClients.filter(d => d.CS?.trim() === selectedCS);
             }
             // Se ambos 'Todos', usa rawClients
         } else { // Não admin, usa o CS do payload
             filteredClientsForTrend = rawClients.filter(d => d.CS?.trim() === selectedCS);
         }
            for (let i = 5; i >= 0; i--) { // Calcula para 6 meses (atual e 5 anteriores) const date = new Date(baseDate); // Define o mês corretamente, considerando mudança de ano date.setUTCMonth(baseDate.getUTCMonth() - i);                 const month = date.getUTCMonth();                 const year = date.getUTCFullYear();

            // Formata label
            const monthLabel = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"][month];
                labels.push(${monthLabel}/${String(year).slice(2)});

            // Calcula métricas para o mês/ano específico, usando os clientes já filtrados
                const periodMetrics = calculateMetricsForPeriod( month, year, rawActivities, filteredClientsForTrend, // Usa a lista de clientes filtrada para a tendência csForTrendCalc, // Usa o CS ('Todos' ou específico) definido para a tendência includeOnboarding, {} // Metas não são necessárias para cálculo de tendência );

                let value = 0;                 if (trendType === 'okr') {                     switch (metricId) {                         case 'cob-carteira':                             const totalClients = periodMetrics['total-clientes-unicos-cs'] || 0;                             const contacted = periodMetrics['cob-carteira']?.length || 0;                             value = totalClients > 0 ? (contacted / totalClients) * 100 : 0;                             break;                         case 'sensescore-avg':                             value = periodMetrics['sensescore-avg'] || 0;                             break; // Adicione outros casos de OKR se necessário                     }                 } else if (trendType === 'playbook') {                     const realizado = (periodMetrics[${metricId}-realizado] || []).length;                     const atrasado = (periodMetrics[${metricId}-atrasado-concluido] || []).length;                     value = realizado + atrasado; // Soma ambos para o total realizado no mês                 } // Garante que o valor é numérico e com 2 casas decimais                 dataPoints.push(parseFloat(value.toFixed(2)));             }

            postMessage({                 type: 'TREND_DATA_COMPLETE',                 payload: { labels, dataPoints, title }             }); }

} catch (err) {
    console.error("Erro no Worker:", err); // Log detalhado do erro no console do worker
    // Envia erros de volta para a thread principal
    postMessage({
        type: 'ERROR',
        payload: {
            error: err.message,
            stack: err.stack // Envia o stack trace para depuração
        }
    });
}
};

// ======================================================= // FIM DO CÓDIGO PARA worker.js ```
