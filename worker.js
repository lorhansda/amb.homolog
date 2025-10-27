/**

===================================================================

WORKER DE PROCESSAMENTO DE OKRs

===================================================================

Este script é executado em uma thread separada.

Ele não pode acessar o DOM (window, document).

Ele se comunica com a thread principal através de postMessage e onmessage. */

// --- VARIÁVEIS GLOBAIS DO WORKER --- let rawActivities = [], rawClients = [], clientOnboardingStartDate = new Map(), // Armazena a data de início do playbook de onboarding por cliente csToSquadMap = new Map(), // Mapeia CS para seu Squad majoritário currentUser = {}, // Informações do usuário logado (passadas pela thread principal) manualEmailToCsMap = {}, // Mapeamento manual de email para CS (passado pela thread principal) processedClientsMap = new Map(); // Mapa para consulta rápida de dados de clientes (inclui FaseNorm), populado no INIT

// --- FUNÇÕES AUXILIARES DE PROCESSAMENTO ---

const normalizeText = (str = '') => String(str).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

const getQuarter = (date) => { if (!(date instanceof Date) || isNaN(date)) return null; return Math.floor(date.getUTCMonth() / 3); // Trimestre baseado no mês UTC (0-3) }

// Função auxiliar para calcular diferença em dias (UTC), tratando apenas a data const daysBetween = (date1, date2) => { if (!(date1 instanceof Date) || isNaN(date1) || !(date2 instanceof Date) || isNaN(date2)) { // console.warn("daysBetween recebeu data inválida:", date1, date2); // Descomente para depurar return null; // Retorna null se alguma data for inválida } // Cria novas datas UTC zerando horas/minutos/segundos para comparar apenas os dias const utcDate1 = Date.UTC(date1.getUTCFullYear(), date1.getUTCMonth(), date1.getUTCDate()); const utcDate2 = Date.UTC(date2.getUTCFullYear(), date2.getUTCMonth(), date2.getUTCDate()); return Math.floor((utcDate2 - utcDate1) / (1000 * 60 * 60 * 24)); };

const formatDate = (date) => { // Usada principalmente para debug no worker, se necessário if (!(date instanceof Date) || isNaN(date)) return ''; const day = String(date.getUTCDate()).padStart(2, '0'); const month = String(date.getUTCMonth() + 1).padStart(2, '0'); const year = date.getUTCFullYear(); return ${day}/${month}/${year}; };

const parseDate = (dateInput) => { if (!dateInput) return null; if (dateInput instanceof Date && !isNaN(dateInput)) return dateInput; // Retorna se já for Date válido

const dateStr = String(dateInput).trim();
const dateOnlyStr = dateStr.split(' ')[0]; // Pega só a parte da data
let parts;

// Tenta formato DD/MM/YYYY ou DD-MM-YYYY (comum no Brasil)
parts = dateOnlyStr.split(/[-/]/);
if (parts.length === 3 && parts[0]?.length <= 2 && parts[1]?.length <= 2 && parts[2]?.length === 4) {
    const day = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10);
    const year = parseInt(parts[2], 10);
    // Valida se os números formam uma data válida em UTC
    const date = new Date(Date.UTC(year, month - 1, day));
    if (!isNaN(date) && date.getUTCDate() === day && date.getUTCMonth() === month - 1 && date.getUTCFullYear() === year) {
         return date;
    }
}
// Tenta formato YYYY-MM-DD ou YYYY/MM/DD (ISOish)
 if (parts.length === 3 && parts[0]?.length === 4 && parts[1]?.length <= 2 && parts[2]?.length <= 2) {
    const year = parseInt(parts[0], 10);
    const month = parseInt(parts[1], 10);
    const day = parseInt(parts[2], 10);
    const date = new Date(Date.UTC(year, month - 1, day));
     if (!isNaN(date) && date.getUTCDate() === day && date.getUTCMonth() === month - 1 && date.getUTCFullYear() === year) {
         return date;
     }
}
 // Tenta formato MM/DD/YYYY ou MM-DD-YYYY (comum nos EUA) - Adicionado por segurança
if (parts.length === 3 && parts[0]?.length <= 2 && parts[1]?.length <= 2 && parts[2]?.length === 4) {
    const month = parseInt(parts[0], 10);
    const day = parseInt(parts[1], 10);
    const year = parseInt(parts[2], 10);
    const date = new Date(Date.UTC(year, month - 1, day));
     if (!isNaN(date) && date.getUTCDate() === day && date.getUTCMonth() === month - 1 && date.getUTCFullYear() === year) {
         // console.warn("Data parseada no formato MM/DD/YYYY:", dateInput); // Avisa se usou esse formato
         return date;
     }
}


// Tenta converter número serial do Excel (se aplicável, mas já deveria vir como Date da thread principal)
if (typeof dateInput === 'number' && dateInput > 20000 && dateInput < 60000) { // Faixa comum para seriais Excel
     try {
         const excelTimestamp = (dateInput - 25569) * 86400 * 1000;
         const date = new Date(excelTimestamp);
         // Ajusta para UTC, pegando os componentes da data local gerada
         const utcDate = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
          if (!isNaN(utcDate)) {
             // console.warn("Data parseada de número serial Excel:", dateInput, "->", utcDate);
             return utcDate;
         }
     } catch(e) { /* Ignora erro na conversão de serial */ }
}


// Se nada funcionou, retorna null
// console.warn("Worker: Falha ao parsear data:", dateInput); // Descomente para depurar datas problemáticas
return null;
};

// --- FUNÇÕES DE INICIALIZAÇÃO DE DADOS (Executadas uma vez no INIT) ---

// Constrói um mapa { CS -> Squad } baseado na maioria dos clientes daquele CS const buildCsToSquadMap = () => { csToSquadMap.clear(); const allCSs = [...new Set(rawClients.map(c => c.CS).filter(Boolean))];

allCSs.forEach(csName => {
    const clientsOfCS = rawClients.filter(c => c.CS === csName);
    const squadCol = 'Squad CS'; // Nome da coluna do Squad
    if (!clientsOfCS.some(c => c[squadCol])) return; // Pula se nenhum cliente do CS tem Squad

    // Conta a ocorrência de cada Squad para os clientes do CS
    const squadCounts = clientsOfCS.reduce((acc, client) => {
        const squad = client[squadCol];
        if (squad) acc[squad] = (acc[squad] || 0) + 1;
        return acc;
    }, {});

    // Encontra o Squad com maior contagem (majoritário)
    let majoritySquad = '';
    let maxCount = 0;
    for (const squad in squadCounts) {
        if (squadCounts[squad] > maxCount) {
            maxCount = squadCounts[squad];
            majoritySquad = squad;
        }
    }
    if (majoritySquad) csToSquadMap.set(csName, majoritySquad);
});
// console.log("Worker: Mapa CS->Squad construído:", Object.fromEntries(csToSquadMap)); // Debug
};

// Processa as atividades brutas para: // - Juntar dados do cliente correspondente (CS, Fase, Segmento, etc.) // - Parsear as colunas de data para objetos Date UTC // - Identificar a data de início do onboarding para cada cliente const processInitialData = () => { // Cria um mapa Cliente (lowercase) -> Objeto Cliente para busca rápida const clienteMap = new Map(rawClients.map(cli => [(cli.Cliente || '').trim().toLowerCase(), cli])); // Lista de nomes normalizados de playbooks que indicam o início do onboarding const onboardingPlaybooks = [ 'onboarding lantek', 'onboarding elétrica', 'onboarding altium', 'onboarding solidworks', 'onboarding usinagem', 'onboarding hp', 'onboarding alphacam', 'onboarding mkf', 'onboarding formlabs', 'tech journey' ];

clientOnboardingStartDate.clear(); // Limpa o mapa de datas de início

// Mapeia cada atividade para um novo objeto enriquecido
rawActivities = rawActivities.map(ativ => {
    const clienteKey = (ativ.Cliente || '').trim().toLowerCase();
    const clienteInfo = clienteMap.get(clienteKey) || {}; // Busca dados do cliente
    const criadoEmDate = parseDate(ativ['Criado em']); // Parseia data de criação
    const playbookNormalized = normalizeText(ativ.Playbook);

    // Encontra a data mais antiga de criação de um playbook de onboarding para este cliente
    if (playbookNormalized && onboardingPlaybooks.includes(playbookNormalized) && criadoEmDate) {
        const existingStartDate = clientOnboardingStartDate.get(clienteKey);
        if (!existingStartDate || criadoEmDate < existingStartDate) {
            clientOnboardingStartDate.set(clienteKey, criadoEmDate);
        }
    }

    // Retorna a atividade enriquecida
    return {
        ...ativ, // Mantém dados originais da atividade
        ClienteCompleto: clienteKey, // Nome normalizado para join e chaves de mapa
        ClienteOriginal: ativ.Cliente, // Nome original (útil para exibição)
        // Dados vindos da planilha de Clientes
        Segmento: clienteInfo.Segmento,
        CS: clienteInfo.CS,
        Squad: clienteInfo['Squad CS'],
        Fase: clienteInfo.Fase,
        ISM: clienteInfo.ISM,
        Negocio: clienteInfo['Negócio'], // Atenção ao acento
        Comercial: clienteInfo['Comercial'],
        NPSOnboarding: clienteInfo['NPS onboarding'],
        ValorNaoFaturado: parseFloat(String(clienteInfo['Valor total não faturado'] || '0').replace(/[^0-9,-]+/g, "").replace(",", ".")) || 0, // Trata valor monetário
        ModeloNegocio: clienteInfo['Modelo de negócio'], // Adiciona para OKR SMB CAD
        Potencial: clienteInfo['Potencial'],             // Adiciona para OKR SMB
        // Datas parseadas para UTC
        CriadoEm: criadoEmDate,
        PrevisaoConclusao: parseDate(ativ['Previsão de conclusão']),
        ConcluidaEm: parseDate(ativ['Concluída em'])
    };
});
// console.log("Worker: Atividades processadas. Exemplo:", rawActivities[0]); // Debug
// console.log("Worker: Datas de início Onboarding:", Object.fromEntries(clientOnboardingStartDate)); // Debug
};

// --- FUNÇÕES DE CÁLCULO DE MÉTRICAS ---

// Calcula as atividades atrasadas para um CS específico ou Todos const calculateOverdueMetrics = (activities, selectedCS) => { // ... (Mantida a lógica original, já estava correta) ... const metrics = {         overdueActivities: {}     };     const now = new Date();     const today = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())); // Usa UTC

    const csActivities = selectedCS === 'Todos' ?         activities :         activities.filter(a => (a['Responsável'] || '').trim() === selectedCS);

    const overdue = csActivities.filter(a => {         const dueDate = a.PrevisaoConclusao; // Já é Date UTC         return !a.ConcluidaEm && dueDate && dueDate < today; // Compara datas UTC     });

    const playbookDefs = {         'plano': { p: 'plano de acao', a: 'analise do cenario do cliente' },         'adocao': { p: null, a: ["revisao da adocao - sponsor (mid/enterprise)", "revisao da adocao - ku (mid/enterprise)", "revisao da adocao - ku (smb)"] },         'reunioes-qbr': { p: null, a: "ligacao de sensibilizacao com o sponsor" },         'planos-sucesso': { p: null, a: "plano de sucesso desenvolvido" },         'followup': { p: 'pos fechamento de loop cs', a: 'follow-up 1' },         'discovery': { p: 'discovery potencial', a: 'contato com o cliente' },         'engajamento-smb': { p: 'contato consolidado', a: 'contato com o cliente' },         'baixo-engajamento-mid': { p: null, a: "cliente com baixo engajamento - mid/enter" }     };

    for (const [key, def] of Object.entries(playbookDefs)) { let filterFn; if (key === 'plano') {             filterFn = (item) => {                 const atividadeNormalizada = normalizeText(item.Atividade || item['Título'] || '');                 const playbookNormalizado = normalizeText(item.Playbook);                 const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);                 const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');                 return isPlanoOriginal || isAlertaSuporte;             };         } else { filterFn = (item) => { const atividadeNormalizada = normalizeText(item.Atividade || item['Título'] || ''); const playbookNormalizado = normalizeText(item.Playbook); return def.p ? (playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => atividadeNormalizada.includes(term)) : atividadeNormalizada.includes(def.a)); }; }         metrics.overdueActivities[key] = overdue.filter(filterFn);     }     return metrics; };

// Calcula as métricas principais para um mês/ano específico, baseado em um conjunto pré-filtrado de clientes const calculateMetricsForPeriod = (month, year, allActivities, filteredClientsForPeriod, csForCalc, includeOnboarding, goals) => { // ... (Mantida a lógica interna revisada da versão anterior) ... const metrics = {};     const startOfPeriod = new Date(Date.UTC(year, month, 1)); const endOfPeriod = new Date(Date.UTC(year, month + 1, 0, 23, 59, 59)); // Fim do mês

 // Funções auxiliares de data para este período específico
    const isConcludedInPeriod = (item) => item.ConcluidaEm && item.ConcluidaEm >= startOfPeriod && item.ConcluidaEm <= endOfPeriod;     const isPredictedForPeriod = (activity) => {         const isDueInPeriod = activity.PrevisaoConclusao && activity.PrevisaoConclusao >= startOfPeriod && activity.PrevisaoConclusao <= endOfPeriod; // Considera previsto se vence no período E (não foi concluída OU foi concluída no período ou depois) const notCompletedBeforePeriod = !activity.ConcluidaEm || activity.ConcluidaEm >= startOfPeriod;         return isDueInPeriod && notCompletedBeforePeriod;     }; // Verifica se foi concluída NO período, mas estava prevista para ANTES do período     const isOverdueButCompletedInPeriod = (activity) => activity.ConcluidaEm && isConcludedInPeriod(activity) && activity.PrevisaoConclusao && activity.PrevisaoConclusao < startOfPeriod;

// Set dos clientes da carteira filtrada para busca rápida
const clientsForPeriodSet = new Set(filteredClientsForPeriod.map(c => (c.Cliente || '').trim().toLowerCase()));

// Filtra atividades concluídas no período E que pertencem aos clientes da carteira filtrada
const concludedActivitiesInPeriodAndPortfolio = allActivities.filter(a =>
    isConcludedInPeriod(a) && clientsForPeriodSet.has(a.ClienteCompleto)
);

// Filtra atividades previstas para o período E que pertencem aos clientes da carteira filtrada
const predictedActivitiesInPeriodAndPortfolio = allActivities.filter(a =>
    isPredictedForPeriod(a) && clientsForPeriodSet.has(a.ClienteCompleto)
);

 // Definições dos playbooks (mantidas)
 const playbookDefs = {
        'plano': { p: 'plano de acao', a: 'analise do cenario do cliente' },         'adocao': { p: null, a: ["revisao da adocao - sponsor (mid/enterprise)", "revisao da adocao - ku (mid/enterprise)", "revisao da adocao - ku (smb)"] },         'reunioes-qbr': { p: null, a: "ligacao de sensibilizacao com o sponsor" },         'planos-sucesso': { p: null, a: "plano de sucesso desenvolvido" },         'followup': { p: 'pos fechamento de loop cs', a: 'follow-up 1' },         'discovery': { p: 'discovery potencial', a: 'contato com o cliente' },         'engajamento-smb': { p: 'contato consolidado', a: 'contato com o cliente' },         'baixo-engajamento-mid': { p: null, a: "cliente com baixo engajamento - mid/enter" }     };

// Calcula Previsto, Realizado (no prazo) e Atrasado Concluído para playbooks
for (const [key, def] of Object.entries(playbookDefs)) {
    let filterFn;
     if (key === 'plano') {
            filterFn = (item) => {                 const atividadeNormalizada = normalizeText(item.Atividade || item['Título'] || '');                 const playbookNormalizado = normalizeText(item.Playbook);                 const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);                 const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');                 return isPlanoOriginal || isAlertaSuporte;             };         } else { filterFn = (item) => { const atividadeNormalizada = normalizeText(item.Atividade || item['Título'] || ''); const playbookNormalizado = normalizeText(item.Playbook); return def.p ? (playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => atividadeNormalizada.includes(term)) : atividadeNormalizada.includes(def.a)); }; }

    metrics[`${key}-previsto`] = predictedActivitiesInPeriodAndPortfolio.filter(filterFn);
    const concludedMatchingActivities = concludedActivitiesInPeriodAndPortfolio.filter(filterFn);
    metrics[`${key}-atrasado-concluido`] = concludedMatchingActivities.filter(isOverdueButCompletedInPeriod);
    metrics[`${key}-realizado`] = concludedMatchingActivities.filter(a => !isOverdueButCompletedInPeriod(a));
}

// --- Métricas Gerais e de Contato ---
 const validContactKeywords = ['e-mail', 'ligacao', 'reuniao', 'whatsapp', 'call sponsor'];
const isCountableContact = (a) => {
    const activityType = normalizeText(a['Tipo de Atividade'] || a['Tipo de atividade'] || a['Tipo'] || '');
    const activityName = normalizeText(a.Atividade || a['Título'] || ''); // Usar Atividade ou Título
    // Exclui regras e inclui tipos válidos ou nome começando com whatsapp octadesk
    return !activityName.includes('regra') && (validContactKeywords.some(type => activityType.includes(type)) || activityName.startsWith('whatsapp octadesk'));
};


// Filtra contatos concluídos que são contáveis DENTRO DA CARTEIRA E PERÍODO
const countableContactsInPeriod = concludedActivitiesInPeriodAndPortfolio.filter(isCountableContact);

// Separa contatos de CS e Onboarding (usa processedClientsMap para checar fase)
let csContactActivities = [];
let onboardingContactActivities = [];
countableContactsInPeriod.forEach(activity => {
    const clientInfo = processedClientsMap.get(activity.ClienteCompleto); // Usa o mapa global
    if (clientInfo?.FaseNorm !== 'onboarding') {
        csContactActivities.push(activity);
    } else if (includeOnboarding) { // Só adiciona se includeOnboarding for true
        onboardingContactActivities.push(activity);
    }
});

// Cobertura: Clientes únicos contatados (CS + Onboarding se incluído)
const contactsForCoverage = [...csContactActivities, ...onboardingContactActivities];
const uniqueClientContactsMap = new Map();
contactsForCoverage.forEach(activity => {
     if (!uniqueClientContactsMap.has(activity.ClienteCompleto)) {
         uniqueClientContactsMap.set(activity.ClienteCompleto, activity);
     }
});
metrics['cob-carteira'] = Array.from(uniqueClientContactsMap.values());


// Tipos de Contato (CS): Prioriza ligação/reunião
 const callKeywords = ['ligacao', 'reuniao', 'call sponsor'];
 const emailKeywords = ['e-mail', 'whatsapp']; // Inclui 'whatsapp' aqui

 const prioritizedCsContacts = new Map();
 csContactActivities.forEach(activity => {
     const clientKey = activity.ClienteCompleto;
     if (!clientKey) return;
     const activityType = normalizeText(activity['Tipo de Atividade'] || activity['Tipo de atividade'] || activity['Tipo'] || '');
     const activityName = normalizeText(activity.Atividade || activity['Título'] || '');
     let currentCategory = '';
     if (callKeywords.some(k => activityType.includes(k))) currentCategory = 'call';
     else if (emailKeywords.some(k => activityType.includes(k)) || activityName.startsWith('whatsapp octadesk')) currentCategory = 'email';

     const existing = prioritizedCsContacts.get(clientKey);
     if (currentCategory === 'call' || (currentCategory === 'email' && (!existing || existing.category !== 'call'))) {
          prioritizedCsContacts.set(clientKey, { activity, category: currentCategory });
     }
 });
 const finalCsContactList = Array.from(prioritizedCsContacts.values());
 metrics['contato-ligacao'] = finalCsContactList.filter(item => item.category === 'call').map(item => item.activity);
 metrics['contato-email'] = finalCsContactList.filter(item => item.category === 'email').map(item => item.activity);

// Contatos Onboarding (apenas para exibição se incluído)
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

// Métricas Gerais da Carteira (baseada em filteredClientsForPeriod)
// NOTE: clientsForPeriod aqui significa os clientes da carteira filtrada (CS/Squad) E pelo filtro Onboarding
metrics['total-clientes-unicos-cs'] = filteredClientsForPeriod.length;
metrics['total-clientes'] = filteredClientsForPeriod; // Guarda a lista exata usada para OKRs e gráficos
const senseScores = filteredClientsForPeriod
    .map(c => parseFloat(String(c['Sense Score'] || '0').replace(',', '.'))) // Trata vírgula e nulo
    .filter(s => !isNaN(s)); // Remove NaN
metrics['sensescore-avg'] = senseScores.length > 0 ? senseScores.reduce((acc, val) => acc + val, 0) / senseScores.length : 0;


// --- Engajamento SMB ---
 const smbConcluidoList = (metrics['engajamento-smb-realizado'] || []).concat(metrics['engajamento-smb-atrasado-concluido'] || []);
    const uniqueSmbContacts = new Map();     // Ordena por data de conclusão DESC para pegar o status mais recente smbConcluidoList.sort((a,b) => (b.ConcluidaEm?.getTime() || 0) - (a.ConcluidaEm?.getTime() || 0)).forEach(a => {         const status = normalizeText(a['Categoria']);         const clientKey = a.ClienteCompleto; // Pega apenas o primeiro status válido (engajado/desengajado) encontrado para o cliente no período         if (!uniqueSmbContacts.has(clientKey) && (status === 'engajado' || status === 'desengajado')) {             uniqueSmbContacts.set(clientKey, { activity: a, status: status });         }     });     const finalSmbList = Array.from(uniqueSmbContacts.values());     metrics['engajamento-smb-engajado'] = finalSmbList.filter(item => item.status === 'engajado').map(item => item.activity);     metrics['engajamento-smb-desengajado'] = finalSmbList.filter(item => item.status === 'desengajado').map(item => item.activity);

// --- Modelo de Negócio e Potencial (baseado nos clientes CONTATADOS NO PERÍODO) ---
const clientesContatados = metrics['cob-carteira'] || []; // Atividades de cobertura
const clientesContatadosComDados = clientesContatados.map(atividade => {
    // Busca dados do cliente no MAPA GERAL (processedClientsMap)
    const clienteData = processedClientsMap.get(atividade.ClienteCompleto);
    return {
        ...atividade,
        'Modelo de negócio': clienteData?.ModeloNegocio || 'Vazio', // Usa a propriedade adicionada no INIT
        'Potencial': clienteData?.Potencial || 'Vazio'           // Usa a propriedade adicionada no INIT
    };
});
metrics['modelo-negocio-preenchido'] = clientesContatadosComDados.filter(item => item['Modelo de negócio'] !== 'Vazio');
metrics['modelo-negocio-faltante'] = clientesContatadosComDados.filter(item => item['Modelo de negócio'] === 'Vazio');
metrics['cliente-potencial-preenchido'] = clientesContatadosComDados.filter(item => item['Potencial'] !== 'Vazio');
metrics['cliente-potencial-faltante'] = clientesContatadosComDados.filter(item => item['Potencial'] === 'Vazio');


// --- SKA LABS (Trimestral - baseado nos clientes DA CARTEIRA FILTRADA NO PERÍODO) ---
const selectedQuarter = getQuarter(startOfPeriod); // Trimestre do mês atual
const isConcludedInCurrentQuarter = (item) => item.ConcluidaEm && getQuarter(item.ConcluidaEm) === selectedQuarter && item.ConcluidaEm.getUTCFullYear() === year;

// Base são os clientes DA CARTEIRA FILTRADA (filteredClientsForPeriod)
const eligibleClientSetForLabs = new Set(filteredClientsForPeriod.map(c => (c.Cliente || '').trim().toLowerCase()));
metrics['total-labs-eligible'] = filteredClientsForPeriod.length; // Base é a carteira filtrada

// Busca atividades de labs concluídas no trimestre ATUAL E que pertencem aos clientes elegíveis
metrics['ska-labs-realizado-list'] = [...new Map(
    allActivities // Usa TODAS as atividades (já processadas)
    .filter(isConcludedInCurrentQuarter) // Filtra pelo trimestre atual
    .filter(a => normalizeText(a.Atividade || a['Título'] || '').includes('participou do ska labs')) // Filtra pelo nome
    .filter(a => eligibleClientSetForLabs.has(a.ClienteCompleto)) // Filtra pelos clientes da carteira no período
    .map(a => [a.ClienteCompleto, a]) // Cria mapa para pegar um por cliente
).values()];


// --- Gráficos de Pizza ---
// Contatos por Negócio/Comercial (baseado nos clientes CONTATADOS)
const contatosPorNegocio = {};
const contatosPorComercial = {};
(metrics['cob-carteira'] || []).forEach(activity => {
    const negocio = activity.Negocio || 'Não Definido';
    const comercial = activity.Comercial || 'Não Definido';
    if (negocio !== 'Não Definido') contatosPorNegocio[negocio] = (contatosPorNegocio[negocio] || 0) + 1;
    if (comercial !== 'Não Definido') contatosPorComercial[comercial] = (contatosPorComercial[comercial] || 0) + 1;
});
metrics['contatos-por-negocio'] = contatosPorNegocio;
metrics['contatos-por-comercial'] = contatosPorComercial;

// Clientes por Negócio (baseado nos clientes DA CARTEIRA FILTRADA no período)
const clientesPorNegocio = {};
filteredClientsForPeriod.forEach(client => {
    const negocio = client['Negócio'] || 'Não Definido'; // Usa 'Negócio'
    if (negocio !== 'Não Definido') {
        clientesPorNegocio[negocio] = (clientesPorNegocio[negocio] || 0) + 1;
    }
});
metrics['clientes-por-negocio'] = clientesPorNegocio;


// --- Distribuição Faltante (para card de cobertura) ---
 const metaCoberturaPerc = (goals.coverage || 40) / 100;
    const clientesTargetCount = Math.max(0, Math.ceil(metrics['total-clientes-unicos-cs'] * metaCoberturaPerc));     const clientesContatadosCount = metrics['cob-carteira']?.length || 0;     metrics['clientes-faltantes-meta-cobertura'] = Math.max(0, clientesTargetCount - clientesContatadosCount);

    let ligacaoPerc = 0.65; // Padrão SMB // Verifica segmento predominante na carteira filtrada (filteredClientsForPeriod) if (filteredClientsForPeriod.length > 0) { const midEntCount = filteredClientsForPeriod.filter(c => normalizeText(c.Segmento || '').includes('mid') || normalizeText(c.Segmento || '').includes('enterprise')).length; if (midEntCount > filteredClientsForPeriod.length / 2) { ligacaoPerc = 0.75; } }     const targetLigacoes = Math.ceil(clientesTargetCount * ligacaoPerc);     const targetEmails = clientesTargetCount - targetLigacoes;

    const realizadoLigacoes = metrics['contato-ligacao']?.length || 0;     const realizadoEmails = metrics['contato-email']?.length || 0;

    metrics['dist-faltante-ligacao'] = Math.max(0, targetLigacoes - realizadoLigacoes);     metrics['dist-faltante-email'] = Math.max(0, targetEmails - realizadoEmails);

return metrics;
};

// Calcula métricas agregadas para um período maior (trimestre, semestre, ano) const calculateMetricsForDateRange = (startDate, endDate, allActivities, allClients, selectedCS, includeOnboarding, goals) => { // ... (Mantida a lógica original, já estava correta) ... const periodMetrics = {         monthlyCoverages: [],         totalClientsInPeriod: new Set(),         playbookTotals: {}     };     const playbookKeys = ['plano', 'adocao', 'reunioes-qbr', 'planos-sucesso', 'followup', 'discovery', 'engajamento-smb', 'baixo-engajamento-mid'];     playbookKeys.forEach(key => periodMetrics.playbookTotals[key] = { previsto: 0, realizado: 0 });

// Filtra os clientes UMA VEZ para o período completo, baseado no CS selecionado
    const clientsForPeriodRange = (selectedCS === 'Todos') ? allClients : allClients.filter(d => d.CS?.trim() === selectedCS);

    // Itera sobre cada mês dentro do intervalo [startDate, endDate] let currentMonthDate = new Date(startDate);     while (currentMonthDate <= endDate) {         const currentMonth = currentMonthDate.getUTCMonth();         const currentYear = currentMonthDate.getUTCFullYear();

    // Calcula métricas mensais usando os clientes já filtrados para o período
        const monthlyMetrics = calculateMetricsForPeriod(currentMonth, currentYear, allActivities, clientsForPeriodRange, selectedCS, includeOnboarding, goals);

    // Agrega Cobertura Mensal
        const totalClientsUnicosCS = monthlyMetrics['total-clientes-unicos-cs'] || 0;         const clientesContatadosCount = monthlyMetrics['cob-carteira']?.length || 0;         const cobCarteiraPerc = totalClientsUnicosCS > 0 ? (clientesContatadosCount / totalClientsUnicosCS) * 100 : 0;         periodMetrics.monthlyCoverages.push(cobCarteiraPerc);

    // Adiciona clientes daquele MÊS ao Set geral do período
        (monthlyMetrics['total-clientes'] || []).forEach(client => { // Garante que estamos adicionando o nome original do cliente const clientKey = (client.Cliente || '').trim().toLowerCase(); const clientDataFromMap = processedClientsMap.get(clientKey); if (clientDataFromMap?.Cliente) { // Usa nome original do mapa se disponível periodMetrics.totalClientsInPeriod.add(clientDataFromMap.Cliente); } else if (client.Cliente) { // Senão, usa o nome que veio periodMetrics.totalClientsInPeriod.add(client.Cliente); } });

    // Agrega Totais de Playbooks
        playbookKeys.forEach(key => {             periodMetrics.playbookTotals[key].previsto += (monthlyMetrics[${key}-previsto] || []).length;             const realizadoNoPrazo = (monthlyMetrics[${key}-realizado] || []).length;             const realizadoAtrasado = (monthlyMetrics[${key}-atrasado-concluido] || []).length;             periodMetrics.playbookTotals[key].realizado += realizadoNoPrazo + realizadoAtrasado;         });

    // Avança para o próximo mês (cuidado com UTC)
    currentMonthDate.setUTCMonth(currentMonthDate.getUTCMonth() + 1);
    }

// Calcula Média de Cobertura
    if (periodMetrics.monthlyCoverages.length > 0) {         const sum = periodMetrics.monthlyCoverages.reduce((acc, val) => acc + val, 0);         periodMetrics.averageCoverage = sum / periodMetrics.monthlyCoverages.length;     } else {         periodMetrics.averageCoverage = 0;     }     periodMetrics.totalUniqueClients = periodMetrics.totalClientsInPeriod.size; // Tamanho do Set     return periodMetrics; };

// Retorna o intervalo de datas UTC para um período (q1, s1, year, etc.) const getPeriodDateRange = (period, year) => { // ... (Mantida a lógica original) ... switch (period) {         case 'q1': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 2, 31, 23, 59, 59, 999)) }; // Fim do dia         case 'q2': return { start: new Date(Date.UTC(year, 3, 1)), end: new Date(Date.UTC(year, 5, 30, 23, 59, 59, 999)) }; // Fim do dia         case 'q3': return { start: new Date(Date.UTC(year, 6, 1)), end: new Date(Date.UTC(year, 8, 30, 23, 59, 59, 999)) }; // Fim do dia         case 'q4': return { start: new Date(Date.UTC(year, 9, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)) };// Fim do dia         case 's1': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 5, 30, 23, 59, 59, 999)) }; // Fim do dia         case 's2': return { start: new Date(Date.UTC(year, 6, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)) };// Fim do dia         case 'year': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59, 999)) };// Fim do dia         default: return null;     } };

// **** INÍCIO DA FUNÇÃO calculateNewOnboardingOKRs CORRIGIDA **** const calculateNewOnboardingOKRs = (month, year, activities, clients, teamView, selectedISM, processedClientsMap) => { // Objeto para armazenar os resultados const okrs = {}; // Datas de início e fim do mês para filtros const startOfMonth = new Date(Date.UTC(year, month, 1)); const endOfMonth = new Date(Date.UTC(year, month + 1, 0, 23, 59, 59, 999)); // Fim do dia const today = new Date(); // Para cálculo de duração de abertos (não precisa ser UTC aqui)

// --- 1. FILTRAGEM INICIAL POR FASE, TIME VIEW E ISM ---
// Começa com TODOS os clientes que JÁ FORAM processados no INIT e estão no MAPA
let onboardingClientsInPhase = [];
processedClientsMap.forEach(clientData => {
    if (clientData.FaseNorm === 'onboarding') {
        onboardingClientsInPhase.push(clientData); // Usa os dados do mapa (que inclui FaseNorm)
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
// Guarda a lista final de clientes filtrados (pode ser útil para debug)
// okrs.filteredClients = filteredOnboardingClients; // Removido para não poluir o payload final
const filteredOnboardingClientNames = new Set(filteredOnboardingClients.map(c => c.ClienteNorm)); // Usa a chave normalizada do mapa

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

// --- NPS Pós Go Live (clientes da carteira filtrada que tiveram go live no mês) ---
const clientKeysWithGoLiveInPeriod = [...new Set(goLiveActivitiesConcluded.map(a => a.ClienteCompleto))];
// Filtra a lista JÁ FILTRADA de clientes (filteredOnboardingClients)
const clientsWithGoLiveAndNPSData = filteredOnboardingClients.filter(c =>
    clientKeysWithGoLiveInPeriod.includes(c.ClienteNorm) // Usa ClienteNorm do mapa
);
okrs.clientsWithGoLive = clientsWithGoLiveAndNPSData; // Clientes *filtrados* que tiveram Go Live no mês
okrs.npsResponded = clientsWithGoLiveAndNPSData.filter(c => c.NPSOnboarding && String(c.NPSOnboarding).trim() !== '');

// --- Clientes > R$50 mil (clientes da carteira filtrada que estão em onboarding) ---
okrs.highValueClients = filteredOnboardingClients.filter(c => c.ValorNaoFaturado > 50000);

// --- 3. TEMPO MÉDIO (APENAS CARTEIRA EM ANÁLISE + FASE ATUAL != ONBOARDING) ---
// Considera clientes que tiveram Go Live CONCLUÍDO *neste* mês
const completedOnboardingsDetails = [];
clientKeysWithGoLiveInPeriod.forEach(clientKey => {
    // Encontra a data MAIS RECENTE de Go Live para o cliente no período
    const goLiveDate = goLiveActivitiesConcluded
        .filter(a => a.ClienteCompleto === clientKey)
        .map(a => a.ConcluidaEm)
        .sort((a,b) => b.getTime() - a.getTime())[0]; // Pega a data mais recente

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

okrs.completedOnboardingsList = completedOnboardingsDetails; // Lista detalhada para o modal

const durations = completedOnboardingsDetails.map(item => item.duration);
okrs.averageCompletedOnboardingTime = durations.length > 0
    ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length)
    : 0; // Média de duração

// --- 4. RANKING ABERTOS (FASE ATUAL = ONBOARDING + PLAYBOOK START vs HOJE) ---
const openOnboardingsDuration = [];

// Itera SOMENTE sobre os clientes FILTRADOS por ISM/Time View
filteredOnboardingClients.forEach(client => {
    const clientKey = client.ClienteNorm; // Já temos a chave normalizada
    // Busca a informação MAIS ATUAL do cliente no MAPA (para checar a Fase)
    const clientInfoCurrent = processedClientsMap.get(clientKey);

    // Considera 'aberto' APENAS se a Fase ATUAL no mapa AINDA é 'onboarding'
    if (clientInfoCurrent && clientInfoCurrent.FaseNorm === 'onboarding') {
        // Busca a data de início do onboarding
        const startDate = clientOnboardingStartDate.get(clientKey);

        if (startDate) {
            // Duração é da data de início até HOJE (não precisa ser UTC para cálculo relativo)
            const duration = daysBetween(startDate, today); // Usa a função auxiliar
             if (duration !== null && duration >= 0) {
                 openOnboardingsDuration.push({ Cliente: client.Cliente, onboardingDuration: duration }); // Usa nome original
             }
        } else {
             // Cliente em onboarding sem data de início registrada (pode acontecer se o playbook não foi encontrado)
             // console.warn(`Cliente em Onboarding sem data de início: ${client.Cliente}`); // Debug
        }
    }
});

// Ordena por duração descendente
openOnboardingsDuration.sort((a, b) => b.onboardingDuration - a.onboardingDuration);

okrs.top5LongestOnboardings = openOnboardingsDuration.slice(0, 5); // Pega os 5 mais longos
okrs.clientsOver120Days = openOnboardingsDuration.filter(item => item.onboardingDuration > 120); // Filtra > 120 dias


// --- 5. MÉTRICAS DE PROCESSO (SLA e PRAZO COM CONCLUSÃO ANTECIPADA OK) ---
const processTargets = {
    // Nome normalizado da atividade : { config }
    'contato de welcome': { key: 'welcome', sla: 3 },
    'call/reuniao kickoff': { key: 'kickoff', sla: 7 },
    'planejamento finalizado': { key: 'planejamento', isDeadline: true },
    'call/reuniao de go live': { key: 'goLiveMeeting', isDeadline: true },
    'mapear contatos': { key: 'mapearContatos', isDeadline: true },
    'acompanhamento / status reports': { key: 'acompanhamento', isDeadline: true }
};

// Inicializa os arrays nos OKRs
Object.values(processTargets).forEach(config => {
    okrs[`${config.key}Previsto`] = [];
    okrs[`${config.key}Realizado`] = [];
    if (config.sla !== undefined) { // Verifica se tem SLA definido
        okrs[`${config.key}SLAOk`] = [];
    }
});

// Itera UMA VEZ sobre as atividades filtradas do onboarding
filteredOnboardingActivities.forEach(a => {
    const atividadeNorm = normalizeText(a['Título'] || a['Nome'] || a['Atividade'] || ''); // Tenta Título, Nome ou Atividade
    const config = processTargets[atividadeNorm];

    if (config) { // Se a atividade for uma das que monitoramos
        const key = config.key;
        const previsao = a.PrevisaoConclusao; // Já é Date UTC
        const conclusao = a.ConcluidaEm;   // Já é Date UTC

        // Verifica se a PREVISÃO está no mês
        if (previsao && previsao >= startOfMonth && previsao <= endOfMonth) {
            okrs[`${key}Previsto`].push(a);
        }

        // Verifica se foi CONCLUÍDA no mês
        if (conclusao && conclusao >= startOfMonth && conclusao <= endOfMonth) {
            okrs[`${key}Realizado`].push(a);

            // Se tem SLA, verifica se cumpriu (incluindo antes do prazo)
            if (config.sla !== undefined && previsao) {
                const diffDays = daysBetween(previsao, conclusao); // Usa a função auxiliar
                // Está OK se foi concluída ATÉ o dia do SLA (negativo, zero ou até SLA dias depois)
                if (diffDays !== null && diffDays <= config.sla) {
                    okrs[`${key}SLAOk`].push(a);
                }
            }
        }
    }
});

// Retorna o objeto com todas as métricas calculadas
return okrs;
}; // **** FIM DA FUNÇÃO calculateNewOnboardingOKRs CORRIGIDA ****

// --- MANIPULADOR DE MENSAGENS DO WORKER ---

self.onmessage = (e) => { const { type, payload } = e.data; // console.log("Worker: Mensagem recebida:", type, payload); // Log geral

try {
    if (type === 'INIT') {
        console.log("Worker: Iniciando INIT...");
        // 1. Recebe os dados brutos
        rawActivities = payload.rawActivities || []; // Garante que seja array
        rawClients = payload.rawClients || [];     // Garante que seja array
        currentUser = payload.currentUser || {};
        manualEmailToCsMap = payload.manualEmailToCsMap || {};
         console.log(`Worker: Dados brutos recebidos. Atividades: ${rawActivities.length}, Clientes: ${rawClients.length}`);


        // 2. Processa e junta os dados das atividades
        processInitialData(); // Modifica rawActivities global
        console.log(`Worker: Atividades processadas. Exemplo primeira:`, rawActivities[0]);


        // 3. Constrói o mapa de Squads
        buildCsToSquadMap(); // Usa rawClients global
        console.log(`Worker: Mapa CS->Squad construído com ${csToSquadMap.size} entradas.`);


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
         // Extrai anos válidos das datas parseadas e ordena
         const allValidDates = [
             ...rawActivities.map(a => a.PrevisaoConclusao),
             ...rawActivities.map(a => a.ConcluidaEm),
             ...rawActivities.map(a => a.CriadoEm) // Inclui CriadoEm para garantir anos mais antigos
         ].filter(d => d instanceof Date && !isNaN(d)); // Filtra apenas datas válidas
        const years = [...new Set(allValidDates.map(d => d.getUTCFullYear()))]
                       .filter(y => y > 1900 && y < 2100) // Filtra anos plausíveis
                       .sort((a, b) => b - a); // Ordena do mais recente para o mais antigo

         console.log(`Worker: Filtros extraídos - CSs: ${csSet.length}, Squads: ${squadSet.length}, ISMs: ${ismSet.length}, Anos: ${years.join(', ')}`);


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
         console.log("Worker: INIT concluído e mensagem enviada.");

    }

    else if (type === 'CALCULATE_MONTHLY') {
        console.log("Worker: Iniciando CALCULATE_MONTHLY...");
         if (processedClientsMap.size === 0) {
             console.error("Worker: ERRO - processedClientsMap está vazio ao iniciar CALCULATE_MONTHLY.");
             throw new Error("processedClientsMap está vazio. Dados iniciais podem não ter sido processados corretamente.");
         }

        // 1. Filtra clientes base para cálculos (CS/Squad)
        let filteredClientsBase = rawClients;
        let csForCalc = payload.selectedCS; // CS a ser passado para calculateMetricsForPeriod

         if (currentUser.isManager) {
             if (payload.selectedSquad && payload.selectedSquad !== 'Todos') {
                 filteredClientsBase = rawClients.filter(d => d['Squad CS'] === payload.selectedSquad);
                 csForCalc = 'Todos'; // Cálculo geral para o Squad
                 console.log(`Worker: Filtro Admin - Squad: ${payload.selectedSquad}, Clientes: ${filteredClientsBase.length}`);
             } else if (payload.selectedCS && payload.selectedCS !== 'Todos') {
                 filteredClientsBase = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
                 console.log(`Worker: Filtro Admin - CS: ${payload.selectedCS}, Clientes: ${filteredClientsBase.length}`);

             } else {
                  console.log(`Worker: Filtro Admin - Todos CS/Squads, Clientes: ${filteredClientsBase.length}`);
             }
         } else { // Não admin, usa o CS do payload (definido na UI)
              if(payload.selectedCS) { // Garante que há um CS selecionado
                 filteredClientsBase = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
                 console.log(`Worker: Filtro Usuário - CS: ${payload.selectedCS}, Clientes: ${filteredClientsBase.length}`);
              } else {
                 console.warn(`Worker: Usuário não admin sem CS selecionado no payload.`);
                 filteredClientsBase = []; // Evita calcular para todos se o CS não estiver definido
              }

         }


        // 2. Calcula métricas do mês atual (Ongoing + base para outros)
        const dataStore = calculateMetricsForPeriod(
            payload.month, payload.year, rawActivities, filteredClientsBase,
            csForCalc, payload.includeOnboarding, payload.goals || {} // Garante que goals seja objeto
        );
        console.log("Worker: Métricas do período calculadas (dataStore).");


        // 3. Calcula métricas de comparação (se necessário)
        if (payload.comparison && payload.comparison !== 'none') {
             console.log(`Worker: Calculando comparação: ${payload.comparison}`);
             let compMes = payload.month, compAno = payload.year;
                if (payload.comparison === 'prev_month') { /* ... lógica de data ... / } else if (payload.comparison === 'prev_year') { / ... lógica de data ... */ }

                const comparisonMetrics = calculateMetricsForPeriod(compMes, compAno, rawActivities, filteredClientsBase, csForCalc, payload.includeOnboarding, payload.goals || {}); // Adiciona dados de comparação ao dataStore dataStore['cob-carteira_comp_perc'] = (comparisonMetrics['cob-carteira']?.length || 0) * 100 / (comparisonMetrics['total-clientes-unicos-cs'] || 1);                 dataStore['sensescore-avg_comp'] = comparisonMetrics['sensescore-avg'];                 dataStore['ska-labs-realizado-list_comp_perc'] = ((comparisonMetrics['ska-labs-realizado-list']?.length || 0) * 100) / (comparisonMetrics['total-labs-eligible'] || 1); console.log("Worker: Métricas de comparação calculadas."); }

        // 4. Calcula OKRs de Onboarding (usando rawClients e filtros internos + mapa)
        console.log(`Worker: Chamando calculateNewOnboardingOKRs. processedClientsMap tem ${processedClientsMap.size} entradas.`);
        const onboardingDataStore = calculateNewOnboardingOKRs(
            payload.month, payload.year, rawActivities, rawClients,
            payload.teamView, payload.selectedISM, processedClientsMap
        );
        console.log("Worker: Métricas de Onboarding calculadas.");


        // 5. Calcula Atividades Atrasadas (baseado no CS selecionado na UI)
        const overdueMetrics = calculateOverdueMetrics(rawActivities, payload.selectedCS);
        console.log("Worker: Métricas de Atraso calculadas.");


        // 6. Calcula Divergência (baseado no CS selecionado na UI)
        const concludedInPeriodActivities = rawActivities.filter(a => a.ConcluidaEm?.getUTCMonth() === payload.month && a.ConcluidaEm?.getUTCFullYear() === payload.year);
        const divergentActivities = concludedInPeriodActivities.filter(a => {
            const activityOwner = (a['Responsável'] || '').trim();
            // Usa CS da atividade (enriquecido no INIT) vs CS do payload
            return a.CS && a.CS.trim() !== payload.selectedCS && activityOwner === payload.selectedCS && payload.selectedCS !== 'Todos';
        });
        dataStore['divergent-activities'] = divergentActivities;
        const divergentClientCount = new Set(divergentActivities.map(a => a.ClienteCompleto)).size;
        console.log(`Worker: Divergência calculada: ${divergentClientCount} clientes.`);


        // 7. Envia tudo de volta
        postMessage({
            type: 'CALCULATION_COMPLETE',
            payload: { dataStore, onboardingDataStore, overdueMetrics, divergentClientCount }
        });
         console.log("Worker: CALCULATE_MONTHLY concluído e mensagem enviada.");

    }

    else if (type === 'CALCULATE_PERIOD') {
         console.log(`Worker: Iniciando CALCULATE_PERIOD para ${payload.period}...`);
         const dateRange = getPeriodDateRange(payload.period, payload.year);
            if (dateRange) { // Filtra clientes UMA VEZ para o período completo const clientsForPeriodRange = (payload.selectedCS === 'Todos') ? rawClients : rawClients.filter(d => d.CS?.trim() === payload.selectedCS); console.log(Worker: Clientes filtrados para o período: ${clientsForPeriodRange.length});

                const periodData = calculateMetricsForDateRange( dateRange.start, dateRange.end, rawActivities, clientsForPeriodRange, payload.selectedCS, payload.includeOnboarding, payload.goals || {} ); console.log("Worker: Métricas de período calculadas.");

                postMessage({                     type: 'PERIOD_CALCULATION_COMPLETE',                     payload: { periodData, periodName: payload.period }                 }); console.log("Worker: CALCULATE_PERIOD concluído e mensagem enviada.");

            } else { console.error(Worker: Período inválido recebido: ${payload.period}); throw new Error(Período inválido recebido: ${payload.period}); } }

    else if (type === 'CALCULATE_TREND') {
        console.log(`Worker: Iniciando CALCULATE_TREND para ${payload.metricId}...`);
         const { metricId, title, type: trendType, baseMonth, baseYear, selectedCS, selectedSquad, includeOnboarding } = payload;
            const labels = [];             const dataPoints = [];             const baseDate = new Date(Date.UTC(baseYear, baseMonth, 1));

            // Filtra clientes UMA VEZ baseado nos filtros de CS/Squad para a tendência             let filteredClientsForTrend = rawClients;             let csForTrendCalc = selectedCS;

         if (currentUser.isManager) {
             if (selectedSquad && selectedSquad !== 'Todos') {
                 filteredClientsForTrend = rawClients.filter(d => d['Squad CS'] === selectedSquad);
                 csForTrendCalc = 'Todos';
             } else if (selectedCS && selectedCS !== 'Todos') {
                 filteredClientsForTrend = rawClients.filter(d => d.CS?.trim() === selectedCS);
             }
         } else {
             filteredClientsForTrend = rawClients.filter(d => d.CS?.trim() === selectedCS);
         }
         console.log(`Worker: Clientes filtrados para tendência: ${filteredClientsForTrend.length}`);
            for (let i = 5; i >= 0; i--) { // Calcula para 6 meses const date = new Date(baseDate); date.setUTCMonth(baseDate.getUTCMonth() - i);                 const month = date.getUTCMonth();                 const year = date.getUTCFullYear();

            const monthLabel = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"][month];
                labels.push(${monthLabel}/${String(year).slice(2)});

                const periodMetrics = calculateMetricsForPeriod( month, year, rawActivities, filteredClientsForTrend, csForTrendCalc, includeOnboarding, {} );

                let value = 0;                 if (trendType === 'okr') {                     switch (metricId) {                         case 'cob-carteira':                             const totalClients = periodMetrics['total-clientes-unicos-cs'] || 0;                             const contacted = periodMetrics['cob-carteira']?.length || 0;                             value = totalClients > 0 ? (contacted / totalClients) * 100 : 0;                             break;                         case 'sensescore-avg':                             value = periodMetrics['sensescore-avg'] || 0;                             break; // Adicione outros OKRs se precisar de tendência para eles                     }                 } else if (trendType === 'playbook') {                     const realizado = (periodMetrics[${metricId}-realizado] || []).length;                     const atrasado = (periodMetrics[${metricId}-atrasado-concluido] || []).length;                     value = realizado + atrasado;                 }                 dataPoints.push(parseFloat(value.toFixed(2))); // Garante número com 2 casas decimais             } console.log("Worker: Dados de tendência calculados.");

            postMessage({                 type: 'TREND_DATA_COMPLETE',                 payload: { labels, dataPoints, title }             }); console.log("Worker: CALCULATE_TREND concluído e mensagem enviada.");

    }

} catch (err) {
    console.error("Erro no Worker:", err.message, err.stack); // Log detalhado do erro no console do worker
    // Envia erros de volta para a thread principal
    postMessage({
        type: 'ERROR',
        payload: {
            error: `Worker Error: ${err.message}`, // Adiciona prefixo para clareza
            stack: err.stack // Envia o stack trace para depuração
        }
    });
}
};

// =======================================================// FIM DO CÓDIGO worker.js```
