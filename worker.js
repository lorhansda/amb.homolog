/**
 * ===================================================================
 * WORKER DE PROCESSAMENTO DE OKRs
 * ===================================================================
 * Este script é executado em uma thread separada.
 * Ele não pode acessar o DOM (window, document).
 * Ele se comunica com a thread principal através de `postMessage` e `onmessage`.
 */

// --- VARIÁVEIS GLOBAIS DO WORKER ---
let rawActivities = [],
    rawClients = [],
    clientOnboardingStartDate = new Map(),
    csToSquadMap = new Map(),
    currentUser = {},
    manualEmailToCsMap = {};

// --- FUNÇÕES AUXILIARES DE PROCESSAMENTO (Movidas do script principal) ---

const normalizeText = (str = '') => String(str).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');

const getQuarter = (date) => Math.floor(date.getUTCMonth() / 3); // Corrigido para getUTCMonth

const formatDate = (date) => {
    if (!(date instanceof Date) || isNaN(date)) return '';
    const day = String(date.getUTCDate()).padStart(2, '0');
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const year = date.getUTCFullYear();
    return `${day}/${month}/${year}`;
};

const parseDate = (dateInput) => {
    if (!dateInput) return null;
    if (dateInput instanceof Date && !isNaN(dateInput)) return dateInput;

    const dateStr = String(dateInput).trim();
    const dateOnlyStr = dateStr.split(' ')[0];
    let parts;

    parts = dateOnlyStr.split(/[-/]/);
    if (parts.length === 3 && parts[2].length === 4) {
        const day = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10);
        const year = parseInt(parts[2], 10);
        if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
            return new Date(Date.UTC(year, month - 1, day));
        }
    }
    if (parts.length === 3 && parts[0].length === 4) {
        const year = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10);
        const day = parseInt(parts[2], 10);
        if (!isNaN(year) && !isNaN(month) && !isNaN(day)) {
            return new Date(Date.UTC(year, month - 1, day));
        }
    }
    return null;
};

// --- FUNÇÕES DE INICIALIZAÇÃO DE DADOS (Executadas uma vez) ---

const buildCsToSquadMap = () => {
    csToSquadMap.clear(); // Limpa o mapa
    const allCSs = [...new Set(rawClients.map(c => c.CS).filter(Boolean))];

    allCSs.forEach(csName => {
        const clientsOfCS = rawClients.filter(c => c.CS === csName);
        if (!clientsOfCS.some(c => c['Squad CS'])) return;

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

const processInitialData = () => {
    const clienteMap = new Map(rawClients.map(cli => [(cli.Cliente || '').trim().toLowerCase(), cli]));
    const onboardingPlaybooks = [
        'onboarding lantek', 'onboarding elétrica', 'onboarding altium',
        'onboarding solidworks', 'onboarding usinagem', 'onboarding hp',
        'onboarding alphacam', 'onboarding mkf', 'onboarding formlabs', 'tech journey'
    ];

    clientOnboardingStartDate.clear(); // Limpa o mapa

    // Esta função agora MODIFICA o `rawActivities` global do worker
    rawActivities = rawActivities.map(ativ => {
        const clienteKey = (ativ.Cliente || '').trim().toLowerCase();
        const clienteInfo = clienteMap.get(clienteKey) || {};
        const criadoEmDate = parseDate(ativ['Criado em']);
        const playbookNormalized = normalizeText(ativ.Playbook);

        if (onboardingPlaybooks.includes(playbookNormalized) && criadoEmDate) {
            const existingStartDate = clientOnboardingStartDate.get(clienteKey);
            if (!existingStartDate || criadoEmDate < existingStartDate) {
                clientOnboardingStartDate.set(clienteKey, criadoEmDate);
            }
        }

        return {
            ...ativ,
            ClienteCompleto: clienteKey,
            Segmento: clienteInfo.Segmento,
            CS: clienteInfo.CS,
            Squad: clienteInfo['Squad CS'],
            Fase: clienteInfo.Fase,
            ISM: clienteInfo.ISM,
            Negocio: clienteInfo['Negócio'],
            Comercial: clienteInfo['Comercial'],
            CriadoEm: criadoEmDate,
            PrevisaoConclusao: parseDate(ativ['Previsão de conclusão']),
            ConcluidaEm: parseDate(ativ['Concluída em']),
            NPSOnboarding: clienteInfo['NPS onboarding'],
            ValorNaoFaturado: parseFloat(String(clienteInfo['Valor total não faturado'] || '0').replace(/[^0-9,-]+/g, "").replace(",", "."))
        };
    });
};

// --- FUNÇÕES DE CÁLCULO PRINCIPAIS (Executadas a cada mudança de filtro) ---

const calculateOverdueMetrics = (activities, selectedCS) => {
    const metrics = {
        overdueActivities: {}
    };
    const now = new Date();
    const today = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()));

    const csActivities = selectedCS === 'Todos' ?
        activities :
        activities.filter(a => (a['Responsável'] || '').trim() === selectedCS);

    const overdue = csActivities.filter(a => {
        const dueDate = a.PrevisaoConclusao;
        return !a.ConcluidaEm && dueDate && dueDate < today;
    });

    const playbookDefs = {
        'plano': { p: 'plano de acao', a: 'analise do cenario do cliente' },
        'adocao': { p: null, a: ["revisao da adocao - sponsor (mid/enterprise)", "revisao da adocao - ku (mid/enterprise)", "revisao da adocao - ku (smb)"] },
        'reunioes-qbr': { p: null, a: "ligacao de sensibilizacao com o sponsor" },
        'planos-sucesso': { p: null, a: "plano de sucesso desenvolvido" },
        'followup': { p: 'pos fechamento de loop cs', a: 'follow-up 1' },
        'discovery': { p: 'discovery potencial', a: 'contato com o cliente' },
        'engajamento-smb': { p: 'contato consolidado', a: 'contato com o cliente' },
        'baixo-engajamento-mid': { p: null, a: "cliente com baixo engajamento - mid/enter" }
    };

    for (const [key, def] of Object.entries(playbookDefs)) {
        let filterFn = (item) => def.p ? (normalizeText(item.Playbook).includes(def.p) && normalizeText(item.Atividade).includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => normalizeText(item.Atividade).includes(term)) : normalizeText(item.Atividade).includes(def.a));
        if (key === 'plano') {
            filterFn = (item) => {
                const atividadeNormalizada = normalizeText(item.Atividade);
                const playbookNormalizado = normalizeText(item.Playbook);
                const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);
                const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');
                return isPlanoOriginal || isAlertaSuporte;
            };
        }
        metrics.overdueActivities[key] = overdue.filter(filterFn);
    }
    // !! REMOVIDO O BLOCO INCORRETO DAQUI !!
    return metrics;
};

const calculateMetricsForPeriod = (month, year, activities, clients, selectedCS, includeOnboarding, goals) => {
    const metrics = {};
    const startOfPeriod = new Date(Date.UTC(year, month, 1));

    const isConcludedInPeriod = (item) => item.ConcluidaEm?.getUTCMonth() === month && item.ConcluidaEm?.getUTCFullYear() === year;
    const isPredictedForPeriod = (activity) => {
        const isDueInPeriod = activity.PrevisaoConclusao?.getUTCMonth() === month && activity.PrevisaoConclusao?.getUTCFullYear() === year;
        const notCompletedBeforePeriod = !activity.ConcluidaEm || activity.ConcluidaEm >= startOfPeriod;
        return isDueInPeriod && notCompletedBeforePeriod;
    };
    const isOverdue = (activity) => activity.PrevisaoConclusao && activity.PrevisaoConclusao < startOfPeriod;

    const clientOwnerMap = new Map(rawClients.map(cli => [(cli.Cliente || '').trim().toLowerCase(), {
        cs: cli.CS,
        fase: normalizeText(cli.Fase || '')
    }]));

    // ESTA É A DEFINIÇÃO CORRETA DE clientsForPeriod
    const clientsForPeriod = includeOnboarding ? clients : clients.filter(c => normalizeText(c.Fase) !== 'onboarding');

    const allConcludedActivitiesInPortfolio = rawActivities
        .filter(isConcludedInPeriod)
        .filter(a => clients.some(c => (c.Cliente || '').trim().toLowerCase() === a.ClienteCompleto));

    let csRealizedActivities = [];
    let onboardingRealizedActivities = [];

    allConcludedActivitiesInPortfolio.forEach(activity => {
        const clientInfo = clientOwnerMap.get(activity.ClienteCompleto);
        const isClientOnboarding = clientInfo?.fase === 'onboarding';
        if (!isClientOnboarding) {
            const activityOwner = (activity['Responsável'] || '').trim();
            if (selectedCS === 'Todos' || activityOwner === selectedCS) {
                csRealizedActivities.push(activity);
            }
        } else {
            onboardingRealizedActivities.push(activity);
        }
    });

    const combinedRealizedActivities = [...csRealizedActivities, ...onboardingRealizedActivities];

    const playbookDefs = {
        'plano': { p: 'plano de acao', a: 'analise do cenario do cliente' },
        'adocao': { p: null, a: ["revisao da adocao - sponsor (mid/enterprise)", "revisao da adocao - ku (mid/enterprise)", "revisao da adocao - ku (smb)"] },
        'reunioes-qbr': { p: null, a: "ligacao de sensibilizacao com o sponsor" },
        'planos-sucesso': { p: null, a: "plano de sucesso desenvolvido" },
        'followup': { p: 'pos fechamento de loop cs', a: 'follow-up 1' },
        'discovery': { p: 'discovery potencial', a: 'contato com o cliente' },
        'engajamento-smb': { p: 'contato consolidado', a: 'contato com o cliente' },
        'baixo-engajamento-mid': { p: null, a: "cliente com baixo engajamento - mid/enter" }
    };

    for (const [key, def] of Object.entries(playbookDefs)) {
        let filterFn;
        if (key === 'plano') {
            filterFn = (item) => {
                const atividadeNormalizada = normalizeText(item.Atividade);
                const playbookNormalizado = normalizeText(item.Playbook);
                const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);
                const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');
                return isPlanoOriginal || isAlertaSuporte;
            };
        } else {
            filterFn = (item) => def.p ? (normalizeText(item.Playbook).includes(def.p) && normalizeText(item.Atividade).includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => normalizeText(item.Atividade).includes(term)) : normalizeText(item.Atividade).includes(def.a));
        }
        const totalRealizadoList = combinedRealizedActivities.filter(filterFn);
        metrics[`${key}-atrasado-concluido`] = totalRealizadoList.filter(isOverdue);
        metrics[`${key}-realizado`] = totalRealizadoList.filter(a => !isOverdue(a));
    }

    const validContactKeywords = ['e-mail', 'ligacao', 'reuniao', 'whatsapp', 'call sponsor'];
    const isCountableContact = (a) => {
        const activityType = normalizeText(a['Tipo de Atividade'] || a['Tipo de atividade'] || a['Tipo']);
        const activityName = normalizeText(a.Atividade || '');
        return !activityName.includes('regra') && (validContactKeywords.some(type => activityType.includes(type)) || activityName.startsWith('whatsapp octadesk'));
    };

    const csContactActivities = csRealizedActivities.filter(isCountableContact);
    const onboardingContactActivities = onboardingRealizedActivities.filter(isCountableContact);

    let contactsForCoverage = [...csContactActivities];
    if (includeOnboarding) {
        contactsForCoverage = [...contactsForCoverage, ...onboardingContactActivities];
    }
    const uniqueClientContactsMap = new Map();
    contactsForCoverage.forEach(activity => uniqueClientContactsMap.set(activity.ClienteCompleto, activity));
    metrics['cob-carteira'] = Array.from(uniqueClientContactsMap.values());

    const callKeywords = ['ligacao', 'reuniao', 'call sponsor'];
    const emailKeywords = ['e-mail', 'whatsapp'];
    const prioritizedCsContacts = new Map();
    csContactActivities.forEach(activity => {
        const clientKey = activity.ClienteCompleto;
        if (!clientKey) return;
        const activityType = normalizeText(activity['Tipo de Atividade'] || activity['Tipo de atividade'] || activity['Tipo']);
        let currentCategory = '';
        if (callKeywords.some(k => activityType.includes(k))) currentCategory = 'call';
        else if (emailKeywords.some(k => activityType.includes(k))) currentCategory = 'email';
        if (currentCategory === 'call') prioritizedCsContacts.set(clientKey, { activity, category: 'call' });
        else if (currentCategory === 'email' && (!prioritizedCsContacts.has(clientKey) || prioritizedCsContacts.get(clientKey).category !== 'call')) prioritizedCsContacts.set(clientKey, { activity, category: 'email' });
    });
    const finalCsContactList = Array.from(prioritizedCsContacts.values());
    metrics['contato-ligacao'] = finalCsContactList.filter(item => item.category === 'call').map(item => item.activity);
    metrics['contato-email'] = finalCsContactList.filter(item => item.category === 'email').map(item => item.activity);

    const prioritizedOnboardingContacts = new Map();
    onboardingContactActivities.forEach(activity => {
        const clientKey = activity.ClienteCompleto;
        if (!clientKey) return;
        prioritizedOnboardingContacts.set(clientKey, activity);
    });
    metrics['onboarding-contacts-display'] = Array.from(prioritizedOnboardingContacts.values());

    const allCallsAndMeetings = metrics['contato-ligacao'] || [];
    const classifiedCallsAndMeetings = allCallsAndMeetings.filter(a => {
        const categoria = normalizeText(a['Categoria']);
        return categoria === 'engajado' || categoria === 'desengajado';
    });
    metrics['ligacoes-classificadas'] = classifiedCallsAndMeetings;
    metrics['ligacoes-engajadas'] = classifiedCallsAndMeetings.filter(a => normalizeText(a['Categoria']) === 'engajado');
    metrics['ligacoes-desengajadas'] = classifiedCallsAndMeetings.filter(a => normalizeText(a['Categoria']) === 'desengajado');

    metrics['total-clientes-unicos-cs'] = clientsForPeriod.length;
    metrics['total-clientes'] = clientsForPeriod; // Guarda a lista filtrada para usar no gráfico de clientes
    const senseScores = clientsForPeriod.map(c => parseFloat(c['Sense Score'])).filter(s => !isNaN(s) && s !== null);
    metrics['sensescore-avg'] = senseScores.length > 0 ? senseScores.reduce((acc, val) => acc + val, 0) / senseScores.length : 0;

    const applyOwnershipRule = (activity) => {
        const clientInfo = clientOwnerMap.get(activity.ClienteCompleto);
        if (!clientInfo) return false;
        if (selectedCS === 'Todos') return true;
        return clientInfo.cs === selectedCS;
    };

    let activitiesByCS_previstos = rawActivities
        .filter(a => clients.some(c => (c.Cliente || '').trim().toLowerCase() === a.ClienteCompleto))
        .filter(applyOwnershipRule);

    if (!includeOnboarding) {
        activitiesByCS_previstos = activitiesByCS_previstos.filter(activity => {
            const clientInfo = clientOwnerMap.get(activity.ClienteCompleto);
            return clientInfo?.fase !== 'onboarding';
        });
    }

    for (const [key, def] of Object.entries(playbookDefs)) {
        let filterFn;
        if (key === 'plano') {
            filterFn = (item) => {
                const atividadeNormalizada = normalizeText(item.Atividade);
                const playbookNormalizado = normalizeText(item.Playbook);
                const isPlanoOriginal = playbookNormalizado.includes(def.p) && atividadeNormalizada.includes(def.a);
                const isAlertaSuporte = atividadeNormalizada.startsWith('alerta suporte');
                return isPlanoOriginal || isAlertaSuporte;
            };
        } else {
            filterFn = (item) => def.p ? (normalizeText(item.Playbook).includes(def.p) && normalizeText(item.Atividade).includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => normalizeText(item.Atividade).includes(term)) : normalizeText(item.Atividade).includes(def.a));
        }
        metrics[`${key}-previsto`] = activitiesByCS_previstos.filter(filterFn).filter(isPredictedForPeriod);
    }

    const smbConcluidoList = (metrics['engajamento-smb-realizado'] || []).concat(metrics['engajamento-smb-atrasado-concluido'] || []);
    const uniqueSmbContacts = new Map();
    smbConcluidoList.forEach(a => {
        const status = normalizeText(a['Categoria']);
        if (status === 'engajado' || status === 'desengajado') {
            uniqueSmbContacts.set(a.ClienteCompleto, { activity: a, status: status });
        }
    });
    const finalSmbList = Array.from(uniqueSmbContacts.values());
    metrics['engajamento-smb-engajado'] = finalSmbList.filter(item => item.status === 'engajado').map(item => item.activity);
    metrics['engajamento-smb-desengajado'] = finalSmbList.filter(item => item.status === 'desengajado').map(item => item.activity);

    const clientesContatados = metrics['cob-carteira'] || [];
    const clientesContatadosComDados = clientesContatados.map(atividade => {
        // CORREÇÃO: Usar 'clients' (todos os clientes do filtro) em vez de 'clientsForPeriod' para buscar dados
        const clienteData = clients.find(c => (c.Cliente || '').trim().toLowerCase() === atividade.ClienteCompleto);
        return { ...atividade,
            'Modelo de negócio': clienteData ? (clienteData['Modelo de negócio'] || 'Vazio') : 'Não encontrado',
            'Potencial': clienteData ? (clienteData['Potencial'] || 'Vazio') : 'Não encontrado'
        };
    });
    metrics['modelo-negocio-preenchido'] = clientesContatadosComDados.filter(item => item['Modelo de negócio'] && item['Modelo de negócio'] !== 'Vazio');
    metrics['modelo-negocio-faltante'] = clientesContatadosComDados.filter(item => !item['Modelo de negócio'] || item['Modelo de negócio'] === 'Vazio');
    metrics['cliente-potencial-preenchido'] = clientesContatadosComDados.filter(item => item['Potencial'] && item['Potencial'] !== 'Vazio');
    metrics['cliente-potencial-faltante'] = clientesContatadosComDados.filter(item => !item['Potencial'] || item['Potencial'] === 'Vazio');

    const selectedQuarter = Math.floor(month / 3);
    const isConcludedInQuarter = (item) => item.ConcluidaEm && Math.floor(item.ConcluidaEm.getUTCMonth() / 3) === selectedQuarter && item.ConcluidaEm.getUTCFullYear() === year;
    // CORREÇÃO: Usar 'clients' (todos os clientes do filtro) para a base do cálculo de LABS
    const eligibleClientSet = new Set(clients.map(c => (c.Cliente || '').trim().toLowerCase()));
    metrics['total-labs-eligible'] = clients.length; // Usar o total de clientes do filtro
    metrics['ska-labs-realizado-list'] = [...new Map(
        rawActivities
        .filter(isConcludedInQuarter)
        .filter(a => normalizeText(a.Atividade).includes('participou do ska labs'))
        .filter(a => eligibleClientSet.has(a.ClienteCompleto)) // Verifica se o cliente está na carteira filtrada
        .map(a => [a.ClienteCompleto, a])
    ).values()];

    const contatosPorNegocio = {};
    const contatosPorComercial = {};
    (metrics['cob-carteira'] || []).forEach(activity => {
        const negocio = activity.Negocio || 'Não Definido';
        const comercial = activity.Comercial || 'Não Definido';
        if (negocio) contatosPorNegocio[negocio] = (contatosPorNegocio[negocio] || 0) + 1;
        if (comercial) contatosPorComercial[comercial] = (contatosPorComercial[comercial] || 0) + 1;
    });
    metrics['contatos-por-negocio'] = contatosPorNegocio;
    metrics['contatos-por-comercial'] = contatosPorComercial;

    // --- Novos cálculos de distribuição (para o card de cobertura) ---
    const metaCobertura = (goals.coverage || 40) / 100;
    const clientesParaAtingirMeta = Math.max(0, Math.ceil(metrics['total-clientes-unicos-cs'] * metaCobertura));
    metrics['clientes-faltantes-meta-cobertura'] = Math.max(0, clientesParaAtingirMeta - clientesContatados.length);

    let ligacaoPerc = 0.65; // Padrão SMB
    // CORREÇÃO: Usar 'clients' (todos os clientes do filtro) para determinar o segmento predominante
    if (clients.length > 0 && (normalizeText(clients[0].Segmento || '').includes('mid') || normalizeText(clients[0].Segmento || '').includes('enterprise'))) {
        ligacaoPerc = 0.75;
    }
    const targetLigacoes = Math.ceil(clientesParaAtingirMeta * ligacaoPerc);
    const targetEmails = clientesParaAtingirMeta - targetLigacoes;
    const realizadoLigacoes = (metrics['contato-ligacao'] || []).length;
    const realizadoEmails = (metrics['contato-email'] || []).length;
    metrics['dist-faltante-ligacao'] = Math.max(0, targetLigacoes - realizadoLigacoes);
    metrics['dist-faltante-email'] = Math.max(0, targetEmails - realizadoEmails);

    // --- CÁLCULO CORRIGIDO: Distribuição de Clientes por Negócio ---
    // Está no lugar certo agora!
    const clientesPorNegocio = {};
    clientsForPeriod.forEach(client => { // Usa clientsForPeriod que existe aqui
        const negocio = client['Negócio'] || 'Não Definido';
        if (negocio) {
            clientesPorNegocio[negocio] = (clientesPorNegocio[negocio] || 0) + 1;
        }
    });
    metrics['clientes-por-negocio'] = clientesPorNegocio;

    return metrics;
};

const getPeriodDateRange = (period, year) => {
    switch (period) {
        case 'q1': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 2, 31, 23, 59, 59)) };
        case 'q2': return { start: new Date(Date.UTC(year, 3, 1)), end: new Date(Date.UTC(year, 5, 30, 23, 59, 59)) };
        case 'q3': return { start: new Date(Date.UTC(year, 6, 1)), end: new Date(Date.UTC(year, 8, 30, 23, 59, 59)) };
        case 'q4': return { start: new Date(Date.UTC(year, 9, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59)) };
        case 's1': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 5, 30, 23, 59, 59)) };
        case 's2': return { start: new Date(Date.UTC(year, 6, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59)) };
        case 'year': return { start: new Date(Date.UTC(year, 0, 1)), end: new Date(Date.UTC(year, 11, 31, 23, 59, 59)) };
        default: return null;
    }
};

const calculateMetricsForDateRange = (startDate, endDate, allActivities, allClients, selectedCS, includeOnboarding, goals) => {
    const periodMetrics = {
        monthlyCoverages: [],
        totalClientsInPeriod: new Set(),
        playbookTotals: {}
    };
    const playbookKeys = ['plano', 'adocao', 'reunioes-qbr', 'planos-sucesso', 'followup', 'discovery', 'engajamento-smb', 'baixo-engajamento-mid'];
    playbookKeys.forEach(key => periodMetrics.playbookTotals[key] = { previsto: 0, realizado: 0 });
    const clientsForPeriod = (selectedCS === 'Todos') ? allClients : allClients.filter(d => d.CS?.trim() === selectedCS);

    for (let d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {
        if (d > endDate) break;
        const currentMonth = d.getUTCMonth();
        const currentYear = d.getUTCFullYear();
        const monthlyMetrics = calculateMetricsForPeriod(currentMonth, currentYear, allActivities, clientsForPeriod, selectedCS, includeOnboarding, goals);
        const totalClientsUnicosCS = monthlyMetrics['total-clientes-unicos-cs'] || 0;
        const clientesContatadosCount = monthlyMetrics['cob-carteira']?.length || 0;
        const cobCarteiraPerc = totalClientsUnicosCS > 0 ? (clientesContatadosCount / totalClientsUnicosCS) * 100 : 0;
        periodMetrics.monthlyCoverages.push(cobCarteiraPerc);
        (monthlyMetrics['total-clientes'] || []).forEach(client => periodMetrics.totalClientsInPeriod.add(client.Cliente)); // Use client.Cliente here
        playbookKeys.forEach(key => {
            periodMetrics.playbookTotals[key].previsto += (monthlyMetrics[`${key}-previsto`] || []).length;
            const realizadoNoPrazo = (monthlyMetrics[`${key}-realizado`] || []).length;
            const realizadoAtrasado = (monthlyMetrics[`${key}-atrasado-concluido`] || []).length;
            periodMetrics.playbookTotals[key].realizado += realizadoNoPrazo + realizadoAtrasado;
        });
    }

    if (periodMetrics.monthlyCoverages.length > 0) {
        const sum = periodMetrics.monthlyCoverages.reduce((acc, val) => acc + val, 0);
        periodMetrics.averageCoverage = sum / periodMetrics.monthlyCoverages.length;
    } else {
        periodMetrics.averageCoverage = 0;
    }
    periodMetrics.totalUniqueClients = periodMetrics.totalClientsInPeriod.size;
    return periodMetrics;
};

const calculateNewOnboardingOKRs = (month, year, activities, clients, teamView, selectedISM) => {
    const okrs = {};
    const startOfMonth = new Date(Date.UTC(year, month, 1));
    const endOfMonth = new Date(Date.UTC(year, month + 1, 0, 23, 59, 59));

    let onboardingClients = clients.filter(c => normalizeText(c.Fase) === 'onboarding');
    if (teamView === 'syneco') {
        onboardingClients = onboardingClients.filter(c => normalizeText(c.Cliente).includes('syneco'));
    } else if (teamView === 'outros') {
        onboardingClients = onboardingClients.filter(c => !normalizeText(c.Cliente).includes('syneco'));
    }
    if (selectedISM !== 'Todos') {
        onboardingClients = onboardingClients.filter(c => c.ISM === selectedISM);
    }
    okrs.filteredClients = onboardingClients;
    const onboardingClientNames = new Set(onboardingClients.map(c => (c.Cliente || '').trim().toLowerCase()));

    const onboardingActivities = activities.filter(a => onboardingClientNames.has(a.ClienteCompleto));
    
    // ==================================================================
    // NOVA LÓGICA: Cálculo de Etapas do Onboarding
    // ==================================================================
    const stageActivityNames = {
        preGoLive: normalizeText('Validação de usabilidade / Agendamento GO LIVE'),
        execucao: normalizeText('Planejamento Finalizado'),
        inicio: normalizeText('Contato de Welcome')
    };

    const stageCounts = { preGoLive: 0, execucao: 0, inicio: 0, backlog: 0 };
    const stageLists = { preGoLive: [], execucao: [], inicio: [], backlog: [] };

    // 1. O total de clientes é simples
    okrs.totalFilteredClients = onboardingClients.length;

    // 2. Encontrar todas as atividades concluídas de uma vez (de todos os tempos)
    const concludedOnboardingActivities = onboardingActivities.filter(a => a.ConcluidaEm);

    // 3. Mapear as atividades concluídas por cliente para checagem rápida
    const clientConcludedActivitiesMap = new Map();
    concludedOnboardingActivities.forEach(activity => {
        const clientKey = activity.ClienteCompleto;
        if (!clientConcludedActivitiesMap.has(clientKey)) {
            clientConcludedActivitiesMap.set(clientKey, new Set());
        }
        clientConcludedActivitiesMap.get(clientKey).add(normalizeText(activity.Atividade));
    });

    // 4. Iterar sobre os clientes filtrados e atribuir a etapa
    onboardingClients.forEach(client => {
        const clientKey = (client.Cliente || '').trim().toLowerCase();
        const concludedSet = clientConcludedActivitiesMap.get(clientKey) || new Set();

        if (concludedSet.has(stageActivityNames.preGoLive)) {
            stageCounts.preGoLive++;
            stageLists.preGoLive.push(client);
        } else if (concludedSet.has(stageActivityNames.execucao)) {
            stageCounts.execucao++;
            stageLists.execucao.push(client);
        } else if (concludedSet.has(stageActivityNames.inicio)) {
            stageCounts.inicio++;
            stageLists.inicio.push(client);
        } else {
            stageCounts.backlog++;
            stageLists.backlog.push(client);
        }
    });

    // 5. Adicionar ao objeto okrs
    okrs.stageCounts = stageCounts;
    okrs.stageLists = stageLists;
    // ==================================================================
    // FIM DA NOVA LÓGICA
    // ==================================================================

    const concludedInPeriod = onboardingActivities.filter(a => a.ConcluidaEm >= startOfMonth && a.ConcluidaEm <= endOfMonth);

    // ==================================================================
    // ALTERAÇÃO 1: Busca exata pelo nome da atividade "Go Live"
    // ==================================================================
    const goLiveActivityName = normalizeText('Call/Reunião de GO LIVE');

    const goLiveActivitiesConcluded = concludedInPeriod.filter(a => normalizeText(a.Atividade) === goLiveActivityName);
    const goLiveActivitiesPredicted = onboardingActivities.filter(a =>
        normalizeText(a.Atividade) === goLiveActivityName &&
        !a.ConcluidaEm &&
        a.PrevisaoConclusao >= startOfMonth &&
        a.PrevisaoConclusao <= endOfMonth
    );
    // ==================================================================
    // FIM DA ALTERAÇÃO 1
    // ==================================================================

    okrs.goLiveActivitiesConcluded = goLiveActivitiesConcluded;
    okrs.goLiveActivitiesPredicted = goLiveActivitiesPredicted;

    const clientsWithGoLive = [...new Set(goLiveActivitiesConcluded.map(a => a.ClienteCompleto))];
    const clientsWithGoLiveAndNPSData = onboardingClients.filter(c => clientsWithGoLive.includes((c.Cliente || '').trim().toLowerCase()));
    okrs.clientsWithGoLive = clientsWithGoLiveAndNPSData;
    okrs.npsResponded = clientsWithGoLiveAndNPSData.filter(c => c.NPSOnboarding && String(c.NPSOnboarding).trim() !== '');
    okrs.highValueClients = onboardingClients.filter(c => c.ValorNaoFaturado > 50000);

    const completedOnboardings = [];
    const clientsWithCompletedGoLive = new Set(goLiveActivitiesConcluded.map(a => a.ClienteCompleto));
    clientsWithCompletedGoLive.forEach(clientKey => {
        const goLiveDate = goLiveActivitiesConcluded
            .filter(a => a.ClienteCompleto === clientKey)
            .map(a => a.ConcluidaEm)
            .sort((a,b) => b - a)[0];
        const playbookStartDate = clientOnboardingStartDate.get(clientKey);
        if (goLiveDate && playbookStartDate) {
            const duration = Math.floor((goLiveDate - playbookStartDate) / (1000 * 60 * 60 * 24));
            completedOnboardings.push({ client: clientKey, duration: duration });
        }
    });
    okrs.completedOnboardingsList = completedOnboardings;
    const durations = completedOnboardings.map(item => item.duration);
    okrs.averageCompletedOnboardingTime = durations.length > 0
        ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length)
        : 0;

    const today = new Date();
    // ==================================================================
    // ALTERAÇÃO 2: Lógica do "Top Ranking"
    // A lista `onboardingClients` já contém APENAS clientes com Fase == 'onboarding'.
    // Portanto, ela é a definição de "em aberto".
    // Apenas calculamos a duração de todos eles até HOJE.
    // ==================================================================
    const openClientsWithDuration = onboardingClients
        .map(client => {
            const clientKey = (client.Cliente || '').trim().toLowerCase();
            const startDate = clientOnboardingStartDate.get(clientKey);
            if (startDate) {
                const duration = Math.floor((today - startDate) / (1000 * 60 * 60 * 24));
                return { ...client, onboardingDuration: duration };
            }
            return { ...client, onboardingDuration: null };
        }).filter(c => c.onboardingDuration !== null); // Mantém filtro para clientes sem data de início
    // ==================================================================
    // FIM DA ALTERAÇÃO 2
    // ==================================================================

    openClientsWithDuration.sort((a, b) => b.onboardingDuration - a.onboardingDuration);
    okrs.top5LongestOnboardings = openClientsWithDuration.slice(0, 5);
    okrs.clientsOver120Days = openClientsWithDuration.filter(c => c.onboardingDuration > 120);

    const processTargets = {
        welcome: { name: 'contato de welcome', sla: 3 },
        kickoff: { name: 'call/reunião kickoff', sla: 7 },
        planejamento: { name: 'planejamento finalizado' },
        goLiveMeeting: { name: 'call/reunião de go live' },
        mapearContatos: { name: 'mapear contatos' },
        acompanhamento: { name: 'acompanhamento / status reports' }
    };

    Object.entries(processTargets).forEach(([key, config]) => {
        const allInPeriod = onboardingActivities.filter(a =>
            normalizeText(a.Atividade) === config.name &&
            ((a.PrevisaoConclusao >= startOfMonth && a.PrevisaoConclusao <= endOfMonth) ||
             (a.ConcluidaEm >= startOfMonth && a.ConcluidaEm <= endOfMonth))
        );
        const previstoList = allInPeriod.filter(a => a.PrevisaoConclusao >= startOfMonth && a.PrevisaoConclusao <= endOfMonth);
        const realizadoList = allInPeriod.filter(a => a.ConcluidaEm >= startOfMonth && a.ConcluidaEm <= endOfMonth);
        okrs[`${key}Previsto`] = previstoList;
        okrs[`${key}Realizado`] = realizadoList;

        // Lógica de SLA (Revisada e confirmada):
        // Se diffDays for negativo (feito antes), -5 <= 3 (true) -> OK
        // Se diffDays for 0 (feito no dia), 0 <= 3 (true) -> OK
        // Se diffDays for 3 (feito 3 dias depois), 3 <= 3 (true) -> OK
        // Se diffDays for 4 (feito 4 dias depois), 4 <= 3 (false) -> Não OK
        if (config.sla) {
            okrs[`${key}SLAOk`] = realizadoList.filter(a => {
                const dueDate = a.PrevisaoConclusao;
                const completedDate = a.ConcluidaEm;
                if (dueDate && completedDate) {
                    const diffDays = (completedDate - dueDate) / (1000 * 60 * 60 * 24);
                    return diffDays <= config.sla;
                }
                return false;
            });
        }
    });

    return okrs;
};


// --- MANIPULADOR DE MENSAGENS DO WORKER ---

self.onmessage = (e) => {
    const { type, payload } = e.data;

    try {
        if (type === 'INIT') {
            // 1. Recebe os dados brutos
            rawActivities = payload.rawActivities;
            rawClients = payload.rawClients;
            currentUser = payload.currentUser;
            manualEmailToCsMap = payload.manualEmailToCsMap;

            // 2. Processa e junta os dados
            processInitialData(); // Modifica rawActivities

            // 3. Constrói o mapa de Squads
            buildCsToSquadMap();

            // 4. Extrai dados para os filtros
            const csSet = [...new Set(rawClients.map(d => d.CS && d.CS.trim()).filter(Boolean))];
            const squadSet = [...new Set(rawClients.map(d => d['Squad CS']).filter(Boolean))];
            const ismSet = [...new Set(rawClients.map(c => c.ISM).filter(Boolean))];
            const allDates = [...rawActivities.map(a => a.PrevisaoConclusao), ...rawActivities.map(a => a.ConcluidaEm)];
            const years = [...new Set(allDates.map(d => d?.getFullYear()).filter(Boolean))];

            // 5. Envia os dados de filtro de volta
            postMessage({
                type: 'INIT_COMPLETE',
                payload: {
                    filterData: {
                        csSet,
                        squadSet,
                        ismSet,
                        years,
                        csToSquadMap: Object.fromEntries(csToSquadMap) // Converte Map para Objeto
                    }
                }
            });
        }

        else if (type === 'CALCULATE_MONTHLY') {
            // 1. Filtra clientes baseado nos filtros de CS/Squad
            let filteredClients = rawClients;
            let csForCalc = payload.selectedCS;

            if (currentUser.isManager) {
                if (payload.selectedSquad !== 'Todos') {
                    filteredClients = rawClients.filter(d => d['Squad CS'] === payload.selectedSquad);
                    csForCalc = 'Todos';
                } else if (payload.selectedCS !== 'Todos') {
                    filteredClients = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
                }
            } else {
                filteredClients = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
            }

            // 2. Calcula métricas do mês atual
            const dataStore = calculateMetricsForPeriod(payload.month, payload.year, rawActivities, filteredClients, csForCalc, payload.includeOnboarding, payload.goals);

            // 3. Calcula métricas de comparação (se necessário)
            if (payload.comparison !== 'none') {
                let compMes = payload.month, compAno = payload.year;
                if (payload.comparison === 'prev_month') {
                    compMes--;
                    if (compMes < 0) { compMes = 11; compAno--; }
                } else if (payload.comparison === 'prev_year') {
                    compAno--;
                }
                const comparisonMetrics = calculateMetricsForPeriod(compMes, compAno, rawActivities, filteredClients, csForCalc, payload.includeOnboarding, payload.goals);

                // Adiciona dados de comparação ao dataStore
                dataStore['cob-carteira_comp_perc'] = (comparisonMetrics['cob-carteira']?.length || 0) * 100 / (comparisonMetrics['total-clientes-unicos-cs'] || 1);
                dataStore['sensescore-avg_comp'] = comparisonMetrics['sensescore-avg'];
                dataStore['ska-labs-realizado-list_comp_perc'] = ((comparisonMetrics['ska-labs-realizado-list']?.length || 0) * 100) / (comparisonMetrics['total-labs-eligible'] || 1);
            }

            // 4. Calcula OKRs de Onboarding
            const onboardingDataStore = calculateNewOnboardingOKRs(payload.month, payload.year, rawActivities, rawClients, payload.teamView, payload.selectedISM);

            // 5. Calcula Atividades Atrasadas
            const overdueMetrics = calculateOverdueMetrics(rawActivities, payload.selectedCS);

            // 6. Calcula Divergência
            const concludedInPeriodActivities = rawActivities.filter(a => a.ConcluidaEm?.getUTCMonth() === payload.month && a.ConcluidaEm?.getUTCFullYear() === payload.year);
            const divergentActivities = concludedInPeriodActivities.filter(a => {
                const activityOwner = (a['Responsável'] || '').trim();
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
            if (dateRange) {
                const periodData = calculateMetricsForDateRange(dateRange.start, dateRange.end, rawActivities, rawClients, payload.selectedCS, payload.includeOnboarding, payload.goals);
                postMessage({
                    type: 'PERIOD_CALCULATION_COMPLETE',
                    payload: {
                        periodData,
                        periodName: payload.period // A thread principal tem o nome
                    }
                });
            }
        }

        else if (type === 'CALCULATE_TREND') {
            const { metricId, title, type, baseMonth, baseYear, selectedCS, selectedSquad, includeOnboarding, segmento } = payload;

            const labels = [];
            const dataPoints = [];
            const baseDate = new Date(baseYear, baseMonth, 1);

            // Filtra clientes UMA VEZ
            let filteredClients = rawClients;
            let csForCalc = selectedCS;
            if (currentUser.isManager) {
                if (selectedSquad !== 'Todos') {
                    filteredClients = rawClients.filter(d => d['Squad CS'] === selectedSquad);
                    csForCalc = 'Todos';
                } else if (selectedCS !== 'Todos') {
                    filteredClients = rawClients.filter(d => d.CS?.trim() === selectedCS);
                }
            } else {
                filteredClients = rawClients.filter(d => d.CS?.trim() === selectedCS);
            }

            for (let i = 5; i >= 0; i--) {
                const date = new Date(baseDate);
                date.setMonth(baseDate.getMonth() - i);
                const month = date.getMonth();
                const year = date.getFullYear();

                labels.push(`${date.toLocaleString('default', { month: 'short' })}/${String(year).slice(2)}`);

                const periodMetrics = calculateMetricsForPeriod(month, year, rawActivities, filteredClients, csForCalc, includeOnboarding, {}); // Não precisa de metas aqui

                let value = 0;
                if (type === 'okr') {
                    switch (metricId) {
                        case 'cob-carteira':
                            const totalClients = periodMetrics['total-clientes-unicos-cs'] || 0;
                            const contacted = periodMetrics['cob-carteira']?.length || 0;
                            value = totalClients > 0 ? (contacted / totalClients) * 100 : 0;
                            break;
                        case 'sensescore-avg':
                            value = periodMetrics['sensescore-avg'] || 0;
                            break;
                    }
                } else if (type === 'playbook') {
                    const realizado = (periodMetrics[`${metricId}-realizado`] || []).length;
                    const atrasado = (periodMetrics[`${metricId}-atrasado-concluido`] || []).length;
                    value = realizado + atrasado;
                }
                dataPoints.push(value.toFixed(2));
            }

            postMessage({
                type: 'TREND_DATA_COMPLETE',
                payload: { labels, dataPoints, title }
            });
        }

    } catch (err) {
        // Envia erros de volta para a thread principal
        postMessage({
            type: 'ERROR',
            payload: {
                error: err.message,
                stack: err.stack
            }
        });
    }
};
