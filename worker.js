/**
 * ===================================================================
 * WORKER DE PROCESSAMENTO DE OKRs (V10 - Correção Datas JSON API)
 * ===================================================================
 */

// --- VARIÁVEIS GLOBAIS DO WORKER ---
let rawActivities = [],
    rawClients = [],
    clientOnboardingStartDate = new Map(),
    csToSquadMap = new Map(),
    currentUser = {},
    manualEmailToCsMap = {},
    ismToFilter = 'Todos';

// --- FUNÇÕES AUXILIARES ---

const normalizeText = (str = '') => String(str).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
const buildClientKey = (name = '') => (name || '').trim().toLowerCase();

const formatClientStatus = (value = '') => {
    if (!value) return 'Desconhecido';
    return value
        .split('-')
        .map(part => part.split(' ').map(word => word ? word.charAt(0).toUpperCase() + word.slice(1).toLowerCase() : '').join(' '))
        .join('-')
        .trim();
};

const getNestedValue = (obj, path) => {
    if (!path) return undefined;
    const keys = path.split('.');
    let current = obj;
    for (const key of keys) {
        if (current === null || current === undefined) return undefined;
        current = current[key];
    }
    return current;
};

const getQuarter = (date) => Math.floor(date.getUTCMonth() / 3);

const formatDate = (date) => {
    if (!(date instanceof Date) || isNaN(date)) return '';
    const day = String(date.getUTCDate()).padStart(2, '0');
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const year = date.getUTCFullYear();
    return `${day}/${month}/${year}`;
};

// --- PARSER DE DATAS (ATUALIZADO V10) ---
const parseDate = (dateInput) => {
    if (!dateInput) return null;
    if (dateInput instanceof Date && !isNaN(dateInput)) return dateInput;

    let dateStr = String(dateInput).trim();
    if (!dateStr || dateStr.toLowerCase() === 'null') return null;

    // CASO 0: Data formato brasileiro "DD/MM/YYYY"
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(dateStr)) {
        const [d, m, y] = dateStr.split('/').map(Number);
        return new Date(Date.UTC(y, m - 1, d));
    }

    // CASO 1: Formato ISO 8601 com T (Vem do JSON da API: "2025-12-03T00:00:00")
    if (dateStr.includes('T')) {
        // Remove milissegundos longos se houver
        if (dateStr.includes('.')) {
            dateStr = dateStr.replace(/(\.\d{3})\d+/, '$1');
        }
        const dateObj = new Date(dateStr);
        if (!isNaN(dateObj.getTime())) return dateObj;
    }

    // CASO 2: Data Simples "YYYY-MM-DD"
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        const [y, m, d] = dateStr.split('-').map(Number);
        return new Date(Date.UTC(y, m - 1, d));
    }

    // CASO 3: SQL Timestamp "YYYY-MM-DD HH:MM:SS" (Sem T)
    if (dateStr.match(/^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}/)) {
        dateStr = dateStr.replace(' ', 'T');
        const dateObj = new Date(dateStr);
        if (!isNaN(dateObj.getTime())) return dateObj;
    }

    // Fallback final
    const fallbackDate = new Date(dateStr);
    if (!isNaN(fallbackDate.getTime())) return fallbackDate;

    return null;
};

// --- FUNÇÕES DE INICIALIZAÇÃO ---

const buildCsToSquadMap = () => {
    csToSquadMap.clear();
    const allCSs = [...new Set(rawClients.map(c => c.CS).filter(Boolean))];

    allCSs.forEach(csName => {
        const clientsOfCS = rawClients.filter(c => c.CS === csName);
        if (!clientsOfCS.some(c => c['Squad CS'])) return;

        const squadCounts = clientsOfCS.reduce((acc, client) => {
            const squad = client['Squad CS'];
            if (squad) acc[squad] = (acc[squad] || 0) + 1;
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
        if (majoritySquad) csToSquadMap.set(csName, majoritySquad);
    });
};

const processInitialData = () => {
    console.log(`[Worker] Processando ${rawActivities.length} atividades (V10 - JSON/Dates)...`);
    
    const clientByIdMap = new Map();
    const clienteMapByName = new Map();

    // 1. Processamento de CLIENTES
    rawClients = rawClients.map(c => {
        const getVal = (keys) => {
            for (const k of keys) {
                if (c[k] !== undefined && c[k] !== null) return c[k];
                const nested = getNestedValue(c, k);
                if (nested !== undefined && nested !== null) return nested;
            }
            return '';
        };

        const normalizedClient = {
            ...c, 
            Cliente: getVal(['cliente', 'Cliente', 'name', 'customer.name']),
            CS: getVal(['cs', 'CS', 'owner', 'customer.cs']),
            Segmento: getVal(['segmento', 'Segmento', 'industry', 'customer.group']),
            Fase: getVal(['fase', 'Fase', 'stage']),
            ISM: getVal(['ism', 'ISM']),
            "Negócio": getVal(['negocio', 'Negócio', 'Negocio']),
            "Comercial": getVal(['comercial', 'Comercial']),
            "Status Cliente": getVal(['status', 'Status', 'situacao']), 
            "Squad CS": getVal(['squad_cs', 'Squad CS']),
            "NPS onboarding": getVal(['nps_onboarding', 'NPS onboarding']),
            "Valor total não faturado": getVal(['valor_total_em_aberto', 'Valor total não faturado']),
            id_legacy: getVal(['id_legacy', 'id_customer_legacy', 'customer.id_legacy'])
        };

        normalizedClient["Dias sem touch"] = normalizedClient['Dias sem touch'] !== undefined ? normalizedClient['Dias sem touch'] : (c['dias_sem_touch'] || 0);
        normalizedClient["Situação"] = normalizedClient['Situação'] || normalizedClient['Status Cliente'];

        if (normalizedClient.id_legacy) {
            clientByIdMap.set(String(normalizedClient.id_legacy).trim(), normalizedClient);
        }
        
        const nameKey = buildClientKey(normalizedClient.Cliente);
        if (nameKey) {
            clienteMapByName.set(nameKey, normalizedClient);
        }

        return normalizedClient;
    });

    const findClientFallback = (name = '') => {
        const normalized = buildClientKey(name);
        return clienteMapByName.get(normalized) || null;
    };

    const onboardingPlaybooks = [
        'onboarding lantek', 'onboarding elétrica', 'onboarding altium',
        'onboarding solidworks', 'onboarding usinagem', 'onboarding hp',
        'onboarding alphacam', 'onboarding mkf', 'onboarding formlabs', 'tech journey'
    ];

    clientOnboardingStartDate.clear();

    // 2. Processamento de ATIVIDADES
    let debugCount = 0;

    rawActivities = rawActivities.map((ativ, index) => {
        const campo = (keys) => {
            for (const k of keys) {
                if (ativ[k] !== undefined && ativ[k] !== null) return ativ[k];
                const nested = getNestedValue(ativ, k);
                if (nested !== undefined && nested !== null) return nested;
            }
            return ''; 
        };

        const idCustomerLegacy = campo(['id_customer_legacy', 'id_legacy', 'customer.id_legacy']);
        
        let clienteInfo = null;
        if (idCustomerLegacy) {
            clienteInfo = clientByIdMap.get(String(idCustomerLegacy).trim());
        }

        const originalClientName = campo(['cliente', 'Cliente', 'name_contract', 'customer.name', 'customer.name_contract']);
        if (!clienteInfo && originalClientName) {
            clienteInfo = findClientFallback(originalClientName);
        }

        clienteInfo = clienteInfo || {};

        const canonicalClientName = (clienteInfo.Cliente || originalClientName || '').trim();
        const clienteKey = buildClientKey(canonicalClientName);

        // --- MAPEAMENTO DE CAMPOS (Priorizando estrutura JSON da API) ---
        const rawData = {
            playbook: campo(['playbook', 'Playbook', 'playbook.name']),
            atividade: campo(['atividade', 'Atividade', 'description']), 
            tipo: campo(['tipo_atividade', 'Tipo de Atividade', 'type.caption', 'type.description']),
            responsavelAtividade: campo(['responsavel', 'Responsável', 'owner.name', 'owner.username']),
            status: campo(['status', 'Status', 'status.description']),
            categoria: campo(['categoria', 'Categoria']),
            notes: campo(['notes', 'Anotações']),
            
            // DATAS: Prioridade para chaves da API (JSON)
            criado_em: campo(['created_on', 'CriadoEm', 'criado_em', 'Criado em']), 
            
            // PREVISÃO: due_date é o padrão da API para prazo
            previsao: campo(['due_date', 'previsao_conclusao', 'PrevisaoConclusao', 'Previsão de conclusão', 'planned_due_date']), 
            
            // CONCLUSÃO: end_date é o padrão da API para conclusão efetiva
            concluido: campo(['end_date', 'system_end_date', 'ConcluidaEm', 'system_end_date_raw', 'Concluída em']) 
        };

        let criadoEmDate = parseDate(rawData.criado_em);
        const previsaoConclusaoDate = parseDate(rawData.previsao);
        const concluidaEmDate = parseDate(rawData.concluido);

        const playbookNormalized = normalizeText(rawData.playbook);
        const statusClienteRaw = clienteInfo['Status Cliente'] || 'Desconhecido';
        const statusCliente = formatClientStatus(statusClienteRaw);

        if (onboardingPlaybooks.includes(playbookNormalized) && criadoEmDate) {
            const existingStartDate = clientOnboardingStartDate.get(clienteKey);
            if (!existingStartDate || criadoEmDate < existingStartDate) {
                clientOnboardingStartDate.set(clienteKey, criadoEmDate);
            }
        }

        // Fallback de CS
        let finalCS = clienteInfo.CS;
        if (!finalCS || finalCS === 'Não Identificado') {
            finalCS = rawData.responsavelAtividade;
        }
        if (!finalCS) finalCS = 'Não Identificado';

        // --- DEBUG NO CONSOLE PARA AS 3 PRIMEIRAS ATIVIDADES ---
        if (debugCount < 3) {
            console.log(`[WORKER DEBUG V10] Ativ ${index}:`, {
                Atividade: rawData.atividade,
                RawEnd: rawData.concluido,
                ParsedEnd: concluidaEmDate,
                RawDue: rawData.previsao,
                ParsedDue: previsaoConclusaoDate
            });
            debugCount++;
        }

        return {
            ...ativ,
            __activityId: campo(['id_sensedata', 'id']),
            Cliente: canonicalClientName,
            ClienteCompleto: clienteKey,
            "Status Cliente": statusCliente,
            Segmento: clienteInfo.Segmento || 'Não Identificado',
            CS: finalCS, 
            Squad: clienteInfo['Squad CS'],
            Fase: clienteInfo.Fase,
            ISM: clienteInfo.ISM,
            Negocio: clienteInfo['Negócio'],
            Comercial: clienteInfo['Comercial'],
            Playbook: rawData.playbook,
            Atividade: rawData.atividade,
            "Tipo de Atividade": rawData.tipo,
            "Responsável": rawData.responsavelAtividade,
            Status: rawData.status,
            Categoria: rawData.categoria,
            "Anotações": rawData.notes,
            CriadoEm: criadoEmDate,
            PrevisaoConclusao: previsaoConclusaoDate,
            ConcluidaEm: concluidaEmDate,
            NPSOnboarding: clienteInfo['NPS onboarding'],
            ValorNaoFaturado: parseFloat(String(clienteInfo['Valor total não faturado'] || '0').replace(/[^0-9,-]+/g, "").replace(",", "."))
        };
    });
};

// --- CÁLCULOS DE MÉTRICAS ---

const calculateOverdueMetrics = (activities, selectedCS) => {
    const metrics = { overdueActivities: {} };
    const now = new Date();
    const today = new Date(Date.UTC(now.getFullYear(), now.getMonth(), now.getDate()));

    const csActivities = selectedCS === 'Todos' ? activities : activities.filter(a => (a.CS || '').trim() === selectedCS);

    const overdue = csActivities.filter(a => {
        const dueDate = a.PrevisaoConclusao;
        return dueDate && !a.ConcluidaEm && dueDate < today;
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
                const ativNorm = normalizeText(item.Atividade);
                const playNorm = normalizeText(item.Playbook);
                return (playNorm.includes(def.p) && ativNorm.includes(def.a)) || ativNorm.startsWith('alerta suporte');
            };
        }
        metrics.overdueActivities[key] = overdue.filter(filterFn);
    }
    return metrics;
};

const calculateMetricsForPeriod = (month, year, activities, clients, selectedCS, includeOnboarding, goals) => {
    const metrics = {};
    const startOfPeriod = new Date(Date.UTC(year, month, 1));
    const endOfMonth = new Date(Date.UTC(year, month + 1, 0, 23, 59, 59));

    const isConcludedInPeriod = (item) => item.ConcluidaEm && item.ConcluidaEm.getUTCMonth() === month && item.ConcluidaEm.getUTCFullYear() === year;
    const isPredictedForPeriod = (activity) => {
        if (!activity.PrevisaoConclusao) return false;
        const isDueInPeriod = activity.PrevisaoConclusao.getUTCMonth() === month && activity.PrevisaoConclusao.getUTCFullYear() === year;
        return isDueInPeriod && !activity.ConcluidaEm; 
    };
    const isOverdue = (activity) => activity.PrevisaoConclusao && activity.PrevisaoConclusao < startOfPeriod;

    const clientsForPeriod = includeOnboarding ? clients : clients.filter(c => normalizeText(c.Fase) !== 'onboarding');

    const allConcludedActivitiesInPortfolio = rawActivities.filter(isConcludedInPeriod);

    let csRealizedActivities = [];
    let onboardingRealizedActivities = [];
    const ismList = [...new Set(rawClients.map(c => c.ISM).filter(Boolean))].map(ism => ism.trim());

    allConcludedActivitiesInPortfolio.forEach(activity => {
        const isClientOnboarding = normalizeText(activity.Fase) === 'onboarding';
        const activityCS = (activity.CS || '').trim(); 
        const activityOwner = (activity['Responsável'] || '').trim();

        if (!isClientOnboarding) {
            if (selectedCS === 'Todos' || activityCS === selectedCS) csRealizedActivities.push(activity);
        } else {
            if (ismList.includes(activityOwner) && ismToFilter !== 'Todos') {
                if (activityOwner === ismToFilter) onboardingRealizedActivities.push(activity);
            } else {
                onboardingRealizedActivities.push(activity);
            }
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
                const an = normalizeText(item.Atividade);
                const pn = normalizeText(item.Playbook);
                return (pn.includes(def.p) && an.includes(def.a)) || an.startsWith('alerta suporte');
            };
        } else {
            filterFn = (item) => def.p ? (normalizeText(item.Playbook).includes(def.p) && normalizeText(item.Atividade).includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => normalizeText(item.Atividade).includes(term)) : normalizeText(item.Atividade).includes(def.a));
        }

        const totalRealizadoList = combinedRealizedActivities.filter(filterFn);
        metrics[`${key}-atrasado-concluido`] = totalRealizadoList.filter(isOverdue);
        const notOverdueList = totalRealizadoList.filter(a => !isOverdue(a));
        metrics[`${key}-realizado`] = notOverdueList.filter(a => a.PrevisaoConclusao <= endOfMonth);
        metrics[`${key}-concluido-antecipado`] = notOverdueList.filter(a => a.PrevisaoConclusao > endOfMonth);
    }

    const validContactKeywords = ['e-mail', 'email', 'ligacao', 'reuniao', 'whatsapp', 'call sponsor'];
    const isCountableContact = (a) => {
        const at = normalizeText(a['Tipo de Atividade'] || a['Tipo de atividade'] || a['Tipo']);
        const an = normalizeText(a.Atividade || '');
        return !an.includes('regra') && (validContactKeywords.some(type => at.includes(type)) || an.startsWith('whatsapp octadesk'));
    };

    const csContactActivities = csRealizedActivities.filter(isCountableContact);
    const onboardingContactActivities = onboardingRealizedActivities.filter(isCountableContact);
    let contactsForCoverage = [...csContactActivities];
    if (includeOnboarding) contactsForCoverage = [...contactsForCoverage, ...onboardingContactActivities];
    
    const uniqueClientContactsMap = new Map();
    contactsForCoverage.forEach(activity => uniqueClientContactsMap.set(activity.ClienteCompleto, activity));
    metrics['cob-carteira'] = Array.from(uniqueClientContactsMap.values());

    const callKeywords = ['ligacao', 'reuniao', 'call sponsor'];
    const emailKeywords = ['e-mail', 'email', 'whatsapp'];
    const prioritizedCsContacts = new Map();
    csContactActivities.forEach(activity => {
        const clientKey = activity.ClienteCompleto;
        if (!clientKey) return;
        const at = normalizeText(activity['Tipo de Atividade'] || a['Tipo']);
        let currentCategory = '';
        if (callKeywords.some(k => at.includes(k))) currentCategory = 'call';
        else if (emailKeywords.some(k => at.includes(k))) currentCategory = 'email';
        
        if (currentCategory === 'call') prioritizedCsContacts.set(clientKey, { activity, category: 'call' });
        else if (currentCategory === 'email' && (!prioritizedCsContacts.has(clientKey) || prioritizedCsContacts.get(clientKey).category !== 'call')) prioritizedCsContacts.set(clientKey, { activity, category: 'email' });
    });
    const finalCsContactList = Array.from(prioritizedCsContacts.values());
    metrics['contato-ligacao'] = finalCsContactList.filter(item => item.category === 'call').map(item => item.activity);
    metrics['contato-email'] = finalCsContactList.filter(item => item.category === 'email').map(item => item.activity);

    const prioritizedOnboardingContacts = new Map();
    onboardingContactActivities.forEach(activity => {
        if (activity.ClienteCompleto) prioritizedOnboardingContacts.set(activity.ClienteCompleto, activity);
    });
    metrics['onboarding-contacts-display'] = Array.from(prioritizedOnboardingContacts.values());

    const allCallsAndMeetings = metrics['contato-ligacao'] || [];
    const classifiedCallsAndMeetings = allCallsAndMeetings.filter(a => {
        const cat = normalizeText(a['Categoria']);
        return cat === 'engajado' || cat === 'desengajado';
    });
    metrics['ligacoes-classificadas'] = classifiedCallsAndMeetings;
    metrics['ligacoes-engajadas'] = classifiedCallsAndMeetings.filter(a => normalizeText(a['Categoria']) === 'engajado');
    metrics['ligacoes-desengajadas'] = classifiedCallsAndMeetings.filter(a => normalizeText(a['Categoria']) === 'desengajado');

    metrics['total-clientes-unicos-cs'] = clientsForPeriod.length;
    metrics['total-clientes'] = clientsForPeriod;
    const senseScores = clientsForPeriod.map(c => parseFloat(c['Sense Score'])).filter(s => !isNaN(s) && s !== null);
    metrics['sensescore-avg'] = senseScores.length > 0 ? senseScores.reduce((acc, val) => acc + val, 0) / senseScores.length : 0;

    const applyOwnershipRule = (activity) => {
        if (selectedCS === 'Todos') return true;
        return activity.CS === selectedCS; 
    };

    let activitiesByCS_previstos = rawActivities.filter(applyOwnershipRule);
    if (!includeOnboarding) {
        activitiesByCS_previstos = activitiesByCS_previstos.filter(activity => normalizeText(activity.Fase) !== 'onboarding');
    }

    for (const [key, def] of Object.entries(playbookDefs)) {
        let filterFn;
        if (key === 'plano') {
            filterFn = (item) => {
                const an = normalizeText(item.Atividade);
                const pn = normalizeText(item.Playbook);
                return (pn.includes(def.p) && an.includes(def.a)) || an.startsWith('alerta suporte');
            };
        } else {
            filterFn = (item) => def.p ? (normalizeText(item.Playbook).includes(def.p) && normalizeText(item.Atividade).includes(def.a)) : (Array.isArray(def.a) ? def.a.some(term => normalizeText(item.Atividade).includes(term)) : normalizeText(item.Atividade).includes(def.a));
        }
        const allActivitiesOfType = activitiesByCS_previstos.filter(filterFn);
        metrics[`${key}-previsto`] = allActivitiesOfType.filter(isPredictedForPeriod);
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
        const clienteData = clients.find(c => (c.Cliente || '').trim().toLowerCase() === atividade.ClienteCompleto);
        return { ...atividade,
            'Modelo de negócio': clienteData ? (clienteData['Negócio'] || 'Vazio') : 'Não encontrado',
            'Potencial': clienteData ? (clienteData['Potencial'] || 'Vazio') : 'Não encontrado'
        };
    });
    metrics['modelo-negocio-preenchido'] = clientesContatadosComDados.filter(item => item['Modelo de negócio'] && item['Modelo de negócio'] !== 'Vazio');
    metrics['modelo-negocio-faltante'] = clientesContatadosComDados.filter(item => !item['Modelo de negócio'] || item['Modelo de negócio'] === 'Vazio');
    metrics['cliente-potencial-preenchido'] = clientesContatadosComDados.filter(item => item['Potencial'] && item['Potencial'] !== 'Vazio');
    metrics['cliente-potencial-faltante'] = clientesContatadosComDados.filter(item => !item['Potencial'] || item['Potencial'] === 'Vazio');

    const selectedQuarter = Math.floor(month / 3);
    const isConcludedInQuarter = (item) => item.ConcluidaEm && Math.floor(item.ConcluidaEm.getUTCMonth() / 3) === selectedQuarter && item.ConcluidaEm.getUTCFullYear() === year;
    const eligibleClientSet = new Set(clients.map(c => (c.Cliente || '').trim().toLowerCase()));
    metrics['total-labs-eligible'] = clients.length;
    metrics['ska-labs-realizado-list'] = [...new Map(rawActivities.filter(isConcludedInQuarter).filter(a => normalizeText(a.Atividade).includes('participou do ska labs')).filter(a => eligibleClientSet.has(a.ClienteCompleto)).map(a => [a.ClienteCompleto, a])).values()];

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

    const metaCobertura = (goals.coverage || 40) / 100;
    const clientesParaAtingirMeta = Math.max(0, Math.ceil(metrics['total-clientes-unicos-cs'] * metaCobertura));
    metrics['clientes-faltantes-meta-cobertura'] = Math.max(0, clientesParaAtingirMeta - clientesContatados.length);

    let ligacaoPerc = 0.65;
    if (clients.length > 0 && (normalizeText(clients[0].Segmento || '').includes('mid') || normalizeText(clients[0].Segmento || '').includes('enterprise'))) ligacaoPerc = 0.75;
    
    const targetLigacoes = Math.ceil(clientesParaAtingirMeta * ligacaoPerc);
    const targetEmails = clientesParaAtingirMeta - targetLigacoes;
    const realizadoLigacoes = (metrics['contato-ligacao'] || []).length;
    const realizadoEmails = (metrics['contato-email'] || []).length;
    metrics['dist-faltante-ligacao'] = Math.max(0, targetLigacoes - realizadoLigacoes);
    metrics['dist-faltante-email'] = Math.max(0, targetEmails - realizadoEmails);

    const clientesPorNegocio = {};
    clientsForPeriod.forEach(client => {
        const negocio = client['Negócio'] || 'Não Definido';
        if (negocio) clientesPorNegocio[negocio] = (clientesPorNegocio[negocio] || 0) + 1;
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
    const periodMetrics = { monthlyCoverages: [], totalClientsInPeriod: new Set(), playbookTotals: {} };
    const playbookKeys = ['plano', 'adocao', 'reunioes-qbr', 'planos-sucesso', 'followup', 'discovery', 'engajamento-smb', 'baixo-engajamento-mid'];
    playbookKeys.forEach(key => periodMetrics.playbookTotals[key] = { previsto: 0, realizado: 0 });
    const clientsForPeriod = (selectedCS === 'Todos') ? allClients : allClients.filter(d => d.CS?.trim() === selectedCS);

    for (let d = new Date(startDate); d <= endDate; d.setUTCMonth(d.getUTCMonth() + 1)) {
        if (d > endDate) break;
        const currentMonth = d.getUTCMonth();
        const currentYear = d.getUTCFullYear();
        const monthlyMetrics = calculateMetricsForPeriod(currentMonth, currentYear, rawActivities, clientsForPeriod, selectedCS, includeOnboarding, goals);
        const totalClientsUnicosCS = monthlyMetrics['total-clientes-unicos-cs'] || 0;
        const clientesContatadosCount = monthlyMetrics['cob-carteira']?.length || 0;
        const cobCarteiraPerc = totalClientsUnicosCS > 0 ? (clientesContatadosCount / totalClientsUnicosCS) * 100 : 0;
        periodMetrics.monthlyCoverages.push(cobCarteiraPerc);
        (monthlyMetrics['total-clientes'] || []).forEach(client => periodMetrics.totalClientsInPeriod.add(client.Cliente));
        playbookKeys.forEach(key => {
            periodMetrics.playbookTotals[key].previsto += (monthlyMetrics[`${key}-previsto`] || []).length;
            const realizadoTotal = (monthlyMetrics[`${key}-realizado`] || []).length + (monthlyMetrics[`${key}-atrasado-concluido`] || []).length + (monthlyMetrics[`${key}-concluido-antecipado`] || []).length;
            periodMetrics.playbookTotals[key].realizado += realizadoTotal;
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
    if (teamView === 'syneco') onboardingClients = onboardingClients.filter(c => normalizeText(c.Cliente).includes('syneco'));
    else if (teamView === 'outros') onboardingClients = onboardingClients.filter(c => !normalizeText(c.Cliente).includes('syneco'));
    if (selectedISM !== 'Todos') onboardingClients = onboardingClients.filter(c => c.ISM === selectedISM);

    okrs.filteredClients = onboardingClients;
    const onboardingClientNames = new Set(onboardingClients.map(c => (c.Cliente || '').trim().toLowerCase()));
    let onboardingActivities = activities.filter(a => onboardingClientNames.has(a.ClienteCompleto));
    if (selectedISM !== 'Todos') onboardingActivities = onboardingActivities.filter(a => (a['Responsável'] || '').trim() === selectedISM);

    const stageActivityNames = { preGoLive: normalizeText('Validação de usabilidade / Agendamento GO LIVE'), execucao: normalizeText('Planejamento Finalizado'), inicio: normalizeText('Contato de Welcome') };
    const stageCounts = { preGoLive: 0, execucao: 0, inicio: 0, backlog: 0 };
    const stageLists = { preGoLive: [], execucao: [], inicio: [], backlog: [] };
    okrs.totalFilteredClients = onboardingClients.length;

    const clientConcludedActivitiesMap = new Map();
    onboardingActivities.filter(a => a.ConcluidaEm).forEach(activity => {
        const clientKey = activity.ClienteCompleto;
        if (!clientConcludedActivitiesMap.has(clientKey)) clientConcludedActivitiesMap.set(clientKey, new Set());
        clientConcludedActivitiesMap.get(clientKey).add(normalizeText(activity.Atividade));
    });

    onboardingClients.forEach(client => {
        const clientKey = (client.Cliente || '').trim().toLowerCase();
        const concludedSet = clientConcludedActivitiesMap.get(clientKey) || new Set();
        if (concludedSet.has(stageActivityNames.preGoLive)) { stageCounts.preGoLive++; stageLists.preGoLive.push(client); }
        else if (concludedSet.has(stageActivityNames.execucao)) { stageCounts.execucao++; stageLists.execucao.push(client); }
        else if (concludedSet.has(stageActivityNames.inicio)) { stageCounts.inicio++; stageLists.inicio.push(client); }
        else { stageCounts.backlog++; stageLists.backlog.push(client); }
    });
    okrs.stageCounts = stageCounts; okrs.stageLists = stageLists;

    const concludedInPeriod = onboardingActivities.filter(a => a.ConcluidaEm >= startOfMonth && a.ConcluidaEm <= endOfMonth);
    const goLiveActivityName = normalizeText('Call/Reunião de GO LIVE');
    okrs.goLiveActivitiesConcluded = concludedInPeriod.filter(a => normalizeText(a.Atividade) === goLiveActivityName);
    okrs.goLiveActivitiesPredicted = onboardingActivities.filter(a => normalizeText(a.Atividade) === goLiveActivityName && !a.ConcluidaEm && a.PrevisaoConclusao >= startOfMonth && a.PrevisaoConclusao <= endOfMonth);

    const clientsWithGoLive = [...new Set(okrs.goLiveActivitiesConcluded.map(a => a.ClienteCompleto))];
    const clientsWithGoLiveAndNPSData = onboardingClients.filter(c => clientsWithGoLive.includes((c.Cliente || '').trim().toLowerCase()));
    okrs.clientsWithGoLive = clientsWithGoLiveAndNPSData;
    okrs.npsResponded = clientsWithGoLiveAndNPSData.filter(c => c.NPSOnboarding && String(c.NPSOnboarding).trim() !== '');
    okrs.highValueClients = onboardingClients.filter(c => c.ValorNaoFaturado > 50000);

    const portfolioClientNames = new Set(clients.map(c => (c.Cliente || '').trim().toLowerCase()));
    const onboardingStartActivities = [normalizeText('Validação e Planejamento'), normalizeText('Validação do pedido'), normalizeText('Contato de Welcome')];
    const onboardingStartCreationDate = new Map();
    activities.forEach(a => {
        const clientKey = a.ClienteCompleto;
        if (portfolioClientNames.has(clientKey) && a.CriadoEm && onboardingStartActivities.includes(normalizeText(a.Atividade))) {
            const existingDate = onboardingStartCreationDate.get(clientKey);
            if (!existingDate || a.CriadoEm < existingDate) onboardingStartCreationDate.set(clientKey, a.CriadoEm);
        }
    });

    const allGoLiveActivitiesConcluded = activities.filter(a => portfolioClientNames.has(a.ClienteCompleto) && normalizeText(a.Atividade) === goLiveActivityName && a.ConcluidaEm);
    const portfolioClientToNegocioMap = new Map();
    const portfolioClientToIsmMap = new Map();
    clients.forEach(c => {
        const clientKey = (c.Cliente || '').trim().toLowerCase();
        portfolioClientToNegocioMap.set(clientKey, c['Negócio'] || 'Não Definido');
        portfolioClientToIsmMap.set(clientKey, c.ISM);
    });

    const productMetrics = {};
    const allCompletedOnboardingsFilteredByIsm = [];
    const clientsWithCompletedGoLiveAllTime = new Set(allGoLiveActivitiesConcluded.map(a => a.ClienteCompleto));

    clientsWithCompletedGoLiveAllTime.forEach(clientKey => {
        const goLiveDate = allGoLiveActivitiesConcluded.filter(a => a.ClienteCompleto === clientKey).map(a => a.ConcluidaEm).sort((a,b) => b - a)[0];
        const playbookStartDate = onboardingStartCreationDate.get(clientKey);
        if (goLiveDate && playbookStartDate) {
            const duration = Math.floor((goLiveDate - playbookStartDate) / (1000 * 60 * 60 * 24));
            if (duration >= 0) {
                const clientIsm = portfolioClientToIsmMap.get(clientKey);
                const negocio = portfolioClientToNegocioMap.get(clientKey) || 'Não Definido';
                const ismMatchesFilter = (selectedISM === 'Todos' || clientIsm === selectedISM);
                if (!productMetrics[negocio]) productMetrics[negocio] = { durations: [], clientCount: 0 };
                if (ismMatchesFilter) {
                    productMetrics[negocio].durations.push(duration);
                    allCompletedOnboardingsFilteredByIsm.push({ client: clientKey, duration: duration });
                }
            }
        }
    });

    onboardingClients.forEach(client => {
        const negocio = client['Negócio'] || 'Não Definido';
        if (!productMetrics[negocio]) productMetrics[negocio] = { durations: [], clientCount: 0 };
        productMetrics[negocio].clientCount++;
    });

    okrs.completedOnboardingsList = allCompletedOnboardingsFilteredByIsm;
    const allDurationsFiltered = allCompletedOnboardingsFilteredByIsm.map(item => item.duration);
    okrs.averageCompletedOnboardingTime = allDurationsFiltered.length > 0 ? Math.round(allDurationsFiltered.reduce((a, b) => a + b, 0) / allDurationsFiltered.length) : 0;

    const chartLabels = [], clientCountData = [], avgTimeData = [];
    Object.keys(productMetrics).sort().forEach(negocio => {
        if (productMetrics[negocio].clientCount > 0 || productMetrics[negocio].durations.length > 0) {
            chartLabels.push(negocio);
            clientCountData.push(productMetrics[negocio].clientCount);
            const durations = productMetrics[negocio].durations;
            avgTimeData.push(durations.length > 0 ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length) : 0);
        }
    });
    okrs.productChartData = { labels: chartLabels, clientCount: clientCountData, avgTime: avgTimeData };

    const today = new Date();
    const openClientsWithDuration = onboardingClients.map(client => {
        const clientKey = (client.Cliente || '').trim().toLowerCase();
        const startDate = onboardingStartCreationDate.get(clientKey);
        return { ...client, onboardingDuration: startDate ? Math.floor((today - startDate) / (1000 * 60 * 60 * 24)) : null };
    }).filter(c => c.onboardingDuration !== null);
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
        const activityNameNormalized = normalizeText(config.name);
        const allActivitiesOfType = onboardingActivities.filter(a => normalizeText(a.Atividade) === activityNameNormalized);
        
        // Previsto (Pendente)
        okrs[`${key}Previsto`] = allActivitiesOfType.filter(a => a.PrevisaoConclusao >= startOfMonth && a.PrevisaoConclusao <= endOfMonth && !a.ConcluidaEm);
        // Realizado (Total no mês)
        const realizadoList = allActivitiesOfType.filter(a => a.ConcluidaEm >= startOfMonth && a.ConcluidaEm <= endOfMonth);
        // Antecipado
        okrs[`${key}Antecipado`] = realizadoList.filter(a => a.PrevisaoConclusao > endOfMonth);
        // Realizado no Prazo
        okrs[`${key}Realizado`] = realizadoList.filter(a => a.PrevisaoConclusao >= startOfMonth && a.PrevisaoConclusao <= endOfMonth);
        // Realizado com Atraso
        okrs[`${key}RealizadoAtrasado`] = realizadoList.filter(a => a.PrevisaoConclusao < startOfMonth);
        
        okrs[`${key}RealizadoTotal`] = realizadoList;

        if (key === 'welcome' || key === 'kickoff') {
            const getDiffDays = (a) => (a.ConcluidaEm && (key === 'welcome' ? a.CriadoEm : a.PrevisaoConclusao)) ? (a.ConcluidaEm - (key === 'welcome' ? a.CriadoEm : a.PrevisaoConclusao)) / (1000 * 60 * 60 * 24) : Infinity;
            okrs[`${key}SLAOk`] = realizadoList.filter(a => getDiffDays(a) <= config.sla);
            okrs[`${key}RealizadoForaPrazo`] = realizadoList.filter(a => getDiffDays(a) > config.sla);
        }
    });

    return okrs;
};


// --- MANIPULADOR DE MENSAGENS DO WORKER ---

self.onmessage = (e) => {
    const { type, payload } = e.data;

    try {
        if (type === 'INIT') {
            rawActivities = payload.rawActivities;
            rawClients = payload.rawClients;
            currentUser = payload.currentUser;
            manualEmailToCsMap = payload.manualEmailToCsMap;
            ismToFilter = currentUser.selectedISM || 'Todos';

            processInitialData(); // Processa as datas e injeta Dias sem Touch

            buildCsToSquadMap();

            const csSet = [...new Set(rawClients.map(d => d.CS && d.CS.trim()).filter(Boolean))];
            const squadSet = [...new Set(rawClients.map(d => d['Squad CS']).filter(Boolean))];
            const ismSet = [...new Set(rawClients.map(c => c.ISM).filter(Boolean))];
            
            // Extração segura de anos
            const allDates = [
                ...rawActivities.map(a => a.PrevisaoConclusao), 
                ...rawActivities.map(a => a.ConcluidaEm)
            ];
            const distinctYears = [...new Set(
                allDates
                .filter(d => d instanceof Date && !isNaN(d))
                .map(d => d.getUTCFullYear())
            )].sort((a, b) => b - a);
            
            // Se não achou nada, forçar o ano atual
            const years = distinctYears.length > 0 ? distinctYears : [new Date().getFullYear()];

            console.log(`[Worker] Anos identificados: ${years.join(', ')}`);

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
            let filteredClients = rawClients;
            let csForCalc = payload.selectedCS;

            if (currentUser.userGroup === 'onboarding' && ismToFilter !== 'Todos') {
                filteredClients = rawClients.filter(d => (d.ISM || '').trim() === ismToFilter);
                csForCalc = 'Todos'; 
            } else if (currentUser.isManager) {
                if (payload.selectedSquad !== 'Todos') {
                    filteredClients = rawClients.filter(d => d['Squad CS'] === payload.selectedSquad);
                    csForCalc = 'Todos';
                } else if (payload.selectedCS !== 'Todos') {
                    filteredClients = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
                }
            } else {
                filteredClients = rawClients.filter(d => d.CS?.trim() === payload.selectedCS);
            }

            const dataStore = calculateMetricsForPeriod(payload.month, payload.year, rawActivities, filteredClients, csForCalc, payload.includeOnboarding, payload.goals);

            if (payload.comparison !== 'none') {
                let compMes = payload.month, compAno = payload.year;
                if (payload.comparison === 'prev_month') {
                    compMes--;
                    if (compMes < 0) { compMes = 11; compAno--; }
                } else if (payload.comparison === 'prev_year') {
                    compAno--;
                }
                const comparisonMetrics = calculateMetricsForPeriod(compMes, compAno, rawActivities, filteredClients, csForCalc, payload.includeOnboarding, payload.goals);
                dataStore['cob-carteira_comp_perc'] = (comparisonMetrics['cob-carteira']?.length || 0) * 100 / (comparisonMetrics['total-clientes-unicos-cs'] || 1);
                dataStore['sensescore-avg_comp'] = comparisonMetrics['sensescore-avg'];
                dataStore['ska-labs-realizado-list_comp_perc'] = ((comparisonMetrics['ska-labs-realizado-list']?.length || 0) * 100) / (comparisonMetrics['total-labs-eligible'] || 1);
            }

            const onboardingDataStore = calculateNewOnboardingOKRs(payload.month, payload.year, rawActivities, filteredClients, payload.teamView, payload.selectedISM);
            const overdueMetrics = calculateOverdueMetrics(rawActivities, payload.selectedCS);
            const concludedInPeriodActivities = rawActivities.filter(a => a.ConcluidaEm?.getUTCMonth() === payload.month && a.ConcluidaEm?.getUTCFullYear() === payload.year);
            const divergentActivities = concludedInPeriodActivities.filter(a => {
                const activityOwner = (a['Responsável'] || '').trim();
                const activityCS = (a.CS || '').trim();
                const statusRaw = (a['Status Cliente'] || '').trim();
                const statusNormalized = normalizeText(statusRaw);
                const collapsedStatus = statusNormalized.replace(/[^a-z]/g, '');
                const isInactiveOrCancelled =
                    collapsedStatus.includes('inativ') ||
                    collapsedStatus.includes('cancel') ||
                    collapsedStatus.includes('semcontrato') ||
                    collapsedStatus.includes('encerrado');
                const hasValidCS =
                    activityCS &&
                    normalizeText(activityCS) !== 'não identificado' &&
                    normalizeText(activityCS) !== 'nao identificado';
                if (
                    payload.selectedCS === 'Todos' ||
                    isInactiveOrCancelled ||
                    !hasValidCS
                ) {
                    return false;
                }
                return activityCS !== payload.selectedCS &&
                    activityOwner === payload.selectedCS;
            });
            dataStore['divergent-activities'] = divergentActivities;
            const divergentClientCount = new Set(divergentActivities.map(a => a.ClienteCompleto)).size;

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
                        periodName: payload.period 
                    }
                });
            }
        }

        else if (type === 'CALCULATE_TREND') {
            const { metricId, title, type, baseMonth, baseYear, selectedCS, selectedSquad, includeOnboarding, segmento } = payload;

            const labels = [];
            const dataPoints = [];
            const baseDate = new Date(baseYear, baseMonth, 1);

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

                const periodMetrics = calculateMetricsForPeriod(month, year, rawActivities, filteredClients, csForCalc, includeOnboarding, {}); 

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
                    const antecipado = (periodMetrics[`${metricId}-concluido-antecipado`] || []).length;
                    value = realizado + atrasado + antecipado;
                }
                dataPoints.push(value.toFixed(2));
            }

            postMessage({
                type: 'TREND_DATA_COMPLETE',
                payload: { labels, dataPoints, title }
            });
        }

    } catch (err) {
        postMessage({
            type: 'ERROR',
            payload: {
                error: err.message,
                stack: err.stack
            }
        });
    }
};
