/**
 * ===================================================================
 * MÓDULO DE INTEGRAÇÃO - OTIMIZADO PARA ALTO VOLUME
 * ===================================================================
 */

class SensedataAPIClient {
    constructor(apiUrl) {
        this.apiUrl = apiUrl;
        this.atividades = [];
        this.clientes = [];
        this.ultimaAtualizacaoClientes = null;
    }

    async carregarDadosClientes() {
        try {
            console.warn('🔍 [DIAGNÓSTICO] Iniciando fetch OTIMIZADO (Paginação + Filtro de Data)...');

            // ==============================================================================
            // 1. BUSCAR CLIENTES (PAGINADO)
            // Agora o Worker suporta paginação, então baixamos em blocos seguros
            // ==============================================================================
            this.clientes = [];
            let clientPage = 1;
            const CLIENT_CHUNK = 2000; 
            let moreClients = true;

            console.log(`📡 Buscando Clientes em lotes de ${CLIENT_CHUNK}...`);

            while (moreClients) {
                // Adiciona um timestamp para evitar cache do navegador
                const url = `${this.apiUrl}/api/clientes?limit=${CLIENT_CHUNK}&page=${clientPage}&t=${Date.now()}`;
                
                try {
                    const resp = await fetch(url);
                    if (!resp.ok) throw new Error(`Erro HTTP ${resp.status} em Clientes`);
                    
                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);
                    
                    if (chunk.length > 0) {
                        this.clientes = this.clientes.concat(chunk);
                        console.log(`   👤 Clientes Pág ${clientPage}: +${chunk.length} (Total: ${this.clientes.length})`);
                        clientPage++;
                        
                        // Se vier menos que o limite, acabou
                        if (chunk.length < CLIENT_CHUNK) {
                            moreClients = false;
                        }
                    } else {
                        moreClients = false;
                    }
                } catch (err) {
                    console.error(`❌ Falha crítica em clientes pág ${clientPage}:`, err);
                    throw err;
                }
            }
            console.log(`✅ Total de Clientes: ${this.clientes.length}`);


            // ==============================================================================
            // 2. BUSCAR ATIVIDADES (COM FILTRO DE DATA OBRIGATÓRIO)
            // Para evitar baixar 2 milhões de linhas, filtramos apenas o ano corrente/recente.
            // ==============================================================================
            this.atividades = [];
            const ACT_CHUNK = 3000; // Lote conservador
            let moreActivities = true;
            let errorCount = 0;
            let cursor = null;
            let supportsCursor = null; // desconhecido
            let chunkIndex = 1;

            // CONFIGURAÇÃO DO FILTRO DE DATA
            // Se quiser limitar o histórico, defina window.DASH_FETCH_CONFIG.activitiesSince = 'YYYY-MM-DD'
            const DATA_INICIO = (window.DASH_FETCH_CONFIG?.activitiesSince || '').trim() || null; 
            console.log(
                DATA_INICIO
                    ? `📡 Buscando Atividades a partir de ${DATA_INICIO}...`
                    : '📡 Buscando TODAS as atividades disponíveis (sem filtro de data)...'
            );

            while (moreActivities) {
                let url = `${this.apiUrl}/api/atividades?limit=${ACT_CHUNK}`;
                if (DATA_INICIO) {
                    url += `&since=${encodeURIComponent(DATA_INICIO)}`;
                }
                if (supportsCursor === true && cursor) {
                    url += `&cursor=${encodeURIComponent(cursor)}`;
                } else if (supportsCursor === false) {
                    url += `&page=${chunkIndex}`;
                }
                console.log(`   🔄 Baixando Atividades Lote ${chunkIndex}${supportsCursor === true ? ` (cursor=${cursor ?? 'inicial'})` : supportsCursor === false ? ` (page=${chunkIndex})` : ''}...`);
                
                try {
                    const resp = await fetch(url);
                    
                    if (!resp.ok) {
                        console.warn(`⚠️ Erro ${resp.status} na pág ${actPage}. Tentativa ${errorCount + 1}...`);
                        errorCount++;
                        if(errorCount > 3) throw new Error("Falha persistente na API de atividades.");
                        await new Promise(r => setTimeout(r, 2000)); 
                        continue; 
                    }

                    const hasCursorHeader = resp.headers.has('x-next-cursor');
                    const nextCursorHeader = hasCursorHeader ? resp.headers.get('x-next-cursor') : null;
                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.results || json.data || []);

                    if (chunk.length > 0) {
                        this.atividades = this.atividades.concat(chunk);
                        console.log(`   📦 +${chunk.length} atividades. Total: ${this.atividades.length}`);
                        chunkIndex++;
                        errorCount = 0;

                        if (supportsCursor === null) {
                            supportsCursor = hasCursorHeader;
                        }

                        if (supportsCursor === true) {
                            cursor = nextCursorHeader || null;
                            if (!cursor) moreActivities = false;
                        } else {
                            if (chunk.length < ACT_CHUNK) {
                                moreActivities = false;
                            }
                            if (chunkIndex > 100) { 
                                console.warn("⚠️ Limite de segurança de páginas atingido (100). Parando.");
                                moreActivities = false; 
                            }
                        }
                    } else {
                        moreActivities = false;
                    }

                } catch (err) {
                    console.error(`❌ Erro ao buscar atividades pág ${actPage}:`, err);
                    moreActivities = false; 
                }
            }

            console.log('📊 [RESUMO FINAL]');
            console.log(`   Clientes: ${this.clientes.length}`);
            console.log(
                DATA_INICIO
                    ? `   Atividades (desde ${DATA_INICIO}): ${this.atividades.length}`
                    : `   Atividades carregadas: ${this.atividades.length}`
            );

            this.ultimaAtualizacaoClientes = new Date();

            return {
                atividades: this.atividades,
                clientes: this.clientes,
                timestamp: this.ultimaAtualizacaoClientes
            };

        } catch (error) {
            console.error('❌ [ERRO GERAL]:', error);
            alert("Erro ao carregar dados. Verifique o console (F12).");
            throw error;
        }
    }

    // Métodos auxiliares para compatibilidade
    filtrarClientesPorSegmento(s) { return []; }
    obterListaCSs() { return []; }
    obterListaSegmentos() { return []; }
    obterListaSquads() { return []; }
    converterClientesParaFormatoOriginal() { 
        return this.clientes.map(c => ({
            Cliente: c.cliente || c.name,
            CS: c.cs || c.owner,
            id_legacy: c.id_legacy,
            ...c
        }));
    }
}
