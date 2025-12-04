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
            let actPage = 1;
            const ACT_CHUNK = 3000; // Lote conservador
            let moreActivities = true;
            let errorCount = 0;

            // CONFIGURAÇÃO DO FILTRO DE DATA
            // Define data de corte: 01/01/2025 (Ajuste conforme sua necessidade de histórico no painel)
            const DATA_INICIO = '2025-01-01'; 
            
            console.log(`📡 Buscando Atividades a partir de ${DATA_INICIO}...`);

            while (moreActivities) {
                // Passamos o parâmetro 'since' para o Worker filtrar no SQL
                const url = `${this.apiUrl}/api/atividades?limit=${ACT_CHUNK}&page=${actPage}&since=${DATA_INICIO}`;
                console.log(`   🔄 Baixando Atividades Pág ${actPage}...`);
                
                try {
                    const resp = await fetch(url);
                    
                    if (!resp.ok) {
                        console.warn(`⚠️ Erro ${resp.status} na pág ${actPage}. Tentativa ${errorCount + 1}...`);
                        errorCount++;
                        if(errorCount > 3) throw new Error("Falha persistente na API de atividades.");
                        await new Promise(r => setTimeout(r, 2000)); 
                        continue; 
                    }

                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);

                    if (chunk.length > 0) {
                        this.atividades = this.atividades.concat(chunk);
                        console.log(`   📦 +${chunk.length} atividades. Total: ${this.atividades.length}`);
                        actPage++;
                        errorCount = 0;
                        
                        if (chunk.length < ACT_CHUNK) {
                            moreActivities = false;
                        }
                    } else {
                        moreActivities = false;
                    }
                    
                    // Trava de segurança (aprox 300k registros)
                    // Se precisar de mais que isso, o navegador vai travar de qualquer jeito.
                    if (actPage > 100) { 
                        console.warn("⚠️ Limite de segurança de páginas atingido (100). Parando.");
                        moreActivities = false; 
                    }

                } catch (err) {
                    console.error(`❌ Erro ao buscar atividades pág ${actPage}:`, err);
                    moreActivities = false; 
                }
            }

            console.log('📊 [RESUMO FINAL]');
            console.log(`   Clientes: ${this.clientes.length}`);
            console.log(`   Atividades (desde ${DATA_INICIO}): ${this.atividades.length}`);

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
