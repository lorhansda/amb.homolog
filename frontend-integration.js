/**
 * ===================================================================
 * MÓDULO DE INTEGRAÇÃO - CORREÇÃO DO LOOP INFINITO DE CLIENTES
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
            console.warn('🔍 [DIAGNÓSTICO] Iniciando fetch CORRIGIDO...');
            console.log('URL Alvo:', this.apiUrl);

            // ==============================================================================
            // 1. BUSCAR CLIENTES (BUSCA ÚNICA - SEM LOOP)
            // O Worker atual retorna todos os clientes de uma vez, então não devemos paginar.
            // ==============================================================================
            this.clientes = [];
            console.log(`📡 Buscando lista completa de Clientes...`);

            try {
                // Removemos parametros de limite para o Worker trazer tudo (comportamento padrão dele)
                const url = `${this.apiUrl}/api/clientes`; 
                const resp = await fetch(url);
                
                if (!resp.ok) throw new Error(`Erro HTTP ${resp.status}`);
                
                const json = await resp.json();
                // Verifica se veio como array ou dentro de um objeto 'data'
                this.clientes = Array.isArray(json) ? json : (json.data || []);
                
                console.log(`✅ Total de Clientes carregados: ${this.clientes.length}`);

            } catch (err) {
                console.error(`❌ Falha crítica ao buscar clientes:`, err);
                throw err;
            }


            // ==============================================================================
            // 2. BUSCAR ATIVIDADES (PAGINADO DE 5 EM 5 MIL)
            // O Worker de atividades SUPORTA paginação, então aqui mantemos o loop.
            // ==============================================================================
            this.atividades = [];
            let actPage = 1;
            const ACT_CHUNK = 5000; 
            let moreActivities = true;
            let errorCount = 0;

            console.log(`📡 Buscando Atividades em lotes de ${ACT_CHUNK}...`);

            while (moreActivities) {
                const url = `${this.apiUrl}/api/atividades?limit=${ACT_CHUNK}&page=${actPage}`;
                console.log(`   🔄 Baixando Atividades Pág ${actPage}...`);
                
                try {
                    const resp = await fetch(url);
                    
                    if (!resp.ok) {
                        console.warn(`⚠️ Erro ${resp.status} na pág ${actPage}. Tentando novamente (Tentativa ${errorCount + 1})...`);
                        errorCount++;
                        if(errorCount > 3) throw new Error("Muitos erros consecutivos na API.");
                        await new Promise(r => setTimeout(r, 2000)); 
                        continue; 
                    }

                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);

                    if (chunk.length > 0) {
                        this.atividades = this.atividades.concat(chunk);
                        console.log(`   📦 +${chunk.length} atividades. Total acumulado: ${this.atividades.length}`);
                        actPage++;
                        errorCount = 0; 
                        
                        // Se vier menos que o limite solicitado, é a última página
                        if (chunk.length < ACT_CHUNK) {
                            moreActivities = false;
                        }
                    } else {
                        moreActivities = false;
                    }
                    
                    // Trava de segurança (aprox 1 milhão de registros)
                    if (actPage > 200) { 
                        console.warn("⚠️ Limite de segurança de páginas atingido. Parando loop.");
                        moreActivities = false; 
                    }

                } catch (err) {
                    console.error(`❌ Erro fatal na página ${actPage}:`, err);
                    moreActivities = false; 
                }
            }

            console.log('📊 [RESUMO FINAL DO CARREGAMENTO]');
            console.log(`   Total Clientes: ${this.clientes.length}`);
            console.log(`   Total Atividades: ${this.atividades.length}`);

            this.ultimaAtualizacaoClientes = new Date();

            return {
                atividades: this.atividades,
                clientes: this.clientes,
                timestamp: this.ultimaAtualizacaoClientes
            };

        } catch (error) {
            console.error('❌ [ERRO CRÍTICO NO FRONTEND]:', error);
            alert("Erro ao carregar dados. Abra o console (F12) para ver os detalhes.");
            throw error;
        }
    }

    // Métodos auxiliares
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
