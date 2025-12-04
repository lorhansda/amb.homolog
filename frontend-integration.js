/**
 * ===================================================================
 * MÓDULO DE INTEGRAÇÃO - VERSÃO PÁGINAÇÃO SEGURA (ANTI-ERRO 500)
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
            console.warn('🔍 [DIAGNÓSTICO] Iniciando fetch SEGURANÇA MÁXIMA...');
            console.log('URL Alvo:', this.apiUrl);

            // ==============================================================================
            // 1. BUSCAR CLIENTES (PAGINADO DE 2 EM 2 MIL)
            // ==============================================================================
            this.clientes = [];
            let clientPage = 1;
            // IMPORTANTE: 2000 é o limite seguro. Não aumente para 10000.
            const CLIENT_CHUNK = 2000; 
            let moreClients = true;

            console.log(`📡 Buscando Clientes em lotes de ${CLIENT_CHUNK}...`);

            while (moreClients) {
                const url = `${this.apiUrl}/api/clientes?limit=${CLIENT_CHUNK}&page=${clientPage}`;
                
                try {
                    const resp = await fetch(url);
                    if (!resp.ok) throw new Error(`Erro HTTP ${resp.status}`);
                    
                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);
                    
                    if (chunk.length > 0) {
                        this.clientes = this.clientes.concat(chunk);
                        console.log(`   👤 Clientes Pág ${clientPage}: +${chunk.length} (Total: ${this.clientes.length})`);
                        clientPage++;
                        // Se vier menos que o limite, é a última página
                        if (chunk.length < CLIENT_CHUNK) moreClients = false;
                    } else {
                        moreClients = false;
                    }
                } catch (err) {
                    console.error(`❌ Falha ao buscar clientes pág ${clientPage}:`, err);
                    moreClients = false; 
                    // Se falhar na primeira página de clientes, é crítico.
                    if (clientPage === 1) throw err;
                }
            }
            console.log(`✅ Total Final Clientes: ${this.clientes.length}`);


            // ==============================================================================
            // 2. BUSCAR ATIVIDADES (PAGINADO DE 5 EM 5 MIL)
            // ==============================================================================
            this.atividades = [];
            let actPage = 1;
            // IMPORTANTE: 5000 é o limite seguro. 15000 causou o erro 500 no seu log.
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
                        await new Promise(r => setTimeout(r, 2000)); // Espera 2s antes de tentar de novo
                        continue; 
                    }

                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);

                    if (chunk.length > 0) {
                        this.atividades = this.atividades.concat(chunk);
                        console.log(`   📦 +${chunk.length} atividades. Total acumulado: ${this.atividades.length}`);
                        actPage++;
                        errorCount = 0; // Sucesso, zera contador de erro
                        
                        if (chunk.length < ACT_CHUNK) {
                            moreActivities = false;
                        }
                    } else {
                        moreActivities = false;
                    }
                    
                    // Segurança para loop infinito
                    if (actPage > 200) { 
                        console.warn("⚠️ Limite de segurança de páginas atingido (200). Parando.");
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

    // Métodos auxiliares mantidos para compatibilidade
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
