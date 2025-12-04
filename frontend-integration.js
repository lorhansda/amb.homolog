/**
 * ===================================================================
 * MÓDULO DE INTEGRAÇÃO - VERSÃO DIAGNÓSTICO
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
            console.warn('🔍 [DIAGNÓSTICO] Iniciando fetch na API (Modo Paginado)...');
            console.log('URL Alvo:', this.apiUrl);

            // 1. BUSCAR CLIENTES (Clientes são leves, podemos manter busca única ou paginada simples)
            // Mantivemos um limite alto seguro para clientes, pois geralmente são menos registros que atividades
            console.log(`📡 Buscando Clientes...`);
            const clientesResp = await fetch(`${this.apiUrl}/api/clientes?limit=10000`); 
            const clientesJson = await clientesResp.json();
            this.clientes = Array.isArray(clientesJson) ? clientesJson : (clientesJson.data || []);
            console.log(`✅ ${this.clientes.length} Clientes carregados.`);

            // 2. BUSCAR ATIVIDADES COM PAGINAÇÃO (LOOP)
            // Isso evita o Erro 500 por estouro de memória no Worker
            this.atividades = [];
            let page = 1;
            const CHUNK_SIZE = 15000; // Tamanho seguro por página (Cloudflare Pro aguenta bem)
            let hasMore = true;

            console.log(`📡 Buscando Atividades em lotes de ${CHUNK_SIZE}...`);

            while (hasMore) {
                const url = `${this.apiUrl}/api/atividades?limit=${CHUNK_SIZE}&page=${page}`;
                console.log(`   🔄 Baixando página ${page}...`);
                
                const resp = await fetch(url);
                
                if (!resp.ok) {
                    console.error(`❌ Erro na página ${page}: ${resp.status}`);
                    throw new Error(`Falha ao buscar atividades (Página ${page})`);
                }

                const json = await resp.json();
                const chunk = Array.isArray(json) ? json : (json.data || []);

                if (chunk.length > 0) {
                    this.atividades = this.atividades.concat(chunk);
                    console.log(`   📦 +${chunk.length} atividades recebidas. Total: ${this.atividades.length}`);
                    page++;
                    
                    // Se o chunk veio menor que o limite, acabaram os dados
                    if (chunk.length < CHUNK_SIZE) {
                        hasMore = false;
                    }
                } else {
                    hasMore = false;
                }
                
                // Segurança para não loopar infinito em caso de erro lógico
                if (page > 50) { 
                    console.warn("⚠️ Limite de segurança de páginas atingido.");
                    hasMore = false; 
                }
            }

            console.log('📊 [RESUMO FINAL]');
            console.log(`   Total Clientes: ${this.clientes.length}`);
            console.log(`   Total Atividades: ${this.atividades.length}`);

            this.ultimaAtualizacaoClientes = new Date();

            return {
                atividades: this.atividades,
                clientes: this.clientes,
                timestamp: this.ultimaAtualizacaoClientes
            };

        } catch (error) {
            console.error('❌ [ERRO FATAL NO FETCH]:', error);
            alert("Erro na conexão com a API. Verifique o console.");
            throw error;
        }
    }

    // Métodos auxiliares mantidos para evitar erro de "not a function"
    filtrarClientesPorSegmento(s) { return []; }
    obterListaCSs() { return []; }
    obterListaSegmentos() { return []; }
    obterListaSquads() { return []; }
    converterClientesParaFormatoOriginal() { 
        // Conversor de emergência
        return this.clientes.map(c => ({
            Cliente: c.cliente || c.name,
            CS: c.cs || c.owner,
            id_legacy: c.id_legacy,
            ...c
        }));
    }
}

// Exportar
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SensedataAPIClient;
}
