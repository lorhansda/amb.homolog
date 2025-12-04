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
            console.warn('🔍 [DIAGNÓSTICO] Iniciando fetch na API...');
            console.log('URL Alvo:', this.apiUrl);

            // Tenta buscar com um limite menor primeiro para testar se não é timeout
            const limit = 150000; 

            // 1. BUSCAR CLIENTES
            console.log(`📡 Buscando Clientes (limit=${limit})...`);
            const clientesResp = await fetch(`${this.apiUrl}/api/clientes?limit=${limit}`);
            console.log('📡 Status Clientes:', clientesResp.status);
            const clientesJson = await clientesResp.json();
            
            // LOG CRÍTICO: Mostra a estrutura real que veio
            console.log('📦 [JSON CLIENTES RECEBIDO]:', clientesJson); 

            // 2. BUSCAR ATIVIDADES
            console.log(`📡 Buscando Atividades (limit=${limit})...`);
            const atividadesResp = await fetch(`${this.apiUrl}/api/atividades?limit=${limit}`);
            console.log('📡 Status Atividades:', atividadesResp.status);
            const atividadesJson = await atividadesResp.json();

            // LOG CRÍTICO: Mostra a estrutura real que veio
            console.log('📦 [JSON ATIVIDADES RECEBIDO]:', atividadesJson);

            // TENTATIVA DE DESCOBRIR ONDE ESTÃO OS DADOS
            // Verifica se estão em 'data', 'results', ou na raiz
            this.clientes = clientesJson.data || clientesJson.results || (Array.isArray(clientesJson) ? clientesJson : []);
            this.atividades = atividadesJson.data || atividadesJson.results || (Array.isArray(atividadesJson) ? atividadesJson : []);

            console.log('📊 [RESUMO DO PROCESSAMENTO]');
            console.log(`   Clientes encontrados: ${this.clientes.length}`);
            console.log(`   Atividades encontradas: ${this.atividades.length}`);

            this.ultimaAtualizacaoClientes = new Date();

            return {
                atividades: this.atividades,
                clientes: this.clientes,
                timestamp: this.ultimaAtualizacaoClientes
            };

        } catch (error) {
            console.error('❌ [ERRO FATAL NO FETCH]:', error);
            alert("Erro na conexão com a API. Abra o Console (F12) e mande um print para o suporte.");
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
