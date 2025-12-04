/**
 * ===================================================================
 * MÓDULO DE INTEGRAÇÃO COM API - Para o Frontend (GitHub Pages)
 * ===================================================================
 * Versão com carga TOTAL de dados (Limites Aumentados)
 */

class SensedataAPIClient {
    constructor(apiUrl) {
        this.apiUrl = apiUrl;
        this.atividades = [];
        this.clientes = [];
        this.ultimaAtualizacaoClientes = null;
    }

    /**
     * Carregar TODOS os dados da API (Clientes + Atividades)
     */
    async carregarDadosClientes() {
        try {
            console.log('🚀 Iniciando carga total de dados da API...');

            // 1. Carregar Clientes (Limite aumentado para 100k)
            const clientesPromise = fetch(`${this.apiUrl}/api/clientes?limit=100000`)
                .then(res => {
                    if (!res.ok) throw new Error(`Erro clientes: ${res.status}`);
                    return res.json();
                });

            // 2. Carregar Atividades (Limite aumentado para 150k para pegar a ID 1441504)
            // ATENÇÃO: Isso pode demorar alguns segundos dependendo da conexão
            const atividadesPromise = fetch(`${this.apiUrl}/api/atividades?limit=150000`)
                .then(res => {
                    if (!res.ok) throw new Error(`Erro atividades: ${res.status}`);
                    return res.json();
                });

            // Executa em paralelo para ser mais rápido
            const [clientesData, atividadesData] = await Promise.all([clientesPromise, atividadesPromise]);

            // Processa Clientes
            this.clientes = clientesData.data || [];
            this.ultimaAtualizacaoClientes = new Date();

            // Processa Atividades
            this.atividades = atividadesData.data || [];

            console.log(`✅ Carga Concluída!`);
            console.log(`   - Clientes: ${this.clientes.length.toLocaleString()}`);
            console.log(`   - Atividades: ${this.atividades.length.toLocaleString()}`);

            return {
                atividades: this.atividades, // Agora retorna os dados reais!
                clientes: this.clientes,
                timestamp: this.ultimaAtualizacaoClientes
            };

        } catch (error) {
            console.error('❌ Erro fatal ao carregar dados da API:', error);
            alert("Erro ao baixar dados do servidor. Verifique o console.");
            throw error;
        }
    }

    /**
     * Filtrar clientes por segmento
     */
    filtrarClientesPorSegmento(segmento) {
        return this.clientes.filter(c => c.segmento === segmento);
    }

    /**
     * Obter lista única de CSs
     */
    obterListaCSs() {
        const csSet = new Set();
        this.clientes.forEach(c => {
            if (c.cs) csSet.add(c.cs);
        });
        return Array.from(csSet).sort();
    }

    /**
     * Obter lista única de segmentos
     */
    obterListaSegmentos() {
        const segmentosSet = new Set();
        this.clientes.forEach(c => {
            if (c.segmento) segmentosSet.add(c.segmento);
        });
        return Array.from(segmentosSet).sort();
    }

    /**
     * Obter lista única de squads
     */
    obterListaSquads() {
        const squadsSet = new Set();
        this.clientes.forEach(c => {
            if (c.squad_cs) squadsSet.add(c.squad_cs);
        });
        return Array.from(squadsSet).sort();
    }

    /**
     * Converter dados de CLIENTES para formato compatível com o worker.js
     */
    converterClientesParaFormatoOriginal() {
        return this.clientes.map(c => {
            const dados = c.dados_json ? JSON.parse(c.dados_json) : {};
            return {
                Cliente: c.cliente,
                Segmento: c.segmento,
                CS: c.cs,
                'Squad CS': c.squad_cs,
                Fase: c.fase,
                ISM: c.ism,
                'Negócio': c.negocio,
                Comercial: c.comercial,
                'NPS onboarding': c.nps_onboarding,
                'Valor total não faturado': c.valor_nao_faturado,
                id_legacy: c.id_legacy, // Importante para o V10 do worker
                ...dados
            };
        });
    }
    
    // Mantém compatibilidade caso algo chame conversion de atividades separadamente
    // (Embora o worker V10 já aceite o formato JSON direto da API)
}

// Exportar para uso em módulos
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SensedataAPIClient;
}
