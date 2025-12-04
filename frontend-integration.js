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
            console.warn('🔍 [DIAGNÓSTICO] Iniciando fetch OTIMIZADO (Lotes menores)...');
            console.log('URL Alvo:', this.apiUrl);

            // ==============================================================================
            // 1. BUSCAR CLIENTES (AGORA COM PAGINAÇÃO PARA EVITAR ERRO 500)
            // ==============================================================================
            this.clientes = [];
            let clientPage = 1;
            const CLIENT_CHUNK = 2000; // Reduzido de 10000 para 2000 para segurança
            let moreClients = true;

            console.log(`📡 Buscando Clientes em lotes de ${CLIENT_CHUNK}...`);

            while (moreClients) {
                const url = `${this.apiUrl}/api/clientes?limit=${CLIENT_CHUNK}&page=${clientPage}`;
                
                try {
                    const resp = await fetch(url);
                    if (!resp.ok) throw new Error(`Erro ${resp.status}`);
                    
                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);
                    
                    if (chunk.length > 0) {
                        this.clientes = this.clientes.concat(chunk);
                        console.log(`   👤 Clientes Pág ${clientPage}: +${chunk.length} (Total: ${this.clientes.length})`);
                        clientPage++;
                        if (chunk.length < CLIENT_CHUNK) moreClients = false;
                    } else {
                        moreClients = false;
                    }
                } catch (err) {
                    console.error(`❌ Falha ao buscar clientes pág ${clientPage}:`, err);
                    moreClients = false; // Aborta clientes para tentar seguir
                }
            }
            console.log(`✅ Total Final Clientes: ${this.clientes.length}`);


            // ==============================================================================
            // 2. BUSCAR ATIVIDADES (COM LOTE REDUZIDO)
            // ==============================================================================
            this.atividades = [];
            let actPage = 1;
            const ACT_CHUNK = 5000; // Reduzido de 15000 para 5000 (Muito mais seguro)
            let moreActivities = true;
            let errorCount = 0;

            console.log(`📡 Buscando Atividades em lotes de ${ACT_CHUNK}...`);

            while (moreActivities) {
                const url = `${this.apiUrl}/api/atividades?limit=${ACT_CHUNK}&page=${actPage}`;
                console.log(`   🔄 Baixando Atividades Pág ${actPage}...`);
                
                try {
                    const resp = await fetch(url);
                    
                    if (!resp.ok) {
                        // Se der erro 500, tenta mais uma vez essa página antes de desistir
                        console.warn(`⚠️ Erro ${resp.status} na pág ${actPage}. Tentando novamente...`);
                        errorCount++;
                        if(errorCount > 3) throw new Error("Muitos erros consecutivos.");
                        await new Promise(r => setTimeout(r, 1000)); // Espera 1s
                        continue; 
                    }

                    const json = await resp.json();
                    const chunk = Array.isArray(json) ? json : (json.data || []);

                    if (chunk.length > 0) {
                        this.atividades = this.atividades.concat(chunk);
                        console.log(`   📦 +${chunk.length} atividades. Total acumulado: ${this.atividades.length}`);
                        actPage++;
                        errorCount = 0; // Reset contador de erro
                        
                        if (chunk.length < ACT_CHUNK) {
                            moreActivities = false;
                        }
                    } else {
                        moreActivities = false;
                    }
                    
                    // Freio de segurança (se passar de 100 páginas/500k registros, para)
                    if (actPage > 100) { 
                        console.warn("⚠️ Limite de segurança de páginas atingido.");
                        moreActivities = false; 
                    }

                } catch (err) {
                    console.error(`❌ Erro fatal na página ${actPage}:`, err);
                    // Opcional: break aqui se quiser parar tudo, ou continue se quiser tentar pular
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
            console.error('❌ [ERRO CRÍTICO]:', error);
            alert("Erro ao carregar dados. O sistema tentou recuperar mas falhou. Verifique o console.");
            throw error;
        }
    }

// Exportar
if (typeof module !== 'undefined' && module.exports) {
    module.exports = SensedataAPIClient;
}
