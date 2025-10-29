/**
 * ===================================================================
 * MÓDULO DE INTEGRAÇÃO COM API - Para o Frontend (GitHub Pages)
 * ===================================================================
 * Este arquivo deve ser incluído no seu index.html:
 * <script src="frontend-integration.js"></script>
 * 
 * Ele substitui a lógica de carregamento manual de arquivos pela
 * chamada automática à API do Cloudflare Worker.
 */

class SensedataAPIClient {
  constructor(apiUrl) {
    this.apiUrl = apiUrl;
    this.atividades = []; // Atividades serão carregadas manualmente por enquanto
    this.clientes = [];
    this.ultimaAtualizacaoClientes = null;
  }

  /**
   * Carregar dados da API
   * [MODIFICADO HÍBRIDO] Carrega apenas clientes. Atividades está desativado.
   */
  async carregarDadosClientes() {
    try {
      console.log('Carregando dados da API (Apenas Clientes)...');
      
      // --- MELHORIA FUTURA ---
      // Para ativar a carga automática de atividades, você usaria um fetch similar a este:
      /*
      const atividadesResponse = await fetch(`${this.apiUrl}/api/atividades?limit=10000`);
      if (!atividadesResponse.ok) {
        throw new Error(`Erro ao carregar atividades: ${atividadesResponse.status}`);
      }
      const atividadesData = await atividadesResponse.json();
      this.atividades = atividadesData.data || [];
      console.log(`   - Atividades: ${this.atividades.length} (automático)`);
      */
      // --- Fim da Melhoria Futura ---

      // Carregar clientes
      const clientesResponse = await fetch(`${this.apiUrl}/api/clientes?limit=10000`);
      if (!clientesResponse.ok) {
        throw new Error(`Erro ao carregar clientes: ${clientesResponse.status}`);
      }
      const clientesData = await clientesResponse.json();
      this.clientes = clientesData.data || [];
      this.ultimaAtualizacaoClientes = new Date();
      
      console.log(`✅ Dados de Clientes carregados com sucesso!`);
      console.log(`   - Clientes: ${this.clientes.length} (automático)`);
      console.log(`   - Última atualização (Clientes): ${this.ultimaAtualizacaoClientes.toLocaleString()}`);

      return {
        // Atividades estarão vazias aqui, pois vêm do upload manual
        atividades: [],
        clientes: this.clientes,
        timestamp: this.ultimaAtualizacaoClientes
      };
    } catch (error) {
      console.error('❌ Erro ao carregar dados de Clientes da API:', error);
      throw error;
    }
  }

  // --- MELHORIA FUTURA ---
  // Quando for carregar atividades da API, você precisará de uma função como esta
  /*
  async carregarDadosAtividades() {
    try {
      console.log('Carregando dados de Atividades da API...');
      const atividadesResponse = await fetch(`${this.apiUrl}/api/atividades?limit=10000`);
      if (!atividadesResponse.ok) {
        throw new Error(`Erro ao carregar atividades: ${atividadesResponse.status}`);
      }
      const atividadesData = await atividadesResponse.json();
      this.atividades = atividadesData.data || [];
      console.log(`✅ Dados de Atividades carregados com sucesso! (${this.atividades.length})`);
      return this.atividades;
    } catch (error) {
      console.error('❌ Erro ao carregar dados de Atividades da API:', error);
      throw error;
    }
  }
  */
  // --- Fim da Melhoria Futura ---


  /**
   * Carregar atividades de um cliente específico (Mantido para possível uso futuro)
   */
  async carregarAtividadesPorCliente(nomeCliente) {
    try {
      const response = await fetch(
        `${this.apiUrl}/api/atividades-por-cliente?cliente=${encodeURIComponent(nomeCliente)}`
      );
      if (!response.ok) {
        throw new Error(`Erro ao carregar atividades: ${response.status}`);
      }
      const data = await response.json();
      return data.data || [];
    } catch (error) {
      console.error(`Erro ao carregar atividades do cliente ${nomeCliente}:`, error);
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
   * Obter lista única de CSs (Baseado nos Clientes da API)
   */
  obterListaCSs() {
    const csSet = new Set();
    this.clientes.forEach(c => {
      if (c.cs) csSet.add(c.cs);
    });
    return Array.from(csSet).sort();
  }

  /**
   * Obter lista única de segmentos (Baseado nos Clientes da API)
   */
  obterListaSegmentos() {
    const segmentosSet = new Set();
    this.clientes.forEach(c => {
      if (c.segmento) segmentosSet.add(c.segmento);
    });
    return Array.from(segmentosSet).sort();
  }

  /**
   * Obter lista única de squads (Baseado nos Clientes da API)
   */
  obterListaSquads() {
    const squadsSet = new Set();
    this.clientes.forEach(c => {
      if (c.squad_cs) squadsSet.add(c.squad_cs);
    });
    return Array.from(squadsSet).sort();
  }

  /**
   * Converter dados de CLIENTES para formato compatível com o worker.js original
   * [MODIFICADO HÍBRIDO] Ignora a conversão de atividades (pois virão do arquivo)
   */
  converterClientesParaFormatoOriginal() {
    // --- MELHORIA FUTURA ---
    // Quando atividades vierem da API, você usará a função converterParaFormatoOriginal() completa
    // que converte ambos, atividades e clientes.
    // --- Fim da Melhoria Futura ---

    // Converter clientes (permanece)
    const clientesFormatados = this.clientes.map(c => {
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
        ...dados // Garante que campos extras sejam incluídos
      };
    });

    return clientesFormatados; // Retorna apenas os clientes formatados
  }

  /**
   * Sincronizar dados de CLIENTES periodicamente (a cada N minutos)
   * [MODIFICADO HÍBRIDO] A sincronização agora afeta apenas os clientes.
   */
  iniciarSincronizacaoAutomaticaClientes(intervaloMinutos = 5) {
    console.log(`⏰ Sincronização automática (Apenas Clientes) ativada a cada ${intervaloMinutos} minutos`);
    
    setInterval(async () => {
      try {
        // carregarDadosClientes() busca só clientes
        await this.carregarDadosClientes(); 
        console.log(`🔄 Clientes sincronizados em ${new Date().toLocaleTimeString()}`);
        
        // Disparar evento customizado para notificar a aplicação
        window.dispatchEvent(new CustomEvent('clientes-sincronizados', {
          detail: {
            // Envia apenas clientes e timestamp
            clientes: this.clientes,
            timestamp: this.ultimaAtualizacaoClientes
          }
        }));
      } catch (error) {
        console.error('Erro na sincronização automática de clientes:', error);
      }
    }, intervaloMinutos * 60 * 1000);
  }
}


// Exportar para uso em módulos
if (typeof module !== 'undefined' && module.exports) {
  module.exports = SensedataAPIClient;
}