/**
 * worker.js
 * Este script roda em segundo plano para fazer todo o processamento pesado,
 * como ler e analisar as planilhas, sem travar a interface do usuário.
 */

// 1. Importa a biblioteca XLSX, pois o worker não tem acesso ao script da página principal.
importScripts('https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js');

// 2. Funções de suporte que foram movidas da página principal para cá.

function normalizeText(str = '') {
    return String(str).toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function parseDate(dateInput) {
    if (!dateInput) return null;
    if (dateInput instanceof Date && !isNaN(dateInput)) {
        return dateInput;
    }
    const dateStr = String(dateInput).trim();
    const dateOnlyStr = dateStr.split(' ')[0];
    let parts;
    parts = dateOnlyStr.split(/[-/]/);
    if (parts.length === 3 && parts[2].length === 4) {
        const day = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10);
        const year = parseInt(parts[2], 10);
        if (!isNaN(day) && !isNaN(month) && !isNaN(year)) {
            return new Date(Date.UTC(year, month - 1, day));
        }
    }
    if (parts.length === 3 && parts[0].length === 4) {
         const year = parseInt(parts[0], 10);
         const month = parseInt(parts[1], 10);
         const day = parseInt(parts[2], 10);
         if (!isNaN(year) && !isNaN(month) && !isNaN(day)) {
            return new Date(Date.UTC(year, month - 1, day));
         }
    }
    return null;
}

function readFile(file) {
    return new Promise((resolve, reject) => {
        const excelSerialToDateObject = (serial) => {
            const excelTimestamp = (serial - 25569) * 86400 * 1000;
            const date = new Date(excelTimestamp);
            const timezoneOffset = date.getTimezoneOffset() * 60000;
            return new Date(date.getTime() + timezoneOffset);
        };
        const reader = new FileReader();
        reader.onload = (event) => {
            try {
                const data = event.target.result;
                const workbook = XLSX.read(data, { type: 'array' });
                const sheetName = workbook.SheetNames[0];
                const jsonData = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName], { defval: "" });
                const dateColumns = ["Criado em", "Previsão de conclusão", "Concluída em"];
                jsonData.forEach(row => {
                    dateColumns.forEach(colName => {
                        if (row[colName]) {
                            if (typeof row[colName] === 'number') {
                                row[colName] = excelSerialToDateObject(row[colName]);
                            } else {
                                row[colName] = parseDate(row[colName]);
                            }
                        }
                    });
                });
                resolve(jsonData);
            } catch (e) {
                console.error("Erro no Worker ao ler o arquivo:", e);
                reject(e);
            }
        };
        reader.onerror = (errorEvent) => reject(errorEvent.target.error);
        reader.readAsArrayBuffer(file);
    });
}

// 3. O "cérebro" do Worker: escuta por mensagens da página principal.
self.onmessage = async (event) => {
    // A página principal enviará um objeto com os dois arquivos.
    const { atividadesFile, clientesFile } = event.data;

    if (!atividadesFile || !clientesFile) {
        self.postMessage({ status: 'error', error: 'Um ou ambos os arquivos não foram recebidos pelo worker.' });
        return;
    }

    try {
        // Avisa a página principal que o trabalho pesado começou.
        self.postMessage({ status: 'processing' });

        // Lê e processa os dois arquivos ao mesmo tempo para ganhar tempo.
        const [rawActivities, rawClients] = await Promise.all([
            readFile(atividadesFile),
            readFile(clientesFile)
        ]);

        // Executa a lógica de unir os dados (que antes estava na `processInitialData`).
        const clienteMap = new Map(rawClients.map(cli => [cli.Cliente?.trim().toLowerCase(), cli]));
        
        const processedActivities = rawActivities.map(ativ => {
            const clienteKey = ativ.Cliente?.trim().toLowerCase();
            const clienteInfo = clienteMap.get(clienteKey) || {};
            return {
                ...ativ,
                ClienteCompleto: clienteKey,
                Segmento: clienteInfo.Segmento,
                CS: clienteInfo.CS,
                Squad: clienteInfo['Squad CS'],
                Fase: clienteInfo.Fase,
                ISM: clienteInfo.ISM,
                Negocio: clienteInfo['Negócio'],
                Comercial: clienteInfo['Comercial']
                // As datas já foram processadas dentro da função `readFile`
            };
        });

        // Envia uma mensagem de volta para a página principal com os dados prontos.
        self.postMessage({
            status: 'complete',
            data: {
                rawActivities: processedActivities,
                rawClients: rawClients
            }
        });

    } catch (error) {
        // Se algo der errado, envia uma mensagem de erro.
        console.error("Erro fatal no Web Worker:", error);
        self.postMessage({ status: 'error', error: error.message });
    }
};