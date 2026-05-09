// Global variables
let currentDataset = null;
let currentChart = null;
let dataSourceMode = null;       // 'dataset' | 'database'
let currentTableName = null;     // имя выбранной таблицы в режиме database
let dbTables = [];               // список таблиц БД (meta)
let dbMeta = {};                 // { dialect, dbname }
let chatList = [];               // [{id, name, updated_at, messages_count}]
let currentChatId = null;        // активный чат
const API_BASE_URL = 'http://localhost:8765';

// Метаданные источников (для demo-БД investment_department_db).
// Если таблицы здесь нет — карточки показывают «—».
const DB_METADATA = {
    clients: {
        owner: 'Команда CRM (Анна Соколова)',
        purpose: 'Профили клиентов, риск-профиль, контакты',
        frequency: 'Ежедневно (ночной ETL)',
        sensitivity: 'Высокая — PII (имя, email)'
    },
    accounts: {
        owner: 'Брокерский back-office',
        purpose: 'Счета клиентов, балансы, статусы',
        frequency: 'Каждые 15 минут',
        sensitivity: 'Высокая — финансовые данные'
    },
    instruments: {
        owner: 'Команда инструментов / Reference Data',
        purpose: 'Справочник торговых инструментов',
        frequency: 'Ежедневно после закрытия рынка',
        sensitivity: 'Низкая — публичные справочные данные'
    },
    trades: {
        owner: 'Trading Desk',
        purpose: 'Журнал всех сделок клиентов',
        frequency: 'Real-time (стриминг из биржи)',
        sensitivity: 'Средняя — операционные данные клиентов'
    },
    cash_transactions: {
        owner: 'Operations / Treasury',
        purpose: 'Денежные операции по счетам (ввод/вывод/комиссии)',
        frequency: 'Каждый час',
        sensitivity: 'Высокая — финансовые транзакции'
    }
};

// Initialize event listeners
document.addEventListener('DOMContentLoaded', () => {
    initializeEventListeners();
    checkBackendStatus();
    updateDbFormByDialect();
});

function initializeEventListeners() {
    // File input
    const fileInput = document.getElementById('fileInput');
    fileInput.addEventListener('change', handleFileUpload);

    // Tab buttons
    const tabButtons = document.querySelectorAll('.tab-btn');
    tabButtons.forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Drag and drop
    const uploadArea = document.getElementById('uploadArea');
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('drop', handleDrop);
    uploadArea.addEventListener('dragleave', handleDragLeave);

    // Тип визуализации — переключаем видимость полей под него.
    const vizTypeEl = document.getElementById('vizType');
    if (vizTypeEl) vizTypeEl.addEventListener('change', onVizTypeChange);
}

// ---------------------------------------------------------------------------
// Source selection
// ---------------------------------------------------------------------------

function chooseSource(kind) {
    document.getElementById('sourceChoice').style.display = 'none';
    if (kind === 'dataset') {
        document.getElementById('uploadArea').style.display = 'block';
        document.getElementById('dbConnectForm').style.display = 'none';
    } else {
        document.getElementById('uploadArea').style.display = 'none';
        document.getElementById('dbConnectForm').style.display = 'block';
    }
}

function backToSourceChoice() {
    document.getElementById('sourceChoice').style.display = 'block';
    document.getElementById('uploadArea').style.display = 'none';
    document.getElementById('dbConnectForm').style.display = 'none';
}

function goHome() {
    // Полный сброс клиентского состояния и возврат к выбору источника.
    currentDataset = null;
    dataSourceMode = null;
    currentTableName = null;
    dbTables = [];
    dbMeta = {};
    chatList = [];
    currentChatId = null;
    if (currentChart) {
        try { currentChart.destroy(); } catch (_) {}
        currentChart = null;
    }

    // Скрыть основной контент и показать секцию выбора.
    const mainContent = document.getElementById('mainContent');
    if (mainContent) mainContent.style.display = 'none';
    const sourceSection = document.getElementById('sourceSection');
    if (sourceSection) sourceSection.style.display = '';

    // Очистить файловый инпут и инфо о файле.
    const fileInput = document.getElementById('fileInput');
    if (fileInput) fileInput.value = '';
    const fileInfo = document.getElementById('fileInfo');
    if (fileInfo) fileInfo.style.display = 'none';

    // Очистить форму подключения к БД.
    ['dbName', 'dbHost', 'dbPort', 'dbUser', 'dbPassword'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.value = '';
    });
    const dialectEl = document.getElementById('dbDialect');
    if (dialectEl) dialectEl.value = 'postgresql';
    updateDbFormByDialect();

    // Сбросить лейблы и вернуться к выбору.
    applyModeLabels('dataset');
    backToSourceChoice();
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function updateDbFormByDialect() {
    const dialectEl = document.getElementById('dbDialect');
    if (!dialectEl) return;
    const dialect = dialectEl.value;

    const hostRow = document.getElementById('dbHostRow');
    const portRow = document.getElementById('dbPortRow');
    const userRow = document.getElementById('dbUserRow');
    const passRow = document.getElementById('dbPasswordRow');
    const nameLabel = document.getElementById('dbNameLabel');
    const nameInput = document.getElementById('dbName');
    const portInput = document.getElementById('dbPort');
    const userInput = document.getElementById('dbUser');

    if (dialect === 'sqlite') {
        hostRow.style.display = 'none';
        portRow.style.display = 'none';
        userRow.style.display = 'none';
        passRow.style.display = 'none';
        nameLabel.textContent = 'Путь к .db-файлу';
    } else {
        hostRow.style.display = '';
        portRow.style.display = '';
        userRow.style.display = '';
        passRow.style.display = '';
        nameLabel.textContent = 'Имя БД';
    }
    // Плейсхолдеры намеренно не заполняем — поля остаются полностью пустыми.
    nameInput.placeholder = '';
    portInput.placeholder = '';
    userInput.placeholder = '';
}

// File handling
function handleFileUpload(event) {
    const file = event.target.files[0];
    if (file) {
        processFile(file);
    }
}

function handleDragOver(event) {
    event.preventDefault();
    event.currentTarget.style.borderColor = 'var(--primary-color)';
    event.currentTarget.style.background = '#f0f0f0';
}

function handleDragLeave(event) {
    event.currentTarget.style.borderColor = 'var(--border-color)';
    event.currentTarget.style.background = '';
}

function handleDrop(event) {
    event.preventDefault();
    event.currentTarget.style.borderColor = 'var(--border-color)';
    event.currentTarget.style.background = '';
    
    const file = event.dataTransfer.files[0];
    if (file) {
        processFile(file);
    }
}

async function processFile(file) {
    // Show file info
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileInfo').style.display = 'flex';
    
    try {
        // Upload to backend
        const formData = new FormData();
        formData.append('file', file);
        
        showLoadingState('Загрузка файла...');
        
        const uploadResponse = await fetch(`${API_BASE_URL}/api/upload`, {
            method: 'POST',
            body: formData
        });
        
        if (!uploadResponse.ok) {
            throw new Error('Ошибка загрузки файла на сервер');
        }
        
        const uploadResult = await uploadResponse.json();
        console.log('Upload result:', uploadResult);
        
        dataSourceMode = 'dataset';
        currentTableName = null;
        dbTables = [];
        dbMeta = {};
        chatList = [];
        currentChatId = null;
        applyModeLabels('dataset');

        await loadAnalysis({ file, displaySize: null });
        await loadChatList();
        hideLoadingState();
        
    } catch (error) {
        hideLoadingState();
        alert('Ошибка обработки файла: ' + error.message);
        console.error(error);
    }
}

// ---------------------------------------------------------------------------
// DB connection flow
// ---------------------------------------------------------------------------

async function connectDatabase() {
    const dialect = document.getElementById('dbDialect').value;
    const dbname = document.getElementById('dbName').value.trim();
    if (!dbname) {
        alert('Укажи имя БД (или путь к .db-файлу для SQLite)');
        return;
    }
    const payload = { dialect, dbname };
    if (dialect !== 'sqlite') {
        const hostVal = document.getElementById('dbHost').value.trim();
        if (hostVal) payload.host = hostVal;
        const portStr = document.getElementById('dbPort').value.trim();
        payload.port = portStr ? parseInt(portStr, 10) : null;
        payload.user = document.getElementById('dbUser').value;
        payload.password = document.getElementById('dbPassword').value;
    }

    showLoadingState('Подключение к базе данных...');
    try {
        const resp = await fetch(`${API_BASE_URL}/api/connect/db`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!resp.ok) {
            const text = await resp.text();
            throw new Error(`${resp.status}: ${text}`);
        }
        const info = await resp.json();
        console.log('DB connected:', info);

        dataSourceMode = 'database';
        dbMeta = { dialect: info.dialect, dbname: info.dbname };
        currentTableName = info.current_table;

        // Подтянем список таблиц и отрендерим селекторы.
        const tablesResp = await fetch(`${API_BASE_URL}/api/db/tables`);
        if (tablesResp.ok) {
            const tablesData = await tablesResp.json();
            dbTables = tablesData.tables || [];
            currentTableName = tablesData.current_table || currentTableName;
            renderTablePickers();
        }

        applyModeLabels('database');
        chatList = [];
        currentChatId = null;

        showLoadingState('Анализ данных...');
        await loadAnalysis({ fileLike: { name: dbMeta.dbname }, displaySize: null });
        await loadChatList();
        hideLoadingState();
    } catch (err) {
        hideLoadingState();
        alert('Не удалось подключиться: ' + err.message);
        console.error(err);
    }
}

async function loadAnalysis({ file, fileLike, displaySize } = {}) {
    const tableParam = (dataSourceMode === 'database' && currentTableName)
        ? `?table=${encodeURIComponent(currentTableName)}`
        : '';
    const analysisResponse = await fetch(`${API_BASE_URL}/api/analysis/full${tableParam}`);
    if (!analysisResponse.ok) {
        throw new Error('Ошибка анализа данных');
    }
    const analysisData = await analysisResponse.json();
    currentDataset = analysisData;

    const label = file || fileLike || { name: currentTableName || 'dataset' };
    displayDatasetFromBackend(label, analysisData);
}

function renderTablePickers() {
    const makeOptions = () => dbTables.map(t =>
        `<option value="${escapeHtml(t.name)}">${escapeHtml(t.name)} (${t.rows} × ${t.columns})</option>`
    ).join('');

    const mainPicker = document.getElementById('tablePicker');
    const vizPicker = document.getElementById('vizTablePicker');
    const mainRow = document.getElementById('tablePickerRow');
    const vizGroup = document.getElementById('vizTableGroup');

    if (mainPicker) mainPicker.innerHTML = makeOptions();
    if (vizPicker) vizPicker.innerHTML = makeOptions();

    if (currentTableName) {
        if (mainPicker) mainPicker.value = currentTableName;
        if (vizPicker) vizPicker.value = currentTableName;
    }

    if (dataSourceMode === 'database') {
        if (mainRow) mainRow.style.display = 'flex';
        if (vizGroup) vizGroup.style.display = 'flex';
    } else {
        if (mainRow) mainRow.style.display = 'none';
        if (vizGroup) vizGroup.style.display = 'none';
    }
}

async function onTableChange(newTable) {
    if (!newTable || newTable === currentTableName) return;
    currentTableName = newTable;
    // Синхронизируем оба пикера.
    const mainPicker = document.getElementById('tablePicker');
    const vizPicker = document.getElementById('vizTablePicker');
    if (mainPicker && mainPicker.value !== newTable) mainPicker.value = newTable;
    if (vizPicker && vizPicker.value !== newTable) vizPicker.value = newTable;
    renderDbMetadata(newTable);

    showLoadingState(`Анализ таблицы «${newTable}»...`);
    try {
        await loadAnalysis({ fileLike: { name: newTable } });
    } catch (err) {
        alert('Ошибка анализа таблицы: ' + err.message);
        console.error(err);
    } finally {
        hideLoadingState();
    }
}

// ---------------------------------------------------------------------------
// Mode labels
// ---------------------------------------------------------------------------

function applyModeLabels(mode) {
    const isDb = mode === 'database';
    const infoHeading = document.getElementById('infoHeading');
    if (infoHeading) infoHeading.textContent = isDb ? 'Информация о базе данных' : 'Информация о датасете';

    const greetEl = document.getElementById('chatGreetingText');
    if (greetEl) {
        greetEl.textContent = isDb
            ? 'Приветик! Я Настя ✿ Подключи БД и спрашивай про таблицы и связи — я сама схожу в SQL/pandas и всё расскажу ☺️'
            : 'Приветик! Я Настя ✿ Загружай датасет и спрашивай что угодно — я посмотрю сама через SQL и pandas и всё тебе расскажу ☺️';
    }

    const dbMetaGrid = document.getElementById('dbMetaGrid');
    if (dbMetaGrid) dbMetaGrid.style.display = isDb ? 'grid' : 'none';
}

function renderDbMetadata(tableName) {
    const grid = document.getElementById('dbMetaGrid');
    if (!grid) return;
    if (dataSourceMode !== 'database') {
        grid.style.display = 'none';
        return;
    }
    grid.style.display = 'grid';
    const meta = (tableName && DB_METADATA[tableName]) || {};
    const set = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = (val && String(val).trim()) || '—';
    };
    set('dbOwner', meta.owner);
    set('dbPurpose', meta.purpose);
    set('dbFrequency', meta.frequency);
    set('dbSensitivity', meta.sensitivity);
}

function parseCSV(text) {
    const lines = text.split('\n').filter(line => line.trim());
    if (lines.length === 0) return [];
    
    const headers = lines[0].split(',').map(h => h.trim());
    const data = [];
    
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map(v => v.trim());
        const row = {};
        headers.forEach((header, index) => {
            row[header] = values[index] || '';
        });
        data.push(row);
    }
    
    return data;
}

function removeFile() {
    currentDataset = null;
    dataSourceMode = null;
    currentTableName = null;
    dbTables = [];
    dbMeta = {};
    chatList = [];
    currentChatId = null;
    document.getElementById('fileInput').value = '';
    document.getElementById('fileInfo').style.display = 'none';
    document.getElementById('mainContent').style.display = 'none';
    const sourceSection = document.getElementById('sourceSection');
    if (sourceSection) sourceSection.style.display = '';
    backToSourceChoice();
}

function displayDatasetFromBackend(file, analysisData) {
    // Show main content, hide source section.
    document.getElementById('mainContent').style.display = 'block';
    const sourceSection = document.getElementById('sourceSection');
    if (sourceSection) sourceSection.style.display = 'none';
    
    // Update dataset info
    const info = analysisData.dataset_info;
    document.getElementById('rowCount').textContent = info.rows.toLocaleString();
    document.getElementById('colCount').textContent = info.columns;
    document.getElementById('fileSize').textContent = info.memory_usage_mb + ' MB';

    let formatLabel;
    if (dataSourceMode === 'database') {
        formatLabel = (dbMeta.dialect || 'DB').toUpperCase();
    } else if (file && file.name && file.name.includes('.')) {
        formatLabel = file.name.split('.').pop().toUpperCase();
    } else {
        formatLabel = 'DATASET';
    }
    document.getElementById('fileFormat').textContent = formatLabel;
    
    // Display quality metrics
    displayQualityMetrics(analysisData);
    
    // Populate column select
    const columns = analysisData.columns.map(col => col.name);
    populateColumnSelect(columns);

    // Убедимся, что таблица-пикеры актуальны (в DB-режиме).
    if (dataSourceMode === 'database') {
        renderTablePickers();
        renderDbMetadata(currentTableName);
    } else {
        renderDbMetadata(null);
    }
    
    // Scroll to content
    document.getElementById('mainContent').scrollIntoView({ behavior: 'smooth' });
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
}

// Data Quality Analysis
function displayQualityMetrics(analysisData) {
    const quality = analysisData.quality_metrics;
    const info = analysisData.dataset_info;
    const duplicates = analysisData.duplicates;
    
    // Update main metrics
    document.getElementById('completeness').querySelector('.value-large').textContent = quality.completeness_percentage.toFixed(1);
    document.getElementById('completenessDetail').textContent = `${(info.total_cells - quality.missing_cells).toLocaleString()} из ${info.total_cells.toLocaleString()} заполнены`;
    
    document.getElementById('missing').querySelector('.value-large').textContent = quality.missing_cells.toLocaleString();
    document.getElementById('missingDetail').textContent = `${quality.missing_percentage.toFixed(1)}% всех данных`;
    
    document.getElementById('duplicates').querySelector('.value-large').textContent = duplicates.total_duplicate_rows;
    document.getElementById('duplicatesDetail').textContent = duplicates.percentage > 0 ? `${duplicates.percentage}% строк` : 'Отлично!';
    
    const dtypes = Object.keys(analysisData.data_types);
    document.getElementById('datatypes').querySelector('.value-large').textContent = dtypes.length;
    document.getElementById('datatypesDetail').textContent = dtypes.join(', ');
    
    // Populate quality table with detailed info
    const tableBody = document.querySelector('#qualityTable tbody');
    tableBody.innerHTML = '';
    
    analysisData.columns.forEach(col => {
        const row = document.createElement('tr');
        
        let status, statusClass;
        const score = col.quality_score;
        if (score >= 90) {
            status = 'Отлично';
            statusClass = 'status-good';
        } else if (score >= 70) {
            status = 'Приемлемо';
            statusClass = 'status-warning';
        } else {
            status = 'Требует внимания';
            statusClass = 'status-error';
        }
        
        // Build additional info
        let extraInfo = '';
        if (col.outliers && col.outliers.count > 0) {
            extraInfo += `<br><small>⚠️ Выбросов: ${col.outliers.count} (${col.outliers.percentage}%)</small>`;
        }
        if (col.entropy) {
            extraInfo += `<br><small>📊 Энтропия: ${col.entropy}</small>`;
        }
        
        row.innerHTML = `
            <td>
                <strong>${col.name}</strong>
                ${extraInfo}
            </td>
            <td>
                <span class="type-badge">${col.type}</span>
                ${col.statistics ? `<br><small>μ=${col.statistics.mean.toFixed(2)}, σ=${col.statistics.std.toFixed(2)}</small>` : ''}
            </td>
            <td>${col.missing_count} (${col.missing_percentage}%)</td>
            <td>
                ${col.unique_count} 
                <br><small>(${(col.unique_ratio * 100).toFixed(1)}%)</small>
            </td>
            <td>
                <span class="status-badge ${statusClass}">${status}</span>
                <br><small>Оценка: ${score}/100</small>
            </td>
        `;
        
        tableBody.appendChild(row);
    });
    
    // Show warnings if any
    displayWarnings(analysisData.warnings);
    
    // Show correlation info
    displayCorrelations(analysisData.correlation);
    
    // Show missing patterns
    displayMissingPatterns(analysisData.missing_patterns);
}

// Tab switching
function switchTab(tabName) {
    // Update buttons
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');
    
    // Update content
    document.querySelectorAll('.tab-pane').forEach(pane => {
        pane.classList.remove('active');
    });
    document.getElementById(tabName).classList.add('active');

    if (tabName === 'chat') {
        loadChatList().catch(err => console.error('chats load failed:', err));
    }
}

// ---------------------------------------------------------------------------
// Multi-chat sidebar
// ---------------------------------------------------------------------------

async function loadChatList() {
    if (!currentDataset) return;
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats`);
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(`${resp.status}: ${t}`);
        }
        const data = await resp.json();
        chatList = data.chats || [];
        renderChatList();
    } catch (err) {
        console.error('Не удалось получить список чатов:', err);
    }
}

function renderChatList() {
    const container = document.getElementById('chatList');
    if (!container) return;
    if (!chatList.length) {
        container.innerHTML = '<div class="chat-list-empty">Нажмите «+ Новый чат», чтобы начать.</div>';
        updateCurrentChatName();
        return;
    }
    container.innerHTML = '';
    chatList.forEach(chat => {
        const item = document.createElement('div');
        item.className = 'chat-list-item' + (chat.id === currentChatId ? ' active' : '');
        item.title = chat.name;
        item.innerHTML = `
            <span class="chat-list-item-name">${escapeHtml(chat.name)}</span>
            <span class="chat-list-item-actions">
                <button title="Переименовать" data-action="rename"><i class="fas fa-pen"></i></button>
                <button title="Удалить" class="danger" data-action="delete"><i class="fas fa-trash"></i></button>
            </span>
        `;
        item.addEventListener('click', (ev) => {
            const btn = ev.target.closest('button[data-action]');
            if (btn) {
                ev.stopPropagation();
                if (btn.dataset.action === 'rename') return renameChat(chat.id);
                if (btn.dataset.action === 'delete') return deleteChat(chat.id);
                return;
            }
            selectChat(chat.id);
        });
        container.appendChild(item);
    });
    updateCurrentChatName();
}

function updateCurrentChatName() {
    const el = document.getElementById('chatCurrentName');
    if (!el) return;
    if (!currentChatId) {
        el.textContent = '';
        return;
    }
    const chat = chatList.find(c => c.id === currentChatId);
    el.textContent = chat ? `Чат: ${chat.name}` : '';
}

async function createNewChat() {
    if (!currentDataset) {
        alert(dataSourceMode === 'database' ? 'Сначала подключите базу данных' : 'Сначала загрузите датасет');
        return;
    }
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        });
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(`${resp.status}: ${t}`);
        }
        const data = await resp.json();
        currentChatId = data.chat.id;
        await loadChatList();
        clearChatMessages();
    } catch (err) {
        alert('Не удалось создать чат: ' + err.message);
    }
}

async function selectChat(chatId) {
    if (!chatId || chatId === currentChatId) return;
    currentChatId = chatId;
    renderChatList();
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(chatId)}`);
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(`${resp.status}: ${t}`);
        }
        const data = await resp.json();
        renderChatHistory(data.chat?.messages || []);
    } catch (err) {
        alert('Не удалось загрузить чат: ' + err.message);
    }
}

async function renameChat(chatId) {
    const chat = chatList.find(c => c.id === chatId);
    const current = chat?.name || '';
    const next = prompt('Новое название чата:', current);
    if (next === null) return;
    const trimmed = next.trim();
    if (!trimmed || trimmed === current) return;
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(chatId)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: trimmed })
        });
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(`${resp.status}: ${t}`);
        }
        await loadChatList();
    } catch (err) {
        alert('Не удалось переименовать: ' + err.message);
    }
}

async function deleteChat(chatId) {
    if (!confirm('Удалить этот чат? Историю восстановить не получится.')) return;
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(chatId)}`, {
            method: 'DELETE'
        });
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(`${resp.status}: ${t}`);
        }
        if (currentChatId === chatId) {
            currentChatId = null;
            clearChatMessages();
        }
        await loadChatList();
    } catch (err) {
        alert('Не удалось удалить: ' + err.message);
    }
}

function clearChatMessages() {
    const messages = document.getElementById('chatMessages');
    if (!messages) return;
    messages.innerHTML = '';
    const greeting = document.createElement('div');
    greeting.className = 'chat-message assistant';
    const text = dataSourceMode === 'database'
        ? 'Приветик! Я Настя ✿ Это новый чат — спрашивай что угодно про подключённую БД ☺️'
        : 'Приветик! Я Настя ✿ Это новый чат — спрашивай что угодно про загруженный датасет ☺️';
    greeting.innerHTML = `
        <div class="message-avatar"><i class="fas fa-heart" style="color:#ec4899;"></i></div>
        <div class="message-content"><p>${escapeHtml(text)}</p></div>
    `;
    messages.appendChild(greeting);
}

function renderChatHistory(messages) {
    const container = document.getElementById('chatMessages');
    if (!container) return;
    container.innerHTML = '';
    if (!messages.length) {
        clearChatMessages();
        return;
    }
    messages.forEach(m => {
        addChatMessage(m.content || '', m.role === 'user' ? 'user' : 'assistant');
    });
}

// Visualization
function populateColumnSelect(columns) {
    // columns теперь — массив имён, но колонки с типами берём из currentDataset.columns.
    const select = document.getElementById('columnSelect');
    select.innerHTML = '<option value="">Выберите столбец...</option>';

    columns.forEach(col => {
        const option = document.createElement('option');
        option.value = col;
        option.textContent = col;
        select.appendChild(option);
    });

    // Y-селектор перезаполняется при смене типа визуализации (зависит от того, что нужно).
    onVizTypeChange();
}

function _columnsByPredicate(predicate) {
    if (!currentDataset || !Array.isArray(currentDataset.columns)) return [];
    return currentDataset.columns.filter(predicate).map(c => c.name);
}

function _isDateColumn(col) {
    if (!col) return false;
    if (col.type === 'datetime') return true;
    // Эвристика: имя колонки выглядит как дата.
    const name = (col.name || '').toLowerCase();
    return /(^|_)(date|dt|day|month|year|created|updated|opened|closed|at)($|_)/.test(name);
}

function _isNumericColumn(col) {
    return col && (col.type === 'numeric' || col.type === 'binary');
}

function onVizTypeChange() {
    const vizType = document.getElementById('vizType').value;
    const colGroup = document.getElementById('columnSelectGroup');
    const colLabel = document.getElementById('columnSelectLabel');
    const colSelect = document.getElementById('columnSelect');
    const yGroup = document.getElementById('yColumnGroup');
    const ySelect = document.getElementById('yColumnSelect');

    if (vizType === 'missing') {
        if (colGroup) colGroup.style.display = 'none';
        if (yGroup) yGroup.style.display = 'none';
        return;
    }

    if (vizType === 'dynamics') {
        if (colGroup) colGroup.style.display = '';
        if (yGroup) yGroup.style.display = '';
        if (colLabel) colLabel.textContent = 'Столбец X (дата):';

        const dateCols = _columnsByPredicate(_isDateColumn);
        const numericCols = _columnsByPredicate(_isNumericColumn);

        const fillSelect = (sel, items, placeholder) => {
            if (!sel) return;
            sel.innerHTML = `<option value="">${placeholder}</option>`;
            items.forEach(name => {
                const opt = document.createElement('option');
                opt.value = name;
                opt.textContent = name;
                sel.appendChild(opt);
            });
        };

        fillSelect(colSelect, dateCols, dateCols.length ? 'Выберите столбец-дату...' : 'Нет столбцов-дат');
        fillSelect(ySelect, numericCols, numericCols.length ? 'Выберите числовой столбец...' : 'Нет числовых столбцов');
        return;
    }

    // distribution / outliers — показываем только основной селектор со всеми столбцами.
    if (colGroup) colGroup.style.display = '';
    if (yGroup) yGroup.style.display = 'none';
    if (colLabel) colLabel.textContent = 'Столбец:';

    if (currentDataset && Array.isArray(currentDataset.columns)) {
        const allCols = currentDataset.columns.map(c => c.name);
        if (colSelect) {
            colSelect.innerHTML = '<option value="">Выберите столбец...</option>';
            allCols.forEach(name => {
                const opt = document.createElement('option');
                opt.value = name;
                opt.textContent = name;
                colSelect.appendChild(opt);
            });
        }
    }
}

async function generateVisualization() {
    if (!currentDataset) {
        alert(dataSourceMode === 'database' ? 'Сначала подключите базу данных' : 'Сначала загрузите датасет');
        return;
    }
    
    const vizType = document.getElementById('vizType').value;
    const column = document.getElementById('columnSelect').value;
    
    if (vizType === 'missing') {
        await createMissingChart();
        return;
    }

    if (vizType === 'dynamics') {
        const yColumn = document.getElementById('yColumnSelect').value;
        if (!column) {
            alert('Выберите столбец-дату для оси X');
            return;
        }
        if (!yColumn) {
            alert('Выберите числовой столбец для оси Y');
            return;
        }
        document.getElementById('vizPlaceholder').style.display = 'none';
        try {
            await createDynamicsChart(column, yColumn);
        } catch (error) {
            alert('Ошибка построения динамики: ' + error.message);
            console.error(error);
        }
        return;
    }

    if (!column) {
        alert('Выберите столбец для визуализации');
        return;
    }
    
    document.getElementById('vizPlaceholder').style.display = 'none';
    
    try {
        if (vizType === 'distribution') {
            await createDistributionChartFromAPI(column);
        } else if (vizType === 'outliers') {
            await createOutliersChart(column);
        }
    } catch (error) {
        alert('Ошибка создания визуализации: ' + error.message);
        console.error(error);
    }
}

async function createDynamicsChart(xCol, yCol) {
    const base = `${API_BASE_URL}/api/visualization/dynamics?x=${encodeURIComponent(xCol)}&y=${encodeURIComponent(yCol)}`;
    const url = buildTableQuery(base);
    const response = await fetch(url);
    if (!response.ok) {
        const t = await response.text();
        throw new Error(`Ошибка ${response.status}: ${t}`);
    }
    const json = await response.json();
    const data = json.data || {};

    if (currentChart) {
        currentChart.destroy();
    }

    if (!Array.isArray(data.labels) || data.labels.length === 0) {
        document.getElementById('vizPlaceholder').style.display = 'flex';
        document.getElementById('vizPlaceholder').querySelector('p').textContent =
            'Нет данных для построения динамики (проверь, что в столбце X есть валидные даты).';
        return;
    }

    const granularityLabel = data.freq === 'M' ? 'по месяцам' : data.freq === 'W' ? 'по неделям' : 'по дням';

    currentChart = new Chart(document.getElementById('chartCanvas'), {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: [{
                label: `Сумма ${yCol} ${granularityLabel}`,
                data: data.values,
                borderColor: 'rgba(79, 70, 229, 1)',
                backgroundColor: 'rgba(79, 70, 229, 0.15)',
                borderWidth: 2,
                fill: true,
                tension: 0.25,
                pointRadius: 2,
                pointHoverRadius: 5,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: true },
                tooltip: { mode: 'index', intersect: false }
            },
            scales: {
                x: { title: { display: true, text: xCol } },
                y: { beginAtZero: true, title: { display: true, text: `Сумма ${yCol}` } }
            }
        }
    });

    document.getElementById('vizPlaceholder').style.display = 'none';
}

function buildTableQuery(initial = '') {
    if (dataSourceMode !== 'database' || !currentTableName) return initial;
    const sep = initial.includes('?') ? '&' : '?';
    return `${initial}${sep}table=${encodeURIComponent(currentTableName)}`;
}

async function createDistributionChartFromAPI(column) {
    try {
        const url = buildTableQuery(`${API_BASE_URL}/api/visualization/distribution/${encodeURIComponent(column)}`);
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error('Ошибка получения данных распределения');
        }
        
        const data = await response.json();
        
        // Destroy previous chart
        if (currentChart) {
            currentChart.destroy();
        }
        
        if (data.type === 'numeric') {
            const labels = [];
            const binEdges = data.data.bin_edges;
            
            for (let i = 0; i < data.data.counts.length; i++) {
                labels.push(`${binEdges[i].toFixed(2)} - ${binEdges[i + 1].toFixed(2)}`);
            }
            
            currentChart = new Chart(document.getElementById('chartCanvas'), {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: `Распределение: ${column}`,
                        data: data.data.counts,
                        backgroundColor: 'rgba(79, 70, 229, 0.6)',
                        borderColor: 'rgba(79, 70, 229, 1)',
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: {
                            display: true
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Количество'
                            }
                        }
                    }
                }
            });
        } else {
            currentChart = new Chart(document.getElementById('chartCanvas'), {
                type: 'bar',
                data: {
                    labels: data.data.labels,
                    datasets: [{
                        label: `Частота: ${column}`,
                        data: data.data.counts,
                        backgroundColor: 'rgba(6, 182, 212, 0.6)',
                        borderColor: 'rgba(6, 182, 212, 1)',
                        borderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: {
                            display: true
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            title: {
                                display: true,
                                text: 'Количество'
                            }
                        }
                    }
                }
            });
        }
        
        document.getElementById('vizPlaceholder').style.display = 'none';
    } catch (error) {
        throw error;
    }
}

async function createMissingChart() {
    try {
        const response = await fetch(buildTableQuery(`${API_BASE_URL}/api/visualization/missing`));
        
        if (!response.ok) {
            throw new Error('Ошибка получения данных о пропусках');
        }
        
        const result = await response.json();
        const data = result.data;
        
        // Destroy previous chart
        if (currentChart) {
            currentChart.destroy();
        }
        
        currentChart = new Chart(document.getElementById('chartCanvas'), {
            type: 'bar',
            data: {
                labels: data.map(d => d.column),
                datasets: [{
                    label: 'Пропущенные значения',
                    data: data.map(d => d.count),
                    backgroundColor: 'rgba(239, 68, 68, 0.6)',
                    borderColor: 'rgba(239, 68, 68, 1)',
                    borderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: true
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const item = data[context.dataIndex];
                                return `${item.count} пропусков (${item.percentage}%)`;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: 'Количество пропусков'
                        }
                    }
                }
            }
        });
        
        document.getElementById('vizPlaceholder').style.display = 'none';
    } catch (error) {
        throw error;
    }
}

async function createOutliersChart(column) {
    if (!currentDataset || !currentDataset.columns) {
        alert('Данные не загружены');
        return;
    }
    
    const colData = currentDataset.columns.find(col => col.name === column);
    
    if (!colData || !colData.outliers) {
        alert('Для этого столбца нет данных о выбросах');
        return;
    }
    
    const outliers = colData.outliers;
    
    // Destroy previous chart
    if (currentChart) {
        currentChart.destroy();
    }
    
    const ctx = document.getElementById('chartCanvas');
    
    // Simple bar chart showing outlier information
    currentChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: ['Нормальные значения', 'Выбросы'],
            datasets: [{
                label: `Анализ выбросов: ${column}`,
                data: [currentDataset.dataset_info.rows - outliers.count, outliers.count],
                backgroundColor: [
                    'rgba(16, 185, 129, 0.6)',
                    'rgba(239, 68, 68, 0.6)'
                ],
                borderColor: [
                    'rgba(16, 185, 129, 1)',
                    'rgba(239, 68, 68, 1)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true
                },
                tooltip: {
                    callbacks: {
                        afterLabel: function(context) {
                            if (context.dataIndex === 1) {
                                return [
                                    `Процент: ${outliers.percentage}%`,
                                    `Метод: ${outliers.method}`,
                                    `Нижняя граница: ${outliers.lower_bound.toFixed(2)}`,
                                    `Верхняя граница: ${outliers.upper_bound.toFixed(2)}`
                                ];
                            }
                            return '';
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Количество значений'
                    }
                }
            }
        }
    });
    
    document.getElementById('vizPlaceholder').style.display = 'none';
}

function quickViz(type) {
    if (!currentDataset) {
        alert(dataSourceMode === 'database' ? 'Сначала подключите базу данных' : 'Сначала загрузите датасет');
        return;
    }
    
    switchTab('viz');
    
    if (type === 'missing') {
        document.getElementById('vizType').value = 'missing';
        setTimeout(() => createMissingChart(), 300);
    } else {
        alert('Эта быстрая визуализация будет доступна в следующей версии');
    }
}

// Helper functions for displaying additional info
function displayWarnings(warnings) {
    const container = document.querySelector('.quality-table');
    
    // Remove old warnings if any
    const oldWarnings = document.getElementById('warningsSection');
    if (oldWarnings) {
        oldWarnings.remove();
    }
    
    if (warnings.constant_columns.length > 0 || warnings.high_cardinality_columns.length > 0) {
        const warningsDiv = document.createElement('div');
        warningsDiv.id = 'warningsSection';
        warningsDiv.style.marginTop = '20px';
        warningsDiv.style.padding = '20px';
        warningsDiv.style.background = '#fef3c7';
        warningsDiv.style.borderRadius = '8px';
        warningsDiv.style.border = '2px solid #f59e0b';
        
        let html = '<h4 style="margin-bottom: 10px;"><i class="fas fa-exclamation-triangle"></i> Предупреждения</h4>';
        
        if (warnings.constant_columns.length > 0) {
            html += `<p><strong>Константные столбцы:</strong> ${warnings.constant_columns.join(', ')}</p>`;
        }
        
        if (warnings.high_cardinality_columns.length > 0) {
            html += `<p><strong>Высокая кардинальность:</strong> ${warnings.high_cardinality_columns.join(', ')}</p>`;
        }
        
        warningsDiv.innerHTML = html;
        container.appendChild(warningsDiv);
    }
}

function displayCorrelations(correlation) {
    const container = document.querySelector('.quality-table');
    
    // Remove old correlations if any
    const oldCorr = document.getElementById('correlationsSection');
    if (oldCorr) {
        oldCorr.remove();
    }
    
    if (correlation.available && correlation.strong_correlations.length > 0) {
        const corrDiv = document.createElement('div');
        corrDiv.id = 'correlationsSection';
        corrDiv.style.marginTop = '20px';
        corrDiv.style.padding = '20px';
        corrDiv.style.background = '#dbeafe';
        corrDiv.style.borderRadius = '8px';
        corrDiv.style.border = '2px solid #3b82f6';
        
        let html = '<h4 style="margin-bottom: 10px;"><i class="fas fa-link"></i> Сильные корреляции</h4>';
        html += '<ul style="margin-left: 20px;">';
        
        correlation.strong_correlations.forEach(corr => {
            const color = Math.abs(corr.correlation) > 0.9 ? '#dc2626' : '#f97316';
            html += `<li><strong>${corr.col1}</strong> ↔ <strong>${corr.col2}</strong>: 
                     <span style="color: ${color}; font-weight: bold;">${corr.correlation.toFixed(3)}</span>
                     (${corr.strength})</li>`;
        });
        
        html += '</ul>';
        corrDiv.innerHTML = html;
        container.appendChild(corrDiv);
    }
}

function displayMissingPatterns(patterns) {
    const container = document.querySelector('.quality-table');
    
    // Remove old patterns if any
    const oldPatterns = document.getElementById('patternsSection');
    if (oldPatterns) {
        oldPatterns.remove();
    }
    
    if (patterns.completely_empty_rows > 0 || patterns.missing_correlation_pairs.length > 0) {
        const patternsDiv = document.createElement('div');
        patternsDiv.id = 'patternsSection';
        patternsDiv.style.marginTop = '20px';
        patternsDiv.style.padding = '20px';
        patternsDiv.style.background = '#fee2e2';
        patternsDiv.style.borderRadius = '8px';
        patternsDiv.style.border = '2px solid #ef4444';
        
        let html = '<h4 style="margin-bottom: 10px;"><i class="fas fa-search"></i> Паттерны пропусков</h4>';
        
        if (patterns.completely_empty_rows > 0) {
            html += `<p><strong>Полностью пустые строки:</strong> ${patterns.completely_empty_rows}</p>`;
        }
        
        html += `<p><strong>Строки с пропусками:</strong> ${patterns.rows_with_missing}</p>`;
        
        if (patterns.missing_correlation_pairs.length > 0) {
            html += '<p><strong>Коррелированные пропуски:</strong></p><ul style="margin-left: 20px;">';
            patterns.missing_correlation_pairs.forEach(pair => {
                html += `<li>${pair.col1} ↔ ${pair.col2}: ${pair.correlation.toFixed(3)}</li>`;
            });
            html += '</ul>';
        }
        
        patternsDiv.innerHTML = html;
        container.appendChild(patternsDiv);
    }
}

// Backend status check
async function checkBackendStatus() {
    try {
        const response = await fetch(`${API_BASE_URL}/api/health`);
        const data = await response.json();
        
        if (data.status === 'healthy') {
            const statusIndicator = document.getElementById('aiStatus');
            statusIndicator.innerHTML = '<i class="fas fa-circle"></i> Backend подключен';
            statusIndicator.classList.add('connected');
            statusIndicator.style.color = 'var(--success-color)';
        }
    } catch (error) {
        console.log('Backend not available:', error);
    }
}

// Loading states
function showLoadingState(message) {
    const mainContent = document.getElementById('mainContent');
    
    let loader = document.getElementById('loadingOverlay');
    if (!loader) {
        loader = document.createElement('div');
        loader.id = 'loadingOverlay';
        loader.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.7);
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            z-index: 9999;
            color: white;
        `;
        document.body.appendChild(loader);
    }
    
    loader.innerHTML = `
        <div class="loading"></div>
        <p style="margin-top: 20px; font-size: 1.2rem;">${message}</p>
    `;
    loader.style.display = 'flex';
}

function hideLoadingState() {
    const loader = document.getElementById('loadingOverlay');
    if (loader) {
        loader.style.display = 'none';
    }
}

// Chat functionality
async function sendMessage() {
    const input = document.getElementById('chatInput');
    const sendBtn = document.querySelector('.btn-send');
    const message = input.value.trim();

    if (!message) return;

    if (!currentDataset) {
        alert(dataSourceMode === 'database' ? 'Сначала подключите базу данных' : 'Сначала загрузите датасет');
        return;
    }

    // Если у пользователя ещё нет активного чата — создаём первый автоматически.
    if (!currentChatId) {
        try {
            const created = await fetch(`${API_BASE_URL}/api/chats`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({})
            });
            if (!created.ok) {
                const t = await created.text();
                throw new Error(`${created.status}: ${t}`);
            }
            const data = await created.json();
            currentChatId = data.chat.id;
            await loadChatList();
        } catch (err) {
            alert('Не удалось создать чат: ' + err.message);
            return;
        }
    }

    // Если в окне ещё стандартное приветствие — почистим, чтобы было видно только сообщения чата.
    const greeting = document.getElementById('chatGreeting');
    if (greeting && greeting.parentElement) greeting.parentElement.removeChild(greeting);

    addChatMessage(message, 'user');
    input.value = '';
    input.disabled = true;
    if (sendBtn) sendBtn.disabled = true;

    const thinkingEl = addChatMessage('Настя думает...', 'assistant', { ephemeral: true });

    try {
        const response = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(currentChatId)}/message`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message })
        });

        if (!response.ok) {
            const errText = await response.text();
            throw new Error(`Ошибка ${response.status}: ${errText}`);
        }

        const data = await response.json();
        if (thinkingEl && thinkingEl.remove) thinkingEl.remove();

        if (Array.isArray(data.trace) && data.trace.length > 0) {
            renderAgentTrace(data.trace);
        }
        addChatMessage(data.reply || '(пустой ответ)', 'assistant');

        // Подтягиваем обновлённый список (имя чата мог автопереименоваться).
        loadChatList().catch(() => {});
    } catch (err) {
        if (thinkingEl && thinkingEl.remove) thinkingEl.remove();
        addChatMessage('Упс, что-то пошло не так: ' + err.message, 'assistant');
        console.error(err);
    } finally {
        input.disabled = false;
        if (sendBtn) sendBtn.disabled = false;
        input.focus();
    }
}

function renderAgentTrace(trace) {
    const messagesContainer = document.getElementById('chatMessages');
    const wrapper = document.createElement('div');
    wrapper.className = 'chat-message assistant';
    wrapper.innerHTML = `
        <div class="message-avatar"><i class="fas fa-cogs"></i></div>
        <div class="message-content">
            <details>
                <summary style="cursor:pointer; color:#64748b;">
                    Настя запустила ${trace.length} инструмент(а/ов) — показать
                </summary>
                <div class="trace-body"></div>
            </details>
        </div>
    `;
    const body = wrapper.querySelector('.trace-body');
    trace.forEach((step, i) => {
        const stepEl = document.createElement('div');
        stepEl.style.marginTop = '10px';
        const argStr = escapeHtml(
            step.name === 'run_sql'
                ? (step.arguments && step.arguments.query) || ''
                : (step.arguments && step.arguments.code) || ''
        );
        const resStr = escapeHtml(formatTraceResult(step.result));
        stepEl.innerHTML = `
            <div><strong>#${i + 1} ${step.name}</strong></div>
            <pre style="background:#0f172a; color:#e2e8f0; padding:8px; border-radius:6px; overflow:auto; font-size:12px;">${argStr}</pre>
            <pre style="background:#f1f5f9; padding:8px; border-radius:6px; overflow:auto; font-size:12px;">${resStr}</pre>
        `;
        body.appendChild(stepEl);
    });
    messagesContainer.appendChild(wrapper);
    messagesContainer.scrollTop = messagesContainer.scrollHeight;
}

function formatTraceResult(res) {
    if (!res) return '';
    if (!res.ok) return 'Ошибка: ' + (res.error || 'unknown');
    if (res.preview) {
        const meta = res.shape
            ? `shape=${res.shape[0]}x${res.shape[1]}`
            : (res.rows !== undefined ? `rows=${res.rows}` : '');
        return (meta ? meta + '\n' : '') + res.preview;
    }
    if (res.value !== undefined) return JSON.stringify(res.value, null, 2);
    return JSON.stringify(res, null, 2);
}

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
}

function handleChatKeypress(event) {
    if (event.key === 'Enter') {
        sendMessage();
    }
}

function sendSuggestion(button) {
    const message = button.textContent.trim();
    document.getElementById('chatInput').value = message;
    sendMessage();
}

function addChatMessage(message, sender, options = {}) {
    const messagesContainer = document.getElementById('chatMessages');
    const messageDiv = document.createElement('div');
    messageDiv.className = `chat-message ${sender}`;
    if (options.ephemeral) messageDiv.dataset.ephemeral = '1';

    const avatar = sender === 'assistant'
        ? '<i class="fas fa-heart" style="color:#ec4899;"></i>'
        : '<i class="fas fa-user"></i>';

    const renderMd = sender === 'assistant' && !options.ephemeral;
    const bodyHtml = renderMd
        ? renderMarkdown(message)
        : `<p>${escapeHtml(message).replace(/\n/g, '<br>')}</p>`;

    messageDiv.innerHTML = `
        <div class="message-avatar">
            ${avatar}
        </div>
        <div class="message-content markdown-body">
            ${bodyHtml}
        </div>
    `;

    messagesContainer.appendChild(messageDiv);
    messagesContainer.scrollTop = messagesContainer.scrollHeight;
    return messageDiv;
}

function renderMarkdown(text) {
    if (typeof marked === 'undefined') {
        return `<p>${escapeHtml(text).replace(/\n/g, '<br>')}</p>`;
    }
    try {
        marked.setOptions({
            gfm: true,
            breaks: true,
            headerIds: false,
            mangle: false,
        });
        const raw = marked.parse(text || '');
        if (typeof DOMPurify !== 'undefined') {
            return DOMPurify.sanitize(raw);
        }
        return raw;
    } catch (e) {
        console.error('markdown render failed', e);
        return `<p>${escapeHtml(text).replace(/\n/g, '<br>')}</p>`;
    }
}

// Legacy fallback (not used now that backend /api/chat is wired to Nastya).
function getSimulatedResponse(message) {
    const lowerMessage = message.toLowerCase();
    
    if (!currentDataset || !currentDataset.columns) {
        return 'Датасет не загружен. Пожалуйста, загрузите файл для анализа.';
    }
    
    if (lowerMessage.includes('пропуск') || lowerMessage.includes('missing')) {
        const missingCols = currentDataset.columns.filter(col => col.missing_count > 0);
        
        if (missingCols.length === 0) {
            return 'Отлично! В датасете нет пропущенных значений. 🎉';
        }
        
        const topMissing = missingCols
            .sort((a, b) => b.missing_count - a.missing_count)
            .slice(0, 5);
        
        let response = 'Пропущенные значения найдены в следующих столбцах:\n\n';
        topMissing.forEach(col => {
            response += `• ${col.name}: ${col.missing_count} (${col.missing_percentage}%)\n`;
        });
        
        return response;
    }
    
    if (lowerMessage.includes('статистик') || lowerMessage.includes('числов')) {
        const numericCols = currentDataset.columns.filter(col => col.statistics);
        
        if (numericCols.length === 0) {
            return 'В датасете нет числовых столбцов для статистического анализа.';
        }
        
        let response = `Статистика по числовым столбцам (всего ${numericCols.length}):\n\n`;
        numericCols.slice(0, 3).forEach(col => {
            const stats = col.statistics;
            response += `📊 ${col.name}:\n`;
            response += `   Среднее: ${stats.mean}\n`;
            response += `   Медиана: ${stats.median}\n`;
            response += `   Станд. откл.: ${stats.std}\n`;
            response += `   Мин: ${stats.min}, Макс: ${stats.max}\n\n`;
        });
        
        return response;
    }
    
    if (lowerMessage.includes('корреляци')) {
        if (currentDataset.correlation && currentDataset.correlation.strong_correlations) {
            const corrs = currentDataset.correlation.strong_correlations;
            
            if (corrs.length === 0) {
                return 'Сильных корреляций между столбцами не обнаружено.';
            }
            
            let response = `Найдены сильные корреляции:\n\n`;
            corrs.forEach(corr => {
                response += `• ${corr.col1} ↔ ${corr.col2}: ${corr.correlation} (${corr.strength})\n`;
            });
            
            return response;
        }
        return 'Данные корреляции недоступны. Убедитесь, что в датасете есть числовые столбцы.';
    }
    
    if (lowerMessage.includes('выброс') || lowerMessage.includes('outlier')) {
        const colsWithOutliers = currentDataset.columns.filter(
            col => col.outliers && col.outliers.count > 0
        );
        
        if (colsWithOutliers.length === 0) {
            return 'Выбросов не обнаружено! Данные выглядят чистыми. ✨';
        }
        
        let response = `Выбросы найдены в ${colsWithOutliers.length} столбцах:\n\n`;
        colsWithOutliers.slice(0, 5).forEach(col => {
            response += `• ${col.name}: ${col.outliers.count} выбросов (${col.outliers.percentage}%)\n`;
        });
        
        return response;
    }
    
    if (lowerMessage.includes('качеств') || lowerMessage.includes('quality')) {
        const avgScore = currentDataset.quality_metrics.overall_quality_score;
        
        let response = `📈 Общая оценка качества данных: ${avgScore}/100\n\n`;
        
        if (avgScore >= 90) {
            response += 'Отличное качество! Данные готовы к анализу. ✅';
        } else if (avgScore >= 70) {
            response += 'Хорошее качество, но есть области для улучшения. ⚠️';
        } else {
            response += 'Данные требуют очистки и предобработки. 🔧';
        }
        
        response += `\n\nПолнота данных: ${currentDataset.quality_metrics.completeness_percentage}%`;
        response += `\nДубликатов: ${currentDataset.duplicates.total_duplicate_rows}`;
        
        return response;
    }
    
    if (lowerMessage.includes('дубликат') || lowerMessage.includes('duplicate')) {
        const dups = currentDataset.duplicates;
        
        if (dups.total_duplicate_rows === 0) {
            return 'Дубликатов не найдено! Все строки уникальны. 🎯';
        }
        
        let response = `Найдено ${dups.total_duplicate_rows} дублирующихся строк (${dups.percentage}%).\n\n`;
        response += `Количество групп дубликатов: ${dups.duplicate_groups}\n\n`;
        
        if (Object.keys(dups.column_duplicates).length > 0) {
            response += 'Дубликаты по отдельным столбцам:\n';
            Object.entries(dups.column_duplicates).slice(0, 5).forEach(([col, count]) => {
                response += `• ${col}: ${count} дубликатов\n`;
            });
        }
        
        return response;
    }
    
    if (lowerMessage.includes('столбц') || lowerMessage.includes('column')) {
        const cols = currentDataset.columns;
        let response = `В датасете ${cols.length} столбцов:\n\n`;
        
        const types = {};
        cols.forEach(col => {
            types[col.type] = (types[col.type] || 0) + 1;
        });
        
        response += 'Распределение типов:\n';
        Object.entries(types).forEach(([type, count]) => {
            response += `• ${type}: ${count}\n`;
        });
        
        return response;
    }
    
    // Default response
    return `Я понял ваш вопрос: "${message}"\n\nВы можете спросить меня о:\n• Пропущенных значениях\n• Статистике по данным\n• Корреляциях\n• Выбросах\n• Дубликатах\n• Качестве данных\n\nДля полноценного анализа с SQL/pandas запросами необходимо подключить LLM API.`;
}

// Remove old CSV parser - not needed with backend
// Parse CSV function removed

// ---------------------------------------------------------------------------
// Support modal
// ---------------------------------------------------------------------------

function openSupportModal() {
    const modal = document.getElementById('supportModal');
    if (!modal) return;
    modal.style.display = 'flex';
    setTimeout(() => {
        const subj = document.getElementById('supportSubject');
        if (subj) subj.focus();
    }, 50);
}

function closeSupportModal() {
    const modal = document.getElementById('supportModal');
    if (!modal) return;
    modal.style.display = 'none';
}

function onSupportOverlayClick(event) {
    if (event.target && event.target.id === 'supportModal') {
        closeSupportModal();
    }
}

async function submitSupport() {
    const subject = (document.getElementById('supportSubject').value || '').trim();
    const description = (document.getElementById('supportDescription').value || '').trim();
    const email = (document.getElementById('supportEmail').value || '').trim();

    if (!subject || !description) {
        alert('Заполните тему и описание.');
        return;
    }

    const submitBtn = document.querySelector('#supportForm button[type="submit"]');
    if (submitBtn) submitBtn.disabled = true;

    try {
        const resp = await fetch(`${API_BASE_URL}/api/support`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ subject, description, email })
        });
        if (!resp.ok) {
            const t = await resp.text();
            throw new Error(`${resp.status}: ${t}`);
        }
        alert('Спасибо! Ваше обращение принято.');
        document.getElementById('supportForm').reset();
        closeSupportModal();
    } catch (err) {
        alert('Не удалось отправить обращение: ' + err.message);
    } finally {
        if (submitBtn) submitBtn.disabled = false;
    }
}

// Initialization message
console.log('Dataset Analyzer initialized! 🚀');
console.log('Frontend ready. Connecting to backend...');


