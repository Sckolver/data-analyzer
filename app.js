// Global variables
let currentDataset = null;
let currentChart = null;
let dataSourceMode = null;       // 'dataset' | 'database'
let currentTableName = null;     // имя выбранной таблицы в режиме database
let dbTables = [];               // список таблиц БД (meta)
let dbMeta = {};                 // { dialect, dbname }
let currentSourceId = null;      // идентификатор текущего источника (для чатов)
let currentChatId = null;        // активный чат
let chatsCache = [];             // последний загруженный список чатов
const API_BASE_URL = 'http://localhost:8765';

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
    currentSourceId = null;
    currentChatId = null;
    chatsCache = [];
    renderChatList();
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
        applyModeLabels('dataset');

        await loadAnalysis({ file, displaySize: null });
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

        showLoadingState('Анализ данных...');
        await loadAnalysis({ fileLike: { name: dbMeta.dbname }, displaySize: null });
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

function formatTodayUpdateLabel() {
    const now = new Date();
    const dd = String(now.getDate()).padStart(2, '0');
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const yyyy = now.getFullYear();
    return `09:00 ${dd}.${mm}.${yyyy}`;
}

function fillSourceMetadataCards() {
    // Метаданные показываем ТОЛЬКО для БД-источника. Для датасета — скрываем целиком.
    const section = document.getElementById('metaSection');
    if (!section) return;

    if (dataSourceMode !== 'database') {
        section.style.display = 'none';
        return;
    }
    section.style.display = '';

    // Захардкоженные значения для демонстрационных целей
    const meta = {
        owner: 'Анастасия Князева (a.knyazeva)',
        purpose: 'Аналитика инвестиционных операций',
        update: formatTodayUpdateLabel(),
        sensitivity: 'Конфиденциальные данные',
    };
    const set = (id, value) => {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    };
    set('metaOwner', meta.owner);
    set('metaPurpose', meta.purpose);
    set('metaUpdate', meta.update);
    set('metaSensitivity', meta.sensitivity);
}

function applyModeLabels(mode) {
    const isDb = mode === 'database';
    const infoHeading = document.getElementById('infoHeading');
    if (infoHeading) infoHeading.textContent = isDb ? 'Информация о базе данных' : 'Информация о датасете';

    const greetEl = document.getElementById('chatGreetingText');
    if (greetEl) {
        greetEl.textContent = isDb
            ? 'Привет! Я — AI-ассистент по анализу данных в сфере инвестиций. Задавай вопрос на основе данных из подключенной БД ☺️'
            : 'Привет! Я — AI-ассистент по анализу данных в сфере инвестиций. Задавай вопрос на основе данных из загруженного датасета ☺️';
    }
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
    currentSourceId = null;
    currentChatId = null;
    chatsCache = [];
    renderChatList();
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
    }

    fillSourceMetadataCards();

    refreshSourceAndChats().catch(err => console.warn('chats init failed', err));

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
    document.getElementById('duplicatesDetail').textContent = duplicates.percentage > 0 ? `${duplicates.percentage}% строк` : 'Дубликатов не обнаружено';
    
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
            extraInfo += `<br><small>📊 Энтропия (непредсказуемость данных): ${col.entropy}</small>`;
        }
        
        row.innerHTML = `
            <td>
                <strong>${col.name}</strong>
                ${extraInfo}
            </td>
            <td>
                <span class="type-badge">${col.type}</span>
                ${col.statistics ? `<br><small>avg=${col.statistics.mean.toFixed(2)}, sd=${col.statistics.std.toFixed(2)}</small>` : ''}
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
}

function looksLikeDateColumn(colInfo) {
    if (!colInfo) return false;
    if (colInfo.type === 'datetime') return true;
    const n = String(colInfo.name || '').toLowerCase();
    return /(date|dt|time|created|updated|opened|closed|трад|дата)/.test(n);
}

function populateColumnSelect(columns) {
    const select = document.getElementById('columnSelect');
    select.innerHTML = '<option>Выберите столбец...</option>';

    columns.forEach(col => {
        const option = document.createElement('option');
        option.value = col;
        option.textContent = col;
        select.appendChild(option);
    });

    const dateSelect = document.getElementById('dateColumnSelect');
    if (dateSelect) {
        dateSelect.innerHTML = '<option>Выберите столбец даты...</option>';
        const colInfos = (currentDataset && Array.isArray(currentDataset.columns))
            ? currentDataset.columns
            : columns.map(name => ({ name }));
        const dateCandidates = colInfos.filter(looksLikeDateColumn);
        const list = dateCandidates.length > 0 ? dateCandidates : colInfos;
        list.forEach(c => {
            const opt = document.createElement('option');
            opt.value = c.name;
            opt.textContent = c.name + (c.type ? ` (${c.type})` : '');
            dateSelect.appendChild(opt);
        });
    }
}

function onVizTypeChange() {
    const vizType = document.getElementById('vizType').value;
    const dateGroup = document.getElementById('dateColumnGroup');
    const colGroup = document.getElementById('columnSelectGroup');
    const colLabel = document.getElementById('columnSelectLabel');
    const aggGroup = document.getElementById('aggSelectGroup');

    if (vizType === 'dynamics') {
        if (dateGroup) dateGroup.style.display = '';
        if (colGroup) colGroup.style.display = '';
        if (colLabel) colLabel.textContent = 'Столбец значений:';
        if (aggGroup) aggGroup.style.display = '';
    } else if (vizType === 'missing') {
        if (dateGroup) dateGroup.style.display = 'none';
        if (colGroup) colGroup.style.display = 'none';
        if (colLabel) colLabel.textContent = 'Столбец:';
        if (aggGroup) aggGroup.style.display = 'none';
    } else {
        if (dateGroup) dateGroup.style.display = 'none';
        if (colGroup) colGroup.style.display = '';
        if (colLabel) colLabel.textContent = 'Столбец:';
        if (aggGroup) aggGroup.style.display = 'none';
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
        const dateColEl = document.getElementById('dateColumnSelect');
        const dateCol = dateColEl ? dateColEl.value : '';
        const aggEl = document.getElementById('aggSelect');
        const agg = aggEl ? aggEl.value : 'sum';
        if (!dateCol || dateCol === 'Выберите столбец даты...') {
            alert('Выберите столбец с датой');
            return;
        }
        if (!column || column === 'Выберите столбец...') {
            alert('Выберите столбец значений для агрегации');
            return;
        }
        document.getElementById('vizPlaceholder').style.display = 'none';
        try {
            await createDynamicsChart(dateCol, column, agg);
        } catch (error) {
            alert('Ошибка создания визуализации: ' + error.message);
            console.error(error);
        }
        return;
    }

    if (!column || column === 'Выберите столбец...') {
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

async function createDynamicsChart(dateCol, valueCol, agg) {
    const aggKey = agg || 'sum';
    const url = buildTableQuery(
        `${API_BASE_URL}/api/visualization/dynamics`
        + `?date_column=${encodeURIComponent(dateCol)}`
        + `&value_column=${encodeURIComponent(valueCol)}`
        + `&agg=${encodeURIComponent(aggKey)}`
    );
    const response = await fetch(url);

    if (!response.ok) {
        const text = await response.text();
        throw new Error(text || 'Ошибка получения данных динамики');
    }

    const result = await response.json();
    const data = result.data || { labels: [], values: [] };
    const aggLabel = result.agg_label || aggKey;

    if (currentChart) {
        currentChart.destroy();
    }

    currentChart = new Chart(document.getElementById('chartCanvas'), {
        type: 'line',
        data: {
            labels: data.labels,
            datasets: [{
                label: `${aggLabel} ${valueCol} по дате (${dateCol})`,
                data: data.values,
                borderColor: 'rgba(79, 70, 229, 1)',
                backgroundColor: 'rgba(79, 70, 229, 0.15)',
                borderWidth: 2,
                fill: true,
                tension: 0.25,
                pointRadius: 2,
                pointHoverRadius: 4,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { display: true }
            },
            scales: {
                x: {
                    title: { display: true, text: dateCol }
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: `${aggLabel} ${valueCol}` }
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
            html += `<p><strong>Высокая кардинальность (уникальность данных):</strong> ${warnings.high_cardinality_columns.join(', ')}</p>`;
        }
        
        warningsDiv.innerHTML = html;
        container.appendChild(warningsDiv);
    }
}

function isIdLikeColumn(name) {
    const n = String(name).toLowerCase().trim();
    if (!n) return false;
    if (n === 'id') return true;
    if (n.endsWith('_id') || n.endsWith('id')) {
        if (/(^|_)id$/.test(n)) return true;
    }
    if (n.startsWith('id_')) return true;
    if (/(^|_)id(_|$)/.test(n)) return true;
    return false;
}

function displayCorrelations(correlation) {
    const container = document.querySelector('.quality-table');

    const oldCorr = document.getElementById('correlationsSection');
    if (oldCorr) {
        oldCorr.remove();
    }

    if (!correlation.available || !correlation.strong_correlations) return;

    const filtered = correlation.strong_correlations.filter(
        c => !isIdLikeColumn(c.col1) && !isIdLikeColumn(c.col2)
    );

    if (filtered.length === 0) return;

    const corrDiv = document.createElement('div');
    corrDiv.id = 'correlationsSection';
    corrDiv.style.marginTop = '20px';
    corrDiv.style.padding = '20px';
    corrDiv.style.background = '#dbeafe';
    corrDiv.style.borderRadius = '8px';
    corrDiv.style.border = '2px solid #3b82f6';

    let html = '<h4 style="margin-bottom: 10px;"><i class="fas fa-link"></i> Сильные корреляции</h4>';
    html += '<ul style="margin-left: 20px;">';

    filtered.forEach(corr => {
        const color = Math.abs(corr.correlation) > 0.9 ? '#dc2626' : '#f97316';
        html += `<li><strong>${corr.col1}</strong> ↔ <strong>${corr.col2}</strong>: 
                 <span style="color: ${color}; font-weight: bold;">${corr.correlation.toFixed(3)}</span>
                 (${corr.strength})</li>`;
    });

    html += '</ul>';
    corrDiv.innerHTML = html;
    container.appendChild(corrDiv);
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

// ---------------------------------------------------------------------------
// Чаты: множественные диалоги, привязанные к текущему источнику данных
// ---------------------------------------------------------------------------

async function refreshSourceAndChats() {
    try {
        const sourceResp = await fetch(`${API_BASE_URL}/api/source`);
        if (sourceResp.ok) {
            const info = await sourceResp.json();
            currentSourceId = info.source_id || null;
        }
    } catch (e) {
        console.warn('Не удалось получить source_id', e);
    }
    await loadChats();
    if (chatsCache.length === 0) {
        await createNewChat({ silent: true });
    } else {
        await selectChat(chatsCache[0].id);
    }
}

async function loadChats() {
    if (!currentSourceId) {
        chatsCache = [];
        renderChatList();
        return;
    }
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats?source_id=${encodeURIComponent(currentSourceId)}`);
        if (!resp.ok) throw new Error('Не удалось загрузить чаты');
        const data = await resp.json();
        chatsCache = data.chats || [];
        renderChatList();
    } catch (e) {
        console.error(e);
        chatsCache = [];
        renderChatList();
    }
}

function renderChatList() {
    const listEl = document.getElementById('chatList');
    if (!listEl) return;

    if (!chatsCache || chatsCache.length === 0) {
        listEl.innerHTML = '<div class="chat-list-empty">Нет сохранённых чатов</div>';
        updateCurrentChatLabel();
        return;
    }

    listEl.innerHTML = '';
    chatsCache.forEach(chat => {
        const item = document.createElement('div');
        item.className = 'chat-list-item' + (chat.id === currentChatId ? ' active' : '');
        item.title = `${chat.name} (сообщений: ${chat.messages_count || 0})`;
        item.innerHTML = `
            <span class="chat-list-item-name">${escapeHtml(chat.name)}</span>
            <button class="chat-list-item-action" title="Переименовать"><i class="fas fa-pen"></i></button>
            <button class="chat-list-item-action" title="Удалить"><i class="fas fa-trash"></i></button>
        `;
        const [renameBtn, deleteBtn] = item.querySelectorAll('.chat-list-item-action');
        renameBtn.addEventListener('click', (e) => { e.stopPropagation(); renameChat(chat.id); });
        deleteBtn.addEventListener('click', (e) => { e.stopPropagation(); deleteChat(chat.id); });
        item.addEventListener('click', () => selectChat(chat.id));
        listEl.appendChild(item);
    });

    updateCurrentChatLabel();
}

function updateCurrentChatLabel() {
    const el = document.getElementById('chatCurrentName');
    if (!el) return;
    const current = chatsCache.find(c => c.id === currentChatId);
    el.textContent = current ? current.name : '';
}

async function createNewChat(opts = {}) {
    if (!currentSourceId) {
        if (!opts.silent) alert('Сначала загрузите данные');
        return;
    }
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source_id: currentSourceId, name: 'Новый чат' })
        });
        if (!resp.ok) throw new Error('Не удалось создать чат');
        const data = await resp.json();
        await loadChats();
        await selectChat(data.chat.id);
    } catch (e) {
        console.error(e);
        if (!opts.silent) alert('Ошибка создания чата: ' + e.message);
    }
}

async function renameChat(chatId) {
    const chat = chatsCache.find(c => c.id === chatId);
    const oldName = chat ? chat.name : '';
    const newName = prompt('Новое название чата:', oldName);
    if (!newName || newName.trim() === '' || newName === oldName) return;
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(chatId)}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: newName.trim() })
        });
        if (!resp.ok) throw new Error('Не удалось переименовать');
        await loadChats();
    } catch (e) {
        console.error(e);
        alert('Ошибка переименования: ' + e.message);
    }
}

async function deleteChat(chatId) {
    const chat = chatsCache.find(c => c.id === chatId);
    if (!confirm(`Удалить чат «${chat ? chat.name : chatId}»? Это действие необратимо.`)) return;
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(chatId)}`, {
            method: 'DELETE'
        });
        if (!resp.ok) throw new Error('Не удалось удалить');
        if (currentChatId === chatId) {
            currentChatId = null;
        }
        await loadChats();
        if (chatsCache.length > 0) {
            await selectChat(chatsCache[0].id);
        } else {
            await createNewChat({ silent: true });
        }
    } catch (e) {
        console.error(e);
        alert('Ошибка удаления: ' + e.message);
    }
}

async function selectChat(chatId) {
    if (!chatId) return;
    try {
        const resp = await fetch(`${API_BASE_URL}/api/chats/${encodeURIComponent(chatId)}`);
        if (!resp.ok) throw new Error('Не удалось загрузить чат');
        const data = await resp.json();
        currentChatId = chatId;
        renderChatHistory(data.chat.history || []);
        renderChatList();
    } catch (e) {
        console.error(e);
        alert('Ошибка загрузки чата: ' + e.message);
    }
}

function renderChatHistory(history) {
    const messagesContainer = document.getElementById('chatMessages');
    if (!messagesContainer) return;
    messagesContainer.innerHTML = '';

    const greetingHtml = `
        <div class="chat-message assistant" id="chatGreeting">
            <div class="message-avatar">
                <i class="fas fa-robot" style="color:#3b82f6;"></i>
            </div>
            <div class="message-content">
                <p id="chatGreetingText">Привет! Я AI-ассистент. Задавай вопрос на основе текущих данных.</p>
            </div>
        </div>
    `;
    messagesContainer.innerHTML = greetingHtml;

    history.forEach(msg => {
        const role = msg.role;
        if (role === 'user' || role === 'assistant') {
            const content = (msg.content || '').toString();
            if (!content.trim()) return;
            addChatMessage(content, role);
        }
    });
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

    if (!currentChatId) {
        await createNewChat({ silent: true });
        if (!currentChatId) {
            alert('Не удалось создать чат');
            return;
        }
    }

    addChatMessage(message, 'user');
    input.value = '';
    input.disabled = true;
    if (sendBtn) sendBtn.disabled = true;

    const thinkingEl = addChatMessage('Анализирую данные...', 'assistant', { ephemeral: true });

    try {
        const response = await fetch(`${API_BASE_URL}/api/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message, chat_id: currentChatId })
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
    } catch (err) {
        if (thinkingEl && thinkingEl.remove) thinkingEl.remove();
        addChatMessage('Упс, что-то пошло не так: ' + err.message, 'assistant');
        console.error(err);
    } finally {
        input.disabled = false;
        if (sendBtn) sendBtn.disabled = false;
        input.focus();
        loadChats().catch(() => {});
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
                    Ассистент запустил ${trace.length} инструмент(а/ов) — показать
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
        ? '<i class="fas fa-robot" style="color:#3b82f6;"></i>'
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
// Поддержка: модалка с формой -> POST /api/support
// ---------------------------------------------------------------------------

function openSupport() {
    const modal = document.getElementById('supportModal');
    if (!modal) return;
    const status = document.getElementById('supportStatus');
    if (status) {
        status.textContent = '';
        status.className = 'modal-status';
    }
    modal.style.display = 'flex';
    setTimeout(() => {
        const subj = document.getElementById('supportSubject');
        if (subj) subj.focus();
    }, 50);
}

function closeSupport() {
    const modal = document.getElementById('supportModal');
    if (modal) modal.style.display = 'none';
}

function onSupportOverlayClick(event) {
    if (event && event.target && event.target.id === 'supportModal') {
        closeSupport();
    }
}

async function submitSupport() {
    const subject = (document.getElementById('supportSubject').value || '').trim();
    const description = (document.getElementById('supportDescription').value || '').trim();
    const email = (document.getElementById('supportEmail').value || '').trim();
    const status = document.getElementById('supportStatus');
    const submitBtn = document.getElementById('supportSubmitBtn');

    if (!subject || !description) {
        if (status) {
            status.textContent = 'Заполните тему и описание.';
            status.className = 'modal-status error';
        }
        return;
    }

    if (status) {
        status.textContent = 'Отправка...';
        status.className = 'modal-status';
    }
    if (submitBtn) submitBtn.disabled = true;

    try {
        const resp = await fetch(`${API_BASE_URL}/api/support`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ subject, description, email: email || null })
        });
        if (!resp.ok) {
            const text = await resp.text();
            throw new Error(text || `HTTP ${resp.status}`);
        }
        if (status) {
            status.textContent = 'Спасибо! Обращение отправлено.';
            status.className = 'modal-status success';
        }
        document.getElementById('supportForm').reset();
        setTimeout(closeSupport, 1500);
    } catch (e) {
        console.error(e);
        if (status) {
            status.textContent = 'Ошибка отправки: ' + e.message;
            status.className = 'modal-status error';
        }
    } finally {
        if (submitBtn) submitBtn.disabled = false;
    }
}

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        const modal = document.getElementById('supportModal');
        if (modal && modal.style.display !== 'none') closeSupport();
    }
});

// Initialization message
console.log('Dataset Analyzer initialized! 🚀');
console.log('Frontend ready. Connecting to backend...');


