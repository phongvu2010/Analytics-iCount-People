/**
 * @file Logic điều khiển cho trang Dashboard Analytics iCount People.
 *
 * Chịu trách nhiệm:
 * - Quản lý trạng thái giao diện (bộ lọc, dữ liệu).
 * - Khởi tạo các thư viện (biểu đồ, lịch).
 * - Gọi API để lấy và hiển thị dữ liệu.
 * - Xử lý các tương tác của người dùng.
 */
document.addEventListener('DOMContentLoaded', async function () {
    // --- STATE & CONSTANTS ---
    const API_BASE_URL = '/api/v1';
    let isInitialLoad = true;
    const state = {
        tableData: [],
        filters: { period: 'month', startDate: '', endDate: '', store: 'all' }
    };

    // --- DOM ELEMENTS CACHING ---
    const elements = {
        skeletonLoader: document.getElementById('skeleton-loader'),
        contentOverlay: document.getElementById('content-overlay'),
        dashboardContent: document.getElementById('dashboard-content'),
        applyFiltersBtn: document.getElementById('apply-filters-btn'),
        periodSelector: document.getElementById('period-selector'),
        storeSelector: document.getElementById('store-selector'),
        tableBody: document.getElementById('details-table-body'),
        downloadCsvBtn: document.getElementById('download-csv-btn'),
        sidebarToggleBtn: document.getElementById('sidebar-toggle-btn'),
        latestTimestamp: document.getElementById('latest-data-timestamp'),
        metrics: {
            totalIn: document.getElementById('metric-total-in'),
            averageIn: document.getElementById('metric-average-in'),
            peakTime: document.getElementById('metric-peak-time'),
            busiestStore: document.getElementById('metric-busiest-store'),
            growth: document.getElementById('metric-growth'),
            growthCard: document.getElementById('metric-growth-card'),
        },
        error: {
            indicator: document.getElementById('error-indicator'),
            bell: document.getElementById('notification-bell'),
            modal: document.getElementById('error-modal'),
            modalPanel: document.getElementById('error-modal-panel'),
            closeBtn: document.getElementById('close-error-modal-btn'),
            logList: document.getElementById('error-log-list'),
        },
        summary: {
            total: document.getElementById('summary-total'),
            average: document.getElementById('summary-average'),
            proportion: document.getElementById('summary-proportion'),
            change: document.getElementById('summary-change'),
        }
    };

    // --- INSTANCES ---
    let trendChart, storeChart, datePickerInstance;

    // --- CHART OPTIONS ---
    const commonChartOptions = {
        chart: {
            toolbar: { show: true },
            foreColor: '#9ca3af'
        },
        grid: { borderColor: '#374151' },
        tooltip: { theme: 'dark' }
    };
    const trendChartOptions = {
        ...commonChartOptions,
        series: [],
        chart: { ...commonChartOptions.chart, type: 'bar', height: 350, background: 'transparent' },
        plotOptions: { bar: { horizontal: false, columnWidth: '60%', borderRadius: 4 } },
        dataLabels: { enabled: false },
        stroke: { show: true, width: 2, colors: ['transparent'] },
        xaxis: { type: 'datetime', labels: { datetimeUTC: false, style: { colors: '#9ca3af' } } },
        yaxis: { title: { text: 'Lượt vào', style: { color: '#9ca3af' } }, labels: { style: { colors: '#9ca3af' } } },
        fill: { opacity: 1 },
        noData: { text: 'Không có dữ liệu', style: { color: '#d1d5db' } }
    };
    const storeChartOptions = {
        ...commonChartOptions,
        series: [],
        chart: { ...commonChartOptions.chart, type: 'donut', height: 350, background: 'transparent' },
        labels: [],
        legend: { position: 'bottom', labels: { colors: '#d1d5db' } },
        dataLabels: { enabled: true, formatter: (val) => `${val.toFixed(1)}%` },
        noData: { ...trendChartOptions.noData }
    };


    // --- UTILITY FUNCTIONS ---

    /** Hiển thị hoặc ẩn lớp phủ loading. */
    const showLoading = (isLoading) => {
        if (isInitialLoad) return;
        elements.contentOverlay.classList.toggle('hidden', !isLoading);
        elements.contentOverlay.classList.toggle('flex', isLoading);
    };

    /** Định dạng số theo kiểu Việt Nam. */
    const formatNumber = (num) => new Intl.NumberFormat('vi-VN').format(num);

    /** Hiển thị hoặc ẩn modal lỗi. */
    const toggleModal = (show) => {
        if (show) {
            elements.error.modal.classList.remove('hidden', 'opacity-0');
            elements.error.modal.classList.add('flex', 'opacity-100');
            setTimeout(() => elements.error.modalPanel.classList.remove('scale-95', 'opacity-0'), 10);
        } else {
            elements.error.modalPanel.classList.add('scale-95', 'opacity-0');
            setTimeout(() => {
                elements.error.modal.classList.add('hidden', 'opacity-0');
                elements.error.modal.classList.remove('flex', 'opacity-100');
            }, 300);
        }
    };

    /** Debounce một hàm để tránh bị gọi liên tục. */
    const debounce = (func, delay) => {
        let timeout;
        return function(...args) {
            clearTimeout(timeout);
            timeout = setTimeout(() => func.apply(this, args), delay);
        };
    };

    // --- URL STATE MANAGEMENT ---

    /** Cập nhật URL với các filter hiện tại mà không tải lại trang. */
    function updateURLWithFilters() {
        const params = new URLSearchParams(window.location.search);
        params.set('period', state.filters.period);
        params.set('startDate', state.filters.startDate);
        params.set('endDate', state.filters.endDate);
        params.set('store', state.filters.store);
        window.history.pushState({}, '', `${window.location.pathname}?${params.toString()}`);
    }

    /** Đọc và áp dụng các filter từ URL khi tải trang. */
    function applyFiltersFromURL() {
        const params = new URLSearchParams(window.location.search);
        const urlPeriod = params.get('period');
        const urlStartDate = params.get('startDate');
        const urlEndDate = params.get('endDate');
        const urlStore = params.get('store');

        if (urlPeriod) {
            state.filters.period = urlPeriod;
            elements.periodSelector.value = urlPeriod;
        }
        if (urlStore) {
            state.filters.store = urlStore;
            // Cần chờ load xong store list mới set value
        }
        if (urlStartDate && urlEndDate) {
            state.filters.startDate = urlStartDate;
            state.filters.endDate = urlEndDate;
            if (datePickerInstance) {
                datePickerInstance.setDateRange(urlStartDate, urlEndDate, true); // true = không trigger event
            }
        }
    }


    // --- INITIALIZATION ---

    /** Khởi tạo các biểu đồ ApexCharts. */
    function initCharts() {
        trendChart = new ApexCharts(document.querySelector('#trend-chart'), trendChartOptions);
        storeChart = new ApexCharts(document.querySelector('#store-chart'), storeChartOptions);
        trendChart.render();
        storeChart.render();
    }

    /** Khởi tạo Date Range Picker. */
    function initDatePicker() {
        const today = new Date();
        const firstDayOfMonth = new Date(today.getFullYear(), today.getMonth(), 1);

        state.filters.startDate = firstDayOfMonth.toISOString().split('T')[0];
        state.filters.endDate = today.toISOString().split('T')[0];

        datePickerInstance = new Litepicker({
            element: document.getElementById('date-range-picker'),
            singleMode: false,
            format: 'YYYY-MM-DD',
            startDate: firstDayOfMonth,
            endDate: today,
            setup: (picker) => picker.on('selected', (d1, d2) => {
                state.filters.startDate = d1.format('YYYY-MM-DD');
                state.filters.endDate = d2.format('YYYY-MM-DD');
            })
        });
    }

    /** Cập nhật Date Picker khi thay đổi bộ lọc Ngày/Tuần/Tháng/Năm. */
    function handlePeriodChange() {
        const period = elements.periodSelector.value;
        const today = new Date();
        let startDate = new Date(), endDate = new Date();

        switch (period) {
            case 'day':
                startDate = endDate = today;
                break;
            case 'week':
                const dayOfWeek = today.getDay();
                const firstDayOfWeek = new Date(today.setDate(today.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1)));
                startDate = firstDayOfWeek;
                endDate = new Date(new Date(startDate).setDate(startDate.getDate() + 6));
                break;
            case 'month':
                startDate = new Date(today.getFullYear(), today.getMonth(), 1);
                endDate = new Date(today.getFullYear(), today.getMonth() + 1, 0);
                break;
            case 'year':
                startDate = new Date(today.getFullYear(), 0, 1);
                endDate = new Date(today.getFullYear(), 11, 31);
                break;
        }
        datePickerInstance.setDateRange(startDate, endDate);
    }

    /** Gắn các event listener cho các element tương tác. */
    function addEventListeners() {
        const debouncedFetch = debounce(() => {
            state.filters.period = elements.periodSelector.value;
            state.filters.store = elements.storeSelector.value;
            updateURLWithFilters();
            fetchDashboardData();
        }, 400);

        elements.applyFiltersBtn.addEventListener('click', debouncedFetch);
        elements.periodSelector.addEventListener('change', handlePeriodChange);
        elements.error.bell.addEventListener('click', () => toggleModal(true));
        elements.error.closeBtn.addEventListener('click', () => toggleModal(false));
        elements.downloadCsvBtn.addEventListener('click', downloadCsv);
        elements.sidebarToggleBtn.addEventListener('click', () => document.body.classList.toggle('sidebar-collapsed'));
        elements.error.modal.addEventListener('click', (e) => {
            if (e.target === elements.error.modal) toggleModal(false);
        });
    }


    // --- DATA HANDLING ---

    /** Tải và hiển thị danh sách cửa hàng vào bộ lọc. */
    async function loadStores() {
        try {
            const response = await fetch(`${API_BASE_URL}/stores`);
            if (!response.ok) throw new Error('Failed to load stores');
            const stores = await response.json();
            stores.forEach(store => {
                const option = document.createElement('option');
                option.value = store;
                option.textContent = store;
                elements.storeSelector.appendChild(option);
            });
            // Áp dụng lại store từ URL nếu có
            if (new URLSearchParams(window.location.search).has('store')) {
                 elements.storeSelector.value = state.filters.store;
            }
        } catch (error) {
            console.error('Error loading stores:', error);
        }
    }

    /** Gọi API chính để lấy tất cả dữ liệu dashboard. */
    async function fetchDashboardData() {
        showLoading(true);
        const params = new URLSearchParams(state.filters);
        const url = `${API_BASE_URL}/dashboard?${params.toString()}`;

        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            updateUI(data);
        } catch (error) {
            console.error('Failed to fetch dashboard data:', error);
            elements.tableBody.innerHTML = `<tr><td colspan="4" class="text-center py-8 text-red-400">Tải dữ liệu thất bại. Vui lòng thử lại.</td></tr>`;
        } finally {
            if (isInitialLoad) {
                elements.skeletonLoader.classList.add('hidden');
                elements.dashboardContent.classList.remove('invisible');
                isInitialLoad = false;
            }
            showLoading(false);
        }
    }

    /** Tải dữ liệu bảng về dưới dạng file CSV. */
    function downloadCsv() {
        if (state.tableData.length === 0) return;
        const headers = ['Ky bao cao', 'Tong luot vao', 'Ty trong (%)', 'Chenh lech (%)'];
        const rows = state.tableData.map(row =>
            [row.period, row.total_in, row.proportion_pct.toFixed(2), row.pct_change.toFixed(1)].join(',')
        );
        const csvContent = [headers.join(','), ...rows].join('\n');
        const blob = new Blob(['\uFEFF' + csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = `bao_cao_tong_hop_${new Date().toISOString().split('T')[0]}.csv`;
        link.click();
        URL.revokeObjectURL(link.href);
    }


    // --- UI UPDATE FUNCTIONS ---

    /** Cập nhật toàn bộ giao diện với dữ liệu mới từ API. */
    function updateUI(data) {
        updateMetrics(data.metrics);
        updateCharts(data.trend_chart, data.store_comparison_chart);
        state.tableData = data.table_data.data;
        updateTable(data.table_data);
        updateSummaryRow(data.table_data.summary);
        updateErrorNotifications(data.error_logs);
        updateLatestTimestamp(data.latest_record_time);
    }

    /** Cập nhật thời gian của dữ liệu gần nhất. */
    function updateLatestTimestamp(timestamp) {
        if (!elements.latestTimestamp || !timestamp) return;
        const formattedDate = new Date(timestamp).toLocaleString('vi-VN', {
            day: '2-digit', month: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit'
        }).replace(',', '');
        elements.latestTimestamp.innerHTML = `Dữ liệu cập nhật lúc: <span class="font-semibold text-gray-300">${formattedDate}</span>`;
    }

    /** Cập nhật các thẻ chỉ số KPI. */
    function updateMetrics(metrics) {
        elements.metrics.totalIn.textContent = formatNumber(metrics.total_in);
        elements.metrics.averageIn.textContent = formatNumber(metrics.average_in);
        elements.metrics.peakTime.textContent = metrics.peak_time || '--:--';
        elements.metrics.busiestStore.textContent = metrics.busiest_store || 'N/A';

        const { growth } = metrics;
        const { growth: growthEl, growthCard } = elements.metrics;
        const iconDiv = growthCard.querySelector('[data-container="icon"]');
        const icon = iconDiv?.querySelector('[data-lucide]');
        if (!icon) return;

        growthEl.textContent = `${growth.toFixed(1)}%`;
        growthEl.className = 'text-4xl font-extrabold'; // Reset classes
        let iconName = 'arrow-right', colorClass = 'gray';

        if (growth > 0) { colorClass = 'green'; iconName = 'arrow-up-right'; }
        else if (growth < 0) { colorClass = 'red'; iconName = 'arrow-down-right'; }

        growthEl.classList.add(`text-${colorClass}-400`);
        iconDiv.className = `p-2 rounded-lg bg-${colorClass}-500/20`;
        icon.setAttribute('data-lucide', iconName);
        icon.className = `h-5 w-5 text-${colorClass}-400`;
        lucide.createIcons();
    }

    /** Cập nhật dữ liệu cho 2 biểu đồ chính. */
    function updateCharts(trendData, storeData) {
        trendChart.updateSeries([{ name: 'Lượt vào', data: trendData.series }]);
        storeChart.updateOptions({
            series: storeData.series.map(p => p.y),
            labels: storeData.series.map(p => p.x)
        });
    }

    /** Cập nhật lại bảng dữ liệu chi tiết. */
    function updateTable(tableData) {
        if (!tableData.data || tableData.data.length === 0) {
            elements.tableBody.innerHTML = `<tr><td colspan="4" class="text-center py-8 text-gray-400">Không có dữ liệu tổng hợp.</td></tr>`;
            return;
        }

        elements.tableBody.innerHTML = tableData.data.map(row => {
            const { pct_change } = row;
            let changeClass = 'text-gray-300', icon = 'minus', sign = '';
            if (pct_change > 0) { changeClass = 'text-green-400'; icon = 'trending-up'; sign = '+'; }
            else if (pct_change < 0) { changeClass = 'text-red-400'; icon = 'trending-down'; }

            return `
                <tr class="hover:bg-gray-800 transition-colors">
                    <td class="px-6 py-4 text-sm text-gray-300">${row.period}</td>
                    <td class="px-6 py-4 text-sm font-semibold text-white">${formatNumber(row.total_in)}</td>
                    <td class="px-6 py-4 text-sm text-gray-300">${row.proportion_pct.toFixed(2)}%</td>
                    <td class="px-6 py-4 text-sm font-semibold">
                        <div class="flex items-center ${changeClass}">
                            <i data-lucide="${icon}" class="h-4 w-4 mr-1"></i>
                            <span>${sign}${pct_change.toFixed(1)}%</span>
                        </div>
                    </td>
                </tr>`;
        }).join('');
        lucide.createIcons();
    }

    /** Cập nhật dòng tổng kết của bảng. */
    function updateSummaryRow(summary) {
        if (!summary) return;
        elements.summary.total.textContent = formatNumber(summary.total_sum || 0);
        elements.summary.average.textContent = `TB: ${formatNumber(parseFloat(summary.average_in || 0).toFixed(0))}`;
    }

    /** Cập nhật thông báo lỗi. */
    function updateErrorNotifications(errorLogs) {
        const hasErrors = errorLogs && errorLogs.length > 0;
        elements.error.indicator.classList.toggle('hidden', !hasErrors);
        elements.error.logList.innerHTML = hasErrors
            ? errorLogs.map(log => `
                <li class="p-4 rounded-lg bg-gray-800/70 border border-gray-700">
                    <div class="flex justify-between items-start">
                        <div>
                            <p class="font-bold text-red-400">${log.error_message}</p>
                            <p class="text-sm text-gray-400">Vị trí: <span class="font-medium text-gray-300">${log.store_name}</span> | Mã lỗi: ${log.error_code}</p>
                        </div>
                        <p class="text-xs text-gray-500 whitespace-nowrap pl-4">${new Date(log.log_time).toLocaleString('vi-VN')}</p>
                    </div>
                </li>`).join('')
            : `<li class="text-gray-400">Không có lỗi nào được ghi nhận gần đây.</li>`;
    }


    // --- MAIN EXECUTION ---
    /** Hàm khởi tạo chính, chạy tuần tự các bước setup. */
    async function initializeDashboard() {
        try {
            initCharts();
            initDatePicker();
            addEventListeners();
            document.body.classList.add('sidebar-collapsed');

            await loadStores(); // Phải load stores trước
            applyFiltersFromURL(); // Rồi mới áp dụng filter từ URL
            await fetchDashboardData(); // Cuối cùng mới fetch data
        } catch (error) {
            console.error('An error occurred during initial page load:', error);
            elements.skeletonLoader.classList.add('hidden');
            elements.dashboardContent.classList.remove('invisible');
        }
    }

    initializeDashboard();
});
