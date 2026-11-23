// RiskFlow Orchestrator — Codelink #113369 Live Dashboard Client
window.addEventListener("load", async () => {
    const processSelect = document.getElementById("process-select");
    const processMeta = document.getElementById("process-meta");
    const dateInput = document.getElementById("target-date");
    const analystSelect = document.getElementById("analyst-select");

    const wsStatus = document.getElementById("ws-status");
    const wsDot = document.getElementById("ws-dot");
    const timerIntervalInput = document.getElementById("timer-interval");
    const saveIntervalBtn = document.getElementById("save-interval");
    const themeToggleBtn = document.getElementById("theme-toggle");

    const dryRunBox = document.getElementById("dry-run");
    const dryRunBadge = document.getElementById("dry-run-badge");
    const dryRunWindow = document.getElementById("dry-run-window");
    const dryRunReason = document.getElementById("dry-run-reason");
    const dryRunLocks = document.getElementById("dry-run-locks");

    const runBtn = document.getElementById("run-calculation");
    const cronBtn = document.getElementById("run-cron");
    const dedupBtn = document.getElementById("test-deduplication");
    const hiddenBtn = document.getElementById("hidden-conflict");
    const parallelDatesBtn = document.getElementById("demo-parallel-dates");

    let processesMap = {};
    let ws = null;
    let reconnectTimer = null;

    // Theme persistence & toggle
    const applyTheme = (isLight) => {
        document.body.classList.toggle("light-theme", isLight);
        if (themeToggleBtn) {
            themeToggleBtn.textContent = isLight ? "🌙 Тёмная тема" : "☀️ Светлая тема";
        }
        try {
            localStorage.setItem("riskflow_theme", isLight ? "light" : "dark");
        } catch (_) {}
    };

    let savedTheme = "dark";
    try {
        savedTheme = localStorage.getItem("riskflow_theme") || "dark";
    } catch (_) {}
    applyTheme(savedTheme === "light");

    if (themeToggleBtn) {
        themeToggleBtn.addEventListener("click", () => {
            applyTheme(!document.body.classList.contains("light-theme"));
        });
    }

    // Tabs switching
    document.querySelectorAll(".tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
            document.querySelectorAll(".tab-content").forEach((c) => c.classList.remove("active"));
            btn.classList.add("active");
            const target = document.getElementById(btn.dataset.tab);
            if (target) target.classList.add("active");
        });
    });

    const setWsStatus = (text, color) => {
        if (wsStatus) wsStatus.textContent = text;
        if (wsDot) wsDot.style.background = color;
    };

    const scheduleReconnect = () => {
        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }
        reconnectTimer = setTimeout(() => {
            reconnectTimer = null;
            connectWs();
        }, 2000);
    };

    const connectWs = () => {
        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }
        if (ws) {
            try {
                ws.onopen = null;
                ws.onclose = null;
                ws.onerror = null;
                ws.onmessage = null;
                if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
                    ws.close();
                }
            } catch (_) {}
            ws = null;
        }

        const protocol = location.protocol === "https:" ? "wss" : "ws";
        const socket = new WebSocket(`${protocol}://${location.host}/ws/events`);
        ws = socket;

        socket.onopen = () => {
            if (ws !== socket) return;
            setWsStatus("Онлайн (WebSocket)", "#10b981");
            if (reconnectTimer) {
                clearTimeout(reconnectTimer);
                reconnectTimer = null;
            }
        };

        socket.onclose = () => {
            if (ws !== socket) return;
            setWsStatus("Переподключение...", "#f59e0b");
            scheduleReconnect();
        };

        socket.onerror = () => {
            if (ws !== socket) return;
            setWsStatus("Ошибка связи", "#ef4444");
        };

        socket.onmessage = (e) => {
            if (ws !== socket) return;
            try {
                const msg = JSON.parse(e.data);
                const snapshot = msg ? (msg.data || msg.payload || msg) : null;
                if (snapshot && typeof snapshot === "object") {
                    renderState(snapshot);
                    refreshDryRun();
                }
            } catch (err) {
                console.error("WS parse error:", err);
            }
        };
    };

    const renderProcessMeta = () => {
        if (!processSelect || !processMeta) return;
        const proc = processesMap[processSelect.value];
        if (!proc) {
            processMeta.innerHTML = "";
            return;
        }
        const tags = (proc.resources || [])
            .map((r) => {
                const mode = (r.mode || r.access_mode || "READ").toUpperCase();
                const resId = r.resource_id || "";
                const modeClass = mode === "WRITE" ? "write" : "read";
                return `<span class="res-tag ${modeClass}">${resId} [${mode}]</span>`;
            })
            .join("");
        processMeta.innerHTML = `<div style="width:100%;margin-bottom:4px;">${proc.description || ""}</div>${tags}`;
    };

    const loadProcesses = async () => {
        const res = await fetch("/api/processes");
        if (!res.ok) throw new Error("Не удалось загрузить процессы");
        const data = await res.json();
        const list = Array.isArray(data)
            ? data
            : (data && Array.isArray(data.processes) ? data.processes : []);

        if (!processSelect) return;
        processSelect.innerHTML = "";
        processesMap = {};
        list.forEach((p) => {
            const pId = p.id || p.process_id;
            processesMap[pId] = p;
            const opt = document.createElement("option");
            opt.value = pId;
            opt.textContent = `${p.name || pId} (окно ${p.window_days || 1} дн.)`;
            processSelect.appendChild(opt);
        });
        renderProcessMeta();
    };

    const setDefaultDate = () => {
        if (!dateInput) return;
        dateInput.value = new Date().toISOString().split("T")[0];
    };

    const payloadFromForm = () => ({
        process_id: processSelect ? processSelect.value : "credit_risk",
        target_date: dateInput ? dateInput.value : new Date().toISOString().split("T")[0],
        user_name: analystSelect ? analystSelect.value : "Аналитик 1",
        initiator: "USER",
    });

    const refreshDryRun = async () => {
        if (!processSelect || !dateInput || !processSelect.value || !dateInput.value) return;
        try {
            const res = await fetch("/api/runs/dry-run", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payloadFromForm()),
            });
            const data = await res.json();
            if (!dryRunBox || !dryRunBadge || !dryRunReason) return;

            if (!res.ok) {
                dryRunBadge.textContent = "Ошибка проверки";
                dryRunReason.textContent = data.detail || `Код ${res.status}`;
                return;
            }

            const map = {
                RUN_IMMEDIATELY: {
                    label: "Немедленный параллельный запуск",
                    color: "var(--green)",
                    bg: "var(--green-bg)",
                },
                DEDUPLICATE: {
                    label: "Дедупликация (подключение к дублю)",
                    color: "var(--cyan)",
                    bg: "var(--cyan-bg)",
                },
                QUEUE: {
                    label: "Постановка в очередь (Lock-менеджер)",
                    color: "var(--amber)",
                    bg: "var(--amber-bg)",
                },
                WAIT_TIMER: {
                    label: "Ожидание по таймеру машины",
                    color: "var(--purple)",
                    bg: "var(--purple-bg)",
                },
            };

            const meta = map[data.prediction] || {
                label: data.prediction,
                color: "var(--blue)",
                bg: "var(--blue-bg)",
            };

            dryRunBox.style.borderLeftColor = meta.color;
            dryRunBadge.textContent = meta.label;
            dryRunBadge.style.color = meta.color;
            dryRunBadge.style.background = meta.bg;
            if (dryRunWindow) {
                dryRunWindow.textContent = (data.date_from && data.date_to)
                    ? `Окно: ${data.date_from} .. ${data.date_to}`
                    : "";
            }
            dryRunReason.textContent = data.reason || "";

            if (dryRunLocks) {
                if (data.required_locks && Array.isArray(data.required_locks) && data.required_locks.length > 0) {
                    dryRunLocks.innerHTML = `<div style="margin-top:6px;display:flex;flex-wrap:wrap;gap:4px;">${data.required_locks
                        .map((l) => {
                            const mode = (l.access_mode || l.mode || "READ").toUpperCase();
                            const resId = l.resource_id || "";
                            const modeClass = mode === "WRITE" ? "write" : "read";
                            return `<span class="res-tag ${modeClass}">${resId} (${mode})</span>`;
                        })
                        .join("")}</div>`;
                } else {
                    dryRunLocks.innerHTML = "";
                }
            }
        } catch (err) {
            if (dryRunBadge) dryRunBadge.textContent = "Нет связи с API";
            if (dryRunReason) dryRunReason.textContent = err.message;
        }
    };

    // Actions
    if (runBtn) {
        runBtn.addEventListener("click", async () => {
            await fetch("/api/runs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payloadFromForm()),
            });
        });
    }

    if (parallelDatesBtn) {
        parallelDatesBtn.addEventListener("click", async () => {
            const user = analystSelect ? analystSelect.value : "Аналитик 1";
            await Promise.all([
                fetch("/api/runs", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        process_id: "credit_risk",
                        target_date: "2026-11-19",
                        user_name: user,
                        initiator: "USER",
                    }),
                }),
                fetch("/api/runs", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        process_id: "credit_risk",
                        target_date: "2026-11-20",
                        user_name: "Аналитик 2",
                        initiator: "USER",
                    }),
                }),
            ]);
        });
    }

    if (dedupBtn) {
        dedupBtn.addEventListener("click", async () => {
            const base = payloadFromForm();
            await Promise.all([
                fetch("/api/runs", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ ...base, user_name: "Аналитик 1" }),
                }),
                fetch("/api/runs", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ ...base, user_name: "Аналитик 2" }),
                }),
            ]);
        });
    }

    if (hiddenBtn) {
        hiddenBtn.addEventListener("click", async () => {
            const user = analystSelect ? analystSelect.value : "Аналитик 1";
            await fetch("/api/runs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    process_id: "credit_risk",
                    target_date: "2026-11-20",
                    user_name: user,
                    initiator: "USER",
                }),
            });
            setTimeout(async () => {
                await fetch("/api/runs", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        process_id: "etl_transactions",
                        target_date: "2026-11-19",
                        user_name: "Аналитик 2",
                        initiator: "USER",
                    }),
                });
            }, 250);
        });
    }

    if (cronBtn) {
        cronBtn.addEventListener("click", async () => {
            const procId = processSelect ? processSelect.value : "credit_risk";
            const tDate = dateInput ? dateInput.value : new Date().toISOString().split("T")[0];
            const params = new URLSearchParams({
                process_id: procId,
                target_date: tDate,
            });
            await fetch(`/api/demo/trigger-cron?${params.toString()}`, { method: "POST" });
        });
    }

    if (saveIntervalBtn && timerIntervalInput) {
        saveIntervalBtn.addEventListener("click", async () => {
            const val = parseInt(timerIntervalInput.value, 10);
            if (Number.isNaN(val) || val < 1) return;
            await fetch("/api/config", {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ machine_timer_interval_sec: val }),
            });
        });
    }

    const stageLabel = (stage) => {
        const stages = {
            EXTRACT: "1. Чтение среза (Snapshot Isolation)",
            STAGING: "2. Изолированный буфер (temp_risk)",
            COMPUTE: "3. Расчёт метрик риска",
            LOAD: "4. Атомарная публикация (MERGE)",
        };
        return stages[stage] || stage || "Инициализация";
    };

    const renderTimeline = (snap) => {
        const container = document.getElementById("timeline-container");
        if (!container) return;

        const all = [
            ...(snap.active_runs || []),
            ...(snap.queue || []),
            ...(snap.timer_waiting || []),
            ...(snap.history || []).slice(0, 6),
        ];

        if (all.length === 0) {
            container.innerHTML = `<div class="empty-state">Запустите расчёты или выберите демо-сценарий слева, чтобы увидеть диаграмму выполнения</div>`;
            return;
        }

        all.sort((a, b) => new Date(a.created_at || 0) - new Date(b.created_at || 0));
        const recent = all.slice(-8);

        const times = recent.map((r) => new Date(r.created_at || Date.now()).getTime());
        const minT = Math.min(...times);
        const maxT = Math.max(Date.now(), ...times) + 1000;
        const span = Math.max(maxT - minT, 4000);

        const statusMeta = {
            RUNNING: { label: "Выполняется", color: "var(--green)" },
            QUEUED: { label: "В очереди", color: "var(--amber)" },
            WAITING_TIMER: { label: "Таймер машины", color: "var(--purple)" },
            COMPLETED: { label: "Завершён", color: "var(--green)" },
            FAILED: { label: "Сбой / TTL", color: "var(--red)" },
            CANCELLED: { label: "Отменён", color: "var(--text-dim)" },
        };

        container.innerHTML = recent
            .map((r) => {
                const startMs = new Date(r.started_at || r.created_at || Date.now()).getTime();
                const endMs = r.finished_at
                    ? new Date(r.finished_at).getTime()
                    : Date.now();
                const leftPct = Math.max(0, Math.min(92, ((startMs - minT) / span) * 100));
                const widthPct = Math.max(6, Math.min(100 - leftPct, ((endMs - startMs) / span) * 100));
                const isMachine = r.initiator === "MACHINE";
                const initBadge = isMachine
                    ? `<span class="badge badge-machine" style="padding:1px 6px;font-size:0.68rem;">MACHINE</span>`
                    : `<span class="badge badge-user" style="padding:1px 6px;font-size:0.68rem;">USER</span>`;
                const st = statusMeta[r.status] || { label: r.status, color: "var(--text-main)" };
                const runIdShort = String(r.run_id || "").slice(0, 8);
                const procTitle = r.process_name || r.process_id || "Расчёт";
                const targetD = r.target_date || "";
                return `
                    <div class="tl-row">
                        <div class="tl-info">
                            <div class="tl-sub"><span class="mono">#${runIdShort}</span> ${initBadge}</div>
                            <div class="tl-name">${procTitle} (${targetD})</div>
                        </div>
                        <div class="tl-track">
                            <div class="tl-bar ${r.status}" style="left:${leftPct.toFixed(1)}%;width:${widthPct.toFixed(1)}%;"></div>
                        </div>
                        <div class="tl-status" style="color:${st.color};">${st.label}${r.status === "RUNNING" ? ` (${r.progress}%)` : ""}</div>
                    </div>
                `;
            })
            .join("");
    };

    const renderState = (snap) => {
        if (!snap) return;

        if (
            snap.config &&
            snap.config.machine_timer_interval_sec &&
            timerIntervalInput &&
            document.activeElement !== timerIntervalInput
        ) {
            timerIntervalInput.value = snap.config.machine_timer_interval_sec;
        }

        const activeRuns = snap.active_runs || [];
        const queue = snap.queue || [];
        const waiting = snap.timer_waiting || [];
        const locks = snap.active_locks || [];
        const history = snap.history || [];
        const logs = snap.audit_logs || [];

        // KPIs
        const elRunning = document.getElementById("kpi-running");
        const elQueued = document.getElementById("kpi-queued");
        const elTimer = document.getElementById("kpi-timer");
        const elLocks = document.getElementById("kpi-locks");
        const elCompleted = document.getElementById("kpi-completed");

        if (elRunning) elRunning.textContent = activeRuns.length;
        if (elQueued) elQueued.textContent = queue.length;
        if (elTimer) elTimer.textContent = waiting.length;
        if (elLocks) elLocks.textContent = locks.length;
        if (elCompleted) elCompleted.textContent = history.filter((h) => h.status === "COMPLETED").length;

        const badgeTimer = document.getElementById("timer-count-badge");
        const badgeRunning = document.getElementById("running-count-badge");
        const badgeQueued = document.getElementById("queued-count-badge");
        const badgeHist = document.getElementById("hist-count");
        const badgeAudit = document.getElementById("audit-count");

        if (badgeTimer) badgeTimer.textContent = `${waiting.length} задач`;
        if (badgeRunning) badgeRunning.textContent = activeRuns.length;
        if (badgeQueued) badgeQueued.textContent = queue.length;
        if (badgeHist) badgeHist.textContent = history.length;
        if (badgeAudit) badgeAudit.textContent = logs.length;

        // Timeline
        renderTimeline(snap);

        // 1. Timer Waiting (MACHINE)
        const timerDiv = document.getElementById("waiting-timer");
        if (timerDiv) {
            timerDiv.innerHTML = "";
            if (waiting.length === 0) {
                timerDiv.innerHTML = `<div class="empty-state">Нет отложенных регламентных задач — машина свободна.</div>`;
            } else {
                waiting.forEach((run) => {
                    const item = document.createElement("div");
                    item.className = "task-card timer";
                    const runIdShort = String(run.run_id || "").slice(0, 8);
                    const procTitle = run.process_name || run.process_id || "Регламент";
                    const intervalVal = snap.config?.machine_timer_interval_sec || 15;
                    item.innerHTML = `
                        <div class="task-top">
                            <div>
                                <span class="badge badge-purple">Таймер машины</span>
                                <span class="task-title" style="margin-left:6px;">${procTitle}</span>
                            </div>
                            <span class="task-id">#${runIdShort}</span>
                        </div>
                        <div class="task-meta">Целевая дата: <strong>${run.target_date || ""}</strong> • Инициатор: Планировщик (Cron)</div>
                        <div class="task-reason">${run.blocked_reason || "Ожидание завершения активных пользовательских задач по таймеру"}</div>
                        <div class="task-footer">
                            <span class="task-meta" style="margin:0;">Опрос каждые ${intervalVal} сек.</span>
                            <button class="btn btn-danger">Отменить</button>
                        </div>
                    `;
                    const cancelBtn = item.querySelector("button");
                    if (cancelBtn) {
                        cancelBtn.addEventListener("click", () => {
                            fetch(`/api/runs/${run.run_id}`, { method: "DELETE" });
                        });
                    }
                    timerDiv.appendChild(item);
                });
            }
        }

        // 2. Running Cards
        const runningDiv = document.getElementById("running-cards");
        if (runningDiv) {
            runningDiv.innerHTML = "";
            if (activeRuns.length === 0) {
                runningDiv.innerHTML = `<div class="empty-state">Нет активных расчётов.</div>`;
            } else {
                activeRuns.forEach((run) => {
                    const card = document.createElement("div");
                    card.className = "task-card running";
                    const subs = (run.subscribers || []).join(", ");
                    const initClass = run.initiator === "MACHINE" ? "badge-machine" : "badge-user";
                    const runIdShort = String(run.run_id || "").slice(0, 8);
                    const procTitle = run.process_name || run.process_id || "Расчёт";
                    card.innerHTML = `
                        <div class="task-top">
                            <div>
                                <span class="badge badge-green">Выполняется</span>
                                <span class="badge ${initClass}" style="margin-left:4px;">${run.initiator || "USER"}</span>
                            </div>
                            <span class="task-id">#${runIdShort}</span>
                        </div>
                        <div class="task-title" style="margin-bottom:4px;">${procTitle}</div>
                        <div class="task-meta">Окно данных: <strong>${run.date_from || ""} .. ${run.date_to || ""}</strong></div>
                        <div class="progress-wrap">
                            <div class="progress-labels">
                                <span>${stageLabel(run.stage)}</span>
                                <strong>${run.progress || 0}%</strong>
                            </div>
                            <div class="progress-track">
                                <div class="progress-fill" style="width:${run.progress || 0}%"></div>
                            </div>
                        </div>
                        <div class="task-footer">
                            <span class="subscribers-list">Подписчики: ${subs || run.user_name || "—"}</span>
                            <button class="btn btn-danger" title="Проверка Watchdog TTL (NFR-02)">Имитировать сбой (TTL)</button>
                        </div>
                    `;
                    const crashBtn = card.querySelector("button");
                    if (crashBtn) {
                        crashBtn.addEventListener("click", () => {
                            fetch(`/api/demo/kill-worker/${run.run_id}`, { method: "POST" });
                        });
                    }
                    runningDiv.appendChild(card);
                });
            }
        }

        // 3. Queued List
        const queuedDiv = document.getElementById("queued-list");
        if (queuedDiv) {
            queuedDiv.innerHTML = "";
            if (queue.length === 0) {
                queuedDiv.innerHTML = `<div class="empty-state">Очередь пуста — конфликтов нет.</div>`;
            } else {
                queue.forEach((run, idx) => {
                    const item = document.createElement("div");
                    item.className = "task-card queued";
                    const initClass = run.initiator === "MACHINE" ? "badge-machine" : "badge-user";
                    const runIdShort = String(run.run_id || "").slice(0, 8);
                    const procTitle = run.process_name || run.process_id || "Расчёт";
                    item.innerHTML = `
                        <div class="task-top">
                            <div>
                                <span class="badge badge-amber">Позиция #${run.position || idx + 1}</span>
                                <span class="badge ${initClass}" style="margin-left:4px;">${run.initiator || "USER"}</span>
                            </div>
                            <span class="task-id">#${runIdShort}</span>
                        </div>
                        <div class="task-title" style="margin-bottom:4px;">${procTitle}</div>
                        <div class="task-meta">Дата: <strong>${run.target_date || ""}</strong> • Инициатор: ${run.user_name || "—"}</div>
                        <div class="task-reason">${run.blocked_reason || "Ожидание освобождения ресурсов"}</div>
                        <div class="task-footer">
                            <span class="task-meta" style="margin:0;">Таблицы БД не заблокированы</span>
                            <button class="btn btn-danger">Отменить</button>
                        </div>
                    `;
                    const cancelBtn = item.querySelector("button");
                    if (cancelBtn) {
                        cancelBtn.addEventListener("click", () => {
                            fetch(`/api/runs/${run.run_id}`, { method: "DELETE" });
                        });
                    }
                    queuedDiv.appendChild(item);
                });
            }
        }

        // 4. Active Locks Table
        const locksDiv = document.getElementById("active-locks");
        if (locksDiv) {
            if (locks.length === 0) {
                locksDiv.innerHTML = `<div class="empty-state">Активных блокировок нет.</div>`;
            } else {
                locksDiv.innerHTML = `
                    <div class="table-wrap">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>Владелец</th>
                                    <th>Ресурс / Таблица</th>
                                    <th>Режим</th>
                                    <th>Диапазон дат</th>
                                    <th style="text-align:right;">TTL истекает</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${locks
                                    .map((l) => {
                                        const mode = (l.access_mode || l.mode || "READ").toUpperCase();
                                        const modeBadge =
                                            mode === "WRITE"
                                                ? `<span class="badge badge-amber">WRITE</span>`
                                                : `<span class="badge badge-user">READ</span>`;
                                        const exp = l.expires_at ? String(l.expires_at).slice(11, 19) : "-";
                                        const runIdShort = String(l.run_id || "").slice(0, 8);
                                        return `
                                            <tr>
                                                <td class="mono" style="color:var(--blue);font-weight:700;">#${runIdShort}</td>
                                                <td class="mono"><strong>${l.resource_id}</strong></td>
                                                <td>${modeBadge}</td>
                                                <td class="mono">${l.date_from} .. ${l.date_to}</td>
                                                <td class="mono" style="text-align:right;color:var(--text-muted);">${exp}</td>
                                            </tr>
                                        `;
                                    })
                                    .join("")}
                            </tbody>
                        </table>
                    </div>
                `;
            }
        }

        // 5. History Table
        const histDiv = document.getElementById("history");
        if (histDiv) {
            if (history.length === 0) {
                histDiv.innerHTML = `<div class="empty-state">Завершённых расчётов пока нет.</div>`;
            } else {
                histDiv.innerHTML = `
                    <div class="table-wrap">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>ID</th>
                                    <th>Процесс</th>
                                    <th>Дата</th>
                                    <th>Статус</th>
                                    <th>Подписчики / Примечание</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${history
                                    .map((h) => {
                                        const stClass =
                                            h.status === "COMPLETED"
                                                ? "badge-green"
                                                : h.status === "FAILED"
                                                ? "badge-red"
                                                : "badge-amber";
                                        const subs = (h.subscribers || []).join(", ");
                                        const runIdShort = String(h.run_id || "").slice(0, 8);
                                        const procTitle = h.process_name || h.process_id || "Расчёт";
                                        return `
                                            <tr>
                                                <td class="mono">#${runIdShort}</td>
                                                <td><strong>${procTitle}</strong></td>
                                                <td class="mono">${h.target_date || "-"}</td>
                                                <td><span class="badge ${stClass}">${h.status}</span></td>
                                                <td>${subs || "—"}${h.blocked_reason ? ` • <span style="color:var(--red);">${h.blocked_reason}</span>` : ""}</td>
                                            </tr>
                                        `;
                                    })
                                    .join("")}
                            </tbody>
                        </table>
                    </div>
                `;
            }
        }

        // 6. Audit Logs Feed
        const auditDiv = document.getElementById("audit-logs");
        if (auditDiv) {
            if (logs.length === 0) {
                auditDiv.innerHTML = `<div class="empty-state">Записей аудита пока нет.</div>`;
            } else {
                auditDiv.innerHTML = logs
                    .map((a) => {
                        const ts = a.created_at ? String(a.created_at).slice(11, 19) : "--:--:--";
                        const evType = a.event_type || "INFO";
                        return `
                            <div class="audit-item">
                                <span class="audit-time">${ts}</span>
                                <span class="audit-type ${evType}">${evType}</span>
                                <span>${a.message || ""}</span>
                            </div>
                        `;
                    })
                    .join("");
            }
        }
    };

    // Initial load
    setDefaultDate();
    try {
        await loadProcesses();
    } catch (err) {
        console.error(err);
    }

    try {
        const res = await fetch("/api/state");
        if (res.ok) {
            renderState(await res.json());
        }
    } catch (err) {
        console.warn("Initial REST state error:", err);
    }

    if (dateInput) dateInput.addEventListener("change", refreshDryRun);
    if (processSelect) {
        processSelect.addEventListener("change", () => {
            renderProcessMeta();
            refreshDryRun();
        });
    }
    if (analystSelect) analystSelect.addEventListener("change", refreshDryRun);

    await refreshDryRun();
    connectWs();
});
