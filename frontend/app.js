// Клиентская логика Live-дашборда Risk Process Orchestrator MVP
window.addEventListener("load", async () => {
    const processSelect = document.getElementById("process-select");
    const dateInput = document.getElementById("target-date");
    const wsStatus = document.getElementById("ws-status");
    const timerIntervalInput = document.getElementById("timer-interval");
    const runBtn = document.getElementById("run-calculation");
    const cronBtn = document.getElementById("run-cron");
    const dedupBtn = document.getElementById("test-deduplication");
    const hiddenBtn = document.getElementById("hidden-conflict");
    const analystSelect = document.getElementById("analyst-select");
    const dryRunDiv = document.getElementById("dry-run");
    const saveIntervalBtn = document.getElementById("save-interval");

    let ws = null;
    let reconnectTimer = null;

    const setWsStatus = (text, color) => {
        wsStatus.textContent = text;
        wsStatus.style.color = color || "#f0f0f0";
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
            } catch (err) {
                // ignore
            }
            ws = null;
        }

        const protocol = location.protocol === "https:" ? "wss" : "ws";
        const socket = new WebSocket(`${protocol}://${location.host}/ws/events`);
        ws = socket;

        socket.onopen = () => {
            if (ws !== socket) return;
            setWsStatus("Подключён", "#4caf50");
            if (reconnectTimer) {
                clearTimeout(reconnectTimer);
                reconnectTimer = null;
            }
        };

        socket.onclose = () => {
            if (ws !== socket) return;
            setWsStatus("Переподключение...", "#ff9800");
            scheduleReconnect();
        };

        socket.onerror = () => {
            if (ws !== socket) return;
            setWsStatus("Ошибка связи", "#ff9800");
        };

        socket.onmessage = (e) => {
            if (ws !== socket) return;
            try {
                const msg = JSON.parse(e.data);
                const snapshot = msg ? (msg.data || msg.payload) : null;
                if (snapshot) {
                    renderState(snapshot);
                    refreshDryRun();
                }
            } catch (err) {
                console.error("Ошибка разбора WS-сообщения:", err);
            }
        };
    };

    const loadInitialState = async () => {
        try {
            const res = await fetch("/api/state");
            if (res.ok) {
                const snap = await res.json();
                renderState(snap);
            }
        } catch (err) {
            console.warn("Не удалось загрузить начальное состояние через REST:", err);
        }
    };

    const loadProcesses = async () => {
        const res = await fetch("/api/processes");
        if (!res.ok) throw new Error("Не удалось загрузить процессы");
        const list = await res.json();
        processSelect.innerHTML = "";
        list.forEach((p) => {
            const opt = document.createElement("option");
            opt.value = p.id;
            opt.textContent = p.name
                ? `${p.name} (${p.id}, окно ${p.window_days || 1} дн.)`
                : p.id;
            processSelect.appendChild(opt);
        });
    };

    const setToday = () => {
        dateInput.value = new Date().toISOString().split("T")[0];
    };

    const payloadFromForm = () => ({
        process_id: processSelect.value,
        target_date: dateInput.value,
        user_name: analystSelect.value,
        initiator: "USER",
    });

    const refreshDryRun = async () => {
        if (!processSelect.value || !dateInput.value) return;
        try {
            const res = await fetch("/api/runs/dry-run", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payloadFromForm()),
            });
            const data = await res.json();
            if (!res.ok) {
                dryRunDiv.innerHTML = `<strong>Ошибка предпроверки:</strong> ${data.detail || res.status}`;
                dryRunDiv.style.color = "#f44336";
                return;
            }
            const colors = {
                RUN_IMMEDIATELY: "#4caf50",
                DEDUPLICATE: "#29b6f6",
                QUEUE: "#ffb74d",
                WAIT_TIMER: "#ba68c8",
            };
            dryRunDiv.innerHTML = `
                <strong>Прогноз:</strong> ${data.prediction}<br>
                <strong>Окно данных:</strong> ${data.date_from} .. ${data.date_to}<br>
                <strong>Причина:</strong> ${data.reason}
            `;
            dryRunDiv.style.color = colors[data.prediction] || "#f0f0f0";
        } catch (err) {
            dryRunDiv.textContent = `Ошибка связи с сервером: ${err.message}`;
            dryRunDiv.style.color = "#f44336";
        }
    };

    runBtn.addEventListener("click", async () => {
        await fetch("/api/runs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payloadFromForm()),
        });
    });

    cronBtn.addEventListener("click", async () => {
        const params = new URLSearchParams({
            process_id: processSelect.value,
            target_date: dateInput.value,
        });
        await fetch(`/api/demo/trigger-cron?${params.toString()}`, { method: "POST" });
    });

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

    hiddenBtn.addEventListener("click", async () => {
        const payload = payloadFromForm();
        await fetch("/api/runs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                process_id: "credit_risk",
                target_date: "2026-11-20",
                user_name: payload.user_name,
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
                    user_name: payload.user_name,
                    initiator: "USER",
                }),
            });
        }, 300);
    });

    saveIntervalBtn.addEventListener("click", async () => {
        const val = parseInt(timerIntervalInput.value, 10);
        if (Number.isNaN(val) || val < 1) return;
        await fetch("/api/config", {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ machine_timer_interval_sec: val }),
        });
    });

    const renderState = (snap) => {
        if (
            snap.config &&
            snap.config.machine_timer_interval_sec &&
            document.activeElement !== timerIntervalInput
        ) {
            timerIntervalInput.value = snap.config.machine_timer_interval_sec;
        }

        const timerDiv = document.getElementById("waiting-timer");
        timerDiv.innerHTML = "";
        const waiting = snap.timer_waiting || [];
        if (waiting.length === 0) {
            timerDiv.textContent = "Нет отложенных регламентных задач.";
        } else {
            waiting.forEach((run) => {
                const item = document.createElement("div");
                item.className = "section";
                item.style.border = "1px solid #2196f3";
                item.style.padding = "8px";
                item.style.borderRadius = "6px";
                item.innerHTML = `
                    <div><span class="status-badge status-waiting">WAITING_TIMER</span> <strong>${run.process_name || run.process_id}</strong> (#${run.run_id.slice(0, 8)})</div>
                    <div>Дата: ${run.target_date || ""} | Причина: ${run.blocked_reason || "Ожидание таймера"}</div>
                    <button style="margin-top:6px;background:#e53935;">Отменить</button>
                `;
                item.querySelector("button").addEventListener("click", () => {
                    fetch(`/api/runs/${run.run_id}`, { method: "DELETE" });
                });
                timerDiv.appendChild(item);
            });
        }

        const runningDiv = document.getElementById("running-cards");
        runningDiv.innerHTML = "";
        const activeRuns = snap.active_runs || [];
        if (activeRuns.length === 0) {
            runningDiv.textContent = "Нет активных расчётов.";
        } else {
            activeRuns.forEach((run) => {
                const card = document.createElement("div");
                card.className = "section";
                card.style.border = "1px solid #4caf50";
                card.style.padding = "10px";
                card.style.borderRadius = "6px";
                const subs = (run.subscribers || []).join(", ");
                card.innerHTML = `
                    <div><span class="status-badge status-running">RUNNING</span> <strong>${run.process_name || run.process_id}</strong> (#${run.run_id.slice(0, 8)})</div>
                    <div>Окно: ${run.date_from || ""} .. ${run.date_to || ""} | Инициатор: ${run.initiator}</div>
                    <div>Этап: ${run.stage || "Старт"} (${run.progress}%)</div>
                    <div class="progress" style="margin:6px 0;"><div class="progress-bar" style="width:${run.progress}%"></div></div>
                    <div>Подписчики: ${subs || run.user_name}</div>
                    <button style="margin-top:6px;background:#e53935;">Убить воркер (Chaos TTL)</button>
                `;
                card.querySelector("button").addEventListener("click", () => {
                    fetch(`/api/demo/kill-worker/${run.run_id}`, { method: "POST" });
                });
                runningDiv.appendChild(card);
            });
        }

        const queuedDiv = document.getElementById("queued-list");
        queuedDiv.innerHTML = "";
        const queue = snap.queue || [];
        if (queue.length === 0) {
            queuedDiv.textContent = "Очередь пуста.";
        } else {
            queue.forEach((run, idx) => {
                const item = document.createElement("div");
                item.className = "section";
                item.style.border = "1px solid #ff9800";
                item.style.padding = "10px";
                item.style.borderRadius = "6px";
                item.innerHTML = `
                    <div><span class="status-badge status-queued">QUEUED #${run.position || idx + 1}</span> <strong>${run.process_name || run.process_id}</strong> (#${run.run_id.slice(0, 8)})</div>
                    <div>Инициатор: ${run.initiator} (${run.user_name}) | Дата: ${run.target_date || ""}</div>
                    <div>Причина: ${run.blocked_reason || "Ожидание ресурсов"}</div>
                    <button style="margin-top:6px;background:#e53935;">Отменить</button>
                `;
                item.querySelector("button").addEventListener("click", () => {
                    fetch(`/api/runs/${run.run_id}`, { method: "DELETE" });
                });
                queuedDiv.appendChild(item);
            });
        }

        const locksDiv = document.getElementById("active-locks");
        const locks = snap.active_locks || [];
        locksDiv.innerHTML = locks.length === 0
            ? "Нет активных блокировок."
            : `<pre style="white-space:pre-wrap;font-size:12px;">${locks
                  .map(
                      (l) =>
                          `#${(l.run_id || "").slice(0, 8)} | ${l.resource_id} [${l.access_mode}] ${l.date_from}..${l.date_to} (до ${l.expires_at})`
                  )
                  .join("\n")}</pre>`;

        const histDiv = document.getElementById("history");
        const history = snap.history || [];
        histDiv.innerHTML = history.length === 0
            ? "История пока пуста."
            : `<pre style="white-space:pre-wrap;font-size:12px;">${history
                  .map(
                      (h) =>
                          `[${h.status}] ${h.process_name || h.process_id} (${h.target_date}) #${h.run_id.slice(0, 8)} — подписчики: ${(h.subscribers || []).join(", ")}${h.blocked_reason ? " | " + h.blocked_reason : ""}`
                  )
                  .join("\n")}</pre>`;

        const auditDiv = document.getElementById("audit-logs");
        const logs = snap.audit_logs || [];
        auditDiv.innerHTML = logs.length === 0
            ? "Записей аудита пока нет."
            : `<pre style="white-space:pre-wrap;font-size:12px;">${logs
                  .map((a) => `${(a.created_at || "").slice(11, 19)} [${a.event_type}] ${a.message}`)
                  .join("\n")}</pre>`;
    };

    setToday();
    try {
        await loadProcesses();
    } catch (err) {
        console.error(err);
    }
    await loadInitialState();
    dateInput.addEventListener("change", refreshDryRun);
    processSelect.addEventListener("change", refreshDryRun);
    analystSelect.addEventListener("change", refreshDryRun);

    await refreshDryRun();
    connectWs();
});