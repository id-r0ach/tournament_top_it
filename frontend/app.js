// JavaScript для Live‑панели
// Загрузка списка процессов, даты, состояния, WebSocket и обработка UI
window.addEventListener("load", () => {
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

    let ws;
    let reconnectTimer;

    const setWsStatus = (text) => wsStatus.textContent = text;

    const connectWs = () => {
        ws = new WebSocket(`ws://${location.host}/ws/events`);
        ws.addEventListener("open", () => {
            setWsStatus("Подключено");
            if (reconnectTimer) clearInterval(reconnectTimer);
        });
        ws.addEventListener("close", () => {
            setWsStatus("Переподключение");
            reconnectTimer = setInterval(connectWs, 2000);
        });
        ws.addEventListener("message", (e) => {
            const event = JSON.parse(e.data);
            renderState(event.payload);
        });
    };

    // --- UI actions ----------------------------------------
    const loadProcesses = async () => {
        const res = await fetch("/api/processes");
        const list = await res.json();
        processSelect.innerHTML = '';
        list.forEach(p => {
            const opt = document.createElement("option");
            opt.value = p.id;
            opt.textContent = p.id;
            processSelect.appendChild(opt);
        });
    };
    const setToday = () => {
        const today = new Date().toISOString().split("T")[0];
        dateInput.value = today;
    };
    const payloadFromForm = () => ({
        process_id: processSelect.value,
        target_date: dateInput.value,
        user_name: analystSelect.value,
        initiator: "USER"
    });

    const refreshDryRun = async () => {
        const payload = payloadFromForm();
        const res = await fetch("/api/runs/dry-run", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload)
        });
        const data = await res.json();
        dryRunDiv.innerHTML = `
            <strong>Прогноз:</strong> ${data.prediction}<br>
            <strong>Дата начала:</strong> ${data.date_from}<br>
            <strong>Дата окончания:</strong> ${data.date_to}<br>
            <strong>Причина:</strong> ${data.reason}
        `;
        dryRunDiv.style.color = {
            RUN_IMMEDIATELY: "green",
            DEDUPLICATE: "orange",
            QUEUE: "blue",
            WAIT_TIMER: "red"
        }[data.prediction] || "black";
    };

    runBtn.addEventListener("click", async () => {
        await fetch("/api/runs", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payloadFromForm())
        });
    });

    cronBtn.addEventListener("click", async () => {
        const params = new URLSearchParams({
            process_id: processSelect.value,
            target_date: dateInput.value
        });
        await fetch(`/api/demo/trigger-cron?${params.toString()}`, {method: "POST"});
    });

    dedupBtn.addEventListener("click", async () => {
        const base = payloadFromForm();
        const promises = [
            fetch("/api/runs", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ ...base, user_name: "Аналитик 1" })
            }),
            fetch("/api/runs", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ ...base, user_name: "Аналитик 2" })
            })
        ];
        await Promise.all(promises);
    });

    hiddenBtn.addEventListener("click", async () => {
        const payload = payloadFromForm();
        await fetch("/api/runs", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({
                process_id: "credit_risk",
                target_date: "2026-11-20",
                user_name: payload.user_name,
                initiator: "USER"
            })
        });
        setTimeout(async () => {
            await fetch("/api/runs", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify({
                    process_id: "etl_transactions",
                    target_date: "2026-11-19",
                    user_name: payload.user_name,
                    initiator: "USER"
                })
            });
        }, 300);
    });

    document.getElementById("save-interval").addEventListener("click", async () => {
        const val = parseInt(timerIntervalInput.value, 10);
        await fetch("/api/config", {
            method: "PUT",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ machine_timer_interval_sec: val })
        });
    });

    // --- Render state -------------------------------------
    const renderState = (snap) => {
        // Running
        const runningDiv = document.getElementById("running-cards");
        runningDiv.innerHTML = '';
        snap.active_runs.forEach(run => {
            const card = document.createElement("div");
            card.className = "run-card";
            card.innerHTML = `
                <div><strong>RUNNING ${run.run_id}</strong></div>
                <div>Прогресс: ${run.progress}%</div>
                <div>Этап: ${run.stage}</div>
                <div>Подписчики: ${run.subscribers.join(', ')}</div>
                <button data-id="${run.run_id}">Убить воркер</button>
            `;
            card.querySelector("button").addEventListener("click", () => {
                fetch(`/api/demo/kill-worker/${run.run_id}`, {method: "POST"});
            });
            runningDiv.appendChild(card);
        });
        // Queued
        const queuedDiv = document.getElementById("queued-list");
        queuedDiv.innerHTML = '';
        snap.queue.forEach(run => {
            const item = document.createElement("div");
            item.className = "queued-item";
            item.innerHTML = `
                <div><strong>QUEUED ${run.run_id}</strong></div>
                <div>Позиция: ${run.position}</div>
                <div>Инициатор: ${run.initiator}</div>
                <div>Причина: ${run.blocked_reason}</div>
                <button data-id="${run.run_id}">Отменить</button>
            `;
            item.querySelector("button").addEventListener("click", () => {
                fetch(`/api/runs/${run.run_id}`, { method: "DELETE" });
            });
            queuedDiv.appendChild(item);
        });
        // Waiting timer
        const timerDiv = document.getElementById("waiting-timer");
        timerDiv.innerHTML = JSON.stringify(snap.timer_waiting, null, 2);
        // Active locks
        const locksDiv = document.getElementById("active-locks");
        locksDiv.innerHTML = JSON.stringify(snap.active_locks, null, 2);
        // History
        const histDiv = document.getElementById("history");
        histDiv.innerHTML = JSON.stringify(snap.history, null, 2);
        // Audit logs
        const auditDiv = document.getElementById("audit-logs");
        auditDiv.innerHTML = JSON.stringify(snap.audit_logs, null, 2);
    };

    // --- Init ---------------------------------------------
    loadProcesses();
    setToday();
    connectWs();
    dateInput.addEventListener("change", refreshDryRun);
    processSelect.addEventListener("change", refreshDryRun);
    analystSelect.addEventListener("change", refreshDryRun);
    // Initial dry-run
    refreshDryRun();
});
