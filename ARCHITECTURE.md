# АРХИТЕКТУРНО-ТЕХНИЧЕСКИЙ ПРОЕКТ (SDD)

## RiskFlow Orchestrator — MVP-модуль оркестрации параллельных процессов оценки рисков

---

## 1. Паспорт продукта и контекст задачи

### 1.1. Суть проблемы и цель продукта

В крупной финансовой/корпоративной системе процессы оценки различных видов рисков запускаются как вручную аналитиками, так и автоматически по регламентному расписанию. Из-за отсутствия единого слоя координации параллельные запуски приводят к взаимным блокировкам СУБД (Deadlocks), перезаписи общих промежуточных таблиц и чтению неконсистентных данных (Dirty Read).

**Цель MVP:** разработать клиент-серверный модуль-оркестратор с пользовательским интерфейсом, который обеспечивает бесконфликтное выполнение параллельных расчётов рисков за счёт приоритезации пользователей перед фоновыми задачами, дедупликации идентичных запусков, композитной блокировки ресурсов с учётом скользящих окон дат и изоляции промежуточных буферов.

### 1.2. Матрица трассируемости: от проблем (AS-IS) к архитектурным решениям (TO-BE)

| Проблема / Инсайт ИИ (AS-IS) | Суть сбоя | Архитектурное решение в MVP | Покрываемые требования |
| --- | --- | --- | --- |
| **BN-01** + Инсайт ИИ №1 (78% сбоев в 15-мин. окна после старта регламента) | Ручные запуски аналитиков накладываются на фоновые задачи планировщика. | **Шлюз приоритета + Таймер машины:** при наличии активных задач `USER` запуск `MACHINE` откладывается по таймеру на $T = 10\text{--}30$ сек. (настраивается в UI) с защитой от вечного голодания (`MAX_MACHINE_WAIT_SEC = 90` сек.). | `BR-03`, `FR-01`, `FR-02`, `FR-03` |
| **BN-01, BN-06** («Слепой» запуск дублей) | Несколько пользователей или Cron запускают один и тот же процесс за одну дату. | **Дедупликатор с повышением приоритета:** перехват дубля по паре `(Process_ID + Target_Date)`, подписка второго инициатора на текущий `run_id`, а при запросе `USER` поверх спящей задачи `MACHINE` — немедленный промоушен (`USER_PROMOTION`). | `UR-03`, `FR-04`, `FR-05` |
| **BN-04** + Инсайт ИИ №2 (Скрытые конфликты скользящих окон `target_date - (window_days - 1)`) | Расчёты за 20.11 (окно 14.11–20.11) и 19.11 (окно 19.11–19.11) пересекаются по историческому окну транзакций и вызывают Deadlock. | **Композитный Lock-менеджер:** проверка пересечений по ключу `(Resource_ID + Date_Range + Access_Mode)`, точечное окно `[target_date, target_date]` для выходных витрин `mart_*` в режиме `WRITE` и атомарный захват «всё или ничего». | `BR-01`, `FR-06`, `FR-07`, `FR-09`, `NFR-01` |
| **BN-04** + Инсайт ИИ №3 (~65% запусков — совместное чтение `READ + READ`) | Избыточная блокировка общих справочников и источников снижает скорость работы. | **Матрица совместимости:** `READ + READ` разрешается параллельно; в очередь (приоритетная FIFO без транзакций БД) ставятся только конфликты `READ + WRITE` и `WRITE + WRITE`. | `BR-04`, `FR-07`, `FR-08`, `FR-10` |
| **BN-02, BN-03, BN-05** (Dirty Read, затирание staging, коллизия витрин) | Гонка чтения/записи источников и очистка (`TRUNCATE`) общих временных таблиц. | **Изоляция воркеров:** чтение в `REPEATABLE READ` (MVCC), персональные таблицы `temp_risk_<run_id>`, финальный атомарный `MERGE` (`INSERT ... ON CONFLICT DO UPDATE`). | `BR-02`, `FR-11`, `FR-12`, `FR-13` |
| **BN-06** (Отсутствие видимости) | Аналитик не видит статус очереди и причину ожидания. | **Live-дашборд + Dry-Run:** WebSocket-стрим статусов ($< 1$ сек.), таймлайн/Гант, карта блокировок и предпроверка конфликтов до нажатия кнопки «Запустить расчёт риска». | `UR-01`, `UR-04`, `FR-14`–`FR-16`, `NFR-04` |
| Зависание при падении воркера | Аварийное завершение расчёта оставляет ресурсы заблокированными навсегда. | **Watchdog (TTL + Heartbeat):** фоновый контроль пульса воркеров (каждые 2 сек.) и автоснятие просроченных блокировок с продвижением очереди. | `NFR-02`, `NFR-06` |

---

## 2. Технологический стек и архитектура системы

### 2.1. Используемые технологии и обоснование

| Слой системы | Технология / Библиотека | Версия | Назначение и обоснование выбора |
| --- | --- | --- | --- |
| **Язык и рантайм** | Python | `3.11+` | Единый стек команды, высокая скорость разработки и нативная асинхронность (`asyncio`) для управления конкурентными воркерами. |
| **Веб-фреймворк и API** | FastAPI + Uvicorn | `0.115.0` / `0.30.0` | Встроенная поддержка REST API, WebSocket (`NFR-04`), Pydantic v2 валидации и автогенерация OpenAPI/Swagger. |
| **СУБД** | PostgreSQL | `16` | Реализация `Snapshot Isolation / MVCC` (`REPEATABLE READ`), создание изолированных буферов `temp_risk_<run_id>` и атомарный `MERGE` (`INSERT ... ON CONFLICT`). |
| **Драйвер БД** | `asyncpg` | `0.29.0` | Неблокирующий пул соединений (`min_size=10`, `max_size=30`); совместно с in-memory кэшем оркестратора обеспечивает время диспетчеризации $< 200$ мс при 50 параллельных запросах (`NFR-01`, `NFR-03`). |
| **Клиентский интерфейс** | HTML5 + CSS3 + Vanilla JS | ES2022 | Лёгкий одностраничный Live-дашборд без тяжёлых сборщиков (Webpack/Node.js), раздаётся напрямую через `StaticFiles` в FastAPI. |
| **Локальный клиент (Desktop)** | `pywebview` | опционально | Опциональный лаунчер (`desktop_app.py`), открывающий UI как нативное окно ОС; при запуске через Docker система доступна по `http://localhost:8000` (или `http://127.0.0.1:8000`). |
| **Инфраструктура** | Docker + Docker Compose | `v2` | Гарантирует воспроизводимое развёртывание PostgreSQL 16 (с `healthcheck`) и сервера одной командой на машине любого члена жюри. |
| **Тестирование** | `pytest` + `pytest-asyncio` + `httpx` | `8.3.2` / `0.23.8` / `0.27.0` | Автоматизированная проверка алгоритмов ядра, сценариев матрицы совместимости, таймера машины, Watchdog TTL и нагрузочный тест на 50 одновременных запросов. |

### 2.2. Компонентная схема (High-Level Architecture)

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                       КЛИЕНТСКИЙ СЛОЙ (Пул 1: UX/UI)                         │
│   [ Нативное окно pywebview (desktop_app.py) ]  ИЛИ  [ Браузер :8000 ]       │
│   • Форма запуска (USER / MACHINE) + Live Dry-Run предпроверка (FR-15)       │
│   • Live-дашборд: KPI / Таймлайн / Таймер / Активные / Очередь / Локи        │
│   • Панель демо-сценариев в 1 клик + Chaos-кнопка «Имитировать сбой (TTL)»   │
└──────────────────────────┬───────────────────────────────▲───────────────────┘
                           │ REST API (JSON)               │ WebSocket (/ws/events)
                           ▼                               │ (< 1 сек, NFR-04)
┌──────────────────────────────────────────────────────────────────────────────┐
│                  СЕРВЕР ОРКЕСТРАТОРА (Пул 2: FastAPI Backend)                │
│                                                                              │
│  ┌─────────────────────┐    ┌─────────────────────────────────────────────┐  │
│  │ Metadata Registry   │    │       CORE ENGINE (asyncio.Lock + Cache)    │  │
│  │ (processes.json,    │───►│ 1. Deduplicator + USER_PROMOTION            │  │
│  │  hot-reload mtime)  │    │ 2. Priority Gate (USER > MACHINE + Timer)   │  │
│  └─────────────────────┘    │ 3. Composite Lock Manager (Resource+Range)  │  │
│  ┌─────────────────────┐    │ 4. Priority FIFO Queue + Async DB Persist   │  │
│  │ Watchdog (TTL 2s) & │◄──►└──────────────────────┬──────────────────────┘  │
│  │ Starvation Guard    │                           │ Атомарный резерв (FR-09)│
│  └─────────────────────┘                           │                         │
└────────────────────────────────────────────────────┼─────────────────────────┘
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  ВЫЧИСЛИТЕЛЬНЫЙ СЛОЙ (Пул 3: Async Workers)                  │
│   [ Worker run_id_1 ]      [ Worker run_id_2 ]      [ Worker run_id_N ]      │
│   1. EXTRACT (REPEATABLE READ) -> 2. STAGING (temp_risk_<run_id>) ->         │
│   3. COMPUTE (Расчёт метрик)   -> 4. LOAD (Atomic MERGE) + Heartbeat (3s)    │
└────────────────────────────────────────────────────┬─────────────────────────┘
                                                     │ asyncpg pool (10..30)
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                        СЛОЙ ДАННЫХ (PostgreSQL 16)                           │
│  • Системные таблицы: runs, active_locks, audit_logs                         │
│  • Источники и витрины: src_transactions, ref_clients, mart_risk_results     │
│  • Временные буферы: temp_risk_<run_id> (создаются и удаляются на каждый run)│
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Детальная структура репозитория и спецификация модулей

### 3.1. Дерево файлов проекта

```text
risk-orchestrator-mvp/
├── docker-compose.yml               # Поднимает PostgreSQL 16 (с healthcheck) и сервис backend
├── Dockerfile                       # Образ Python 3.11-slim для бэкенда
├── requirements.txt                 # Зависимости Python (FastAPI, asyncpg, Pydantic, pytest)
├── pytest.ini                       # Конфигурация запуска тестов pytest
├── ARCHITECTURE.md                  # Полный архитектурно-технический проект (SDD)
├── README.md                        # Документация запуска и гайд по проверке для жюри
├── config/
│   └── processes.json               # Декларативный реестр процессов, ресурсов и скользящих окон
├── backend/
│   ├── __init__.py
│   ├── main.py                      # Инициализация FastAPI, lifespan (БД, сид, фон), раздача статики
│   ├── db/
│   │   ├── database.py              # Пул asyncpg, миграция DDL, сидирование 50 клиентов / 1500 транз.
│   │   └── models.py                # Pydantic v2 модели запросов, ответов и снимков состояния
│   ├── core/
│   │   ├── __init__.py
│   │   ├── registry.py              # Парсинг processes.json (mtime reload), окна и точечные витрины
│   │   ├── lock_manager.py          # Проверка пересечений (Resource + Date_Range + Mode) и TTL
│   │   ├── orchestrator.py          # Диспетчер (in-memory кэш, приоритеты, таймер, очередь, Watchdog)
│   │   └── worker.py                # Пайплайн расчёта (Extract -> Staging -> Compute -> Load)
│   └── api/
│       ├── routes.py                # REST-эндпоинты (/api/processes, /runs, /dry-run, /state, /demo/*)
│       └── ws.py                    # Менеджер WebSocket-подключений и рассылка STATE_UPDATE
├── frontend/
│   ├── index.html                   # Единый экран Live-дашборда, формы запуска и демо-сценариев
│   ├── styles.css                   # Стилизация интерфейса (тёмная и светлая темы, бренд #113369)
│   ├── app.js                       # Логика клиента: REST, Dry-Run, Гант, таймер и авто-реконнект WS
│   ├── logo.png                     # Логотип команды CodeLink
│   └── favicon.png / favicon.ico    # Иконки вкладки браузера
├── desktop_app.py                   # Опциональный запуск сервера + нативного окна через pywebview
└── tests/
    ├── conftest.py                  # Фикстуры pytest, автоопределение БД (Docker/localhost), очистка
    ├── test_compatibility.py        # 2 модульных + 7 интеграционных тестов (TC-01..TC-06 + соседние даты)
    └── test_load_50.py              # Нагрузочный тест на 50 параллельных запросов (NFR-01, NFR-03)
```

### 3.2. Спецификация программных модулей

| Модуль / Файл | Ответственность | Ключевые классы и функции | Требования |
| --- | --- | --- | --- |
| `config/processes.json` | Конфигурация бизнес-процессов, длительности, скользящих окон и карт доступа к ресурсам (`READ`/`WRITE`). | Параметры `machine_timer_interval_sec`, `lock_ttl_sec`, `heartbeat_interval_sec`, `processes.{id}.{window_days, duration_sec, resources}` | `UR-05`, `FR-06` |
| `backend/db/database.py` | Управление пулом `asyncpg` (`10..30`), идемпотентное создание 6 таблиц и индексов, генерация 50 синтетических клиентов и 1500 транзакций, запись аудита. | `get_pool()`, `close_pool()`, `init_db()`, `seed_synthetic_data()`, `log_audit_event()` | `FR-11`, `NFR-05`, `NFR-06` |
| `backend/db/models.py` | Строгая типизация данных между слоями и API (Pydantic v2 с нормализацией `UUID`, `initiator`, `access_mode`). | `RunCreateRequest`, `DryRunResponse`, `RunInfo`, `LockInfo`, `AuditLogEntry`, `SystemStateSnapshot`, `ConfigUpdateRequest` | `FR-04`, `FR-14`, `FR-15` |
| `backend/core/registry.py` | Ленивая загрузка `processes.json` с автоперезагрузкой по `mtime`, расчёт скользящих окон `window_days - 1` и точечных ключей для витрин `mart_*` (`WRITE`). | `ResourceLockSpec`, `ProcessRegistry.get_required_locks()`, `ProcessRegistry.get_date_range()` | `FR-04`, `FR-06` |
| `backend/core/lock_manager.py` | Проверка совместимости ключей по 3 формальным правилам; управление реестром `active_locks`. | `check_conflicts()`, `acquire_locks()`, `release_locks()`, `extend_ttl()` | `FR-06`, `FR-07`, `FR-09`, `NFR-02` |
| `backend/core/orchestrator.py` | Координация жизненного цикла заявок под единым `asyncio.Lock` с оперативным in-memory кэшем и фоновой персистентностью в БД. Реализует дедупликацию + `USER_PROMOTION`, приоритет `USER > MACHINE`, таймер с защитой от голодания, очередь FIFO и Watchdog. | `submit_run()`, `dry_run()`, `cancel_run()`, `complete_run()`, `process_queue()`, `_check_machine_timer()`, `_machine_timer_loop()`, `_watchdog_loop()`, `get_state_snapshot()`, `update_config()` | `FR-01`–`FR-10`, `FR-15`, `FR-16`, `NFR-01`–`NFR-03` |
| `backend/core/worker.py` | Выполнение расчёта в изолированной среде PostgreSQL (4 этапа) с отправкой прогресса и `Heartbeat`. Поддерживает множество `CRASHED_WORKERS` для Chaos-теста Watchdog TTL. | `execute_risk_job()`, `update_stage()`, `kill_worker()`, `_heartbeat_loop()` | `FR-11`–`FR-13`, `NFR-02` |
| `backend/api/routes.py` | HTTP-интерфейс для клиента, настройки таймингов и демонстрационных сценариев; хранит синглтоны `ws_manager` и `orchestrator`. | `GET /api/processes`, `POST /api/runs/dry-run`, `POST /api/runs`, `DELETE /api/runs/{id}`, `GET /api/state`, `PUT /api/config`, `POST /api/demo/*` | `FR-01`, `FR-14`–`FR-16` |
| `backend/api/ws.py` | Пул активных WebSocket-подключений и широковещательная рассылка события `STATE_UPDATE`. | `ConnectionManager.connect()`, `disconnect()`, `send_state_to_client()`, `broadcast_state()` | `NFR-04` |
| `desktop_app.py` | Опциональная точка входа для локального запуска без браузера: поднимает Uvicorn в фоновом потоке, ждёт порт `8000` и открывает нативное окно `pywebview`. | `_start_server()`, `_wait_for_port()`, `main()` | `UR-01` |

---

## 4. Модель данных, конфигурация и машина состояний

### 4.1. Конфигурация процессов (`config/processes.json`)

В конфиг заложены **4 эталонных процесса**, которые покрывают все сценарии матрицы совместимости, параллельный запуск за соседние даты и скрытый конфликт скользящих окон:

```json
{
  "machine_timer_interval_sec": 15,
  "lock_ttl_sec": 25,
  "heartbeat_interval_sec": 3,
  "processes": {
    "credit_risk": {
      "name": "Оценка кредитного риска",
      "description": "Расчёт PD/LGD по транзакциям за скользящее окно 7 дней",
      "window_days": 7,
      "duration_sec": 14,
      "resources": [
        { "resource_id": "src_transactions", "mode": "READ" },
        { "resource_id": "ref_clients", "mode": "READ" },
        { "resource_id": "mart_credit_risk", "mode": "WRITE" }
      ]
    },
    "counterparty_risk": {
      "name": "Оценка риска контрагентов",
      "description": "Анализ лимитов контрагентов (совместное чтение источников с кредитным риском)",
      "window_days": 7,
      "duration_sec": 14,
      "resources": [
        { "resource_id": "src_transactions", "mode": "READ" },
        { "resource_id": "ref_clients", "mode": "READ" },
        { "resource_id": "mart_counterparty_risk", "mode": "WRITE" }
      ]
    },
    "etl_transactions": {
      "name": "Обновление витрины транзакций (ETL)",
      "description": "Загрузка и корректировка транзакций за дату (пишет в src_transactions)",
      "window_days": 1,
      "duration_sec": 12,
      "resources": [
        { "resource_id": "src_transactions", "mode": "WRITE" }
      ]
    },
    "integrated_risk": {
      "name": "Сводный риск-отчёт (Агрегатор)",
      "description": "Читает готовые витрины кредитного риска и контрагентов",
      "window_days": 7,
      "duration_sec": 12,
      "resources": [
        { "resource_id": "mart_credit_risk", "mode": "READ" },
        { "resource_id": "mart_counterparty_risk", "mode": "READ" },
        { "resource_id": "mart_integrated_risk", "mode": "WRITE" }
      ]
    }
  }
}
```

Дополнительно в оркестраторе поддерживаются переменные окружения (переопределяющие значения из JSON): `DATABASE_URL`, `MACHINE_TIMER_INTERVAL_SEC` (по умолчанию `15`), `LOCK_TTL_SEC` (по умолчанию `25`), `HEARTBEAT_INTERVAL_SEC` (по умолчанию `3`) и `MAX_MACHINE_WAIT_SEC` (по умолчанию `90` сек. — порог защиты от голодания машины).

### 4.2. Схема таблиц PostgreSQL (DDL)

```sql
-- 1. Реестр всех запусков (активных, ждущих и завершённых)
CREATE TABLE IF NOT EXISTS runs (
    run_id          UUID PRIMARY KEY,
    process_id      VARCHAR(64) NOT NULL,
    process_name    VARCHAR(128) NOT NULL,
    target_date     DATE NOT NULL,
    date_from       DATE NOT NULL,          -- target_date - (window_days - 1)
    date_to         DATE NOT NULL,          -- target_date
    initiator       VARCHAR(16) NOT NULL,   -- 'USER' | 'MACHINE'
    user_name       VARCHAR(64) NOT NULL,
    status          VARCHAR(32) NOT NULL,   -- 'WAITING_TIMER' | 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED'
    stage           VARCHAR(32),            -- 'EXTRACT' | 'STAGING' | 'COMPUTE' | 'LOAD'
    progress        INT NOT NULL DEFAULT 0, -- 0..100
    subscribers     JSONB NOT NULL DEFAULT '[]'::jsonb,
    blocked_reason  TEXT,                   -- Человекочитаемая причина ожидания для UI (FR-14)
    lock_token      UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_dedup
    ON runs(process_id, target_date)
    WHERE status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING');
CREATE INDEX IF NOT EXISTS idx_runs_initiator_status ON runs(initiator, status);

-- 2. Таблица активных блокировок с поддержкой TTL (NFR-02)
CREATE TABLE IF NOT EXISTS active_locks (
    lock_id         SERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    resource_id     VARCHAR(64) NOT NULL,
    date_from       DATE NOT NULL,
    date_to         DATE NOT NULL,
    access_mode     VARCHAR(8) NOT NULL,    -- 'READ' | 'WRITE'
    expires_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_active_locks_resource ON active_locks(resource_id);
CREATE INDEX IF NOT EXISTS idx_active_locks_res_exp ON active_locks(resource_id, expires_at);

-- 3. Структурированный журнал событий для аудита и ИИ-анализа (NFR-06)
CREATE TABLE IF NOT EXISTS audit_logs (
    id              SERIAL PRIMARY KEY,
    run_id          UUID,
    event_type      VARCHAR(64) NOT NULL,   -- 'STARTED' | 'QUEUED' | 'TIMER_WAIT' | 'DEDUPLICATED' | 'USER_PROMOTION' | 'STARVATION_PROMOTION' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'TTL_EXPIRED' | 'WORKER_CRASH_SIMULATED'
    message         TEXT NOT NULL,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Синтетические предметные таблицы для работы воркеров (FR-11..FR-13)
CREATE TABLE IF NOT EXISTS ref_clients (
    client_id       INT PRIMARY KEY,
    segment         VARCHAR(32) NOT NULL,
    pd_base         NUMERIC(6, 4) NOT NULL
);

CREATE TABLE IF NOT EXISTS src_transactions (
    tx_id           SERIAL PRIMARY KEY,
    tx_date         DATE NOT NULL,
    client_id       INT NOT NULL REFERENCES ref_clients(client_id),
    amount          NUMERIC(14, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_src_tx_date ON src_transactions(tx_date);

CREATE TABLE IF NOT EXISTS mart_risk_results (
    process_id      VARCHAR(64) NOT NULL,
    target_date     DATE NOT NULL,
    total_exposure  NUMERIC(18, 2) NOT NULL,
    weighted_risk   NUMERIC(18, 4) NOT NULL,
    rows_processed  INT NOT NULL,
    last_run_id     UUID NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (process_id, target_date)
);
```

### 4.3. Машина состояний задачи (`runs.status`)

| Текущий статус | Событие / Условие перехода | Следующий статус | Действие оркестратора |
| --- | --- | --- | --- |
| *(Новая заявка)* | Найден активный/ждущий дубль `(process_id, target_date)` в `RUNNING` / `QUEUED` (или `WAITING_TIMER` от `MACHINE`) | *(Без создания)* | Добавляет `user_name` в `subscribers` существующей задачи (`DEDUPLICATED`, `action="ATTACHED"`, `FR-05`). |
| `WAITING_TIMER` (`MACHINE`) | Поступил дублирующий запрос от `USER` на те же `(process_id, target_date)` | `RUNNING` или `QUEUED` | Повышает приоритет задачи до `USER` (`USER_PROMOTION`), добавляет в `subscribers` и немедленно проверяет блокировки. |
| *(Новая заявка)* | `initiator == 'MACHINE'` И есть активные/ждущие задачи `USER` | `WAITING_TIMER` | Откладывает регламент до тика фонового таймера (`TIMER_WAIT`, `FR-03`). |
| *(Новая заявка)* | `LockManager` выявил конфликт по `(Resource + Date_Range + Mode)` | `QUEUED` | Постановка в приоритетную FIFO-очередь без транзакций БД (`FR-08`). |
| *(Новая заявка)* | Конфликтов нет (и приоритет соблюдён) | `RUNNING` | Атомарный резерв ключей в кэше и `active_locks`, старт воркера (`FR-09`). |
| `WAITING_TIMER` | Тик таймера (`_check_machine_timer`): счётчик `USER == 0` ИЛИ время ожидания $> 90$ сек. (`STARVATION_PROMOTION`) | `QUEUED` $\rightarrow$ `RUNNING` | Перевод в общую очередь и немедленный вызов `process_queue()`. |
| `QUEUED` | Освободились конфликтующие блокировки (`process_queue`) | `RUNNING` | Атомарный резерв ключей и старт воркера (`FR-10`). |
| `QUEUED` / `WAITING_TIMER` | Пользователь нажал «Отменить» (`DELETE /api/runs/{id}`) | `CANCELLED` | Снятие задачи из очереди/таймера без влияния на БД (`FR-16`). |
| `RUNNING` | Воркер завершил шаг `LOAD` (`MERGE`) | `COMPLETED` | Снятие блокировок в кэше и `active_locks`, вызов `process_queue()` (`FR-10`). |
| `RUNNING` | Исключение в воркере ИЛИ истёк `TTL` в Watchdog | `FAILED` | Принудительное снятие блокировок, запись `FAILED` / `TTL_EXPIRED` в `audit_logs`, вызов `process_queue()` (`NFR-02`). |

---

## 5. Алгоритмы ядра и бизнес-логика

### 5.1. Расчёт окон и формальное правило конфликта (`registry.py`, `lock_manager.py`)

Для процесса с глубиной скользящего окна `window_days` и целевой датой `target_date` реестр `ProcessRegistry` вычисляет диапазон:

$$D^{from} = \text{target\_date} - (\text{window\_days} - 1), \quad D^{to} = \text{target\_date}$$

* **Исключение для выходных витрин (`mart_*` в режиме `WRITE`):** для целевых витрин выставляется точечный интервал $[ \text{target\_date}, \text{target\_date} ]$, так как первичный ключ таблицы `mart_risk_results` равен `(process_id, target_date)`. Благодаря этому один и тот же процесс (например, `credit_risk`) может выполняться параллельно за соседние даты `2026-11-19` и `2026-11-20` без ложного конфликта `WRITE + WRITE`.
* Для всех `READ`-ресурсов и источников `src_*` (включая `WRITE` в `src_transactions`) используется полный интервал скользящего окна $[D^{from}, D^{to}]$.

Пусть новая задача запрашивает ключ $K_{req} = (R_1, [D_{1}^{from}, D_{1}^{to}], M_1)$, а в реестре активных блокировок уже существует ключ $K_{act} = (R_2, [D_{2}^{from}, D_{2}^{to}], M_2)$. Конфликт возникает **тогда и только тогда**, когда одновременно выполняются 3 условия:

1. **Совпадение ресурса:** $R_1 = R_2$
2. **Пересечение интервалов дат:** $D_{1}^{from} \le D_{2}^{to} \;\land\; D_{2}^{from} \le D_{1}^{to}$
3. **Несовместимость режимов доступа:** $\neg (M_1 = \text{'READ'} \;\land\; M_2 = \text{'READ'})$

Если хотя бы один из требуемых ключей задачи конфликтует с любым активным ключом, **ни один ключ не захватывается** (атомарность «всё или ничего» по `FR-09`), а задача уходит в `QUEUED` с указанием конкретного конфликтующего ресурса, интервала дат и короткого `run_id` владельца.

### 5.2. Диспетчеризация, очередь и защита от голодания (`orchestrator.py`)

* **Быстрое принятие решений ($< 200$ мс):** критическая секция в `submit_run()` и `dry_run()` работает под единым `asyncio.Lock` поверх оперативного in-memory кэша (`_mem_runs`, `_mem_locks`), а запись в PostgreSQL (`INSERT INTO runs`, `acquire_locks`, `audit_logs`) выполняется в фоновой задаче `_persist_run_in_db()` в рамках одной транзакции.
* **Приоритет очереди (`process_queue`, `FR-08`, `FR-10`):** при освобождении блокировок оркестратор сортирует все задачи со статусом `QUEUED` по кортежу:

```python
key = (0 if str(row["initiator"]).upper() == "USER" else 1, row["created_at"])
```

Задачи с непересекающимися ресурсами могут обгонять стоящую впереди заблокированную задачу (отсутствие Head-of-Line Blocking). При этом задачи `MACHINE` из очереди не стартуют, пока в системе остаются активные или ожидающие задачи `USER`.

* **Таймер машины и защита от голодания (`_machine_timer_loop`):** цикл пробуждается каждые `0.5` сек., мгновенно реагирует на динамическое изменение `machine_timer_interval_sec` через UI и публикует метку следующего тика `next_timer_check_at`. На каждом тике `_check_machine_timer()`:
  1. Принудительно переводит в `QUEUED` задачи `WAITING_TIMER`, ожидающие дольше `MAX_MACHINE_WAIT_SEC` (90 сек.), фиксируя событие `STARVATION_PROMOTION`.
  2. Если активных и ждущих в очереди задач `USER` равно `0`, планово переводит все оставшиеся `WAITING_TIMER` в `QUEUED` и запускает `process_queue()`.

### 5.3. Жизненный цикл вычислительного воркера (`worker.py`)

```python
async def execute_risk_job(run_id: UUID, orchestrator: Any, ws_manager: Any = None) -> None:
    heartbeat_task = asyncio.create_task(_heartbeat_loop(run_id, orchestrator))
    temp_table = f"temp_risk_{run_uuid.hex}"
    crashed = False
    try:
        # Шаг 1: EXTRACT в режиме Snapshot Isolation / MVCC (FR-11)
        async with pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read"):
                rows = await conn.fetch(
                    "SELECT t.tx_date, t.amount, c.pd_base FROM src_transactions t "
                    "JOIN ref_clients c ON t.client_id = c.client_id "
                    "WHERE t.tx_date BETWEEN $1 AND $2",
                    date_from, date_to,
                )
        # Соединение пула и транзакция закрыты ДО паузы; пауза отслеживает имитацию сбоя
        await update_stage(run_id, "EXTRACT", 20, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return  # Не вызываем complete_run: блокировки снимет Watchdog по TTL (NFR-02)

        # Шаг 2: STAGING в персональной буферной таблице (FR-12)
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {temp_table} "
                f"(amount NUMERIC(14, 2), weighted NUMERIC(18, 4))"
            )
            await conn.executemany(f"INSERT INTO {temp_table} (amount, weighted) VALUES ($1, $2)", data)
        await update_stage(run_id, "STAGING", 50, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        # Шаг 3: COMPUTE — агрегация метрик риска
        async with pool.acquire() as conn:
            compute_row = await conn.fetchrow(
                f"SELECT COUNT(*)::INT AS rows_processed, COALESCE(SUM(amount), 0) AS total_exposure, "
                f"COALESCE(SUM(weighted), 0) AS weighted_risk FROM {temp_table}"
            )
        await update_stage(run_id, "COMPUTE", 80, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        # Шаг 4: LOAD — атомарный MERGE в целевую витрину (FR-13)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO mart_risk_results (
                    process_id, target_date, total_exposure,
                    weighted_risk, rows_processed, last_run_id, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (process_id, target_date) DO UPDATE SET
                    total_exposure = EXCLUDED.total_exposure,
                    weighted_risk  = EXCLUDED.weighted_risk,
                    rows_processed = EXCLUDED.rows_processed,
                    last_run_id    = EXCLUDED.last_run_id,
                    updated_at     = NOW()
                """,
                process_id, target_date, compute_row["total_exposure"],
                compute_row["weighted_risk"], compute_row["rows_processed"], run_id,
            )
        await update_stage(run_id, "LOAD", 95, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        await orchestrator.complete_run(run_id, status="COMPLETED")
    except Exception as exc:
        if not crashed and not _is_crashed(run_id):
            await orchestrator.complete_run(run_id, status="FAILED", error=str(exc))
    finally:
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        CRASHED_WORKERS.discard(run_id)
        async with pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
```

---

## 6. Контракт REST API, WebSocket и требования к UI

### 6.1. REST API Endpoints

| Метод и URL | Описание | Формат Request / Response (JSON) |
| --- | --- | --- |
| `GET /api/processes` | Справочник доступных процессов из `config/processes.json` | **Res `200`:** `[{"id": "credit_risk", "name": "Оценка кредитного риска", "description": "...", "window_days": 7, "duration_sec": 14, "resources": [...]}]` |
| `POST /api/runs/dry-run` | Предпроверка конфликтов до запуска (`FR-15`) | **Req:** `{"process_id": "etl_transactions", "target_date": "2026-11-19", "initiator": "USER", "user_name": "Аналитик 1"}`<br><br>**Res `200`:** `{"prediction": "QUEUE", "date_from": "2026-11-19", "date_to": "2026-11-19", "reason": "Конфликт WRITE/READ по ресурсу src_transactions (2026-11-14..2026-11-20) с запуском #8f14c2", "required_locks": [...]}`<br><br>*(Значения `prediction`: `RUN_IMMEDIATELY`, `DEDUPLICATE`, `QUEUE`, `WAIT_TIMER`)* |
| `POST /api/runs` | Создание запуска или подключение к дублю (`FR-01`..`FR-09`) | **Req:** `{"process_id": "credit_risk", "target_date": "2026-11-20", "initiator": "USER", "user_name": "Аналитик 1"}`<br><br>**Res `200`:** `{"action": "CREATED" \| "ATTACHED", "run": { ...объект RunInfo... }}` |
| `DELETE /api/runs/{run_id}` | Отмена задачи из очереди или таймера (`FR-16`) | **Res `200`:** `{"status": "CANCELLED", "run_id": "..."}`<br>**Res `404`:** запуск не найден<br>**Res `409`:** попытка отменить задачу не в `QUEUED`/`WAITING_TIMER` |
| `GET /api/state` | Полный снимок состояния дашборда (`FR-14`) | **Res `200`:** `{"active_runs": [...], "queue": [...], "timer_waiting": [...], "active_locks": [...], "history": [...], "audit_logs": [...], "config": {"machine_timer_interval_sec": 15, "lock_ttl_sec": 25, "heartbeat_interval_sec": 3, "max_machine_wait_sec": 90, "next_timer_check_at": "..."}}` |
| `PUT /api/config` | Динамическое изменение таймингов оркестратора (`UR-05`, `FR-03`) | **Req:** `{"machine_timer_interval_sec": 10, "lock_ttl_sec": 25, "heartbeat_interval_sec": 3}` *(все поля опциональны)*<br><br>**Res `200`:** актуальный объект `config` (5 полей, включая обновлённый `next_timer_check_at`) |
| `POST /api/demo/trigger-cron` | **Демо:** запуск регламента `MACHINE` | **Query / JSON (опционально):** `process_id`, `target_date`. Если пара не указана или уже активна/ожидает, сервер автоматически подбирает свободную комбинацию.<br><br>**Res `200`:** `{"action": "CREATED" \| "ATTACHED", "run": { ...RunInfo с initiator="MACHINE"... }}` |
| `POST /api/demo/reset` | **Демо:** очистка завершённой истории и журнала аудита | Удаляет завершённые задачи (`COMPLETED`, `FAILED`, `CANCELLED`) и очищает `audit_logs` (не затрагивая активные и ждущие запуски).<br><br>**Res `200`:** `{"status": "RESET", "message": "История завершённых задач и журнал аудита очищены"}` |
| `POST /api/demo/kill-worker/{run_id}` | **Демо:** эмуляция аварийного падения воркера (`NFR-02`) | Останавливает Heartbeat и имитирует зависание воркера до срабатывания Watchdog TTL.<br><br>**Res `200`:** `{"status": "HEARTBEAT_STOPPED", "ttl_remaining_sec": 25, "run_id": "..."}` |

### 6.2. WebSocket протокол (`/ws/events`)

При подключении клиента и при любом изменении состояния задачи, прогресса шага или таблицы блокировок сервер рассылает всем подключённым клиентам JSON-сообщение (`NFR-04` — задержка $< 1$ сек.):

```json
{
  "type": "STATE_UPDATE",
  "event": "STATE_UPDATE",
  "timestamp": "2026-11-22T12:30:05.120Z",
  "data": { "...полный объект SystemStateSnapshot из GET /api/state..." },
  "payload": { "...дублирует data для обратной совместимости..." }
}
```

### 6.3. Экраны и виджеты клиентского интерфейса (`frontend/`)

1. **Верхняя панель (Topbar) и KPI-лента:** настройка интервала таймера машины с живым обратным отсчётом (`Через N с`), переключатель тёмной/светлой темы, индикатор WebSocket и 5 счётчиков (`Выполняется сейчас`, `В очереди`, `Ожидание таймера`, `Активных блокировок`, `Завершено / Дедупл.`).
2. **Панель запуска и живой Dry-Run (`FR-15`):** выбор инициатора (`Аналитик 1`, `Аналитик 2`, `Риск-менеджер` или `Планировщик (Cron / MACHINE)` с динамическим бейджем приоритета), выбор процесса с тегами ресурсов `READ`/`WRITE`, выбор целевой даты и карточка предпроверки (`RUN_IMMEDIATELY` — зелёный, `DEDUPLICATE` — голубой, `QUEUE` — янтарный, `WAIT_TIMER` — фиолетовый).
3. **Панель демо-сценариев в 1 клик:**
   * `1. Один процесс за разные даты (19 и 20 ноя)` — параллельный старт с точечным окном витрины `mart_credit_risk`;
   * `2. Дублирующий запуск (2 аналитика подряд)` — дедупликация с подключением второго подписчика;
   * `3. Скрытый конфликт окна (20.11 + 19.11 ETL)` — постановка ETL в очередь по пересечению 7-дневного окна;
   * `4. Регламентный запуск машины (Cron)` — переход в `WAITING_TIMER` при активных задачах пользователей.
4. **Live-Таймлайн / Гант:** наглядная шкала параллельных расчётов, очереди Lock-менеджера и ожидания таймера машины.
5. **Блок «Таймер ожидания машины» (`FR-03`):** карточки задач `WAITING_TIMER` с интервалом опроса и кнопкой «Отменить».
6. **Блоки «Активные расчёты» и «Очередь ожидания»:** карточки `RUNNING` с прогресс-баром, текущим этапом (`EXTRACT` $\rightarrow$ `STAGING` $\rightarrow$ `COMPUTE` $\rightarrow$ `LOAD`), списком подписчиков и кнопкой «Имитировать сбой (TTL)»; карточки `QUEUED` с позицией, причиной `blocked_reason` и кнопкой «Отменить» (`FR-16`).
7. **Карта блокировок (`active_locks`), История и Журнал аудита (`audit_logs`):** таблица текущих композитных ключей с временем истечения TTL и вкладки «История запусков» / «Журнал аудита оркестратора» с кнопкой «Очистить историю».

---

## 7. План тестирования и критерии готовности (QA & Demo)

### 7.1. Модульные и интеграционные автотесты (`tests/test_compatibility.py`)

Всего в `tests/test_compatibility.py` реализовано **9 автотестов** (2 автономных модульных и 7 интеграционных с PostgreSQL):

| ID / Имя теста | Сценарий | Входные действия | Ожидаемый результат |
| --- | --- | --- | --- |
| **Unit 1** (`test_registry_sliding_windows`) | Расчёт окон и точечных витрин | Запрос `get_required_locks` для `credit_risk` (`20.11`) и `etl_transactions` (`19.11`). | `src_transactions` получает `14.11..20.11` (`READ`), `mart_credit_risk` — точечно `20.11..20.11` (`WRITE`), `etl_transactions` — `19.11..19.11` (`WRITE`). |
| **Unit 2** (`test_lock_manager_intersection_logic`) | Правило пересечения интервалов | Проверка `_intersects` на пересекающихся, граничных и непересекающихся датах. | `[14.11..20.11]` и `[19.11..19.11]` $\rightarrow$ `True`; соседние витрины `[19.11]` и `[20.11]` $\rightarrow$ `False`. |
| **TC-01** (`test_tc01_deduplication`) | Дедупликация (`FR-05`) | Запустить `credit_risk` на `2026-11-20` от «Аналитик 1», затем сразу от «Аналитик 2». | Создан ровно **1** воркер; второй запрос получил `action="ATTACHED"` и попал в `subscribers`. |
| **TC-02** (`test_tc02_buffer_isolation`) | Изоляция буферов (`FR-12`) | Запустить `credit_risk` на `2026-11-10` и `2026-11-20` одновременно. | Оба выполняются параллельно (`RUNNING`) с разными `run_id` и изолированными таблицами `temp_risk_<run_id>`. |
| **TC-02b** (`test_parallel_credit_risk_adjacent_dates`) | Параллельный запуск за соседние даты | Запустить `credit_risk` на `2026-11-19` и `2026-11-20` одновременно. | Благодаря точечному окну `mart_credit_risk` (`WRITE`) и `READ + READ` по источникам оба немедленно переходят в `RUNNING`. |
| **TC-03** (`test_tc03_concurrent_read_read`) | Совместное чтение `READ + READ` (`FR-07`) | Запустить `credit_risk` и `counterparty_risk` на `2026-11-20` одновременно. | Оба выполняются параллельно без взаимной блокировки общих `src_transactions` и `ref_clients`. |
| **TC-04** (`test_tc04_sliding_window_conflict`) | Скрытый конфликт скользящего окна (Инсайт ИИ №2) | Запустить `credit_risk` на `2026-11-20` (`14.11–20.11`, `READ`), проверить `dry-run` и запустить `etl_transactions` на `2026-11-19` (`19.11–19.11`, `WRITE`). | `dry-run` прогнозирует `QUEUE`; реальный запуск `etl_transactions` встаёт в `QUEUED` с указанием конфликта по `src_transactions`. |
| **TC-05** (`test_tc05_user_priority_over_machine`) | Приоритет «Люди vs Машина» + `USER_PROMOTION` (`FR-03`) | При активном `USER`-расчёте вызвать `POST /api/demo/trigger-cron`, затем запросить ту же задачу от `USER`. | Задача машины встаёт в `WAITING_TIMER`; последующий запрос аналитика получает `action="ATTACHED"`, повышает инициатора до `USER` и переводит задачу в `RUNNING`. |
| **TC-06** (`test_tc06_watchdog_ttl`) | Отказоустойчивость TTL Watchdog (`NFR-02`) | Установить `lock_ttl_sec=5`, запустить `credit_risk`, поставить в очередь `etl_transactions`, вызвать `POST /api/demo/kill-worker/{run_id}`. | Watchdog переводит зависшую задачу в `FAILED`, освобождает её блокировки и автоматически запускает ожидающую задачу из очереди. |

### 7.2. Нагрузочный тест (`tests/test_load_50.py`)

* **Методика:** одновременная отправка **50 асинхронных запросов** (`asyncio.gather`) со случайной воспроизводимой комбинацией (`seed=42`) из 4 процессов, 6 дат (`2026-11-15`..`2026-11-20`) и инициаторов (`USER` / `MACHINE`).
* **Проверяемые критерии прохождения (`NFR-01`, `NFR-03`):**
  * Количество взаимных блокировок СУБД (Deadlocks): **0 (0%)**;
  * Количество HTTP-ошибок и исключений (`5xx`): **0**;
  * Среднее и 95-перцентиль времени диспетчеризации оркестратором: **$< 200$ мс**;
  * Все принятые заявки корректно отражаются в снимке состояния `GET /api/state`.