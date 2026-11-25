# АРХИТЕКТУРНО-ТЕХНИЧЕСКИЙ ПРОЕКТ (SDD)


## MVP-модуль оркестрации параллельных процессов оценки рисков

---

## 1. Паспорт продукта и контекст задачи

### 1.1. Суть проблемы и цель продукта

В крупной финансовой/корпоративной системе процессы оценки различных видов рисков запускаются как вручную аналитиками, так и автоматически по регламентному расписанию. Из-за отсутствия единого слоя координации параллельные запуски приводят к взаимным блокировкам СУБД (Deadlocks), перезаписи общих промежуточных таблиц и чтению неконсистентных данных (Dirty Read).

**Цель MVP:** разработать клиент-серверный модуль-оркестратор с пользовательским интерфейсом, который обеспечивает бесконфликтное выполнение параллельных расчётов рисков за счёт приоритезации пользователей перед фоновыми задачами, дедупликации идентичных запусков, композитной блокировки ресурсов с учётом скользящих окон дат и изоляции промежуточных буферов.


### 1.2. Матрица трассируемости: от проблем (AS-IS) к архитектурным решениям (TO-BE)

| Проблема / Инсайт ИИ (AS-IS) | Суть сбоя | Архитектурное решение в MVP | Покрываемые требования |
| --- | --- | --- | --- |
| **BN-01** + Инсайт ИИ №1 (78% сбоев в 15-мин. окна после старта регламента) | Ручные запуски аналитиков накладываются на фоновые задачи планировщика. | **Шлюз приоритета + Таймер машины:** при наличии активных задач `USER` запуск `MACHINE` откладывается по таймеру на $T = 10\text{–}30$ сек. | `BR-03`, `FR-01`, `FR-02`, `FR-03` |
| **BN-01, BN-06** («Слепой» запуск дублей) | Несколько пользователей или Cron запускают один и тот же процесс за одну дату. | **Дедупликатор:** перехват дубля по паре `(Process_ID + Target_Date)` и подписка второго пользователя на текущий `run_id`. | `UR-03`, `FR-04`, `FR-05` |
| **BN-04** + Инсайт ИИ №2 (Скрытые конфликты скользящих окон `target_date - (window_days - 1)`) | Расчёты за 20.11 (окно 14.11–20.11) и 19.11 (окно 19.11–19.11) пересекаются по историческому окну транзакций и вызывают Deadlock. | **Композитный Lock-менеджер:** проверка пересечений по ключу `(Resource_ID + Date_Range + Access_Mode)` и атомарный захват «всё или ничего». | `BR-01`, `FR-06`, `FR-07`, `FR-09`, `NFR-01` |
| **BN-04** + Инсайт ИИ №3 (~65% запусков — совместное чтение `READ + READ`) | Избыточная блокировка общих справочников и источников снижает скорость работы. | **Матрица совместимости:** `READ + READ` разрешается параллельно; в очередь (FIFO без транзакций БД) ставятся только `READ+WRITE` и `WRITE+WRITE`. | `BR-04`, `FR-07`, `FR-08`, `FR-10` |
| **BN-02, BN-03, BN-05** (Dirty Read, затирание staging, коллизия витрин) | Гонка чтения/записи источников и очистка (`TRUNCATE`) общих временных таблиц. | **Изоляция воркеров:** чтение в `REPEATABLE READ` (MVCC), персональные таблицы `temp_risk_<run_id>`, финальный атомарный `MERGE`. | `BR-02`, `FR-11`, `FR-12`, `FR-13` |
| **BN-06** (Отсутствие видимости) | Аналитик не видит статус очереди и причину ожидания. | **Live-дашборд + Dry-Run:** WebSocket-стрим статусов ($<1$ сек.) и предпроверка конфликтов до нажатия кнопки «Запустить». | `UR-01`, `UR-04`, `FR-14`–`FR-16`, `NFR-04` |
| Зависание при падении воркера | Аварийное завершение расчёта оставляет ресурсы заблокированными навсегда. | **Watchdog (TTL + Heartbeat):** фоновый контроль пульса воркеров и автоснятие просроченных блокировок. | `NFR-02`, `NFR-06` |

---

## 2. Технологический стек и архитектура системы

### 2.1. Используемые технологии и обоснование

| Слой системы | Технология / Библиотека | Версия | Назначение и обоснование выбора |
| --- | --- | --- | --- |
| **Язык и рантайм** | Python | `3.11+` | Единый стек команды, высокая скорость разработки и нативная асинхронность (`asyncio`) для управления конкурентными воркерами. |
| **Веб-фреймворк и API** | FastAPI + Uvicorn | `0.115+` | Встроенная поддержка REST API, WebSocket (`NFR-04`), Pydantic-валидации и автогенерация OpenAPI/Swagger для синхронизации с фронтендом. |
| **СУБД** | PostgreSQL | `16` | Позволяет честно реализовать `Snapshot Isolation / MVCC` (`REPEATABLE READ`), создание изолированных буферов `temp_risk_<run_id>` и атомарный `MERGE` (`INSERT ... ON CONFLICT`). |
| **Драйвер БД** | `asyncpg` | `0.29+` | Неблокирующий высокопроизводительный драйвер PostgreSQL; обеспечивает время диспетчеризации $< 200$ мс при 50 параллельных запросах (`NFR-01`, `NFR-03`). |
| **Клиентский интерфейс** | HTML5 + CSS3 + Vanilla JS | ES2022 | Лёгкий одностраничный дашборд без тяжелых сборщиков (Webpack/Node.js), мгновенно отдаётся самим FastAPI. |
| **Локальный клиент (Desktop)** | `pywebview` | `5.0+` | Опциональный лаунчер (`desktop_app.py`), открывающий UI как нативное окно ОС без браузера; при запуске через Docker система точно так же доступна по `http://localhost:8000`. |
| **Инфраструктура** | Docker + Docker Compose | `v2` | Гарантирует воспроизводимое развёртывание PostgreSQL и сервера одной командой на машине любого члена жюри. |
| **Тестирование** | `pytest` + `pytest-asyncio` + `httpx` | `8.0+` | Автоматизированная проверка всех 4 сценариев матрицы совместимости и нагрузочный тест на 50 одновременных запросов. |

### 2.2. Компонентная схема (High-Level Architecture)

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                       КЛИЕНТСКИЙ СЛОЙ (Пул 1: UX/UI)                         │
│   [ Нативное окно pywebview (desktop_app.py) ]  ИЛИ  [ Браузер :8000 ]       │
│   • Форма запуска + Dry-Run предпроверка (FR-15)                             │
│   • Live-дашборд: Активные / Очередь / Таймер машины / Локи (FR-14, FR-16)   │
│   • Панель демо-сценариев для жюри (Trigger Cron, Chaos Kill Worker)         │
└──────────────────────────┬───────────────────────────────▲───────────────────┘
                           │ REST API (JSON)               │ WebSocket (/ws/events)
                           ▼                               │ (< 1 сек, NFR-04)
┌──────────────────────────────────────────────────────────────────────────────┐
│                  СЕРВЕР ОРКЕСТРАТОРА (Пул 2: FastAPI Backend)                │
│                                                                              │
│  ┌─────────────────────┐    ┌─────────────────────────────────────────────┐  │
│  │ Metadata Registry   │    │            CORE ENGINE (asyncio.Lock)       │  │
│  │ (processes.json)    │───►│ 1. Priority Gate (USER vs MACHINE + Timer)  │  │
│  └─────────────────────┘    │ 2. Deduplicator (Process_ID + Target_Date)  │  │
│                             │ 3. Composite Lock Manager (Resource+Range)  │  │
│  ┌─────────────────────┐    │ 4. Priority FIFO Queue (без транзакций БД)  │  │
│  │ Watchdog (TTL 2s)   │◄──►└──────────────────────┬──────────────────────┘  │
│  └─────────────────────┘                           │ Атомарный резерв (FR-09)│
└────────────────────────────────────────────────────┼─────────────────────────┘
                                                     ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                  ВЫЧИСЛИТЕЛЬНЫЙ СЛОЙ (Пул 3: Async Workers)                  │
│   [ Worker run_id_1 ]      [ Worker run_id_2 ]      [ Worker run_id_N ]      │
│   1. EXTRACT (REPEATABLE READ) -> 2. STAGING (temp_risk_<run_id>) ->         │
│   3. COMPUTE (Расчёт метрик)   -> 4. LOAD (Atomic MERGE в витрину)           │
└────────────────────────────────────────────────────┬─────────────────────────┘
                                                     │ asyncpg pool
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
├── docker-compose.yml               # Поднимает PostgreSQL 16 и сервис backend
├── Dockerfile                       # Образ Python 3.11-slim для бэкенда
├── requirements.txt                 # Зависимости Python
├── pytest.ini                       # Конфигурация запуска тестов
├── README.md                        # Документация запуска и гайд по проверке для жюри
├── config/
│   └── processes.json               # Декларативный реестр процессов, ресурсов и окон дат
├── backend/
│   ├── __init__.py
│   ├── main.py                      # Инициализация FastAPI, lifespan (БД, Watchdog), раздача статики
│   ├── db/
│   │   ├── database.py              # Пул asyncpg, миграция DDL, генератор синтетических данных
│   │   └── models.py                # Pydantic-модели запросов, ответов и внутренних сущностей
│   ├── core/
│   │   ├── registry.py              # Парсинг processes.json, расчёт Date_Range по window_days
│   │   ├── lock_manager.py          # Проверка пересечений (Resource + Date_Range + Mode) и TTL
│   │   ├── orchestrator.py          # Главный диспетчер (Приоритеты, Таймер, Дедупликация, Очередь)
│   │   └── worker.py                # Пайплайн расчёта (Extract -> Staging -> Compute -> Load)
│   └── api/
│       ├── routes.py                # REST-эндпоинты (/api/runs, /dry-run, /state, /demo/*)
│       └── ws.py                    # Менеджер WebSocket-подключений и рассылка STATE_UPDATE
├── frontend/
│   ├── index.html                   # Единый экран дашборда и управления
│   ├── styles.css                   # Стилизация интерфейса
│   └── app.js                       # Логика клиента: REST-запросы, предпроверка, рендер по WS
├── desktop_app.py                   # Запуск сервера + нативного окна через pywebview
└── tests/
    ├── conftest.py                  # Фикстуры pytest, клиент httpx и очистка базы
    ├── test_compatibility.py        # Тесты 4 сценариев матрицы + таймер машины + TTL Watchdog
    └── test_load_50.py              # Нагрузочный тест на 50 параллельных запросов (NFR-01, NFR-03)

```

### 3.2. Спецификация программных модулей

| Модуль / Файл | Ответственность | Ключевые классы и функции | Требования |
| --- | --- | --- | --- |
| `config/processes.json` | Конфигурация бизнес-процессов, их длительности, скользящих окон и карт доступа к ресурсам (`READ`/`WRITE`). | JSON-схема: `machine_timer_interval_sec`, `lock_ttl_sec`, `processes.{id}.{window_days, duration_sec, resources}` | `UR-05`, `FR-06` |
| `backend/db/database.py` | Управление пулом `asyncpg`, создание таблиц при старте, генерация 1000+ синтетических транзакций и справочника клиентов. | `init_db()`, `seed_synthetic_data()`, `get_pool()`, `log_audit_event()` | `FR-11`, `NFR-05`, `NFR-06` |
| `backend/db/models.py` | Строгая типизация данных между слоями и API. | `RunCreateRequest`, `DryRunResponse`, `RunInfo`, `LockInfo`, `SystemStateSnapshot`, `ConfigUpdateRequest` | `FR-04`, `FR-14`, `FR-15` |
| `backend/core/registry.py` | Загрузка конфига и вычисление списка композитных ключей для любой пары `(process_id, target_date)`. | `ProcessRegistry.get_required_locks(process_id, target_date) -> list[ResourceLockSpec]` | `FR-04`, `FR-06` |
| `backend/core/lock_manager.py` | Проверка совместимости ключей по правилам интервалов дат и режимов доступа; управление реестром `active_locks`. | `check_conflicts(required_locks)`, `acquire_locks(run_id, locks, ttl)`, `release_locks(run_id)`, `extend_ttl(run_id, ttl)` | `FR-06`, `FR-07`, `FR-09`, `NFR-02` |
| `backend/core/orchestrator.py` | Координация жизненного цикла заявок под единым `asyncio.Lock`. Реализует дедупликацию, приоритет `USER > MACHINE`, таймер ожидания машины, очередь FIFO и Watchdog. | `submit_run()`, `dry_run()`, `cancel_run()`, `process_queue()`, `_machine_timer_loop()`, `_watchdog_loop()` | `FR-01`–`FR-10`, `FR-15`, `FR-16`, `NFR-01`–`NFR-03` |
| `backend/core/worker.py` | Выполнение расчёта в изолированной среде PostgreSQL с отправкой прогресса и Heartbeat. Поддерживает флаг `simulate_crash` для Chaos-теста. | `execute_risk_job(run_id, orchestrator, ws_manager)`, `kill_worker(run_id)` | `FR-11`–`FR-13`, `NFR-02` |
| `backend/api/routes.py` | HTTP-интерфейс для клиента и демонстрационных сценариев. | `POST /api/runs`, `POST /api/runs/dry-run`, `DELETE /api/runs/{id}`, `GET /api/state`, `POST /api/demo/*` | `FR-01`, `FR-14`–`FR-16` |
| `backend/api/ws.py` | Пул активных WebSocket-клиентов и мгновенная широковещательная рассылка состояния. | `ConnectionManager.connect()`, `disconnect()`, `broadcast_state(state)` | `NFR-04` |
| `desktop_app.py` | Точка входа для локального запуска без браузера: поднимает Uvicorn в фоновом потоке и открывает окно `webview.create_window()`. | `start_server()`, `main()` | `UR-01` |

---

## 4. Модель данных, конфигурация и машина состояний

### 4.1. Конфигурация процессов (`config/processes.json`)

В конфиг заложены **4 эталонных процесса**, которые позволяют продемонстрировать все 4 сценария матрицы совместимости и скрытый конфликт скользящих окон:

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
CREATE INDEX IF NOT EXISTS idx_runs_dedup ON runs(process_id, target_date) WHERE status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING');
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
    event_type      VARCHAR(64) NOT NULL,   -- 'CREATED' | 'DEDUPLICATED' | 'TIMER_WAIT' | 'QUEUED' | 'STARTED' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'TTL_EXPIRED'
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
| *(Новая заявка)* | Найден активный/ждущий дубль `(process_id, target_date)` | *(Без создания)* | Добавляет `user_name` в `subscribers` существующей задачи (`FR-05`). |
| *(Новая заявка)* | `initiator == 'MACHINE'` И есть активные/ждущие задачи `USER` | `WAITING_TIMER` | Запуск таймера опроса каждые $T$ сек. (`FR-03`). |
| *(Новая заявка)* | `LockManager` выявил конфликт по `(Resource + Date_Range + Mode)` | `QUEUED` | Постановка в FIFO-очередь без транзакций БД (`FR-08`). |
| *(Новая заявка)* | Конфликтов нет (и приоритет соблюдён) | `RUNNING` | Атомарный резерв ключей в `active_locks`, старт воркера (`FR-09`). |
| `WAITING_TIMER` | Сработал таймер И счётчик задач `USER == 0` | `RUNNING` или `QUEUED` | Повторная прогонка заявки через `LockManager`. |
| `QUEUED` | Освободились конфликтующие блокировки (`process_queue`) | `RUNNING` | Атомарный резерв ключей и старт воркера (`FR-10`). |
| `QUEUED` / `WAITING_TIMER` | Пользователь нажал «Отменить» (`DELETE /api/runs/{id}`) | `CANCELLED` | Удаление из очереди без влияния на БД (`FR-16`). |
| `RUNNING` | Воркер завершил шаг `LOAD` (`MERGE`) | `COMPLETED` | Снятие блокировок в `active_locks`, вызов `process_queue()` (`FR-10`). |
| `RUNNING` | Исключение в воркере ИЛИ истёк `TTL` в Watchdog | `FAILED` | Принудительное снятие блокировок, запись в `audit_logs`, вызов `process_queue()` (`NFR-02`). |

---

## 5. Алгоритмы ядра и бизнес-логика

### 5.1. Формальное правило конфликта в `LockManager` (`FR-06`, `FR-07`)

Пусть новая задача запрашивает ключ $K_{req} = (R_1, [D_{1}^{from}, D_{1}^{to}], M_1)$, а в таблице `active_locks` уже существует ключ $K_{act} = (R_2, [D_{2}^{from}, D_{2}^{to}], M_2)$.

Конфликт возникает **тогда и только тогда**, когда одновременно выполняются 3 условия:

1. **Совпадение ресурса:** $R_1 = R_2$
2. **Пересечение интервалов дат:** $D_{1}^{from} \le D_{2}^{to} \quad \text{И} \quad D_{2}^{from} \le D_{1}^{to}$
3. **Несовместимость режимов доступа:** $\neg (M_1 = \text{'READ'} \;\land\; M_2 = \text{'READ'})$



Если хотя бы один из требуемых ключей задачи конфликтует с любым активным ключом, **ни один ключ не захватывается** (атомарность «всё или ничего» по `FR-09`), а задача уходит в `QUEUED` с указанием конкретного конфликтующего ресурса и `run_id` владельца.

### 5.2. Приоритет очереди (`process_queue`, `FR-08`, `FR-10`)

При освобождении блокировок оркестратор под `asyncio.Lock` сортирует все задачи со статусом `QUEUED` по кортежу:

```python
key = (0 if run.initiator == "USER" else 1, run.created_at)

```

Задачи проверяются по порядку: если для задачи все ключи свободны — она атомарно забирает блокировки и стартует. Задачи с непересекающимися ресурсами могут обогнать стоящую впереди заблокированную задачу (отсутствие Head-of-Line Blocking для независимых ресурсов).

### 5.3. Жизненный цикл вычислительного воркера (`worker.py`)

```python
async def execute_risk_job(run_id: UUID):
    # Фоновая задача Heartbeat: каждые 3 сек продлевает active_locks.expires_at на +25 сек (NFR-02)
    heartbeat_task = asyncio.create_task(_heartbeat_loop(run_id))
    temp_table = f"temp_risk_{run_id.hex}"
    try:
        # Шаг 1: EXTRACT в режиме Snapshot Isolation / MVCC (FR-11)
        async with pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read"):
                rows = await conn.fetch(
                    "SELECT t.tx_date, t.amount, c.pd_base FROM src_transactions t "
                    "JOIN ref_clients c ON t.client_id = c.client_id "
                    "WHERE t.tx_date BETWEEN $1 AND $2",
                    date_from, date_to
                )
        # Соединение освобождено сразу после чтения; пауза без удержания транзакции и коннекта
        await update_stage(run_id, stage="EXTRACT", progress=20)
        await asyncio.sleep(step_delay)

        # Шаг 2: STAGING в персональной буферной таблице (FR-12)
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE TABLE {temp_table} (amount NUMERIC, weighted NUMERIC)")
            await conn.executemany(
                f"INSERT INTO {temp_table} VALUES ($1, $2)",
                [(r["amount"], r["amount"] * r["pd_base"]) for r in rows]
            )
        await update_stage(run_id, stage="STAGING", progress=50)
        await asyncio.sleep(step_delay)

        # Шаг 3: COMPUTE агрегация метрик риска
        async with pool.acquire() as conn:
            agg = await conn.fetchrow(
                f"SELECT COUNT(*) AS cnt, COALESCE(SUM(amount),0) AS exp, COALESCE(SUM(weighted),0) AS risk FROM {temp_table}"
            )
        await update_stage(run_id, stage="COMPUTE", progress=80)
        await asyncio.sleep(step_delay)

        # Шаг 4: LOAD атомарный MERGE в целевую витрину (FR-13)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO mart_risk_results (process_id, target_date, total_exposure, weighted_risk, rows_processed, last_run_id, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (process_id, target_date) DO UPDATE SET
                    total_exposure = EXCLUDED.total_exposure,
                    weighted_risk  = EXCLUDED.weighted_risk,
                    rows_processed = EXCLUDED.rows_processed,
                    last_run_id    = EXCLUDED.last_run_id,
                    updated_at     = NOW()
                """,
                process_id, target_date, agg["exp"], agg["risk"], agg["cnt"], run_id
            )
        await update_stage(run_id, stage="LOAD", progress=95)
        await asyncio.sleep(step_delay)

        await orchestrator.complete_run(run_id, status="COMPLETED")
    except Exception as exc:
        await orchestrator.complete_run(run_id, status="FAILED", error=str(exc))
    finally:
        heartbeat_task.cancel()
        async with pool.acquire() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")

```

---

## 6. Контракт REST API, WebSocket и требования к UI

### 6.1. REST API Endpoints

| Метод и URL | Описание | Формат Request / Response (JSON) |
| --- | --- | --- |
| `GET /api/processes` | Справочник доступных процессов из `processes.json` | **Res `200`:** `[{"id": "credit_risk", "name": "Оценка кредитного риска", "window_days": 7, "resources": [...]}]` |
| `POST /api/runs/dry-run` | Предпроверка конфликтов до запуска (`FR-15`) | **Req:** `{"process_id": "etl_transactions", "target_date": "2026-11-19", "initiator": "USER"}`<br><br>**Res `200`:** `{"prediction": "QUEUE", "date_from": "2026-11-19", "date_to": "2026-11-19", "reason": "Конфликт WRITE/READ по src_transactions (19.11–19.11) с запуском #8f14c2"}`<br><br>*(Значения `prediction`: `RUN_IMMEDIATELY`, `DEDUPLICATE`, `QUEUE`, `WAIT_TIMER`)* |
| `POST /api/runs` | Создание или подключение к запуску (`FR-01`..`FR-09`) | **Req:** `{"process_id": "credit_risk", "target_date": "2026-11-20", "initiator": "USER", "user_name": "Аналитик 1"}`<br><br>**Res `200`:** `{"action": "CREATED" \| "ATTACHED", "run": { ...объект RunInfo... }}` |
| `DELETE /api/runs/{run_id}` | Отмена задачи из очереди или таймера (`FR-16`) | **Res `200`:** `{"status": "CANCELLED", "run_id": "..."}`<br><br>**Res `409`:** `{"detail": "Нельзя отменить уже выполняющуюся задачу"}` |
| `GET /api/state` | Полный снимок состояния дашборда (`FR-14`) | **Res `200`:** `{"active_runs": [...], "queue": [...], "timer_waiting": [...], "active_locks": [...], "history": [...], "audit_logs": [...], "config": {"machine_timer_interval_sec": 15}}` |
| `PUT /api/config` | Изменение интервала таймера машины (`UR-05`, `FR-03`) | **Req:** `{"machine_timer_interval_sec": 10}`<br><br>**Res `200`:** `{"machine_timer_interval_sec": 10}` |
| `POST /api/demo/trigger-cron` | **Демо:** запуск регламента `MACHINE` | **Req:** `{"process_id": "credit_risk", "target_date": "2026-11-20"}`<br><br>**Res `200`:** возвращает созданный `RunInfo` с `initiator="MACHINE"` |
| `POST /api/demo/reset` | **Демо:** очистка истории и журнала аудита | Удаляет завершённые задачи (`COMPLETED`, `FAILED`, `CANCELLED`) и очищает `audit_logs`.<br><br>**Res `200`:** `{"status": "RESET", "message": "История завершённых задач и журнал аудита очищены"}` |
| `POST /api/demo/kill-worker/{run_id}` | **Демо:** эмуляция аварийного падения воркера (`NFR-02`) | Останавливает heartbeat воркера, чтобы Watchdog снял блокировку по TTL.<br><br>**Res `200`:** `{"status": "HEARTBEAT_STOPPED", "ttl_remaining_sec": 25}` |

### 6.2. WebSocket протокол (`/ws/events`)

При любом изменении состояния задачи, прогресса шага или таблицы блокировок сервер рассылает всем клиентам JSON-сообщение (`NFR-04` — задержка $< 1$ сек.):

```json
{
  "type": "STATE_UPDATE",
  "timestamp": "2026-11-22T12:30:05.120Z",
  "data": { "...полный объект SystemStateSnapshot из GET /api/state..." }
}

```

### 6.3. Чек-лист экранов и виджетов для UX/UI (`UR-01`–`UR-05`, `FR-14`–`FR-16`)

1. **Панель запуска (Слева/Сверху):** выбор имени аналитика, выбор процесса (`credit_risk` и др.), выбор `target_date`, **живой индикатор предпроверки (`dry-run`)**, который при изменении даты/процесса сразу подсвечивает зелёным («Запустится параллельно»), синим («Подключится к активному дублю») или жёлтым («Встанет в очередь: конфликт по ресурсу X»).


2. **Виджет «Таймер машины» (`FR-03`):** индикатор регламентных задач в статусе `WAITING_TIMER` с обратным отсчётом и полем настройки интервала $T$ (10–30 сек.).


3. **Блок «Активные расчёты» (`RUNNING`):** карточки процессов с прогресс-баром (0–100%), текущим этапом (`EXTRACT` → `STAGING` → `COMPUTE` → `LOAD`), списком подписчиков (если сработала дедупликация) и кнопкой «Имитировать сбой» для демо.


4. **Блок «Очередь ожидания» (`QUEUED`):** таблица с позицией в очереди, причиной блокировки (`blocked_reason`) и кнопкой «Отменить» (`FR-16`).


5. **Карта блокировок (`active_locks`) и Журнал аудита (`audit_logs`):** таблица текущих захваченных ресурсов (`Resource_ID`, `Date_Range`, `READ/WRITE`, оставшийся `TTL`) и лента событий.


6. **Панель быстрых демо-сценариев:** кнопки «Запустить регламент (Cron)» и «Симулировать конфликт скользящего окна (20.11 + 19.11)» для проверки в один клик.



---

## 7. План тестирования и критерии готовности (QA & Demo)

### 7.1. Автотесты функциональных сценариев (`tests/test_compatibility.py`)

| ID теста | Сценарий | Входные действия | Ожидаемый результат |
| --- | --- | --- | --- |
| **TC-01** | Дедупликация (`FR-05`) | Запустить `credit_risk` на `2026-11-20` от «Аналитик 1», затем сразу от «Аналитик 2». | Создан только **1** воркер; второй запрос получил `action="ATTACHED"` и попал в `subscribers`. |
| **TC-02** | Изоляция буферов (`FR-12`) | Запустить `credit_risk` на `2026-11-10` и `credit_risk` на `2026-11-20` одновременно. | Оба выполняются параллельно (`RUNNING`), используют `temp_risk_<run_id_1>` и `temp_risk_<run_id_2>`, обе строки корректно записаны в `mart_risk_results`. |
| **TC-03** | Совместное чтение `READ+READ` (`FR-07`) | Запустить `credit_risk` и `counterparty_risk` на `2026-11-20` одновременно. | Оба выполняются параллельно без блокировки общих `src_transactions` и `ref_clients`. |
| **TC-04** | Скрытый конфликт скользящего окна (Инсайт ИИ №2) | Запустить `credit_risk` на `2026-11-20` (окно `14.11–20.11`, `READ src_transactions`), затем `etl_transactions` на `2026-11-19` (окно `19.11–19.11`, `WRITE src_transactions`). | `etl_transactions` автоматически встаёт в `QUEUED` из-за пересечения интервала `19.11–19.11` и стартует только после завершения `credit_risk`. |
| **TC-05** | Приоритет «Люди vs Машина» (`FR-03`) | При активном `USER`-расчёте вызвать `POST /api/demo/trigger-cron` (`MACHINE`). | Задача машины получает статус `WAITING_TIMER`, ждёт обнуления пользовательских задач и только затем стартует. |
| **TC-06** | Отказоустойчивость TTL Watchdog (`NFR-02`) | Запустить задачу и вызвать `POST /api/demo/kill-worker/{run_id}` при наличии задачи в очереди. | По истечении `lock_ttl_sec` Watchdog переводит зависшую задачу в `FAILED`, снимает её блокировки и автоматически запускает следующую задачу из очереди. |

### 7.2. Нагрузочный тест (`tests/test_load_50.py`)

* **Методика:** одновременная отправка **50 асинхронных запросов** (`asyncio.gather`) со случайной комбинацией процессов, дат и инициаторов (`USER` / `MACHINE`).


* Критерии прохождения (`NFR-01`, `NFR-03`):


* Количество взаимных блокировок СУБД (Deadlocks): **0 (0%)**.


* Среднее и 95-перцентиль времени принятия решения оркестратором: **$< 200$ мс**.


* Все запущенные задачи доходят до терминального статуса `COMPLETED`, таблица `active_locks` по завершении теста полностью пуста (`0` зависших блокировок).



---

