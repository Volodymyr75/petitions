# 📊 Petition Analytics — Project State

> **Останнє оновлення**: 2026-03-27
> **Призначення:** Швидке відновлення контексту після перезапуску розмови

---

## 📅 Сесія 2026-03-27: Hotfix Cloud Sync (Akamai Bypass)
1. **Збій синхронізації**: Отримано помилку *Failed to fetch (404/Timeout)* під час `pre-flight` перевірки (`cloud_sync.py`). Причина — посилення захисту від ботів (Akamai Bot Manager) на сайті петицій Президента (код `403 Forbidden` для стандартних HTTP-клієнтів).
2. **Вирішення (Bypass)**: Бібліотеку `requests` повністю замінено на `curl_cffi` (імітація TLS відбитків Chrome). 
3. **Реалізація**: Оновлено імпорти в `scraper_detail.py` та `scraper_president.py`. Видалено статичний `HEADERS` для уникнення конфліктів fingerprint, додано аргумент `impersonate="chrome"`. Залежність `curl-cffi` додано в `requirements.txt`.
4. **Deploy**: Зміни протестовані локально і завантажені (pushed) на GitHub для відновлення роботи GitHub Actions.

---

## 📅 Сесія 2026-02-20: Dashboard V2 — Premium Redesign ✅ COMPLETED
1. **Аналіз рекомендацій**: Проаналізовано 2 блоки рекомендацій (25 пунктів). Відібрано 11 фіч для реалізації.
2. **ETL розширення**: ✅ 6 нових SQL запитів у `pipeline.py` + 5 авто-інсайтів.
3. **Dashboard Rewrite**: ✅ Повний рерайт `Dashboard.jsx` — 7 нових графіків, insight pills, platform comparison.
4. **Source Toggle**: ✅ Фільтрація KPI, status distribution, scatter plot по source (All/President/Cabinet).
5. **Design System**: ✅ Dark Mode + Glassmorphism + Google Fonts + micro-animations + timeline annotation.
6. **Bugfix: Status Normalization**: ✅ Cabinet використовував англійські назви статусів (Unsupported, Approved, etc.), а President — українські. Додано `CASE WHEN` маппінг у `pipeline.py`: Unsupported→Архів, Approved→На розгляді, Answered→З відповіддю, Supported→Збір підписів. Тепер stacked bar chart коректно агрегує обидва source.
7. **Deploy**: ✅ Запушено на GitHub (`c779bcd`).

### ✅ Що зроблено (V2) — повний список

| Компонент | Статус | Деталі |
|-----------|--------|--------|
| **ETL: 6 нових SQL запитів** | ✅ | status_distribution, top_authors, categories (regex), vote_velocity, keywords_top10, platform_comparison |
| **ETL: Auto-Insights** | ✅ | 5 narrative фактів (military %, viral rarity, median, response rate, platform diff) |
| **UI: Insight Pills** | ✅ | Горизонтальна сітка з емоджі + текст під KPI картками |
| **UI: Platform Comparison** | ✅ | Side-by-side картки President vs Cabinet |
| **UI: Status Distribution** | ✅ | Stacked horizontal bar chart (per source) |
| **UI: Scatter Plot** | ✅ | text_length vs votes, колір по source |
| **UI: Top Authors** | ✅ | Horizontal bar chart, top 7 за total_votes |
| **UI: Category Breakdown** | ✅ | Progress bars з % (6 категорій) |
| **UI: Keywords Top-10** | ✅ | Horizontal bar chart з частотними словами з заголовків |
| **UI: Vote Velocity** | ✅ | Таблиця (при наявності даних у votes_history) |
| **UI: Source Toggle** | ✅ | Segmented control + фільтрація KPI, charts |
| **UI: Data Freshness** | ✅ | Pills у footer (Auto-ETL, data span, coverage) |
| **UI: Enhanced Footer** | ✅ | Tech Stack + Roadmap + coverage badge |
| **Dark Mode** | ✅ | Toggle ☀️/🌙 з `localStorage`, повна адаптація фону/карток/графіків |
| **Glassmorphism** | ✅ | `backdrop-blur`, glass borders, hover lift (-2px) + glow |
| **Google Fonts** | ✅ | Inter (UI) + DM Mono (числа), gradient text на заголовку |
| **Micro-animations** | ✅ | Slide-up на секціях, smooth transitions (300ms), custom scrollbar |
| **Timeline анотація** | ✅ | Червона пунктирна лінія лютий 2022 "Full-scale invasion" |

### 📊 Нові дані в JSON (analytics_data.json) — ✅ Реалізовано
- `analytics.status_distribution` — розподіл по статусах
- `analytics.top_authors` — топ-10 авторів за голосами
- `analytics.categories` — категоризація (regex-based, 6 категорій)
- `analytics.vote_velocity` — швидкість набору голосів (7 днів)
- `analytics.keywords_top10` — частотні слова з заголовків
- `overview.platform_comparison` — порівняння President vs Cabinet
- `insights[]` — 5 авто-генерованих narrative фактів

---

## 📅 Сесія 2026-02-05 - 2026-02-14: Редизайн та стабілізація
1. **Dashboard Refinement**: Повний редизайн футера на багаторівневу структуру. Додано прямі посилання на GitHub, email та ШІ-roadmap.
2. **Sync Analysis**: Досліджено 503 помилки сайту Президента. Валідація `pre-flight` підтвердила свою ефективність у захисті даних.
3. **Data Integrity**: Підтверджено цілісність 95,479 записів.
4. **Deploy**: Зміни синхронізовано з GitHub, враховуючи автоматичні коміти від GitHub Actions.

---

## 📅 Сесія 2026-01-07: Завершення великого Backfill
1. **Daily Sync**: Зібрано **+63,351** голос. 2 петиції перейшли в статус "На розгляді".
2. **Archive Complete**: База зросла до **95,464** петицій. Діапазон до 257,750 перевірено — архівні дані зібрані максимально повно.
3. **Статистика**: База тепер охоплює майже 100% значущих петицій за останні роки.
4. **Deploy**: Фінальний (на даному етапі) масив архівних даних та аналітики відправлено на GitHub.

---

## 🚀 Phase 2: Automation Plan

**Мета:** Автоматизувати добове оновлення з валідацією та сповіщеннями.

### Архітектура
- **Database**: MotherDuck (cloud DuckDB) — база `petitions_prod`
- **Runner**: GitHub Actions (cron: 04:00 UTC / 06:00 Kyiv)
- **Notifications**: Telegram + GitHub Issues

### Компоненти
| Компонент | Файл | Опис |
|-----------|------|------|
| Валідація | `validator.py` | Pre-flight (5 маркерних петицій) + Post-sync перевірки |
| Сповіщення | `notifier.py` | Telegram алерти + GitHub Issue з логами |
| Бекап | SQL в workflow | `CREATE TABLE backup AS SELECT...` перед синхронізацією |
| Workflow | `daily_sync.yml` | GitHub Actions з rollback логікою |

### Перевірки (Sanity Checks)
- `votes > 0` для активних петицій
- `status != 'Unknown'`
- `text_length > 0`
- Error rate < 20%

### GitHub Secrets
- `MOTHERDUCK_TOKEN` — токен MotherDuck
- `TELEGRAM_BOT_TOKEN` — токен Telegram бота
- `TELEGRAM_CHAT_ID` — ID чату

---

### 🛠️ Automation & GitHub Actions Fixes
- **Error 1: `Resource not accessible by integration`**
  - *Причина:* Відсутність прав на запис (write permissions) для GitHub Actions (необхідно для комітів та створення Issues).
  - *Рішення:* Додано блок `permissions` (contents: write, issues: write) у YAML-конфігурацію.
- **Error 2: `No file matched to requirements.txt`**
  - *Причина:* Увімкнено кешування pip, але файл залежностей відсутній.
  - *Рішення:* Створено `requirements.txt` у корені проекту зі списком `duckdb, requests, beautifulsoup4`.

- **Update: Dynamic Validation Strategy**
  - *Проблема:* Використання статичних (hardcoded) ID у `validator.py` призводило до хибних зупинок через видалення петицій на сайті (404 для ID 250000).
  - *Рішення:* Перехід на динамічний вибір 5 петицій прямо з бази MotherDuck перед кожним запуском (2 найактивніші, 2 випадкові архівні, 1 з відповіддю). Це робить систему самостійною та стійкою до змін на сайті.

---

## 📅 Сесія 2026-01-03: Масштабне розширення архіву
1. **Daily Sync**: Зібрано **+55,823** голосів! Додано **13** нових петицій Президента.
2. **Backfill**: Гігантський приріст — база зросла до **83,951** петицій (додано діапазон 120,000–153,800).
3. **Data Integrity**: Перевірка 10 рандомних записів з нового діапазону підтвердила повну коректність (автори, номери, текст — все ок).
4. **Deploy**: Оновлено аналітику та стан проекту.

---

## 📅 Сесія 2026-01-02: Динамічний ріст та безпека даних
1. **Daily Sync**: Зібрано **+48,193** голосів (Президент + Кабмін). Додано **7** нових петицій Кабміну.
2. **Backfill**: База зросла до рекордних **77,039** петицій.
3. **Data Security**: Оновлено `.gitignore` для повного виключення файлів бази даних (`*.duckdb*`) з репозиторію.
4. **Deploy**: Актуальна аналітика відправлена на GitHub.

---

## 📅 Сесія 2026-01-01: Перший запуск року та великий Backfill
1. **Daily Sync**: Успішно виконано перший запуск у 2026 році. Зібрано **+24,203** голосів.
2. **Backfill**: Завдяки активній роботі база зросла до **62,809** петицій (додано тисячі архівних записів).
3. **Data Integrity**: Перевірка рандомних записів (60k-68k) підтвердила 100% якість: автори та текст (text_length > 0) на місці.
4. **Deploy**: Оновлено `analytics_data.json` та зафіксовано стан у репозиторії.

---

## 📅 Сесія 2025-12-26

**Зроблено:**
- ✅ **Cabinet sync** — оновлено 5,360 петицій, топ дельта: ID 9010 (+87 голосів)
- ✅ **Очистка бази** — видалено 137 сміттєвих записів (title = "404. Такої сторінки не існує")
- ✅ **smart_complete.py** — додано retry логіку (3 спроби з паузами 30/60/90с для timeout)
- ✅ **Нові петиції** — додано 21 нових (ID 257400-257700)
- ✅ **План редизайну дашборду** — затверджено 4-блокову архітектуру

**Поточний стан бази:** 45,254 president + 5,360 cabinet = **50,614 петицій**

---

## 📅 Сесія 2025-12-28

**Зроблено:**
- ✅ **Fix Scraper** — виправлено селектори в `scraper_detail.py` (сайт змінив HTML структуру, голоси повертали 0)
- ✅ **Daily Sync Success** — успішний запуск `daily_sync.py`: +169,522 нових голосів, оновлено статуси 554 петицій
- ✅ **DB Maintenance** — відновлено базу з бекапу, створено новий backup (`petitions.duckdb.bak_latest`)
- ✅ **Dashboard Redesign** — повний редизайн `src/Dashboard.jsx` (Slate/Emerald theme, Sparklines, Progress Bars)
- ✅ **Pipeline Update** — `pipeline.py` тепер додає `history` (7 днів) у JSON для графіків

**Стан системи:**
- Frontend: Build успішний, Sparklines працюють
- Backend: Scraper працює коректно, 0 errors
- Data: `analytics_data.json` містить історію за 7 днів

---

## 📅 Сесія 2025-12-30: Backfill та оновлення
1. **Backfill**:
   - Пакет 1: +16 нових за графіком Discovery (30.12).
   - Пакет 2: +1892 архівні петиції (ID 40001-45000).
   - Перевірка: Всі записи мають метадані та коректний `text_length`.
2. **Загальний ріст**: +1913 нових записів за сьогодні.
3. **Статистика**: База тепер налічує близько **47,950** заповнених петицій.
4. **JSON**: Аналітика оновлена.

---

## 📅 Сесія 2025-12-29: Виправлення збору текстів та backfill
1. **Зміна структури сайту Президента**: Виявлено, що текст петиції переїхав з `article.article` до `#pet-tab-1`. Це спричинило запис `text_length = 0` для нових петицій.
2. **Виправлення скрапера**: Оновлено `etl/scraper_detail.py` (новий селектор) та `etl/daily_sync.py` (авто-відновлення довжини тексту при оновленні).
3. **Data Backfill**:
   - Пакет 1: +28 петицій (ID 1-1000).
   - Пакет 2: +343 петиції (ID 1001-10000).
   - Пакет 3: +242 петиції (ID 10001-20000).
   - Пакет 4: +172 петиції (ID 20001-25000).
   - Виправлено `text_length` для всіх нових записів (тепер всюди > 0).
4. **Статистика**: База тепер налічує понад 46,050 заповнених петицій.
5. **Безпека**: Створено свіжий бекап бази `petitions.duckdb.bak_20251229_latest`.

---

## 🎨 Редизайн дашборду (ПЛАН)

### Архітектура 4 блоків

| Блок | Зміст |
|------|-------|
| **1. Overview** | 4 KPI (total, success rate, median votes, response rate) + platform split |
| **2. Daily Dynamics** | +нових петицій, +голосів, biggest movers (топ по дельтах), 7-day sparkline |
| **3. Deep Analytics** | Histogram votes, timeline (stacked area by month), scatter text_length vs votes |
| **4. Pipeline Info** | Architecture diagram, tech stack, data freshness, "AI Agent coming soon" |

### Нові таблиці (потрібно створити)

```sql
-- Щоденна статистика для блоку 2
CREATE TABLE daily_stats (
    date DATE PRIMARY KEY,
    president_new INTEGER,
    cabinet_new INTEGER,
    total_votes_delta INTEGER,
    status_changes JSON
);

-- Історія голосів для sparklines (майбутнє)
CREATE TABLE votes_history (
    petition_id VARCHAR,
    source VARCHAR,
    date DATE,
    votes INTEGER,
    PRIMARY KEY (petition_id, source, date)
);
```

### ⚠️ Відомі проблеми

| Проблема | Деталі | Статус |
|----------|--------|--------|
| `has_answer` баг | 100% president петицій мають `true`. Рішення: ігноруємо, використовуємо `status='З відповіддю'` | 🟡 Ігноруємо |
| ~~Різні формати дат~~ | Додано поле `date_normalized` (DATE), конвертовано 100% записів | ✅ Вирішено |

---


## 📅 Сесія 2026-01-08: Автоматизація та "Чиста міграція"
1. **Clean Migration**: Виправлено проблему `Binder Error` у MotherDuck. Таблиці перестворено з явними **PRIMARY KEY**.
2. **Data Sync**: База повністю синхронізована (95,477 петицій).
3. **Cumulative Logic**: Впроваджено адитивну систему оновлення статистики в `cloud_sync.py` (дозволяє кілька запусків на день без втрати даних).
4. **GitHub Actions**: Успішно протестовано ручний запуск з Telegram-сповіщенням.

---

## ⚠️ Git-Workflow (Важливо!)

Оскільки GitHub Actions тепер автоматично оновлює та комітить `src/analytics_data.json` о 04:00 UTC, ваш локальний репозиторій може відрізнятися від віддаленого.

**Правило:** Завжди робіть `git pull` перед початком локальної роботи або пушем:
```bash
git pull --rebase origin main
```

---

## ✅ Виправлення схеми та міграція

- **2026-01-08**: Проведено "Чисту міграцію" (`migrate_to_cloud_final.py`).
- Всі таблиці (`petitions`, `votes_history`, `daily_stats`) мають коректні **PRIMARY KEY**.
- Дані в MotherDuck повністю синхронізовані з локальною базою (95,477 записів).

## 🔄 Останні оновлення (2026-01-08)

- **Локальна синхронізація**: Успішно виконано `daily_sync.py` для 2026-01-08.
- **Статистика**: Додано 13 нових петицій (6 Президент, 7 Кабінет).
- **Активність**: Дельта голосів за добу склала +94,474.
- **Дані**: Оновлено `src/analytics_data.json` та завантажено на GitHub.

## 🎯 Мета проекту

Аналітичний дашборд для українських електронних петицій (Президент + Кабмін) з:
- Автоматичним збором даних
- Щоденними оновленнями
- Трендами та візуалізаціями

---

## 📂 Структура проекту

```
petition/
├── etl/                    # Data pipeline скрипти
│   ├── pipeline.py         # Основний пайплайн (scrape → DB → JSON)
│   ├── scraper_president.py
│   ├── scraper_cabinet.py
│   ├── smart_complete.py   # Розумне автодоповнення
│   ├── backfill_*.py       # Бекфіл історичних даних
│   └── fix_cabinet_*.py    # Фікси для Cabinet API
├── src/                    # React Dashboard
│   ├── Dashboard.jsx       # Основний UI (KPI, charts, trending)
│   └── analytics_data.json # Дані для дашборду
├── petitions.duckdb        # DuckDB база (~27MB)
└── netlify/                # Deployment конфіги
```

---

## 🗄️ База даних (DuckDB)

### Таблиці
| Таблиця | Статус | Опис |
|---------|--------|------|
| `petitions` | ✅ Є | +`votes_previous`, `updated_at`, `date_normalized` |
| `votes_history` | ✅ Є | Історія голосів для sparklines |
| `daily_stats` | ✅ Є | Агрегована статистика за добу |

### Статистика petitions (2026-01-08)
| source | status | count |
|--------|--------|-------|
| president | Триває збір підписів | 499 |
| president | На розгляді | 3,053 |
| president | З відповіддю | 1,637 |
| president | Архів | 84,881 |
| cabinet | Answered | 50 |
| cabinet | Approved | 235 |
| cabinet | Supported | 3 |
| cabinet | Unsupported | 5,117 |
| president | Не підтримано | 2 |

**Всього:** 95,479 петицій

---

## ✅ Що вже зроблено

1. **ETL Pipeline** (`pipeline.py`)
   - Скрапінг президентських петицій (active, in_process, processed, archive)
   - Скрапінг кабмінівських петицій через API
   - Розрахунок дельт голосів (trending)
   - Експорт в `analytics_data.json`


2. **Dashboard** (`Dashboard.jsx`)
   - ✅ Редизайн на 4 блоки (Overview, Daily, Analytics, Pipeline)
   - ✅ Інтеграція Recharts (Histogram, AreaChart, ScatterChart)
   - ✅ KPI та Insight блоки

3. **Дані оновлені**
   - `analytics_data.json` оновлено під нову структуру (2025-12-26)
   - DB Schema: +`votes_history`, `daily_stats`, `date_normalized`

---

## 🚧 Що в процесі / Наступні кроки

### TODO:
- [x] Оновити `Dashboard.jsx` (4 блоки + Sparklines + Redesign + Footer Centering)
- [x] Оновити `pipeline.py` (JSON структура + History)
- [x] Оновити схему бази даних (MotherDuck PK Fix)
- [x] Реалізувати script `daily_sync.py` (Local) / `cloud_sync.py` (Cloud)
- [x] Виправлено `scraper_detail.py` (підтримка нового HTML)
- [x] Налаштувати автоматичний запуск (GitHub Actions + Cron)
- [x] **ETL V2**: 6 нових SQL запитів + auto-insights
- [x] **Dashboard V2 Charts**: 7 нових графіків (status dist, scatter, authors, categories, keywords, velocity, platform)
- [x] **Dashboard V2 Design**: Dark Mode + Glassmorphism + Google Fonts + micro-animations
- [x] **Source Toggle**: Фільтрація KPI + charts по source
- [x] **Timeline анотація**: Feb 2022 invasion marker
- [ ] Додати OpenAI/LLM для чат-бота (Заплановано в Roadmap)
- [ ] Додати аналітику "На розгляді" — days pending

---

## 🔧 Як запустити

```bash
# ETL Pipeline
cd etl && python pipeline.py

# Dev server
npm run dev

# Build
npm run build
```

---

## 💡 Корисні команди для діагностики

```python
# Перевірка бази
import duckdb
conn = duckdb.connect('petitions.duckdb')
print(conn.execute("SELECT source, status, COUNT(*) FROM petitions GROUP BY source, status").fetchdf())

# Перевірка таблиць
print(conn.execute("SHOW TABLES").fetchall())
```

---

## 📝 Нотатки

- Cabinet API іноді повертає неповні дані (author = null)
- Президентські петиції мають різні формати статусів (укр.)
- Деякі старі петиції мають `status = 'Unknown'`
