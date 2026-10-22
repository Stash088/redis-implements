# Handoff: состояние проекта `redis-implements` на 2026-10-21

Документ для того, кто продолжит работу в новой сессии. Он отвечает на
вопросы «где мы», «что делать дальше» и «на что обратить внимание».

## 1. Где мы

| | |
|---|---|
| Покрытие команд | **162 из 267** команд Redis 8.2 (61 %) — все 162 совпадают с реальными командами Redis |
| Веток/MR | 16 draft-MR, стек друг на друга (см. §2) |
| Производительность | паритет с Redis 8.2 под пайплайном (0.97–1.06×), 0.84–0.90× на одиночных командах |
| Сверка с Redis | харнесс `cmd/compat-check`: **все 7 сценариев совпадают** с настоящим Redis 8.2 |
| Тесты | `go test -race ./...` — зелёный и стабильный при `-count=3` |

Реализованные вехи: M0, M1, M3, M4, M5, M6, M9–M18.
Не сделано: M2 (шардирование — отложено осознанно), M19–M23 (Streams, репликация).

## 2. Ветки и MR

Стек: каждая веха основана на предыдущей. Мержит владелец, по порядку снизу вверх.

| MR | Ветка | База для PR | Веха |
|---|---|---|---|
| !1 | `feat/m0-foundation` | `main` | M0 — RLock на чтениях, единый реестр команд |
| !2 | `feat/m1-pipeline-batching` | `feat/m0-foundation` | M1 — батчинг ответов пайплайна |
| !3 | `feat/m3-multi-db` | `feat/m1-pipeline-batching` | M3 — `SELECT` + 16 баз |
| !4 | `feat/m4-hello-resp3` | `feat/m3-multi-db` | M4 — `HELLO` + RESP3 |
| !5 | `feat/m5-auth` | `feat/m4-hello-resp3` | M5 — `AUTH` / `--requirepass` |
| !6 | `feat/m6-admin-commands` | `feat/m5-auth` | M6 — `COMMAND`, `RESET`, `CLIENT …` |
| !7 | `feat/m9-string-commands` | `feat/m6-admin-commands` | M9 — строки v2 |
| !8 | `feat/m10-collection-scans` | `feat/m9-string-commands` | M10 — `HSCAN`/`SSCAN`/`ZSCAN` |
| !9 | `feat/m11-hash-commands` | `feat/m10-collection-scans` | M11 — hash v2 |
| !10 | `feat/m12-set-commands` | `feat/m11-hash-commands` | M12 — set v2 |
| !11 | `feat/m13-list-commands` | `feat/m12-set-commands` | M13 — list v2 |
| !12 | `feat/m14-zset-range-commands` | `feat/m13-list-commands` | M14 — zset: score/lex |
| !13 | `feat/m15-zset-operations` | `feat/m14-zset-range-commands` | M15 — zset: операции |
| !14 | `feat/m16-key-management` | `feat/m15-zset-operations` | M16 — управление ключами |
| !15 | `feat/m17-blocking-commands` | `feat/m16-key-management` | M17 — блокирующие команды |
| !16 | `feat/m18-compat-harness` | `feat/m17-blocking-commands` | M18 — харнесс сверки |

⚠️ **Важно про `main`**: удалённый `origin/main` на `72327ba` (Stage 7), а локальный
`main` — на `537103b` (Stage 7 + Dockerfile + Stage 8). Коммиты Stage 8 и
Dockerfile **не запушены в main**. Перед слиянием их нужно доставить в `main`
(например, `git push origin main` из локального main или слияние последнего MR
вместе с ними).

## 3. Что делать дальше

Порядок вех — в `docs/roadmap-m0-plus.md` (§ «M19–M23»), там же — детали по
каждой вехе. Коротко:

### M19 — Streams: модель данных
- Новый `TypeStream` в `internal/store`: записи (id `ms-seq`, поля), генерация id.
- `XADD` (в т.ч. `*`/`ms-*`, `MAXLEN`/`MINID`), `XLEN`, `XRANGE`, `XREVRANGE`,
  `XDEL`, `XTRIM`, `XSETID`.
- AOF-пропагация `XADD`/`XDEL`/`XTRIM`.
- Начать с: `internal/store/stream.go` (тип записи, id, контейнер), затем
  `internal/server/commands_stream.go`.

### M20 — Streams: группы и чтение
- `XREAD` (блокирующий и нет), `XREADGROUP`, `XACK`, `XGROUP`
  (`CREATE`/`DESTROY`/`CREATECONSUMER`/`DELCONSUMER`/`SETID`), `XINFO`,
  `XPENDING`, `XAUTOCLAIM`.
- Заблокированный `XREAD` переиспользует механизм ожидания из M17
  (`internal/server/blocking.go`): регистрируем ожидание на ключе потока и
  будим его командой `XADD`.

### M21 — Streams: персистентность (полная интероп-совместимость)
- **listpack-энкодер** в `internal/persist` — сейчас есть только декодер
  (`listpack.go`); суффикс backlen писать умеем (`listpackBacklen`).
- Формат `RDB_TYPE_STREAM_LISTPACKS` (15): метаданные стрима, по listpack на
  запись, consumer-группы. Точный layout — в исходниках Redis (`rdb.c`).
- Интероп-тест: наш дамп со стримами читает настоящий `redis-server` 8.2.

### M22 — Репликация: каркас
- `REPLICAOF`/`SLAVEOF`, `ROLE`, секция `replication` в `INFO`, `REPLCONF`
  (`listening-port`, `capa`), handshake `PSYNC`, полный sync (передача RDB).

### M23 — Репликация: инкрементальная синхронизация
- Поток пишущих команд на реплики, буферизация, команда `WAIT`, частичный
  resync по offset/replication ID, `WAITAOF`.

## 4. Известные ограничения (зафиксированы намеренно)

| Что | Почему | Где смотреть |
|---|---|---|
| `DUMP`/`RESTORE` — свой формат payload'а | настоящий Redis такой payload не восстановит | `internal/persist/dump.go` |
| Нет команд с TTL полей хэша (`HEXPIRE`/`HTTL`/`HSETEX`) | требуют поддержки TTL полей в RDB, иначе данные терялись бы при сохранении | `docs/roadmap-m0-plus.md` (M11) |
| `SORT` без `BY`/`GET` требует числовых элементов | как в Redis: иначе `ERR One or more scores can't be converted into double` | `internal/server/commands_keys.go` |
| `SCAN`/`HSCAN`/… — курсор = индекс в отсортированный снимок | снимок строится заново на каждый вызов, O(n log n) | `internal/server/commands_scan.go` |
| `COUNT` в сканах — строгий лимит (в Redis — подсказка) | границы батчей у разных реализаций разные, поэтому харнесс сравнивает весь обход | `testdata/compat/scan.txt` |
| Обрыв соединения заблокированного клиента замечается только по таймауту | в Redis помогает `CLIENT KILL` | `internal/server/blocking.go` |
| Нет `CLIENT KILL`/`CLIENT UNBLOCK`/`CLIENT NO-EVICT` (реального эффекта) | `CLIENT KILL` в текущем виде не реализован | `internal/server/commands_admin.go` |
| Нет `EVAL`/`FUNCTION`, кластера, `ACL`, Geo, HyperLogLog, битовых операций | отложены | `ROADMAP.md` |

## 5. Тонкости Redis, которые пришлось повторить (неочевидные!)

Это места, где документация Redis вводит в заблуждение или не покрывает
поведение. Харнесс их уже проверяет — при добавлении новых команд смотри
сюда, а не в доки Redis.

- `LINSERT` возвращает **-1**, когда pivot не найден, и **0**, когда нет ключа
  (в доках наоборот).
- `HGETDEL` (Redis 8) возвращает **массив удалённых значений**, а не число;
  для отсутствующего поля — `[nil]`. Синтаксис: `HGETDEL key FIELDS n field…`.
- `SORT` без `ALPHA` требует, чтобы все элементы были числами (иначе ошибка);
  с `BY` нечисловой вес считается **нулём**; `GET` подставляет значения
  найденных ключей (нет ключа → `nil`).
- `LPOS`: `COUNT` не может быть отрицательным; `MAXLEN` ограничивает **окно
  просмотра**, а не число ответов; при заданном `COUNT` ответ — массив, без
  него — одно число.
- `ZRANGE … REV` без `BYSCORE`/`BYLEX` применяет индексы к **обратному**
  порядку множества; с `BYSCORE`/`BYLEX` границы **меняются местами**.
- `TTL` округляется до **ближайшей секунды** (`(ms+500)/1000`).
- `BLPOP`/`BRPOP` → `[key, value]`, `BZPOPMIN`/`BZPOPMAX` → `[key, member, score]`,
  по таймауту — null-массив.
- `INCRBYFLOAT` печатает **17 значащих цифр** без экспоненты
  (`10.5 + 0.1` → `10.59999999999999964`), а `ZSCORE` — кратчайшую форму
  (`10.6`).
- Формы ответов RESP3: `ZRANGE WITHSCORES` → массив пар `[member, double]`
  (не плоский массив!), `CONFIG GET`/`HGETALL` → карта, `SMEMBERS` →
  множество, `SDIFF`/`SINTER`/`SUNION` → множество, Pub/Sub → push-кадры.
- `DUMP`/`RESTORE` у настоящего Redis: `RESTORE` без `REPLACE` на занятый
  ключ → `BUSYKEY Target key name already exists.`

## 6. Как проверять

```bash
go build ./... && go vet ./... && gofmt -l .    # должно быть чисто
go test -race ./...                              # зелёный
go test -race -count=3 ./internal/...            # стабильность

# сверка с настоящим Redis 8.2 (поднимает оба сервера)
scripts/compat-check.sh

# только наша реализация
go run ./cmd/redis-server --port 6379

# производительность
redis-benchmark -p 6379 -t get,set -n 100000 -c 50 -P 50
```

Правила оформления кода (по образцу уже сделанного):
- Хранилище: доступ на чтение — под `RLock`, на запись — под `Lock`;
  `Entry.lastAccess` — `atomic.Int64`; чтение коллекции выполняется **под**
  блокировкой (иначе гонка «concurrent map read and map write»).
- Новая команда: регистрация в `internal/server/commands.go` через
  `reg`/`regW`/`regWG`/`regWS`/`regWSG` (арность + флаги write/aofSelf/memAdd)
  и **обязательно** обновление списков в `internal/server/registry_test.go` —
  тест инвариантов реестра сам поймает забытую write-команду.
- AOF: новая пишущая команда добавляется в `aofWriteCommands` (через реестр)
  и в разбор `internal/persist/aof.go` (`applyAOFCommand`).
- Сценарий сверки: `testdata/compat/<family>.txt`, новые команды — туда же.
