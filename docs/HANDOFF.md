# Handoff: состояние проекта `redis-implements` на 2026-10-22

Документ для того, кто продолжит работу в новой сессии. Он отвечает на
вопросы «где мы», «что делать дальше» и «на что обратить внимание».

## 1. Где мы

| | |
|---|---|
| Покрытие команд | **177 из 267** команд Redis 8.2 (66 %) — все 177 совпадают с реальными командами Redis |
| Веток/MR | 18 draft-MR, стек друг на друга (см. §2) |
| Производительность | паритет с Redis 8.2 под пайплайном (0.97–1.06×), 0.84–0.90× на одиночных командах |
| Сверка с Redis | харнесс `cmd/compat-check`: **все 9 сценариев совпадают** с настоящим Redis 8.2 |
| Тесты | `go test -race ./...` — зелёный и стабильный при `-count=3` |

Реализованные вехи: M0, M1, M3, M4, M5, M6, M9–M20.
Не сделано: M2 (шардирование — отложено осознанно), M21–M23 (персистентность
стримов, репликация).

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
| !17 | `feat/m19-streams` | `feat/m18-compat-harness` | M19 — Streams: модель данных |
| !18 | `feat/m20-stream-groups` | `feat/m19-streams` | M20 — Streams: группы и чтение |

⚠️ **Важно про `main`**: удалённый `origin/main` на `72327ba` (Stage 7), а локальный
`main` — на `537103b` (Stage 7 + Dockerfile + Stage 8). Коммиты Stage 8 и
Dockerfile **не запушены в main**. Перед слиянием их нужно доставить в `main`
(например, `git push origin main` из локального main или слияние последнего MR
вместе с ними).

## 3. Что делать дальше

Порядок вех — в `docs/roadmap-m0-plus.md` (§ «M21–M23»), там же — детали по
каждой вехе. Коротко:

### M21 — Streams: персистентность (полная интероп-совместимость)
- **listpack-энкодер** в `internal/persist` — сейчас есть только декодер
  (`listpack.go`); суффикс backlen писать умеем (`listpackBacklen`).
- Формат `RDB_TYPE_STREAM_LISTPACKS` (15): метаданные стрима, по listpack на
  запись, consumer-группы. Точный layout — в исходниках Redis (`rdb.c`).
- Интероп-тест: наш дамп со стримами читает настоящий `redis-server` 8.2.
- Пока формата нет, `SAVE` пропускает ключи-потоки (предупреждение в лог),
  а `DUMP` потока возвращает ошибку — см. §4.
- **Группы потребителей готовы** (M20): `internal/store/stream_group.go` —
  `StreamGroup`, `StreamConsumer`, `StreamNack`; операции `StreamGroupRead`
  (выдача записей и правка списка неподтверждённых), `StreamAck`,
  `StreamClaim`, `StreamAutoClaim`, `StreamPendingInfo`, `StreamPendingRange`,
  `StreamGroupsInfo`, `StreamConsumersInfo`; разбор аргументов `XGROUP` и
  `XCLAIM` — `ParseStreamGroupArgs`/`ParseStreamClaimArgs` (ими же пользуется
  реплей AOF).

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
| **Стримы не сохраняются в RDB** (формат стримов — в вехе M21) | `SAVE` пропускает ключи-потоки и пишет предупреждение в лог; AOF их сохраняет | `internal/persist/rdb.go` (`rdbEntries`), `internal/server/persist.go` |
| `DUMP` потока возвращает ошибку | нет payload'а для стримов до M21 | `internal/server/commands_keys.go` |
| Приблизительная обрезка (`XTRIM … ~`) удаляет целыми блоками по 100 записей | в Redis блоки — это узлы rax (до 100 записей и 4 КБ), у нас единого узла нет | `internal/store/stream.go` (`approximateCut`) |
| `XINFO STREAM`: `radix-tree-keys`/`radix-tree-nodes` — приблизительные | эти поля описывают внутреннее устройство Redis (radix-дерево); у нас поток — это один срез | `internal/server/commands_stream_group.go` (`radixTreeKeys`) |
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
- **Streams** (Redis 8.2):
  - идентификатор записи — `ms-seq`; `XADD` с явным `0-0` → `ERR The ID
    specified in XADD must be greater than 0-0`; идентификатор должен быть
    строго больше верхнего элемента, иначе `ERR The ID specified in XADD is
    equal or smaller than the target stream top item`;
  - `XADD key 5` (без `-seq`) допустим — это `5-0`; `XADD key 5-*` — авто-seq;
    `ms-*` с миллисекундой **меньше** последней записи → ошибка;
  - `XSETID` сравнивается с последней **записью**, а не с `lastID` (который
    сам `XSETID` и поднимает): `XSETID s 100-0` потом `XSETID s 50-0` — ошибка
    только если 50-0 меньше верхней записи;
  - `XRANGE`/`XREVRANGE` отвечают массивом пар `[id, [f, v, f, v …]]`;
    эксклюзивная граница — префикс `(`; `COUNT 0` → **null-массив**;
  - маркеры `~`/`=` у `MAXLEN`/`MINID` распознаются, только если после них
    есть значение; `LIMIT` без `~` → `ERR syntax error, LIMIT cannot be used
    without the special ~ option`;
  - приблизительная обрезка (`~`) в Redis удаляет целые узлы rax, поэтому её
    результат зависит от раскладки — у нас тоже (блоки по 100 записей);
  - в AOF попадает **фактический** идентификатор записи, а `~` переписывается
    в `=` с получившейся длиной — иначе реплей дал бы другой поток.
- **Группы потребителей** (Redis 8.2):
  - `XREADGROUP … >` отдаёт записи потока **после** идентификатора группы и
    кладёт их в список неподтверждённых; `XREADGROUP … <id>` отдаёт
    **собственные** неподтверждённые записи потребителя (и увеличивает число
    доставок), а не записи потока;
  - `XACK` на несуществующей группе/ключе → `0` (не ошибка), а `XPENDING`,
    `XAUTOCLAIM`, `XINFO CONSUMERS` → ошибка `NOGROUP`;
  - `XINFO GROUPS`: `entries-read` и `lag` — **null**, если счётчик группы
    неизвестен (группа создана без `ENTRIESREAD` и ещё ничего не читала);
  - в сводке `XPENDING` число неподтверждённых записей потребителя — **bulk
    строка** (в Redis `addReplyBulkLongLong`), а не целое число: харнесс
    сравнивает типы значений;
  - `XINFO STREAM` без `FULL` — 10 пар, с `FULL` — 9 пар (поля потока, его
    записи и группы); поля `radix-tree-keys`/`radix-tree-nodes` описывают
    внутреннее устройство Redis, у нас они приблизительные (1 и 2 для
    небольших потоков — совпадают);
  - `XGROUP HELP` и `XINFO HELP` отвечают **массивом** строк (по строке на
    элемент), а не одной строкой;
  - `XGROUP` с несуществующим ключом → «ERR The XGROUP subcommand requires
    the key to exist…», а с несуществующей группой → `NOGROUP`;
  - приблизительная обрезка `XTRIM … ~` в Redis удаляет целые узлы rax, поэтому
    её результат зависит от раскладки — у нас тоже (блоки по 100 записей).

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
- Разбор аргументов команды, который нужен и серверу, и реплею AOF, живёт в
  `internal/store` (как `ParseStreamAddArgs`/`ParseStreamTrimArgs`): пакет
  `persist` не может импортировать `server` (цикл), а дублировать разбор
  опасно.
- Сценарий сверки: `testdata/compat/<family>.txt`, новые команды — туда же.
  Проверять поведение лучше живым `redis-cli` против локального
  `redis-server 8.2`: в zsh `~` и `*` expanding — кавычить (`"~"`, `"*"`).
