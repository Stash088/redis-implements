# Redis на Go — Roadmap

Репозиторий: собственная реализация Redis на чистой stdlib Go.

## Ключевые решения
- **Конкурентность**: горутина на каждое TCP-соединение + `RWMutex` на хранилище.
- **MVP**: только строковый тип данных.
- **Зависимости**: только stdlib (RESP-парсер и всё остальное пишем сами).
- **Тесты**: unit (парсер, хранилище, команды) + integration (реальное TCP-соединение).

## Структура проекта
```
redis-implements/
├── go.mod                          # module: github.com/amir/redis-implements
├── ROADMAP.md                      # этот файл
├── cmd/redis-server/main.go        # точка входа, запуск сервера
└── internal/
    ├── resp/                       # протокол RESP: парсер и сериализатор
    │   ├── value.go                # типы RESP-значений
    │   ├── reader.go               # RESP-парсер из io.Reader (буферизованный, с лимитами)
    │   ├── writer.go               # сериализация ответов
    │   ├── reader_test.go
    │   ├── writer_test.go
    │   └── bench_test.go           # бенчмарки (этап 6)
    ├── store/                      # in-memory хранилище
    │   ├── store.go                # Entry{Type, контейнеры, ExpiresAt} + строковые операции
    │   ├── list.go                 # двусвязный список (LIST)
    │   ├── collections.go          # HASH и SET (map-based)
    │   ├── skiplist.go             # zskiplist (как в Redis)
    │   ├── zset.go                 # ZSET: dict + skiplist + store-операции
    │   ├── stream.go               # STREAM: записи с id ms-seq + операции (M19)
    │   ├── expiry.go               # Expirer — активная очистка (фоновый тикер)
    │   ├── watch.go                # WATCH/CAS реестр наблюдателей (этап 8)
    │   ├── evict.go                # maxmemory eviction + учёт памяти (этап 8)
    │   ├── store_test.go
    │   ├── collections_test.go
    │   ├── skiplist_test.go
    │   └── bench_test.go           # бенчмарки (этап 6)
    ├── server/                     # TCP-сервер и обработчик команд
    │   ├── server.go               # net.Listen, accept-цикл, горутина на conn, диспетчеризация
    │   ├── client.go               # per-connection state (подписки, MULTI, WATCH, насос Pub/Sub)
    │   ├── pubsub.go               # Pub/Sub hub (каналы/паттерны, доставка)
    │   ├── commands_pubsub.go      # SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH
    │   ├── commands_transaction.go # MULTI/EXEC/DISCARD/WATCH/UNWATCH
    │   ├── slowlog.go              # кольцевой журнал медленных команд
    │   ├── glob.go                 # Redis-glob (*, ?, [...], \\) для KEYS/SCAN/PSUBSCRIBE
    │   ├── commands_admin.go       # KEYS/SCAN/CONFIG/CLIENT/SLOWLOG
    │   ├── persist.go              # SAVE/BGSAVE/BGREWRITEAOF + --save политика
    │   ├── commands.go             # регистр + общие/служебные команды
    │   ├── commands_string.go      # строковые команды
    │   ├── commands_list.go
    │   ├── commands_hash.go
    │   ├── commands_set.go
    │   ├── commands_zset.go
    │   ├── commands_stream.go      # XADD/XLEN/XRANGE/XREVRANGE/XDEL/XTRIM/XSETID (M19)
    │   ├── server_test.go          # integration-тесты (клиент-хелпер)
    │   ├── commands_pubsub_test.go
    │   ├── commands_transaction_test.go
    │   ├── commands_eviction_test.go
    │   ├── commands_admin_test.go
    │   ├── glob_test.go
    │   ├── persist_test.go         # перезапуск: данные на месте (RDB/AOF)
    │   ├── commands_types_test.go
    │   └── bench_test.go           # бенчмарки (этап 6)
    ├── persist/                    # персистентность (этап 7)
    │   ├── rdb.go                  # RDB writer/reader (настоящий формат Redis)
    │   ├── aof.go                  # AOF appender/rewrite/loader + перепись TTL
    │   ├── crc64.go                # CRC-64 (Jones), как в Redis
    │   ├── listpack.go             # декодер listpack (list/hash/set/zset из дампов Redis 7+)
    │   ├── lzf.go                  # декомпрессор LZF
    │   ├── *_test.go               # round-trip, повреждения, интероп с настоящим Redis
    └── config/                     # парсинг флагов --host/--port/--dir/--appendonly/...
        ├── config.go
        └── config_test.go
```


## Статус этапов

### ✅ Этап 1 — Фундамент: RESP-протокол
- [x] `go mod init`, структура пакетов
- [x] Парсер `internal/resp`: `+` / `-` / `:` / `$` (вкл. null `$-1`) / `*` (вкл. null `*-1`)
- [x] Буферизованное чтение, лимиты (bulk/line/array), проверка CRLF
- [x] Сериализатор ответов
- [x] Unit-тесты: round-trip, краевые случаи, чанкованный ввод

### ✅ Этап 2 — Хранилище (строки + TTL)
- [x] `store.Store` на `map[string]*Entry` под `RWMutex`
- [x] `Entry{Value []byte; ExpiresAt time.Time}` — raw-байты без аллокаций строк
- [x] Методы: Get, Set, SetWithTTL, Delete, Exists, Expire, TTL, Persist, IncrBy (с overflow-проверкой), Append, StrLen, Keys, Flush
- [x] Истечение: ленивое (при чтении) + активное (`Expirer` с фоновым тикером)
- [x] Инъекция часов (`SetClock`) для тестов TTL
- [x] Unit-тесты: `-race`, конкурентный доступ, TTL, ленивое/активное истечение

### ✅ Этап 3 — TCP-сервер и команды
- [x] `server.Server`: net.Listen, accept-цикл, `go handleConn(conn)`
- [x] `handleConn`: чтение RESP-массивов → диспетчеризация → ответ
- [x] Регистр команд + `-ERR unknown command`
- [x] Команды: PING, ECHO, SET (EX/PX/NX/XX), GET, DEL, EXISTS, APPEND, STRLEN,
      INCR/DECR/INCRBY/DECRBY, EXPIRE/PEXPIRE, TTL/PTTL, PERSIST, QUIT, DBSIZE, FLUSHDB, INFO
- [x] Настраиваемый логгер (`SetLogger`), тихие логи в тестах
- [x] Integration-тесты через `net.Conn` (`-race`): pipelining, QUIT,
      параллельные клиенты, истечение через активный экспиратор

### ✅ Этап 4 — Первый релиз MVP
- [x] `internal/config`: флаги `--host`, `--port`, `--dir` (+ тесты)
- [x] Проверка с настоящим `redis-cli` и `redis-benchmark`
- [x] `go vet` + `go test -race ./...`
- [x] `redis-cli --pipe` (batch-режим) работает

### ✅ Этап 5 — Объёмные типы данных
- [x] Типизация значения: `Entry.Type` + контейнеры (string/list/hash/set/zset), `ErrWrongType`
- [x] `TYPE` команда; `-WRONGTYPE` при несовпадении типа
- [x] **LIST** (`list.go`): собственный двусвязный список; LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX/LSET
- [x] **HASH** (`collections.go`): HSET/HGET/HGETALL/HDEL/HLEN/HEXISTS/HKEYS/HVALS
- [x] **SET**: SADD/SREM/SMEMBERS/SISMEMBER/SCARD
- [x] **ZSET** (`skiplist.go` + `zset.go`): собственный zskiplist (dict + skip list);
      ZADD/ZRANGE/ZREVRANGE/WITHSCORES/ZSCORE/ZREM/ZCARD/ZRANK/ZREVRANK
- [x] Пустые контейнеры удаляют ключ; SET перезаписывает любой тип
- [x] INCR/APPEND/STRLEN/GET отдают WRONGTYPE на не-строковом ключе
- [x] Unit-тесты: skiplist (вставка/дубликаты/обновление/ранги/удаление), коллекции, WRONGTYPE, конкурентность
- [x] Integration-тесты команд + проверка через настоящий `redis-cli`

### ✅ Этап 6 — Инструменты и удобство разработки
- [x] Свой клиент **не пишем**: используем настоящий `redis-cli`/`redis-benchmark`
      (совместимость с RESP-протоколом проверена: REPL, one-shot, `--pipe`, `--raw`)
- [x] Бенчмарки (`internal/resp`, `internal/store`, `internal/server`) + профилирование `pprof`
- [x] Оптимизация буферизации записи: `resp.NewWriterSize`; замер показал, что для
      типовых нагрузок `bufio` и так флашит при заполнении, поэтому сервер использует
      дефолтный буфер (экономия памяти без потери скорости)

### ✅ Этап 7 — Персистентность
- [x] **RDB** — настоящий формат Redis (version 9, опкоды SELECTDB/RESIZEDB/EXPIRETIME_MS/EOF,
      length-энкодинг 6/14/32/64-bit, CRC-64 Jones). Плоские энкодинги: STRING/LIST/SET/HASH/ZSET_2.
      Атомарное сохранение (tmp + fsync + rename), ленивое удаление протухших при загрузке.
- [x] **Загрузка дампов Redis 7+**: декодер listpack (hash/set/zset listpack, quicklist2)
      и LZF, целочисленные и LZF-энкодинги строк.
- [x] **Интероп с настоящим Redis 8.2** (тест `internal/persist/interop_test.go`):
      реальный `redis-server` загружает наш `dump.rdb`; наш сервер читает дамп, сделанный Redis.
      Лимитация: legacy-типы (ziplist/intset) и hash с TTL полей не поддерживаются (понятная ошибка).
- [x] **AOF**: RESP-журнал пишущих команд, перепись относительных TTL в абсолютные `PEXPIREAT`,
      политики fsync `always|everysec|no`, толерантность к обрезанному хвосту,
      `BGREWRITEAOF` (дамп набора командами + атомарная замена файла).
- [x] **Команды**: `SAVE`, `BGSAVE`, `BGREWRITEAOF`, `EXPIREAT`, `PEXPIREAT`.
- [x] **Конфиг**: `--save <сек>`, `--save-changes <N>` (фоновая политика),
      `--appendonly`, `--appendfsync`, `--dbfilename`, `--appendfilename`.
- [x] **Тесты**: запись → перезапуск → данные на месте (все типы, TTL), повреждённые файлы,
      checksum, обрезанный AOF-хвост, перепись TTL при AOF-загрузке.

### ✅ Этап 8 — Продвинутые протокольные фичи
- [x] **Per-connection state** (`client.go`): подписки, очередь MULTI, WATCH-набор,
      имя/id клиента; сериализация записи между command loop и насосом Pub/Sub (writeMu).
- [x] **Pub/Sub**: `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBLISH`;
      hub каналов/паттернов, push-сообщения через горутину-насос, отключение медленного
      подписчика; режим подписки ограничивает набор команд (как в Redis), `PING`
      в подписке отвечает `["pong", msg]`.
- [x] **Транзакции**: `MULTI`/`EXEC`/`DISCARD`; queue-time валидация (unknown command,
      arity-таблица, no-MULTI команды) → `-EXECABORT`; runtime-ошибки не прерывают EXEC;
      команды из очереди прогоняются через общий путь (AOF-пропагация, eviction).
- [x] **WATCH (CAS)**: реестр наблюдателей в store (`watch.go`) — модификация любого
      watch-ключа (включая ленивое истечение и удаление) прерывает EXEC с null-ответом;
      повторный WATCH сбрасывает флаг (работают retry-петли); memory-O(1) на клиента.
- [x] **Eviction**: приблизительный учёт памяти (`usedBytes`, cost на Entry),
      `Entry.lastAccess` для LRU; `--maxmemory` (суффиксы k/m/g), `--maxmemory-policy`
      (noeviction/allkeys-lru/volatile-lru/allkeys-random/volatile-random/volatile-ttl),
      `--maxmemory-samples`; сэмплирующий evict-цикл; OOM-ошибка при noeviction.
- [x] **Серверные команды**: `KEYS` (glob), `SCAN` (курсор по отсортированному снапшоту,
      `MATCH`/`COUNT`), `CONFIG GET`/`CONFIG SET` (maxmemory*, slowlog-*), `CLIENT ID/LIST/
      SETNAME/GETNAME`, `SLOWLOG GET/LEN/RESET` (`--slowlog-log-slower-than`, `--slowlog-max-len`).
- [x] **INFO**: секции Memory (`used_memory`/`maxmemory*`), `evicted_keys`,
      `pubsub_channels/patterns/messages`.
- [x] **Проверено с настоящим redis-cli**: два подписчика + публикация, pmessage,
      MULTI/EXEC/WATCH-аборт через один коннект, `redis-benchmark` больше не WARNINGит
      про CONFIG (135k+ RPS set/get), eviction на живом сервере (2kb → 11 выслано), OOM.
- Лимитации: evicted ключи не пишутся в AOF; SCAN строит отсортированный снапшот за O(n log n);
  LFU-политики не реализованы; CLIENT KILL не поддерживается.

### ⏳ Этап 9 — Production-уровень (исследовательский)
- [ ] Репликация master→replica (PSYNC, RDB-передача, failover)
- [ ] Кластер/шардирование (hash slots, routing)
- [ ] Lua-скриптинг (EVAL)
- [ ] Streams: группы потребителей и чтение (`XREAD`/`XREADGROUP`, `XACK`, `XGROUP`, `XPENDING`)
      — модель данных сделана в вехе **M19**
- [ ] TLS, Unix-сокеты, модули

### 🚀 Развитие после этапа 8 — вехи M0+

Подробный план (утверждён 2026-10-19) лежит в
[`docs/roadmap-m0-plus.md`](docs/roadmap-m0-plus.md), текущее состояние и
список тонкостей Redis — в [`docs/HANDOFF.md`](docs/HANDOFF.md). Каждая веха —
отдельная ветка `feat/<milestone>` + draft-MR; мержит владелец.

| Веха | Содержание | Статус |
|---|---|---|
| **M0** | Гигиена и быстрые победы: нестабильный тест Pub/Sub, чтения на `RLock`, единый реестр команд | ✅ |
| M1 | Батчинг пайплайна + снижение аллокаций в RESP-парсере | ✅ |
| M2 | Шардирование стора — **отложено** (после M1 узкое место — syscall'ы, а не блокировка) | ⏳ |
| M3 | `SELECT` и мульти-БД (16 баз) | ✅ |
| M4 | `HELLO` + RESP3 (map/set/double/bignum/verbatim/push) | ✅ |
| M5 | `AUTH` / `--requirepass` | ✅ |
| M6 | Админ-команды v2: `COMMAND`, `RESET`, `CLIENT INFO/KILL/REPLY`, `PUBSUB CHANNELS`, `UNLINK`, `TOUCH`, `RANDOMKEY`, `TIME`, `DEBUG` | ✅ |
| M7 | Управление ключами — сделано в вехе **M16** | ✅ |
| M8 | Харнесс сверки — сделан в вехе **M18** | ✅ |
| M9 | Строки v2: `MSET`/`MSETNX`/`MGET`, `GETSET`, `GETDEL`, `GETEX`, `SETEX`/`PSETEX`, `SETNX`, `GETRANGE`, `SETRANGE`, `INCRBYFLOAT` | ✅ |
| M10 | Инкрементальные обходы: `HSCAN`, `SSCAN`, `ZSCAN` | ✅ |
| M11 | Hash v2: `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`, `HMGET`, `HMSET`, `HSETNX`, `HRANDFIELD`, `HGETDEL` | ✅ |
| M12 | Set v2: `SINTERCARD`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SMISMEMBER`, `SDIFF`/`SINTER`/`SUNION` + `_STORE` | ✅ |
| M13 | List v2: `LINSERT`, `LREM`, `LTRIM`, `LMOVE`, `RPOPLPUSH`, `LPOS`, `LPUSHX`/`RPUSHX` | ✅ |
| M14 | ZSet v2 (часть 1): `ZRANGEBYSCORE`/`BYLEX`, `ZINCRBY`, `ZCOUNT`, `ZMSCORE`, `ZLEXCOUNT`, `ZRANGE BYSCORE/BYLEX/REV` | ✅ |
| M15 | ZSet v2 (часть 2): `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFF`/`ZINTERCARD`, `ZREMRANGEBY*`, `ZPOPMIN`/`ZPOPMAX`, `ZRANDMEMBER`, `ZRANGESTORE` | ✅ |
| M16 | Управление ключами: `RENAME`/`RENAMENX`, `COPY`, `MOVE`, `DUMP`/`RESTORE`, `EXPIRETIME`, `SWAPDB`, `OBJECT`, `SORT` | ✅ |
| M17 | Блокирующие команды: `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX` | ✅ |
| M18 | Харнесс сверки с настоящим Redis (`cmd/compat-check` + `testdata/compat`) | ✅ |
| M19 | Streams: модель данных (`XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`, `XTRIM`, `XSETID`) | ✅ |
| M20 | Streams: группы и чтение (`XREADGROUP`, `XACK`, `XGROUP`, `XPENDING`) | ⏳ |
| M21 | Streams: персистентность (RDB-энкодинг с полной интероп-совместимостью, AOF) | ⏳ |
| M22 | Репликация: каркас (`REPLICAOF`, `PSYNC`, передача RDB) | ⏳ |
| M23 | Репликация: инкрементальная синхронизация (`WAIT`, partial resync) | ⏳ |

Отложено до отдельного решения: кластер, Lua (`EVAL`), TLS/Unix-сокеты, `ACL`,
Geo, HyperLogLog, битовые операции.

## Контроль качества
- `go test -race ./...` — после каждого этапа
- `go vet ./...` — статический анализ
- `golangci-lint` (опционально, позже)
- Бенчмарки + `pprof` — начиная с этапа 6

## Проверка с реальным redis-cli
```bash
go run ./cmd/redis-server --port 6379
redis-cli -p 6379 PING          # -> PONG
redis-cli -p 6379 SET foo bar   # -> OK
redis-cli -p 6379 GET foo       # -> bar
```
