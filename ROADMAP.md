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
    │   ├── expiry.go               # Expirer — активная очистка (фоновый тикер)
    │   ├── store_test.go
    │   ├── collections_test.go
    │   ├── skiplist_test.go
    │   └── bench_test.go           # бенчмарки (этап 6)
    ├── server/                     # TCP-сервер и обработчик команд
    │   ├── server.go               # net.Listen, accept-цикл, горутина на conn, диспетчеризация
    │   ├── commands.go             # регистр + общие/служебные команды
    │   ├── commands_string.go      # строковые команды
    │   ├── commands_list.go
    │   ├── commands_hash.go
    │   ├── commands_set.go
    │   ├── commands_zset.go
    │   ├── server_test.go          # integration-тесты (клиент-хелпер)
    │   ├── commands_types_test.go
    │   └── bench_test.go           # бенчмарки (этап 6)
    └── config/                     # парсинг флагов --host/--port/--dir
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

### ⏳ Этап 7 — Персистентность
- [ ] RDB-снапшот: сериализация, загрузка при старте, фоновое сохранение
- [ ] AOF (Append-Only File): запись команд, rewrite, восстановление
- [ ] Конфиг-параметры `--save`, `--appendonly`
- [ ] Тесты: запись → перезапуск → данные на месте

### ⏳ Этап 8 — Продвинутые протокольные фичи
- [ ] Pub/Sub: SUBSCRIBE/PUBLISH/UNSUBSCRIBE/PSUBSCRIBE
- [ ] Транзакции: MULTI/EXEC/DISCARD/WATCH
- [ ] Eviction: `--maxmemory`, allkeys-lru/lfu, volatile-ttl
- [ ] CONFIG GET/SET, CLIENT, SLOWLOG, KEYS/SCAN

### ⏳ Этап 9 — Production-уровень (исследовательский)
- [ ] Репликация master→replica (PSYNC, RDB-передача, failover)
- [ ] Кластер/шардирование (hash slots, routing)
- [ ] Lua-скриптинг (EVAL)
- [ ] Streams (XADD/XRANGE/XREAD, consumer groups)
- [ ] TLS, Unix-сокеты, модули

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
