# Redis на Go — собственная реализация

Учебная реализация Redis на чистой стандартной библиотеке Go (`stdlib`), без сторонних зависимостей. Проект создан для изучения того, как устроен Redis изнутри: протокол RESP, типизированное хранилище, конкурентный TCP-сервер, структуры данных (включая zskiplist).

## Возможности

- **Протокол RESP** — полный парсер и сериализатор: simple strings, errors, integers, bulk strings, arrays, null-значения. Буферизованное чтение, лимиты на размер сообщений, защита от malformed-ввода.
- **Типизированное хранилище** — `string`, `list`, `hash`, `set`, `zset` с проверкой типов и ошибкой `-WRONGTYPE`.
- **Структуры данных**:
  - `list` — собственный двусвязный список (O(1) push/pop с обоих концов);
  - `hash` / `set` — на основе `map`;
  - `zset` — собственный **zskiplist** (dict + skip list), как в настоящем Redis.
- **TTL / истечение** — ленивое (при чтении) + активное (фоновая горутина), `EXPIRE`, `TTL`, `PTTL`, `PERSIST`.
- **Конкурентность** — горутина на каждое соединение + `RWMutex` на хранилище; все тесты гоняются с `-race`.
- **Совместимость** — проверена работа с настоящим `redis-cli` и `redis-benchmark`.

## Быстрый старт

```bash
# Запуск сервера (по умолчанию 127.0.0.1:6379)
go run ./cmd/redis-server

# Другой порт
go run ./cmd/redis-server --host 0.0.0.0 --port 6380
```

Проверка с настоящим клиентом:

```bash
redis-cli PING                    # PONG
redis-cli SET foo bar             # OK
redis-cli GET foo                 # "bar"
redis-cli RPUSH list a b c        # 3
redis-cli LRANGE list 0 -1        # a b c
redis-cli HSET h f1 v1            # 1
redis-cli SADD st m1 m2           # 2
redis-cli ZADD z 1 a 2 b 3 c      # 3
redis-cli ZRANGE z 0 -1 WITHSCORES
redis-cli TYPE foo                # string
```

## Поддерживаемые команды

| Категория | Команды |
|---|---|
| Общие | `PING`, `ECHO`, `QUIT`, `DEL`, `EXISTS`, `TYPE`, `DBSIZE`, `FLUSHDB`, `INFO` |
| Ключи / TTL | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` |
| Строки | `SET` (EX/PX/NX/XX), `GET`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Списки | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET` |
| Хэши | `HSET`, `HGET`, `HGETALL`, `HDEL`, `HLEN`, `HEXISTS`, `HKEYS`, `HVALS` |
| Множества | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Сортированные множества | `ZADD`, `ZRANGE`, `ZREVRANGE` (вкл. `WITHSCORES`), `ZSCORE`, `ZREM`, `ZCARD`, `ZRANK`, `ZREVRANK` |

## Структура проекта

```
├── cmd/redis-server/        # точка входа сервера
└── internal/
    ├── resp/                # протокол RESP: value.go, reader.go, writer.go
    ├── store/               # типизированное хранилище
    │   ├── store.go         # Entry {Type, контейнеры, ExpiresAt}, строковые операции
    │   ├── list.go          # двусвязный список
    │   ├── collections.go   # hash и set на map
    │   ├── skiplist.go      # zskiplist (как в Redis)
    │   ├── zset.go          # zset = dict + skiplist
    │   └── expiry.go        # активное истечение (фоновая горутина)
    ├── server/              # TCP-сервер и обработчики команд
    └── config/              # флаги --host/--port/--dir
```

## Тестирование и качество

```bash
go test -race ./...    # unit + integration тесты
go vet ./...           # статический анализ
```

Нагрузочный тест (если установлен `redis-benchmark`):

```bash
redis-benchmark -t set,get -n 20000 -c 50
```

> Настоящий `redis-cli` работает с сервером как с обычным Redis: интерактивный режим,
> one-shot команды, `--pipe`, `--raw`.

## Бенчмарки и профилирование

Go-бенчмарки покрывают RESP-парсер, хранилище и сетевой цикл сервера:

```bash
go test -bench=. -benchmem ./internal/...
```

Профилирование CPU и памяти через `pprof`:

```bash
go test -run='^$' -bench='BenchmarkServerRoundTripGet' -benchmem \
  -cpuprofile cpu.prof -memprofile mem.prof ./internal/server/
go tool pprof -top cpu.prof          # топ горячих функций
go tool pprof -web cpu.prof          # визуальная диаграмма
```

Пример результата (`Apple M1`): `SET`/`GET` — ~18µs за round-trip по TCP,
`redis-benchmark` — 100k+ запросов/сек. Профиль показывает, что узкое место —
системные вызовы сетевого I/O, логика хранилища не является бутылочным горлышком.

## Roadmap

Актуальный план разработки и прогресс ведётся в [`ROADMAP.md`](ROADMAP.md):

- [x] RESP-протокол
- [x] Типизированное хранилище (string/list/hash/set/zset)
- [x] TCP-сервер, TTL, конкурентность
- [x] Совместимость с `redis-cli`/`redis-benchmark` + бенчмарки/pprof
- [ ] Персистентность: RDB-снапшот и AOF
- [ ] Pub/Sub, транзакции MULTI/EXEC, eviction-политики
- [ ] Репликация, кластер, Lua-скриптинг

## Зачем это

Это обучающий проект: каждый компонент — RESP-парсер, структуры данных, конкурентное хранилище — написан вручную на stdlib, чтобы глубже понять устройство реального Redis. Код сопровождается unit- и integration-тестами.
