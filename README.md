# Redis на Go — собственная реализация

Учебная реализация Redis на чистой стандартной библиотеке Go (`stdlib`), без сторонних зависимостей. Проект создан для изучения того, как устроен Redis изнутри: протокол RESP, типизированное хранилище, конкурентный TCP-сервер, структуры данных (включая zskiplist).

## Возможности

- **Протокол RESP** — полный парсер и сериализатор: simple strings, errors, integers, bulk strings, arrays, null-значения. Буферизованное чтение, лимиты на размер сообщений, защита от malformed-ввода.
- **RESP3** — `HELLO 3` переключает соединение на RESP3: карты, множества, double, big number, verbatim string, push-кадры. Проверено живым клиентом redis-py.
- **Типизированное хранилище** — `string`, `list`, `hash`, `set`, `zset` с проверкой типов и ошибкой `-WRONGTYPE`.
- **Структуры данных**:
  - `list` — собственный двусвязный список (O(1) push/pop с обоих концов);
  - `hash` / `set` — на основе `map`;
  - `zset` — собственный **zskiplist** (dict + skip list), как в настоящем Redis.
- **TTL / истечение** — ленивое (при чтении) + активное (фоновая горутина), `EXPIRE`, `TTL`, `PTTL`, `PERSIST`.
- **Pub/Sub** — `SUBSCRIBE`/`PUBLISH`/`PSUBSCRIBE` с glob-паттернами; push-сообщения доставляются
  отдельной горутиной «насоса», медленные подписчики отключаются (как в Redis).
- **Транзакции** — `MULTI`/`EXEC`/`DISCARD` с очередью команд и полноценным **WATCH** (check-and-set):
  изменение watch-ключа между `WATCH` и `EXEC` прерывает транзакцию (null-ответ).
- **Eviction** — `--maxmemory` с политиками `noeviction`, `allkeys-lru`, `volatile-lru`,
  `allkeys-random`, `volatile-random`, `volatile-ttl` и сэмплированием (`--maxmemory-samples`).
- **Персистентность** — RDB-снапшоты в **настоящем формате Redis** (интероп с реальным `redis-server`),
  AOF-журнал команд с переписью TTL, `SAVE`/`BGSAVE`/`BGREWRITEAOF`.
- **Мульти-БД** — `SELECT` переключает базу на соединении (16 баз по умолчанию,
  `--databases N`); изоляция баз, общий лимит `maxmemory`, RDB/AOF хранят все базы.
- **Диагностика** — `KEYS`/`SCAN` (курсор + `MATCH`/`COUNT`), `CONFIG GET/SET`, `CLIENT ID/LIST/SETNAME/GETNAME`,
  `SLOWLOG GET/LEN/RESET` с настраиваемым порогом.
- **Конкурентность** — горутина на каждое соединение + `RWMutex` на хранилище; все тесты гоняются с `-race`.
- **Аутентификация** — `--requirepass` и команда `AUTH` (`AUTH user pass`): без
  пароля любой, кто достучился до порта, управляет сервером.
- **Совместимость** — проверена работа с настоящим `redis-cli`, `redis-benchmark`
  и клиентом `redis-py` (в том числе в режиме RESP3).

## Быстрый старт

```bash
# Запуск сервера (по умолчанию 127.0.0.1:6379)
go run ./cmd/redis-server

# Другой порт
go run ./cmd/redis-server --host 0.0.0.0 --port 6380

# С персистентностью (RDB по политике --save, AOF включён)
go run ./cmd/redis-server --dir ./data --save 60 --save-changes 1000 --appendonly

# Число баз данных (по умолчанию 16)
go run ./cmd/redis-server --databases 4

# С ограничением памяти и eviction
go run ./cmd/redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

# С паролем (клиент подключается с redis-cli -a <пароль>)
go run ./cmd/redis-server --requirepass mysecret
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

# Несколько баз данных
redis-cli -n 3 SET k v            # записать в базу 3
redis-cli -n 3 GET k              # прочитать из базы 3
redis-cli SELECT 3                # переключить базу вручную
```

## Развёртывание в Docker / на VPS

Сборка образа (multi-stage, статический бинарь на `alpine:3.20`, ~8-9 МБ):

```bash
docker build -t redis-implements .
```

Запуск контейнера (AOF `everysec` + RDB-политика `--save 60 --save-changes 1`, данные в volume `/data`):

```bash
docker run -d --name redis-impl --restart unless-stopped \
    -p 6379:6379 -v /var/lib/redis-implements:/data redis-implements
```

Проверка работоспособности с любого хоста:

```bash
redis-cli -h <VPS_IP> PING               # PONG
redis-cli -h <VPS_IP> SET hello world    # OK
redis-cli -h <VPS_IP> GET hello          # world
redis-cli -h <VPS_IP> RPUSH l a b c
redis-cli -h <VPS_IP> LRANGE l 0 -1      # a b c
redis-cli -h <VPS_IP> HSET h f v
redis-cli -h <VPS_IP> ZADD z 1 a 2 b
redis-cli -h <VPS_IP> INFO
redis-benchmark -h <VPS_IP> -t set,get -n 20000 -c 50
```

Проверка персистентности через перезапуск:

```bash
redis-cli -h <VPS_IP> SET survived 1
docker restart redis-impl
redis-cli -h <VPS_IP> GET survived       # 1
```

Контейнер останавливается корректно: по `docker stop` (SIGTERM) делается финальный RDB-сейв
и аккуратно закрывается AOF. Healthcheck (`docker inspect ... .State.Health.Status`) шлёт
RESP `PING` через busybox `nc`.

> **Безопасность:** аутентификация включается флагом `--requirepass <пароль>`
> (или `CONFIG SET requirepass`). Без него любой, кто достучится до порта 6379,
> управляет сервером. Не публикуйте порт наружу без ограничений. Варианты:
> SSH-туннель (`ssh -L 6379:127.0.0.1:6379 user@VPS`), firewall только для
> вашего IP (`sudo ufw allow from <ваш_IP> to any port 6379`) и `--requirepass`.

## Поддерживаемые команды

| Категория | Команды |
|---|---|
| Общие | `PING`, `ECHO`, `QUIT`, `DEL`, `EXISTS`, `TYPE`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `INFO` |
| Протокол | `HELLO` |
| Аутентификация | `AUTH` |
| Базы данных | `SELECT` |
| Ключи / TTL | `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `SCAN` (MATCH/COUNT) |
| Обходы коллекций | `HSCAN`, `SSCAN`, `ZSCAN` (MATCH/COUNT, NOVALUES у HSCAN) |
| Персистентность | `SAVE`, `BGSAVE`, `BGREWRITEAOF` |
| Строки | `SET` (EX/PX/NX/XX), `GET`, `APPEND`, `STRLEN`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `INCRBYFLOAT`, `MSET`, `MSETNX`, `MGET`, `GETSET`, `GETDEL`, `GETEX`, `SETEX`, `PSETEX`, `SETNX`, `GETRANGE`, `SETRANGE` |
| Списки | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `RPOPLPUSH`, `LPUSHX`, `RPUSHX` |
| Хэши | `HSET`, `HGET`, `HGETALL`, `HDEL`, `HLEN`, `HEXISTS`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`, `HMGET`, `HMSET`, `HSETNX`, `HRANDFIELD`, `HGETDEL` |
| Множества | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SMISMEMBER`, `SINTERCARD`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SDIFF`, `SINTER`, `SUNION` (+ варианты `_STORE`) |
| Сортированные множества | `ZADD`, `ZRANGE`, `ZREVRANGE` (вкл. `WITHSCORES`), `ZSCORE`, `ZREM`, `ZCARD`, `ZRANK`, `ZREVRANK` |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH` |
| Транзакции | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Сервер / диагностика | `CONFIG GET/SET`, `CLIENT ID/LIST/INFO/SETNAME/GETNAME/KILL/REPLY`, `SLOWLOG GET/LEN/RESET`, `COMMAND COUNT/INFO/LIST/DOCS`, `RESET`, `TIME`, `LASTSAVE`, `RANDOMKEY`, `TOUCH`, `UNLINK`, `DEBUG` |
| Pub/Sub (диагностика) | `PUBSUB CHANNELS/NUMSUB/NUMPAT` |

## Pub/Sub и транзакции

```bash
# Терминал 1 — подписчик (каналы и glob-паттерны)
redis-cli SUBSCRIBE news sports
redis-cli PSUBSCRIBE 'user:*'

# Терминал 2 — публикация (возвращает число получателей)
redis-cli PUBLISH news "hello"          # 1
redis-cli PUBLISH user:42 alice         # 1 (по паттерну)

# Транзакция в одном соединении
redis-cli
> MULTI
> SET k1 v1
> INCR hits
> EXEC                   # [OK, 1] — обе команды атомарно

# WATCH: изменение ключа между WATCH и EXEC прерывает транзакцию
> WATCH k1
> MULTI
> SET k1 changed
> EXEC                   # (nil) — ключ изменил кто-то ещё
```

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
    │   ├── expiry.go        # активное истечение (фоновая горутина)
    │   └── snapshot.go      # ForEach/RestoreEntry для персистентности
    ├── server/              # TCP-сервер и обработчики команд (вкл. SAVE/BGSAVE/BGREWRITEAOF)
    ├── persist/             # RDB (настоящий формат) + AOF + CRC64 + listpack/LZF
    └── config/              # флаги --host/--port/--dir/--save/--appendonly/...
```

## Персистентность

- **RDB** — настоящий формат Redis (version 9): опкоды `SELECTDB`/`RESIZEDB`/`EXPIRETIME_MS`/`EOF`,
  length-энкодинг, CRC-64 (Jones). Сохранение атомарное (tmp + fsync + rename).
  Реальный `redis-server` загружает наши дампы, а наш сервер — дампы Redis 7+ (listpack, LZF, int-энкодинги).
  Лимитация загрузчика: legacy-типы (ziplist/intset) и hash с TTL полей не поддерживаются.
- **AOF** — RESP-журнал пишущих команд. Относительные TTL (`SET … EX/PX`, `EXPIRE`, `PEXPIRE`)
  пишутся как абсолютные `PEXPIREAT`, поэтому при загрузке сроки жизни не «съезжают».
  `--appendfsync always|everysec|no`; обрезанный хвост при загрузке игнорируется.
- **Политика `--save`**: фоновая горутина делает `SAVE` раз в секунду, если прошло ≥ `--save` секунд
  и накоплено ≥ `--save-changes` изменений.
- **Приоритет загрузки**: если `--appendonly` включён и AOF существует — грузится AOF, иначе RDB.
- Файлы лежат в `--dir` (`dump.rdb`, `appendonly.aof` по умолчанию).

## Ограничение памяти (eviction)

```bash
go run ./cmd/redis-server --maxmemory 100mb --maxmemory-policy allkeys-lru --maxmemory-samples 5
```

- Учёт памяти приблизительный: длина ключа + payload (+ overhead на элемент коллекции).
- `noeviction` (по умолчанию) — при переполнении memory-добавляющие команды получают
  `-OOM command not allowed when used memory > 'maxmemory'.` (чтение и удаление работают).
- `allkeys-lru` / `volatile-lru` — жертва выбирается по времени последнего доступа среди
  `--maxmemory-samples` случайных ключей; `*-random` — случайно; `volatile-ttl` — с ближайшим TTL;
  `volatile-*` трогают только ключи с TTL.
- Высланные ключи в AOF не пропагируются (осознанное упрощение: при реплее набор может отличаться).

## Диагностика

```bash
CONFIG GET maxmemory*                  # flattened [name, value, ...] по glob-паттерну
CONFIG SET maxmemory 200mb             # maxmemory / maxmemory-policy / slowlog-* на лету
CLIENT LIST                            # id, addr, name, sub/psub/multi/watch, последняя команда
SLOWLOG GET 10                         # последние медленные команды (порог в µs)
SLOWLOG LEN / SLOWLOG RESET
```

Порог и длина журнала: `--slowlog-log-slower-than` (µs, `-1` — выключено) и `--slowlog-max-len`.

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
- [x] Персистентность: RDB (настоящий формат) и AOF
- [x] Pub/Sub, транзакции MULTI/EXEC/WATCH, eviction, KEYS/SCAN/CONFIG/CLIENT/SLOWLOG
- [x] Мульти-БД: SELECT, FLUSHALL, изоляция баз, общий maxmemory, RDB/AOF по всем базам
- [ ] Репликация, кластер, Lua-скриптинг, Streams, TLS

## Зачем это

Это обучающий проект: каждый компонент — RESP-парсер, структуры данных, конкурентное хранилище — написан вручную на stdlib, чтобы глубже понять устройство реального Redis. Код сопровождается unit- и integration-тестами.
