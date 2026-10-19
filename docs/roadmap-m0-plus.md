# Roadmap M0+ — развитие после этапа 8

Утверждён 2026-10-19. Документ описывает, что сделано на текущий момент и
что можно продолжать развивать дальше.

## 1. Текущее состояние

Этапы 1–8 закрыты (~11 000 строк Go, только stdlib, 0 внешних зависимостей):
RESP-протокол, типизированное хранилище (string/list/hash/set/zset со своим
zskiplist), TTL, TCP-сервер, персистентность (RDB в настоящем формате Redis
с интеропом и AOF), Pub/Sub, транзакции `MULTI`/`EXEC`/`WATCH`, eviction,
диагностика (`KEYS`/`SCAN`/`CONFIG`/`CLIENT`/`SLOWLOG`).

**Покрытие команд: 71 из 267** команд Redis 8.2 (27 %).

## 2. Найденные проблемы (на 2026-10-19)

1. **Нестабильный тест** `TestSubscribeCleanupOnDisconnect`: гонка между
   обнаружением обрыва соединения (в горутине читателя) и `PUBLISH` с
   другого соединения. Обнаружение обрыва по своей природе асинхронно —
   тест теперь ждёт reap'а подписчика.
2. **Глобальный `RWMutex` и эксклюзивные блокировки на чтениях**: все
   store-методы брали `Lock()`, включая чистые чтения, потому что
   `entryLocked` обновляет `lastAccess` (метку LRU). Потолок ~150k rps.
3. **Нет мульти-БД** (`SELECT`), **нет RESP3** (`HELLO`), **нет `AUTH`** —
   блокирует работу с современными клиентами (redis-py, ioredis, node-redis).
4. **Покрытие команд 27 %** — нет `MSET`/`MGET`, `HSCAN`/`SSCAN`/`ZSCAN`,
   `ZRANGEBYSCORE`, `LINSERT`, `SINTERCARD` и многих других.
5. **Ручной список AOF-команд** (`aofWriteCommands`) — новую пишущую команду
   легко забыть внести, и данные потеряются при реплее AOF.

## 3. Целевые метрики

| Метрика | Сейчас | Цель |
|---|---|---|
| Покрытие команд Redis 8.2 | 71 / 267 | ≥ 150 (56 %) |
| Мульти-БД (`SELECT`) | нет | да, 16 БД |
| RESP3 (`HELLO 3`) | нет | полный набор типов |
| Аутентификация | нет | `--requirepass` + `AUTH` |
| Нестабильные тесты | 0 | 0 |
| `GET` в хранилище | 74.2 ns/op | 56.9 ns/op ✅ |

## 4. Вехи

Общий DoD для каждой вехи: `go build` ✅, `go vet` ✅, `go test -race ./...` ✅
(стабильно при `-count=5`), живая проверка настоящим `redis-cli`, обновление
`ROADMAP.md` и `README.md`, ветка `feat/<milestone>` + draft-MR (мержит владелец).

### ✅ M0 — Гигиена и быстрые победы

- [x] Нестабильный тест Pub/Sub: детерминированное ожидание reap'а подписчика.
- [x] Читающие пути `store` переведены на `RLock`; `Entry.lastAccess` →
      `atomic.Int64`; `entryLocked` (write) и `readEntry` (read) разделены.
- [x] Единый реестр команд: `cmdSpec{handler, arity, write, aofSelf, memAdd}`
      вместо разрозненных карт `arity` / `aofWriteCommands` /
      `aofHandlerCommands` / `memoryAddCommands`.
- [x] Тесты: `registry_test.go` (инварианты реестра), `TestConcurrentCollectionAccess`
      (параллельные чтения всех типов контейнеров), `gofmt` по всему репозиторию.
- [x] Замеры до/после — в `BENCHMARKS.md`.

### ⏳ M1 — Батчинг пайплайна + снижение аллокаций

- [ ] `handleConn`: не делать `Flush()` на каждую команду, если в буфере чтения
      уже лежит следующая (сейчас — один syscall на команду).
- [ ] Сократить аллокации в `internal/resp/reader.go` (11–15 allocs/round-trip,
      1500 на батч из 100).
- [ ] Точечная оптимизация zskiplist (`ZADD` отстаёт в 1.57×).
- [ ] Пересчёт `BENCHMARKS.md`.

### ⏳ M2 — Шардирование стора (решать по результатам M0–M1)

- [ ] Разбить `Store.data` на N шардов по хешу ключа, чтобы снять глобальный
      потолок. Риск для `WATCH`/eviction/персистентности — нужен отдельный замер.

### ✅ M3 — `SELECT` и мульти-БД

- [x] `Server.dbs []*store.Store` (16 по умолчанию, флаг `--databases`),
      `client.db int`; `Ctx.Store` подставляется per-команду из БД клиента,
      поэтому обработчики команд не изменились.
- [x] Команда `SELECT` (выбор БД на соединение) и `FLUSHALL`; `INFO` показывает
      секцию `# Keyspace` по всем БД; `CLIENT LIST` — поле `db=`.
- [x] Персистентность: RDB пишет `SELECTDB` перед каждой БД и читает их обратно
      (настоящий Redis 8.2 читает наш дамп с несколькими БД — проверено);
      AOF пишет `SELECT` при смене БД и `FLUSHALL` при переписывании.
- [x] `maxmemory` теперь общий для всех БД: бюджет делится между базами
      пропорционально их размеру.
- [x] БД, которых нет в дампе, пропускаются при загрузке (как в Redis).

### ✅ M4 — `HELLO` + RESP3

- [x] `internal/resp`: типы map (`%`), set (`~`), double (`,`), big number (`(`),
      verbatim (`=`), push (`>`) — чтение и запись, с тестами round-trip.
- [x] `HELLO [2|3]` переключает протокол соединения; в RESP3 ответ — карта, в
      RESP2 — плоский массив; неподдерживаемая версия → `NOPROTO`.
- [x] Различия ответов RESP3, как в Redis: `HGETALL` → карта, `CONFIG GET` →
      карта, `SMEMBERS` → множество, `ZSCORE` → double, `ZRANGE WITHSCORES` →
      массив пар `[member, double]`, Pub/Sub → push-кадры.
- [x] Проверено живым клиентом **redis-py 8.1** (`protocol=3`): ping/set/get,
      zrange withscores, hgetall, smembers, config_get, pipeline, info, scan —
      всё работает.
- [x] Попутно исправлен `HSET` с несколькими парами поле-значение (как в
      Redis 4+), который раньше отвечал ошибкой.

### ✅ M5 — `AUTH` / `--requirepass`

- [x] Флаг `--requirepass`, команда `AUTH [user] pass` (пользователь только
      `default`), `CONFIG GET/SET requirepass` (пустой пароль отключает
      аутентификацию), аргумент `AUTH` в `HELLO`.
- [x] Ошибки как в Redis: `NOAUTH Authentication required.`,
      `WRONGPASS invalid username-password pair or user is disabled.`,
      `ERR Client sent AUTH, but no password is set`.
- [x] Проверено живым клиентом redis-py (пароль верный/неверный).

### ⏳ M6 — Админ-команды v2

- [ ] `COMMAND` (`COUNT`/`DOCS`/`LIST`/`INFO`), `RESET`, `FLUSHALL`, `LASTSAVE`,
      `TIME`, `DEBUG`, `CLIENT` (`KILL`/`REPLY`/`UNBLOCK`/`NO-EVICT`/`INFO`),
      `PUBSUB` (`CHANNELS`/`NUMSUB`/`NUMPAT`), `UNLINK`, `TOUCH`, `RANDOMKEY`.

### ⏳ M7 — Управление ключами

- [ ] `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `DUMP`, `RESTORE`, `EXPIRETIME`,
      `PEXPIRETIME`, `SWAPDB`, `OBJECT ENCODING`, `SORT`.

### ⏳ M8 — Дифф-тест против настоящего Redis

- [ ] Harness: сценарии (`.txt` с командами) → прогон против нашей реализации
      и против локального `redis-server` 8.2 → нормализация и diff ответов.
      Скрипт `scripts/compat-check.sh` + golden-файлы. Используется всеми
      последующими вехами как регрессионный фильтр.

### ⏳ M9 — Строки v2

- [ ] `GETSET`, `GETDEL`, `GETEX`, `SETEX`, `PSETEX`, `SETNX`, `MSET`, `MSETNX`,
      `MGET`, `GETRANGE`/`SUBSTR`, `SETRANGE`, `INCRBYFLOAT`.

### ⏳ M10 — Инкрементальные обходы

- [ ] `HSCAN`, `SSCAN`, `ZSCAN` (курсор + `MATCH`/`COUNT`/`TYPE`).

### ⏳ M11 — Hash v2

- [ ] `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN`, `HMGET`, `HMSET`, `HSETNX`,
      `HRANDFIELD`, `HGETDEL`.

### ⏳ M12 — Set v2

- [ ] `SINTERCARD`, `SMOVE`, `SPOP`, `SRANDMEMBER`, `SMISMEMBER`, `SDIFF`(`_STORE`),
      `SINTER`(`_STORE`), `SUNION`(`_STORE`).

### ⏳ M13 — List v2

- [ ] `LINSERT`, `LREM`, `LTRIM`, `LPOS`, `LMOVE`, `RPOPLPUSH`, `LPUSHX`, `RPUSHX`.

### ⏳ M14 — ZSet v2 (range-семейство)

- [ ] `ZINCRBY`, `ZCOUNT`, `ZMSCORE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`,
      `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZRANGE ... BYSCORE/BYLEX`, `ZLEXCOUNT`,
      `ZREMRANGEBYRANK/SCORE/LEX`, `ZPOPMIN`, `ZPOPMAX`, `ZRANDMEMBER`,
      `ZRANGESTORE`, `ZDIFF`(`_STORE`), `ZUNIONSTORE`, `ZINTERSTORE`, `ZINTERCARD`.

### ⏳ M15 — Блокирующие команды

- [ ] `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BZPOPMIN`, `BZPOPMAX`:
      механизм ожидания на клиенте, таймауты, разблокировка при обрыве.

### ⏳ M16 — Streams: модель данных

- [ ] Новый `TypeStream` в `internal/store`: записи (id `ms-seq`, поля),
      `XADD` (в т.ч. `*`/`ms-*`, `MAXLEN`/`MINID`), `XLEN`, `XRANGE`,
      `XREVRANGE`, `XDEL`, `XTRIM`, `XSETID`.

### ⏳ M17 — Streams: группы и чтение

- [ ] `XREAD` (блокирующий и нет), `XREADGROUP`, `XACK`, `XGROUP`
      (`CREATE`/`DESTROY`/`CREATECONSUMER`/`DELCONSUMER`/`SETID`), `XINFO`,
      `XPENDING`, `XAUTOCLAIM`.

### ⏳ M18 — Streams: персистентность

- [ ] RDB-энкодинг stream (listpack, как в Redis 7+), AOF-пропагация,
      round-trip и интероп-тесты.

### ⏳ M19 — Репликация: каркас

- [ ] `REPLICAOF`/`SLAVEOF`, `ROLE`, секция `replication` в `INFO`, `REPLCONF`
      (`listening-port`, `capa`), handshake `PSYNC`, полный sync (передача RDB).

### ⏳ M20 — Репликация: инкрементальная синхронизация

- [ ] Поток пишущих команд на реплики, буферизация при медленной реплике,
      команда `WAIT`, частичный resync по offset/replication ID, `WAITAOF`.

## 5. Отложено (требуют отдельного решения)

Кластер (hash slots, `MOVED`/`ASK`), Lua (`EVAL` — нужен интерпретатор),
TLS/Unix-сокеты, `ACL`, Geo, HyperLogLog (`PFADD`), битовые операции
(`SETBIT`/`BITCOUNT`), `SORT_RO`.

## 6. Риски

| Риск | Митигация |
|---|---|
| Рефакторинг блокировок ломает инварианты `lastAccess`/eviction | атомарный `lastAccess` + `-race`-тесты на `allkeys-lru` |
| RESP3 — большой протокольный риск | изолировать в `internal/resp`, покрыть round-trip тестами, дифф-харинг из M8 |
| Мульти-БД ломает `WATCH`/eviction/AOF-семантику | `WATCH` остаётся per-store; отдельные тесты на изоляцию БД |
| Поток репликации рассинхронизируется | сначала только full sync; расхождение фиксировать через `WAIT` и сверку набора ключей |
| Рост объёма кода без покрытия | дифф-харинг M8 обязателен до треков Streams и репликации |
