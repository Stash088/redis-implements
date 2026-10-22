# Roadmap M0+ — развитие после этапа 8

Утверждён 2026-10-19. Документ описывает, что сделано на текущий момент и
что можно продолжать развивать дальше. Актуально на 2026-10-22: сделаны вехи
M0–M20 (кроме отложенной M2), покрытие команд — 177 из 267.

## 1. Текущее состояние

Этапы 1–8 закрыты (~11 000 строк Go, только stdlib, 0 внешних зависимостей):
RESP-протокол, типизированное хранилище (string/list/hash/set/zset со своим
zskiplist), TTL, TCP-сервер, персистентность (RDB в настоящем формате Redis
с интеропом и AOF), Pub/Sub, транзакции `MULTI`/`EXEC`/`WATCH`, eviction,
диагностика (`KEYS`/`SCAN`/`CONFIG`/`CLIENT`/`SLOWLOG`).

**Покрытие команд: 177 из 267** команд Redis 8.2 (66 %) — на 2026-10-19 было
71 из 267 (27 %).

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

### ✅ M1 — Батчинг пайплайна + снижение аллокаций

- [x] `handleConn`: `Flush()` не делается на каждую команду, если в буфере чтения
      уже лежит следующая (один syscall на команду → один на батч).
- [x] Аллокации в `internal/resp/reader.go` сокращены: `readLine` не копирует
      строку (8 → 4 allocs/round-trip).
- [x] Точечная оптимизация zskiplist.
- [x] Пересчёт `BENCHMARKS.md`: под пайплайном разрыв с Redis 8.2 закрыт
      (0.23× → 1.00–1.06×).

### ⏳ M2 — Шардирование стора (отложено осознанно)

После M1 узкое место — не блокировка, а syscall'ы на каждое соединение, поэтому
шардирование `Store.data` отложено: сначала M3–M18 (совместимость и покрытие
команд). Вернуться к нему, если профиль покажет потолок на `RWMutex`.

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

### ✅ M6 — Админ-команды v2

- [x] `COMMAND` (`COUNT`/`INFO`/`LIST`/`DOCS`) с таблицей метаданных
      (флаги, позиции ключей, группа, краткое описание — `command_meta.go`).
- [x] `RESET` (сбрасывает транзакцию, подписки, имя и базу; внутри MULTI —
      ошибка, как в Redis), `LASTSAVE`, `TIME`, `RANDOMKEY`, `TOUCH`, `UNLINK`,
      `DEBUG` (`SLEEP`/`JMAP`/`OBJECT`).
- [x] `CLIENT`: `INFO`, `KILL ID|ADDR`, `REPLY ON|OFF|SKIP`, `NO-EVICT`,
      `NO-TOUCH`, `UNBLOCK`.
- [x] `PUBSUB`: `CHANNELS`, `NUMSUB`, `NUMPAT`.

### ✅ M7 — Управление ключами (сделано в вехе **M16**)

- [x] `RENAME`, `RENAMENX`, `COPY`, `MOVE`, `DUMP`, `RESTORE`, `EXPIRETIME`,
      `PEXPIRETIME`, `SWAPDB`, `OBJECT ENCODING`, `SORT` — подробности в M16.

### ✅ M8 — Харнесс сверки (сделан в вехе **M18**)

- [x] `cmd/compat-check` + сценарии `testdata/compat/*.txt` + скрипт
      `scripts/compat-check.sh` — подробности в M18.

### ✅ M9 — Строки v2

- [x] `MSET`, `MSETNX`, `MGET`, `GETSET`, `GETDEL`, `GETEX` (EX/PX/EXAT/PXAT/
      PERSIST), `SETEX`, `PSETEX`, `SETNX`, `GETRANGE`, `SETRANGE`,
      `INCRBYFLOAT`.
- [x] Формат float как в Redis: точное десятичное значение с 17 значащими
      цифрами (`10.5 + 0.1` → `10.59999999999999964`), без экспоненты.
- [x] AOF: разбор новых команд и переписывание относительных TTL
      (`SETEX`/`PSETEX` → `SET` + `PEXPIREAT`).
- [x] Попутно исправлено: `GETRANGE`/`GETSET`/`GETDEL`/`GETEX` на ключе
      другого типа возвращали nil вместо `WRONGTYPE`.

### ✅ M10 — Инкрементальные обходы

- [x] `HSCAN`, `SSCAN`, `ZSCAN` (курсор + `MATCH`/`COUNT`, `NOVALUES` только у
      `HSCAN` — как в Redis). Логика курсора вынесена из `SCAN` в общие
      помощники (`parseScanOptions`, `scanRange`).
- [x] Хранилище: `ScanHash`, `ScanSet`, `ScanZSet` — члены под read-локом,
      для хэша и множества отсортированы, для zset — по score.
- [x] Сценарий сверки `testdata/compat/scan.txt` (данные для харнесса M18).

### ✅ M11 — Hash v2

- [x] `HINCRBY`, `HINCRBYFLOAT` (формат float — как у Redis), `HSTRLEN`,
      `HMGET`, `HMSET`, `HSETNX`, `HRANDFIELD` (`count`/`WITHVALUES`),
      `HGETDEL` (синтаксис Redis 8: `key FIELDS numfields field…`).
- [x] Хранилище: `HashIncrBy`, `HashIncrByFloat`, `HashStrLen`, `HashMGet`,
      `HashSetNX`, `HashRandomField(s)`, `HashGetDel`.
- [x] AOF: разбор новых команд; сценарий сверки `testdata/compat/hash.txt`.
- [ ] Поле-TTL команды (`HEXPIRE`/`HTTL`/`HSETEX`) не делаем: они требуют
      поддержки TTL полей в RDB, которой нет — иначе данные терялись бы при
      сохранении. Зафиксировано как ограничение.

### ✅ M12 — Set v2

- [x] `SINTERCARD` (с `LIMIT`), `SMOVE`, `SPOP` (`count`), `SRANDMEMBER`
      (`count`), `SMISMEMBER`, `SDIFF`/`SDIFFSTORE`, `SINTER`/`SINTERSTORE`,
      `SUNION`/`SUNIONSTORE`.
- [x] В RESP3 `SDIFF`/`SINTER`/`SUNION`/`SPOP`/`SRANDMEMBER` отдаются
      множеством — как в Redis.
- [x] Хранилище: `SetIntersectCard`, `SetMove`, `SetPop`, `SetRandomMembers`,
      `SetIsMemberMany`, `SetDiff`, `SetInter`, `SetUnion`, `SetStore`.
- [x] AOF: разбор новых команд; сценарий сверки `testdata/compat/set.txt`.

### ✅ M13 — List v2

- [x] `LINSERT` (BEFORE/AFTER), `LREM` (count), `LTRIM`, `LPOS`
      (RANK/COUNT/MAXLEN), `LMOVE`, `RPOPLPUSH`, `LPUSHX`, `RPUSHX`.
- [x] Семантика как в Redis: `LINSERT` возвращает -1, когда pivot не найден,
      и 0, когда нет ключа; `COUNT` в `LPOS` не может быть отрицательным,
      `MAXLEN` ограничивает окно **просмотра**, а не число ответов; при
      заданном `COUNT` ответ — массив, без него — число.
- [x] Список: `IndexOfValue`, `InsertBefore`/`InsertAfter`, `RemoveAt`.
- [x] Хранилище: `ListInsert`, `ListRemove`, `ListTrim`, `ListPosition`,
      `ListMove`, `ListPushExist`.
- [x] AOF: разбор новых команд; сценарий сверки `testdata/compat/list.txt`.

### ✅ M14 — ZSet v2, часть 1: score/lex-семейство

- [x] `ZINCRBY`, `ZCOUNT`, `ZMSCORE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`,
      `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZLEXCOUNT`, `ZRANGE … BYSCORE|BYLEX|REV|LIMIT`.
- [x] Границы как в Redis: `(1` — исключительно, `-inf`/`+inf`, `[a`/`(a` — lex;
      с `REV` границы меняются местами, без `BYSCORE`/`BYLEX` индексы
      применяются к обратному порядку множества.
- [x] Хранилище: `ZRangeByScore`, `ZRangeByLex`, `ZCount`, `ZLexCount`,
      `ZIncrBy`, `ZMScore`, `ZLen`; в `ZSet` — `RangeByScore`, `RangeByLex`,
      `CountByScore`, `CountByLex`.
- [x] AOF: разбор `ZINCRBY`; сценарий сверки `testdata/compat/zset.txt`.

### ✅ M15 — ZSet v2, часть 2: операции и выборка

- [x] `ZUNIONSTORE`, `ZINTERSTORE` (WEIGHTS/AGGREGATE SUM|MIN|MAX), `ZDIFF`,
      `ZDIFFSTORE`, `ZINTERCARD` (LIMIT), `ZREMRANGEBYRANK`/`BYSCORE`/`BYLEX`,
      `ZPOPMIN`, `ZPOPMAX`, `ZRANDMEMBER`, `ZRANGESTORE`.
- [x] Ошибки как в Redis: `ERR at least 1 input key is needed for '<cmd>' command`,
      `ERR syntax error` для неизвестного агрегата.
- [x] `ZRANGESTORE` без `BYSCORE`/`BYLEX` работает с индексами рангов.
- [x] Хранилище: `ZUnionStore`, `ZInterStore`, `ZDiff`, `ZDiffStore`,
      `ZInterCard`, `ZRemRangeByRank/Score/Lex`, `ZPop`, `ZRandomMembers`,
      `ZRangeStoreByRank/Score/Lex`; в `ZSet` — `entryAt`.

### ✅ M16 — Управление ключами

- [x] `RENAME`, `RENAMENX`, `COPY` (`DB`/`REPLACE`), `MOVE`, `DUMP`, `RESTORE`,
      `EXPIRETIME`, `PEXPIRETIME`, `SWAPDB`, `OBJECT` (`ENCODING`/`REFCOUNT`/
      `IDLETIME`/`FREQ`), `SORT` (`ASC`/`DESC`, `ALPHA`, `LIMIT`, `STORE`, `BY`,
      `GET`).
- [x] `DUMP`/`RESTORE`: payload — сериализованное значение (формат RDB + CRC64)
      с экспортируемыми `persist.DumpValue`/`persist.RestoreValue`. Формат
      payload'а наш: настоящий Redis его не восстановит — отмечено как
      ограничение.
- [x] `TTL` теперь округляется до ближайшей секунды, как в Redis.
- [x] `SORT` без `ALPHA` требует, чтобы все элементы были числами, иначе
      `ERR One or more scores can't be converted into double` — как в Redis.
      С `BY` вес, которого нет, считается нулём; `GET` подставляет значения
      найденных ключей.
- [x] AOF: разбор `RENAME`/`RENAMENX`/`RESTORE`; сценарий сверки
      `testdata/compat/keys.txt`.

### ✅ M17 — Блокирующие команды

- [x] `BLPOP`, `BRPOP`, `BLMOVE`, `BZPOPMIN`, `BZPOPMAX`.
- [x] Механизм: реестр ожидающих клиентов (`internal/server/blocking.go`),
      писающая команда будит ожидающих ключа; клиент просыпается и повторяет
      команду. Ожидание с таймаутом (`0` — ждать вечно) и разблокировка при
      обрыве соединения.
- [x] Пока клиент ждёт, он не читает новые команды (как в Redis), но
      `writeMu` отпускается — насос Pub/Sub продолжает работать.
- [x] Ответы как в Redis: `BLPOP`/`BRPOP` → `[key, value]`,
      `BZPOPMIN`/`BZPOPMAX` → `[key, member, score]`, по таймауту — null-массив.
- [ ] Ограничение: обрыв соединения заблокированного клиента замечается
      только по таймауту (как и в Redis, где помогает `CLIENT KILL`).

### ✅ M18 — Харнесс сверки

- [x] Инструмент `cmd/compat-check` (Go, без зависимостей): подключается к
      двум серверам, выполняет сценарии и сравнивает **декодированные значения
      RESP (типы + значения)**. Перед каждым сценарием базы очищаются, так что
      сценарии независимы.
- [x] Режимы сравнения в сценариях: `~` — волатильный ответ (не сравнивается:
      время, случайные значения, формат `DUMP`), `%` — элементы ответа
      сравниваются как мультимножество (порядок в хэше/множестве не задан),
      `=` — ответ курсора собирается целиком (границы батчей SCAN у разных
      реализаций разные).
- [x] Сценарии — `testdata/compat/*.txt`: blocking, hash, keys, list, scan,
      set, zset.
- [x] Скрипт `scripts/compat-check.sh` поднимает оба сервера и запускает
      сравнение.
- [x] Харнесс нашёл реальные расхождения, которые исправлены: `HGETDEL`
      возвращает массив удалённых значений (а не число), `SORT … BY/GET` —
      полноценная поддержка, ошибки `OBJECT HELP` и `SORT` без `ALPHA`.

**Текущее состояние: все 8 сценариев совпадают с настоящим Redis 8.2.**

### ✅ M19 — Streams: модель данных

- [x] Новый `TypeStream` в `internal/store`: записи с идентификатором `ms-seq`,
      генерация идентификаторов (`*` — от времени, `ms-*` — с автоматической
      последовательностью), `lastID`, `maxDeletedID`, `entriesAdded`.
- [x] Команды: `XADD` (в т.ч. `*`/`ms-*`, `MAXLEN`/`MINID` с `~`/`=`, `LIMIT`,
      `NOMKSTREAM`), `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`, `XTRIM`, `XSETID`.
- [x] Семантика как в Redis 8.2: идентификатор `0-0` отвергается, идентификатор
      должен быть строго больше верхнего элемента, `XSETID` сравнивается с
      верхним **элементом** (а не с `lastID`), эксклюзивные границы `(id`,
      `COUNT 0` → null-массив, `XRANGE` отвечает массивом пар `[id, [f, v, …]]`.
- [x] AOF: разбор `XADD`/`XDEL`/`XTRIM`/`XSETID` при реплее, переписывание AOF
      (`XADD` с явными идентификаторами) и собственная пропагация `XADD` —
      в AOF попадает **фактический** идентификатор, а приблизительная обрезка
      (`~`) переписывается в точную (как в Redis).
- [x] Сценарий сверки `testdata/compat/stream.txt` — все 8 сценариев харнесса
      совпадают с настоящим Redis 8.2.
- [ ] RDB-формат стримов — в вехе **M21**. Пока `SAVE` пропускает ключи-потоки
      и пишет об этом предупреждение в лог; AOF их сохраняет. `DUMP` потока
      возвращает ошибку.

### ✅ M20 — Streams: группы и чтение

- [x] Модель групп в `internal/store/stream_group.go`: `StreamGroup`
      (идентификатор последней доставленной записи, счётчик прочитанных,
      потребители, список неподтверждённых записей), `StreamConsumer`,
      `StreamNack` (время доставки и число доставок).
- [x] Команды: `XGROUP` (`CREATE`/`DESTROY`/`CREATECONSUMER`/`DELCONSUMER`/
      `SETID`/`HELP`), `XREAD` (в т.ч. блокирующий), `XREADGROUP`, `XACK`,
      `XCLAIM`, `XAUTOCLAIM`, `XPENDING`, `XINFO STREAM/GROUPS/CONSUMERS`.
- [x] Семантика как в Redis 8.2: `XREADGROUP … >` отдаёт записи после
      идентификатора группы и кладёт их в список неподтверждённых; `XREADGROUP
      … <id>` отдаёт **собственные** неподтверждённые записи потребителя и
      увеличивает число доставок; `XACK` убирает запись из списков группы и
      потребителя; `XCLAIM`/`XAUTOCLAIM` переносят записи другому
      потребителю; `XINFO GROUPS` отдаёт `entries-read` и `lag` как null,
      когда счётчик неизвестен (группа без `ENTRIESREAD`).
- [x] AOF: изменения списка неподтверждённых пишутся идемпотентной формой
      `XCLAIM … FORCE JUSTID`, движение идентификатора группы — `XGROUP SETID`,
      новый потребитель — `XGROUP CREATECONSUMER`; переписывание AOF
      восстанавливает и группы, и список неподтверждённых.
- [x] Сценарий сверки `testdata/compat/stream_group.txt` — все 9 сценариев
      харнесса совпадают с настоящим Redis 8.2.

### ⏳ M21 — Streams: персистентность

- [ ] RDB-энкодинг stream (listpack, как в Redis 7+), AOF-пропагация,
      round-trip и интероп-тесты.

### ⏳ M22 — Репликация: каркас

- [ ] `REPLICAOF`/`SLAVEOF`, `ROLE`, секция `replication` в `INFO`, `REPLCONF`
      (`listening-port`, `capa`), handshake `PSYNC`, полный sync (передача RDB).

### ⏳ M23 — Репликация: инкрементальная синхронизация

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
