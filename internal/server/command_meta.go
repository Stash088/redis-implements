package server

import "strings"

// cmdMeta describes a command for the COMMAND command: its flags, the
// positions of its keys and a short summary.
type cmdMeta struct {
	flags    []string
	firstKey int // 1-based position of the first key, 0 when the command has none
	lastKey  int // 1-based position of the last key, 0 when the command has none
	step     int // step between the keys of a multi-key command
	group    string
	summary  string
}

// commandMeta holds the metadata of the commands. Commands that are not
// listed get a default entry derived from their name (see defaultMeta).
var commandMeta = map[string]cmdMeta{
	// Strings.
	"get":      {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Get the value of a key"},
	"set":      {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Set the string value of a key"},
	"append":   {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Append a value to a key"},
	"strlen":   {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Get the length of the value stored in a key"},
	"incr":     {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Increment the integer value of a key by one"},
	"decr":     {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Decrement the integer value of a key by one"},
	"incrby":   {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Increment the integer value of a key by the given amount"},
	"decrby":   {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Decrement the integer value of a key by the given amount"},
	"mget":     {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: -1, step: 1, group: "string", summary: "Get the values of all the given keys"},
	"mset":     {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: -1, step: 2, group: "string", summary: "Set multiple keys to multiple values"},
	"getset":   {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Set the string value of a key and return its old value"},
	"getdel":   {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Get the value of a key and delete the key"},
	"getex":    {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Get the value of a key and optionally set its expiration"},
	"setex":    {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Set the value and expiration of a key"},
	"psetex":   {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Set the value and expiration in milliseconds of a key"},
	"setnx":    {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Set the value of a key, only if the key does not exist"},
	"getrange": {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Get a substring of the string stored at a key"},
	"setrange": {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "string", summary: "Overwrite part of the string stored at a key"},

	// Keys.
	"del":        {flags: []string{"write", "fast"}, firstKey: 1, lastKey: -1, step: 1, group: "generic", summary: "Delete a key"},
	"unlink":     {flags: []string{"write", "fast"}, firstKey: 1, lastKey: -1, step: 1, group: "generic", summary: "Delete a key asynchronously in another thread"},
	"exists":     {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: -1, step: 1, group: "generic", summary: "Determine if a key exists"},
	"type":       {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Determine the type stored at key"},
	"expire":     {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Set a key's time to live in seconds"},
	"pexpire":    {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Set a key's time to live in milliseconds"},
	"expireat":   {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Set the expiration for a key as a UNIX timestamp"},
	"pexpireat":  {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Set the expiration for a key as a UNIX timestamp in milliseconds"},
	"ttl":        {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Get the time to live for a key"},
	"pttl":       {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Get the time to live for a key in milliseconds"},
	"persist":    {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Remove the expiration from a key"},
	"touch":      {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: -1, step: 1, group: "generic", summary: "Alters the last access time of a key(s)"},
	"rename":     {flags: []string{"write"}, firstKey: 1, lastKey: 2, step: 1, group: "generic", summary: "Rename a key"},
	"renamenx":   {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 2, step: 1, group: "generic", summary: "Rename a key, only if the new key does not exist"},
	"copy":       {flags: []string{"write"}, firstKey: 1, lastKey: 2, step: 1, group: "generic", summary: "Copy a key"},
	"move":       {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Move a key to another database"},
	"keys":       {flags: []string{"readonly", "slow"}, group: "generic", summary: "Find all keys matching the given pattern"},
	"scan":       {flags: []string{"readonly", "slow"}, group: "generic", summary: "Incrementally iterate the keys space"},
	"randomkey":  {flags: []string{"readonly", "slow"}, group: "generic", summary: "Return a random key from the keyspace"},
	"dump":       {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Return a serialized version of the value stored at the specified key"},
	"expiretime": {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "generic", summary: "Get the expiration Unix timestamp of a key"},

	// Lists.
	"lpush":     {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Prepend one or multiple elements to a list"},
	"rpush":     {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Append one or multiple elements to a list"},
	"lpop":      {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Pop an element from the head of a list"},
	"rpop":      {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Pop an element from the tail of a list"},
	"lrange":    {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Get a range of elements from a list"},
	"llen":      {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Get the length of a list"},
	"lindex":    {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Get an element from a list by its index"},
	"lset":      {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Set the value of an element in a list by its index"},
	"linsert":   {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Insert an element before or after another element in a list"},
	"lrem":      {flags: []string{"write"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Remove elements from a list"},
	"ltrim":     {flags: []string{"write"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Trim a list to the specified range"},
	"lpos":      {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "list", summary: "Return the index of matching elements on a list"},
	"lmove":     {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 2, step: 1, group: "list", summary: "Pop an element from a list, push it to another list and return it"},
	"rpoplpush": {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 2, step: 1, group: "list", summary: "Remove the last element in a list, prepend it to another list and return it"},

	// Hashes.
	"hset":         {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Set the string value of a hash field"},
	"hget":         {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get the value of a hash field"},
	"hgetall":      {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get all the fields and values in a hash"},
	"hdel":         {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Delete one or more hash fields"},
	"hlen":         {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get the number of fields in a hash"},
	"hexists":      {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Determine if a hash field exists"},
	"hkeys":        {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get all the fields in a hash"},
	"hvals":        {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get all the values in a hash"},
	"hstrlen":      {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get the length of the value of a hash field"},
	"hincrby":      {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Increment the integer value of a hash field by the given number"},
	"hincrbyfloat": {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Increment the float value of a hash field by the given amount"},
	"hmget":        {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get the values of all the given hash fields"},
	"hmset":        {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Set multiple hash fields to multiple values"},
	"hsetnx":       {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Set the value of a hash field, only if the field does not exist"},
	"hrandfield":   {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Get one or multiple random fields from a hash"},
	"hscan":        {flags: []string{"readonly", "slow"}, firstKey: 1, lastKey: 1, step: 1, group: "hash", summary: "Incrementally iterate hash fields and associated values"},

	// Sets.
	"sadd":        {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Add one or more members to a set"},
	"srem":        {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Remove one or more members from a set"},
	"smembers":    {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Get all the members in a set"},
	"sismember":   {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Determine if a given value is a member of a set"},
	"scard":       {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Get the number of members in a set"},
	"smismember":  {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Returns the membership associated with the given elements for a set"},
	"smove":       {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 2, step: 1, group: "set", summary: "Move a member from one set to another"},
	"spop":        {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Remove and return one or multiple random members from a set"},
	"srandmember": {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Get one or multiple random members from a set"},
	"sscan":       {flags: []string{"readonly", "slow"}, firstKey: 1, lastKey: 1, step: 1, group: "set", summary: "Incrementally iterate Set elements"},
	"sdiff":       {flags: []string{"readonly"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Subtract multiple sets"},
	"sdiffstore":  {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Subtract multiple sets and store the resulting set in a key"},
	"sinter":      {flags: []string{"readonly"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Intersect multiple sets"},
	"sinterstore": {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Intersect multiple sets and store the resulting set in a key"},
	"sunion":      {flags: []string{"readonly"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Add multiple sets"},
	"sunionstore": {flags: []string{"write", "denyoom"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Add multiple sets and store the resulting set in a key"},
	"sintercard":  {flags: []string{"readonly"}, firstKey: 1, lastKey: -1, step: 1, group: "set", summary: "Intersect multiple sets and return the cardinality of the result"},

	// Sorted sets.
	"zadd":             {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Add one or more members to a sorted set, or update its score if it already exists"},
	"zrem":             {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Remove one or more members from a sorted set"},
	"zscore":           {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Get the score associated with the given member in a sorted set"},
	"zcard":            {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Get the number of members in a sorted set"},
	"zrank":            {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Determine the index of a member in a sorted set"},
	"zrevrank":         {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Determine the index of a member in a sorted set, with scores ordered from high to low"},
	"zrange":           {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Return a range of members in a sorted set"},
	"zrevrange":        {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Return a range of members in a sorted set, by index, with scores ordered from high to low"},
	"zrangeByScore":    {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Return a range of members in a sorted set, by score"},
	"zrevrangeByScore": {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Return a range of members in a sorted set, by score, with scores ordered from high to low"},
	"zrangeByLex":      {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Return a range of members in a sorted set, by lexicographical range"},
	"zcount":           {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Count the members in a sorted set with scores within the given values"},
	"zincrby":          {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Increment the score of a member in a sorted set"},
	"zpopmin":          {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Remove and return members with the lowest scores in a sorted set"},
	"zpopmax":          {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Remove and return members with the highest scores in a sorted set"},
	"zscan":            {flags: []string{"readonly", "slow"}, firstKey: 1, lastKey: 1, step: 1, group: "sorted-set", summary: "Incrementally iterate sorted sets elements and associated scores"},

	// Streams.
	"xadd":       {flags: []string{"write", "denyoom", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Appends a new entry to a stream"},
	"xlen":       {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return the number of entries in a stream"},
	"xrange":     {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return a range of entries in a stream"},
	"xrevrange":  {flags: []string{"readonly"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return a range of entries in a stream, in reverse order"},
	"xdel":       {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Removes the specified entries from the stream"},
	"xtrim":      {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Trims the stream to (approximately) a certain size"},
	"xsetid":     {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Set the last delivered ID of a stream"},
	"xgroup":     {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Manage the consumer groups of a stream"},
	"xack":       {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Acknowledge the messages of a consumer group"},
	"xclaim":     {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Claim the pending messages of a consumer group"},
	"xautoclaim": {flags: []string{"write", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Claim the idle pending messages of a consumer group"},
	"xpending":   {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return the pending messages of a consumer group"},
	"xinfo":      {flags: []string{"readonly", "slow"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return the information and statistics of a stream"},
	"xread":      {flags: []string{"readonly", "blocking"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return the messages of one or more streams"},
	"xreadgroup": {flags: []string{"write", "blocking", "fast"}, firstKey: 1, lastKey: 1, step: 1, group: "stream", summary: "Return the messages of one or more streams to a consumer"},

	// Pub/Sub.
	"subscribe":    {flags: []string{"readonly", "pubsub", "fast"}, group: "pubsub", summary: "Listen for messages published to the given channels"},
	"unsubscribe":  {flags: []string{"readonly", "pubsub", "fast"}, group: "pubsub", summary: "Stop listening for messages posted to the given channels"},
	"psubscribe":   {flags: []string{"readonly", "pubsub", "fast"}, group: "pubsub", summary: "Listen for messages published to channels matching the given patterns"},
	"punsubscribe": {flags: []string{"readonly", "pubsub", "fast"}, group: "pubsub", summary: "Stop listening for messages posted to channels matching the given patterns"},
	"publish":      {flags: []string{"readonly", "pubsub", "fast"}, group: "pubsub", summary: "Post a message to a channel"},
	"pubsub":       {flags: []string{"readonly", "pubsub", "fast"}, group: "pubsub", summary: "Inspect the state of the Pub/Sub subsystem"},

	// Transactions.
	"multi":   {flags: []string{"readonly", "fast"}, group: "transactions", summary: "Mark the start of a transaction block"},
	"exec":    {flags: []string{"readonly", "noscript"}, group: "transactions", summary: "Execute all commands issued after MULTI"},
	"discard": {flags: []string{"readonly", "fast"}, group: "transactions", summary: "Discard all commands issued after MULTI"},
	"watch":   {flags: []string{"readonly", "fast"}, firstKey: 1, lastKey: -1, step: 1, group: "transactions", summary: "Watch the given keys to determine execution of the MULTI/EXEC block"},
	"unwatch": {flags: []string{"readonly", "fast"}, group: "transactions", summary: "Forget about all watched keys"},

	// Server.
	"ping":         {flags: []string{"fast"}, group: "generic", summary: "Ping the server"},
	"echo":         {flags: []string{"fast"}, group: "generic", summary: "Echo the given string"},
	"quit":         {flags: []string{"fast"}, group: "generic", summary: "Close the connection"},
	"select":       {flags: []string{"readonly", "fast"}, group: "server", summary: "Change the selected database for the current connection"},
	"swapdb":       {flags: []string{"write", "fast"}, group: "server", summary: "Swaps two Redis databases"},
	"dbsize":       {flags: []string{"readonly", "fast"}, group: "server", summary: "Return the number of keys in the selected database"},
	"flushdb":      {flags: []string{"write"}, group: "server", summary: "Remove all keys from the selected database"},
	"flushall":     {flags: []string{"write"}, group: "server", summary: "Remove all keys from all databases"},
	"info":         {flags: []string{"readonly", "slow"}, group: "server", summary: "Get information and statistics about the server"},
	"config":       {flags: []string{"readonly", "slow"}, group: "server", summary: "Manage the configuration of the server"},
	"client":       {flags: []string{"readonly", "slow"}, group: "server", summary: "Manage the connection of the client"},
	"slowlog":      {flags: []string{"readonly", "slow"}, group: "server", summary: "Manage the Redis slow queries log"},
	"command":      {flags: []string{"readonly", "loading", "stale"}, group: "server", summary: "Get array of Redis command details"},
	"time":         {flags: []string{"readonly", "fast"}, group: "server", summary: "Return the current server time"},
	"lastsave":     {flags: []string{"readonly", "fast"}, group: "server", summary: "Get the UNIX time stamp of the last successful save to disk"},
	"debug":        {flags: []string{"admin", "noscript", "loading", "stale"}, group: "server", summary: "Internal debugging commands"},
	"reset":        {flags: []string{"no-auth", "fast"}, group: "connection", summary: "Resets the connection"},
	"hello":        {flags: []string{"no-auth", "fast"}, group: "connection", summary: "Handshake with Redis"},
	"auth":         {flags: []string{"no-auth", "fast"}, group: "connection", summary: "Authenticate to the server"},
	"save":         {flags: []string{"admin", "noscript"}, group: "server", summary: "Synchronously save the dataset to disk"},
	"bgsave":       {flags: []string{"admin", "noscript"}, group: "server", summary: "Asynchronously save the dataset to disk"},
	"bgrewriteaof": {flags: []string{"admin", "noscript"}, group: "server", summary: "Asynchronously rewrite the append-only file"},
}

// defaultMeta returns the metadata of a command that is not listed in
// commandMeta. The group is derived from the command name.
func defaultMeta(name string) cmdMeta {
	group := "generic"
	switch {
	case strings.HasPrefix(name, "get"), strings.HasPrefix(name, "set"),
		strings.HasPrefix(name, "incr"), strings.HasPrefix(name, "decr"),
		strings.HasPrefix(name, "append"), strings.HasPrefix(name, "strlen"),
		strings.HasPrefix(name, "mget"), strings.HasPrefix(name, "mset"):
		group = "string"
	case strings.HasPrefix(name, "h"):
		group = "hash"
	case strings.HasPrefix(name, "l"):
		group = "list"
	case strings.HasPrefix(name, "s"):
		group = "set"
	case strings.HasPrefix(name, "z"):
		group = "sorted-set"
	}
	return cmdMeta{
		flags:   []string{"readonly", "fast"},
		group:   group,
		summary: "Redis command " + name,
	}
}

// metaFor returns the metadata of a command.
func metaFor(name string) cmdMeta {
	if m, ok := commandMeta[name]; ok {
		return m
	}
	return defaultMeta(name)
}
