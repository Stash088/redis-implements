// Command compat-check compares the replies of two Redis servers command by
// command.
//
// It runs the scenarios of a directory against both servers and reports the
// differences in the decoded RESP replies (types and values). The tool is used
// to check that our implementation behaves like the real Redis.
//
// Usage:
//
//	go run ./cmd/compat-check --ours 127.0.0.1:7799 --redis 127.0.0.1:6379 \
//	    --scenarios testdata/compat
//
// The scenario files hold one command per line:
//
//	# comments and empty lines are ignored
//	SET key value
//	~ TIME                 # a volatile reply: the command runs, the reply is not compared
//	#! proto 3             # a directive: both connections negotiate RESP3
//
// Arguments may be quoted with double quotes to include spaces.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/amir/redis-implements/internal/resp"
)

func main() {
	ours := flag.String("ours", "127.0.0.1:7799", "address of our server")
	redisAddr := flag.String("redis", "127.0.0.1:6379", "address of the reference Redis")
	dir := flag.String("scenarios", "testdata/compat", "directory with the scenario files")
	verbose := flag.Bool("v", false, "print every command")
	keep := flag.Bool("keep", false, "do not flush the databases before each scenario")
	flag.Parse()

	files, err := scenarioFiles(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "compat-check: %v\n", err)
		os.Exit(2)
	}
	if len(files) == 0 {
		fmt.Fprintf(os.Stderr, "compat-check: no scenarios in %s\n", *dir)
		os.Exit(2)
	}

	failed := 0
	for _, file := range files {
		ok, err := runScenario(file, *ours, *redisAddr, *verbose, !*keep)
		if err != nil {
			fmt.Printf("%-28s ERROR %v\n", filepath.Base(file), err)
			failed++
			continue
		}
		if ok {
			fmt.Printf("%-28s OK\n", filepath.Base(file))
			continue
		}
		failed++
	}
	if failed > 0 {
		fmt.Printf("\n%d of %d scenarios differ\n", failed, len(files))
		os.Exit(1)
	}
	fmt.Printf("\nall %d scenarios match\n", len(files))
}

// scenarioFiles returns the scenario files of a directory, sorted by name.
func scenarioFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".txt") {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out, nil
}

// scenario is a parsed scenario file.
type scenario struct {
	name string
	// proto is the protocol version to negotiate (2 or 3).
	proto int
	// commands holds the commands to run; a volatile command is not compared.
	commands []scenarioCommand
}

// scenarioCommand is one command of a scenario.
type scenarioCommand struct {
	line string
	args []string
	// mode tells how the reply is compared.
	mode compareMode
}

// compareMode tells how the reply of a command is compared.
type compareMode uint8

const (
	// compareExact compares the replies as they are.
	compareExact compareMode = iota
	// compareVolatile runs the command but does not compare the reply.
	compareVolatile
	// compareUnordered compares the elements of the reply as a multiset:
	// the order of the elements is not specified by Redis.
	compareUnordered
	// compareCollected iterates a cursor reply until it returns to zero and
	// compares the collected elements as a multiset. It is used for the
	// SCAN family, where the batch boundaries are implementation specific.
	compareCollected
)

// parseScenario reads and parses a scenario file.
func parseScenario(path string) (*scenario, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := &scenario{name: filepath.Base(path), proto: 2}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "#!") {
			fields := strings.Fields(strings.TrimPrefix(line, "#!"))
			if len(fields) == 2 && fields[0] == "proto" {
				n, err := strconv.Atoi(fields[1])
				if err != nil {
					return nil, fmt.Errorf("bad proto directive: %q", line)
				}
				sc.proto = n
			}
			continue
		}
		// A trailing comment (a "#" preceded by a space) is ignored.
		if i := strings.Index(line, " #"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		cmd := scenarioCommand{line: line}
		switch {
		case strings.HasPrefix(line, "~"):
			cmd.mode = compareVolatile
			line = strings.TrimSpace(strings.TrimPrefix(line, "~"))
		case strings.HasPrefix(line, "%"):
			cmd.mode = compareUnordered
			line = strings.TrimSpace(strings.TrimPrefix(line, "%"))
		case strings.HasPrefix(line, "="):
			cmd.mode = compareCollected
			line = strings.TrimSpace(strings.TrimPrefix(line, "="))
		}
		cmd.args = splitArgs(line)
		if len(cmd.args) == 0 {
			continue
		}
		sc.commands = append(sc.commands, cmd)
	}
	return sc, scanner.Err()
}

// splitArgs splits a command line into arguments, honouring double quotes.
func splitArgs(line string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	started := false
	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch {
		case ch == '"':
			inQuote = !inQuote
			started = true
		case ch == ' ' && !inQuote:
			if started {
				out = append(out, cur.String())
				cur.Reset()
				started = false
			}
		default:
			cur.WriteByte(ch)
			started = true
		}
	}
	if started {
		out = append(out, cur.String())
	}
	return out
}

// runScenario runs one scenario against both servers and compares the
// replies. It reports whether the scenario matched.
func runScenario(path, ours, redisAddr string, verbose, flush bool) (bool, error) {
	sc, err := parseScenario(path)
	if err != nil {
		return false, err
	}
	ourConn, err := dial(ours, sc.proto)
	if err != nil {
		return false, err
	}
	defer ourConn.Close()
	refConn, err := dial(redisAddr, sc.proto)
	if err != nil {
		return false, err
	}
	defer refConn.Close()

	if flush {
		// Every scenario starts from an empty state, so it does not depend on
		// what the previous one left behind.
		for _, c := range []*conn{ourConn, refConn} {
			if err := c.commandErr([]string{"FLUSHALL"}); err != nil {
				return false, err
			}
		}
	}

	for _, cmd := range sc.commands {
		ourReply, ourErr := ourConn.command(cmd.args)
		refReply, refErr := refConn.command(cmd.args)
		if (ourErr == nil) != (refErr == nil) {
			fmt.Printf("%s: %s: ours: %v, redis: %v\n", sc.name, cmd.line, ourErr, refErr)
			return false, nil
		}
		if ourErr != nil {
			// Both failed: the error messages are not compared (they may
			// differ in the wording).
			continue
		}
		switch cmd.mode {
		case compareVolatile:
			if verbose {
				fmt.Printf("%s: %s -> (volatile, not compared)\n", sc.name, cmd.line)
			}
			continue
		case compareUnordered:
			if diff := compareUnorderedValues(ourReply, refReply); diff != "" {
				fmt.Printf("%s: %s: %s\n", sc.name, cmd.line, diff)
				return false, nil
			}
		case compareCollected:
			ourItems, err := collectCursor(ourConn, cmd.args)
			if err != nil {
				fmt.Printf("%s: %s: ours: %v\n", sc.name, cmd.line, err)
				return false, nil
			}
			refItems, err := collectCursor(refConn, cmd.args)
			if err != nil {
				fmt.Printf("%s: %s: redis: %v\n", sc.name, cmd.line, err)
				return false, nil
			}
			if diff := compareStringSets(ourItems, refItems); diff != "" {
				fmt.Printf("%s: %s: %s\n", sc.name, cmd.line, diff)
				return false, nil
			}
		default:
			if diff := compareValues(ourReply, refReply); diff != "" {
				fmt.Printf("%s: %s: %s\n", sc.name, cmd.line, diff)
				return false, nil
			}
		}
		if verbose {
			fmt.Printf("%s: %s -> ok\n", sc.name, cmd.line)
		}
	}
	return true, nil
}

// compareUnorderedValues compares two replies whose elements are not ordered
// (a hash or a set): the elements are sorted before the comparison.
func compareUnorderedValues(ours, ref resp.Value) string {
	if ours.Kind != ref.Kind {
		return fmt.Sprintf("different types: ours %v, redis %v", ours.Kind, ref.Kind)
	}
	switch ours.Kind {
	case resp.KindArray, resp.KindSet, resp.KindMap:
		if len(ours.Array) != len(ref.Array) {
			return fmt.Sprintf("different lengths: ours %d, redis %d", len(ours.Array), len(ref.Array))
		}
		a := sortedStrings(ours.Array)
		b := sortedStrings(ref.Array)
		for i := range a {
			if a[i] != b[i] {
				return fmt.Sprintf("element %d: ours %q, redis %q", i, a[i], b[i])
			}
		}
		return ""
	}
	return compareValues(ours, ref)
}

// sortedStrings returns the string forms of the elements, sorted.
func sortedStrings(vals []resp.Value) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		out = append(out, string(v.Str))
	}
	sort.Strings(out)
	return out
}

// compareStringSets compares two sets of strings.
func compareStringSets(ours, ref []string) string {
	if len(ours) != len(ref) {
		return fmt.Sprintf("different sizes: ours %d, redis %d", len(ours), len(ref))
	}
	a := append([]string(nil), ours...)
	b := append([]string(nil), ref...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return fmt.Sprintf("element %d: ours %q, redis %q", i, a[i], b[i])
		}
	}
	return ""
}

// collectCursor runs a cursor command until the cursor returns to zero and
// returns the elements it produced. The batch boundaries are implementation
// specific, so only the whole iteration is comparable.
func collectCursor(c *conn, args []string) ([]string, error) {
	var out []string
	cursor := "0"
	for i := 0; i < 10000; i++ {
		call := append([]string(nil), args...)
		call[2] = cursor
		v, err := c.command(call)
		if err != nil {
			return nil, err
		}
		if v.Kind != resp.KindArray || len(v.Array) != 2 {
			return nil, fmt.Errorf("unexpected reply %+v", v)
		}
		for _, item := range v.Array[1].Array {
			out = append(out, string(item.Str))
		}
		cursor = string(v.Array[0].Str)
		if cursor == "0" {
			return out, nil
		}
	}
	return nil, fmt.Errorf("the iteration did not finish")
}

// compareValues compares two decoded replies and describes the first
// difference.
func compareValues(ours, ref resp.Value) string {
	switch ours.Kind {
	case resp.KindArray, resp.KindPush:
		if len(ours.Array) != len(ref.Array) {
			return fmt.Sprintf("different lengths: ours %d, redis %d", len(ours.Array), len(ref.Array))
		}
		for i := range ours.Array {
			if diff := compareValues(ours.Array[i], ref.Array[i]); diff != "" {
				return fmt.Sprintf("element %d: %s", i, diff)
			}
		}
		return ""
	case resp.KindSimpleString, resp.KindError, resp.KindBulkString, resp.KindVerbatim:
		if string(ours.Str) != string(ref.Str) {
			return fmt.Sprintf("ours %q, redis %q", ours.Str, ref.Str)
		}
		return ""
	case resp.KindInteger:
		if ours.Int != ref.Int {
			return fmt.Sprintf("ours %d, redis %d", ours.Int, ref.Int)
		}
		return ""
	case resp.KindDouble:
		if ours.Float != ref.Float {
			return fmt.Sprintf("ours %v, redis %v", ours.Float, ref.Float)
		}
		return ""
	case resp.KindMap:
		if len(ours.Array) != len(ref.Array) {
			return fmt.Sprintf("different map sizes: ours %d, redis %d", len(ours.Array), len(ref.Array))
		}
		for i := range ours.Array {
			if diff := compareValues(ours.Array[i], ref.Array[i]); diff != "" {
				return fmt.Sprintf("map entry %d: %s", i, diff)
			}
		}
		return ""
	case resp.KindSet:
		if len(ours.Array) != len(ref.Array) {
			return fmt.Sprintf("different set sizes: ours %d, redis %d", len(ours.Array), len(ref.Array))
		}
		return ""
	}
	if ours.IsNull != ref.IsNull {
		return fmt.Sprintf("null mismatch: ours %v, redis %v", ours.IsNull, ref.IsNull)
	}
	return ""
}
