package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	dbm "github.com/tendermint/tm-db"
	"golang.org/x/exp/maps"
)

// Using var here instead of const so that they can be set during compilation.
var (
	// MaxExamples is the max number of examples to print for each module and prefix.
	MaxExamples = 1
	// DBType is the type of DB to use.
	DBType = dbm.GoLevelDBBackend
)

// Stat is a basic collection of statistics about a set of db keys.
type Stat struct {
	Count   int
	KeySize int
	ValSize int
}

// GetTotalSize returns the keySize + valSize.
func (s Stat) GetTotalSize() int {
	return s.KeySize + s.ValSize
}

func (s Stat) String() string {
	return fmt.Sprintf("Count: %d, TotalSize: %s", s.Count, ByteCountDecimal(s.GetTotalSize()))
}

// ModuleStat is statistics on a module, both totals and broken down by prefix.
type ModuleStat struct {
	Totals   Stat
	ByPrefix map[string]*Stat
}

func NewModuleStat() *ModuleStat {
	return &ModuleStat{
		Totals:   Stat{},
		ByPrefix: make(map[string]*Stat),
	}
}

// GetTotalSize returns the total size of this module.
func (s ModuleStat) GetTotalSize() int {
	return s.Totals.GetTotalSize()
}

type ModuleTotals map[string]*ModuleStat

// StatusManager is an object to facilitate a long process that should have log messages printed periodically.
type StatusManager struct {
	TimeStarted  time.Time
	TimeFinished time.Time
	Totals       *Stat
	ModuleTotals ModuleTotals
	CurKey       []byte

	StatusTicker   *time.Ticker
	StopTickerChan chan bool

	SigChan     chan os.Signal
	StopSigChan chan bool
}

// StartStatusManager creates a new StatusManager and starts sub-processes used for monitoring and logging.
func StartStatusManager() *StatusManager {
	rv := &StatusManager{
		TimeStarted:    time.Now(),
		Totals:         &Stat{},
		ModuleTotals:   make(ModuleTotals),
		StopTickerChan: make(chan bool, 1),
		SigChan:        make(chan os.Signal, 1),
		StopSigChan:    make(chan bool, 1),
	}

	// Monitor for the signals and handle them appropriately.
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		rv.LogFatalWithRunTime("could not identify the running process: %v", err)
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				rv.LogFatalWithRunTime("The signal watcher subprocess encountered a panic: %v", r)
			}
		}()
		select {
		case s := <-rv.SigChan:
			signal.Stop(rv.SigChan)
			err2 := proc.Signal(s)
			if err2 != nil {
				rv.LogFatalWithRunTime("Error propagating signal: %v", err2)
			}
			return
		case <-rv.StopSigChan:
			return
		}
	}()
	signal.Notify(rv.SigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSEGV, syscall.SIGQUIT)

	// Fire up another sub-process for outputting a status message every now and then.
	rv.StatusTicker = time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-rv.StatusTicker.C:
				rv.LogWithRunTime("%s key=%q", rv.Totals.String(), string(rv.CurKey))
			case <-rv.StopTickerChan:
				return
			}
		}
	}()

	return rv
}

// Finish records the TimeFinished and cleans up the logging sub-process.
func (m *StatusManager) Finish() {
	m.StopSigChan <- true
	m.StopTickerChan <- true
	m.StatusTicker.Stop()
	m.TimeFinished = time.Now()
	m.CurKey = nil
}

// Close closes up things that this manages.
func (m *StatusManager) Close() {
	close(m.StopSigChan)
	signal.Stop(m.SigChan)
	close(m.SigChan)
}

// GetRunTime gets the duration that this StatusManager has been running.
func (m StatusManager) GetRunTime() string {
	if m.TimeStarted.IsZero() {
		return "0.000000000s"
	}
	if m.TimeFinished.IsZero() {
		return time.Since(m.TimeStarted).String()
	}
	return m.TimeFinished.Sub(m.TimeStarted).String()
}

// LogWithRunTime is similar to log.Printf except this one includes the current runtime of this manager.
func (m StatusManager) LogWithRunTime(format string, v ...any) {
	log.Printf(fmt.Sprintf("% 16s", m.GetRunTime())+" "+format, v...)
}

// LogFatalWithRunTime is similar to log.Fatalf except this one includes the current runtime of this manager.
func (m StatusManager) LogFatalWithRunTime(format string, v ...any) {
	log.Fatalf(fmt.Sprintf("% 16s", m.GetRunTime())+" "+format, v...)
}

// WeightedName is used for sorting names using something other than alphabetical.
type WeightedName struct {
	name   string
	weight int
}

func NewWeightedName(name string, weight int) *WeightedName {
	return &WeightedName{
		name:   name,
		weight: weight,
	}
}

// WeightedNames is a slice of WeightedName pointers that you're going to want sorted.
type WeightedNames []*WeightedName

func (a WeightedNames) Len() int           { return len(a) }
func (a WeightedNames) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a WeightedNames) Less(i, j int) bool { return a[i].weight < a[j].weight }

// ByteCountDecimal converts an integer byte size to a more human-readable version, e.g. 12.5 MB.
func ByteCountDecimal(b int) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := int64(b) / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

func main() {
	fullDBDir, moduleOfInterest := getDbDirAndMOI(os.Args[1:])

	db := openDB(fullDBDir)

	printDBStats(db)

	grandTotal, moduleStats := getModuleStats(db, moduleOfInterest)

	printModuleStats(grandTotal, moduleStats)
}

// printUsageAndExit outputs the usage of this program and exits with a code 0.
func printUsageAndExit() {
	fmt.Printf("Usage: iavl-viewer [<db dir>] [<module of interest>]")
	os.Exit(0)
}

// getDbDirAndMOI parses the provided args and returns the db dir and module of interest.
func getDbDirAndMOI(args []string) (string, string) {
	for _, arg := range args {
		if arg == "--help" || arg == "-h" {
			printUsageAndExit()
		}
	}

	dbDir := "./data/application.db"
	moduleOfInterest := ""

	switch len(args) {
	case 0:
		// leave the defaults
	case 1:
		if !strings.ContainsAny(args[0], "/.") {
			moduleOfInterest = args[0]
		} else {
			dbDir = args[0]
		}
	case 2:
		if !strings.ContainsAny(args[0], "/.") {
			moduleOfInterest = args[0]
			dbDir = args[1]
		} else {
			dbDir = args[0]
			moduleOfInterest = args[1]
		}
	default:
		printUsageAndExit()
	}

	return dbDir, moduleOfInterest
}

func openDB(path string) dbm.DB {
	dbdir, dbname := splitDBDirAndName(path)
	log.Printf("Opening db. type=%q dir=%q name=%q\n", DBType, dbdir, dbname)
	db, err := dbm.NewDB(dbname, DBType, dbdir)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	return db
}

// splitDBDirAndName converts a db dir path into the directory holding it and the db name.
func splitDBDirAndName(path string) (string, string) {
	dir, name := filepath.Split(path)
	name = strings.TrimSuffix(name, ".db")
	return dir, name
}

func printDBStats(db dbm.DB) {
	log.Printf("Getting DB stats.\n")
	// The db.Stats() function can sometimes take quite a while. So don't use it if we don't have to.
	switch db2 := db.(type) {
	case *dbm.GoLevelDB:
		levelDBStats, err := db2.DB().GetProperty("leveldb.stats")
		if err != nil {
			log.Fatalf("Failed to get LevelDB stats: %v", err)
		}
		fmt.Printf("%s\n", levelDBStats)
	default:
		dbStats := db.Stats()
		if levelDBStats, found := dbStats["leveldb.stats"]; found {
			fmt.Printf("%s\n", levelDBStats)
		} else {
			statProps := maps.Keys(dbStats)
			sort.Strings(statProps)
			for _, prop := range statProps {
				fmt.Printf("%s: %s", prop, dbStats[prop])
			}
			fmt.Printf("All stat keys: %q\n", statProps)
		}
	}
}

func getModuleStats(db dbm.DB, moduleOfInterest string) (*Stat, ModuleTotals) {
	if moduleOfInterest != "" {
		log.Printf("Getting %q module stats.\n", moduleOfInterest)
	} else {
		log.Printf("Getting module stats.\n")
	}

	var startKey []byte
	if moduleOfInterest != "" && moduleOfInterest != "misc" {
		startKey = []byte(fmt.Sprintf("s/k:%s/", moduleOfInterest))
	}

	iter, err := db.Iterator(startKey, nil)
	if err != nil {
		log.Fatalf("Failed to create db iterator: %v", err)
	}
	defer iter.Close()
	sm := StartStatusManager()
	defer sm.Close()

	// module name -> prefix -> number of examples printed for it.
	exampleCounts := make(map[string]map[string]int)

	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		sm.CurKey = key

		statKey, keyPre := splitKey(key)

		if statKey != moduleOfInterest {
			if moduleOfInterest == "misc" {
				// The misc keys aren't necessarily all together. So we just go to the next entry.
				continue
			}
			if moduleOfInterest != "" {
				// A module of interest is defined, and it's not misc, and we're not on it anymore. We're done.
				break
			}
		}

		keySize := len(key)
		valSize := len(iter.Value())

		sm.Totals.Count++
		sm.Totals.KeySize += keySize
		sm.Totals.ValSize += valSize

		if _, found := sm.ModuleTotals[statKey]; !found {
			sm.ModuleTotals[statKey] = NewModuleStat()
		}
		sm.ModuleTotals[statKey].Totals.Count++
		sm.ModuleTotals[statKey].Totals.KeySize += keySize
		sm.ModuleTotals[statKey].Totals.ValSize += valSize

		if _, found := sm.ModuleTotals[statKey].ByPrefix[keyPre]; !found {
			sm.ModuleTotals[statKey].ByPrefix[keyPre] = &Stat{}
		}
		sm.ModuleTotals[statKey].ByPrefix[keyPre].Count++
		sm.ModuleTotals[statKey].ByPrefix[keyPre].KeySize += keySize
		sm.ModuleTotals[statKey].ByPrefix[keyPre].ValSize += valSize

		if _, found := exampleCounts[statKey]; !found {
			exampleCounts[statKey] = make(map[string]int)
		}
		if exampleCounts[statKey][keyPre] < MaxExamples {
			exampleCounts[statKey][keyPre]++
			fmt.Printf("%q %q %d: (%d) %q = %s\n",
				statKey, keyPre, exampleCounts[statKey][keyPre],
				keySize, key, ByteCountDecimal(valSize),
			)
		}
	}
	sm.Finish()
	sm.LogWithRunTime("Done getting module stats.")

	return sm.Totals, sm.ModuleTotals
}

func printModuleStats(grandTotal *Stat, moduleStats ModuleTotals) {
	header := table.Row{
		"Module", "Prefix",
		"Avg Key Size", "Avg Value Size",
		"Total Key Size", "Total Value Size", "Total Size",
		"Total Entries",
	}
	footer := table.Row{
		"Total", "",
		ByteCountDecimal(grandTotal.KeySize / grandTotal.Count), ByteCountDecimal(grandTotal.ValSize / grandTotal.Count),
		ByteCountDecimal(grandTotal.KeySize), ByteCountDecimal(grandTotal.ValSize), ByteCountDecimal(grandTotal.GetTotalSize()),
		grandTotal.Count,
	}
	newRow := func(module, prefix string, stat *Stat) []interface{} {
		return []interface{}{
			module, strings.Trim(fmt.Sprintf("%q", prefix), `"`),
			ByteCountDecimal(stat.KeySize / stat.Count), ByteCountDecimal(stat.ValSize / stat.Count),
			ByteCountDecimal(stat.KeySize), ByteCountDecimal(stat.ValSize), ByteCountDecimal(stat.GetTotalSize()),
			stat.Count,
		}
	}

	// Get the module names sorted by size, smallest to largest.
	modules := make(WeightedNames, 0, len(moduleStats))
	for name, stats := range moduleStats {
		modules = append(modules, NewWeightedName(name, stats.GetTotalSize()))
	}
	sort.Sort(modules)

	// Output a table of stats for each module broken down by prefix.
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(header)

	for _, module := range modules {
		m := module.name
		stats := moduleStats[m]
		// Get the prefixes sorted by size, smallest to largest.
		pres := make(WeightedNames, 0, len(stats.ByPrefix))
		for pre, stat := range stats.ByPrefix {
			pres = append(pres, NewWeightedName(pre, stat.GetTotalSize()))
		}
		sort.Sort(pres)
		for _, pre := range pres {
			p := pre.name
			preStats := stats.ByPrefix[p]
			t.AppendRow(newRow(m, p, preStats))
		}
	}
	t.AppendFooter(footer)
	t.Render()

	if len(modules) > 1 {
		// Create a table of stats with just the totals in them.
		t = table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(header)
		for _, module := range modules {
			m := module.name
			stats := moduleStats[m].Totals
			t.AppendRow(newRow(m, "", &stats))
		}
		t.AppendFooter(footer)
		t.Render()
	}
}

// Some regexp objects used when splitting keys.
var (
	moduleRe   = regexp.MustCompile(`^s/k:(\w+)/`)
	miscNumRe  = regexp.MustCompile(`^s/(\d+)$`)
	miscWordRe = regexp.MustCompile(`^s/(\w+)$`)
	fWordRe    = regexp.MustCompile(`^([fm][a-zA-Z_-]+)`)
)

// splitKey splits the key into the module and a prefix.
func splitKey(key []byte) (string, string) {
	module := "misc"

	// First check if it's a key from a module.
	if tokens := moduleRe.FindStringSubmatch(string(key)); len(tokens) > 0 && len(tokens[0]) > 0 {
		module = tokens[1]
		// Remove the leading "s/k:module/"
		remainderBz := key[len(module)+5:]
		switch remainderBz[0] {
		case 'n', 'o', 'r':
			// 'o', 'r', and 'n' keys are orphan, root, and node.
			// Not sure how to get anything meaningful out of them yet.
			return module, string(remainderBz[0:1])
		case 'm', 'f':
			// As of writing this, 'm' is always followed by "storage_version"
			// And the 'f' keys are "fast format keys". They look like they're followed by the
			// keystore keys for the given module.
			// In most cases that's a prefix key byte, but in some cases, it's a word.
			// If it's a word (possibly followed by other stuff) we want the whole word.
			// Otherwise, we're going to just assume that the key prefix is only one byte, and use that.
			if ftokens := fWordRe.FindStringSubmatch(string(remainderBz)); len(ftokens) > 0 && len(tokens[0]) > 0 {
				return module, ftokens[1]
			}
			return module, string(remainderBz[0:2])
		default:
			// Something new? Just get the two bytes.
			// With only one byte, it's probably just an entry type (e.g. 'n' or 'o')
			// followed by the interesting part. So get both, maybe a pattern will show up.
			return module, string(remainderBz[0:2])
		}
	}

	if miscNumRe.Match(key) {
		// keys like "s/<number>". Just use the first digit as the prefix.
		return module, string(key[2:3])
	}

	if tokens := miscWordRe.FindStringSubmatch(string(key)); len(tokens) > 0 && len(tokens[0]) > 0 {
		return module, tokens[1]
	}

	// Nothing expected. Just grab the first 3 bytes for the prefix.
	// I'm kind of assuming it looks something like "s/...".
	// So I'm going with 3 bytes to get the first one after the slash.
	return module, string(key[0:3])
}
