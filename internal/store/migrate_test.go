package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// shape is the structure of a database: every table with its columns, every
// index, trigger and view. CREATE text is compared only for indexes, triggers
// and views, which the migration creates verbatim; tables are compared by
// pragma, because ALTER TABLE rewrites their text.
type shape struct {
	Tables   map[string][]string
	Indexes  map[string]string
	Triggers map[string]string
	Views    map[string]string
}

var spaces = regexp.MustCompile(`\s+`)

func readShape(t *testing.T, db *sql.DB) shape {
	t.Helper()
	s := shape{Tables: map[string][]string{}, Indexes: map[string]string{}, Triggers: map[string]string{}, Views: map[string]string{}}
	rows, err := db.Query(`SELECT type,name,COALESCE(sql,'') FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatal(err)
	}
	type object struct{ kind, name, sql string }
	var objects []object
	for rows.Next() {
		var o object
		if err = rows.Scan(&o.kind, &o.name, &o.sql); err != nil {
			t.Fatal(err)
		}
		objects = append(objects, o)
	}
	rows.Close()
	for _, o := range objects {
		text := spaces.ReplaceAllString(strings.TrimSpace(o.sql), " ")
		switch o.kind {
		case "table":
			cols, err := db.Query(fmt.Sprintf(`SELECT cid,name,type,"notnull",COALESCE(dflt_value,''),pk FROM pragma_table_info('%s')`, o.name))
			if err != nil {
				t.Fatal(err)
			}
			for cols.Next() {
				var cid, notNull, pk int
				var name, typ, dflt string
				if err = cols.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
					t.Fatal(err)
				}
				s.Tables[o.name] = append(s.Tables[o.name], fmt.Sprintf("%d %s %s notnull=%d default=%q pk=%d", cid, name, typ, notNull, dflt, pk))
			}
			cols.Close()
		case "index":
			s.Indexes[o.name] = text
		case "trigger":
			s.Triggers[o.name] = text
		case "view":
			s.Views[o.name] = text
		}
	}
	return s
}

// buildSchema opens a database holding a historical schema from testdata.
func buildSchema(t *testing.T, file string, version int) *sql.DB {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", file))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), file+".db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(string(raw) + fmt.Sprintf("\nPRAGMA user_version=%d;", version)); err != nil {
		t.Fatalf("building %s: %v", file, err)
	}
	return db
}

func TestMigrate15MatchesFreshSchema(t *testing.T) {
	ctx := context.Background()
	migration, err := os.ReadFile(filepath.Join("..", "..", "deploy", "migrate-15.sql"))
	if err != nil {
		t.Fatal(err)
	}
	old := buildSchema(t, "schema-v13.sql", 13)
	defer old.Close()
	if _, err = old.ExecContext(ctx, `
		INSERT INTO agents(id,status) VALUES('a','running');
		INSERT INTO account_followers(agent_id,followers,at) VALUES('a',7,'2026-11-01T00:00:00Z');`); err != nil {
		t.Fatalf("seeding v13: %v", err)
	}
	if _, err = old.ExecContext(ctx, string(migration)); err != nil {
		t.Fatalf("applying migrate-15.sql: %v", err)
	}
	fresh, err := Open(ctx, filepath.Join(t.TempDir(), "v15.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if want, got := readShape(t, fresh.db), readShape(t, old); !reflect.DeepEqual(want, got) {
		t.Fatalf("migrated schema differs from a fresh v15 schema\nfresh:    %+v\nmigrated: %+v", want, got)
	}
	var version int
	if err = old.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("user_version = %d, %v; want %d", version, err, schemaVersion)
	}
	var followers int
	var source string
	if err = old.QueryRow(`SELECT followers,source FROM account_followers WHERE agent_id='a'`).Scan(&followers, &source); err != nil || followers != 7 || source != "" {
		t.Errorf("follower row after migration = %d %q, %v; want 7 and an empty source", followers, source, err)
	}
	if _, err = old.Exec(`INSERT INTO account_daily(agent_id,day,metric,value,fetched_at,starts_at,ends_at) VALUES('a','2026-11-14','views',NULL,'2026-11-16T00:00:00Z','2026-11-14T08:00:00Z','2026-11-15T08:00:00Z')`); err != nil {
		t.Fatalf("writing account_daily after migration: %v", err)
	}
}

func TestMigrate15RefusesOtherVersions(t *testing.T) {
	migration, err := os.ReadFile(filepath.Join("..", "..", "deploy", "migrate-15.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{14, 15} {
		db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "x.db"))
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if _, err = db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, version)); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(migration)); err == nil {
			t.Errorf("migration ran on a v%d database", version)
		}
		db.Close()
	}
}
