package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
)

type Migrator struct {
	db      *sql.DB
	fs      fs.FS
	logFunc LogFunc
	idFunc  IDFunc
}

func New(db *sql.DB, fs fs.ReadDirFS) *Migrator {
	return &Migrator{
		db:      db,
		fs:      fs,
		logFunc: func(_ string, _ ...any) {},
		idFunc:  DefaultIDFunc,
	}
}

func (m *Migrator) WithLogFunc(logFunc LogFunc) *Migrator {
	m.logFunc = logFunc
	return m
}

func (m *Migrator) WithIDFunc(idFunc IDFunc) *Migrator {
	m.idFunc = idFunc
	return m
}

func (m *Migrator) Migrate(ctx context.Context) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("BEGIN: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var currentID uint32
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&currentID); err != nil {
		return fmt.Errorf("PRAGMA user_version: %w", err)
	}

	entries, err := fs.ReadDir(m.fs, ".")
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}

	ids := make(map[uint32]string, len(entries))
	unappliedMigrations := make([]*migrationFile, 0, len(entries))

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		baseName := entry.Name()

		content, err := fs.ReadFile(m.fs, baseName)
		if err != nil {
			return fmt.Errorf("readfile: %w", err)
		}
		id, err := m.idFunc(baseName)
		if err != nil {
			return fmt.Errorf("invalid migration filename: %w", err)
		}

		if duplicateFilename, ok := ids[id]; ok {
			return fmt.Errorf("duplicate migration id (%d): %s, %s", id, duplicateFilename, baseName)
		}
		ids[id] = baseName

		// Migration has already been applied
		if id <= currentID {
			continue
		}

		unappliedMigrations = append(unappliedMigrations, &migrationFile{
			ID:       id,
			Filename: baseName,
			SQL:      string(content),
		})
	}

	if len(unappliedMigrations) == 0 {
		return nil
	}

	sort.Slice(unappliedMigrations, func(i, j int) bool {
		return unappliedMigrations[i].ID < unappliedMigrations[j].ID
	})

	for _, migration := range unappliedMigrations {
		if _, err := tx.ExecContext(ctx, migration.SQL); err != nil {
			return fmt.Errorf("%s: %w", migration.Filename, err)
		}
		m.logFunc("migration applied", "id", migration.ID, "filename", migration.Filename)
	}

	finalID := unappliedMigrations[len(unappliedMigrations)-1].ID

	if _, err := tx.ExecContext(ctx, "PRAGMA user_version = ?", finalID); err != nil {
		return fmt.Errorf("PRAGMA user_version: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("COMMIT: %w", err)
	}

	return nil
}

// LogFunc defines a function used to log the migration process.
// attrs is a list of key-value pairs, where the key is a string.
type LogFunc func(msg string, attrs ...any)
