package migrator_test

import (
	"context"
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	migrator "ytils.dev/sqlite-migrator"
	"ytils.dev/sqlite-migrator/fixtures/base"
	"ytils.dev/sqlite-migrator/fixtures/duplicate"
	"ytils.dev/sqlite-migrator/fixtures/invalid"
)

func sqlMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	mock.MatchExpectationsInOrder(true)
	require.NoError(t, err)
	return db, mock
}

func TestMigrator_Migrate(t *testing.T) {
	t.Parallel()

	t.Run("nothing applied", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectExec("create table test").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("create table test2").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version = ?").WithArgs(13129933).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()

		err := m.Migrate(context.Background())
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("one applied", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(13046400))
		mock.ExpectExec("create table test2").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version = ?").WithArgs(13129933).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()

		err := m.Migrate(context.Background())
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("all applied", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(13129933))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("sql error", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectExec("create table test").WillReturnError(errors.New("sql error"))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "20230601000000_one.sql: sql error")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("duplicate ID", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, duplicate.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "duplicate migration id (13046400): 20230601000000_one.sql, 20230601000000_two.sql")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("user_version read error", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnError(errors.New("read error"))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "PRAGMA user_version: read error")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("user_version write error", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectExec("create table test").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("create table test2").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version = ?").WithArgs(13129933).WillReturnError(errors.New("write error"))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "PRAGMA user_version: write error")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("invalid filename", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, invalid.FileName)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "invalid migration filename: invalid_name.sql")

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("invalid format", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, invalid.Format)

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "invalid migration filename: 000001_invalid_format.sql")

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestMigrator_WithIDFunc(t *testing.T) {
	t.Parallel()

	t.Run("ids", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)
		m.WithIDFunc(func(filename string) (uint32, error) {
			// Inverse order
			if strings.Contains(filename, "two") {
				return 1, nil
			} else {
				return 2, nil
			}
		})

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))

		// Expect migrations to be executed in inverse order
		mock.ExpectExec("create table test2").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("create table test").WillReturnResult(sqlmock.NewResult(0, 0))

		mock.ExpectExec("PRAGMA user_version = ?").WithArgs(2).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()

		err := m.Migrate(context.Background())
		require.NoError(t, err)

		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)
		m.WithIDFunc(func(filename string) (uint32, error) {
			return 0, errors.New("id error")
		})

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectRollback()

		err := m.Migrate(context.Background())
		require.ErrorContains(t, err, "invalid migration filename: id error")

		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestMigrator_WithLogFunc(t *testing.T) {
	t.Parallel()

	t.Run("log", func(t *testing.T) {
		t.Parallel()

		db, mock := sqlMock(t)

		m := migrator.New(db, base.FS)
		calls := 0
		m.WithLogFunc(func(msg string, args ...any) {
			assert.Equal(t, "migration applied", msg)
			if calls == 0 {
				assert.ElementsMatch(t, []any{"id", uint32(13046400), "filename", "20230601000000_one.sql"}, args)
			} else {
				assert.ElementsMatch(t, []any{"id", uint32(13129933), "filename", "20230601231213_two.sql"}, args)
			}
			calls++
		})

		mock.ExpectBegin()
		mock.ExpectQuery("PRAGMA user_version").WillReturnRows(sqlmock.NewRows([]string{"user_version"}).AddRow(0))
		mock.ExpectExec("create table test").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("create table test2").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("PRAGMA user_version = ?").WithArgs(13129933).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectCommit()

		err := m.Migrate(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 2, calls)

		require.NoError(t, mock.ExpectationsWereMet())
	})
}
