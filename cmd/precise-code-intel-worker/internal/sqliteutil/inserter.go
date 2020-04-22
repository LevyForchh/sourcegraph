package sqliteutil

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/keegancsmith/sqlf"
)

const MaxNumSqliteParameters = 999

type Execable interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type BatchInserter struct {
	db            Execable
	tableName     string
	columnNames   []string
	batches       [][]interface{}
	flushThresold int
}

func NewBatchInserter(db Execable, tableName string, columnNames ...string) *BatchInserter {
	return &BatchInserter{
		db:            db,
		tableName:     tableName,
		columnNames:   columnNames,
		flushThresold: MaxNumSqliteParameters / len(columnNames),
	}
}

func (i *BatchInserter) Insert(values ...interface{}) error {
	if len(values) != len(i.columnNames) {
		return fmt.Errorf("expected %d values, got %d", len(i.columnNames), len(values))
	}

	i.batches = append(i.batches, values)

	if len(i.batches) < i.flushThresold {
		return nil
	}
	return i.Flush()
}

func (i *BatchInserter) Flush() error {
	var batch [][]interface{}
	if len(i.batches) < i.flushThresold {
		batch, i.batches = i.batches, nil
	} else {
		batch, i.batches = i.batches[:i.flushThresold], i.batches[i.flushThresold:]
	}

	if len(batch) == 0 {
		return nil
	}

	// TODO - refactor this, don't build this every time
	query := "INSERT INTO " + i.tableName + " (" + strings.Join(i.columnNames, ", ") + ") VALUES %s"

	var queries []*sqlf.Query
	for _, args := range batch {
		var ps []string
		for range args {
			ps = append(ps, "%s")
		}

		queries = append(queries, sqlf.Sprintf("("+strings.Join(ps, ",")+")", args...))
	}

	qx := sqlf.Sprintf(query, sqlf.Join(queries, ","))
	_, err := i.db.ExecContext(context.Background(), qx.Query(sqlf.SimpleBindVar), qx.Args()...)
	return err
}
