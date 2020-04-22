package sqliteutil

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type BatchInserter struct {
	db                Execable
	numColumns        int
	maxBatchSize      int
	batch             []interface{}
	queryPrefix       string
	queryPlaceholders []string
}

const MaxNumSqliteParameters = 999

type Execable interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func NewBatchInserter(db Execable, tableName string, columnNames ...string) *BatchInserter {
	numColumns := len(columnNames)
	maxBatchSize := (MaxNumSqliteParameters / len(columnNames)) * len(columnNames)

	placeholders := make([]string, numColumns)
	quotedColumnNames := make([]string, numColumns)
	queryPlaceholders := make([]string, maxBatchSize/numColumns)

	for i, columnName := range columnNames {
		placeholders[i] = "?"
		quotedColumnNames[i] = fmt.Sprintf(`"%s"`, columnName)
	}

	for i := range queryPlaceholders {
		queryPlaceholders[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
	}

	return &BatchInserter{
		db:                db,
		numColumns:        numColumns,
		maxBatchSize:      maxBatchSize,
		batch:             make([]interface{}, 0, maxBatchSize),
		queryPrefix:       fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES `, tableName, strings.Join(quotedColumnNames, ",")),
		queryPlaceholders: queryPlaceholders,
	}
}

func (bi *BatchInserter) Insert(ctx context.Context, values ...interface{}) error {
	if len(values) != bi.numColumns {
		return fmt.Errorf("expected %d values, got %d", bi.numColumns, len(values))
	}

	bi.batch = append(bi.batch, values...)

	if len(bi.batch) >= bi.maxBatchSize {
		return bi.Flush(ctx)
	}

	return nil
}

func (bi *BatchInserter) Flush(ctx context.Context) error {
	if batch := bi.pop(); len(batch) > 0 {
		query := bi.queryPrefix + strings.Join(bi.queryPlaceholders[:len(batch)/bi.numColumns], ",")

		if _, err := bi.db.ExecContext(ctx, query, batch...); err != nil {
			return err
		}
	}

	return nil
}

func (bi *BatchInserter) pop() (batch []interface{}) {
	if len(bi.batch) < bi.maxBatchSize {
		batch, bi.batch = bi.batch, bi.batch[:0]
		return
	}

	batch, bi.batch = bi.batch[:bi.maxBatchSize], bi.batch[bi.maxBatchSize:]
	return
}
