package db

import (
	"context"

	"github.com/keegancsmith/sqlf"
)

type Package struct {
	Scheme  string
	Name    string
	Version string
}

type Reference struct {
	Scheme      string
	Name        string
	Version     string
	Identifiers []string // TODO - should be filter by now
}

func (db *dbImpl) UpdatePackages(ctx context.Context, tw *transactionWrapper, uploadID int, packages []Package) (err error) {
	if len(packages) == 0 {
		return nil
	}

	if tw == nil {
		tw, err = db.beginTx(ctx)
		if err != nil {
			return err
		}
		defer func() {
			err = closeTx(tw.tx, err)
		}()
	}

	query := `
		INSERT INTO lsif_packages (dump_id, scheme, name, version)
		VALUES %s
		ON CONFLICT DO NOTHING
	`

	var values []*sqlf.Query
	for _, p := range packages {
		values = append(values, sqlf.Sprintf("(%s, %s, %s, %s)", uploadID, p.Scheme, p.Name, p.Version))
	}

	_, err = tw.exec(ctx, sqlf.Sprintf(query, sqlf.Join(values, ",")))
	return err
}

func (db *dbImpl) UpdateReferences(ctx context.Context, tw *transactionWrapper, uploadID int, references []Reference) (err error) {
	if len(references) == 0 {
		return nil
	}

	if tw == nil {
		tw, err = db.beginTx(ctx)
		if err != nil {
			return err
		}

		defer func() {
			err = closeTx(tw.tx, err)
		}()
	}

	query := `
		INSERT INTO lsif_references (dump_id, scheme, name, version, filter)
		VALUES %s
	`

	var values []*sqlf.Query
	for _, r := range references {
		values = append(values, sqlf.Sprintf("(%s, %s, %s, %s, %s)", uploadID, r.Scheme, r.Name, r.Version, ""))
	}

	_, err = tw.exec(ctx, sqlf.Sprintf(query, sqlf.Join(values, ",")))
	return err
}
