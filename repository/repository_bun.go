package repository

import (
	"context"
	"crud-repository/connection"
	"strings"

	"github.com/aso779/go-ddd/domain/usecase/dataset"
	"github.com/aso779/go-ddd/domain/usecase/metadata"
	"github.com/aso779/go-ddd/infrastructure/dataspec"
	"github.com/uptrace/bun"
)

type BunCrudRepository[E metadata.Entity, T bun.Tx] struct {
	connSet connection.BunConnSet
	Meta    metadata.Meta
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) FindOne(
	ctx context.Context,
	tx bun.IDB,
	columns []string,
	spec dataset.Specifier,
) (*E, error) {
	var entity E

	if tx == nil {
		tx = r.connSet.ReadPool()
	}

	query := tx.
		NewSelect().
		Model(&entity).
		Column(columns...)

	if spec != nil && !spec.IsEmpty() {
		for _, j := range spec.Joins(r.Meta) {
			query.Join(j.JoinString, j.Args...)
		}

		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	err := query.Scan(ctx)

	if err != nil {
		return nil, err
	}

	return &entity, err
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) FindOneByPk(
	ctx context.Context,
	tx bun.IDB,
	columns []string,
	pk metadata.PrimaryKey, // TODO must be slice ?
) (*E, error) {
	spec := dataspec.NewAnd()
	for k, v := range pk {
		spec.Append(dataspec.NewEqual(k, v))
	}

	return r.FindOne(ctx, tx, columns, spec)
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) FindAll(
	ctx context.Context,
	tx bun.IDB,
	columns []string,
	spec dataset.Specifier,
) ([]E, error) {
	var entities = make([]E, 0)

	if tx == nil {
		tx = r.connSet.ReadPool()
	}

	query := tx.
		NewSelect().
		Model(&entities).
		Column(columns...)

	if spec != nil && !spec.IsEmpty() {
		for _, j := range spec.Joins(r.Meta) {
			query.Join(j.JoinString, j.Args...)
		}

		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	err := query.Scan(ctx)

	return entities, err
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) FindPage(
	ctx context.Context,
	tx bun.IDB,
	columns []string,
	spec dataset.Specifier,
	page dataset.Pager,
	sort dataset.Sorter,
) ([]E, error) {
	var entities = make([]E, 0)

	if tx == nil {
		tx = r.connSet.ReadPool()
	}

	query := tx.
		NewSelect().
		Model(&entities).
		Column(columns...)

	if spec != nil && !spec.IsEmpty() {
		for _, j := range spec.Joins(r.Meta) {
			query.Join(j.JoinString, j.Args...)
		}

		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	if page != nil && !page.IsEmpty() {
		query.Limit(page.GetSize())
		query.Offset(page.GetOffset())
	}

	if sort != nil && !sort.IsEmpty() {
		query.OrderExpr(sort.OrderBy(r.Meta))
	}

	err := query.Scan(ctx)

	return entities, err
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) FindAllByPks(
	ctx context.Context,
	tx bun.IDB,
	columns []string,
	pks []metadata.PrimaryKey,
) ([]E, error) {
	var (
		keys        []string
		values      []any
		isComposite bool
		spec        dataset.Specifier
	)

	for i, pk := range pks {
		if i == 0 {
			isComposite = pk.IsComposite()
			keys = pk.Keys()
		}

		if isComposite {
			var valuesGroup []any

			for _, vv := range pk {
				valuesGroup = append(valuesGroup, vv)
			}

			values = append(values, valuesGroup)
		} else {
			for _, vv := range pk {
				values = append(values, vv)
			}
		}
	}

	if isComposite {
		spec = dataspec.NewCompositeIn(keys, bun.In(values))
	} else {
		spec = dataspec.NewIn(keys[0], bun.In(values))
	}

	return r.FindAll(ctx, tx, columns, spec)
}

func (r BunCrudRepository[E, T]) Count(
	ctx context.Context,
	tx bun.IDB,
	spec dataset.Specifier,
) (int, error) {
	var entity E

	if tx == nil {
		tx = r.connSet.ReadPool()
	}

	query := tx.
		NewSelect().
		Model(&entity)

	if spec != nil && !spec.IsEmpty() {
		for _, j := range spec.Joins(r.Meta) {
			query.Join(j.JoinString, j.Args...)
		}

		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	return query.Count(ctx)
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) CreateOne(
	ctx context.Context,
	tx bun.IDB,
	entity *E,
	columns []string,
) (*E, error) {
	if tx == nil {
		tx = r.connSet.WritePool()
	}

	_, err := tx.NewInsert().
		Model(entity).
		Returning(strings.Join(columns, ",")).
		Exec(ctx)

	return entity, err
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) CreateAll(
	ctx context.Context,
	tx bun.IDB,
	entities []E,
	columns []string,
) ([]E, error) {
	if tx == nil {
		tx = r.connSet.WritePool()
	}

	_, err := tx.NewInsert().
		Model(&entities).
		Returning(strings.Join(columns, ",")).
		Exec(ctx)

	return entities, err
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) UpdateOne(
	ctx context.Context,
	tx bun.IDB,
	entity *E,
	columns []string,
) (*E, error) {
	if tx == nil {
		tx = r.connSet.WritePool()
	}

	_, err := tx.NewUpdate().
		Model(entity).
		WherePK().
		Returning(strings.Join(columns, ",")).
		Exec(ctx)

	return entity, err
}

func (r BunCrudRepository[E, T]) ForceDelete(
	ctx context.Context,
	tx bun.IDB,
	spec dataset.Specifier,
) (int, error) {
	var entity E

	if tx == nil {
		tx = r.connSet.WritePool()
	}

	query := tx.NewDelete().
		ForceDelete().
		Model(&entity)
	if spec != nil && !spec.IsEmpty() {
		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	res, err := query.Exec(ctx)
	if err != nil {
		return 0, err
	}

	rows, err := res.RowsAffected()

	return int(rows), err
}

func (r BunCrudRepository[E, T]) Delete(
	ctx context.Context,
	tx bun.IDB,
	spec dataset.Specifier,
) (int, error) {
	var entity E

	if tx == nil {
		tx = r.connSet.WritePool()
	}

	query := tx.NewDelete().
		Model(&entity)
	if spec != nil && !spec.IsEmpty() {
		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	res, err := query.Exec(ctx)
	if err != nil {
		return 0, err
	}

	rows, err := res.RowsAffected()

	return int(rows), err
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) IsColumnValueUnique(
	ctx context.Context,
	tx bun.IDB,
	column string,
	value any,
) (bool, error) {
	if tx == nil {
		tx = r.connSet.ReadPool()
	}

	exists, err := tx.
		NewSelect().
		Column("id").
		Model((*E)(nil)).
		Where(column+" = ?", value).
		Exists(ctx)
	if err != nil {
		return false, err
	}

	return exists, nil
}
