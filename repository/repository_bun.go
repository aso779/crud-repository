package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/aso779/bun-pg-connector"

	"github.com/aso779/go-ddd/domain/usecase/dataset"
	"github.com/aso779/go-ddd/domain/usecase/metadata"
	"github.com/aso779/go-ddd/infrastructure/dataspec"
	"github.com/uptrace/bun"
)

type BunCrudRepository[E metadata.Entity, T bun.Tx] struct {
	ConnSet bunpgconnector.BunConnSet
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
		tx = r.ConnSet.ReadPool()
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
		return nil, fmt.Errorf("find one: %w", err)
	}

	return &entity, nil
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) FindOneByPk(
	ctx context.Context,
	tx bun.IDB,
	columns []string,
	pk metadata.PrimaryKey,
) (*E, error) {
	spec := dataspec.NewAnd()

	for _, v := range pk.Sorted() {
		for kk, vv := range v {
			spec.Append(dataspec.NewEqual(kk, vv))
		}
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
		tx = r.ConnSet.ReadPool()
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
	if err != nil {
		return entities, fmt.Errorf("find all: %w", err)
	}

	return entities, nil
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
		tx = r.ConnSet.ReadPool()
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
	if err != nil {
		return entities, fmt.Errorf("find page: %w", err)
	}

	return entities, nil
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
			keys = pk.SortedKeys()
		}

		if isComposite {
			var valuesGroup []any

			for _, vv := range pk.Sorted() {
				for _, vvv := range vv {
					valuesGroup = append(valuesGroup, vvv)
				}
			}

			values = append(values, valuesGroup) // nolint:asasalint
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
		tx = r.ConnSet.ReadPool()
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

	count, err := query.Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("count: %w", err)
	}

	return count, nil
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) CreateOne(
	ctx context.Context,
	tx bun.IDB,
	entity *E,
	columns []string,
) (*E, error) {
	if tx == nil {
		tx = r.ConnSet.WritePool()
	}

	_, err := tx.NewInsert().
		Model(entity).
		Returning(strings.Join(columns, ",")).
		Exec(ctx)

	if err != nil {
		return nil, fmt.Errorf("crate one: %w", err)
	}

	return entity, nil
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) CreateAll(
	ctx context.Context,
	tx bun.IDB,
	entities []E,
	columns []string,
) ([]E, error) {
	if tx == nil {
		tx = r.ConnSet.WritePool()
	}

	_, err := tx.NewInsert().
		Model(&entities).
		Returning(strings.Join(columns, ",")).
		Exec(ctx)

	if err != nil {
		return entities, fmt.Errorf("create one: %w", err)
	}

	return entities, nil
}

// TODO field instead column ?

func (r BunCrudRepository[E, T]) UpdateOne(
	ctx context.Context,
	tx bun.IDB,
	entity *E,
	columnsToUpdate []string,
	columns []string,
) (*E, error) {
	if tx == nil {
		tx = r.ConnSet.WritePool()
	}

	_, err := tx.NewUpdate().
		Model(entity).
		Column(columnsToUpdate...).
		WherePK().
		Returning(strings.Join(columns, ",")).
		Exec(ctx)

	if err != nil {
		return entity, fmt.Errorf("update one: %w", err)
	}

	return entity, nil
}

func (r BunCrudRepository[E, T]) ForceDelete(
	ctx context.Context,
	tx bun.IDB,
	spec dataset.Specifier,
) (int, error) {
	var entity E

	if tx == nil {
		tx = r.ConnSet.WritePool()
	}

	query := tx.NewDelete().
		ForceDelete().
		Model(&entity)
	if spec != nil && !spec.IsEmpty() {
		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	res, err := query.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("force delete: %w", err)
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
		tx = r.ConnSet.WritePool()
	}

	query := tx.NewDelete().
		Model(&entity)
	if spec != nil && !spec.IsEmpty() {
		query.Where(spec.Query(r.Meta), spec.Values()...)
	}

	res, err := query.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
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
		tx = r.ConnSet.ReadPool()
	}

	exists, err := tx.
		NewSelect().
		Column("id").
		Model((*E)(nil)).
		Where(column+" = ?", value).
		Exists(ctx)
	if err != nil {
		return false, fmt.Errorf("is column value unique: %w", err)
	}

	return exists, nil
}
