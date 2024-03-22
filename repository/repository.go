package repository

import (
	"context"

	"github.com/aso779/go-ddd/domain/usecase/dataset"
	"github.com/aso779/go-ddd/domain/usecase/metadata"
	"github.com/uptrace/bun"
)

type CrudRepository[E metadata.Entity, T bun.Tx] interface {
	FindOne(
		ctx context.Context,
		tx bun.IDB,
		columns []string,
		spec dataset.Specifier,
	) (*E, error)

	FindOneByPk(
		ctx context.Context,
		tx bun.IDB,
		columns []string,
		pk metadata.PrimaryKey,
	) (*E, error)

	FindAll(
		ctx context.Context,
		tx bun.IDB,
		columns []string,
		spec dataset.Specifier,
	) ([]E, error)

	FindPage(
		ctx context.Context,
		tx bun.IDB,
		columns []string,
		spec dataset.Specifier,
		page dataset.Pager,
		sort dataset.Sorter,
	) ([]E, error)

	FindAllByPks(
		ctx context.Context,
		tx bun.IDB,
		columns []string,
		pks []metadata.PrimaryKey,
	) ([]E, error)

	Count(
		ctx context.Context,
		tx bun.IDB,
		spec dataset.Specifier,
	) (int, error)

	CreateOne(
		ctx context.Context,
		tx bun.IDB,
		entity *E,
		columns []string,
	) (*E, error)

	CreateAll(
		ctx context.Context,
		tx bun.IDB,
		entities []E,
		columns []string,
	) ([]E, error)

	UpdateOne(
		ctx context.Context,
		tx bun.IDB,
		entity *E,
		columns []string,
	) (*E, error)

	ForceDelete(
		ctx context.Context,
		tx bun.IDB,
		spec dataset.Specifier,
	) (int, error)

	Delete(
		ctx context.Context,
		tx bun.IDB,
		spec dataset.Specifier,
	) (int, error)

	IsColumnValueUnique(
		ctx context.Context,
		tx bun.IDB,
		column string,
		value any,
	) (bool, error)
}
