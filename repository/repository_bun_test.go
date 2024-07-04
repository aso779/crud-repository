package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aso779/crud-repository/connection"
	"github.com/aso779/crud-repository/entrel"
	"github.com/aso779/crud-repository/meta"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aso779/go-ddd/domain/usecase/dataset"
	"github.com/aso779/go-ddd/domain/usecase/metadata"
	"github.com/aso779/go-ddd/infrastructure/dataspec"
	"github.com/aso779/go-ddd/infrastructure/entmeta"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bundebug"
)

type MockBunConnSet struct {
	Mock sqlmock.Sqlmock
	db   *sql.DB
}

func NewMockBunConnSet() (*MockBunConnSet, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	return &MockBunConnSet{
		Mock: mock,
		db:   db,
	}, nil
}

func (r *MockBunConnSet) ReadPool() *bun.DB {
	return r.connect()
}

func (r *MockBunConnSet) WritePool() *bun.DB {
	return r.connect()
}

func (r *MockBunConnSet) connect() *bun.DB {
	db := bun.NewDB(r.db, pgdialect.New())
	db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))

	return db
}

type TestSimpleEnt struct {
	bun.BaseModel `bun:"table:test_simple_entities,alias:test_simple_entities"`

	ID   int    `bun:"id,pk" json:"id"`
	Name string `bun:"name" json:"name"`
}

func (r TestSimpleEnt) EntityName() string {
	return "TestSimpleEnt"
}

func (r TestSimpleEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"id": r.ID}
}

type TestComplexEnt struct {
	bun.BaseModel `bun:"table:test_complex_entities,alias:test_complex_entities"`

	FirstID     int    `bun:"first_id,pk" json:"firstId"`
	SecondID    int    `bun:"second_id,pk" json:"secondId"`
	Name        string `bun:"complex_name" json:"complexName"`
	Description string `bun:"complex_description" json:"complexDescription"`
}

func (r TestComplexEnt) EntityName() string {
	return "TestComplexEnt"
}

func (r TestComplexEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"firstId": r.FirstID, "secondId": r.SecondID}
}

type TestSoftDeleteEnt struct {
	bun.BaseModel `bun:"table:test_soft_delete_entities,alias:test_soft_delete_entities"`

	ID        int       `bun:"id,pk" json:"id"`
	Name      string    `bun:"name" json:"name"`
	DeletedAt time.Time `bun:"deleted_at,soft_delete,nullzero" json:"deletedAt"`
}

func (r TestSoftDeleteEnt) EntityName() string {
	return "TestSoftDeleteEnt"
}

func (r TestSoftDeleteEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"id": r.ID}
}

type TestSimpleEntMeta struct {
	TestSimpleEnt
}

func (r TestSimpleEntMeta) Entity() metadata.Entity { return r.TestSimpleEnt }

func (r TestSimpleEntMeta) Relations() (relations map[string]metadata.Relation) { return }

type TestComplexEntMeta struct {
	TestComplexEnt
}

func (r TestComplexEntMeta) Entity() metadata.Entity { return r.TestComplexEnt }

func (r TestComplexEntMeta) Relations() (relations map[string]metadata.Relation) { return }

type TestSoftDeleteEntMeta struct {
	TestSoftDeleteEnt
}

func (r TestSoftDeleteEntMeta) Entity() metadata.Entity { return r.TestSoftDeleteEnt }

func (r TestSoftDeleteEntMeta) Relations() (relations map[string]metadata.Relation) { return }

type TestItemEnt struct {
	bun.BaseModel `bun:"table:test_items,alias:test_items"`

	ID   int    `bun:"id,pk" json:"id"`
	Name string `bun:"name" json:"name"`
}

func (r TestItemEnt) EntityName() string {
	return "TestItemEnt"
}

func (r TestItemEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"id": r.ID}
}

type TestItemEntMeta struct {
	TestItemEnt
}

func (r TestItemEntMeta) Entity() metadata.Entity { return r.TestItemEnt }

func (r TestItemEntMeta) Relations() (relations map[string]metadata.Relation) { return }

type TestCategoryEnt struct {
	bun.BaseModel `bun:"table:test_categories,alias:test_categories"`

	ID         int    `bun:"id,pk" json:"id"`
	Name       string `bun:"name" json:"name"`
	MainItemID int    `bun:"main_item_id" json:"mainItemId"`
}

func (r TestCategoryEnt) EntityName() string {
	return "TestCategoryEnt"
}

func (r TestCategoryEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"id": r.ID}
}

type TestCategoryEntMeta struct {
	TestCategoryEnt
}

func (r TestCategoryEntMeta) Entity() metadata.Entity { return r.TestCategoryEnt }

func (r TestCategoryEntMeta) Relations() map[string]metadata.Relation {
	relations := make(map[string]metadata.Relation)

	relations["Items"] = entrel.ToMany{
		Meta:      meta.Parser(TestItemEntMeta{}),
		JoinTable: "test_items",
		ViaTable:  "test_category_items",
		JoinColumns: []entrel.JoinColumn{
			{
				Name:           "test_category_items.category_id",
				ReferencedName: "test_categories.id",
			},
		},
		InverseJoinColumns: []entrel.JoinColumn{
			{
				Name:           "item_id",
				ReferencedName: "test_items.id",
			},
		},
	}

	relations["MainItem"] = entrel.ToOne{
		Meta:      meta.Parser(TestItemEntMeta{}),
		JoinTable: "test_items",
		JoinColumns: []entrel.JoinColumn{
			{
				Name:           "main_item_id",
				ReferencedName: "id",
			},
		},
	}

	return relations
}

type TestCategoryItemEnt struct {
	bun.BaseModel `bun:"table:test_category_items,alias:test_category_items"`

	ItemID     int `bun:"item_id,pk" json:"itemId"`
	CategoryID int `bun:"category_id,pk" json:"categoryId"`
}

func (r TestCategoryItemEnt) EntityName() string {
	return "TestCategoryItemEnt"
}

func (r TestCategoryItemEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"item_id": r.ItemID, "category_id": r.CategoryID}
}

type TestCategoryItemEntMeta struct {
	TestCategoryItemEnt
}

func (r TestCategoryItemEntMeta) Entity() metadata.Entity { return r.TestCategoryItemEnt }

func (r TestCategoryItemEntMeta) Relations() (relations map[string]metadata.Relation) { return }

func NewEntities() metadata.EntityMetaContainer {
	c := entmeta.NewContainer()
	c.Add(TestSimpleEntMeta{}, meta.Parser)
	c.Add(TestComplexEntMeta{}, meta.Parser)
	c.Add(TestSoftDeleteEntMeta{}, meta.Parser)
	c.Add(TestItemEntMeta{}, meta.Parser)
	c.Add(TestCategoryEntMeta{}, meta.Parser)
	c.Add(TestCategoryItemEntMeta{}, meta.Parser)

	return c
}

type TestSimpleEntBunRepo struct {
	BunCrudRepository[TestSimpleEnt, bun.Tx]
}

func NewTestSimpleEntRepository(
	connSet connection.BunConnSet,
) *TestSimpleEntBunRepo {
	c := NewEntities()

	return &TestSimpleEntBunRepo{
		BunCrudRepository[TestSimpleEnt, bun.Tx]{
			connSet: connSet,
			Meta:    c.Get(TestSimpleEnt{}.EntityName()),
		},
	}
}

type TestComplexEntBunRepo struct {
	BunCrudRepository[TestComplexEnt, bun.Tx]
}

func NewTestComplexEntRepository(
	connSet connection.BunConnSet,
) *TestComplexEntBunRepo {
	c := NewEntities()

	return &TestComplexEntBunRepo{
		BunCrudRepository[TestComplexEnt, bun.Tx]{
			connSet: connSet,
			Meta:    c.Get(TestComplexEnt{}.EntityName()),
		},
	}
}

type TestSoftDeleteEntBunRepo struct {
	BunCrudRepository[TestSoftDeleteEnt, bun.Tx]
}

func NewTestSoftDeleteEntRepository(
	connSet connection.BunConnSet,
) *TestSoftDeleteEntBunRepo {
	c := NewEntities()

	return &TestSoftDeleteEntBunRepo{
		BunCrudRepository[TestSoftDeleteEnt, bun.Tx]{
			connSet: connSet,
			Meta:    c.Get(TestSoftDeleteEnt{}.EntityName()),
		},
	}
}

type TestCategoryBunRepo struct {
	BunCrudRepository[TestCategoryEnt, bun.Tx]
}

func NewTestCategoryEntRepository(
	connSet connection.BunConnSet,
) *TestCategoryBunRepo {
	c := NewEntities()

	return &TestCategoryBunRepo{
		BunCrudRepository[TestCategoryEnt, bun.Tx]{
			connSet: connSet,
			Meta:    c.Get(TestCategoryEnt{}.EntityName()),
		},
	}
}

// TODO move to src.
func NewPager(
	size int,
	number int,
) dataset.Pager {
	return Page{
		size:   size,
		number: number,
	}
}

type Page struct {
	size   int
	number int
}

func (r Page) GetSize() int {
	return r.size
}

func (r Page) GetNumber() int {
	return r.number
}

func (r Page) GetOffset() int {
	return r.size * r.number
}

func (r Page) IsEmpty() bool {
	return r.size == 0 && r.number == 0
}

func NewSorter() Sort {
	return Sort{
		directions: make(map[string]string),
	}
}

type Sort struct {
	directions map[string]string
}

func (r Sort) OrderBy(meta metadata.Meta) string {
	var clause = make([]string, 0)

	for k, v := range r.directions {
		column := meta.PresenterToPersistence(k)
		if column == "" {
			column = k
		}

		clause = append(clause, fmt.Sprintf("%s %s", column, strings.ToUpper(v)))
	}

	return strings.Join(clause, ",")
}

func (r Sort) WithSort(column, direction string) Sort {
	r.directions[column] = direction

	return r
}

func (r Sort) IsEmpty() bool {
	return len(r.directions) == 0
}

type crudRepositoryShortTest struct {
	conn *MockBunConnSet
}

func crudRepositoryShortTestSetUp(
	t *testing.T,
) crudRepositoryShortTest {
	t.Helper()

	conn, err := NewMockBunConnSet()
	assert.NoError(t, err)

	return crudRepositoryShortTest{
		conn: conn,
	}
}

func TestBunCrudRepository_FindOne(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		expected func(t *testing.T, res *TestSimpleEnt, err error)
	}{
		{
			name: "find one with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotNil(t, res)
			},
		},
		{
			name: "find one with multiple rows result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName").
					AddRow(2, "testName2")
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotNil(t, res)
			},
		},
		{
			name: "find one with err no rows",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"})
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestSimpleEnt, err error) {
				t.Helper()
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Nil(t, res)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindOne(context.Background(), nil, []string{"*"}, nil)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_FindOneByPk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		expected func(t *testing.T, res *TestComplexEnt, err error)
	}{
		// TODO pass
		{
			name: "find one by composite pk with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"first_id", "second_id"}).
					AddRow(1, 2)
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities.first_id = 1 AND test_complex_entities.second_id = 2\\)\\)$").
					WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestComplexEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotNil(t, res)
			},
		},
		{
			name: "find one by composite pk with multiple rows result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"first_id", "second_id"}).
					AddRow(1, 2).
					AddRow(3, 4)
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities.first_id = 1 AND test_complex_entities.second_id = 2\\)\\)$").
					WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestComplexEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotNil(t, res)
			},
		},
		{
			name: "find one by composite pk with err no rows",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"first_id", "second_id"})
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities.first_id = 1 AND test_complex_entities.second_id = 2\\)\\)$").
					WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestComplexEnt, err error) {
				t.Helper()
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Nil(t, res)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestComplexEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindOneByPk(
				context.Background(), nil, []string{"*"}, metadata.PrimaryKey{"firstId": 1, "secondId": 2},
			)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_FindAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     dataset.Specifier
		expected func(t *testing.T, res []TestSimpleEnt, err error)
	}{
		{
			name: "find all with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").WillReturnRows(rows)
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 1, len(res))
			},
		},
		{
			name: "find all with multiple rows result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName").
					AddRow(2, "testName2")
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").WillReturnRows(rows)
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 2, len(res))
			},
		},
		{
			name: "find all with err no rows",
			mock: func(conn *MockBunConnSet) {
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").
					WillReturnError(sql.ErrNoRows)
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Empty(t, res)
			},
		},
		{
			name: "find all with err no rows and spec",
			mock: func(conn *MockBunConnSet) {
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" WHERE \\(test_simple_entities\\.name = 'John'\\)$").
					WillReturnError(sql.ErrNoRows)
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Empty(t, res)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("name", "John")
			}(),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindAll(context.Background(), nil, []string{"*"}, tt.spec)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_FindPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		page     func() dataset.Pager
		sort     Sort
		expected func(t *testing.T, res []TestSimpleEnt, err error)
	}{
		{
			name: "find page with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" LIMIT 5$").
					WillReturnRows(rows)
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: NewSorter(),
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 1, len(res))
			},
		},
		{
			name: "find page with multiple rows result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1").
					AddRow(2, "testName2").
					AddRow(3, "testName3").
					AddRow(4, "testName4").
					AddRow(5, "testName5")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" LIMIT 5$").
					WillReturnRows(rows)
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: NewSorter(),
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 5, len(res))
			},
		},
		{
			name: "find page with with multiple rows result and sort",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1").
					AddRow(2, "testName2").
					AddRow(3, "testName3").
					AddRow(4, "testName4").
					AddRow(5, "testName5")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" ORDER BY name DESC LIMIT 5$").
					WillReturnRows(rows)
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: NewSorter().WithSort("name", "DESC"),
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 5, len(res))
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindPage(context.Background(), nil, []string{"*"}, nil, tt.page(), tt.sort)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_FindPageWithRelations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     func() dataset.Specifier
		page     func() dataset.Pager
		sort     Sort
		expected func(t *testing.T, res []TestCategoryEnt, err error)
	}{
		{
			name: "find page with many to many relations relation",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_categories\" INNER JOIN test_category_items ON test_category_items.category_id = test_categories.id INNER JOIN test_items ON item_id = test_items.id WHERE \\(\"test_items\".\"name\" = 'John'\\) LIMIT 5$").
					WillReturnRows(rows)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("Category.Items.name", "John")
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: NewSorter(),
			expected: func(t *testing.T, res []TestCategoryEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 1, len(res))
			},
		},
		{
			name: "find page with one to one relations relation",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_categories\" INNER JOIN test_items ON main_item_id = test_items.id WHERE \\(\"test_items\".\"name\" ILIKE 'Joh'\\) LIMIT 5$").
					WillReturnRows(rows)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewILike("Category.MainItem.name", "Joh")
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: NewSorter(),
			expected: func(t *testing.T, res []TestCategoryEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 1, len(res))
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestCategoryEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindPage(context.Background(), nil, []string{"*"}, tt.spec(), tt.page(), tt.sort)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_FindAllByPks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		expected func(t *testing.T, res []TestComplexEnt, err error)
	}{
		{
			name: "find all by primary keys with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"first_id", "second_id"}).
					AddRow(1, 2).
					AddRow(3, 4)

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities\\.first_id,test_complex_entities\\.second_id\\) IN \\(\\(1, 2\\), \\(3, 4\\)\\)\\)$").
					WillReturnRows(rows)
			},
			expected: func(t *testing.T, res []TestComplexEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestComplexEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindAllByPks(context.Background(), nil, []string{"*"}, []metadata.PrimaryKey{
				{
					"firstId":  1,
					"secondId": 2,
				},
				{
					"firstId":  3,
					"secondId": 4,
				},
			})

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_Count(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     dataset.Specifier
		expected func(t *testing.T, res int, err error)
	}{
		{
			name: "count",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(2)

				conn.Mock.ExpectQuery("^SELECT count\\(\\*\\) FROM \"test_simple_entities\"$").
					WillReturnRows(rows)
			},
			spec: func() dataset.Specifier {
				return nil
			}(),
			expected: func(t *testing.T, res int, err error) {
				t.Helper()
				assert.Equal(t, 2, res)
			},
		},
		{
			name: "count with spec",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"count"}).AddRow(2)

				conn.Mock.ExpectQuery("^SELECT count\\(\\*\\) FROM \"test_simple_entities\" WHERE \\(test_simple_entities\\.name = 'John'\\)$").
					WillReturnRows(rows)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("name", "John")
			}(),
			expected: func(t *testing.T, res int, err error) {
				t.Helper()
				assert.Equal(t, 2, res)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.Count(context.Background(), nil, tt.spec)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_CreateOne(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		entity   *TestSimpleEnt
		columns  []string
		expected func(t *testing.T, res *TestSimpleEnt, err error)
	}{
		{
			name: "create one",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(222, "TestName")

				conn.Mock.ExpectQuery("^INSERT INTO \"test_simple_entities\" \\(\"id\", \"name\"\\) VALUES \\(0, 'TestName'\\) RETURNING \\*$").
					WillReturnRows(rows)
			},
			entity: &TestSimpleEnt{
				ID:   0,
				Name: "TestName",
			},
			columns: []string{"*"},
			expected: func(t *testing.T, res *TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.Equal(t, 222, res.ID)
				assert.Equal(t, "TestName", res.Name)
			},
		},
		{
			name: "create one with returning columns",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(222, "TestName")

				conn.Mock.ExpectQuery("^INSERT INTO \"test_simple_entities\" \\(\"id\", \"name\"\\) VALUES \\(222, 'TestName'\\) RETURNING id,name$").
					WillReturnRows(rows)
			},
			entity: &TestSimpleEnt{
				ID:   222,
				Name: "TestName",
			},
			columns: []string{"id", "name"},
			expected: func(t *testing.T, res *TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.Equal(t, 222, res.ID)
				assert.Equal(t, "TestName", res.Name)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.CreateOne(context.Background(), nil, tt.entity, tt.columns)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_CreateAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		entities []TestSimpleEnt
		columns  []string
		expected func(t *testing.T, res []TestSimpleEnt, err error)
	}{
		{
			name: "create all",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1").AddRow(2, "test2")

				conn.Mock.ExpectQuery("^INSERT INTO \"test_simple_entities\" \\(\"id\", \"name\"\\) VALUES \\(1, 'test1'\\), \\(2, 'test2'\\)  RETURNING id,name$").
					WillReturnRows(rows)
			},
			entities: []TestSimpleEnt{
				{
					ID:   1,
					Name: "test1",
				},
				{
					ID:   2,
					Name: "test2",
				},
			},
			columns: []string{"id", "name"},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.CreateAll(context.Background(), nil, tt.entities, tt.columns)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_UpdateOneSimple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		mock            func(set *MockBunConnSet)
		entity          *TestSimpleEnt
		columnsToUpdate []string
		columnsToReturn []string
		expected        func(t *testing.T, res *TestSimpleEnt, err error)
	}{
		{
			name: "update one",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "test1").AddRow(2, "test2")

				conn.Mock.ExpectQuery("^UPDATE \"test_simple_entities\" AS \"test_simple_entities\" SET \"name\" = 'updatedName' WHERE \\(\"test_simple_entities\"\\.\"id\" = 333\\) RETURNING id,name$").
					WillReturnRows(rows)
			},
			entity: &TestSimpleEnt{
				ID:   333,
				Name: "updatedName",
			},
			columnsToUpdate: []string{"name"},
			columnsToReturn: []string{"id", "name"},
			expected: func(t *testing.T, res *TestSimpleEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.UpdateOne(context.Background(), nil, tt.entity, tt.columnsToUpdate, tt.columnsToReturn)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_UpdateOneComplex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		mock            func(set *MockBunConnSet)
		entity          *TestComplexEnt
		columnsToUpdate []string
		columnsToReturn []string
		expected        func(t *testing.T, res *TestComplexEnt, err error)
	}{
		{
			name: "update one",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"first_id", "second_id", "complex_name"}).AddRow(111, 222, "complex name")

				conn.Mock.ExpectQuery("^UPDATE \"test_complex_entities\" AS \"test_complex_entities\" SET \"complex_name\" = 'complex name' WHERE \\(\"test_complex_entities\"\\.\"first_id\" = 111 AND \"test_complex_entities\"\\.\"second_id\" = 222\\) RETURNING first_id,second_id,name$").
					WillReturnRows(rows)
			},
			entity: &TestComplexEnt{
				FirstID:  111,
				SecondID: 222,
				Name:     "complex name",
			},
			columnsToUpdate: []string{"complex_name"},
			columnsToReturn: []string{"first_id", "second_id", "name"},
			expected: func(t *testing.T, res *TestComplexEnt, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestComplexEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.UpdateOne(context.Background(), nil, tt.entity, tt.columnsToUpdate, tt.columnsToReturn)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_ForceDeleteWithSoftDeleteEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     dataset.Specifier
		expected func(t *testing.T, res int, err error)
	}{
		{
			name: "force delete",
			mock: func(conn *MockBunConnSet) {
				res := sqlmock.NewResult(0, 1)

				conn.Mock.ExpectExec("^DELETE FROM \"test_soft_delete_entities\" AS \"test_soft_delete_entities\" WHERE \\(test_soft_delete_entities.id = 1\\)").
					WillReturnResult(res)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("id", 1)
			}(),
			expected: func(t *testing.T, res int, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSoftDeleteEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.ForceDelete(context.Background(), nil, tt.spec)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_ForceDeleteWithSimpleEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     dataset.Specifier
		expected func(t *testing.T, res int, err error)
	}{
		{
			name: "force delete",
			mock: func(conn *MockBunConnSet) {
				res := sqlmock.NewResult(0, 1)

				conn.Mock.ExpectExec("^DELETE FROM \"test_simple_entities\" AS \"test_simple_entities\" WHERE \\(test_simple_entities.id = 1\\)").
					WillReturnResult(res)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("id", 1)
			}(),
			expected: func(t *testing.T, res int, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.ForceDelete(context.Background(), nil, tt.spec)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_DeleteWithSoftDeleteEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     dataset.Specifier
		expected func(t *testing.T, res int, err error)
	}{
		{
			name: "delete",
			mock: func(conn *MockBunConnSet) {
				res := sqlmock.NewResult(0, 1)

				conn.Mock.ExpectExec("^UPDATE \"test_soft_delete_entities\" AS \"test_soft_delete_entities\" SET \"deleted_at\" = '.+' WHERE \\(test_soft_delete_entities.id = 1\\) AND \"test_soft_delete_entities\".\"deleted_at\" IS NULL").
					WillReturnResult(res)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("id", 1)
			}(),
			expected: func(t *testing.T, res int, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSoftDeleteEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.Delete(context.Background(), nil, tt.spec)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_DeleteWithSimpleEntity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		spec     dataset.Specifier
		expected func(t *testing.T, res int, err error)
	}{
		{
			name: "delete",
			mock: func(conn *MockBunConnSet) {
				res := sqlmock.NewResult(0, 1)

				conn.Mock.ExpectExec("^DELETE FROM \"test_simple_entities\" AS \"test_simple_entities\" WHERE \\(test_simple_entities.id = 1\\)").
					WillReturnResult(res)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("id", 1)
			}(),
			expected: func(t *testing.T, res int, err error) {
				t.Helper()
				assert.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.Delete(context.Background(), nil, tt.spec)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_IsColumnValueUnique(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		column   string
		value    string
		expected func(t *testing.T, res bool, err error)
	}{
		{
			name: "is column value unique",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"exists"}).AddRow(true)

				conn.Mock.ExpectQuery("^SELECT EXISTS \\(SELECT \"test_simple_entities\".\"id\" FROM \"test_simple_entities\" WHERE \\(name = 'test'\\)\\)$").
					WillReturnRows(rows)
			},
			column: "name",
			value:  "test",
			expected: func(t *testing.T, res bool, err error) {
				t.Helper()
				assert.NoError(t, err)
				assert.Equal(t, true, res)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)
			res, err := repo.IsColumnValueUnique(context.Background(), nil, tt.column, tt.value)

			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())

			tt.expected(t, res, err)
		})
	}
}
