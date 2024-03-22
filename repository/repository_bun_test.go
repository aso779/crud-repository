package repository

import (
	"context"
	"crud-repository/connection"
	"crud-repository/meta"
	"database/sql"
	"fmt"
	"strings"
	"testing"

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
		return nil, err
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

	ID   int    `bun:"id" json:"id"`
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

	FirstID  int `bun:"first_id" json:"firstId"`
	SecondID int `bun:"second_id" json:"secondId"`
}

func (r TestComplexEnt) EntityName() string {
	return "TestComplexEnt"
}

func (r TestComplexEnt) PrimaryKey() metadata.PrimaryKey {
	return metadata.PrimaryKey{"firstId": r.FirstID, "secondId": r.SecondID}
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

func NewEntities() metadata.EntityMetaContainer {
	c := entmeta.NewContainer()
	c.Add(TestSimpleEntMeta{}, meta.Parser)
	c.Add(TestComplexEntMeta{}, meta.Parser)

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

// TODO move to src
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
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Nil(t, res)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		expected func(t *testing.T, res *TestComplexEnt, err error)
	}{
		{
			name: "find one by composite pk with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"first_id", "second_id"}).
					AddRow(1, 2)
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities.first_id = 1 AND test_complex_entities.second_id = 2\\)\\)$").
					WillReturnRows(rows)
			},
			expected: func(t *testing.T, res *TestComplexEnt, err error) {
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
				assert.NoError(t, err)
				assert.NotNil(t, res)
			},
		},
		// TODO pass
		//{
		//	name: "find one by composite pk with err no rows",
		//	mock: func(conn *MockBunConnSet) {
		//		rows := sqlmock.NewRows([]string{"first_id", "second_id"})
		//		conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities.first_id = 1 AND test_complex_entities.second_id = 2\\)\\)$").
		//			WillReturnRows(rows)
		//	},
		//	expected: func(t *testing.T, res *TestComplexEnt, err error) {
		//		assert.ErrorIs(t, err, sql.ErrNoRows)
		//		assert.Nil(t, res)
		//	},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 2, len(res))
			},
		},
		{
			name: "find all with err no rows",
			mock: func(conn *MockBunConnSet) {
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\"$").WillReturnError(sql.ErrNoRows)
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Empty(t, res)
			},
		},
		{
			name: "find all with err no rows and spec",
			mock: func(conn *MockBunConnSet) {
				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" WHERE \\(test_simple_entities\\.name = 'John'\\)$").WillReturnError(sql.ErrNoRows)
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				assert.ErrorIs(t, err, sql.ErrNoRows)
				assert.Empty(t, res)
			},
			spec: func() dataset.Specifier {
				return dataspec.NewEqual("name", "John")
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		page     func() dataset.Pager
		sort     func() Sort
		expected func(t *testing.T, res []TestSimpleEnt, err error)
	}{
		{
			name: "find page with single row result",
			mock: func(conn *MockBunConnSet) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "testName1")

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" LIMIT 5$").WillReturnRows(rows)
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: func() Sort {
				return NewSorter()
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
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

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" LIMIT 5$").WillReturnRows(rows)
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: func() Sort {
				return NewSorter()
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
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

				conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_simple_entities\" ORDER BY name DESC LIMIT 5$").WillReturnRows(rows)
			},
			page: func() dataset.Pager {
				return NewPager(5, 0)
			},
			sort: func() Sort {
				return NewSorter().WithSort("name", "DESC")
			},
			expected: func(t *testing.T, res []TestSimpleEnt, err error) {
				assert.NoError(t, err)
				assert.NotEmpty(t, res)
				assert.Equal(t, 5, len(res))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject := crudRepositoryShortTestSetUp(t)
			repo := NewTestSimpleEntRepository(subject.conn)

			tt.mock(subject.conn)

			res, err := repo.FindPage(context.Background(), nil, []string{"*"}, nil, tt.page(), tt.sort())
			assert.NoError(t, subject.conn.Mock.ExpectationsWereMet())
			tt.expected(t, res, err)
		})
	}
}

func TestBunCrudRepository_FindAllByPks(t *testing.T) {
	tests := []struct {
		name     string
		mock     func(set *MockBunConnSet)
		expected func(t *testing.T, res []TestComplexEnt, err error)
	}{
		// TODO pass
		//{
		//	name: "find all by primary keys with single row result",
		//	mock: func(conn *MockBunConnSet) {
		//		rows := sqlmock.NewRows([]string{"first_id", "second_id"}).
		//			AddRow(1, 2).
		//			AddRow(3, 4)
		//
		//		conn.Mock.ExpectQuery("^SELECT \\* FROM \"test_complex_entities\" WHERE \\(\\(test_complex_entities\\.first_id,test_complex_entities\\.second_id\\) IN \\(\\(1, 2\\), \\(3, 4\\)\\)\\)$").
		//			WillReturnRows(rows)
		//	},
		//},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
