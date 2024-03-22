package meta

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/aso779/go-ddd/domain/usecase/metadata"
	"github.com/aso779/go-ddd/infrastructure/entmeta"
)

type FieldTags struct {
	Name        string
	Presenter   string
	Persistence string
}

func structParser(t reflect.Type, m *entmeta.Meta, prefix ...string) {
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Name == "BaseModel" {
			tag := t.Field(i).Tag.Get("bun")
			tagValues := strings.Split(tag, ",")
			m.SetPersistenceName(strings.TrimPrefix(tagValues[0], "table:"))

			continue
		}

		tag := t.Field(i).Tag.Get("bun")

		if strings.HasPrefix(tag, "embed:") {
			if t.Field(i).Type.Kind() == reflect.Struct {
				structParser(t.Field(i).Type, m, strings.TrimPrefix(tag, "embed:"))
			}
		}

		if ok, fieldTags := fieldParser(t.Field(i)); ok {
			present := strings.Join(append(prefix, fieldTags.Presenter), "")
			persist := strings.Join(append(prefix, fieldTags.Persistence), "")
			m.AddFieldToPresenter(fieldTags.Name, fieldTags.Presenter)
			m.AddPresenterToPersistence(present, persist)
			m.AddPersistenceToPresenter(persist, present)
		}
	}
}

func fieldParser(field reflect.StructField) (bool, *FieldTags) {
	bunName, bunOk := field.Tag.Lookup("bun")
	if !bunOk || strings.Contains(bunName, "fk") || strings.Contains(bunName, "many2many") {
		return false, nil
	}

	bunParts := strings.Split(bunName, ",")

	jsonName, jsonOk := field.Tag.Lookup("json")

	if jsonOk {
		return true, &FieldTags{
			Name:        field.Name,
			Presenter:   jsonName,
			Persistence: bunParts[0],
		}
	}

	return false, nil
}

func relationsParser(relations map[string]metadata.Relation, parent string) map[string]metadata.Relation {
	result := make(map[string]metadata.Relation)

	for k, v := range relations {
		var fullKey string
		if parent == "" {
			fullKey = k
		} else {
			fullKey = fmt.Sprintf("%s.%s", parent, k)
		}

		result[fullKey] = v

		if len(v.GetMeta().Relations()) > 0 {
			child := relationsParser(v.GetMeta().Relations(), fullKey)
			for kk, vv := range child {
				result[kk] = vv
			}
		}
	}

	return relations
}

var Parser = func(decorator metadata.EntityMetaDecorator) metadata.Meta {
	m := entmeta.NewMeta()
	m.SetDecorator(decorator)
	m.SetEntityName(decorator.Entity().EntityName())

	t := reflect.TypeOf(decorator.Entity())

	structParser(t, m)

	m.SetRelations(relationsParser(decorator.Relations(), ""))

	return m
}
