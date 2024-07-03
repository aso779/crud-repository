package entrel

import (
	"strings"

	"github.com/aso779/go-ddd/domain/usecase/metadata"
)

type ToMany struct {
	Meta               metadata.Meta
	JoinTable          string
	ViaTable           string
	JoinColumns        []JoinColumn
	InverseJoinColumns []JoinColumn
}

func (r ToMany) Join() []metadata.Join {
	var result []metadata.Join

	sb := strings.Builder{}
	sb.WriteString("INNER JOIN ")
	sb.WriteString(r.ViaTable)
	sb.WriteString(" ON ")

	for _, v := range r.JoinColumns {
		sb.WriteString(v.Name)
		sb.WriteString(" = ")
		sb.WriteString(v.ReferencedName)
	}

	result = append(result, metadata.Join{
		JoinString: sb.String(),
	})

	sb = strings.Builder{}
	sb.WriteString("INNER JOIN ")
	sb.WriteString(r.JoinTable)
	sb.WriteString(" ON ")

	for _, v := range r.InverseJoinColumns {
		sb.WriteString(v.Name)
		sb.WriteString(" = ")
		sb.WriteString(v.ReferencedName)
	}

	result = append(result, metadata.Join{
		JoinString: sb.String(),
	})

	return result
}

func (r ToMany) Table() string {
	return r.JoinTable
}

func (r ToMany) GetMeta() metadata.Meta {
	return r.Meta
}
