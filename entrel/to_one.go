package entrel

import (
	"strings"

	"github.com/aso779/go-ddd/domain/usecase/metadata"
)

type ToOne struct {
	Meta        metadata.Meta
	JoinTable   string
	JoinColumns []JoinColumn
}

func (r ToOne) Join() []metadata.Join {
	sb := strings.Builder{}
	for _, v := range r.JoinColumns {
		sb.WriteString("INNER JOIN ")
		sb.WriteString(r.JoinTable)
		sb.WriteString(" ON ")
		sb.WriteString(v.Name)
		sb.WriteString(" = ")
		sb.WriteString(r.JoinTable)
		sb.WriteString(".")
		sb.WriteString(v.ReferencedName)
	}

	return []metadata.Join{
		{
			JoinString: sb.String(),
		},
	}
}

func (r ToOne) Table() string {
	return r.JoinTable
}

func (r ToOne) GetMeta() metadata.Meta {
	return r.Meta
}
