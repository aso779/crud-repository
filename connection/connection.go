package connection

import "github.com/uptrace/bun"

type BunConnSet interface {
	ReadPool() *bun.DB
	WritePool() *bun.DB
}
