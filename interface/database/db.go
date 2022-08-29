package database

type DB interface {
	Exec()
	Close()
}
