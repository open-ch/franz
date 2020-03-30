package franz

import "errors"

var (
	ErrNoMessages = errors.New("no messages available")
	ErrNoRegistry = errors.New("registry undefined")
)
