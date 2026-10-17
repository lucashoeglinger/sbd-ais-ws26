package model

import "time"

type Order struct {
	ID uint64 `json:"id"`
	DrinkID uint64 `json:"drink_id"` // foreign key
	CreatedAt time.Time `json:"created_at"`
	Amount uint64 `json:"amount"`
	// todo Add fields: CreatedAt (time.Time), Amount with suitable types
	// todo json attributes need to be snakecase, i.e. name, created_at, my_variable, ..
}
