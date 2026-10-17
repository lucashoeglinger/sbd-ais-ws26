package repository

import (
	"ordersystem/model"
	"time"
)

type DatabaseHandler struct {
	drinks []model.Drink
	orders []model.Order
}

func NewDatabaseHandler() *DatabaseHandler {
	drinks := []model.Drink{
		{ID: 0, Name: "Beer", Price: 1, Description: "gschmakig"},
		{ID: 1, Name: "Spritzer", Price: 2, Description: "weniger gschmakig"},
	}

	orders := []model.Order{
		{ID: 0, DrinkID: 1, Amount: 20, CreatedAt: time.Now()},
		{ID: 1, DrinkID: 0, Amount: 4, CreatedAt: time.Now()},
	}

	return &DatabaseHandler{
		drinks: drinks,
		orders: orders,
	}
}

func (db *DatabaseHandler) GetDrinks() []model.Drink {
	return db.drinks
}

func (db *DatabaseHandler) GetOrders() []model.Order {
	return db.orders
}

func (db *DatabaseHandler) GetTotalledOrders() map[uint64]uint64 {
	totalledOrders := make(map[uint64]uint64)

	for _, o := range db.orders {
		totalledOrders[o.DrinkID] += o.Amount
	}

	return totalledOrders
}

func (db *DatabaseHandler) AddOrder(order *model.Order) {
	if order != nil {
		db.orders = append(db.orders, *order)
	}
}