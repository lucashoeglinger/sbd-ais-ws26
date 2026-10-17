package rest

import (
	"encoding/json"
	"net/http"
	"ordersystem/model"
	"ordersystem/repository"

	"github.com/go-chi/render"
)

// GetMenu          godoc
// @tags            Menu
// @Description     Returns the menu of all drinks
// @Produce         json
// @Success         200 {array} model.Drink
// @Router          /api/menu [get]
func GetMenu(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		drinks := db.GetDrinks()

		render.Status(r, http.StatusOK)
		render.JSON(w, r, drinks)
	}
}

// GetOrders        godoc
// @tags            Order
// @Description     Gets all Orders from the db
// @Produce         json
// @Success         200 {array} model.Order
// @Router          /api/order/all [get]
func GetOrders(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orders := db.GetOrders()

		render.Status(r, http.StatusOK)
		render.JSON(w, r, orders)
	}
}

// GetOrdersTotal   godoc
// @tags            Order
// @Description     Gets the Order Total from the db
// @Produce         json
// @Success         200 {object} map[uint64]uint64
// @Router          /api/order/total [get]
func GetOrdersTotal(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orders := db.GetOrders()
		drinks := db.GetDrinks()

		drinkMap := make(map[uint64]model.Drink, len(drinks))
		for _, d := range drinks {
			drinkMap[d.ID] = d
		}

		total := make(map[uint64]uint64)

		for _, o := range orders {
			if drink, exists := drinkMap[o.DrinkID]; exists {
				total[drink.ID] += uint64(o.Amount) * uint64(drink.Price)
			}
		}

		render.Status(r, http.StatusOK)
		render.JSON(w, r, total)
	}
}

// PostOrder        godoc
// @tags            Order
// @Description     Adds an order to the db
// @Accept          json
// @Param           b body model.Order true "Order"
// @Produce         json
// @Success         200
// @Failure         400
// @Router          /api/order [post]
func PostOrder(db *repository.DatabaseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var order model.Order

		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			render.Status(r, http.StatusBadRequest)
			render.JSON(w, r, map[string]string{"error": "Invalid request payload"})
			return
		}

		db.AddOrder(&order)
		render.Status(r, http.StatusOK)
		render.JSON(w, r, map[string]string{"status": "ok"})
	}
}