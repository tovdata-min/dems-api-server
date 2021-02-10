package main

import (
	// Echo
	middleware "github.com/labstack/echo/middleware"
	// Router
	requestRouter "dems-api-server/routes"
)

func main() {
	echo := requestRouter.Router()
	// Set middleware
	echo.Use(middleware.Logger())
	echo.Use(middleware.Recover())
	// Start
	echo.Logger.Fatal(echo.Start(":4000"))
}