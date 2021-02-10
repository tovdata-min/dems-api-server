package routes

import (
	"net/http"
	// Echo
	echo "github.com/labstack/echo"
	requestHandler "dems-api-server/handlers/request"
)

func Router() *echo.Echo {
	e := echo.New()
	// Main
	e.File("/", "views/main.html")
	e.Static("/assets", "public")

	// Health check
	e.GET("/health", func (ctx echo.Context) error {
		return ctx.String(http.StatusOK, "alive")
	})

	// Create router groups
	requestRouter := e.Group("/request")
	{
		requestRouter.GET("/list", requestHandler.RequestList)
		requestRouter.GET("/:requestID", requestHandler.ExportRequest)
		requestRouter.GET("/info", requestHandler.RequestInfo)
	}

	return e
}