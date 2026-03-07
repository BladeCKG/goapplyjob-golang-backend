package app

import (
	"net/http"
	"time"

	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/jobs"
	"goapplyjob-golang-backend/internal/pricing"

	"github.com/gin-gonic/gin"
)

func NewRouter(cfg config.Config, db *database.DB) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(accessLog())

	authHandler := auth.NewHandler(cfg, db)
	jobsHandler := jobs.NewHandler(cfg, db, authHandler)
	pricingHandler := pricing.NewHandler(cfg, db, authHandler)

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/db/health", func(c *gin.Context) {
		status := "ok"
		if !db.Ping(c.Request.Context()) {
			status = "failed"
		}
		c.JSON(http.StatusOK, gin.H{"status": status})
	})

	authHandler.Register(router)
	jobsHandler.Register(router)
	pricingHandler.Register(router)

	return router
}

func accessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		_ = time.Now()
		c.Next()
	}
}
