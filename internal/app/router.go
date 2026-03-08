package app

import (
	"context"
	"log"
	"net/http"
	"time"

	"goapplyjob-golang-backend/internal/admin"
	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/employer"
	"goapplyjob-golang-backend/internal/jobactions"
	"goapplyjob-golang-backend/internal/jobs"
	"goapplyjob-golang-backend/internal/pricing"

	"github.com/gin-gonic/gin"
)

func NewRouter(cfg config.Config, db *database.DB) *gin.Engine {
	if cfg.APIDebug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(accessLog(cfg.APIDebug))

	authHandler := auth.NewHandler(cfg, db)
	adminHandler := admin.NewHandler(db, authHandler)
	jobsHandler := jobs.NewHandler(cfg, db, authHandler)
	if err := jobsHandler.WarmFilterCache(context.Background()); err != nil {
		log.Printf("failed to warm jobs filter cache: %v", err)
	}
	jobActionsHandler := jobactions.NewHandler(db, authHandler)
	pricingHandler := pricing.NewHandler(cfg, db, authHandler)
	employerHandler := employer.NewHandler(cfg, db, authHandler)

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
	adminHandler.Register(router)
	employerHandler.Register(router)
	jobActionsHandler.Register(router)
	jobsHandler.Register(router)
	pricingHandler.Register(router)

	return router
}

func accessLog(debug bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		statusCode := http.StatusInternalServerError
		defer func() {
			durationMS := time.Since(start).Milliseconds()
			if debug {
				log.Printf("request method=%s path=%s query=%s status_code=%d duration_ms=%d", c.Request.Method, c.Request.URL.Path, c.Request.URL.RawQuery, statusCode, durationMS)
				return
			}
			log.Printf("request method=%s path=%s status_code=%d duration_ms=%d", c.Request.Method, c.Request.URL.Path, statusCode, durationMS)
		}()
		c.Next()
		statusCode = c.Writer.Status()
	}
}
