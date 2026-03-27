package app

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/admin"
	"goapplyjob-golang-backend/internal/auth"
	"goapplyjob-golang-backend/internal/companies"
	"goapplyjob-golang-backend/internal/config"
	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/employer"
	"goapplyjob-golang-backend/internal/jobactions"
	"goapplyjob-golang-backend/internal/jobs"
	"goapplyjob-golang-backend/internal/pricing"

	"github.com/gin-gonic/gin"
)

func NewRouter(cfg config.Config, db *database.DB) *gin.Engine {
	configureGin(cfg)
	router := gin.New()
	applyTrustedProxies(router, cfg.GinTrustedProxies)
	router.Use(gin.Recovery())
	router.Use(accessLog())

	authHandler := auth.NewHandler(cfg, db)
	adminHandler := admin.NewHandler(cfg, db, authHandler)
	jobsHandler := jobs.NewHandler(cfg, db, authHandler)
	companiesHandler := companies.NewHandler(cfg, db, authHandler)
	if err := jobsHandler.WarmFilterCache(context.Background()); err != nil {
		log.Printf("failed to warm jobs filter cache: %v", err)
	}
	jobActionsHandler := jobactions.NewHandler(db, authHandler)
	pricingHandler := pricing.NewHandler(cfg, db, authHandler)
	employerHandler := employer.NewHandler(cfg, db, authHandler)

	registerHealthRoutes(router, db)
	authHandler.Register(router)
	adminHandler.Register(router)
	employerHandler.Register(router)
	jobActionsHandler.Register(router)
	jobsHandler.Register(router)
	companiesHandler.Register(router)
	pricingHandler.Register(router)

	return router
}

func NewHealthRouter(db *database.DB) *gin.Engine {
	cfg := config.Load()
	configureGin(cfg)
	router := gin.New()
	applyTrustedProxies(router, cfg.GinTrustedProxies)
	router.Use(gin.Recovery())
	router.Use(accessLog())
	registerHealthRoutes(router, db)
	return router
}

func configureGin(cfg config.Config) {
	mode := strings.TrimSpace(cfg.GinMode)
	if mode == "" {
		mode = gin.ReleaseMode
	}
	gin.SetMode(mode)
}

func applyTrustedProxies(router *gin.Engine, raw string) {
	proxies := []string{}
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		proxies = append(proxies, value)
	}
	if len(proxies) == 0 {
		proxies = nil
	}
	if err := router.SetTrustedProxies(proxies); err != nil {
		log.Printf("failed to set trusted proxies: %v", err)
	}
}

func registerHealthRoutes(router *gin.Engine, db *database.DB) {
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
}

func accessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		statusCode := http.StatusInternalServerError
		defer func() {
			durationMS := time.Since(start).Milliseconds()
			log.Printf("request method=%s path=%s status_code=%d duration_ms=%d", c.Request.Method, c.Request.URL.Path, statusCode, durationMS)
		}()
		c.Next()
		statusCode = c.Writer.Status()
	}
}
