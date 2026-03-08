package app

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"

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
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(accessLog())

	authHandler := auth.NewHandler(cfg, db)
	jobsHandler := jobs.NewHandler(cfg, db, authHandler)
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
	router.GET("/debug/hiringcafe/total-count", func(c *gin.Context) {
		rawURL := config.Getenv("WATCH_HIRINGCAFE_TOTAL_COUNT_URL", "")
		if rawURL == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "WATCH_HIRINGCAFE_TOTAL_COUNT_URL is not set"})
			return
		}
		req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, rawURL, nil)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "Mozilla/5.0")
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		payload := map[string]any{}
		_ = json.Unmarshal(body, &payload)
		c.JSON(http.StatusOK, gin.H{
			"url":             rawURL,
			"status_code":     resp.StatusCode,
			"total_count_raw": payload,
		})
	})

	authHandler.Register(router)
	employerHandler.Register(router)
	jobActionsHandler.Register(router)
	jobsHandler.Register(router)
	pricingHandler.Register(router)

	return router
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
