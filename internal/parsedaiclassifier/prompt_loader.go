package parsedaiclassifier

import (
	"embed"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var promptCache = struct {
	mu    sync.RWMutex
	items map[string]string
}{
	items: map[string]string{},
}

//go:embed prompts/job_title_classification.txt prompts/job_category_inference.txt
var promptFS embed.FS

func loadEmbeddedDefaultClassifierPrompt() string {
	body, err := promptFS.ReadFile("prompts/job_title_classification.txt")
	if err != nil {
		log.Printf("parsed-job-worker prompt_embedded_load_failed error=%v", err)
		return ""
	}
	return strings.TrimSpace(string(body))
}

func loadEmbeddedDefaultCategoryPrompt() string {
	body, err := promptFS.ReadFile("prompts/job_category_inference.txt")
	if err != nil {
		log.Printf("parsed-job-worker category_prompt_embedded_load_failed error=%v", err)
		return ""
	}
	return strings.TrimSpace(string(body))
}

func loadClassifierPromptContent(source string) string {
	return loadPromptContent(source, loadEmbeddedDefaultClassifierPrompt())
}

func loadCategoryPromptContent(source string) string {
	return loadPromptContent(source, loadEmbeddedDefaultCategoryPrompt())
}

func loadPromptContent(source, embeddedDefault string) string {
	source = strings.TrimSpace(source)
	if strings.TrimSpace(embeddedDefault) == "" {
		return ""
	}
	if source == "" {
		cacheKey := "embedded:" + embeddedDefault
		promptCache.mu.RLock()
		if cached := promptCache.items[cacheKey]; cached != "" {
			promptCache.mu.RUnlock()
			return cached
		}
		promptCache.mu.RUnlock()

		promptCache.mu.Lock()
		promptCache.items[cacheKey] = embeddedDefault
		promptCache.mu.Unlock()
		return embeddedDefault
	}

	promptCache.mu.RLock()
	if cached := promptCache.items[source]; cached != "" {
		promptCache.mu.RUnlock()
		return cached
	}
	promptCache.mu.RUnlock()

	readContent, err := readPromptContentSource(source)
	if err != nil {
		log.Printf("parsed-job-worker prompt_load_failed source=%q error=%v", source, err)
		return embeddedDefault
	}
	content := strings.TrimSpace(readContent)
	if strings.TrimSpace(content) == "" {
		return embeddedDefault
	}

	promptCache.mu.Lock()
	promptCache.items[source] = content
	promptCache.mu.Unlock()
	return content
}

func readPromptContentSource(source string) (string, error) {
	if !strings.Contains(source, "://") {
		source = filepath.Clean(source)
	}
	if strings.HasPrefix(source, "http://") || strings.HasPrefix(source, "https://") {
		client := &http.Client{Timeout: 20 * time.Second}
		req, err := http.NewRequest(http.MethodGet, source, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", "goapplyjob-ai-classifier/1.0")
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", &url.Error{Op: "GET", URL: source, Err: io.ErrUnexpectedEOF}
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 512*1024))
		if err != nil {
			return "", err
		}
		return string(body), nil
	}

	if strings.HasPrefix(source, "file://") {
		parsed, err := url.Parse(source)
		if err != nil {
			return "", err
		}
		body, err := os.ReadFile(parsed.Path)
		if err != nil {
			return "", err
		}
		return string(body), nil
	}

	body, err := os.ReadFile(source)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
