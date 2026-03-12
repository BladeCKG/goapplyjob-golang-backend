package watcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"goapplyjob-golang-backend/internal/database"
	"goapplyjob-golang-backend/internal/sources/builtin"
	"goapplyjob-golang-backend/internal/sources/dailyremote"
	"goapplyjob-golang-backend/internal/sources/hiringcafe"
	"goapplyjob-golang-backend/internal/sources/remotive"
	"goapplyjob-golang-backend/internal/sources/workable"
)

const (
	sourceRemoterocketship = "remoterocketship"
	sourceBuiltin          = "builtin"
	sourceRemotive         = "remotive"
	sourceHiringCafe       = "hiringcafe"
	sourceWorkable         = "workable"
	sourceDailyremote      = "dailyremote"
	payloadTypeDelta       = "delta"
	payloadTypeXML         = "delta_xml"
)

var (
	lastmodPattern     = regexp.MustCompile(`(?is)<lastmod>\s*([^<]+?)\s*</lastmod>`)
	urlOpenPattern     = regexp.MustCompile(`(?is)<url(?:\s|>)`)
	urlBlockPattern    = regexp.MustCompile(`(?is)<url(?:\s[^>]*)?>.*?</url>`)
	urlSetClosePattern = regexp.MustCompile(`(?is)</urlset\s*>`)
	remotiveJobIDRE    = regexp.MustCompile(`(?i)(?:job-)?(\d+)(?:[/?#]|$)`)
	remotiveIndexRE    = regexp.MustCompile(`(?i)sitemap-job-postings-(\d+)\.xml`)
)

type Config struct {
	Enabled                         bool
	RemoteRocketshipUSJobSitemapURL string
	IntervalMinutes                 float64
	SampleKB                        int
	TimeoutSeconds                  float64
	BuiltinBaseURL                  string
	BuiltinMaxPage                  int
	BuiltinPagesPerCycle            int
	BuiltinCheckpointPages          int
	BuiltinFetchIntervalSeconds     float64
	Builtin429RetryCount            int
	Builtin429BackoffSeconds        float64
	WorkableAPIURL                  string
	WorkablePageLimit               int
	RemotiveSitemapURLTemplate      string
	RemotiveSitemapMaxIndex         int
	RemotiveSitemapMinIndex         int
	DailyRemoteBaseURL              string
	DailyRemoteMaxPage              int
	DailyRemotePagesPerCycle        int
	HiringCafeSearchAPIURL          string
	HiringCafeTotalCountURL         string
	HiringCafePageSize              int
	EnabledSources                  map[string]struct{}
}

type (
	FetchSampleFunc func() ([]byte, error)
	FetchFullFunc   func() ([]byte, error)
)

type Service struct {
	Config                                   Config
	DB                                       *database.DB
	RemoteRocketShipUSJobsSitemapFetchSample FetchSampleFunc
	RemoteRocketShipUSJobsSitemapFetchFull   FetchFullFunc
	FetchText                                func(string) (string, error)
	status                                   map[string]any
}

func New(config Config, db *database.DB) *Service {
	svc := &Service{Config: config, DB: db}
	svc.status = map[string]any{
		"enabled":                       config.Enabled,
		"url":                           config.RemoteRocketshipUSJobSitemapURL,
		"interval_minutes":              config.IntervalMinutes,
		"sample_kb":                     config.SampleKB,
		"enabled_sources":               sortedSourceNames(config.EnabledSources),
		"workable_api_url":              config.WorkableAPIURL,
		"remotive_sitemap_url_template": config.RemotiveSitemapURLTemplate,
		"remotive_sitemap_max_index":    config.RemotiveSitemapMaxIndex,
		"remotive_sitemap_min_index":    config.RemotiveSitemapMinIndex,
		"dailyremote_base_url":          config.DailyRemoteBaseURL,
		"dailyremote_max_page":          config.DailyRemoteMaxPage,
		"dailyremote_pages_per_cycle":   config.DailyRemotePagesPerCycle,
		"hiringcafe_search_api_url":     config.HiringCafeSearchAPIURL,
		"hiringcafe_total_count_url":    config.HiringCafeTotalCountURL,
		"hiringcafe_page_size":          config.HiringCafePageSize,
		"running":                       false,
		"last_check_at":                 nil,
		"last_change_at":                nil,
		"last_sample_hash":              nil,
		"last_error":                    nil,
		"last_overlap_bytes":            0,
		"last_delta_source":             nil,
		"last_delta_size":               0,
		"last_new_sample_lastmod":       nil,
		"last_previous_first_lastmod":   nil,
		"last_delta_payload_id":         nil,
	}
	timeoutSeconds := config.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = 30
	}
	httpClient := &http.Client{Timeout: time.Duration(timeoutSeconds * float64(time.Second))}
	userAgent := "goapplyjob-backend/watcher"
	readBody := func(resp *http.Response, limitBytes int64) ([]byte, error) {
		if limitBytes <= 0 {
			limitBytes = 25 * 1024 * 1024
		}
		defer resp.Body.Close()
		return io.ReadAll(io.LimitReader(resp.Body, limitBytes))
	}
	var doFetchBytes func(rawURL string, rangeHeader string) ([]byte, error)
	doFetchBytes = func(rawURL string, rangeHeader string) ([]byte, error) {
		if strings.TrimSpace(rawURL) == "" {
			return nil, errors.New("empty url")
		}
		req, err := http.NewRequest(http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		if strings.TrimSpace(rangeHeader) != "" {
			req.Header.Set("Range", rangeHeader)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && rangeHeader != "" {
				_ = resp.Body.Close()
				return doFetchBytes(rawURL, "")
			}
			bodyPreview, _ := readBody(resp, 1024)
			return nil, errors.New("http status " + strconv.Itoa(resp.StatusCode) + " body=" + strings.TrimSpace(string(bodyPreview)))
		}
		return readBody(resp, 25*1024*1024)
	}
	svc.RemoteRocketShipUSJobsSitemapFetchSample = func() ([]byte, error) {
		sampleBytes := int64(max(config.SampleKB, 1) * 1024)
		return doFetchBytes(config.RemoteRocketshipUSJobSitemapURL, "bytes=0-"+strconv.FormatInt(sampleBytes-1, 10))
	}
	svc.RemoteRocketShipUSJobsSitemapFetchFull = func() ([]byte, error) {
		return doFetchBytes(config.RemoteRocketshipUSJobSitemapURL, "")
	}
	svc.FetchText = func(rawURL string) (string, error) {
		data, err := doFetchBytes(rawURL, "")
		if err != nil {
			return "", err
		}
		return string(data), nil
	}
	return svc
}

func (s *Service) Status() map[string]any {
	out := map[string]any{}
	for k, v := range s.status {
		out[k] = v
	}
	return out
}

func (s *Service) setStatus(values map[string]any) {
	for k, v := range values {
		s.status[k] = v
	}
}

func (s *Service) RunForever(runOnce bool) error {
	if !s.Config.Enabled {
		s.setStatus(map[string]any{"last_error": nil})
		return nil
	}
	if len(s.Config.EnabledSources) == 0 {
		s.setStatus(map[string]any{"last_error": "No source enabled"})
		return nil
	}

	s.setStatus(map[string]any{"running": true})
	defer s.setStatus(map[string]any{"running": false})

	for {
		if err := s.RunOnce(); err != nil {
			return err
		}
		if runOnce {
			return nil
		}
		sleepSeconds := s.Config.IntervalMinutes * 60
		if sleepSeconds < 1 {
			sleepSeconds = 1
		}
		time.Sleep(time.Duration(sleepSeconds * float64(time.Second)))
	}
}

func (s *Service) RunOnce() error {
	log.Printf("watcher cycle_start enabled_sources=%v", sortedSourceNames(s.Config.EnabledSources))
	if strings.TrimSpace(s.Config.RemoteRocketshipUSJobSitemapURL) != "" && s.isSourceEnabled(sourceRemoterocketship) {
		log.Printf("watcher source_start source=%s runner=runOnceRemoteRocketship", sourceRemoterocketship)
		if err := s.runOnceRemoteRocketship(); err != nil {
			log.Printf("watcher source_failed source=%s runner=runOnceRemoteRocketship error=%v", sourceRemoterocketship, err)
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		} else {
			log.Printf("watcher source_done source=%s runner=runOnceRemoteRocketship", sourceRemoterocketship)
		}
	}
	if s.isRemotiveConfigured() && s.isSourceEnabled(sourceRemotive) {
		log.Printf("watcher source_start source=%s runner=runOnceRemotive", sourceRemotive)
		if err := s.runOnceRemotive(); err != nil {
			log.Printf("watcher source_failed source=%s runner=runOnceRemotive error=%v", sourceRemotive, err)
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		} else {
			log.Printf("watcher source_done source=%s runner=runOnceRemotive", sourceRemotive)
		}
	}
	if strings.TrimSpace(s.Config.WorkableAPIURL) != "" && s.isSourceEnabled("workable") {
		log.Printf("watcher source_start source=workable runner=runOnceWorkable")
		if err := s.runOnceWorkable(); err != nil {
			log.Printf("watcher source_failed source=workable runner=runOnceWorkable error=%v", err)
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		} else {
			log.Printf("watcher source_done source=workable runner=runOnceWorkable")
		}
	}
	if strings.TrimSpace(s.Config.DailyRemoteBaseURL) != "" && strings.Contains(s.Config.DailyRemoteBaseURL, "{page}") && s.Config.DailyRemoteMaxPage >= 1 && s.Config.DailyRemotePagesPerCycle >= 1 && s.isSourceEnabled(sourceDailyremote) {
		log.Printf("watcher source_start source=%s runner=runOnceDailyremote", sourceDailyremote)
		if err := s.runOnceDailyremote(); err != nil {
			log.Printf("watcher source_failed source=%s runner=runOnceDailyremote error=%v", sourceDailyremote, err)
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		} else {
			log.Printf("watcher source_done source=%s runner=runOnceDailyremote", sourceDailyremote)
		}
	}
	if strings.TrimSpace(s.Config.BuiltinBaseURL) != "" && s.isSourceEnabled(sourceBuiltin) {
		log.Printf("watcher source_start source=%s runner=runOnceBuiltin", sourceBuiltin)
		if err := s.runOnceBuiltin(); err != nil {
			log.Printf("watcher source_failed source=%s runner=runOnceBuiltin error=%v", sourceBuiltin, err)
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		} else {
			log.Printf("watcher source_done source=%s runner=runOnceBuiltin", sourceBuiltin)
		}
	}
	if strings.TrimSpace(s.Config.HiringCafeSearchAPIURL) != "" && strings.TrimSpace(s.Config.HiringCafeTotalCountURL) != "" && s.isSourceEnabled(sourceHiringCafe) {
		log.Printf("watcher source_start source=%s runner=runOnceHiringCafe", sourceHiringCafe)
		if err := s.runOnceHiringCafe(); err != nil {
			log.Printf("watcher source_failed source=%s runner=runOnceHiringCafe error=%v", sourceHiringCafe, err)
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		} else {
			log.Printf("watcher source_done source=%s runner=runOnceHiringCafe", sourceHiringCafe)
		}
	}
	return nil
}

func (s *Service) runOnceDailyremote() error {
	statePayload, err := s.loadStatePayload(sourceDailyremote)
	if err != nil {
		return err
	}
	previousLatestExternalID := intFromAny(statePayload["latest_external_id"], 0)
	newestExternalID := previousLatestExternalID
	pagesScanned := 0
	payloadRows := make([]map[string]any, 0)

	for page := 1; page <= s.Config.DailyRemoteMaxPage && pagesScanned < s.Config.DailyRemotePagesPerCycle; page++ {
		pageURL := strings.ReplaceAll(s.Config.DailyRemoteBaseURL, "{page}", strconv.Itoa(page))
		log.Printf("DailyRemote fetch_start page=%d url=%s", page, pageURL)
		htmlText, err := s.FetchText(pageURL)
		if err != nil {
			return err
		}
		pagesScanned++

		listings := dailyremote.ExtractJobListings(htmlText, pageURL, time.Now().UTC())
		if len(listings) == 0 {
			break
		}
		firstExternalID := intFromAny(listings[0]["external_id"], 0)
		if firstExternalID > newestExternalID {
			newestExternalID = firstExternalID
		}

		stopScan := false
		for _, listing := range listings {
			externalID := intFromAny(listing["external_id"], 0)
			if externalID <= 0 {
				continue
			}
			postDate, _ := listing["post_date"].(time.Time)
			rowURL, _ := listing["url"].(string)
			if strings.TrimSpace(rowURL) == "" || postDate.IsZero() {
				continue
			}
			if previousLatestExternalID > 0 && externalID <= previousLatestExternalID {
				stopScan = true
				break
			}
			payloadRows = append(payloadRows, map[string]any{
				"url":       strings.TrimSpace(rowURL),
				"post_date": postDate.UTC(),
			})
		}
		if stopScan {
			break
		}
	}

	var payloadID any
	if len(payloadRows) > 0 {
		savedID, err := s.saveDeltaPayloadForSource(
			sourceDailyremote,
			strings.ReplaceAll(s.Config.DailyRemoteBaseURL, "{page}", "1"),
			dailyremote.PayloadType,
			dailyremote.SerializeImportRows(payloadRows),
		)
		if err != nil {
			return err
		}
		payloadID = savedID
	}

	latestExternalIDValue := any(nil)
	switch {
	case newestExternalID > 0:
		latestExternalIDValue = newestExternalID
	case previousLatestExternalID > 0:
		latestExternalIDValue = previousLatestExternalID
	}

	return s.saveStatePayload(sourceDailyremote, map[string]any{
		"latest_external_id":       latestExternalIDValue,
		"pages_scanned_last_cycle": pagesScanned,
		"latest_delta_count":       len(payloadRows),
		"latest_delta_payload_id":  payloadID,
	})
}

func (s *Service) runOnceRemoteRocketship() error {
	sample, err := s.RemoteRocketShipUSJobsSitemapFetchSample()
	if err != nil {
		s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
		return err
	}

	currentHash := sha256Hex(sample)
	previousHash, previousFirstLastmod, _ := s.loadRemoteRocketshipState(context.Background())
	currentFirstLastmod := s.ExtractFirstLastmod(sample)

	s.setStatus(map[string]any{
		"last_check_at":    utcNowISO(),
		"last_sample_hash": currentHash,
		"last_error":       nil,
	})

	if currentHash == previousHash {
		_ = s.saveRemoteRocketshipState(context.Background(), currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))
		s.setStatus(map[string]any{"last_overlap_bytes": len(sample)})
		return nil
	}

	newSampleLastmod := s.ExtractLastLastmod(sample)
	previousDT := s.parseLastmod(previousFirstLastmod)
	sampleLastDT := s.parseLastmod(newSampleLastmod)
	hasCompleteSampleBlocks := urlBlockPattern.Find(sample) != nil
	useSampleDelta := previousFirstLastmod != "" &&
		!previousDT.IsZero() &&
		!sampleLastDT.IsZero() &&
		(sampleLastDT.Before(previousDT) || sampleLastDT.Equal(previousDT)) &&
		hasCompleteSampleBlocks

	var fullData []byte
	deltaData := sample
	deltaSource := "full_no_previous_lastmod"
	overlapBytes := 0
	if useSampleDelta {
		deltaData = s.DeltaNewerThanLastmod(sample, previousFirstLastmod)
		deltaSource = "sample_lastmod_window"
		overlapBytes = max(len(sample)-len(deltaData), 0)
		if len(deltaData) == 0 {
			_ = s.saveRemoteRocketshipState(context.Background(), currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))
			s.setStatus(map[string]any{
				"last_change_at":              utcNowISO(),
				"last_overlap_bytes":          overlapBytes,
				"last_delta_size":             0,
				"last_new_sample_lastmod":     emptyToNil(newSampleLastmod),
				"last_previous_first_lastmod": emptyToNil(previousFirstLastmod),
				"last_delta_payload_id":       nil,
			})
			return nil
		}
	} else {
		fullData, err = s.RemoteRocketShipUSJobsSitemapFetchFull()
		if err != nil {
			s.setStatus(map[string]any{"last_check_at": utcNowISO(), "last_error": err.Error()})
			return err
		}
		deltaData = fullData
		if previousFirstLastmod != "" {
			deltaData = s.DeltaNewerThanLastmod(fullData, previousFirstLastmod)
			overlapBytes = max(len(fullData)-len(deltaData), 0)
			deltaSource = "full_lastmod_window"
		}
	}

	if len(fullData) > 0 && currentFirstLastmod == "" {
		currentFirstLastmod = s.ExtractFirstLastmod(fullData)
	}
	_ = s.saveRemoteRocketshipState(context.Background(), currentHash, firstNonEmpty(currentFirstLastmod, previousFirstLastmod))

	var payloadID any
	if len(deltaData) > 0 {
		saved, err := s.saveRemoteRocketshipDeltaPayload(context.Background(), string(deltaData))
		if err != nil {
			return err
		}
		payloadID = saved
	}

	s.setStatus(map[string]any{
		"last_change_at":              utcNowISO(),
		"last_overlap_bytes":          overlapBytes,
		"last_delta_source":           deltaSource,
		"last_delta_size":             len(deltaData),
		"last_new_sample_lastmod":     emptyToNil(newSampleLastmod),
		"last_previous_first_lastmod": emptyToNil(previousFirstLastmod),
		"last_delta_payload_id":       payloadID,
	})
	return nil
}

func (s *Service) runOnceBuiltin() error {
	statePayload, err := s.loadStatePayload(sourceBuiltin)
	if err != nil {
		return err
	}
	nextPage := intFromAny(statePayload["next_page"], s.Config.BuiltinMaxPage)
	if nextPage <= 0 {
		nextPage = s.Config.BuiltinMaxPage
	}
	lastJobURL, _ := statePayload["last_job_url"].(string)
	lastPostDate, _ := statePayload["last_post_date"].(string)
	lastPostDateDT := parseISOTime(lastPostDate)
	currentPage := nextPage
	pagesScanned := 0
	payloadsCreated := 0
	phase1BoundaryMatched := false
	checkpointEveryPages := max(s.Config.BuiltinCheckpointPages, 1)
	log.Printf(
		"Builtin watcher cycle_start next_page=%d last_job_url=%s last_post_date=%s pages_per_cycle=%d",
		nextPage,
		lastJobURL,
		lastPostDate,
		s.Config.BuiltinPagesPerCycle,
	)
	saveCheckpoint := func(nextPageValue int) error {
		nextSavedPage := nextPageValue
		if nextSavedPage < 1 {
			nextSavedPage = 1
		}
		return s.saveStatePayload(sourceBuiltin, map[string]any{
			"next_page":                   nextSavedPage,
			"last_post_date":              valueOrNil(lastPostDate),
			"last_job_url":                valueOrNil(lastJobURL),
			"last_scan_at":                utcNowISO(),
			"pages_scanned_last_cycle":    pagesScanned,
			"payloads_created_last_cycle": payloadsCreated,
		})
	}

	if (lastJobURL != "" || lastPostDateDT != nil) && currentPage < s.Config.BuiltinMaxPage {
		probePage := currentPage + 1
		for probePage <= s.Config.BuiltinMaxPage && pagesScanned < s.Config.BuiltinPagesPerCycle {
			pageURL := strings.ReplaceAll(s.Config.BuiltinBaseURL, "{page}", strconv.Itoa(probePage))
			log.Printf("Builtin phase1 fetch_start page=%d url=%s", probePage, pageURL)
			htmlText := s.fetchBuiltinPageText(pageURL, probePage, "next-page")
			pagesScanned++
			if strings.TrimSpace(htmlText) == "" {
				probePage++
				continue
			}
			if strings.Contains(htmlText, "No job results") {
				pagesScanned = max(pagesScanned-1, 0)
				break
			}
			listings := builtin.ExtractJobListings(htmlText)
			if len(listings) == 0 {
				break
			}
			if _, err := s.saveDeltaPayloadForSource(sourceBuiltin, pageURL, payloadTypeDelta, mustMarshalJSON(listings)); err != nil {
				return err
			}
			payloadsCreated++
			if containsListingURL(listings, lastJobURL) || allListingsOlderThan(listings, lastPostDateDT) {
				phase1BoundaryMatched = true
				break
			}
			if pagesScanned%checkpointEveryPages == 0 {
				if err := saveCheckpoint(probePage); err != nil {
					return err
				}
			}
			probePage++
		}
	}

	skipPhase2UntilBoundary := !phase1BoundaryMatched && (lastJobURL != "" || lastPostDateDT != nil)
	for currentPage >= 1 && pagesScanned < s.Config.BuiltinPagesPerCycle {
		pageURL := strings.ReplaceAll(s.Config.BuiltinBaseURL, "{page}", strconv.Itoa(currentPage))
		log.Printf("Builtin phase2 fetch_start page=%d url=%s", currentPage, pageURL)
		htmlText := s.fetchBuiltinPageText(pageURL, currentPage, "upper-page")
		pagesScanned++
		if strings.TrimSpace(htmlText) == "" {
			currentPage--
			continue
		}
		if strings.Contains(htmlText, "No job results") {
			pagesScanned = max(pagesScanned-1, 0)
			currentPage--
			continue
		}
		listings := builtin.ExtractJobListings(htmlText)
		if skipPhase2UntilBoundary && len(listings) > 0 {
			boundaryHit := containsListingURL(listings, lastJobURL) || allListingsOlderThan(listings, lastPostDateDT)
			if boundaryHit {
				skipPhase2UntilBoundary = false
			}
			currentPage--
			continue
		}
		if len(listings) > 0 {
			if _, err := s.saveDeltaPayloadForSource(sourceBuiltin, pageURL, payloadTypeDelta, mustMarshalJSON(listings)); err != nil {
				return err
			}
			payloadsCreated++
			if firstURL, ok := listings[0]["url"].(string); ok {
				lastJobURL = firstURL
			}
			if firstPostDate, ok := listings[0]["post_date"].(string); ok {
				lastPostDate = firstPostDate
			}
		}
		if pagesScanned%checkpointEveryPages == 0 {
			if err := saveCheckpoint(currentPage); err != nil {
				return err
			}
		}
		currentPage--
	}

	return saveCheckpoint(currentPage)
}

func (s *Service) fetchBuiltinPageText(pageURL string, pageNo int, phase string) string {
	maxRetries := max(s.Config.Builtin429RetryCount, 0)
	backoff := s.Config.Builtin429BackoffSeconds
	if backoff < 0 {
		backoff = 0
	}
	pauseSeconds := s.Config.BuiltinFetchIntervalSeconds
	if pauseSeconds < 0 {
		pauseSeconds = 0
	}
	attempt := 0
	for {
		htmlText, err := s.FetchText(pageURL)
		if pauseSeconds > 0 {
			time.Sleep(time.Duration(pauseSeconds * float64(time.Second)))
		}
		if err == nil {
			return htmlText
		}
		errText := strings.ToLower(err.Error())
		if strings.Contains(errText, "429") && attempt < maxRetries {
			waitSeconds := backoff * math.Pow(2, float64(attempt))
			log.Printf("Builtin %s fetch rate-limited page=%d url=%s attempt=%d/%d wait_seconds=%.1f", phase, pageNo, pageURL, attempt+1, maxRetries+1, waitSeconds)
			if waitSeconds > 0 {
				time.Sleep(time.Duration(waitSeconds * float64(time.Second)))
			}
			attempt++
			continue
		}
		log.Printf("Builtin %s fetch failed page=%d url=%s error=%T: %v", phase, pageNo, pageURL, err, err)
		return ""
	}
}

func (s *Service) runOnceWorkable() error {
	statePayload, err := s.loadStatePayload(sourceWorkable)
	if err != nil {
		return err
	}
	previousFirstJobPostDate, _ := statePayload["first_job_post_date"].(string)
	previousFirstDT := parseISOTime(previousFirstJobPostDate)
	isBootstrap := previousFirstDT == nil

	pagesScanned := 0
	insertedRows := 0
	updatedRows := 0
	var firstPageLatestPostDate *time.Time
	firstPageFirstURL := ""
	nextToken := ""

	log.Printf("Workable watcher cycle_start previous_first_job_post_date=%s page_limit=%d", previousFirstJobPostDate, s.Config.WorkablePageLimit)

	for {
		pageURL := workable.BuildAPIURL(s.Config.WorkableAPIURL, nextToken, max(s.Config.WorkablePageLimit, 1))
		log.Printf("Workable fetch_start token=%s url=%s", valueOrNil(nextToken), pageURL)

		bodyText, err := s.FetchText(pageURL)
		if err != nil {
			return err
		}

		var response map[string]any
		if err := json.Unmarshal([]byte(bodyText), &response); err != nil || response == nil {
			break
		}
		jobsRaw, _ := response["jobs"].([]any)
		if len(jobsRaw) == 0 {
			break
		}

		if pagesScanned == 0 {
			firstItem, _ := jobsRaw[0].(map[string]any)
			if firstItem != nil {
				if urlValue, ok := firstItem["url"].(string); ok {
					urlValue = strings.TrimSpace(urlValue)
					if urlValue != "" {
						firstPageFirstURL = urlValue
					}
				}
				createdDT := parseISOTime(stringValue(firstItem["created"]))
				updatedDT := parseISOTime(stringValue(firstItem["updated"]))
				switch {
				case createdDT != nil && updatedDT != nil:
					if updatedDT.After(*createdDT) {
						firstPageLatestPostDate = updatedDT
					} else {
						firstPageLatestPostDate = createdDT
					}
				case createdDT != nil:
					firstPageLatestPostDate = createdDT
				case updatedDT != nil:
					firstPageLatestPostDate = updatedDT
				}
			}
		}

		rows, _ := workable.NormalizeJobs(bodyText)
		if len(rows) == 0 {
			break
		}
		log.Printf("Workable fetch_done jobs=%d token=%s", len(rows), valueOrNil(nextToken))

		toUpsert := rows
		if !isBootstrap && previousFirstDT != nil {
			urls := make([]string, 0, len(rows))
			for _, row := range rows {
				if rowURL, _, ok := extractRowURLAndPostDate(row); ok {
					urls = append(urls, rowURL)
				}
			}
			existingURLs, err := s.findExistingSourceURLs(sourceWorkable, urls)
			if err != nil {
				return err
			}
			filtered := make([]map[string]any, 0, len(rows))
			for _, row := range rows {
				rowURL, rowDT, ok := extractRowURLAndPostDate(row)
				if !ok {
					continue
				}
				_, seen := existingURLs[rowURL]
				isNewer := !rowDT.IsZero() && rowDT.After(*previousFirstDT)
				if !seen || isNewer {
					filtered = append(filtered, row)
				}
			}
			toUpsert = filtered
		}

		if len(toUpsert) > 0 {
			inserted, updated, err := s.upsertWorkableJobs(toUpsert)
			if err != nil {
				return err
			}
			insertedRows += inserted
			updatedRows += updated
		}

		pagesScanned++
		nextValue, _ := response["nextPageToken"].(string)
		nextToken = strings.TrimSpace(nextValue)
		if nextToken == "" {
			break
		}
	}

	firstJobPostDate := valueOrNil(previousFirstJobPostDate)
	if firstPageLatestPostDate != nil {
		firstJobPostDate = firstPageLatestPostDate.UTC().Format(time.RFC3339Nano)
	}
	return s.saveStatePayload(sourceWorkable, map[string]any{
		"first_job_post_date": firstJobPostDate,
		"first_job_url":       valueOrNil(firstPageFirstURL),
	})
}

func (s *Service) runOnceRemotive() error {
	if !s.isRemotiveConfigured() {
		return nil
	}
	statePayload, err := s.loadStatePayload(sourceRemotive)
	if err != nil {
		return err
	}
	previousLatestJobID := intFromAny(statePayload["latest_job_id"], 0)
	if previousLatestJobID <= 0 {
		previousLatestJobID = 0
	}

	latestIndex, latestURL, xmlText := s.fetchRemotiveLatestSitemapXML()
	if strings.TrimSpace(xmlText) == "" || strings.TrimSpace(latestURL) == "" {
		return s.saveStatePayload(sourceRemotive, map[string]any{
			"latest_job_id": previousLatestJobID,
			"last_scan_at":  utcNowISO(),
		})
	}

	now := time.Now().UTC()
	deltaRows := make([]map[string]any, 0)
	seenURLs := map[string]struct{}{}
	scannedRowsCount := 0
	newLatestJobID := previousLatestJobID
	scannedIndexes := make([]int, 0)

	hasLatestIndex := latestIndex > 0
	partitionsToScan := []int{}
	if hasLatestIndex && previousLatestJobID > 0 {
		for partition := latestIndex; partition >= s.Config.RemotiveSitemapMinIndex; partition-- {
			partitionsToScan = append(partitionsToScan, partition)
		}
	} else if hasLatestIndex {
		partitionsToScan = append(partitionsToScan, latestIndex)
	}

	processRows := func(rows []map[string]any) (crossedPrevious bool) {
		if len(rows) == 0 {
			return false
		}
		lastURL, _, ok := extractRowURLAndPostDate(rows[len(rows)-1])
		if !ok {
			return false
		}
		partitionMaxJobID := extractRemotiveJobIDFromURL(lastURL)
		if partitionMaxJobID > newLatestJobID {
			newLatestJobID = partitionMaxJobID
		}
		for idx := len(rows) - 1; idx >= 0; idx-- {
			rowURL, postDate, ok := extractRowURLAndPostDate(rows[idx])
			if !ok {
				continue
			}
			jobID := extractRemotiveJobIDFromURL(rowURL)
			if previousLatestJobID > 0 && jobID > 0 && jobID <= previousLatestJobID {
				return true
			}
			scannedRowsCount++
			if _, exists := seenURLs[rowURL]; exists {
				continue
			}
			seenURLs[rowURL] = struct{}{}
			if postDate.IsZero() {
				postDate = now
			}
			deltaRows = append(deltaRows, map[string]any{
				"url":       rowURL,
				"post_date": postDate,
			})
		}
		return false
	}

	if len(partitionsToScan) == 0 {
		rows, _ := remotive.ParseSitemapRows(xmlText)
		_ = processRows(rows)
	} else {
		for _, partition := range partitionsToScan {
			var partitionXML string
			if partition == latestIndex {
				partitionXML = xmlText
			} else {
				fetchedPartition, fetchedURL, fetchedXML := s.fetchRemotiveSitemapXMLByPartition(partition)
				if fetchedPartition <= 0 || strings.TrimSpace(fetchedURL) == "" || strings.TrimSpace(fetchedXML) == "" {
					continue
				}
				partitionXML = fetchedXML
			}
			rows, _ := remotive.ParseSitemapRows(partitionXML)
			if len(rows) == 0 {
				continue
			}
			scannedIndexes = append(scannedIndexes, partition)
			if processRows(rows) {
				break
			}
		}
	}

	var payloadID any
	if len(deltaRows) > 0 {
		if savedID, err := s.saveDeltaPayloadForSource(sourceRemotive, latestURL, remotive.PayloadType, remotive.SerializeImportRows(deltaRows)); err != nil {
			return err
		} else {
			payloadID = savedID
		}
	}

	latestJobIDValue := any(nil)
	switch {
	case newLatestJobID > 0:
		latestJobIDValue = newLatestJobID
	case previousLatestJobID > 0:
		latestJobIDValue = previousLatestJobID
	}

	sitemapURLCount := scannedRowsCount
	if sitemapURLCount == 0 {
		sitemapURLCount = len(deltaRows)
	}

	return s.saveStatePayload(sourceRemotive, map[string]any{
		"sitemap_url":                    latestURL,
		"latest_sitemap_index":           latestIndex,
		"latest_sitemap_url":             latestURL,
		"last_scan_at":                   utcNowISO(),
		"rows_seen_last_cycle":           len(deltaRows),
		"rows_skipped_last_cycle":        0,
		"payloads_created_last_cycle":    map[bool]int{true: 1, false: 0}[len(deltaRows) > 0],
		"latest_job_id":                  latestJobIDValue,
		"latest_sitemap_url_count":       sitemapURLCount,
		"latest_delta_count":             len(deltaRows),
		"latest_delta_payload_id":        payloadID,
		"latest_scanned_sitemap_indexes": scannedIndexes,
	})
}

func (s *Service) isRemotiveConfigured() bool {
	if !strings.Contains(s.Config.RemotiveSitemapURLTemplate, "{partition}") {
		return false
	}
	if s.Config.RemotiveSitemapMaxIndex <= 0 || s.Config.RemotiveSitemapMinIndex <= 0 {
		return false
	}
	return s.Config.RemotiveSitemapMaxIndex >= s.Config.RemotiveSitemapMinIndex
}

func (s *Service) fetchRemotiveLatestSitemapXML() (int, string, string) {
	for partition := s.Config.RemotiveSitemapMaxIndex; partition >= s.Config.RemotiveSitemapMinIndex; partition-- {
		fetchedPartition, sitemapURL, xmlText := s.fetchRemotiveSitemapXMLByPartition(partition)
		if fetchedPartition > 0 && strings.TrimSpace(sitemapURL) != "" && strings.TrimSpace(xmlText) != "" {
			return fetchedPartition, sitemapURL, xmlText
		}
	}
	return 0, "", ""
}

func (s *Service) fetchRemotiveSitemapXMLByPartition(partition int) (int, string, string) {
	sitemapURL := buildRemotiveSitemapURL(s.Config.RemotiveSitemapURLTemplate, partition)
	if strings.TrimSpace(sitemapURL) == "" {
		return 0, sitemapURL, ""
	}
	xmlText, err := s.FetchText(sitemapURL)
	if err != nil {
		return 0, sitemapURL, ""
	}
	if !strings.Contains(strings.ToLower(xmlText), "<urlset") {
		return 0, sitemapURL, ""
	}
	return partition, sitemapURL, xmlText
}

func extractRemotiveJobIDFromURL(rawURL string) int {
	match := remotiveJobIDRE.FindStringSubmatch(strings.TrimSpace(rawURL))
	if len(match) < 2 {
		return 0
	}
	id, _ := strconv.Atoi(strings.TrimSpace(match[1]))
	return id
}

func extractRemotiveSitemapIndex(rawURL string) (int, bool) {
	match := remotiveIndexRE.FindStringSubmatch(strings.TrimSpace(rawURL))
	if len(match) < 2 {
		return 0, false
	}
	index, err := strconv.Atoi(strings.TrimSpace(match[1]))
	if err != nil || index <= 0 {
		return 0, false
	}
	return index, true
}

func buildRemotiveSitemapURL(currentURL string, partition int) string {
	if partition <= 0 || strings.TrimSpace(currentURL) == "" {
		return ""
	}
	return strings.ReplaceAll(currentURL, "{partition}", strconv.Itoa(partition))
}

func (s *Service) runOnceHiringCafe() error {
	statePayload, err := s.loadStatePayload(sourceHiringCafe)
	if err != nil {
		return err
	}
	previousFirstJobPostDate, _ := statePayload["first_job_post_date"].(string)
	previousFirstJobURL, _ := statePayload["first_job_url"].(string)
	previousFirstDT := parseISOTime(previousFirstJobPostDate)

	totalCountPayload, err := s.fetchJSON(s.Config.HiringCafeTotalCountURL)
	if err != nil {
		return err
	}
	totalCount := hiringcafe.ParseTotalCount(totalCountPayload)
	if totalCount <= 0 {
		return s.saveStatePayload(sourceHiringCafe, map[string]any{
			"search_api_url":           s.Config.HiringCafeSearchAPIURL,
			"total_count_url":          s.Config.HiringCafeTotalCountURL,
			"first_job_post_date":      valueOrNil(previousFirstJobPostDate),
			"first_job_url":            valueOrNil(previousFirstJobURL),
			"last_scan_at":             utcNowISO(),
			"total_count_last_run":     0,
			"pages_scanned_last_cycle": 0,
			"rows_saved_last_cycle":    0,
		})
	}
	if s.Config.HiringCafePageSize < 1 {
		s.Config.HiringCafePageSize = 1
	}
	totalPages := (totalCount + s.Config.HiringCafePageSize - 1) / s.Config.HiringCafePageSize
	pagesScanned := 0
	rowsSaved := 0
	firstPageLatestPostDate := previousFirstDT
	firstPageFirstURL := previousFirstJobURL
	isBootstrap := previousFirstDT == nil

	for page := 0; page < totalPages; page++ {
		pageURL := hiringcafe.BuildSearchAPIURL(s.Config.HiringCafeSearchAPIURL, page, s.Config.HiringCafePageSize)
		response, err := s.fetchJSON(pageURL)
		if err != nil {
			return err
		}
		pagesScanned++
		rowsRaw, _ := response["results"].([]any)
		results := make([]map[string]any, 0, len(rowsRaw))
		for _, row := range rowsRaw {
			item, _ := row.(map[string]any)
			if item != nil {
				results = append(results, item)
			}
		}
		rows := hiringcafe.NormalizeJobs(results)
		if len(rows) == 0 {
			continue
		}
		if page == 0 {
			firstPageLatestPostDate = &rows[0].PostDate
			firstPageFirstURL = rows[0].URL
		}

		toUpsert := rows
		if !isBootstrap && previousFirstDT != nil {
			urls := make([]string, 0, len(rows))
			for _, row := range rows {
				urls = append(urls, row.URL)
			}
			existingURLs, err := s.findExistingSourceURLs(sourceHiringCafe, urls)
			if err != nil {
				return err
			}
			toUpsert = make([]hiringcafe.NormalizedJob, 0, len(rows))
			for _, row := range rows {
				_, seen := existingURLs[row.URL]
				if row.PostDate.After(*previousFirstDT) || !seen {
					toUpsert = append(toUpsert, row)
				}
			}
		}
		if len(toUpsert) == 0 {
			continue
		}
		inserted, updated, err := s.upsertHiringCafeJobs(toUpsert)
		if err != nil {
			return err
		}
		rowsSaved += inserted + updated
	}

	var firstPageLatestPostDateISO any
	if firstPageLatestPostDate != nil {
		firstPageLatestPostDateISO = firstPageLatestPostDate.UTC().Format(time.RFC3339Nano)
	}
	return s.saveStatePayload(sourceHiringCafe, map[string]any{
		"search_api_url":           s.Config.HiringCafeSearchAPIURL,
		"total_count_url":          s.Config.HiringCafeTotalCountURL,
		"first_job_post_date":      firstPageLatestPostDateISO,
		"first_job_url":            valueOrNil(firstPageFirstURL),
		"last_scan_at":             utcNowISO(),
		"total_count_last_run":     totalCount,
		"pages_scanned_last_cycle": pagesScanned,
		"rows_saved_last_cycle":    rowsSaved,
	})
}

func (s *Service) loadRemoteRocketshipState(ctx context.Context) (string, string, error) {
	if s.DB == nil {
		return "", "", nil
	}
	var sampleHash, firstLastmod string
	var stateJSON sql.NullString
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`SELECT COALESCE(state_json::text, '')
		 FROM watcher_states
		 WHERE source = ?
		 ORDER BY updated_at DESC, id DESC
		 LIMIT 1`,
		sourceRemoterocketship,
	).Scan(&stateJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	if stateJSON.Valid && strings.TrimSpace(stateJSON.String) != "" {
		payload := map[string]any{}
		if err := json.Unmarshal([]byte(stateJSON.String), &payload); err == nil {
			sampleHash, _ = payload["sample_hash"].(string)
			firstLastmod, _ = payload["first_lastmod"].(string)
		}
	}
	return sampleHash, firstLastmod, nil
}

func (s *Service) saveRemoteRocketshipState(ctx context.Context, sampleHash, firstLastmod string) error {
	if s.DB == nil {
		return nil
	}
	stateJSON := mustMarshalJSON(map[string]any{
		"source_url":    s.Config.RemoteRocketshipUSJobSitemapURL,
		"sample_hash":   sampleHash,
		"first_lastmod": emptyToNil(firstLastmod),
	})
	updatedAt := utcNowISO()
	updateResult, err := s.DB.SQL.ExecContext(
		ctx,
		`UPDATE watcher_states
		 SET state_json = ?, updated_at = ?
		 WHERE source = ?`,
		stateJSON,
		updatedAt,
		sourceRemoterocketship,
	)
	if err != nil {
		return err
	}
	affected, err := updateResult.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	_, err = s.DB.SQL.ExecContext(
		ctx,
		`INSERT INTO watcher_states (source, state_json, updated_at)
		 VALUES (?, ?, ?)`,
		sourceRemoterocketship,
		stateJSON,
		updatedAt,
	)
	return err
}

func (s *Service) inferFileExtension() string {
	parsed, err := url.Parse(s.Config.RemoteRocketshipUSJobSitemapURL)
	if err != nil {
		return ".xml"
	}
	ext := strings.ToLower(parsed.Path)
	if ext == "" {
		return ".xml"
	}
	return ext
}

func (s *Service) saveRemoteRocketshipDeltaPayload(ctx context.Context, bodyText string) (int64, error) {
	if s.DB == nil {
		return 0, nil
	}
	var payloadID int64
	err := s.DB.SQL.QueryRowContext(
		ctx,
		`INSERT INTO watcher_payloads (source, source_url, payload_type, body_text, created_at)
		 VALUES (?, ?, ?, ?, ?)
		 RETURNING id`,
		sourceRemoterocketship,
		s.Config.RemoteRocketshipUSJobSitemapURL,
		payloadTypeXML,
		bodyText,
		utcNowISO(),
	).Scan(&payloadID)
	if err != nil {
		return 0, err
	}
	return payloadID, nil
}

func (s *Service) saveDeltaPayloadForSource(source, sourceURL, payloadType, bodyText string) (int64, error) {
	if s.DB == nil {
		return 0, nil
	}
	var existingID sql.NullInt64
	var existingBody sql.NullString
	err := s.DB.SQL.QueryRowContext(
		context.Background(),
		`SELECT id, COALESCE(body_text, '')
		 FROM watcher_payloads
		 WHERE source = ? AND source_url = ? AND payload_type = ? AND consumed_at IS NULL
		 ORDER BY id DESC
		 LIMIT 1`,
		source,
		sourceURL,
		payloadType,
	).Scan(&existingID, &existingBody)
	if err == nil && existingID.Valid && existingBody.Valid && existingBody.String == bodyText {
		return existingID.Int64, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	var payloadID int64
	err = s.DB.SQL.QueryRowContext(
		context.Background(),
		`INSERT INTO watcher_payloads (source, source_url, payload_type, body_text, created_at)
		 VALUES (?, ?, ?, ?, ?)
		 RETURNING id`,
		source,
		sourceURL,
		payloadType,
		bodyText,
		utcNowISO(),
	).Scan(&payloadID)
	if err != nil {
		return 0, err
	}
	return payloadID, nil
}

func (s *Service) loadStatePayload(source string) (map[string]any, error) {
	var stateJSON sql.NullString
	err := s.DB.SQL.QueryRowContext(context.Background(), `SELECT COALESCE(state_json::text, '') FROM watcher_states WHERE source = ? ORDER BY updated_at DESC, id DESC LIMIT 1`, source).Scan(&stateJSON)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	payload := map[string]any{}
	if stateJSON.Valid && strings.TrimSpace(stateJSON.String) != "" {
		_ = json.Unmarshal([]byte(stateJSON.String), &payload)
	}
	return payload, nil
}

func (s *Service) saveStatePayload(source string, payload map[string]any) error {
	ctx := context.Background()
	stateJSON := mustMarshalJSON(payload)
	updatedAt := utcNowISO()
	updateResult, err := s.DB.SQL.ExecContext(ctx,
		`UPDATE watcher_states
		 SET state_json = ?, updated_at = ?
		 WHERE source = ?`,
		stateJSON,
		updatedAt,
		source,
	)
	if err != nil {
		return err
	}
	affected, err := updateResult.RowsAffected()
	if err != nil {
		return err
	}
	if affected > 0 {
		return nil
	}
	_, err = s.DB.SQL.ExecContext(ctx,
		`INSERT INTO watcher_states (source, state_json, updated_at)
		 VALUES (?, ?, ?)`,
		source,
		stateJSON,
		updatedAt,
	)
	return err
}

func (s *Service) findExistingSourceURLs(source string, urls []string) (map[string]struct{}, error) {
	if len(urls) == 0 {
		return map[string]struct{}{}, nil
	}
	rows, err := s.DB.SQL.QueryContext(
		context.Background(),
		`SELECT url
		   FROM raw_us_jobs
		  WHERE source = ?
		    AND url = ANY(?::text[])`,
		source,
		urls,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]struct{}{}
	for rows.Next() {
		var rowURL string
		if err := rows.Scan(&rowURL); err != nil {
			return nil, err
		}
		out[rowURL] = struct{}{}
	}
	return out, rows.Err()
}

func (s *Service) upsertWorkableJobs(rows []map[string]any) (int, int, error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}
	tx, err := s.DB.SQL.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	inserted := 0
	updated := 0
	for _, row := range rows {
		rowURL, postDate, rawPayload, ok := extractWorkableRow(row)
		if !ok {
			continue
		}

		var existingID int64
		var existingSource string
		var existingPostDateRaw string
		var existingRawJSON sql.NullString
		err := tx.QueryRow(`SELECT id, source, post_date, raw_json FROM raw_us_jobs WHERE url = ? LIMIT 1`, rowURL).Scan(&existingID, &existingSource, &existingPostDateRaw, &existingRawJSON)
		rawPayloadText := mustMarshalJSON(rawPayload)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, execErr := tx.Exec(
					`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json)
					 VALUES (?, ?, ?, true, false, false, 0, ?)`,
					sourceWorkable,
					rowURL,
					postDate.UTC().Format(time.RFC3339Nano),
					rawPayloadText,
				); execErr != nil {
					return 0, 0, execErr
				}
				inserted++
				continue
			}
			return 0, 0, err
		}

		if existingSource != sourceWorkable {
			continue
		}
		existingPostDate := parseISOTime(existingPostDateRaw)
		needsUpdate := existingPostDate == nil || postDate.After(*existingPostDate) || !existingRawJSON.Valid || strings.TrimSpace(existingRawJSON.String) == ""
		if !needsUpdate {
			continue
		}
		if _, execErr := tx.Exec(
			`UPDATE raw_us_jobs
			 SET post_date = ?, is_ready = true, is_skippable = false, is_parsed = false, retry_count = 0, raw_json = ?
			 WHERE id = ?`,
			postDate.UTC().Format(time.RFC3339Nano),
			rawPayloadText,
			existingID,
		); execErr != nil {
			return 0, 0, execErr
		}
		updated++
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return inserted, updated, nil
}

func (s *Service) fetchJSON(rawURL string) (map[string]any, error) {
	text, err := s.FetchText(rawURL)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return map[string]any{}, nil
	}
	return payload, nil
}

func (s *Service) upsertHiringCafeJobs(jobs []hiringcafe.NormalizedJob) (int, int, error) {
	if len(jobs) == 0 {
		return 0, 0, nil
	}
	tx, err := s.DB.SQL.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	inserted := 0
	updated := 0
	for _, row := range jobs {
		var existingID int64
		var existingPostDateRaw string
		err := tx.QueryRow(`SELECT id, post_date FROM raw_us_jobs WHERE source = ? AND url = ? LIMIT 1`, sourceHiringCafe, row.URL).Scan(&existingID, &existingPostDateRaw)
		payloadRaw, _ := json.Marshal(row.RawPayload)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if _, execErr := tx.Exec(`INSERT INTO raw_us_jobs (source, url, post_date, is_ready, is_skippable, is_parsed, retry_count, raw_json) VALUES (?, ?, ?, true, false, false, 0, ?)`,
					sourceHiringCafe, row.URL, row.PostDate.UTC().Format(time.RFC3339Nano), string(payloadRaw)); execErr != nil {
					return 0, 0, execErr
				}
				inserted++
				continue
			}
			return 0, 0, err
		}
		existingPostDate := parseISOTime(existingPostDateRaw)
		if existingPostDate != nil && !row.PostDate.After(*existingPostDate) {
			continue
		}
		if _, execErr := tx.Exec(`UPDATE raw_us_jobs SET post_date = ?, is_ready = true, is_skippable = false, is_parsed = false, retry_count = 0, raw_json = ? WHERE id = ?`,
			row.PostDate.UTC().Format(time.RFC3339Nano), string(payloadRaw), existingID); execErr != nil {
			return 0, 0, execErr
		}
		updated++
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return inserted, updated, nil
}

func mustMarshalJSON(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func parseISOTime(value string) *time.Time {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse("2006-01-02T15:04:05", value); err == nil {
		utc := parsed.UTC()
		return &utc
	}
	return nil
}

func intFromAny(value any, fallback int) int {
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	default:
		return fallback
	}
}

func containsListingURL(listings []map[string]any, targetURL string) bool {
	if strings.TrimSpace(targetURL) == "" {
		return false
	}
	for _, listing := range listings {
		if urlValue, _ := listing["url"].(string); urlValue == targetURL {
			return true
		}
	}
	return false
}

func extractRowURLAndPostDate(row map[string]any) (string, time.Time, bool) {
	if row == nil {
		return "", time.Time{}, false
	}
	rowURL, ok := row["url"].(string)
	if !ok {
		return "", time.Time{}, false
	}
	rowURL = strings.TrimSpace(rowURL)
	if rowURL == "" {
		return "", time.Time{}, false
	}
	postDate, ok := row["post_date"].(time.Time)
	if !ok {
		return "", time.Time{}, false
	}
	return rowURL, postDate, true
}

func extractWorkableRow(row map[string]any) (string, time.Time, map[string]any, bool) {
	rowURL, postDate, ok := extractRowURLAndPostDate(row)
	if !ok || postDate.IsZero() {
		return "", time.Time{}, nil, false
	}
	rawPayload, ok := row["raw_payload"].(map[string]any)
	if !ok || rawPayload == nil {
		return "", time.Time{}, nil, false
	}
	return rowURL, postDate, rawPayload, true
}

func allListingsOlderThan(listings []map[string]any, marker *time.Time) bool {
	if marker == nil {
		return false
	}
	foundAny := false
	for _, listing := range listings {
		postDate, _ := listing["post_date"].(string)
		listingDT := parseISOTime(postDate)
		if listingDT == nil {
			continue
		}
		foundAny = true
		if !listingDT.Before(*marker) {
			return false
		}
	}
	return foundAny
}

func valueOrNil(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func (s *Service) ExtractFirstLastmod(data []byte) string {
	match := lastmodPattern.FindSubmatch(data)
	if len(match) < 2 {
		return ""
	}
	return strings.TrimSpace(string(match[1]))
}

func (s *Service) ExtractLastLastmod(data []byte) string {
	matches := lastmodPattern.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return ""
	}
	return strings.TrimSpace(string(matches[len(matches)-1][1]))
}

func (s *Service) parseLastmod(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	return time.Time{}
}

func (s *Service) DeltaNewerThanLastmod(fullData []byte, previousFirstLastmod string) []byte {
	previousDT := s.parseLastmod(previousFirstLastmod)
	if previousDT.IsZero() {
		return fullData
	}

	blocks := make([][]byte, 0)
	for _, match := range urlBlockPattern.FindAll(fullData, -1) {
		blockLastmod := s.ExtractFirstLastmod(match)
		blockDT := s.parseLastmod(blockLastmod)
		if blockDT.IsZero() {
			continue
		}
		if blockDT.After(previousDT) {
			blocks = append(blocks, bytes.Clone(match))
		} else {
			break
		}
	}
	if len(blocks) == 0 {
		return []byte{}
	}

	firstURL := urlOpenPattern.FindIndex(fullData)
	if firstURL == nil {
		return bytes.Join(blocks, nil)
	}
	suffix := []byte{}
	if match := urlSetClosePattern.Find(fullData); len(match) > 0 {
		suffix = match
	}

	output := make([]byte, 0, len(fullData))
	output = append(output, fullData[:firstURL[0]]...)
	output = append(output, bytes.Join(blocks, nil)...)
	output = append(output, suffix...)
	return output
}

func utcNowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func emptyToNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (s *Service) isSourceEnabled(source string) bool {
	if len(s.Config.EnabledSources) == 0 {
		return false
	}
	_, ok := s.Config.EnabledSources[source]
	return ok
}

func sortedSourceNames(values map[string]struct{}) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
