package hiringcafe

import "testing"

func TestNormalizeJobsDetectsSeniorFromSrWithPeriodInTitle(t *testing.T) {
	results := []map[string]any{
		{
			"id":            float64(1001),
			"requisition_id": "abc123",
			"updated_at":     "2026-02-18T03:21:08.178Z",
			"created_at":     "2026-02-18T03:21:08.178Z",
			"apply_url":      "https://example.com/apply/abc123",
			"v5_processed_job_data": map[string]any{
				"job_title_raw":          "Sr., Platform Engineer",
				"estimated_publish_date": "2026-02-18T03:21:08.178Z",
				"seniority_level":        "",
			},
		},
	}
	normalized := NormalizeJobs(results)
	if len(normalized) != 1 {
		t.Fatalf("expected one normalized row, got %d", len(normalized))
	}
	payload := normalized[0].RawPayload
	if payload["isSenior"] != true || payload["isMidLevel"] != false {
		t.Fatalf("unexpected seniority flags %#v", payload)
	}
	if payload["location"] != nil {
		t.Fatalf("expected nil location when country is not provided, got %#v", payload["location"])
	}
	locationCountries, _ := payload["locationCountries"].([]string)
	if len(locationCountries) != 0 {
		t.Fatalf("expected empty locationCountries, got %#v", payload["locationCountries"])
	}
}
