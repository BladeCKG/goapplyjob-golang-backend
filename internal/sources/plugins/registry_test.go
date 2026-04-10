package plugins

import (
	"goapplyjob-golang-backend/internal/sources/parseerr"
	"testing"
)

func TestRegistryIncludesWorkable(t *testing.T) {
	plugin, ok := Get("workable")
	if !ok || plugin.Source != "workable" {
		t.Fatalf("expected workable plugin, got %#v ok=%v", plugin, ok)
	}
}

func TestRegistryIncludesHiringCafe(t *testing.T) {
	plugin, ok := Get("hiringcafe")
	if !ok || plugin.Source != "hiringcafe" {
		t.Fatalf("expected hiringcafe plugin, got %#v ok=%v", plugin, ok)
	}
}

func TestRegistryIncludesRemoteDotCo(t *testing.T) {
	plugin, ok := Get("remotedotco")
	if !ok || plugin.Source != "remotedotco" {
		t.Fatalf("expected remotedotco plugin, got %#v ok=%v", plugin, ok)
	}
}

func TestPluginParityWithPageExtractForWorkableAndHiringCafe(t *testing.T) {
	workablePlugin, ok := Get("workable")
	if !ok {
		t.Fatal("missing workable plugin")
	}
	if workablePlugin.PayloadType != "" || workablePlugin.ParseImportRows != nil || workablePlugin.SerializeImportRows != nil {
		t.Fatalf("workable plugin should not define import payload handlers, got payloadType=%q", workablePlugin.PayloadType)
	}
	if payload, err := workablePlugin.ParseRawHTML("<html></html>", "https://example.com/job"); err == nil || payload != nil || parseerr.Reason(err) != "unsupported_raw_html" {
		t.Fatalf("workable parse_raw_html should return unsupported_raw_html, got payload=%#v err=%v", payload, err)
	}

	hiringCafePlugin, ok := Get("hiringcafe")
	if !ok {
		t.Fatal("missing hiringcafe plugin")
	}
	if hiringCafePlugin.PayloadType != "" || hiringCafePlugin.ParseImportRows != nil || hiringCafePlugin.SerializeImportRows != nil {
		t.Fatalf("hiringcafe plugin should not define import payload handlers, got payloadType=%q", hiringCafePlugin.PayloadType)
	}
	if payload, err := hiringCafePlugin.ParseRawHTML("<html></html>", "https://example.com/job"); err == nil || payload != nil || parseerr.Reason(err) != "unsupported_raw_html" {
		t.Fatalf("hiringcafe parse_raw_html should return unsupported_raw_html, got payload=%#v err=%v", payload, err)
	}
}

func TestDailyRemotePluginUsesIdentityURLNormalizer(t *testing.T) {
	plugin, ok := Get("dailyremote")
	if !ok {
		t.Fatal("missing dailyremote plugin")
	}
	rawURL := "https://dailyremote.com/remote-job/example-123?x=1#y"
	if got := plugin.ToTargetJobURL(rawURL); got != rawURL {
		t.Fatalf("dailyremote to_target_job_url should be identity, got %q", got)
	}
}
