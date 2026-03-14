package plugins

import (
	"goapplyjob-golang-backend/internal/sources/builtin"
	"goapplyjob-golang-backend/internal/sources/dailyremote"
	"goapplyjob-golang-backend/internal/sources/hiringcafe"
	"goapplyjob-golang-backend/internal/sources/remotedotco"
	"goapplyjob-golang-backend/internal/sources/remoterocketship"
	"goapplyjob-golang-backend/internal/sources/remotive"
	"goapplyjob-golang-backend/internal/sources/workable"
)

type SourcePlugin struct {
	Source               string
	PayloadType          string
	ToTargetJobURL       func(string) string
	ParseRawHTML         func(string, string) map[string]any
	ParseImportRows      func(string) ([]map[string]any, int)
	SerializeImportRows  func([]map[string]any) string
	UseExternalCompanyID bool
	UseCompanyMatchKeys  bool
	RunDuplicateCheck    bool
	InferCategories      bool
}

var registry = map[string]SourcePlugin{
	remoterocketship.Source: {
		Source:               remoterocketship.Source,
		PayloadType:          remoterocketship.PayloadType,
		ToTargetJobURL:       remoterocketship.ToTargetJobURL,
		ParseRawHTML:         remoterocketship.ParseRawHTML,
		ParseImportRows:      remoterocketship.ParseImportRows,
		SerializeImportRows:  remoterocketship.SerializeImportRows,
		UseExternalCompanyID: true,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      false,
	},
	builtin.Source: {
		Source:      builtin.Source,
		PayloadType: builtin.PayloadType,
		ToTargetJobURL: func(rawURL string) string {
			return rawURL
		},
		ParseRawHTML:         builtin.ExtractJobFromHTML,
		ParseImportRows:      builtin.ParseImportRows,
		SerializeImportRows:  builtin.SerializeImportRows,
		UseExternalCompanyID: false,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      true,
	},
	workable.Source: {
		Source:      workable.Source,
		PayloadType: "",
		ToTargetJobURL: func(rawURL string) string {
			return rawURL
		},
		ParseRawHTML: func(_ string, _ string) map[string]any {
			return map[string]any{}
		},
		ParseImportRows:      nil,
		SerializeImportRows:  nil,
		UseExternalCompanyID: false,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      true,
	},
	hiringcafe.Source: {
		Source:      hiringcafe.Source,
		PayloadType: "",
		ToTargetJobURL: func(rawURL string) string {
			return rawURL
		},
		ParseRawHTML: func(_ string, _ string) map[string]any {
			return map[string]any{}
		},
		ParseImportRows:      nil,
		SerializeImportRows:  nil,
		UseExternalCompanyID: false,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      true,
	},
	remotive.Source: {
		Source:               remotive.Source,
		PayloadType:          remotive.PayloadType,
		ToTargetJobURL:       remotive.ToTargetJobURL,
		ParseRawHTML:         remotive.ParseRawHTML,
		ParseImportRows:      remotive.ParseImportRows,
		SerializeImportRows:  remotive.SerializeImportRows,
		UseExternalCompanyID: false,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      true,
	},
	dailyremote.Source: {
		Source:      dailyremote.Source,
		PayloadType: dailyremote.PayloadType,
		ToTargetJobURL: func(rawURL string) string {
			return rawURL
		},
		ParseRawHTML:         dailyremote.ParseRawHTML,
		ParseImportRows:      dailyremote.ParseImportRows,
		SerializeImportRows:  dailyremote.SerializeImportRows,
		UseExternalCompanyID: false,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      true,
	},
	remotedotco.Source: {
		Source:               remotedotco.Source,
		PayloadType:          remotedotco.PayloadType,
		ToTargetJobURL:       remotedotco.ToTargetJobURL,
		ParseRawHTML:         remotedotco.ParseRawHTML,
		ParseImportRows:      remotedotco.ParseImportRows,
		SerializeImportRows:  remotedotco.SerializeImportRows,
		UseExternalCompanyID: true,
		UseCompanyMatchKeys:  true,
		RunDuplicateCheck:    true,
		InferCategories:      true,
	},
}

func Get(source string) (SourcePlugin, bool) {
	plugin, ok := registry[source]
	return plugin, ok
}

func List() []string {
	out := make([]string, 0, len(registry))
	for key := range registry {
		out = append(out, key)
	}
	return out
}

func valueString(value any) string {
	text, _ := value.(string)
	return text
}
