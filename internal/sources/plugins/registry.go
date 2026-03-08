package plugins

import (
	"time"

	"goapplyjob-golang-backend/internal/sources/builtin"
	"goapplyjob-golang-backend/internal/sources/hiringcafe"
	"goapplyjob-golang-backend/internal/sources/remotive"
	"goapplyjob-golang-backend/internal/sources/remoterocketship"
	"goapplyjob-golang-backend/internal/sources/workable"
)

type SourcePlugin struct {
	Source              string
	PayloadType         string
	ToTargetJobURL      func(string) string
	ParseRawHTML        func(string, string) map[string]any
	ParseImportRows     func(string) ([]map[string]any, int)
	SerializeImportRows func([]map[string]any) string
}

var registry = map[string]SourcePlugin{
	remoterocketship.Source: {
		Source:              remoterocketship.Source,
		PayloadType:         remoterocketship.PayloadType,
		ToTargetJobURL:      remoterocketship.ToTargetJobURL,
		ParseRawHTML:        remoterocketship.ParseRawHTML,
		ParseImportRows:     remoterocketship.ParseImportRows,
		SerializeImportRows: remoterocketship.SerializeImportRows,
	},
	builtin.Source: {
		Source:      builtin.Source,
		PayloadType: builtin.PayloadType,
		ToTargetJobURL: func(rawURL string) string {
			return rawURL
		},
		ParseRawHTML: func(htmlText, sourceURL string) map[string]any {
			return builtin.ExtractJobFromHTML(htmlText, sourceURL)
		},
		ParseImportRows: func(bodyText string) ([]map[string]any, int) {
			rows, skipped := builtin.ParseImportRows(bodyText)
			out := make([]map[string]any, 0, len(rows))
			for _, row := range rows {
				out = append(out, map[string]any{"url": row.URL, "post_date": row.PostDate})
			}
			return out, skipped
		},
		SerializeImportRows: func(rows []map[string]any) string {
			items := make([]builtin.ImportRow, 0, len(rows))
			for _, row := range rows {
				postDate, _ := row["post_date"].(time.Time)
				items = append(items, builtin.ImportRow{URL: valueString(row["url"]), PostDate: postDate})
			}
			return builtin.SerializeImportRows(items)
		},
	},
	workable.Source: {
		Source:              workable.Source,
		PayloadType:         workable.PayloadType,
		ToTargetJobURL:      workable.ToTargetJobURL,
		ParseRawHTML:        workable.ParseRawHTML,
		ParseImportRows:     workable.ParseImportRows,
		SerializeImportRows: workable.SerializeImportRows,
	},
	hiringcafe.Source: {
		Source:              hiringcafe.Source,
		PayloadType:         hiringcafe.PayloadType,
		ToTargetJobURL:      hiringcafe.ToTargetJobURL,
		ParseRawHTML:        hiringcafe.ParseRawHTML,
		ParseImportRows:     hiringcafe.ParseImportRows,
		SerializeImportRows: hiringcafe.SerializeImportRows,
	},
	remotive.Source: {
		Source:              remotive.Source,
		PayloadType:         remotive.PayloadType,
		ToTargetJobURL:      remotive.ToTargetJobURL,
		ParseRawHTML:        remotive.ParseRawHTML,
		ParseImportRows:     remotive.ParseImportRows,
		SerializeImportRows: remotive.SerializeImportRows,
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
