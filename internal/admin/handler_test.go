package admin

import (
	"reflect"
	"testing"
	"time"
)

func TestBuildColumnFilterSQLParsesBoolValues(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{name: "bool true", value: true, expected: true},
		{name: "bool false", value: false, expected: false},
		{name: "numeric one", value: float64(1), expected: true},
		{name: "numeric zero", value: float64(0), expected: false},
		{name: "string true", value: "true", expected: true},
		{name: "string false", value: "false", expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sql, args, err := buildColumnFilterSQL(
				filterDef{columnExpr: "is_ready", valueType: "bool"},
				adminColumnFilter{Column: "is_ready", Operator: "=", Value: tc.value},
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != "is_ready = ?" {
				t.Fatalf("unexpected sql: %q", sql)
			}
			if len(args) != 1 {
				t.Fatalf("expected one arg, got %d", len(args))
			}
			got, ok := args[0].(bool)
			if !ok {
				t.Fatalf("expected bool arg, got %T (%v)", args[0], args[0])
			}
			if got != tc.expected {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestBuildColumnFilterSQLParsesNumericValues(t *testing.T) {
	sql, args, err := buildColumnFilterSQL(
		filterDef{columnExpr: "retry_count", valueType: "int"},
		adminColumnFilter{Column: "retry_count", Operator: ">=", Value: "3"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "retry_count >= ?" {
		t.Fatalf("unexpected sql: %q", sql)
	}
	if !reflect.DeepEqual(args, []any{3}) {
		t.Fatalf("unexpected args: %#v", args)
	}

	sql, args, err = buildColumnFilterSQL(
		filterDef{columnExpr: "salary_min_usd", valueType: "float"},
		adminColumnFilter{Column: "salary_min_usd", Operator: ">=", Value: "12.5"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "salary_min_usd >= ?" {
		t.Fatalf("unexpected sql: %q", sql)
	}
	if !reflect.DeepEqual(args, []any{12.5}) {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestBuildColumnFilterSQLParsesDateTimeValues(t *testing.T) {
	sql, args, err := buildColumnFilterSQL(
		filterDef{columnExpr: "post_date", valueType: "datetime"},
		adminColumnFilter{Column: "post_date", Operator: ">=", Value: "2026-03-01T00:00:00Z"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "post_date >= ?" {
		t.Fatalf("unexpected sql: %q", sql)
	}
	if len(args) != 1 {
		t.Fatalf("expected one arg, got %d", len(args))
	}
	got, ok := args[0].(time.Time)
	if !ok {
		t.Fatalf("expected time.Time arg, got %T (%v)", args[0], args[0])
	}
	expected := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	if !got.Equal(expected) {
		t.Fatalf("unexpected datetime arg: %s", got.UTC().Format(time.RFC3339))
	}
}
