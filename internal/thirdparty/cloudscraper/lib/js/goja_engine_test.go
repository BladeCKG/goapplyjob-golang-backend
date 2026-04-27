package js

import (
	"testing"

	"github.com/dop251/goja"
)

func TestSetupScriptPersistsDOMElements(t *testing.T) {
	vm := goja.New()
	if err := vm.Set("__cloudscraper_domain__", "example.com"); err != nil {
		t.Fatalf("set domain: %v", err)
	}
	if _, err := vm.RunString(setupScript); err != nil {
		t.Fatalf("run setup script: %v", err)
	}
	if _, err := vm.RunString(`document.getElementById("jschl-answer").value = "token-123";`); err != nil {
		t.Fatalf("assign answer: %v", err)
	}
	value, err := vm.RunString(`document.getElementById("jschl-answer").value`)
	if err != nil {
		t.Fatalf("read answer: %v", err)
	}
	if got := value.String(); got != "token-123" {
		t.Fatalf("expected persisted answer, got %q", got)
	}
}

func TestSetupScriptRunsSetTimeoutSynchronously(t *testing.T) {
	vm := goja.New()
	if err := vm.Set("__cloudscraper_domain__", "example.com"); err != nil {
		t.Fatalf("set domain: %v", err)
	}
	if _, err := vm.RunString(setupScript); err != nil {
		t.Fatalf("run setup script: %v", err)
	}
	value, err := vm.RunString(`
		var answer = "";
		setTimeout(function() { answer = "done"; }, 4000);
		answer;
	`)
	if err != nil {
		t.Fatalf("run timeout script: %v", err)
	}
	if got := value.String(); got != "done" {
		t.Fatalf("expected synchronous timeout shim, got %q", got)
	}
}
