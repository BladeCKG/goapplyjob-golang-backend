package js

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/dop251/goja"
	"goapplyjob-golang-backend/internal/thirdparty/cloudscraper/lib/security"
)

// Create a Simulated Browser Environment (DOM Shim)
//
//go:embed setup.js
var setupScript string

// GojaEngine uses the embedded goja interpreter.
type GojaEngine struct{}

// NewGojaEngine creates a new engine that uses the built-in goja interpreter.
func NewGojaEngine() *GojaEngine {
	return &GojaEngine{}
}

// Run executes a script in goja. It captures output by overriding console.log.
func (e *GojaEngine) Run(ctx context.Context, script string) (string, error) {
	// Security: Check script size to prevent DoS attacks
	if err := security.ValidateScriptSize(script, security.MaxGojaScriptSize); err != nil {
		return "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	vm := goja.New()
	var result string

	// Setup safe console.log capturing
	console := vm.NewObject()
	console.Set("log", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) > 0 {
			result = call.Arguments[0].String()
		}
		return goja.Undefined()
	})
	vm.Set("console", console)

	// === Hardened Execution ===
	const maxExecutionTime = 3 * time.Second
	done := make(chan struct{})
	var execErr error

	// Run the script in a goroutine with timeout
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("goja: panic during execution: %v", r)
			}
		}()

		_, execErr = vm.RunString(script)
	}()

	// Wait for completion or timeout
	timer := time.NewTimer(maxExecutionTime)
	defer timer.Stop()

	select {
	case <-done:
		if execErr != nil {
			return "", fmt.Errorf("goja: script execution failed: %w", execErr)
		}
		return result, nil
	case <-timer.C:
		vm.Interrupt("execution timeout")
		<-done // Wait for goroutine to finish
		return "", fmt.Errorf("goja: script execution timed out after %v", maxExecutionTime)
	case <-ctx.Done():
		vm.Interrupt(ctx.Err())
		<-done
		return "", ctx.Err()
	}
}

// SolveV2Challenge uses the original synchronous method to solve v2 challenges,
// as goja does not support asynchronous operations like setTimeout without additional setup.
func (e *GojaEngine) SolveV2Challenge(ctx context.Context, body, domain string, scriptMatches [][]string, logger *log.Logger) (string, error) {
	// Security: Check total script size
	if err := security.ValidateTotalScriptSize(scriptMatches, security.MaxGojaScriptSize); err != nil {
		if logger != nil {
			logger.Printf("cloudscraper: goja SolveV2Challenge script_size_validation_failed domain=%q err=%v", domain, err)
		}
		return "", fmt.Errorf("goja: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if logger != nil {
		logger.Printf("cloudscraper: goja SolveV2Challenge start domain=%q script_blocks=%d body_len=%d", domain, len(scriptMatches), len(body))
	}

	vm := goja.New()
	if err := vm.Set("__cloudscraper_domain__", domain); err != nil {
		if logger != nil {
			logger.Printf("cloudscraper: goja SolveV2Challenge set_domain_failed domain=%q err=%v", domain, err)
		}
		return "", fmt.Errorf("goja: failed to set domain: %w", err)
	}

	// Security: Running setup script in VM.
	if _, err := vm.RunString(setupScript); err != nil {
		if logger != nil {
			logger.Printf("cloudscraper: goja SolveV2Challenge setup_failed domain=%q err=%v", domain, err)
		}
		return "", fmt.Errorf("goja: failed to set up DOM shim: %w", err)
	}

	// Execute all extracted Cloudflare scripts in the same VM context.
	for idx, match := range scriptMatches {
		if len(match) > 1 {
			scriptContent := match[1]
			scriptContent = strings.ReplaceAll(scriptContent, `document.getElementById('challenge-form');`, "({})")
			// Security: This executes JavaScript from the Cloudflare challenge page.
			// The goja VM is sandboxed, but this is an inherent risk of the library's function.
			if _, err := vm.RunString(scriptContent); err != nil {
				if logger != nil {
					logger.Printf("cloudscraper: goja SolveV2Challenge script_block_failed domain=%q index=%d err=%v", domain, idx, err)
				}
			} else if logger != nil {
				logger.Printf("cloudscraper: goja SolveV2Challenge script_block_ok domain=%q index=%d script_len=%d", domain, idx, len(scriptContent))
			}
		}
	}

	// Wait for the script's internal timeouts to complete.
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(4 * time.Second):
	}

	// Get the final answer from the most likely challenge fields in the dummy document.
	// Security: This executes a small, controlled script to retrieve a value.
	answerVal, err := vm.RunString(`
(function() {
	var ids = ['jschl-answer', 'jschl_answer', 'cf-chl-answer', 'cf_chl_answer'];
	for (var i = 0; i < ids.length; i++) {
		var el = document.getElementById(ids[i]);
		if (el && typeof el.value === 'string' && el.value) {
			return el.value;
		}
	}
	return '';
})()
`)
	if err != nil {
		if logger != nil {
			logger.Printf("cloudscraper: goja SolveV2Challenge answer_extract_failed domain=%q err=%v", domain, err)
		}
		return "", fmt.Errorf("goja: could not retrieve final answer from VM: %w", err)
	}

	answer := answerVal.String()
	if answer == "" || answer == "undefined" {
		if logger != nil {
			debugVal, debugErr := vm.RunString(`
(function() {
	var ids = ['jschl-answer', 'jschl_answer', 'cf-chl-answer', 'cf_chl_answer'];
	var out = [];
	for (var i = 0; i < ids.length; i++) {
		var el = document.getElementById(ids[i]);
		var value = '';
		if (el && typeof el.value !== 'undefined') {
			value = String(el.value);
		}
		out.push(ids[i] + '=' + value);
	}
	return out.join(', ');
})()
`)
			if debugErr != nil {
				logger.Printf("cloudscraper: goja SolveV2Challenge empty_answer domain=%q answer=%q fields_debug_err=%v", domain, answer, debugErr)
			} else {
				logger.Printf("cloudscraper: goja SolveV2Challenge empty_answer domain=%q answer=%q fields=%q", domain, answer, debugVal.String())
			}
		}
		return "", fmt.Errorf("goja: answer value is empty or undefined")
	}
	if logger != nil {
		logger.Printf("cloudscraper: goja SolveV2Challenge success domain=%q answer_len=%d", domain, len(answer))
	}

	return answer, nil
}
