package techstacknorm

import (
	"regexp"
	"strings"
)

var aliases = map[string]string{
	"nodejs":                   "Node.js",
	"node.js":                  "Node.js",
	"node js":                  "Node.js",
	"reactjs":                  "React",
	"react.js":                 "React",
	"nextjs":                   "Next.js",
	"next.js":                  "Next.js",
	"vuejs":                    "Vue.js",
	"vue.js":                   "Vue.js",
	"angularjs":                "AngularJS",
	"javascript":               "JavaScript",
	"typescript":               "TypeScript",
	"c#":                       "C#",
	"csharp":                   "C#",
	"c++":                      "C++",
	"cplusplus":                "C++",
	"golang":                   "Go",
	"postgres":                 "PostgreSQL",
	"postgresql":               "PostgreSQL",
	"mongodb":                  "MongoDB",
	"graphql":                  "GraphQL",
	"graph ql":                 "GraphQL",
	"rest api":                 "REST API",
	"restful api":              "REST API",
	"rest apis":                "REST API",
	"restful apis":             "REST API",
	"apis":                     "API",
	"aws":                      "AWS",
	"gcp":                      "GCP",
	"azure":                    "Azure",
	".net":                     ".NET",
	"dotnet":                   ".NET",
	"asp.net":                  "ASP.NET",
	"asp.net core":             "ASP.NET Core",
	"grpc":                     "gRPC",
	"json":                     "JSON",
	"xml":                      "XML",
	"html":                     "HTML",
	"html5":                    "HTML5",
	"css":                      "CSS",
	"css3":                     "CSS3",
	"scss":                     "SCSS",
	"sass":                     "Sass",
	"sql":                      "SQL",
	"nosql":                    "NoSQL",
	"no-sql":                   "NoSQL",
	"etl":                      "ETL",
	"elt":                      "ELT",
	"etl/elt":                  "ETL/ELT",
	"ci/cd":                    "CI/CD",
	"cicd":                     "CI/CD",
	"iac":                      "IaC",
	"infrastructure as code":   "Infrastructure as Code",
	"k8s":                      "Kubernetes",
	"kubernetes (k8s)":         "Kubernetes",
	"tailwindcss":              "Tailwind CSS",
	"tailwind css":             "Tailwind CSS",
	"google tag manager (gtm)": "Google Tag Manager",
	"google tag manager":       "Google Tag Manager",
	"gtm":                      "Google Tag Manager",
	"sfdc":                     "Salesforce",
	"sfdc crm":                 "Salesforce",
	"salesforce.com":           "Salesforce",
	"salesforce crm":           "Salesforce",
}

var dropValues = map[string]struct{}{
	"n/a": {}, "na": {}, "none": {}, "null": {}, "unknown": {}, "tbd": {},
}

func NormalizeValue(value string) string {
	normalized := strings.TrimSpace(value)
	normalized = strings.Trim(normalized, "\"'")
	normalized = regexp.MustCompile(`\([^)]*\)`).ReplaceAllString(normalized, "")
	if strings.Contains(normalized, "(") && !strings.Contains(normalized, ")") {
		normalized = strings.SplitN(normalized, "(", 2)[0]
	}
	normalized = strings.ReplaceAll(normalized, ")", "")
	normalized = strings.ReplaceAll(normalized, "]", "")
	normalized = strings.ReplaceAll(normalized, "}", "")
	normalized = regexp.MustCompile(`\s*/\s*`).ReplaceAllString(normalized, "/")
	normalized = regexp.MustCompile(`\s*-\s*`).ReplaceAllString(normalized, "-")
	normalized = regexp.MustCompile(`[;,:]+$`).ReplaceAllString(normalized, "")
	normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")
	normalized = strings.Trim(normalized, " -_/")
	normalized = strings.TrimRight(normalized, ".")
	if normalized == "" {
		return ""
	}
	lowered := strings.ToLower(normalized)
	if _, ok := dropValues[lowered]; ok {
		return ""
	}
	if alias, ok := aliases[lowered]; ok {
		return alias
	}
	return normalized
}

func Normalize(values any) []string {
	list, ok := values.([]any)
	if !ok {
		if direct, ok := values.([]string); ok {
			list = make([]any, 0, len(direct))
			for _, value := range direct {
				list = append(list, value)
			}
		} else {
			return []string{}
		}
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(list))
	for _, item := range list {
		value, _ := item.(string)
		next := NormalizeValue(value)
		if next == "" {
			continue
		}
		key := strings.ToLower(next)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, next)
	}
	return out
}
