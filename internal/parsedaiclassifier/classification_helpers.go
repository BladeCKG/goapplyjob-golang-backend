package parsedaiclassifier

import (
	"regexp"
	"strings"
)

var techStackExtractAllowedCategoryTokens = []string{
	"expert",
	"experts",
	"engineer",
	"engineers",
	"programmer",
	"programmers",
	"developer",
	"developers",
	"scientist",
	"scientists",
	"analyst",
	"analysts",
	"des",
	"designer",
	"designers",
	"eng",
	"engr",
	"engrs",
	"dev",
	"devs",
	"scient",
	"anal",
	"arch",
	"architect",
	"architects",
	"swe",
	"se",
	"sse",
	"sde",
	"sdet",
	"de",
	"da",
	"ba",
	"ds",
	"dba",
	"sysadmin",
	"netadmin",
	"uxd",
	"uid",
	"ux",
	"ui",
	"engineering",
	"technician",
	"technicians",
	"tech",
	"technol",
	"technologist",
	"technologists",
	"spec",
	"specialist",
	"specialists",
	// "administrator",
	// "administrators",
	// "admin",
	// "admins",
	// "machinist",
	// "machinists",
	// "manager",
	// "managers",
	// "therapist",
	// "therapists",
	// "coordinator",
	// "coordinators",
	// "machin",
	// "mgr",
	// "therap",
	// "spec",
	// "specs",
	// "coord",
	// "coords",
	// "ea",
	// "ta",
	// "pm",
	// "em",
	// "pt",
	// "ot",
	// "rt",
	// "technology",
}

func shouldInferCategoryWithSkills(roleTitle string) bool {
	normalized := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(strings.ToLower(strings.TrimSpace(roleTitle)), " ")
	normalized = strings.TrimSpace(normalized)
	if normalized == "" {
		return false
	}
	return func(a, b []string) bool {
		if len(a) == 0 || len(b) == 0 {
			return false
		}
		if len(a) > len(b) {
			a, b = b, a
		}
		set := make(map[string]struct{}, len(a))
		for _, s := range a {
			set[s] = struct{}{}
		}
		for _, s := range b {
			if _, ok := set[s]; ok {
				return true
			}
		}
		return false
	}(strings.Fields(normalized), techStackExtractAllowedCategoryTokens)
}
