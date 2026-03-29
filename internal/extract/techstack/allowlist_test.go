package techstack

import "testing"

func TestIsAllowedInference(t *testing.T) {
	if !IsAllowedInference(JobCategoryAIResearchScientist, "") {
		t.Fatal("expected AI Research Scientist category to be allowed")
	}
	if !IsAllowedInference("", JobFunctionSecurityEngineer) {
		t.Fatal("expected Security Engineer function to be allowed")
	}
	if !IsAllowedInference("", JobFunctionTechnicalRecruiter) {
		t.Fatal("expected Technical Recruiter function to be allowed")
	}
	if IsAllowedInference("", "Customer Success") {
		t.Fatal("did not expect Customer Success function to be allowed")
	}
}
