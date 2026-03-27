package techstack

const (
	JobFunctionSoftwareEngineer = "Software Engineer"
	DevOpsEngineer              = "DevOps Engineer"
	MachineLearningEngineer     = "Machine Learning Engineer"
)

var AllowedJobCategories = map[string]struct{}{}

var AllowedJobFunctions = map[string]struct{}{
	JobFunctionSoftwareEngineer: {},
	DevOpsEngineer:              {},
	MachineLearningEngineer:     {},
}

func IsAllowedInference(jobCategory, jobFunction string) bool {
	_, categoryAllowed := AllowedJobCategories[jobCategory]
	_, functionAllowed := AllowedJobFunctions[jobFunction]
	return categoryAllowed && functionAllowed
}
