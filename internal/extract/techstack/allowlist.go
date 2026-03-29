package techstack

const (
	JobCategoryAIResearchScientist = "AI Research Scientist"

	JobFunctionSoftwareEngineer       = "Software Engineer"
	JobFunctionSales                  = "Sales"
	JobFunctionEngineer               = "Engineer"
	JobFunctionAnalyticsEngineer      = "Analytics Engineer"
	JobFunctionApplicationEngineer    = "Application Engineer"
	JobFunctionBlockchainEngineer     = "Blockchain Engineer"
	JobFunctionCloudEngineer          = "Cloud Engineer"
	JobFunctionElectricalEngineer     = "Electrical Engineer"
	JobFunctionFieldEngineer          = "Field Engineer"
	JobFunctionHardwareEngineer       = "Hardware Engineer"
	JobFunctionInfrastructureEngineer = "Infrastructure Engineer"
	JobFunctionDataEngineer           = "Data Engineer"
	JobFunctionMechanicalEngineer     = "Mechanical Engineer"
	JobFunctionNetworkEngineer        = "Network Engineer"
	JobFunctionPlatformEngineer       = "Platform Engineer"
	JobFunctionQAEngineer             = "QA Engineer"
	JobFunctionResearchEngineer       = "Research Engineer"
	JobFunctionSalesEngineer          = "Sales Engineer"
	JobFunctionSecurityEngineer       = "Security Engineer"
	JobFunctionSmartContractEngineer  = "Smart Contract Engineer"
	JobFunctionSolutionsEngineer      = "Solutions Engineer"
	JobFunctionSupportEngineer        = "Support Engineer"
	JobFunctionSystemsEngineer        = "Systems Engineer"
	JobFunctionDataAnalyst            = "Data Analyst"
	JobFunctionMarketingAnalyst       = "Marketing Analyst"
	JobFunctionSalesforceAnalyst      = "Salesforce Analyst"
	JobFunctionSecurityAnalyst        = "Security Analyst"
	JobFunctionDataScientist          = "Data Scientist"
	JobFunctionResearchScientist      = "Research Scientist"
	JobFunctionSEOMarketing           = "SEO Marketing"
	JobFunctionProductMarketing       = "Product Marketing"
	JobFunctionGrowthMarketing        = "Growth Marketing"
	JobFunctionEmailMarketingManager  = "Email Marketing Manager"
	JobFunctionDesigner               = "Designer"
	JobFunctionWebDesigner            = "Web Designer"
	JobFunctionGraphicsDesigner       = "Graphics Designer"
	JobFunctionBrandDesigner          = "Brand Designer"
	JobFunctionArchitect              = "Architect"
	JobFunctionTechnicalProjectManager = "Technical Project Manager"
	JobFunctionTechnicalRecruiter     = "Technical Recruiter"
)

var AllowedJobCategories = map[string]struct{}{
	JobCategoryAIResearchScientist: {},
}

var AllowedJobFunctions = map[string]struct{}{
	JobFunctionSoftwareEngineer:        {},
	JobFunctionSales:                   {},
	JobFunctionEngineer:                {},
	JobFunctionAnalyticsEngineer:       {},
	JobFunctionApplicationEngineer:     {},
	JobFunctionBlockchainEngineer:      {},
	JobFunctionCloudEngineer:           {},
	JobFunctionElectricalEngineer:      {},
	JobFunctionFieldEngineer:           {},
	JobFunctionHardwareEngineer:        {},
	JobFunctionInfrastructureEngineer:  {},
	JobFunctionDataEngineer:            {},
	JobFunctionMechanicalEngineer:      {},
	JobFunctionNetworkEngineer:         {},
	JobFunctionPlatformEngineer:        {},
	JobFunctionQAEngineer:              {},
	JobFunctionResearchEngineer:        {},
	JobFunctionSalesEngineer:           {},
	JobFunctionSecurityEngineer:        {},
	JobFunctionSmartContractEngineer:   {},
	JobFunctionSolutionsEngineer:       {},
	JobFunctionSupportEngineer:         {},
	JobFunctionSystemsEngineer:         {},
	JobFunctionDataAnalyst:             {},
	JobFunctionMarketingAnalyst:        {},
	JobFunctionSalesforceAnalyst:       {},
	JobFunctionSecurityAnalyst:         {},
	JobFunctionDataScientist:           {},
	JobFunctionResearchScientist:       {},
	JobFunctionSEOMarketing:            {},
	JobFunctionProductMarketing:        {},
	JobFunctionGrowthMarketing:         {},
	JobFunctionEmailMarketingManager:   {},
	JobFunctionDesigner:                {},
	JobFunctionWebDesigner:             {},
	JobFunctionGraphicsDesigner:        {},
	JobFunctionBrandDesigner:           {},
	JobFunctionArchitect:               {},
	JobFunctionTechnicalProjectManager: {},
	JobFunctionTechnicalRecruiter:      {},
}

func IsAllowedInference(jobCategory, jobFunction string) bool {
	_, categoryAllowed := AllowedJobCategories[jobCategory]
	_, functionAllowed := AllowedJobFunctions[jobFunction]
	return categoryAllowed || functionAllowed
}
