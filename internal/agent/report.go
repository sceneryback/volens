package agent

func finalizeReport(report *Report) {
	report.Passed = true

	for _, check := range report.Checks {
		report.Passed = report.Passed && check.Passed && check.Determinate
	}

	if failure := firstKnownFailure(*report); failure != "" {
		report.Conclusion = failure

		return
	}

	if report.Passed {
		report.Conclusion = "All deterministic checks passed; inspect queue ordering, fairness, and scheduler runtime state."

		return
	}

	report.Conclusion = "Common deterministic rules found no failure; one or more branch-specific plugin checks require source and scheduler-log analysis."
}

func firstKnownFailure(report Report) string {
	for _, check := range report.Checks {
		if check.Determinate && !check.Passed {
			return check.Name + ": " + check.Reason
		}
	}

	return ""
}
