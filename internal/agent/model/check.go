package model

func Known(
	id string,
	stage string,
	name string,
	passed bool,
	reason string,
	source []string,
) Check {
	return Check{
		ID:          id,
		Stage:       stage,
		Name:        name,
		Passed:      passed,
		Determinate: true,
		Reason:      reason,
		Source:      source,
	}
}

func Unknown(
	id string,
	stage string,
	name string,
	reason string,
	evidence any,
	source []string,
) Check {
	return Check{
		ID:          id,
		Stage:       stage,
		Name:        name,
		Passed:      false,
		Determinate: false,
		Reason:      reason,
		Evidence:    evidence,
		Source:      source,
	}
}
