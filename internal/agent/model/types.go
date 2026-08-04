package model

type Request struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Branch    string `json:"branch,omitempty"`
}

type Check struct {
	ID          string   `json:"id"`
	Stage       string   `json:"stage"`
	Name        string   `json:"name"`
	Passed      bool     `json:"passed"`
	Determinate bool     `json:"determinate"`
	Reason      string   `json:"reason"`
	Evidence    any      `json:"evidence,omitempty"`
	Source      []string `json:"source,omitempty"`
}

type NodeResult struct {
	Name        string  `json:"name"`
	Passed      bool    `json:"passed"`
	Determinate bool    `json:"determinate"`
	Checks      []Check `json:"checks"`
}

type Report struct {
	Request        Request      `json:"request"`
	Scheduler      any          `json:"scheduler"`
	Checks         []Check      `json:"checks"`
	Nodes          []NodeResult `json:"nodes"`
	Passed         bool         `json:"passed"`
	Conclusion     string       `json:"conclusion"`
	SourceFallback bool         `json:"sourceFallback"`
	LLM            string       `json:"llm,omitempty"`
}
