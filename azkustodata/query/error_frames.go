package query

type OneApiError struct {
	Error ErrorMessage `json:"error"`
}

type ErrorMessage struct {
	Code        string       `json:"code"`
	Message     string       `json:"message"`
	Type        string       `json:"@type"`
	Context     ErrorContext `json:"@context"`
	IsPermanent bool         `json:"@permanent"`
}

type ErrorContext struct {
	Timestamp        string `json:"timestamp"`
	ServiceAlias     string `json:"serviceAlias"`
	MachineName      string `json:"machineName"`
	ProcessName      string `json:"processName"`
	ProcessId        int    `json:"processId"`
	ThreadId         int    `json:"threadId"`
	ClientRequestId  string `json:"clientRequestId"`
	ActivityId       string `json:"activityId"`
	SubActivityId    string `json:"subActivityId"`
	ActivityType     string `json:"activityType"`
	ParentActivityId string `json:"parentActivityId"`
	ActivityStack    string `json:"activityStack"`
}
