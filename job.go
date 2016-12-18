package manta

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/errwrap"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// JobPhase represents the specification for a map or reduce phase of a Manta
// job.
type JobPhase struct {
	// Type is the type of phase. Must be `map` or `reduce`.
	Type string `json:"type,omitempty"`

	// Assets is an array of objects to be placed in your compute zones.
	Assets []string `json:"assets,omitempty"`

	// Exec is the shell statement to execute. It may be any valid shell
	// command, including pipelines and other shell syntax. You can also
	// execute programs stored in the service by including them in "assets"
	// and referencing them as /assets/$manta_path.
	Exec string `json:"exec"`

	// Init is a shell statement to execute in each compute zone before
	// any tasks are executed. The same constraints apply as to Exec.
	Init string `json:"init"`

	// ReducerCount is an optional number of reducers for this phase. The
	// default value if not specified is 1. The maximum value is 1024.
	ReducerCount uint `json:"count,omitempty"`

	// Memory is the amount of DRAM in MB to be allocated to the compute
	// zone. Valid values are 256, 512, 1024, 2048, 4096 or 8192.
	Memory uint64 `json:"memory,omitempty"`

	// Disk is the amount of disk space in GB to be allocated to the compute
	// zone. Valid values are 2, 4, 8, 16, 32, 64, 128, 256, 512 or 1024.
	Disk uint64 `json:"disk,omitempty"`
}

// JobSummary represents the summary of a compute job in Manta.
type JobSummary struct {
	ModifiedTime time.Time `json:"mtime"`
	ID           string    `json:"name"`
}

// CreateJobInput represents parameters to a CreateJob operation.
type CreateJobInput struct {
	Name   string      `json:"name"`
	Phases []*JobPhase `json:"phases"`
}

// CreateJobOutput contains the outputs of a CreateJob operation.
type CreateJobOutput struct {
	JobID string
}

// CreateJob submits a new job to be executed. This call is not
// idempotent, so calling it twice will create two jobs.
func (c *Client) CreateJob(input *CreateJobInput) (*CreateJobOutput, error) {
	path := fmt.Sprintf("/%s/jobs", c.accountName)

	respBody, respHeaders, err := c.executeRequest(http.MethodPost, path, nil, nil, input)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing CreateJob request: {{err}}", err)
	}

	jobURI := respHeaders.Get("Location")
	parts := strings.Split(jobURI, "/")
	jobID := parts[len(parts)-1]

	response := &CreateJobOutput{
		JobID: jobID,
	}

	return response, nil
}

// AddJobInputs represents parameters to a AddJobInputs operation.
type AddJobInputsInput struct {
	JobID       string
	ObjectPaths []string
}

// AddJobInputs submits inputs to an already created job.
func (c *Client) AddJobInputs(input *AddJobInputsInput) error {
	path := fmt.Sprintf("/%s/jobs/%s/live/in", c.accountName, input.JobID)
	headers := &http.Header{}
	headers.Set("Content-Type", "text/plain")

	reader := strings.NewReader(strings.Join(input.ObjectPaths, "\n"))

	respBody, _, err := c.executeRequestNoEncode(http.MethodPost, path, nil, headers, reader)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing AddJobInputs request: {{err}}", err)
	}

	return nil
}

// EndJobInputInput represents parameters to a EndJobInput operation.
type EndJobInputInput struct {
	JobID string
}

// EndJobInput submits inputs to an already created job.
func (c *Client) EndJobInput(input *EndJobInputInput) error {
	path := fmt.Sprintf("/%s/jobs/%s/live/in/end", c.accountName, input.JobID)

	respBody, _, err := c.executeRequestNoEncode(http.MethodPost, path, nil, nil, nil)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing EndJobInput request: {{err}}", err)
	}

	return nil
}

// CancelJobInput represents parameters to a CancelJob operation.
type CancelJobInput struct {
	JobID string
}

// CancelJob cancels a job from doing any further work. Cancellation
// is asynchronous and "best effort"; there is no guarantee the job
// will actually stop. For example, short jobs where input is already
// closed will likely still run to completion.
//
// This is however useful when:
// 	- input is still open
// 	- you have a long-running job
func (c *Client) CancelJob(input *CancelJobInput) error {
	path := fmt.Sprintf("/%s/jobs/%s/live/cancel", c.accountName, input.JobID)

	respBody, _, err := c.executeRequestNoEncode(http.MethodPost, path, nil, nil, nil)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return errwrap.Wrapf("Error executing CancelJob request: {{err}}", err)
	}

	return nil
}

// ListJobsInput represents parameters to a ListJobs operation.
type ListJobsInput struct {
	RunningOnly bool
	Limit       uint64
	Marker      string
}

// ListJobsOutput contains the outputs of a ListJobs operation.
type ListJobsOutput struct {
	Jobs          []*JobSummary
	ResultSetSize uint64
}

func (c *Client) ListJobs(input *ListJobsInput) (*ListJobsOutput, error) {
	path := fmt.Sprintf("/%s/jobs", c.accountName)
	query := &url.Values{}
	if input.RunningOnly {
		query.Set("state", "running")
	}
	if input.Limit != 0 {
		query.Set("limit", strconv.FormatUint(input.Limit, 10))
	}
	if input.Marker != "" {
		query.Set("manta_path", input.Marker)
	}

	respBody, respHeader, err := c.executeRequest(http.MethodGet, path, query, nil, nil)
	if respBody != nil {
		defer respBody.Close()
	}
	if err != nil {
		return nil, errwrap.Wrapf("Error executing ListJobs request: {{err}}", err)
	}

	var results []*JobSummary
	for {
		current := &JobSummary{}
		decoder := json.NewDecoder(respBody)
		if err = decoder.Decode(&current); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errwrap.Wrapf("Error decoding ListJobs response: {{err}}", err)
		}
		results = append(results, current)
	}

	output := &ListJobsOutput{
		Jobs: results,
	}

	resultSetSize, err := strconv.ParseUint(respHeader.Get("Result-Set-Size"), 10, 64)
	if err == nil {
		output.ResultSetSize = resultSetSize
	}

	return output, nil
}
