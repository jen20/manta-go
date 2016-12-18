package manta

import (
	"fmt"
	"github.com/hashicorp/errwrap"
	"net/http"
	"strings"
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
