package main

import (
	"fmt"
	"log"

	"github.com/joyent/manta-go"
	"github.com/joyent/manta-go/authentication"
)

const accountName = "tritongo"

func main() {
	sshKeySigner, err := authentication.NewSSHAgentSigner(
		"fd:9e:9a:9c:28:99:57:05:18:9f:b6:44:6b:cc:fd:3a", accountName)
	if err != nil {
		log.Fatalf("NewSSHAgentSigner: %s", err)
	}

	client, err := manta.NewClient(&manta.ClientOptions{
		Endpoint:    "https://us-east.manta.joyent.com/",
		AccountName: accountName,
		Signers:     []authentication.Signer{sshKeySigner},
	})
	if err != nil {
		log.Fatalf("NewClient: %s", err)
	}

	job, err := client.CreateJob(&manta.CreateJobInput{
		Name: "WordCount",
		Phases: []*manta.JobPhase{
			{
				Type: "map",
				Exec: "wc",
			},
			{
				Type: "reduce",
				Exec: "awk '{ l += $1; w += $2; c += $3 } END { print l, w, c }'",
			},
		},
	})
	if err != nil {
		log.Fatalf("CreateJob: %s", err)
	}

	fmt.Printf("Job ID: %s\n", job.JobID)

	err = client.AddJobInputs(&manta.AddJobInputsInput{
		JobID: job.JobID,
		ObjectPaths: []string{
			fmt.Sprintf("/%s/stor/books/treasure_island.txt", accountName),
			fmt.Sprintf("/%s/stor/books/moby_dick.txt", accountName),
			fmt.Sprintf("/%s/stor/books/huck_finn.txt", accountName),
			fmt.Sprintf("/%s/stor/books/dracula.txt", accountName),
		},
	})
	if err != nil {
		log.Fatalf("AddJobInputs: %s", err)
	}

	err = client.AddJobInputs(&manta.AddJobInputsInput{
		JobID: job.JobID,
		ObjectPaths: []string{
			fmt.Sprintf("/%s/stor/books/sherlock_holmes.txt", accountName),
		},
	})
	if err != nil {
		log.Fatalf("AddJobInputs: %s", err)
	}

	err = client.EndJobInput(&manta.EndJobInputInput{
		JobID: job.JobID,
	})
	if err != nil {
		log.Fatalf("EndJobInput: %s", err)
	}
}
