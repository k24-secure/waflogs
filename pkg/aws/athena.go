package aws

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type AthenaClient struct {
	Profile      string
	ctx          context.Context
	skipExisting bool // to skip queries we have results for already

	Catalog        string
	Database       string
	OutputLocation string

	AWS *athena.Client
}

func NewAthenaClient(ctx context.Context, profile string, region string, skipExisting bool) (*AthenaClient, error) {
	cfg, err := loadConfig(ctx, profile, region)
	if err != nil {
		return nil, fmt.Errorf("creating AWS config: %s", err)
	}

	return &AthenaClient{
		Profile:      profile,
		ctx:          ctx,
		skipExisting: skipExisting,

		Catalog:        "AwsDataCatalog",
		Database:       "waflogs",
		OutputLocation: "s3://aws-athena-query-results-433833759926-eu-central-1/",
		AWS:            athena.NewFromConfig(cfg),
	}, nil
}

// Query executes an sql statement and stores the result locally at dstPath
func (c *AthenaClient) Query(sql string, dstPath string) error {
	if c.skipExisting && fileExists(dstPath) {
		return nil // assume the existing file contains the result
	}

	qid, err := c.startQueryExecution(sql)
	if err != nil {
		return fmt.Errorf("starting query: %s", err)
	}

	fmt.Printf("[+] Query %s started\n", qid)

	for i := 0; i < 10; i++ {
		select {
		case <-c.ctx.Done():
			// cancel in-flight query, no need to pay for it
			if err := c.stopQueryExecution(qid); err != nil {
				return fmt.Errorf("cancelling query %s: %s", qid, err)
			}
			return nil
		case <-time.After(time.Duration(i) * 3 * time.Second):
			// wait a few seconds for results
		}

		nerr := 0
		e, err := c.getQueryExecution(qid)
		if err != nil {
			nerr += 1
			if nerr > 3 {
				return fmt.Errorf("getting query status failed too often, last error: %s", err)
			}
		}

		if e.State == QueryPending {
			fmt.Printf("[+] Query %s pending\n", qid)
			continue
		}

		if e.State == QueryFailed {
			return fmt.Errorf("query execution failed (Reason: %s)", e.Reason)
		}

		// success
		fmt.Printf("[+] Query %s finished successfully, scanned %s (~%s)\n", qid, bytesToHuman(e.BytesScanned), estimatedQueryCost(e.BytesScanned))
		break
	}

	if err := c.getQueryResults(qid, dstPath); err != nil {
		return fmt.Errorf("getting query results: %s", err)
	}

	return nil
}

func (c *AthenaClient) startQueryExecution(sql string) (string, error) {
	resp, err := c.AWS.StartQueryExecution(
		c.ctx,
		&athena.StartQueryExecutionInput{
			QueryString: awssdk.String(sql),
			QueryExecutionContext: &types.QueryExecutionContext{
				Catalog:  awssdk.String(c.Catalog),
				Database: awssdk.String(c.Database),
			},
			WorkGroup: awssdk.String("athena3"),
			ResultConfiguration: &types.ResultConfiguration{
				OutputLocation: awssdk.String(c.OutputLocation),
				EncryptionConfiguration: &types.EncryptionConfiguration{
					EncryptionOption: types.EncryptionOptionSseS3,
				},
			},
		},
	)
	if err != nil {
		return "", fmt.Errorf("starting query execution: %s", err)
	}

	if resp.QueryExecutionId == nil || len(*resp.QueryExecutionId) < 1 {
		return "", fmt.Errorf("no query string returned")
	}

	return *resp.QueryExecutionId, nil
}

type QueryStatus struct {
	State        QueryState
	Reason       string
	BytesScanned int64
}

type QueryState int

const (
	QueryPending QueryState = iota
	QuerySuccessful
	QueryFailed
)

func (c *AthenaClient) getQueryExecution(qid string) (*QueryStatus, error) {
	resp, err := c.AWS.GetQueryExecution(
		c.ctx,
		&athena.GetQueryExecutionInput{
			QueryExecutionId: awssdk.String(qid),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("getting query execution: %s", err)
	}

	if resp.QueryExecution == nil {
		return nil, fmt.Errorf("no query execution returned")
	}

	if resp.QueryExecution.Status == nil {
		return nil, fmt.Errorf("no query execution status returned")
	}

	reason := ""
	if resp.QueryExecution.Status.StateChangeReason != nil {
		reason = *(resp.QueryExecution.Status.StateChangeReason)
	}

	// we know it worked
	if resp.QueryExecution.Status.State == types.QueryExecutionStateSucceeded {
		if resp.QueryExecution.Statistics == nil {
			return nil, fmt.Errorf("no query execution statistics returned")
		}
		return &QueryStatus{
			State:        QuerySuccessful,
			Reason:       reason,
			BytesScanned: safeInt64(resp.QueryExecution.Statistics.DataScannedInBytes),
		}, nil
	}

	// we assume it failed (TODO: AWS docs say Athena may retry on its own later, find out if that ever happens)
	if resp.QueryExecution.Status.State == types.QueryExecutionStateFailed || resp.QueryExecution.Status.State == types.QueryExecutionStateCancelled {
		return &QueryStatus{
			State:        QueryFailed,
			Reason:       reason,
			BytesScanned: safeInt64(resp.QueryExecution.Statistics.DataScannedInBytes),
		}, nil
	}

	return &QueryStatus{
		State:  QueryPending,
		Reason: reason,
	}, nil
}

func (c *AthenaClient) getQueryResults(qid string, dstPath string) error {
	f, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("opening destination file path: %s", err)
	}
	defer f.Close()

	wFile := csv.NewWriter(f)
	wStdOut := csv.NewWriter(os.Stdout)

	var next *string
	var numRowsPreview int
	for {
		resp, err := c.AWS.GetQueryResults(
			c.ctx,
			&athena.GetQueryResultsInput{
				QueryExecutionId: awssdk.String(qid),
				NextToken:        next,
				MaxResults:       awssdk.Int32(1000),
			},
		)
		if err != nil {
			return fmt.Errorf("describing images: %s", err)
		}

		if resp.ResultSet == nil {
			return fmt.Errorf("no result set returned")
		}

		for _, row := range resp.ResultSet.Rows {
			data := []string{}
			for _, d := range row.Data {
				data = append(data, safeString(d.VarCharValue))
			}

			// print first 20 rows
			if numRowsPreview < 20 {
				numRowsPreview += 1
				wStdOut.Write(data)
			}

			wFile.Write(data)
		}

		wStdOut.Flush()

		if resp.NextToken != nil {
			next = resp.NextToken
		} else {
			break
		}

	}

	wFile.Flush()

	return nil
}

func (c *AthenaClient) stopQueryExecution(qid string) error {
	_, err := c.AWS.StopQueryExecution(
		c.ctx,
		&athena.StopQueryExecutionInput{
			QueryExecutionId: awssdk.String(qid),
		},
	)
	if err != nil {
		return fmt.Errorf("getting query execution: %s", err)
	}

	return nil
}

func safeString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func safeInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

func bytesToHuman(b int64) string {
	bf := float64(b)
	for _, unit := range []string{"", "K", "M", "G", "T", "P", "E", "Z"} {
		if math.Abs(bf) < 1024.0 {
			return fmt.Sprintf("%3.1f%sB", bf, unit)
		}
		bf /= 1024.0
	}
	return fmt.Sprintf("%.1fYiB", bf)
}

const TB = 1099511627776 // number of bytes of a TB

// price is 5 USD per TB scanned
func estimatedQueryCost(bytesScanned int64) string {
	return fmt.Sprintf("%.2f USD", 5.0*(float64(bytesScanned)/float64(TB)))
}

func fileExists(path string) bool {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}
