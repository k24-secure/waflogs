package aws

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
)

func loadConfig(ctx context.Context, profile string, region string) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithSharedConfigProfile(profile),
		config.WithRegion(region), config.WithRetryer(
			func() aws.Retryer {
				return retry.AddWithMaxAttempts(retry.NewStandard(), 3)
			}),
	)
	if err != nil {
		return cfg, fmt.Errorf("loading default config: %s", err)
	}

	_, err = cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return cfg, fmt.Errorf("retrieving credentials: %s", err)

	}

	return cfg, nil
}
