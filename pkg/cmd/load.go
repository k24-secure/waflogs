package cmd

import (
	"fmt"
	"kfzteile24/waflogs/pkg/aws"
	"kfzteile24/waflogs/pkg/query"
	"kfzteile24/waflogs/pkg/report"
	"log"
	"time"

	"github.com/urfave/cli/v2"
)

func MakeLoadCmd() *cli.Command {

	return &cli.Command{
		Name:    "load",
		Aliases: []string{"l"},
		Usage:   "load data from Athena",
		Flags:   makeLoadFlags(),
		Subcommands: []*cli.Command{
			{
				Name:    "rate-limit-report",
				Aliases: []string{"r"},
				Usage:   "for the rate limit report",
				Flags:   makeLoadFlags(),
				Action: func(cCtx *cli.Context) error {
					ctx := watchSignals()
					log.Printf("[+] Loading data for the rate limit report\n")
					log.Printf("    Params: time = %s, waf = %s, profile = %s region = %s force = %t\n", t.Format("2006-01-02"), waf, profile, region, force > 0)

					athena, err := aws.NewAthenaClient(ctx, profile, region, force > 0)
					if err != nil {
						log.Fatalf("Error making Athena client: %s", err)
					}

					r := report.NewRateLimitReportLoader(athena, query.WafBC, t)
					if err := r.Run(); err != nil {
						log.Fatalf("Error running rate limit report: %s", err)
					}

					return nil
				},
			},
			{
				Name:  "apc1",
				Usage: "for the APC1 report",
				Flags: makeLoadFlags(),
				Action: func(cCtx *cli.Context) error {
					ctx := watchSignals()
					log.Printf("[+] Loading data for the APC1 report\n")
					log.Printf("    Params: time = %s, waf = %s, profile = %s region = %s force = %t\n", t.Format("2006-01-02"), waf, profile, region, force > 0)

					athena, err := aws.NewAthenaClient(ctx, profile, region, force > 0)
					if err != nil {
						log.Fatalf("Error making Athena client: %s", err)
					}

					r := report.NewAPC1ReportLoader(athena, query.WafBC, t)
					if err := r.Run(); err != nil {
						log.Fatalf("Error running APC1 report: %s", err)
					}

					return nil
				},
			},
		},
	}
}

func makeLoadFlags() []cli.Flag {
	return []cli.Flag{
		&cli.TimestampFlag{
			Name:    "timestamp",
			Aliases: []string{"t"},
			Usage:   "Day to check, e.g., 2023-02-21",
			Layout:  "2006-01-02",
			Action: func(ctx *cli.Context, v *time.Time) error {
				if v == nil {
					t = time.Now()
					return nil
				}

				t = *v
				return nil
			},
		},
		&cli.StringFlag{
			Name:    "waf",
			Aliases: []string{"w"},
			Value:   "BC",
			Usage:   "WAF to check (BC or ECP)",
			Action: func(ctx *cli.Context, v string) error {
				switch v {
				case "BC", "bc":
					waf = query.WafBC
				case "ECP", "ecp":
					waf = query.WafECP
				default:
					return fmt.Errorf("WAF %s unknown, must be one of 'BC', 'ECP'", v)
				}

				return nil
			},
		},
		&cli.StringFlag{
			Name:        "profile",
			Aliases:     []string{"p"},
			Value:       "k24SecruityRule-433833759926",
			Usage:       "AWS profile for the account to run queries in",
			Destination: &profile,
		},
		&cli.StringFlag{
			Name:        "region",
			Aliases:     []string{"r"},
			Value:       "eu-central-1",
			Usage:       "AWS region to run queries in",
			Destination: &region,
		},
		&cli.BoolFlag{
			Name:    "force",
			Aliases: []string{"f"},
			Value:   false,
			Usage:   "force execution of queries for which results are already on disk",
			Count:   &force,
		},
	}
}
