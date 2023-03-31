package cmd

import (
	"fmt"
	"kfzteile24/waflogs/pkg/query"
	"kfzteile24/waflogs/pkg/report/printer"
	"log"

	"github.com/urfave/cli/v2"
)

func MakeReportCmd() *cli.Command {

	return &cli.Command{
		Name:    "report",
		Aliases: []string{"r"},
		Usage:   "print summary reports",
		Flags:   makeReportFlags(),
		Subcommands: []*cli.Command{
			{
				Name:    "rate-limit",
				Aliases: []string{"r"},
				Usage:   "for the rate limit report",
				Flags:   makeReportFlags(),
				Action: func(cCtx *cli.Context) error {
					log.Printf("[+] Rate rate limit\n")
					log.Printf("    Params: time = %s, waf = %s, profile = %s region = %s force = %t\n", t.Format("2006-01-02"), waf, profile, region, force > 0)

					rp := printer.NewRateLimitReportPrinter(waf)
					if err := rp.Print(); err != nil {
						log.Fatalf("[!] Error printing rate limit report: %s", err)
					}

					return nil
				},
			},
		},
	}
}

func makeReportFlags() []cli.Flag {
	return []cli.Flag{
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
	}
}
