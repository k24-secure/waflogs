package main

import (
	"log"
	"os"

	"kfzteile24/waflogs/pkg/cmd"

	"github.com/urfave/cli/v2"
)

func main() {
	var cmds []*cli.Command
	cmds = append(cmds, cmd.MakeLoadCmd())
	cmds = append(cmds, cmd.MakeReportCmd())

	app := &cli.App{
		Commands: cmds,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
