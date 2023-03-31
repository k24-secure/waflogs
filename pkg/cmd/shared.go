package cmd

import (
	"context"
	"fmt"
	"kfzteile24/waflogs/pkg/query"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// variables for the flags
var t time.Time = time.Now().Add(-24 * time.Hour)
var waf query.WAF = query.WafBC
var profile string
var region string
var force int

func watchSignals() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		s := <-sigChan
		fmt.Println("[!] Cancelling execution due to ", s)
		cancel()
	}()

	return ctx
}
