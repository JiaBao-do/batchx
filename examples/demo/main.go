// Realistic multi-step job: prepare, load two reference datasets in parallel,
// then turn orders into invoices (joining the reference data, retrying a flaky
// tax lookup, skipping bad rows), and finally report.
// Run: go run ./examples/demo
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/JiaBao-do/batchx"
)

type Customer struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Invoice struct {
	Customer string `json:"customer"`
	Item     string `json:"item"`
	Cents    int    `json:"cents"`
}

const (
	customersJSONL = "{\"id\":\"c1\",\"name\":\"Ann\"}\n{\"id\":\"c2\",\"name\":\"Bob\"}\n"
	pricesCSV      = "item,cents\nbook,1000\npen,150\n"
	ordersCSV      = "customer,item,qty\nc1,book,2\nc2,pen,10\nc9,pen,1\nc1,pen,x\nc2,book,1\n"
)

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func main() {
	ctx := context.Background()
	var customers batchx.SliceWriter[Customer]
	var prices batchx.SliceWriter[[]string]
	var invoices batchx.SliceWriter[Invoice]

	prepare := batchx.NewTaskletStep("prepare", func(context.Context) error {
		fmt.Println("prepare: ok")
		return nil
	})
	// These two steps share nothing but their own writers, so they run in parallel.
	loadCustomers := must(batchx.NewCopyStep("load-customers", batchx.NewJSONLReader[Customer](strings.NewReader(customersJSONL)), &customers))
	loadPrices := must(batchx.NewCopyStep("load-prices", batchx.NewCSVReader(strings.NewReader(pricesCSV), true), &prices))

	// The invoice step reads the loaded data; the parallel stage has finished, so this is race free.
	names := map[string]string{}
	cents := map[string]int{}
	taxCalls := 0
	invoice := batchx.ProcessorFunc[[]string, Invoice](func(_ context.Context, r []string) (Invoice, error) {
		name, ok := names[r[0]]
		if !ok {
			return Invoice{}, batchx.Skippable(fmt.Errorf("unknown customer %q", r[0]))
		}
		qty, err := strconv.Atoi(r[2])
		if err != nil {
			return Invoice{}, batchx.Skippable(err)
		}
		if taxCalls++; taxCalls == 1 { // the first tax lookup fails once
			return Invoice{}, errors.New("tax service timeout")
		}
		return Invoice{Customer: name, Item: r[1], Cents: cents[r[1]] * qty}, nil
	})
	invoiceStep := must(batchx.NewStep("invoice", batchx.NewCSVReader(strings.NewReader(ordersCSV), true), invoice, &invoices,
		batchx.WithChunkSize(2),
		batchx.WithSkipLimit(5),
		batchx.WithRetry(batchx.Retry{MaxAttempts: 3, Backoff: batchx.ConstantBackoff(time.Millisecond), If: func(err error) bool { return !batchx.IsSkippable(err) }}),
		batchx.WithStepListener(batchx.Listener{
			OnSkip: func(_ context.Context, _ string, _ batchx.Phase, _ any, err error) { fmt.Println("skipped:", err) },
			OnRetry: func(_ context.Context, _ string, _ batchx.Phase, n int, err error) {
				fmt.Printf("retry %d: %v\n", n, err)
			},
		})))
	report := batchx.NewTaskletStep("report", func(context.Context) error {
		total := 0
		for _, inv := range invoices.Items() {
			total += inv.Cents
		}
		fmt.Printf("report: %d invoices, total %d cents\n", len(invoices.Items()), total)
		return nil
	})

	// Fill the lookup maps between the parallel stage and the invoice step.
	fillMaps := batchx.NewTaskletStep("index", func(context.Context) error {
		for _, c := range customers.Items() {
			names[c.ID] = c.Name
		}
		for _, p := range prices.Items() {
			n, _ := strconv.Atoi(p[1])
			cents[p[0]] = n
		}
		return nil
	})

	job := batchx.NewJob("orders", nil).
		Then(prepare).
		ThenParallel(loadCustomers, loadPrices).
		Then(fillMaps).
		Then(invoiceStep).
		Then(report)
	rec, err := job.Run(ctx, batchx.Params{"day": "2026-09-21"})
	if err != nil {
		log.Fatal(err)
	}
	for _, name := range []string{"prepare", "load-customers", "load-prices", "index", "invoice", "report"} {
		s, _ := rec.Step(name)
		fmt.Printf("%-15s %-9s read=%d written=%d skipped=%d\n", name, s.Status, s.Read, s.Written, s.Skipped)
	}
	for _, inv := range invoices.Items() {
		fmt.Printf("invoice: %+v\n", inv)
	}
}
