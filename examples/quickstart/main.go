// Quickstart: read CSV, process each row, write JSON lines, in chunks of 2.
// Run: go run ./examples/quickstart
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/JiaBao-do/batchx"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func main() {
	in := strings.NewReader("id,name\n1,ann\n2,bob\n3,cy\n")
	toUser := batchx.ProcessorFunc[[]string, User](func(_ context.Context, r []string) (User, error) {
		return User{ID: r[0], Name: strings.ToUpper(r[1])}, nil
	})
	step, err := batchx.NewStep("import", batchx.NewCSVReader(in, true), toUser,
		batchx.NewJSONLWriter[User](os.Stdout), batchx.WithChunkSize(2))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := batchx.NewJob("quickstart", nil).Then(step).Run(context.Background(), nil); err != nil {
		log.Fatal(err)
	}
}
