// Command gomigrate runs the Go SDK's migrate entry for the Python SDK's interoperability test: the same files and
// the same schema, so both SDKs must read and write the same migration state (be-protocol P5.12).
package main

import (
	"os"

	"github.com/brickKit/be-sdk-go/v3/component"
	"github.com/brickKit/be-sdk-go/v3/launcher"
	"github.com/brickKit/be-sdk-go/v3/runtime"
)

func main() {
	launcher.Main(component.Definition{ID: os.Getenv("MEMBER_ID"), Version: "3.0.0",
		Migrations: os.DirFS(os.Getenv("MIGRATIONS_DIR")),
		New:        func(*runtime.Runtime) (component.Module, error) { return nil, nil }})
}
