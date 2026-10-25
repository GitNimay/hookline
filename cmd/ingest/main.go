// Command ingest runs the Hookline ingest service.
package main

import "github.com/GitNimay/hookline/internal/app"

func main() { app.Main("ingest", app.RunIngest) }
