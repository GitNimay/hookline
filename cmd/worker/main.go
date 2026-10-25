// Command worker runs the Hookline worker service.
package main

import "github.com/GitNimay/hookline/internal/app"

func main() { app.Main("worker", app.RunWorker) }
