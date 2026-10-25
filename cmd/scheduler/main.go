// Command scheduler runs the Hookline scheduler service.
package main

import "github.com/GitNimay/hookline/internal/app"

func main() { app.Main("scheduler", app.RunScheduler) }
