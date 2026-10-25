// Command control runs the Hookline control service.
package main

import "github.com/GitNimay/hookline/internal/app"

func main() { app.Main("control", app.RunControl) }
