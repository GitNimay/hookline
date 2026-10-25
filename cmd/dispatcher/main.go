// Command dispatcher runs the Hookline dispatcher service.
package main

import "github.com/GitNimay/hookline/internal/app"

func main() { app.Main("dispatcher", app.RunDispatcher) }
