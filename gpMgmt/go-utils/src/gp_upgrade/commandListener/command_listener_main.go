package main

import (
	"os"
	"runtime/debug"

	"github.com/jessevdk/go-flags"
	"gp_upgrade/commandListener/services"
	"gp_upgrade/utils"
)

type ServiceCommands struct {
	Start services.CommandListenerStartCommand `command:"start" alias:"m" description:"Start the Command Listener (blocks)"`
}

var AllServices ServiceCommands

func main() {
	debug.SetTraceback("all")
	parser := flags.NewParser(&AllServices, flags.HelpFlag|flags.PrintErrors)

	_, err := parser.Parse()
	if err != nil {
		os.Exit(utils.GetExitCodeForError(err))
	}
}
