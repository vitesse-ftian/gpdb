package main

import (
	//"os"
	"gp_upgrade/commands"
	"runtime/debug"
	//"gp_upgrade/utils"
	//"github.com/jessevdk/go-flags"
	_ "github.com/lib/pq"
	//"fmt"
	//"strings"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"fmt"
)

type AllCommands struct {
	Monitor commands.MonitorCommand `command:"monitor" alias:"m" description:"Monitor Greenplum upgrade process"`
	Check   commands.CheckCommand   `command:"check" alias:"c" description:"collects information and validates the target Greenplum installation can be upgraded" subcommands-optional:"true"`
	//Version commands.VersionCommand `command:"version" alias:"v" description:"Version of gp_upgrade"`
}

var ALL AllCommands

func main() {
	debug.SetTraceback("all")

	var masterHost string
	var port int

	var cmdCheck = &cobra.Command{
		Use:   "check",
		Short: "collects information and validates the target Greenplum installation can be upgraded",
		Long:  `collects information and validates the target Greenplum installation can be upgraded`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if masterHost == "" {
				return errors.New("the required flag '--master-host' was not specified")
			}
			return nil
		},
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.NewCheckCommand(masterHost, port).Execute(args)
		},

	}

	cmdCheck.PersistentFlags().StringVar(&masterHost, "master-host", "", "host IP for master")
	cmdCheck.PersistentFlags().IntVar(&port, "port", 15432, "port for Greenplum on master")
	//cmdCheck.Flags()
	cmdCheck.MarkFlagRequired("master-host")

	var cmdCheckSubCheckVersionCommand = &cobra.Command{
		Use:   "version",
		Short: "validate current version is upgradable",
		Long:  `validate current version is upgradable`,
		Aliases: []string{"ver"},
		Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.CheckVersionCommand{
				MasterHost:masterHost,
				MasterPort:port,
			}.Execute(args)
		},
	}

	var cmdCheckSubObjectCountCommand = &cobra.Command{
		Use: "object-count",
		Short: "count database objects and numeric objects",
		Long: "count database objects and numeric objects",
		Aliases: []string{"oc"},
		//Args: cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			ocCommand := commands.NewObjectCountCommand(masterHost, port, commands.RealDbConnectionFactory{})
			return ocCommand.Execute(args)
		},
	}

	var cmdVersion = &cobra.Command{
		Use:   "version",
		Short: "Version of gp_upgrade",
		Long:  `Version of gp_upgrade`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			commands.VersionCommand{}.Execute(args)
		},
	}
	var rootCmd = &cobra.Command{Use: "gp_upgrade"}

	// all root level
	rootCmd.AddCommand(cmdCheck, cmdVersion)

	// subcommands
	cmdCheck.AddCommand(cmdCheckSubCheckVersionCommand)
	cmdCheck.AddCommand(cmdCheckSubObjectCountCommand)

	err := rootCmd.Execute()
	if err != nil {
		fmt.Printf("%+v\n", err)
	}
}
