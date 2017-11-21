package main

import (
	"fmt"
	"gp_upgrade/cli/commanders"
	"gp_upgrade/commands"
	"log"
	"os"
	"runtime/debug"

	pb "gp_upgrade/idl"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	hubPort = "7527"
)

func main() {
	debug.SetTraceback("all")
	//empty logdir defaults to ~/gpAdminLogs
	gpbackupUtils.InitializeLogging("gp_upgrade_cli", "")

	var masterHost string
	var dbPort int

	var cmdPrepare = &cobra.Command{
		Use:   "prepare",
		Short: "subcommands to help you get ready for a gp_upgrade",
		Long:  "subcommands to help you get ready for a gp_upgrade",
	}

	var cmdPrepareSubStartHub = &cobra.Command{
		Use:   "start-hub",
		Short: "starts the hub",
		Long:  "starts the hub",
		RunE: func(cmd *cobra.Command, args []string) error {
			gpbackupUtils.InitializeLogging("gp_upgrade_cli", "")
			preparer := commanders.Preparer{}
			return preparer.StartHub()
		},
	}

	var cmdStatus = &cobra.Command{
		Use:   "status",
		Short: "subcommands to show the status of a gp_upgrade",
		Long:  "subcommands to show the status of a gp_upgrade",
	}

	var cmdStatusSubUpgrade = &cobra.Command{
		Use:   "upgrade",
		Short: "the status of the upgrade",
		Long:  "the status of the upgrade",
		Run: func(cmd *cobra.Command, args []string) {
			gpbackupUtils.InitializeLogging("gp_upgrade_cli", "")

			conn, connConfigErr := grpc.Dial("localhost:"+hubPort, grpc.WithInsecure())
			if connConfigErr != nil {
				fmt.Println(connConfigErr)
				os.Exit(1)
			}
			client := pb.NewCliToHubClient(conn)
			reporter := commanders.NewReporter(client)
			err := reporter.OverallUpgradeStatus()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

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
	}

	cmdCheck.PersistentFlags().StringVar(&masterHost, "master-host", "", "host IP for master")
	cmdCheck.PersistentFlags().IntVar(&dbPort, "port", 15432, "port for Greenplum on master")
	cmdCheck.MarkFlagRequired("master-host")

	var cmdCheckSubCheckVersionCommand = &cobra.Command{
		Use:     "version",
		Short:   "validate current version is upgradable",
		Long:    `validate current version is upgradable`,
		Aliases: []string{"ver"},
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
				grpc.WithInsecure())
			if connConfigErr != nil {
				fmt.Println(connConfigErr)
				os.Exit(1)
			}
			client := pb.NewCliToHubClient(conn)
			err := commanders.NewVersionChecker(client).Execute(masterHost, dbPort)
			return err
		},
	}

	var cmdCheckSubObjectCountCommand = &cobra.Command{
		Use:     "object-count",
		Short:   "count database objects and numeric objects",
		Long:    "count database objects and numeric objects",
		Aliases: []string{"oc"},
		RunE: func(cmd *cobra.Command, args []string) error {
			conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
				grpc.WithInsecure())
			if connConfigErr != nil {
				fmt.Println(connConfigErr)
				os.Exit(1)
			}
			client := pb.NewCliToHubClient(conn)
			commanders.NewObjectCountChecker(client).Execute(dbPort)
			//ocCommand := commands.NewObjectCountCommand(masterHost, dbPort, commands.RealDbConnectionFactory{})
			//return ocCommand.Execute(args)
			return nil
		},
	}

	var cmdCheckSubDiskSpaceCommand = &cobra.Command{
		Use:     "disk-space",
		Short:   "check that disk space usage is less than 80% on all segments",
		Long:    "check that disk space usage is less than 80% on all segments",
		Aliases: []string{"du"},
		Run: func(cmd *cobra.Command, args []string) {
			conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
				grpc.WithInsecure())
			if connConfigErr != nil {
				fmt.Println(connConfigErr)
				os.Exit(1)
			}
			client := pb.NewCliToHubClient(conn)
			commanders.NewDiskUsageChecker(client).Execute(dbPort)
		},
	}

	var cmdCheckSubConfigCommand = &cobra.Command{
		Use:   "config",
		Short: "gather cluster configuration",
		Long:  "gather cluster configuration",
		Run: func(cmd *cobra.Command, args []string) {
			conn, connConfigErr := grpc.Dial("localhost:"+hubPort,
				grpc.WithInsecure())
			if connConfigErr != nil {
				fmt.Println(connConfigErr)
				os.Exit(1)
			}
			client := pb.NewCliToHubClient(conn)
			err := commanders.NewConfigChecker(client).Execute(dbPort)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
	}

	var cmdVersion = &cobra.Command{
		Use:   "version",
		Short: "Version of gp_upgrade",
		Long:  `Version of gp_upgrade`,
		Run: func(cmd *cobra.Command, args []string) {
			commands.VersionCommand{}.Execute(args)
		},
	}

	var rootCmd = &cobra.Command{Use: "gp_upgrade"}

	//TODO this could be improved.
	// Also, if another command is added, the message will need to be updated.
	if len(os.Args[1:]) < 1 {
		log.Fatal("Please specify one command of: prepare, check, status or version")
	}
	// all root level
	rootCmd.AddCommand(cmdPrepare, cmdStatus, cmdCheck, cmdVersion)

	// prepare subcommmands
	cmdPrepare.AddCommand(cmdPrepareSubStartHub)

	// status subcommands
	cmdStatus.AddCommand(cmdStatusSubUpgrade)

	// check subcommands
	cmdCheck.AddCommand(cmdCheckSubCheckVersionCommand)
	cmdCheck.AddCommand(cmdCheckSubObjectCountCommand)
	cmdCheck.AddCommand(cmdCheckSubDiskSpaceCommand)
	cmdCheck.AddCommand(cmdCheckSubConfigCommand)

	//TODO if give a subcommand that doesn't exist, we should give the user feedback

	err := rootCmd.Execute()
	if err != nil {
		// Use v to print the stack trace of an object errors.
		fmt.Printf("%+v\n", err)
		os.Exit(1)
	}
}
