package main

import (
	"fmt"
	"gp_upgrade/cli/commanders"
	"gp_upgrade/commands"
	"gp_upgrade/hub/configutils"
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
	var host string
	var dbPort int
	var sshPort int
	var segmentID int
	var privateKey string
	var user string

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
			return commands.CheckVersionCommand{
				MasterHost: masterHost,
				MasterPort: int(dbPort),
			}.Execute(args)
		},
	}

	var cmdCheckSubObjectCountCommand = &cobra.Command{
		Use:     "object-count",
		Short:   "count database objects and numeric objects",
		Long:    "count database objects and numeric objects",
		Aliases: []string{"oc"},
		RunE: func(cmd *cobra.Command, args []string) error {
			ocCommand := commands.NewObjectCountCommand(masterHost, dbPort, commands.RealDbConnectionFactory{})
			return ocCommand.Execute(args)
		},
	}

	var cmdCheckSubDiskSpaceCommand = &cobra.Command{
		Use:     "disk-space",
		Short:   "check that disk space usage is less than 80% on all segments",
		Long:    "check that disk space usage is less than 80% on all segments",
		Aliases: []string{"du"},
		Run: func(cmd *cobra.Command, args []string) {
			clients := configutils.RPCClients{}.GetRPCClients()
			hub := commands.Hub{}
			hub.CheckDiskUsage(clients, os.Stdout)
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

	var cmdMonitor = &cobra.Command{
		Use:   "monitor",
		Short: "Monitor Greenplum upgrade process",
		Long:  `Monitor Greenplum upgrade process`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			//TODO report both erros at once
			if host == "" {
				return errors.New("the required flag '--host' was not specified")
			}
			if segmentID == -1 {
				return errors.New("the required flag '--segment-id' was not specified")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return commands.MonitorCommand{Host: host,
				Port:       sshPort,
				User:       user,
				PrivateKey: privateKey,
				SegmentID:  segmentID}.Execute(args)
		},
	}

	cmdMonitor.PersistentFlags().StringVar(&host, "host", "", "Domain name or IP of host")
	cmdMonitor.PersistentFlags().IntVar(&segmentID, "segment-id", -1, "ID of segment to monitor")
	cmdMonitor.PersistentFlags().IntVar(&sshPort, "port", 22, "SSH port for communication")
	cmdMonitor.PersistentFlags().StringVar(&privateKey, "private-key", "", "Private key for ssh destination")
	cmdMonitor.PersistentFlags().StringVar(&user, "user", "", "Name of user at ssh destination")

	var rootCmd = &cobra.Command{Use: "gp_upgrade"}

	//TODO this could be improved.
	// Also, if another command is added, the message will need to be updated.
	if len(os.Args[1:]) < 1 {
		log.Fatal("Please specify one command of: prepare, check, monitor, or version")
	}
	// all root level
	rootCmd.AddCommand(cmdPrepare, cmdStatus, cmdCheck, cmdVersion,
		cmdMonitor)

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
