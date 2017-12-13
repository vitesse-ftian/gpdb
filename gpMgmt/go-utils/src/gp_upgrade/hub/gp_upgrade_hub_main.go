package main

import (
	"gp_upgrade/hub/services"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "gp_upgrade/idl"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	"github.com/spf13/cobra"
	"os"
	"runtime/debug"
)

const (
	cliToHubPort = ":7527"
)

// This directory to have the implementation code for the gRPC server to serve
// Minimal CLI command parsing to embrace that booting this binary to run the hub might have some flags like a log dir

func main() {
	var logdir string
	var RootCmd = &cobra.Command{
		Use:   "gp_upgrade_hub [--log-directory path]",
		Short: "Start the gp_upgrade_hub (blocks)",
		Long:  `Start the gp_upgrade_hub (blocks)`,
		RunE: func(cmd *cobra.Command, args []string) error {
			debug.SetTraceback("all")
			gpbackupUtils.InitializeLogging("gp_upgrade_hub", logdir)
			logger := gpbackupUtils.GetLogger()
			errorChannel := make(chan error)
			defer close(errorChannel)
			lis, err := net.Listen("tcp", cliToHubPort)
			if err != nil {
				logger.Fatal(err, "failed to listen")
			}

			server := grpc.NewServer()
			myImpl := services.NewCliToHubListener()
			pb.RegisterCliToHubServer(server, myImpl)
			reflection.Register(server)
			go func(myListener net.Listener) {
				if err := server.Serve(myListener); err != nil {
					logger.Fatal(err, "failed to serve", err)
					errorChannel <- err
				}

				close(errorChannel)
			}(lis)

			select {
			case err := <-errorChannel:
				if err != nil {
					logger.Fatal(err, "error during Listening")
				}
			}
			return nil
		},
	}

	RootCmd.PersistentFlags().StringVar(&logdir, "log-directory", "", "gp_upgrade_hub log directory")

	if err := RootCmd.Execute(); err != nil {
		gpbackupUtils.GetLogger().Error(err.Error())
		os.Exit(1)
	}

}
