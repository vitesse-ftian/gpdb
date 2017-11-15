package commands

import (
	"context"
	"fmt"

	"gp_upgrade/hub/configutils"
	pb "gp_upgrade/idl"
	"io"
)

type Hub struct{}

func (h Hub) CheckDiskUsage(clients []configutils.ClientAndHostname, writer io.Writer) {

	//var diskUsageResults []string

	for i := 0; i < len(clients); i++ {
		reply, err := clients[i].Client.CheckDiskUsage(context.Background(), &pb.CheckDiskUsageRequest{})
		if err != nil {
			fmt.Fprint(writer, fmt.Sprintf("diskspace check - WARNING - unable to connect to %s\n", clients[i].Hostname))
		} else {
			noIssuesYet := true
			for _, line := range reply.ListOfFileSysUsage {
				if line.Usage >= 80 {
					writer.Write([]byte(fmt.Sprintf(`diskspace check - %s - WARNING %s %.f%% use\n`,
						clients[i].Hostname, line.Filesystem, line.Usage)))
					noIssuesYet = false
				}
			}
			if noIssuesYet {
				writer.Write([]byte("gp_upgrade: Disk Usage Check [OK]\n"))
			}
		}
		// TODO: do we need to close the connection (which at this point is internal to the Client)?
	}
}
