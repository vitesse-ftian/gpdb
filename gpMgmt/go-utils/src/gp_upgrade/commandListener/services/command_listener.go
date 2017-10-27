//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/idl.proto

package services

import (
	"github.com/cloudfoundry/gosigar"
	"golang.org/x/net/context"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
)

type commandListenerImpl struct {
	getDiskUsage func() (map[string]float64, error)
}

func NewCommandListener() pb.CommandListenerServer {
	return &commandListenerImpl{diskUsage}
}

func (s *commandListenerImpl) CheckUpgradeStatus(ctx context.Context, in *pb.CheckUpgradeStatusRequest) (*pb.CheckUpgradeStatusReply, error) {
	cmd := "ps auxx | grep pg_upgrade"

	output, err := utils.System.ExecCmdOutput("bash", "-c", cmd)
	if err != nil {
		return nil, err
	}
	return &pb.CheckUpgradeStatusReply{ProcessList: string(output)}, nil
}

func (s *commandListenerImpl) CheckDiskUsage(ctx context.Context, in *pb.CheckDiskUsageRequest) (*pb.CheckDiskUsageReply, error) {
	diskUsage, err := s.getDiskUsage()
	if err != nil {
		return nil, err
	}
	var listDiskUsages []*pb.FileSysUsage
	for k, v := range diskUsage {
		listDiskUsages = append(listDiskUsages, &pb.FileSysUsage{Filesystem: k, Usage: v})
	}
	return &pb.CheckDiskUsageReply{ListOfFileSysUsage: listDiskUsages}, nil
}

// diskUsage() wraps a pair of calls to the gosigar library.
// This is local repetition of the sys_utils function pointer pattern. If there was more than one of these,
// we would've refactored.
// "Adapted" from the gosigar usage example at https://github.com/cloudfoundry/gosigar/blob/master/examples/df.go
func diskUsage() (map[string]float64, error) {
	diskUsagePerFS := make(map[string]float64)
	fslist := sigar.FileSystemList{}
	err := fslist.Get()
	if err != nil {
		return nil, err
	}

	for _, fs := range fslist.List {
		dirName := fs.DirName

		usage := sigar.FileSystemUsage{}

		err = usage.Get(dirName)
		if err != nil {
			return nil, err
		}

		diskUsagePerFS[dirName] = usage.UsePercent()
	}
	return diskUsagePerFS, nil
}
