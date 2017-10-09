package commands

import (
	"fmt"
	"io"
	"os"

	"gp_upgrade/config"
	"gp_upgrade/shellParsers"
	"gp_upgrade/sshClient"

	"github.com/pkg/errors"
	"gp_upgrade/utils"
)

type MonitorCommand struct {
	Host       string
	Port       int
	User       string
	PrivateKey string
	SegmentID  int
}

func (cmd MonitorCommand) Execute([]string) error {
	connector, err := sshClient.NewSSHConnector(cmd.PrivateKey)
	if err != nil {
		return errors.New(err.Error())
	}
	return cmd.execute(connector, &shellParsers.RealShellParser{}, os.Stdout)
}

func (cmd MonitorCommand) execute(connector sshClient.SSHConnector, shellParser shellParsers.ShellParser, writer io.Writer) error {
	targetPort, err := readConfigForSegmentPort(cmd.SegmentID)
	if err != nil {
		return errors.New(err.Error())
	}

	user := cmd.User
	if user == "" {
		user, _, _ = utils.GetUser() // todo last arg is for error--bubble up that error here? with what message?
	}

	output, err := connector.ConnectAndExecute(cmd.Host, cmd.Port, user, "ps auxx | grep pg_upgrade")
	if err != nil {
		return errors.New(err.Error())
	}

	status := "active"

	if !shellParser.IsPgUpgradeRunning(targetPort, output) {
		status = "inactive"
	}
	msg := fmt.Sprintf(`pg_upgrade state - %s {"segment_id":%d,"host":"%s"}`, status, cmd.SegmentID, cmd.Host)
	fmt.Fprintf(writer, "%s\n", msg)

	return nil
}

func readConfigForSegmentPort(segmentID int) (int, error) {
	var err error
	reader := config.Reader{}
	err = reader.Read()
	if err != nil {
		return -1, errors.New(err.Error())
	}
	targetPort := reader.GetPortForSegment(segmentID)
	if targetPort == -1 {
		return -1, fmt.Errorf("segment_id %d not known in this cluster configuration", segmentID)
	}

	return targetPort, nil
}
