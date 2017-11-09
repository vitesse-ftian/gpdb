package services

import (
	pb "gp_upgrade/idl"
)

func NewCliToHubListener() pb.CliToHubServer {
	return &cliToHubListenerImpl{}
}

type cliToHubListenerImpl struct{}
