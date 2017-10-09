package sshClient

import (
	"fmt"

	"io/ioutil"

	"net"

	"github.com/pkg/errors"
	"io"

	"golang.org/x/crypto/ssh"
)

type SSHConnector interface {
	ConnectAndExecute(host string, port int, user string, command string) (string, error)
	Connect(Host string, Port int, user string) (SSHSession, error)
}

type SSHSession interface {
	Output(cmd string) ([]byte, error)
	Close() error
}

type RealSSHConnector struct {
	SSHDialer      Dialer
	SSHKeyParser   KeyParser
	PrivateKeyPath string
}

func NewSSHConnector(privateKeyPath string) (SSHConnector, error) {
	privateKey, err := NewPrivateKeyGuarantor().Check(privateKeyPath)
	if err != nil {
		return nil, errors.New(err.Error())
	}

	return &RealSSHConnector{
		SSHKeyParser:   RealKeyParser{},
		SSHDialer:      RealDialer{},
		PrivateKeyPath: privateKey,
	}, nil
}

func (ssh_connector *RealSSHConnector) ConnectAndExecute(host string, port int, user string, command string) (string, error) {
	session, err := ssh_connector.Connect(host, port, user)
	if err != nil {
		return "", errors.New(err.Error())
	}

	// pgrep could be used, but it was messy because of exit code 1 when not found;
	// seems nicer with ps to have 0 exit when not found (but not error)
	outputBytes, err := session.Output(command)
	output := string(outputBytes)
	session.Close() // we just ignore any error from Close() if we had a successful output already

	if err != nil && err != io.EOF {
		msg := fmt.Sprintf("cannot run '%s' command on remote host, output: %s \nError: %s", command, output, err.Error())
		return "", errors.New(msg)
	}

	return output, nil
}

func (ssh_connector *RealSSHConnector) Connect(Host string, Port int, user string) (SSHSession, error) {
	pemBytes, err := ioutil.ReadFile(ssh_connector.PrivateKeyPath)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	signer, err := ssh_connector.SSHKeyParser.ParsePrivateKey(pemBytes)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil },
	}
	hostAndPort := fmt.Sprintf("%s:%v", Host, Port)
	client, err := ssh_connector.SSHDialer.Dial("tcp", hostAndPort, config)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	session, err := client.NewSession()
	if err != nil {
		return nil, errors.New(err.Error())
	}
	return session, nil
}

type RealKeyParser struct{}

func (parser RealKeyParser) ParsePrivateKey(pemBytes []byte) (ssh.Signer, error) {
	return ssh.ParsePrivateKey(pemBytes)
}
