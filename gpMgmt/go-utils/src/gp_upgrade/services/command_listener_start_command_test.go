package services

import (
	"fmt"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gp_upgrade/idl"
	"gp_upgrade/testUtils"
	"io"
	"io/ioutil"
	"os"
)

var _ = Describe("CommandListenerStartCommand", func() {
	var (
		subject CommandListenerStartCommand
	)

	BeforeEach(func() {
		subject = CommandListenerStartCommand{LogDir: "/tmp"}
	})

	AfterEach(func() {
	})

	Describe("logdir", func() {
		It("log directory is set", func() {
			utils.InitializeLogging("command_listener", "/tmp")
			logDirPath := utils.GetLogger().GetLogFilePath()
			os.Remove(logDirPath)

			server, errorChannel, err := subject.execute(nil)
			// IMPORTANT: this stop call causes error logging, like
			// "failed to serve: accept tcp [::]:xxx: use of closed network connection'
			// just ignore this.  We want to preserve logging for a real error, and in real usage, we
			// do not call Stop()
			defer server.Stop()

			Expect(err).ToNot(HaveOccurred())
			clsClient, connCloser := establishClient(fmt.Sprintf("localhost%v", port))
			defer connCloser.Close()
			request := idl.TransmitStateRequest{"transmit request"}
			_, err = clsClient.TransmitState(context.Background(), &request)
			Expect(err).ToNot(HaveOccurred())
			Consistently(errorChannel).ShouldNot(Receive())

			dat, err := ioutil.ReadFile(utils.GetLogger().GetLogFilePath())
			testUtils.Check("failed to read file", err)
			Expect(string(dat)).To(ContainSubstring("Starting Command Listener"))
		})
	})

})

var establishClient = func(clsAddr string) (idl.CommandListenerClient, io.Closer) {
	conn, err := grpc.Dial(clsAddr, grpc.WithInsecure())
	Expect(err).ToNot(HaveOccurred())
	client := idl.NewCommandListenerClient(conn)

	return client, conn
}
