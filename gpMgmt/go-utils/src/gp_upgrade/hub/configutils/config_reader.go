package configutils

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
)

type Reader struct {
	config SegmentConfiguration
}

func (reader *Reader) Read() error {
	contents, err := ioutil.ReadFile(GetConfigFilePath())
	if err != nil {
		return errors.New(err.Error())
	}
	err = json.Unmarshal([]byte(contents), &reader.config)
	if err != nil {
		return errors.New(err.Error())
	}
	return nil
}

// returns -1 for not found
func (reader Reader) GetPortForSegment(segmentDbid int) int {
	result := -1
	for i := 0; i < len(reader.config); i++ {
		segment := reader.config[i]
		if segment.DBID == segmentDbid {
			result = segment.Port
			break
		}
	}

	return result
}

func (reader Reader) GetHostnames() []string {
	if len(reader.config) == 0 {
		reader.Read()
	}
	hostnamesSeen := make(map[string]bool)
	for i := 0; i < len(reader.config); i++ {
		_, contained := hostnamesSeen[reader.config[i].Hostname]
		if !contained {
			hostnamesSeen[reader.config[i].Hostname] = true
		}
	}
	var hostnames []string
	for k := range hostnamesSeen {
		hostnames = append(hostnames, k)
	}
	return hostnames
}

func (reader Reader) GetSegmentConfiguration() SegmentConfiguration {
	return reader.config
}
