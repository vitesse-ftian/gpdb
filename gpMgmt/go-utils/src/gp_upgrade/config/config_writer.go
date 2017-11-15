package config

import (
	"os"

	"encoding/json"
	"github.com/pkg/errors"
	"gp_upgrade/utils"
)

type Store interface {
	Load(rows utils.RowsWrapper) error
	Write() error
}

type Writer struct {
	TableJSONData []map[string]interface{}
	Formatter     Formatter
	FileWriter    FileWriter
	PathToFile    string
}

func NewWriter(PathToFile string) *Writer {
	return &Writer{
		Formatter:  NewJSONFormatter(),
		FileWriter: NewRealFileWriter(),
		PathToFile: PathToFile,
	}
}

func (configWriter *Writer) Load(rows utils.RowsWrapper) error {
	var err error
	configWriter.TableJSONData, err = translateColumnsIntoGenericStructure(rows)
	return err
}

func (configWriter *Writer) Write() error {
	jsonData, err := json.Marshal(configWriter.TableJSONData)
	if err != nil {
		return errors.New(err.Error())
	}

	pretty, err := configWriter.Formatter.Format(jsonData)
	if err != nil {
		return errors.New(err.Error())
	}

	err = os.MkdirAll(GetConfigDir(), 0700)
	if err != nil {
		return errors.New(err.Error())
	}
	f, err := os.Create(configWriter.PathToFile)
	if err != nil {
		return errors.New(err.Error())
	}
	defer f.Close()

	err = configWriter.FileWriter.Write(f, pretty)
	if err != nil {
		return errors.New(err.Error())
	}
	return nil
}

func translateColumnsIntoGenericStructure(rows utils.RowsWrapper) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.New(err.Error())
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, errors.New(err.Error())
		}
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}

	return tableData, nil
}
