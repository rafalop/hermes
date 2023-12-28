package parser

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"

	"hermes/backend/perf"
	"hermes/log"
)

type CpuProfileParser struct{}

func GetCpuProfileParser() (ParserInstance, error) {
	return &CpuProfileParser{}, nil
}

func (parser *CpuProfileParser) parseStackCollapsedData(logPath string, recordHandler *perf.RecordHandler) error {
	fp, err := os.Open(logPath)
	if err != nil {
		return err
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)

	for scanner.Scan() {
		bytes := scanner.Bytes()
		if err := recordHandler.Parse(bytes); err != nil {
			return err
		}
	}
	return nil
}

func (parser *CpuProfileParser) Parse(logPathManager log.LogPathManager, timestamp int64, logDataPostfix, outputDir string) error {
	recordHandler := perf.GetRecordHandler()
	matches, err := filepath.Glob(logPathManager.DataPath(logDataPostfix))
	if err != nil {
		return err
	}

	for _, filePath := range matches {
		if err := parser.parseStackCollapsedData(filePath, recordHandler); err != nil {
			return err
		}
	}

	outputPath := filepath.Join(outputDir, strconv.FormatInt(timestamp, 10), "overall_cpu.stack.json")
	if err := os.MkdirAll(filepath.Dir(outputPath), os.ModePerm); err != nil {
		return err
	}
	return recordHandler.GetFlameGraphData().WriteToFile(outputPath)
}
