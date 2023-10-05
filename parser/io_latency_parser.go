package parser

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	iolat "hermes/backend/ebpf/io_latency"
	"hermes/log"
)

type IoLatParser struct{}

type RawBlkLatRec iolat.BlkLatRec

// microsecond latencies
//const (
//	Bucket1 = 1000 // 1ms
//	Bucket2 = 10000 // 10ms
//	Bucket3 = 100000 // 100ms
//	Bucket4 = 200000 // 200ms
//	Bucket5 = 1000000 // high
//)
//
//type LatencyBucket struct {
//	Count int `json:"count"`
//	LatencyRange	string `json:"latency_range"`
//}

//func GetLatBucket(lat uint64) int {
//	if lat < Bucket1 {
//		return Bucket1
//	} else if lat < Bucket2 {
//		return Bucket2
//	} else if lat < Bucket3 {
//		return Bucket3
//	} else if lat < Bucket4 {
//		return Bucket4
//	} else {
//		return Bucket5
//	}
//}
//

type BlkLatRecord struct {
	TotalIos   int    `json:"total_ios"`
	Reads      int    `json:"reads"`
	SyncReads  int    `json:"sync_reads"`
	Writes     int    `json:"writes"`
	SyncWrites int    `json:"sync_writes"`
	Other      int    `json:"other"`
	SyncOther  int    `json:"sync_other"`
	LatAvgUs   uint64 `json:"lat_avg_us"`
	LatHighUs  uint64 `json:"lat_high_us"`
	LatLowUs   uint64 `json:"lat_low_us"`
	// latency buckets count how many requests fell into each
	// one is 0-10ms, two is 10-100,
	//Bucket1 LatencyBucket
	//Bucket2 LatencyBucket
	//Bucket3 LatencyBucket
	//Bucket4 LatencyBucket
	//Bucket5 LatencyBucket
}

type PidBlkLatRecord struct {
	Comm   string       `json:"comm"`
	BlkLat BlkLatRecord `json:"blk_lat"`
}

func GetOutputBlkData(rawRecs []RawBlkLatRec) OutputBlkData {
	all := BlkLatRecord{}
	perPid := map[uint32]PidBlkLatRecord{}
	perDev := map[string]BlkLatRecord{}

	var allSum uint64
	pidSum := map[uint32]uint64{}
	devSum := map[string]uint64{}

	for i, rec := range rawRecs {
		pid := rec.Pid
		pidRec := perPid[pid]
		pidBlkRec := pidRec.BlkLat

		dev := rec.Device
		devBlkRec := perDev[dev]

		// set comm for per pid and do sums for averages
		if pidBlkRec.TotalIos == 0 {
			pidRec.Comm = rec.Comm
		}
		allSum += rec.LatUs
		pidSum[pid] += rec.LatUs
		devSum[dev] += rec.LatUs

		// update high
		if rec.LatUs > all.LatHighUs || i == 0 {
			all.LatHighUs = rec.LatUs
		}
		if rec.LatUs > pidBlkRec.LatHighUs || pidBlkRec.TotalIos == 0 {
			pidBlkRec.LatHighUs = rec.LatUs
		}
		if rec.LatUs > devBlkRec.LatHighUs || devBlkRec.TotalIos == 0 {
			devBlkRec.LatHighUs = rec.LatUs
		}

		// update low
		if rec.LatUs < all.LatLowUs || i == 0 {
			all.LatLowUs = rec.LatUs
		}
		if rec.LatUs < pidBlkRec.LatLowUs || pidBlkRec.TotalIos == 0 {
			pidBlkRec.LatLowUs = rec.LatUs
		}
		if rec.LatUs < devBlkRec.LatLowUs || devBlkRec.TotalIos == 0 {
			devBlkRec.LatLowUs = rec.LatUs
		}

		// update counts
		if rec.OpInfo.Op == "read" {
			if rec.OpInfo.Sync == true {
				all.SyncReads += 1
				pidBlkRec.SyncReads += 1
				devBlkRec.SyncReads += 1
			} else {
				all.Reads += 1
				pidBlkRec.Reads += 1
				devBlkRec.Reads += 1
			}
		}

		if rec.OpInfo.Op == "write" {
			if rec.OpInfo.Sync == true {
				all.SyncWrites += 1
				pidBlkRec.SyncWrites += 1
				devBlkRec.SyncWrites += 1
			} else {
				all.Writes += 1
				pidBlkRec.Writes += 1
				devBlkRec.Writes += 1
			}
		}

		if rec.OpInfo.Op == "other" {
			if rec.OpInfo.Sync == true {
				all.SyncOther += 1
				pidBlkRec.SyncOther += 1
				devBlkRec.SyncOther += 1
			} else {
				all.Other += 1
				pidBlkRec.Other += 1
				devBlkRec.Other += 1
			}
		}
		all.TotalIos += 1
		pidBlkRec.TotalIos += 1
		devBlkRec.TotalIos += 1

		// update records
		pidRec.BlkLat = pidBlkRec
		perPid[pid] = pidRec

		perDev[dev] = devBlkRec

	}

	// calculate averages
	all.LatAvgUs = allSum / uint64(all.TotalIos)
	for k, v := range pidSum {
		pidRec := perPid[k]
		pidBlkRec := pidRec.BlkLat
		pidBlkRec.LatAvgUs = v / uint64(pidBlkRec.TotalIos)
		pidRec.BlkLat = pidBlkRec
		perPid[k] = pidRec
	}
	for k, v := range devSum {
		devBlkRec := perDev[k]
		devBlkRec.LatAvgUs = v / uint64(devBlkRec.TotalIos)
		perDev[k] = devBlkRec
	}

	return OutputBlkData{all, perPid, perDev}
}

type OutputBlkData struct {
	AllIo  BlkLatRecord               `json:"all"`
	PerPid map[uint32]PidBlkLatRecord `json:"per_pid"`
	PerDev map[string]BlkLatRecord    `json:"per_dev"`
}

func GetIoLatEbpfParser() (ParserInstance, error) {
	return &IoLatParser{}, nil
}

// get raw records created by collector
func (p *IoLatParser) getRawBlkRecord(timestamp int64, path string) ([]RawBlkLatRec, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rawRecords []RawBlkLatRec
	if err := json.Unmarshal(bytes, &rawRecords); err != nil {
		return nil, err
	}
	return rawRecords, nil
}

func (p *IoLatParser) writeJSONDataBlk(rec OutputBlkData, path string) error {
	bytes, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, bytes, 0644)
}

func (p *IoLatParser) Parse(logDataPathGenerator log.LogDataPathGenerator, timestamp int64, logDataPostfix, outputDir string) error {
	var rawRecs []RawBlkLatRec

	rawRecs, err := p.getRawBlkRecord(timestamp, logDataPathGenerator(logDataPostfix))
	if err != nil {
		return err
	}
	outputBlkData := GetOutputBlkData(rawRecs)

	outputBlkPath := filepath.Join(outputDir, strconv.FormatInt(timestamp, 10), "blk_ios.json")
	if err := os.MkdirAll(filepath.Dir(outputBlkPath), os.ModePerm); err != nil {
		return err
	}
	err = p.writeJSONDataBlk(outputBlkData, outputBlkPath)
	if err != nil {
		return err
	}

	return nil
}
