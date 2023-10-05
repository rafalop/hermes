package common

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
	"golang.org/x/sys/cpu"
)

const EnvFile = "hermes.env"

func LoadEnv(metaDir string) error {
	envPath := filepath.Join(metaDir, EnvFile)

	err := godotenv.Load(envPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func Contains[T comparable](slices []T, val T) bool {
	for _, _val := range slices {
		if val == _val {
			return true
		}
	}
	return false
}

func NativeEndian() binary.ByteOrder {
	endians := map[bool]binary.ByteOrder{}
	endians[true] = binary.BigEndian
	endians[false] = binary.LittleEndian
	return endians[cpu.IsBigEndian]

	// this also works but requires "unsafe" module
	//var bigEndian = (*(*[2]uint8)(unsafe.Pointer(&[]uint16{1}[0])))[0] == 0
	//return endians[bigEndian]
}
