package cluster

import (
	"debug/elf"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

func InspectPayload(dir string, binaries []string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("no CNI binary payload directory is configured")
	}
	architecture := ""
	for _, binary := range binaries {
		path := filepath.Join(dir, binary)
		info, err := os.Stat(path)
		if err != nil {
			return "", fmt.Errorf("CNI binary payload %s is incomplete: %w", dir, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			return "", fmt.Errorf("CNI binary %s is not an executable file", path)
		}
		binaryArchitecture, err := elfArchitecture(path)
		if err != nil {
			return "", err
		}
		if architecture != "" && binaryArchitecture != architecture {
			return "", fmt.Errorf("CNI binary payload %s mixes architectures: %s is built for %s, the rest for %s", dir, path, binaryArchitecture, architecture)
		}
		architecture = binaryArchitecture
	}
	return architecture, nil
}

func elfArchitecture(path string) (string, error) {
	file, err := elf.Open(path)
	if err != nil {
		return "", fmt.Errorf("CNI binary %s is not an ELF executable: %w", path, err)
	}
	defer file.Close()
	switch file.Machine {
	case elf.EM_X86_64:
		return "amd64", nil
	case elf.EM_AARCH64:
		return "arm64", nil
	case elf.EM_386:
		return "386", nil
	case elf.EM_ARM:
		return "arm", nil
	case elf.EM_S390:
		return "s390x", nil
	case elf.EM_RISCV:
		return "riscv64", nil
	case elf.EM_PPC64:
		if file.ByteOrder == binary.LittleEndian {
			return "ppc64le", nil
		}
		return "ppc64", nil
	}
	return "", fmt.Errorf("CNI binary %s is built for the unsupported machine type %s", path, file.Machine)
}
