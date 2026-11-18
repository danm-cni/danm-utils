package v43to44

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	CniTargetDir  = "/host/cni"
	CniMarkerFile = "/tmp/dma-cni-done"
	cniTempPrefix = "_"
)

type CniCopyConfig struct {
	Payload string
	Target  string
	Suffix  string
	Marker  string
	Restore bool
	Cleanup bool
	Wait    bool
}

func CopyCniBinaries(ctx context.Context, config CniCopyConfig, out io.Writer) error {
	if config.Target == "" {
		config.Target = CniTargetDir
	}
	if config.Marker == "" {
		config.Marker = CniMarkerFile
	}
	if config.Suffix == "" {
		return fmt.Errorf("no backup suffix is configured")
	}
	for _, binary := range PayloadBinaries {
		var err error
		switch {
		case config.Restore:
			err = restoreBinary(config, binary, out)
		case config.Cleanup:
			err = removeBackup(config, binary, out)
		default:
			err = installBinary(config, binary, out)
		}
		if err != nil {
			return err
		}
	}
	if err := os.WriteFile(config.Marker, []byte("done\n"), 0644); err != nil {
		return fmt.Errorf("cannot write the marker file %s: %w", config.Marker, err)
	}
	if !config.Wait {
		return nil
	}
	<-ctx.Done()
	return nil
}

func installBinary(config CniCopyConfig, binary string, out io.Writer) error {
	source := filepath.Join(config.Payload, binary)
	live := filepath.Join(config.Target, binary)
	backup := live + config.Suffix
	if _, err := os.Stat(backup); os.IsNotExist(err) {
		if err := copyFile(live, backup); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("cannot back up %s: %w", live, err)
		}
		if err == nil {
			fmt.Fprintf(out, "backed up %s as %s\n", live, backup)
		}
	} else if err != nil {
		return fmt.Errorf("cannot read the backup %s: %w", backup, err)
	}
	staged := filepath.Join(config.Target, cniTempPrefix+binary)
	if err := copyFile(source, staged); err != nil {
		return fmt.Errorf("cannot write %s: %w", staged, err)
	}
	if err := os.Rename(staged, live); err != nil {
		return fmt.Errorf("cannot move %s into place as %s: %w", staged, live, err)
	}
	fmt.Fprintf(out, "installed %s\n", live)
	return nil
}

func restoreBinary(config CniCopyConfig, binary string, out io.Writer) error {
	live := filepath.Join(config.Target, binary)
	backup := live + config.Suffix
	if _, err := os.Stat(backup); os.IsNotExist(err) {
		fmt.Fprintf(out, "no backup of %s to restore\n", live)
		return nil
	} else if err != nil {
		return fmt.Errorf("cannot read the backup %s: %w", backup, err)
	}
	staged := filepath.Join(config.Target, cniTempPrefix+binary)
	if err := copyFile(backup, staged); err != nil {
		return fmt.Errorf("cannot write %s: %w", staged, err)
	}
	if err := os.Rename(staged, live); err != nil {
		return fmt.Errorf("cannot move %s into place as %s: %w", staged, live, err)
	}
	if err := os.Remove(backup); err != nil {
		return fmt.Errorf("cannot delete the backup %s: %w", backup, err)
	}
	fmt.Fprintf(out, "restored %s\n", live)
	return nil
}

func removeBackup(config CniCopyConfig, binary string, out io.Writer) error {
	backup := filepath.Join(config.Target, binary) + config.Suffix
	if err := os.Remove(backup); err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(out, "no backup of %s to delete\n", binary)
			return nil
		}
		return fmt.Errorf("cannot delete the backup %s: %w", backup, err)
	}
	fmt.Fprintf(out, "deleted %s\n", backup)
	return nil
}

func copyFile(source, target string) error {
	reader, err := os.Open(source)
	if err != nil {
		return err
	}
	defer reader.Close()
	info, err := reader.Stat()
	if err != nil {
		return err
	}
	writer, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(writer, reader); err != nil {
		writer.Close()
		return err
	}
	if err := writer.Sync(); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}
