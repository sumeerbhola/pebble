// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
)

func loadVersion2(d *datadriven.TestData) (*version, *Options, string) {
	opts := &Options{}
	opts.EnsureDefaults()

	if len(d.CmdArgs) != 1 {
		return nil, nil, fmt.Sprintf("%s expects 1 argument", d.Cmd)
	}
	var err error
	opts.LBaseMaxBytes, err = strconv.ParseInt(d.CmdArgs[0].Key, 10, 64)
	if err != nil {
		return nil, nil, err.Error()
	}

	vers := &version{}
	if len(d.Input) > 0 {
		for _, data := range strings.Split(d.Input, "\n") {
			parts := strings.Split(data, ":")
			if len(parts) != 2 {
				return nil, nil, fmt.Sprintf("malformed test:\n%s", d.Input)
			}
			level, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, nil, err.Error()
			}
			if vers.Files[level] != nil {
				return nil, nil, fmt.Sprintf("level %d already filled", level)
			}
			size, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
			if err != nil {
				return nil, nil, err.Error()
			}
			if level == 0 {
				for i := uint64(0); i < size; i++ {
					vers.Files[level] = append(vers.Files[level], fileMetadata{
						Size: 1,
					})
				}
			} else {
				vers.Files[level] = append(vers.Files[level], fileMetadata{
					Size: size,
				})
			}
		}
	}

	return vers, opts, ""
}

func TestCompactionPicker2LevelMaxBytes(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_level_max_bytes2",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, opts, errMsg := loadVersion2(d)
				if errMsg != "" {
					return errMsg
				}

				p := newCompactionPicker2(vers, opts, nil)
				var buf bytes.Buffer
				for level := p.currBaseLevel; level < numLevels; level++ {
					fmt.Fprintf(&buf, "%d: %d\n", level, p.levelMaxBytes[level])
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPicker2TargetLevel(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_target_level2",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "pick":
				vers, opts, errMsg := loadVersion2(d)
				if errMsg != "" {
					return errMsg
				}

				p := newCompactionPicker2(vers, opts, nil)
				if len(p.compactionQueue) > 0 {
					return fmt.Sprintf("%d: %.1f\n", p.compactionQueue[0].level, p.compactionQueue[0].score)
				}
				return ""

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestCompactionPicker2EstimatedCompactionDebt(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_picker_estimated_debt2",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				vers, opts, errMsg := loadVersion2(d)
				if errMsg != "" {
					return errMsg
				}
				opts.MemTableSize = 1000

				p := newCompactionPicker2(vers, opts, nil)
				return fmt.Sprintf("%d\n", p.estimatedCompactionDebt(0))

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
