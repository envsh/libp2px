package pbexec

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/envsh/libp2px/p2put"
	"github.com/libp2p/go-libp2p/core/network"
)

const (
	execProto    = "shexec/1.0"
	cmdTimeout   = 3 * time.Second
	maxCmdLen    = 4096
	maxOutputLen = 1024
	readTimeout  = 10 * time.Second
)

var ShouldReject func(network.Stream) bool

type ExecResult struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error,omitempty"`
}

var shell string

func init() {
	for _, name := range []string{"bash", "sh"} {
		if p, err := exec.LookPath(name); err == nil {
			shell = p
			break
		}
	}
	if shell == "" {
		shell = "/bin/sh"
	}
	p2put.MustRegisterProtocol(execProto, handleExec)
}

func handleExec(s network.Stream) {
	defer s.Close()
	defer func() { recover() }()

	if ShouldReject != nil && ShouldReject(s) {
		s.Reset()
		return
	}

	type readRes struct {
		buf []byte
		err error
	}
	readCh := make(chan readRes, 1)
	go func() {
		buf, err := io.ReadAll(io.LimitReader(s, maxCmdLen))
		readCh <- readRes{buf, err}
	}()
	var buf []byte
	select {
	case r := <-readCh:
		if r.err != nil {
			return
		}
		buf = r.buf
	case <-time.After(readTimeout):
		return
	}

	cmdStr := strings.TrimSpace(string(buf))
	if cmdStr == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), cmdTimeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, shell, "-c", cmdStr)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	execCh := make(chan error, 1)
	go func() {
		execCh <- cmd.Run()
	}()

	var exitCode int
	var execErr string
	select {
	case err := <-execCh:
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				exitCode = ee.ExitCode()
			} else {
				exitCode = -1
				execErr = err.Error()
			}
		}
	case <-ctx.Done():
		exitCode = -1
		execErr = "execution timeout"
		select {
		case <-execCh:
		case <-time.After(2 * time.Second):
		}
	}

	json.NewEncoder(s).Encode(ExecResult{
		Stdout:   truncateStr(stdout.String(), maxOutputLen),
		Stderr:   truncateStr(stderr.String(), maxOutputLen),
		ExitCode: exitCode,
		Error:    execErr,
	})
}

func Exec(peerID, command string, ctx ...context.Context) (*ExecResult, error) {
	var c context.Context
	if len(ctx) > 0 {
		c = ctx[0]
	} else {
		c = context.Background()
	}
	s, err := p2put.OpenStream(c, peerID, execProto)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if _, err := s.Write([]byte(command)); err != nil {
		return nil, err
	}
	if sc, ok := s.(interface{ CloseWrite() error }); ok {
		sc.CloseWrite()
	}
	var res ExecResult
	if err := json.NewDecoder(s).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func truncateStr(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
