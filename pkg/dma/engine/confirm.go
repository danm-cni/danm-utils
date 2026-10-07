package engine

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// ErrNotConfirmed is returned when the operator did not type the exact token a step demands.
var ErrNotConfirmed = errors.New("the operation was not confirmed")

// Confirmer asks the operator to confirm an operation by typing an exact token. A plain yes or
// no prompt is deliberately not offered: muscle memory defeats it precisely where it matters
// most, which is the irreversible cleanup phase.
type Confirmer interface {
	Confirm(prompt, token string) error
}

// PromptConfirmer reads the token from a terminal. Input which is not a terminal is a hard
// error rather than an implicit yes, so a scripted run can never half-execute a plan.
type PromptConfirmer struct {
	In  io.Reader
	Out io.Writer
}

// NewPromptConfirmer returns a confirmer reading from standard input.
func NewPromptConfirmer(out io.Writer) *PromptConfirmer {
	return &PromptConfirmer{In: os.Stdin, Out: out}
}

// Confirm prints the prompt and compares what the operator typed against the expected token.
func (confirmer *PromptConfirmer) Confirm(prompt, token string) error {
	if !isTerminal(confirmer.In) {
		return fmt.Errorf("%w: standard input is not a terminal, run the assistant with \"kubectl exec -it\" or pass --confirm %s", ErrNotConfirmed, token)
	}
	fmt.Fprintln(confirmer.Out, prompt)
	fmt.Fprintf(confirmer.Out, "Type %s to confirm, anything else to abort: ", token)
	reader := bufio.NewReader(confirmer.In)
	typed, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("cannot read the confirmation: %w", err)
	}
	if strings.TrimSpace(typed) != token {
		return fmt.Errorf("%w: the typed text does not match %s", ErrNotConfirmed, token)
	}
	return nil
}

// TokenConfirmer compares a token supplied up front on the command line. It is what makes a
// migration scriptable without ever making it accidental.
type TokenConfirmer struct {
	Supplied string
	Out      io.Writer
}

// Confirm compares the supplied token against the expected one.
func (confirmer *TokenConfirmer) Confirm(prompt, token string) error {
	if confirmer.Supplied != token {
		return fmt.Errorf("%w: --confirm was given %q but this operation demands %s", ErrNotConfirmed, confirmer.Supplied, token)
	}
	if confirmer.Out != nil {
		fmt.Fprintln(confirmer.Out, prompt)
		fmt.Fprintf(confirmer.Out, "Confirmed with %s.\n", token)
	}
	return nil
}

// AlwaysConfirmed accepts everything. It exists for dry runs and for tests, and is never wired
// to a command line switch.
type AlwaysConfirmed struct{}

// Confirm accepts the operation.
func (AlwaysConfirmed) Confirm(_, _ string) error {
	return nil
}

func isTerminal(in io.Reader) bool {
	file, isFile := in.(*os.File)
	if !isFile {
		return false
	}
	return term.IsTerminal(int(file.Fd()))
}
