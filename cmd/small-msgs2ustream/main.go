package main

import (
	"bufio"
	"context"
	"iter"
	"log"
	"os"

	mu "github.com/takanoriyanagitani/go-small-msgs2ustream"
)

var rpath mu.StreamPath = mu.StreamPath(os.Getenv("REMOTE_PATH"))

var mlstr mu.Maybe[string] = mu.MaybeNew(os.LookupEnv("LOCAL_PATH"))
var mlpat mu.Maybe[mu.StreamPath] = mu.MaybeMap(
	mlstr,
	func(path string) mu.StreamPath { return mu.StreamPath(path) },
)

var radr mu.StreamAddr = rpath.ToAddr()
var mlad mu.Maybe[mu.StreamAddr] = mu.MaybeMap(
	mlpat,
	func(mpat mu.StreamPath) mu.StreamAddr { return mpat.ToAddr() },
)

var lines iter.Seq[[]byte] = func(yield func([]byte) bool) {
	var scanner *bufio.Scanner = bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var line []byte = scanner.Bytes()
		if !yield(line) {
			return
		}
	}
}

var msgs iter.Seq2[*mu.SmallMessage, error] = func(
	yield func(*mu.SmallMessage, error) bool,
) {
	var buf mu.SmallMessage
	for line := range lines {
		mu.SmallMessageFromSlice(line, &buf)
		if !yield(&buf, nil) {
			return
		}
	}
}

func sub(ctx context.Context) error {
	scon, err := radr.DialUnix(mlad)
	if nil != err {
		return err
	}
	defer scon.Close()

	return mu.SmallMsgs(msgs).WriteAll(ctx, scon)
}

func main() {
	err := sub(context.Background())
	if nil != err {
		log.Printf("%v\n", err)
	}
}
