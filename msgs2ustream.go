package msgs2ustream

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"iter"
	"net"
)

type SmallMessage struct {
	data [65536]byte
	size uint16
}

func SmallMessageFromSlice(raw []byte, msg *SmallMessage) {
	var sz int = len(raw)
	var limited uint16 = uint16(sz & 0xffff)
	var truncated []byte = raw[:limited]
	copy(msg.data[:], truncated)
	msg.size = limited
}

func (m *SmallMessage) Length() int { return int(m.size) }

func (m *SmallMessage) AsSlice() []byte {
	return m.data[:m.size]
}

func (m *SmallMessage) WriteTo(wtr io.Writer) (int64, error) {
	var written int64 = 0
	var size [2]byte

	binary.BigEndian.PutUint16(size[:], m.size)
	wsz, err := wtr.Write(size[:])
	if nil != err {
		return int64(wsz), err
	}

	written += int64(wsz)

	var data []byte = m.AsSlice()
	dsz, err := wtr.Write(data)

	written += int64(dsz)

	return written, err
}

type Maybe[T any] struct {
	V     T
	Valid bool
}

func Just[T any](val T) Maybe[T] {
	return Maybe[T]{
		V:     val,
		Valid: true,
	}
}

func Nothing[T any]() Maybe[T] {
	var ret Maybe[T]
	return ret
}

func (m Maybe[T]) IsJust() bool    { return m.Valid }
func (m Maybe[T]) IsNothing() bool { return !m.IsJust() }

func (m Maybe[T]) Value() (T, bool) { return m.V, m.Valid }

func (m Maybe[T]) ValueOr(alt T) T {
	switch m.Valid {
	case true:
		return m.V
	default:
		return alt
	}
}

func (m Maybe[T]) ValueOrElse(alt func() T) T {
	switch m.Valid {
	case true:
		return m.V
	default:
		return alt()
	}
}

func MaybeMap[T, U any](m Maybe[T], mapper func(T) U) Maybe[U] {
	switch m.Valid {
	case true:
		return Just(mapper(m.V))
	default:
		return Nothing[U]()
	}
}

func MaybeNew[T any](val T, valid bool) Maybe[T] {
	return Maybe[T]{V: val, Valid: valid}
}

type StreamConn struct{ *net.UnixConn }

func (c StreamConn) Close() error { return c.UnixConn.Close() }

type StreamAddr struct{ *net.UnixAddr }

type Either[R any] struct {
	Left  error
	Right R
}

func Left[R any](err error) Either[R] { return Either[R]{Left: err} }

func Right[R any](val R) Either[R] { return Either[R]{Right: val} }

func EitherNew[R any](right R, left error) Either[R] {
	return Either[R]{
		Right: right,
		Left:  left,
	}
}

func EitherMap[R, T any](e Either[R], mapper func(R) T) Either[T] {
	switch nil == e.Left {
	case true:
		return Right(mapper(e.Right))
	default:
		return Left[T](e.Left)
	}
}

func (e Either[R]) Value() (R, error) { return e.Right, e.Left }

func (a StreamAddr) DialUnix(local Maybe[StreamAddr]) (StreamConn, error) {
	var mapd Maybe[Either[StreamConn]] = MaybeMap(
		local,
		func(ladr StreamAddr) Either[StreamConn] {
			var network string = "unix"
			var laddr *net.UnixAddr = ladr.UnixAddr
			var raddr *net.UnixAddr = a.UnixAddr
			var eucon Either[*net.UnixConn] = EitherNew(
				net.DialUnix(network, laddr, raddr),
			)
			return EitherMap(eucon, func(right *net.UnixConn) StreamConn {
				return StreamConn{UnixConn: right}
			})
		},
	)
	var fallback Either[StreamConn] = mapd.ValueOrElse(
		func() Either[StreamConn] {
			var network string = "unix"
			var laddr *net.UnixAddr = nil
			var raddr *net.UnixAddr = a.UnixAddr
			var eucon Either[*net.UnixConn] = EitherNew(
				net.DialUnix(network, laddr, raddr),
			)
			return EitherMap(eucon, func(right *net.UnixConn) StreamConn {
				return StreamConn{UnixConn: right}
			})
		},
	)
	return fallback.Value()
}

type SmallMsgs iter.Seq2[*SmallMessage, error]

func (m SmallMsgs) WriteAll(ctx context.Context, scon StreamConn) error {
	var bw *bufio.Writer = bufio.NewWriter(scon.UnixConn)

	for smsg, err := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != err {
			return err
		}

		_, err := smsg.WriteTo(bw)
		if nil != err {
			return err
		}
	}

	return bw.Flush()
}

type StreamPath string

func (p StreamPath) ToAddr() StreamAddr {
	return StreamAddr{
		UnixAddr: &net.UnixAddr{
			Name: string(p),
			Net:  "unix",
		},
	}
}
