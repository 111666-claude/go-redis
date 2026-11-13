package proto_test

import (
	"bytes"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A nested RESP3 frame exercising every aggregate and scalar boundary:
// attribute prefix, map with a nil value, set, nested array with a RESP2 nil,
// an in-array server error, and a verbatim string. A sentinel reply follows so
// tests can verify the frame was consumed exactly — no more, no less.
const (
	nestedFrameAttr = "|1\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	nestedFrameBody = "*5\r\n" +
		"%2\r\n$1\r\na\r\n:1\r\n$1\r\nb\r\n_\r\n" +
		"~2\r\n+one\r\n+two\r\n" +
		"*2\r\n$3\r\nfoo\r\n$-1\r\n" +
		"-ERR nested\r\n" +
		"=13\r\ntxt:verbatim!\r\n"
	nestedFrame         = nestedFrameAttr + nestedFrameBody
	nestedFrameSentinel = "+NEXT\r\n"
)

// mustNotPanic runs f and fails the test if it panics, returning f's error.
// Protocol-boundary bugs in this package historically surfaced as makeslice
// panics and unrecoverable runtime OOM throws on crafted lengths.
func mustNotPanic(t *testing.T, f func() error) error {
	t.Helper()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ReadReply panicked: %v", r)
			}
		}()
		err = f()
	}()
	return err
}

func checkNestedFrameValue(t *testing.T, v interface{}) {
	t.Helper()
	arr, ok := v.([]interface{})
	if !ok {
		t.Fatalf("got %T, want []interface{}", v)
	}
	if len(arr) != 5 {
		t.Fatalf("got %d elements, want 5: %v", len(arr), arr)
	}
	m, ok := arr[0].(map[interface{}]interface{})
	if !ok || m["a"] != int64(1) || m["b"] != nil {
		t.Fatalf("element 0 = %#v, want map{a:1, b:nil}", arr[0])
	}
	set, ok := arr[1].([]interface{})
	if !ok || len(set) != 2 || set[0] != "one" || set[1] != "two" {
		t.Fatalf("element 1 = %#v, want [one two]", arr[1])
	}
	nested, ok := arr[2].([]interface{})
	if !ok || len(nested) != 2 || nested[0] != "foo" || nested[1] != nil {
		t.Fatalf("element 2 = %#v, want [foo nil]", arr[2])
	}
	errVal, ok := arr[3].(error)
	if !ok || errVal.Error() != "ERR nested" {
		t.Fatalf("element 3 = %#v, want server error \"ERR nested\"", arr[3])
	}
	if arr[4] != "verbatim!" {
		t.Fatalf("element 4 = %#v, want \"verbatim!\"", arr[4])
	}
}

func TestReaderNestedFrameConsumedExactly(t *testing.T) {
	r := proto.NewReader(strings.NewReader(nestedFrame + nestedFrameSentinel))
	v, err := r.ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	checkNestedFrameValue(t, v)

	// The very next read must see the sentinel: the nested frame — attribute
	// included — was consumed exactly, leaving the stream aligned.
	line, err := r.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "+NEXT" {
		t.Fatalf("next line = %q, want +NEXT (stream desynchronized)", line)
	}
}

// Every proper prefix of a valid frame is a truncated frame: parsing it must
// fail (EOF / unexpected EOF / invalid reply), never succeed and never panic.
func TestReaderTruncatedFrameEveryOffset(t *testing.T) {
	full := nestedFrame + nestedFrameSentinel
	for i := 0; i < len(nestedFrame); i++ {
		prefix := full[:i]
		err := mustNotPanic(t, func() error {
			r := proto.NewReader(strings.NewReader(prefix))
			_, err := r.ReadReply()
			return err
		})
		if err == nil {
			t.Fatalf("prefix len %d parsed successfully; truncated frames must fail", i)
		}
	}
}

// Short reads: a transport that delivers one byte per Read must not change
// the parse result — frames are reassembled across read boundaries.
func TestReaderShortReads(t *testing.T) {
	r := proto.NewReader(iotest.OneByteReader(strings.NewReader(nestedFrame + nestedFrameSentinel)))
	v, err := r.ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	checkNestedFrameValue(t, v)
	line, err := r.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "+NEXT" {
		t.Fatalf("next line = %q, want +NEXT", line)
	}
}

func TestReaderUnknownReplyType(t *testing.T) {
	for _, input := range []string{
		"?1\r\n+OK\r\n",    // streamed aggregate marker: not supported
		"x\r\n",            // garbage type byte
		"EOF:3\r\nabc\r\n", // streamed string marker: not supported
	} {
		err := mustNotPanic(t, func() error {
			r := proto.NewReader(strings.NewReader(input))
			_, err := r.ReadReply()
			return err
		})
		if err == nil {
			t.Fatalf("input %q parsed successfully; unknown types must fail", input)
		}
	}
}

// Lengths whose n+2 (payload + CRLF) overflows int, and aggregate counts far
// beyond any real reply, must produce a clean error — previously they panicked
// makeslice, threw an unrecoverable runtime OOM, or (ReadRawReplyWriteTo)
// silently reported success while leaving the payload in the stream.
func TestReaderLengthOverflow(t *testing.T) {
	huge := strconv.Itoa(math.MaxInt)
	hugeM1 := strconv.Itoa(math.MaxInt - 1)

	t.Run("ReadReply", func(t *testing.T) {
		inputs := []string{
			"$" + huge + "\r\n",
			"$" + hugeM1 + "\r\n",
			"!" + huge + "\r\n",
			"=" + huge + "\r\n",
			"*" + huge + "\r\n",
			"%" + huge + "\r\n",
			"~" + huge + "\r\n",
			">" + huge + "\r\n",
			"*2000000000\r\n", // no overflow, but preallocating it used to OOM
			"%2000000000\r\n",
			"*-5\r\n", // counts below -1 are invalid
			"$-5\r\n",
		}
		for _, input := range inputs {
			err := mustNotPanic(t, func() error {
				r := proto.NewReader(strings.NewReader(input))
				_, err := r.ReadReply()
				return err
			})
			if err == nil {
				t.Fatalf("input %q parsed successfully; want error", input)
			}
		}
	})

	t.Run("DiscardNext", func(t *testing.T) {
		err := mustNotPanic(t, func() error {
			r := proto.NewReader(strings.NewReader("$" + huge + "\r\n"))
			return r.DiscardNext()
		})
		if err == nil {
			t.Fatal("DiscardNext succeeded on overflowing length; want error")
		}
	})

	t.Run("ReadRawReply", func(t *testing.T) {
		err := mustNotPanic(t, func() error {
			r := proto.NewReader(strings.NewReader("$" + huge + "\r\n"))
			_, err := r.ReadRawReply()
			return err
		})
		if err == nil {
			t.Fatal("ReadRawReply succeeded on overflowing length; want error")
		}
	})

	t.Run("ReadRawReplyWriteTo", func(t *testing.T) {
		var buf bytes.Buffer
		err := mustNotPanic(t, func() error {
			r := proto.NewReader(strings.NewReader("$" + huge + "\r\nabc"))
			_, err := r.ReadRawReplyWriteTo(&buf)
			return err
		})
		if err == nil {
			t.Fatal("ReadRawReplyWriteTo silently succeeded on overflowing length")
		}
	})

	t.Run("ReadStringInto", func(t *testing.T) {
		err := mustNotPanic(t, func() error {
			r := proto.NewReader(strings.NewReader("$" + huge + "\r\n"))
			_, err := r.ReadStringInto(make([]byte, 8))
			return err
		})
		if err == nil {
			t.Fatal("ReadStringInto succeeded on overflowing length; want error")
		}
	})
}

// Nil encodings carry no payload: the stream must stay aligned for the reply
// that follows them.
func TestReaderNilFramesKeepStreamAligned(t *testing.T) {
	for _, nilFrame := range []string{"$-1\r\n", "*-1\r\n", "_\r\n"} {
		r := proto.NewReader(strings.NewReader(nilFrame + "+OK\r\n"))
		_, err := r.ReadReply()
		if err != proto.Nil {
			t.Fatalf("%q: got %v, want proto.Nil", nilFrame, err)
		}
		line, err := r.ReadLine()
		if err != nil || string(line) != "+OK" {
			t.Fatalf("%q: next line = %q, %v; want +OK", nilFrame, line, err)
		}
	}
}

func TestReaderBlobErrorFrame(t *testing.T) {
	r := proto.NewReader(strings.NewReader("!21\r\nSYNTAX invalid syntax\r\n+OK\r\n"))
	_, err := r.ReadReply()
	if err == nil || err.Error() != "SYNTAX invalid syntax" {
		t.Fatalf("got %v, want \"SYNTAX invalid syntax\"", err)
	}
	line, err := r.ReadLine()
	if err != nil || string(line) != "+OK" {
		t.Fatalf("next line = %q, %v; want +OK", line, err)
	}
}

func TestReaderPushFrame(t *testing.T) {
	input := ">3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$5\r\nhello\r\n+OK\r\n"
	r := proto.NewReader(strings.NewReader(input))
	typ, err := r.PeekReplyType()
	if err != nil {
		t.Fatal(err)
	}
	if typ != proto.RespPush {
		t.Fatalf("PeekReplyType = %q, want %q", typ, proto.RespPush)
	}
	v, err := r.ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.([]interface{})
	if !ok || len(arr) != 3 || arr[0] != "message" || arr[1] != "channel" || arr[2] != "hello" {
		t.Fatalf("push frame = %#v", v)
	}
	line, err := r.ReadLine()
	if err != nil || string(line) != "+OK" {
		t.Fatalf("next line = %q, %v; want +OK", line, err)
	}
}

// A huge declared bulk length with a truncated payload must fail on read, not
// on allocation of the full declared size.
func TestReaderTruncatedBulkPayload(t *testing.T) {
	r := proto.NewReader(strings.NewReader("$10\r\nabc"))
	_, err := r.ReadReply()
	if err != io.ErrUnexpectedEOF && err != io.EOF {
		t.Fatalf("got %v, want EOF-ish error", err)
	}
}
