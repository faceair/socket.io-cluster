package sio

import (
	"bytes"
	"testing"
)

func TestParsePacketViewEvent(t *testing.T) {
	input := []byte(`2/chat,17["message",{"x":[1,2,{"y":"z"}]},true,null]`)
	p, err := ParsePacketView(input)
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != PacketEvent || !bytes.Equal(p.Namespace, []byte("/chat")) || !p.HasID || p.ID != 17 {
		t.Fatalf("bad header: %#v", p)
	}
	if !bytes.Equal(p.Event, []byte("message")) {
		t.Fatalf("bad event: %q", p.Event)
	}
	want := [][]byte{[]byte(`{"x":[1,2,{"y":"z"}]}`), []byte(`true`), []byte(`null`)}
	for i, w := range want {
		got, ok, err := p.Args.Next()
		if err != nil {
			t.Fatalf("arg %d error: %v", i, err)
		}
		if !ok || !bytes.Equal(got, w) {
			t.Fatalf("arg %d = %q ok=%v want %q", i, got, ok, w)
		}
	}
	if got, ok, err := p.Args.Next(); err != nil || ok || got != nil {
		t.Fatalf("unexpected extra arg got=%q ok=%v err=%v", got, ok, err)
	}
}

func TestAppendPackets(t *testing.T) {
	buf := make([]byte, 0, 128)
	buf = AppendConnectPacket(buf, []byte("/chat"), "n1-abc", "")
	if string(buf) != `0/chat,{"sid":"n1-abc"}` {
		t.Fatalf("connect packet = %s", buf)
	}
	buf = buf[:0]
	buf = AppendEventPacket(buf, []byte("/"), 9, true, "hello", []byte(`1`), []byte(`{"a":2}`))
	if string(buf) != `29["hello",1,{"a":2}]` {
		t.Fatalf("event packet = %s", buf)
	}
	buf = buf[:0]
	buf = AppendBinaryEventPacket(buf, []byte("/"), 0, false, "bin", 1, []byte(`{"_placeholder":true,"num":0}`))
	if string(buf) != `51-["bin",{"_placeholder":true,"num":0}]` {
		t.Fatalf("binary event packet = %s", buf)
	}
}

func TestParsePacketViewBinaryEvent(t *testing.T) {
	p, err := ParsePacketView([]byte(`51-/chat,12["bin",{"_placeholder":true,"num":0}]`))
	if err != nil {
		t.Fatal(err)
	}
	if p.Type != PacketBinaryEvent || p.Attachments != 1 || !p.HasID || p.ID != 12 || !bytes.Equal(p.Namespace, []byte("/chat")) {
		t.Fatalf("bad binary packet: %#v", p)
	}
	if !bytes.Equal(p.Event, []byte("bin")) {
		t.Fatalf("bad event: %q", p.Event)
	}
}

func TestParsePacketViewAllocs(t *testing.T) {
	input := []byte(`2/chat,17["message",{"x":[1,2,{"y":"z"}]},true,null]`)
	allocs := testing.AllocsPerRun(1000, func() {
		p, err := ParsePacketView(input)
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, ok, err := p.Args.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
		}
	})
	if allocs != 0 {
		t.Fatalf("ParsePacketView allocations = %v, want 0", allocs)
	}
}

func TestParsePacketViewRejectsCorruptedFramesRepeatedly(t *testing.T) {
	inputs := [][]byte{
		nil,
		[]byte(`x`),
		[]byte(`2/chat,`),
		[]byte(`2/chat,not-json`),
		[]byte(`5-/chat,["bin"]`),
		[]byte(`50-/chat,["bin"]`),
		[]byte(`51/chat,["bin"]`),
	}
	for i := 0; i < 1000; i++ {
		for _, input := range inputs {
			if _, err := ParsePacketView(input); err == nil {
				t.Fatalf("ParsePacketView(%q) succeeded, want error", input)
			}
		}
	}
}
