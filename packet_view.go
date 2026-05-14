package sio

import (
	"bytes"
	"fmt"
	"strconv"
)

type PacketType byte

const (
	PacketConnect PacketType = iota
	PacketDisconnect
	PacketEvent
	PacketAck
	PacketConnectError
	PacketBinaryEvent
	PacketBinaryAck
)

type PacketView struct {
	Type        PacketType
	Namespace   []byte
	ID          uint64
	HasID       bool
	Attachments int
	Binary      [][]byte
	Payload     []byte
	Event       []byte
	Args        JSONArrayView
}

type JSONArrayView struct {
	data []byte
	pos  int
}

func ParsePacketView(data []byte) (PacketView, error) {
	var p PacketView
	if len(data) == 0 {
		return p, fmt.Errorf("socket.io packet: empty packet")
	}
	if data[0] < '0' || data[0] > '6' {
		return p, fmt.Errorf("socket.io packet: invalid type byte %q", data[0])
	}
	p.Type = PacketType(data[0] - '0')
	data = data[1:]
	if p.Type == PacketBinaryEvent || p.Type == PacketBinaryAck {
		i := 0
		attachments := 0
		for i < len(data) && isDigit(data[i]) {
			attachments = attachments*10 + int(data[i]-'0')
			i++
		}
		if i == 0 || i >= len(data) || data[i] != '-' {
			return p, fmt.Errorf("socket.io packet: binary packet attachment count expected")
		}
		if attachments <= 0 {
			return p, fmt.Errorf("socket.io packet: binary packet attachment count must be positive")
		}
		p.Attachments = attachments
		data = data[i+1:]
	}

	if len(data) > 0 && data[0] == '/' {
		idx := bytes.IndexByte(data, ',')
		if idx < 0 {
			p.Namespace = data
			data = data[len(data):]
		} else {
			p.Namespace = data[:idx]
			data = data[idx+1:]
		}
	} else {
		p.Namespace = defaultNamespaceBytes
	}

	if len(data) > 0 && isDigit(data[0]) {
		i := 0
		var id uint64
		for i < len(data) && isDigit(data[i]) {
			id = id*10 + uint64(data[i]-'0')
			i++
		}
		p.ID = id
		p.HasID = true
		data = data[i:]
	}
	p.Payload = data

	switch p.Type {
	case PacketEvent, PacketBinaryEvent:
		event, args, err := splitEventPayload(data)
		if err != nil {
			return p, err
		}
		p.Event = event
		p.Args = args
	case PacketAck, PacketBinaryAck:
		if len(data) > 0 {
			args, err := NewJSONArrayView(data)
			if err != nil {
				return p, err
			}
			p.Args = args
		}
	}
	return p, nil
}

func AppendPacketHeader(dst []byte, typ PacketType, namespace []byte, id uint64, hasID bool) []byte {
	return appendPacketHeader(dst, typ, 0, namespace, id, hasID)
}

func appendPacketHeader(dst []byte, typ PacketType, attachments int, namespace []byte, id uint64, hasID bool) []byte {
	dst = append(dst, byte('0'+typ))
	if typ == PacketBinaryEvent || typ == PacketBinaryAck {
		dst = strconv.AppendInt(dst, int64(attachments), 10)
		dst = append(dst, '-')
	}
	if len(namespace) != 0 && !bytes.Equal(namespace, defaultNamespaceBytes) {
		dst = append(dst, namespace...)
		dst = append(dst, ',')
	}
	if hasID {
		dst = strconv.AppendUint(dst, id, 10)
	}
	return dst
}

func appendPacketHeaderString(dst []byte, typ PacketType, attachments int, namespace string, id uint64, hasID bool) []byte {
	dst = append(dst, byte('0'+typ))
	if typ == PacketBinaryEvent || typ == PacketBinaryAck {
		dst = strconv.AppendInt(dst, int64(attachments), 10)
		dst = append(dst, '-')
	}
	if namespace != "" && namespace != "/" {
		dst = append(dst, namespace...)
		dst = append(dst, ',')
	}
	if hasID {
		dst = strconv.AppendUint(dst, id, 10)
	}
	return dst
}

func AppendConnectPacket(dst []byte, namespace []byte, sid string, pid string) []byte {
	dst = AppendPacketHeader(dst, PacketConnect, namespace, 0, false)
	dst = append(dst, `{"sid":"`...)
	dst = appendJSONStringContent(dst, sid)
	if pid != "" {
		dst = append(dst, `","pid":"`...)
		dst = appendJSONStringContent(dst, pid)
	}
	dst = append(dst, `"}`...)
	return dst
}

func appendConnectPacketString(dst []byte, namespace string, sid string, pid string) []byte {
	dst = appendPacketHeaderString(dst, PacketConnect, 0, namespace, 0, false)
	dst = append(dst, `{"sid":"`...)
	dst = appendJSONStringContent(dst, sid)
	if pid != "" {
		dst = append(dst, `","pid":"`...)
		dst = appendJSONStringContent(dst, pid)
	}
	dst = append(dst, `"}`...)
	return dst
}

func AppendDisconnectPacket(dst []byte, namespace []byte) []byte {
	return AppendPacketHeader(dst, PacketDisconnect, namespace, 0, false)
}

func appendDisconnectPacketString(dst []byte, namespace string) []byte {
	return appendPacketHeaderString(dst, PacketDisconnect, 0, namespace, 0, false)
}

func AppendEventPacket(dst []byte, namespace []byte, id uint64, hasID bool, event string, encodedArgs ...[]byte) []byte {
	return appendEventPacket(dst, PacketEvent, 0, namespace, id, hasID, event, encodedArgs...)
}

func AppendBinaryEventPacket(dst []byte, namespace []byte, id uint64, hasID bool, event string, attachments int, encodedArgs ...[]byte) []byte {
	return appendEventPacket(dst, PacketBinaryEvent, attachments, namespace, id, hasID, event, encodedArgs...)
}

func appendEventPacket(dst []byte, typ PacketType, attachments int, namespace []byte, id uint64, hasID bool, event string, encodedArgs ...[]byte) []byte {
	dst = appendPacketHeader(dst, typ, attachments, namespace, id, hasID)
	dst = append(dst, '[')
	dst = strconv.AppendQuote(dst, event)
	for _, arg := range encodedArgs {
		dst = append(dst, ',')
		dst = append(dst, arg...)
	}
	dst = append(dst, ']')
	return dst
}

func appendEventPacketString(dst []byte, typ PacketType, attachments int, namespace string, id uint64, hasID bool, event string, encodedArgs ...[]byte) []byte {
	dst = appendPacketHeaderString(dst, typ, attachments, namespace, id, hasID)
	dst = append(dst, '[')
	dst = strconv.AppendQuote(dst, event)
	for _, arg := range encodedArgs {
		dst = append(dst, ',')
		dst = append(dst, arg...)
	}
	dst = append(dst, ']')
	return dst
}

func AppendAckPacket(dst []byte, namespace []byte, id uint64, encodedArgs ...[]byte) []byte {
	return appendAckPacket(dst, PacketAck, 0, namespace, id, encodedArgs...)
}

func AppendBinaryAckPacket(dst []byte, namespace []byte, id uint64, attachments int, encodedArgs ...[]byte) []byte {
	return appendAckPacket(dst, PacketBinaryAck, attachments, namespace, id, encodedArgs...)
}

func appendAckPacket(dst []byte, typ PacketType, attachments int, namespace []byte, id uint64, encodedArgs ...[]byte) []byte {
	dst = appendPacketHeader(dst, typ, attachments, namespace, id, true)
	dst = append(dst, '[')
	for i, arg := range encodedArgs {
		if i != 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, arg...)
	}
	dst = append(dst, ']')
	return dst
}

func appendAckPacketString(dst []byte, typ PacketType, attachments int, namespace string, id uint64, encodedArgs ...[]byte) []byte {
	dst = appendPacketHeaderString(dst, typ, attachments, namespace, id, true)
	dst = append(dst, '[')
	for i, arg := range encodedArgs {
		if i != 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, arg...)
	}
	dst = append(dst, ']')
	return dst
}

func NewJSONArrayView(data []byte) (JSONArrayView, error) {
	if len(data) < 2 || data[0] != '[' {
		return JSONArrayView{}, fmt.Errorf("socket.io packet: JSON array expected")
	}
	return JSONArrayView{data: data, pos: 1}, nil
}

func (v *JSONArrayView) Next() ([]byte, bool, error) {
	if len(v.data) == 0 {
		return nil, false, nil
	}
	for v.pos < len(v.data) && isJSONSpace(v.data[v.pos]) {
		v.pos++
	}
	if v.pos >= len(v.data) {
		return nil, false, fmt.Errorf("socket.io packet: unterminated JSON array")
	}
	if v.data[v.pos] == ']' {
		v.pos++
		return nil, false, nil
	}
	start := v.pos
	end, err := scanJSONValue(v.data, start)
	if err != nil {
		return nil, false, err
	}
	v.pos = end
	for v.pos < len(v.data) && isJSONSpace(v.data[v.pos]) {
		v.pos++
	}
	if v.pos >= len(v.data) {
		return nil, false, fmt.Errorf("socket.io packet: unterminated JSON array")
	}
	switch v.data[v.pos] {
	case ',':
		v.pos++
	case ']':
		// The next call will consume the terminator.
	default:
		return nil, false, fmt.Errorf("socket.io packet: invalid JSON array delimiter %q", v.data[v.pos])
	}
	return v.data[start:end], true, nil
}

func splitEventPayload(data []byte) ([]byte, JSONArrayView, error) {
	arr, err := NewJSONArrayView(data)
	if err != nil {
		return nil, JSONArrayView{}, err
	}
	first, ok, err := arr.Next()
	if err != nil {
		return nil, JSONArrayView{}, err
	}
	if !ok {
		return nil, JSONArrayView{}, fmt.Errorf("socket.io packet: event payload must not be empty")
	}
	event, err := unquoteJSONStringView(first)
	if err != nil {
		return nil, JSONArrayView{}, fmt.Errorf("socket.io packet: invalid event name: %w", err)
	}
	if len(event) == 0 {
		return nil, JSONArrayView{}, fmt.Errorf("socket.io packet: empty event name")
	}
	return event, arr, nil
}

func unquoteJSONStringView(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return nil, fmt.Errorf("JSON string expected")
	}
	body := data[1 : len(data)-1]
	for _, b := range body {
		if b == '\\' || b < 0x20 {
			return nil, errEscapedJSONString
		}
	}
	return body, nil
}

func scanJSONValue(data []byte, pos int) (int, error) {
	if pos >= len(data) {
		return pos, fmt.Errorf("socket.io packet: JSON value expected")
	}
	switch data[pos] {
	case '"':
		return scanJSONString(data, pos)
	case '{':
		return scanJSONComposite(data, pos, '{', '}')
	case '[':
		return scanJSONComposite(data, pos, '[', ']')
	case 't':
		return scanLiteral(data, pos, "true")
	case 'f':
		return scanLiteral(data, pos, "false")
	case 'n':
		return scanLiteral(data, pos, "null")
	default:
		if data[pos] == '-' || isDigit(data[pos]) {
			return scanJSONNumber(data, pos)
		}
		return pos, fmt.Errorf("socket.io packet: invalid JSON value start %q", data[pos])
	}
}

func scanJSONString(data []byte, pos int) (int, error) {
	for i := pos + 1; i < len(data); i++ {
		switch data[i] {
		case '\\':
			i++
		case '"':
			return i + 1, nil
		}
	}
	return pos, fmt.Errorf("socket.io packet: unterminated JSON string")
}

func scanJSONComposite(data []byte, pos int, open, close byte) (int, error) {
	var stack [64]byte
	depth := 1
	stack[0] = close
	for i := pos + 1; i < len(data); i++ {
		switch data[i] {
		case '"':
			end, err := scanJSONString(data, i)
			if err != nil {
				return pos, err
			}
			i = end - 1
		case '{':
			if depth == len(stack) {
				return pos, fmt.Errorf("socket.io packet: JSON nesting exceeds %d", len(stack))
			}
			stack[depth] = '}'
			depth++
		case '[':
			if depth == len(stack) {
				return pos, fmt.Errorf("socket.io packet: JSON nesting exceeds %d", len(stack))
			}
			stack[depth] = ']'
			depth++
		case '}', ']':
			if depth == 0 || data[i] != stack[depth-1] {
				return pos, fmt.Errorf("socket.io packet: mismatched JSON delimiter %q", data[i])
			}
			depth--
			if depth == 0 {
				return i + 1, nil
			}
		}
	}
	return pos, fmt.Errorf("socket.io packet: unterminated JSON composite starting with %q", open)
}

func scanLiteral(data []byte, pos int, lit string) (int, error) {
	if len(data)-pos < len(lit) {
		return pos, fmt.Errorf("socket.io packet: invalid JSON literal")
	}
	for i := 0; i < len(lit); i++ {
		if data[pos+i] != lit[i] {
			return pos, fmt.Errorf("socket.io packet: invalid JSON literal")
		}
	}
	return pos + len(lit), nil
}

func scanJSONNumber(data []byte, pos int) (int, error) {
	i := pos
	if data[i] == '-' {
		i++
	}
	if i >= len(data) || !isDigit(data[i]) {
		return pos, fmt.Errorf("socket.io packet: invalid JSON number")
	}
	for i < len(data) && isDigit(data[i]) {
		i++
	}
	if i < len(data) && data[i] == '.' {
		i++
		if i >= len(data) || !isDigit(data[i]) {
			return pos, fmt.Errorf("socket.io packet: invalid JSON number fraction")
		}
		for i < len(data) && isDigit(data[i]) {
			i++
		}
	}
	if i < len(data) && (data[i] == 'e' || data[i] == 'E') {
		i++
		if i < len(data) && (data[i] == '+' || data[i] == '-') {
			i++
		}
		if i >= len(data) || !isDigit(data[i]) {
			return pos, fmt.Errorf("socket.io packet: invalid JSON number exponent")
		}
		for i < len(data) && isDigit(data[i]) {
			i++
		}
	}
	return i, nil
}

func appendJSONStringContent(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', '"':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if c < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				dst = append(dst, c)
			}
		}
	}
	return dst
}

func appendJSONStringContentOwned(dst *pooledBytes, s string) {
	dst.Ensure(len(s) * 6)
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', '"':
			dst.AppendByte('\\')
			dst.AppendByte(c)
		case '\n':
			dst.AppendString(`\n`)
		case '\r':
			dst.AppendString(`\r`)
		case '\t':
			dst.AppendString(`\t`)
		default:
			if c < 0x20 {
				dst.AppendString(`\u00`)
				dst.AppendByte(hex[c>>4])
				dst.AppendByte(hex[c&0xf])
			} else {
				dst.AppendByte(c)
			}
		}
	}
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

func isJSONSpace(b byte) bool { return b == ' ' || b == '\n' || b == '\r' || b == '\t' }

var (
	defaultNamespaceBytes = []byte("/")
	errEscapedJSONString  = fmt.Errorf("escaped JSON string requires unescape buffer")
	hex                   = "0123456789abcdef"
)
