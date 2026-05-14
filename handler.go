package sio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

var (
	jsonPlaceholderKey = []byte(`"_placeholder"`)
	jsonTrueLiteral    = []byte(`true`)
	jsonPlaceholderNum = []byte(`"num"`)
)

type eventHandler struct {
	fn     reflect.Value
	args   []reflect.Type
	hasAck bool
	once   bool
}

type eventHandlers struct {
	mu sync.RWMutex
	m  map[string][]*eventHandler
}

func (h *eventHandlers) dispatchServerSide(p PacketView) error {
	event := bytesToStringView(p.Event)
	h.mu.Lock()
	list := h.m[event]
	if len(list) == 0 {
		h.mu.Unlock()
		return nil
	}
	kept := list[:0]
	for _, item := range list {
		if !item.once {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(h.m, event)
	} else {
		h.m[event] = kept
	}
	items := append([]*eventHandler(nil), list...)
	h.mu.Unlock()
	for _, item := range items {
		if item.hasAck {
			return fmt.Errorf("sio: serverSideEmit handler %q must not declare ack callback", event)
		}
		if err := item.callServerSide(p); err != nil {
			return err
		}
	}
	return nil
}

func newEventHandlers() *eventHandlers { return &eventHandlers{m: make(map[string][]*eventHandler)} }

func (h *eventHandlers) add(event string, fn any, once bool) error {
	handler, err := newEventHandler(fn, once)
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.m[event] = append(h.m[event], handler)
	h.mu.Unlock()
	return nil
}

func (h *eventHandlers) off(event string, fn ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(fn) == 0 || fn[0] == nil {
		delete(h.m, event)
		return
	}
	rv := reflect.ValueOf(fn[0])
	if rv.Kind() != reflect.Func {
		return
	}
	ptr := rv.Pointer()
	list := h.m[event]
	kept := list[:0]
	for _, item := range list {
		if item.fn.Pointer() != ptr {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(h.m, event)
	} else {
		h.m[event] = kept
	}
}

func (h *eventHandlers) offAll() {
	h.mu.Lock()
	h.m = make(map[string][]*eventHandler)
	h.mu.Unlock()
}

func (h *eventHandlers) dispatch(socket *serverSocket, p PacketView) error {
	event := bytesToStringView(p.Event)
	h.mu.Lock()
	list := h.m[event]
	if len(list) == 0 {
		h.mu.Unlock()
		return nil
	}
	kept := list[:0]
	for _, item := range list {
		if !item.once {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		delete(h.m, event)
	} else {
		h.m[event] = kept
	}
	items := append([]*eventHandler(nil), list...)
	h.mu.Unlock()
	for _, item := range items {
		if err := item.call(socket, p); err != nil {
			return err
		}
	}
	return nil
}

func newEventHandler(fn any, once bool) (*eventHandler, error) {
	if fn == nil {
		return nil, fmt.Errorf("sio: event handler is nil")
	}
	rv := reflect.ValueOf(fn)
	if rv.Kind() != reflect.Func {
		return nil, fmt.Errorf("sio: event handler must be function, got %T", fn)
	}
	rt := rv.Type()
	if rt.NumOut() != 0 {
		return nil, fmt.Errorf("sio: event handler %s must not return values", rt.String())
	}
	args := make([]reflect.Type, rt.NumIn())
	for i := range args {
		args[i] = rt.In(i)
	}
	hasAck := len(args) > 0 && args[len(args)-1].Kind() == reflect.Func
	if hasAck && args[len(args)-1].NumOut() != 0 {
		return nil, fmt.Errorf("sio: event handler %s ack function must not return values", rt.String())
	}
	return &eventHandler{fn: rv, args: args, hasAck: hasAck, once: once}, nil
}

func (h *eventHandler) call(socket *serverSocket, p PacketView) error {
	argTypes := h.args
	if h.hasAck {
		argTypes = argTypes[:len(argTypes)-1]
	}
	values := make([]reflect.Value, 0, len(h.args))
	args := p.Args
	for i, typ := range argTypes {
		raw, ok, err := args.Next()
		if err != nil {
			return fmt.Errorf("sio: event %q arg %d scan failed: %w", p.Event, i, err)
		}
		if !ok {
			return fmt.Errorf("sio: event %q expected %d args, got %d", p.Event, len(argTypes), i)
		}
		v, err := decodeValueWithBinary(raw, typ, p.Binary)
		if err != nil {
			return fmt.Errorf("sio: event %q arg %d decode failed: %w", p.Event, i, err)
		}
		values = append(values, v)
	}
	if h.hasAck {
		ackType := h.args[len(h.args)-1]
		ack := reflect.MakeFunc(ackType, func(args []reflect.Value) []reflect.Value {
			if !p.HasID {
				return nil
			}
			encoded := encodeReflectArgs(args)
			socket.sendAckEncoded(p.ID, encoded)
			return nil
		})
		values = append(values, ack)
	}
	defer func() { _ = recover() }()
	h.fn.Call(values)
	return nil
}

func (h *eventHandler) callServerSide(p PacketView) error {
	values := make([]reflect.Value, 0, len(h.args))
	args := p.Args
	for i, typ := range h.args {
		raw, ok, err := args.Next()
		if err != nil {
			return fmt.Errorf("sio: serverSideEmit %q arg %d scan failed: %w", p.Event, i, err)
		}
		if !ok {
			return fmt.Errorf("sio: serverSideEmit %q expected %d args, got %d", p.Event, len(h.args), i)
		}
		v, err := decodeValueWithBinary(raw, typ, p.Binary)
		if err != nil {
			return fmt.Errorf("sio: serverSideEmit %q arg %d decode failed: %w", p.Event, i, err)
		}
		values = append(values, v)
	}
	defer func() { _ = recover() }()
	h.fn.Call(values)
	return nil
}

func decodeValueWithBinary(raw []byte, typ reflect.Type, attachments [][]byte) (reflect.Value, error) {
	if idx, ok := binaryPlaceholderNum(raw); ok {
		if idx < 0 || idx >= len(attachments) {
			return reflect.Value{}, fmt.Errorf("binary attachment %d missing", idx)
		}
		if v, ok := binaryReflectValue(attachments[idx], typ); ok {
			return v, nil
		}
	}
	if typ.Kind() == reflect.Ptr {
		v := reflect.New(typ.Elem())
		if err := json.Unmarshal(raw, v.Interface()); err != nil {
			return reflect.Value{}, err
		}
		return v, nil
	}
	v := reflect.New(typ)
	if err := json.Unmarshal(raw, v.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return v.Elem(), nil
}

func binaryReflectValue(data []byte, typ reflect.Type) (reflect.Value, bool) {
	binaryType := reflect.TypeOf(Binary(nil))
	bytesType := reflect.TypeOf([]byte(nil))
	switch {
	case typ == bytesType:
		return reflect.ValueOf(data), true
	case typ == binaryType:
		return reflect.ValueOf(Binary(data)), true
	case typ.Kind() == reflect.Interface && binaryType.AssignableTo(typ):
		return reflect.ValueOf(Binary(data)), true
	default:
		return reflect.Value{}, false
	}
}

func binaryPlaceholderNum(raw []byte) (int, bool) {
	if !bytes.Contains(raw, jsonPlaceholderKey) || !bytes.Contains(raw, jsonTrueLiteral) {
		return 0, false
	}
	idx := bytes.Index(raw, jsonPlaceholderNum)
	if idx < 0 {
		return 0, false
	}
	idx += len(`"num"`)
	for idx < len(raw) && (raw[idx] == ' ' || raw[idx] == '\t' || raw[idx] == '\n' || raw[idx] == '\r' || raw[idx] == ':') {
		idx++
	}
	if idx >= len(raw) || !isDigit(raw[idx]) {
		return 0, false
	}
	num := 0
	for idx < len(raw) && isDigit(raw[idx]) {
		num = num*10 + int(raw[idx]-'0')
		idx++
	}
	return num, true
}

func encodeReflectArgs(args []reflect.Value) encodedArgs {
	if len(args) == 0 {
		return encodedArgs{}
	}
	encoded := encodedArgs{
		json:   acquireByteBatch(len(args), len(args)*16),
		binary: acquireOptionalReflectBinaryBatch(args),
	}
	for _, arg := range args {
		if !arg.IsValid() || !arg.CanInterface() {
			encoded.json.AppendString("null")
			continue
		}
		if b, ok := binaryArg(arg.Interface()); ok {
			encoded.json.AppendPlaceholder(len(encoded.BinaryViews()))
			encoded.binary.AppendBytes(b)
			continue
		}
		b, err := json.Marshal(arg.Interface())
		if err != nil {
			encoded.json.AppendString("null")
		} else {
			encoded.json.AppendBytes(b)
		}
	}
	return encoded
}

func acquireOptionalReflectBinaryBatch(args []reflect.Value) *byteBatch {
	byteCap := estimateReflectBinaryBytes(args)
	if byteCap == 0 && countReflectBinaryArgs(args) == 0 {
		return nil
	}
	return acquireByteBatch(len(args), byteCap)
}

func estimateReflectBinaryBytes(args []reflect.Value) int {
	n := 0
	for _, arg := range args {
		if arg.IsValid() && arg.CanInterface() {
			if b, ok := binaryArg(arg.Interface()); ok {
				n += len(b)
			}
		}
	}
	return n
}

func countReflectBinaryArgs(args []reflect.Value) int {
	n := 0
	for _, arg := range args {
		if arg.IsValid() && arg.CanInterface() {
			if _, ok := binaryArg(arg.Interface()); ok {
				n++
			}
		}
	}
	return n
}

type ackHandler struct {
	fn       reflect.Value
	args     []reflect.Type
	hasError bool
	once     sync.Once
}

func newAckHandler(fn any, hasError bool) (*ackHandler, error) {
	rv := reflect.ValueOf(fn)
	if fn == nil || rv.Kind() != reflect.Func {
		return nil, fmt.Errorf("sio: ack handler must be function, got %T", fn)
	}
	rt := rv.Type()
	if rt.NumOut() != 0 {
		return nil, fmt.Errorf("sio: ack handler %s must not return values", rt.String())
	}
	if hasError && (rt.NumIn() == 0 || !rt.In(0).Implements(reflect.TypeOf((*error)(nil)).Elem())) {
		return nil, fmt.Errorf("sio: timeout ack handler %s must take error as first argument", rt.String())
	}
	args := make([]reflect.Type, rt.NumIn())
	for i := range args {
		args[i] = rt.In(i)
	}
	return &ackHandler{fn: rv, args: args, hasError: hasError}, nil
}

func (h *ackHandler) call(args JSONArrayView, attachments [][]byte) error {
	var err error
	h.once.Do(func() {
		values := make([]reflect.Value, 0, len(h.args))
		start := 0
		if h.hasError {
			values = append(values, reflect.Zero(h.args[0]))
			start = 1
		}
		for i := start; i < len(h.args); i++ {
			raw, ok, scanErr := args.Next()
			if scanErr != nil {
				err = scanErr
				return
			}
			if !ok {
				values = append(values, reflect.Zero(h.args[i]))
				continue
			}
			v, decErr := decodeValueWithBinary(raw, h.args[i], attachments)
			if decErr != nil {
				err = decErr
				return
			}
			values = append(values, v)
		}
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("sio: ack handler panic: %v", r)
			}
		}()
		h.fn.Call(values)
	})
	return err
}

func (h *ackHandler) timeout() {
	h.once.Do(func() {
		if !h.hasError {
			return
		}
		values := make([]reflect.Value, len(h.args))
		values[0] = reflect.ValueOf(ErrAckTimeout)
		for i := 1; i < len(values); i++ {
			values[i] = reflect.Zero(h.args[i])
		}
		defer func() { _ = recover() }()
		h.fn.Call(values)
	})
}
