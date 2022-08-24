package invoker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Invoker is a func invoker.
// args can only be passed by codec.
type Invoker struct {
	fn    sync.Map
	codec Codec
}

type Codec interface {
	// Marshal serializes v to bytes.
	Marshal(v interface{}) ([]byte, error)
	// Unmarshal unserializes v from bytes.
	Unmarshal(data []byte, v interface{}) error
}

func NewInvoker(codec Codec) *Invoker {
	return &Invoker{codec: codec}
}

var (
	ErrorInvalidFunc    = errors.New("fn must be a function")
	ErrorDuplicatedFunc = errors.New("fn duplicated")
	ErrorNotExisted     = errors.New("fn not existed")
	ErrorArgsNotMatch   = errors.New("fn args not match")
)

// ctxType will be allowed when register fn.
var ctxType reflect.Type

func init() {
	ctx := context.TODO()
	ctxType = reflect.TypeOf(&ctx).Elem()
}

func Valid(fn interface{}) error {
	return valid(reflect.ValueOf(fn))
}

func valid(fnVal reflect.Value) error {
	if kind := fnVal.Kind(); kind != reflect.Func {
		return errors.Wrapf(ErrorInvalidFunc, "fn(kind = %s) is not func", fnVal.Kind())
	}
	fnTyp := fnVal.Type()
	for i := 0; i < fnTyp.NumIn(); i++ {
		in := fnTyp.In(i)
		if err := validArg(in); err != nil {
			return err
		}
	}
	return nil
}

func validArg(typ reflect.Type) error {
	if kind := typ.Kind(); kind == reflect.Interface && typ != ctxType {
		fmt.Println(typ, ctxType)
		return errors.Wrapf(ErrorInvalidFunc, "arg(type = %s,kind = %s) is not supported", typ, kind)
	}
	return nil
}

func IsTemperatureFnID(fnID string) bool {
	return strings.HasPrefix(fnID, "tmp-") || strings.HasPrefix(fnID, "temp-")
}

func (fm *Invoker) MarshalArgs(args ...interface{}) ([][]byte, error) {
	var argsRaw [][]byte
	for i, arg := range args {
		if i == 0 {
			if _, ok := arg.(context.Context); ok {
				continue
			}
		}
		argRaw, err := fm.codec.Marshal(arg)
		if err != nil {
			return nil, err
		}
		argsRaw = append(argsRaw, argRaw)
	}
	return argsRaw, nil
}

func (fm *Invoker) RegisterTemperature(fn interface{}) (string, error) {
	fnVal := reflect.ValueOf(fn)

	if err := valid(fnVal); err != nil {
		return "", err
	}

	var fnID string
	for {
		id := uuid.New()
		fnID = "tmp-" + hex.EncodeToString(id[:])
		if _, loaded := fm.fn.LoadOrStore(fnID, fnVal); !loaded {
			break
		}
	}
	return fnID, nil
}

func (fm *Invoker) Register(fnID string, fn interface{}) error {
	fnVal := reflect.ValueOf(fn)

	if err := valid(fnVal); err != nil {
		return err
	}

	if _, loaded := fm.fn.LoadOrStore(fnID, fnVal); loaded {
		return ErrorDuplicatedFunc
	}

	return nil
}

func (fm *Invoker) Invoke(ctx context.Context, fnID string, argsRaw ...[]byte) error {
	loadFn := fm.fn.Load
	if IsTemperatureFnID(fnID) {
		loadFn = fm.fn.LoadAndDelete
	}
	t, ok := loadFn(fnID)
	if !ok {
		return errors.Wrapf(ErrorNotExisted, "failed to get fn %s", fnID)
	}

	fnVal := t.(reflect.Value)
	fnTyp := fnVal.Type()
	var fnArgs []reflect.Value

	argsCount := fnTyp.NumIn()
	if argsCount > 0 && fnTyp.In(0) == ctxType {
		fnArgs = append(fnArgs, reflect.ValueOf(ctx))
		argsCount--
	}

	if argsCount != len(argsRaw) {

		return errors.Wrapf(ErrorArgsNotMatch, "want len(fnArgs) == %d, input len(fnArgs) == %d", argsCount, len(argsRaw))
	}

	for _, argRaw := range argsRaw {
		i := len(fnArgs)
		arg := reflect.New(fnTyp.In(i))
		obj := arg.Interface()
		if err := fm.codec.Unmarshal(argRaw, obj); err != nil {
			return errors.Wrapf(ErrorArgsNotMatch, "parsing no.%d arg: %s", i, err)
		}
		fnArgs = append(fnArgs, arg.Elem())
	}

	fnRet := fnVal.Call(fnArgs)
	if len(fnRet) > 0 {
		lastRet := fnRet[len(fnRet)-1]
		if lastRet.Kind() == reflect.Interface {
			err, ok := lastRet.Interface().(error)
			if ok {
				// return fn's error
				return err
			}
		}
	}

	return nil
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

