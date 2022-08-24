package invoker

import (
	"context"
	"git.in.chaitin.net/dev/go/encoding"
	"git.in.chaitin.net/dev/go/errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestInvoker(t *testing.T) {
	fm := NewInvoker(encoding.JSON{})

	type CtxKeyDelta struct{}

	var err error
	var val int64
	var ctx = context.Background()

	var (
		err1 = errors.New("error1")
		err2 = errors.Errorf("error2")
	)

	err = fm.Register("fn1", func(fnCtx context.Context) error {
		val += fnCtx.Value(CtxKeyDelta{}).(int64)
		return err1
	})
	assert.NoError(t, err)
	err = fm.Register("fn2", func(delta int64) (int64, error) {
		val += delta
		return val, errors.Wrapf(err2, "warp with sth")
	})
	assert.NoError(t, err)
	type D struct {
		Delta int64 `json:"delta"`
	}
	tmpID, err := fm.RegisterTemperature(func(delta *D) error {
		val += delta.Delta
		return nil
	})
	assert.NoError(t, err)

	assert.EqualValues(t, 0, val)

	if err = fm.Invoke(context.WithValue(context.Background(), CtxKeyDelta{}, int64(1)), "fn1"); assert.ErrorIs(t, err, err1) {
		assert.EqualValues(t, 1, val)
	}
	if err = fm.Invoke(context.WithValue(context.Background(), CtxKeyDelta{}, int64(1)), "fn1"); assert.ErrorIs(t, err, err1) {
		assert.EqualValues(t, 2, val)
	}
	if err = fm.Invoke(ctx, "fn2", []byte(`2`)); assert.ErrorIs(t, err, err2) {
		assert.EqualValues(t, 4, val)
	}
	if err = fm.Invoke(ctx, "fn2", []byte(`4`)); assert.ErrorIs(t, err, err2) {
		assert.EqualValues(t, 8, val)
	}
	if err = fm.Invoke(ctx, "fn2", []byte(`4`), []byte(`4`), []byte(`4`)); assert.ErrorIs(t, err, ErrorArgsNotMatch) {
	}
	if err = fm.Invoke(ctx, "fn2", []byte(`"asd"`)); assert.ErrorIs(t, err, ErrorArgsNotMatch) {
	}
	argsRaw, err := fm.MarshalArgs(D{Delta: 8})
	if assert.NoError(t, err) {
		if err = fm.Invoke(ctx, tmpID, argsRaw...); assert.NoError(t, err) {
			assert.EqualValues(t, 16, val)
		}
		if err = fm.Invoke(ctx, tmpID, []byte(`8`)); assert.ErrorIs(t, err, ErrorNotExisted) {
		}
	}
}

func BenchmarkInvokerInvoke(b *testing.B) {
	type B struct {
		N int64 `json:"n"`
	}
	fn := func(a string, b B, c int64) (int64, error) {
		time.Sleep(time.Millisecond)
		return c + b.N, errors.New(a)
	}

	b.Run("Normal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = fn("this is a string", B{10}, 12)
		}
	})

	b.Run("Invoker", func(b *testing.B) {
		invoker := NewInvoker(encoding.JSON{})
		if err := invoker.Register("a", fn); err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = invoker.Invoke(ctx, "a", []byte(`"this is a string"`), []byte(`{"n": 10}`), []byte(`12`))
		}
	})
}
