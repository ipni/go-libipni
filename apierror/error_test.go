package apierror_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/ipni/go-libipni/apierror"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	err := apierror.New(errors.New("test error"), 0)
	require.Equal(t, "test error", err.Error())

	err = apierror.New(nil, http.StatusNotFound)
	require.Equal(t, fmt.Sprintf("%d %s", http.StatusNotFound, http.StatusText(http.StatusNotFound)), err.Error())

	err = apierror.New(nil, 0)
	require.Equal(t, "", err.Error())

	err = apierror.New(nil, 999)
	require.Equal(t, "999", err.Error())
}

func TestFromResponse(t *testing.T) {
	err := apierror.FromResponse(0, []byte(" hello world\n"))
	require.Equal(t, "hello world", err.Error())

	err = apierror.FromResponse(http.StatusTeapot, []byte(" hello world\n"))
	require.Equal(t, "hello world", err.Error())

	ae, ok := err.(*apierror.Error)
	require.True(t, ok)
	require.Equal(t, http.StatusTeapot, ae.Status())

	err = apierror.FromResponse(http.StatusTeapot, nil)
	require.Equal(t, fmt.Sprintf("%d %s", http.StatusTeapot, http.StatusText(http.StatusTeapot)), err.Error())
}

func TestEncodeDecode(t *testing.T) {
	data := apierror.EncodeError(nil)
	require.Nil(t, data)

	derr := apierror.DecodeError(nil)
	require.Nil(t, derr)

	derr = apierror.DecodeError([]byte("hello world"))
	require.ErrorContains(t, derr, "cannot decode error message")

	err := apierror.New(errors.New("cannot find it"), http.StatusNotFound)
	data = apierror.EncodeError(err)

	derr = apierror.DecodeError(data)
	require.Equal(t, "cannot find it", derr.Error())

	ae, ok := derr.(*apierror.Error)
	require.True(t, ok)
	require.Equal(t, http.StatusNotFound, ae.Status())
	require.Equal(t, fmt.Sprintf("%d %s: cannot find it", http.StatusNotFound, http.StatusText(http.StatusNotFound)), ae.Text())

	someErr := errors.New("some error")
	data = apierror.EncodeError(someErr)

	derr = apierror.DecodeError(data)
	require.Equal(t, "some error", derr.Error())
	_, ok = derr.(*apierror.Error)
	require.False(t, ok)
}

func TestUnwrap(t *testing.T) {
	errEOF := errors.New("end of file")
	err := apierror.New(errEOF, 0)
	require.ErrorIs(t, err, errEOF)
}
