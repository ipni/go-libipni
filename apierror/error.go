package apierror

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Error is the type of error returned by a network client. It contains an HTTP
// status code so that API clients can interpret the error emssage.
type Error struct {
	err    error
	status int
}

type ErrorMessage struct {
	Message string `json:",omitempty"`
	Status  int    `json:",omitempty"`
}

var serverError []byte

func init() {
	// Make sure there is always an error to return in case encoding fails
	e := ErrorMessage{
		Message: http.StatusText(http.StatusInternalServerError),
	}

	eb, err := json.Marshal(&e)
	if err != nil {
		panic(err)
	}
	serverError = eb
}

func New(err error, status int) *Error {
	return &Error{
		err:    err,
		status: status,
	}
}

func FromResponse(status int, body []byte) error {
	var err error
	text := strings.TrimSpace(string(body))
	if text != "" {
		err = errors.New(text)
	}
	if status == 0 {
		return err
	}
	return New(err, status)
}

func (e *Error) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	if e.status == 0 {
		return ""
	}
	// If there is only status, then return status text
	if text := http.StatusText(e.status); text != "" {
		return fmt.Sprintf("%d %s", e.status, text)
	}
	return fmt.Sprintf("%d", e.status)
}

func (e *Error) Status() int {
	return e.status
}

func (e *Error) Text() string {
	parts := make([]string, 0, 5)
	if e.status != 0 {
		parts = append(parts, fmt.Sprintf("%d", e.status))
		text := http.StatusText(e.status)
		if text != "" {
			parts = append(parts, " ")
			parts = append(parts, text)
		}
	}
	if e.err != nil {
		if len(parts) != 0 {
			parts = append(parts, ": ")
		}
		parts = append(parts, e.err.Error())
	}

	return strings.Join(parts, "")
}

func (e *Error) Unwrap() error {
	return e.err
}

func EncodeError(err error) []byte {
	if err == nil {
		return nil
	}

	e := ErrorMessage{
		Message: err.Error(),
	}
	var apierr *Error
	if errors.As(err, &apierr) {
		e.Status = apierr.Status()
	}

	data, err := json.Marshal(&e)
	if err != nil {
		return serverError
	}
	return data
}

func DecodeError(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var e ErrorMessage
	err := json.Unmarshal(data, &e)
	if err != nil {
		return fmt.Errorf("cannot decode error message: %s", err)
	}

	err = errors.New(e.Message)
	if e.Status == 0 {
		return err
	}
	return New(err, e.Status)
}
