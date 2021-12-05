package util

import (
	"errors"
	"log"
	"net/http"
)

// Define our own errors because some errors return the same
// HTTP error status code (ex: 401).
var (
	ErrBadRequest       = errors.New("Bad Request.")
	ErrInternal         = errors.New("Internal error.")
	ErrInvalidAPICall   = errors.New("Invalid API call.")
	ErrNotAuthenticated = errors.New("Not authenticated.")
	ErrResourceNotFound = errors.New("Resource not found.")
)

// ErrorResponse is sent to clients when an error is returned.
type ErrorResponse struct {
	ErrorCode int
	Cause     string
}

// Error codes
const (
	ErrorCodeInternal           = 0
	ErroCodeInvalidJSONBody     = 30
	ErrorCodeInvalidCredentials = 201
	ErrorCodeEntityNotFound     = 404
	ErrorCodeValidation         = 500
)

// serverError represents the error that is used in the server
type serverError struct {
	code      int
	cause     string
	errorType error
}

// serverError implements the Error() interface, which has only one method named Error() that returns a string
func (e serverError) Error() string {
	return e.cause
}

var (
	// MapErrorTypeToHTTPStatus maps errors to their corresponding
	// HTTP status codes
	MapErrorTypeToHTTPStatus = mapErrorTypeToHTTPStatus

	// IsError returns the underlying error Type
	IsError = isError

	// NewError creates a new Error object
	NewError = newError
)

// mapErrorTypeToHTTPStatus maps an error to its corresponding
// HTTP Status.
func mapErrorTypeToHTTPStatus(err error) int {
	switch err {
	case ErrBadRequest:
		return http.StatusBadRequest
	case ErrInternal:
		return http.StatusInternalServerError
	case ErrInvalidAPICall, ErrResourceNotFound:
		return http.StatusNotFound
	case ErrNotAuthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

// isError returns whether the error is of type serverError. If true,
// it returns its values.
func isError(errorType error) (bool, int, string, error) {
	err, isError := errorType.(serverError)
	if !isError {
		return false, 0, "", errorType
	}
	return true, err.code, err.cause, err.errorType
}

// newError returns a serverError and logs the error that occurred. We log the
// error in case it should be kept internal, such as a database query error.
func newError(cause string, code int, errorType, err error) error {
	if err != nil {
		log.Printf("error: %v: %v", cause, err)
	} else {
		log.Printf("error: %v:", cause)
	}

	return serverError{code, cause, errorType}
}
