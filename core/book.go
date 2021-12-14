package core

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/rjseymour66/library-go/util"
	"github.com/rjseymour66/library-go/values"
)

var (
	CreateBook         = createBook
	GetBook            = getBook
	GetAllBooks        = getAllBooks
	UpdateBook         = updateBook
	DeleteBook         = deleteBook
	BorrowOrReturnBook = borrowOrReturnBook
)

func createBook(ctx context.Context, requestBody io.Reader) (response interface{}, err error) {
	type createBookRequest struct {
		BookName    string
		AuthorName  string
		Publisher   string
		Description string
	}

	request := &createBookRequest{}
	err = json.NewDecoder(requestBody).Decode(request)
	if err != nil {
		cause := "Failed to decode JSON"
		err = util.NewError(cause, util.ErrorCodeInvalidJSONBody, util.ErrBadRequest, err)
		return
	}

	request.BookName = strings.TrimSpace(request.BookName)
	if request.BookName == "" {
		cause := "Trying to create book with empty name"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	request.AuthorName = strings.TrimSpace(request.AuthorName)
	if request.AuthorName == "" {
		cause := "Trying to create book with empty author name"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	request.Publisher = strings.TrimSpace(request.Publisher)
	if request.Publisher == "" {
		cause := "Trying to create book with empty publisher"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	response, err = data.CreateBook(
		ctx,
		request.BookName,
		request.AuthorName,
		request.Publisher,
		util.NewNullableString(request.Description))
	if err != nil {
		cause := "Failed to create book"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	return
}

func getBook(ctx context.Context, bookID string) (response interface{}, err error) {
	if bookID == "" {
		cause := "Invalid value for bookID parameter"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	book, err := data.GetBook(ctx, bookID)

	if err != nil {
		cause := "Failed to get book"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	if book == nil {
		cause := "Book not found"
		err = util.NewError(cause, util.ErrorCodeEntityNotFound, util.ErrResourceNotFound, err)
		return
	}

	response = book
	return
}

func getAllBooks(ctx context.Context, searchTerm string, rowOffset, rowLimit, userRole int) (response interface{}, err error) {
	if rowOffset < 0 {
		cause := "Invalid value for row offset parameter"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	if rowLimit < 0 || rowLimit > values.MaxRowLimit {
		cause := "Invalid value for row limit parameter"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	if rowLimit == 0 {
		rowLimit = values.MaxRowLimit
	}

	var books interface{}

	if userRole == values.UserRoleMember {
		books, err = data.GetAllBooksForMember(ctx, searchTerm, rowOffset, rowLimit)
	} else {
		books, err = data.GetAllBooksForLibrarian(ctx, searchTerm, rowOffset, rowLimit)
	}

	if err != nil {
		cause := "Failed to get all books"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	type metaData struct {
		SearchTerm string `json: ", omitempty"`
		RowOffset  int    `json: ", omitempty"`
		RowLimit   int
	}

	meta := &metaData{
		SearchTerm: searchTerm,
		RowOffset:  rowOffset,
		RowLimit:   rowLimit,
	}

	type getAllResponse struct {
		Data interface{} `json: "data"`
		Meta interface{} `json: "meta"`
	}

	response = &getAllResponse{
		Data: books,
		Meta: meta,
	}

	return
}

func updateBook(ctx context.Context, requestBody io.Reader) (response interface{}, err error) {
	type updateBookRequest struct {
		BookID      string
		BookName    string
		AuthorName  string
		Publisher   string
		Description string
	}

	request := &updateBookRequest{}
	err = json.NewDecoder(requestBody).Decode(request)
	if err != nil {
		cause := "Failed to decode JSON"
		err = util.NewError(cause, util.ErrorCodeInvalidJSONBody, util.ErrBadRequest, err)
		return
	}

	request.BookID = strings.TrimSpace(request.BookID)
	if request.BookID == "" {
		cause := "Invalid value for bookID parameter"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	request.BookName = strings.TrimSpace(request.BookName)
	if request.BookName == "" {
		cause := "Invalid value for book name"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	request.AuthorName = strings.TrimSpace(request.AuthorName)
	if request.AuthorName == "" {
		cause := "Invalid value for author name"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	request.Publisher = strings.TrimSpace(request.Publisher)
	if request.Publisher == "" {
		cause := "Invalid value for publisher"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	updatedAt, err := data.UpdateBook(
		ctx,
		request.BookID,
		request.BookName,
		request.AuthorName,
		request.Publisher,
		util.NewNullableString(request.Description))

	if err != nil {
		cause := "Failed to update book"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	if updatedAt == time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC) {
		cause := "Book not found"
		err = util.NewError(cause, util.ErrorCodeEntityNotFound, util.ErrResourceNotFound, err)
		return
	}

	type updateBookResponse struct {
		UpdatedAt time.Time
	}

	response = &updateBookResponse{
		UpdatedAt: updatedAt,
	}
	return
}

func deleteBook(ctx context.Context, bookID string) (err error) {

	bookID = strings.TrimSpace(bookID)

	if bookID == "" {
		cause := "Invalid value for bookID parameter"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	rowsAffected, err := data.DeleteBook(ctx, bookID)

	if err != nil {
		cause := "Failed to delete book"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	if rowsAffected == 0 {
		cause := "Book not found"
		err = util.NewError(cause, util.ErrorCodeEntityNotFound, util.ErrResourceNotFound, err)
		return
	}
	return
}

func borrowOrReturnBook(ctx context.Context, token string, requestBody io.Reader) (err error) {

	type borrowOrReturnRequest struct {
		BookID string
	}

	request := &borrowOrReturnRequest{}
	err = json.NewDecoder(requestBody).Decode(request)

	if err != nil {
		cause := "Failed to decode JSON"
		err = util.NewError(cause, util.ErrorCodeInvalidJSONBody, util.ErrBadRequest, err)
		return
	}

	request.BookID = strings.TrimSpace(request.BookID)

	if request.BookID == "" {
		cause := "Invalid value for bookID parameter"
		err = util.NewError(cause, util.ErrorCodeValidation, util.ErrBadRequest, err)
		return
	}

	status, err := data.GetBookStatus(ctx, request.BookID)

	if err != nil {
		cause := "Failed to get book status"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	if status == values.BookStatusUnkown {
		cause := "Book not found"
		err = util.NewError(cause, util.ErrorCodeEntityNotFound, util.ErrResourceNotFound, err)
		return
	}

	userUID, err := data.GetUserID(ctx, token)

	if err != nil {
		cause := "Failed to get userUID"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	// If the book is borrowed by one who did not take the error
	newStatus := values.BookStatusAvailable

	if status == values.BookStatusAvailable {
		newStatus = values.BookStatusBorrowed
	} else {
		borrowerID := ""
		borrowerID, err = data.GetBorrowerId(ctx, request.BookID)

		if err != nil {
			cause := "Failed to get borrowerID"
			err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
			return
		}

		if borrowerID != userUID {
			cause := "Book is not available"
			err = util.NewError(cause, util.ErrorCodeEntityNotFound, util.ErrResourceNotFound, err)
			return
		}

		userUID = ""
		newStatus = values.BookStatusAvailable
	}

	err = data.ChangeBookStatus(ctx, request.BookID, newStatus, util.NewNullableString(userID))

	if err != nil {
		cause := "Failed to change book status"
		err = util.NewError(cause, util.ErrorCodeInternal, util.ErrInternal, err)
		return
	}

	return
}
