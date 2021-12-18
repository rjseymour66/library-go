package data

import (
	"context"
	"time"

	"github.com/rjseymour66/library-go/util"
	"github.com/rjseymour66/library-go/values"
)

type BookEntity struct {
	BookID      string
	BookName    string
	AuthorName  string
	Publisher   string
	Description string `json:", omitempty"`
	Status      int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	BorrowerID  string `json:", omitempty"`
}

type BookDetails struct {
	BookID      string
	BookName    string
	AuthorName  string
	Publisher   string
	Description string `json:", omitempty"`
}

type BookInfoLibrarian struct {
	BookID     string
	BookName   string
	AuthorName string
	Publisher  string
	Status     int64
	Borrower   string `json:", omitempty"`
}

type BookInfoMember struct {
	BookID     string
	BookName   string
	AuthorName string
	Publisher  string
}

var (
	CreateBook              = createBook
	GetBook                 = getBook
	GetAllBooksForMember    = getAllBooksForMember
	GetAllBooksForLibrarian = getAllBooksForLibrarian
	UpdateBook              = updateBook
	DeleteBook              = deleteBook
	GetBookStatus           = getBookStatus
	GetBorrowerID           = getBorrowerID
	ChangeBookStatus        = changeBookStatus
)

func createBook(ctx context.Context, bookName, authorName, publisher string, description util.NullString) (response *BookEntity, err error) {
	dbRunner := ctx.Value(values.ContextKeyDbRunner).(dbserver.Runner)
	query := `
		INSERT into book(
			book_name, author_name, publisher, book_description)
		values ($1 $2 $3 $4)
		returning book_id, created_at`

	rows, err = dbRunner.Query(ctx, query, bookName, authorName, publisher, description)

	if err != nil {
		return
	}

	defer rows.Close()

	rr, err := dbserver.GetRowReader(rows)
	if err != nil {
		return
	}

	if rr.ScanNext() {
		response = &BookEntity{}
		response.BookID = rr.ReadByIdxString(0)
		response.BookName = bookName
		response.AuthorName = authorName
		response.Publisher = publisher
		response.Description = util.GetNullStringValue(description)
		response.Status = values.BookStatusAvailable
		response.CreatedAt = rr.ReadByIdxTime(1)
		response.UpdatedAt = rr.ReadByIdxTime(1)
		response.BorrowerID = ""
	}

	err = rr.Error()

	return
}

func getBook(ctx context.Context, bookID string) (response *BookDetails, err error) {
	dbRunner := ctx.Value(values.ContextKeyDbRunner).(dbserver.Runner)

	query := `
		SELECT
			book_id as "BookID",
			book_name as "BookName",
			author_name as "AuthorName",
			publisher as "Publisher",
			book_description as "Description"
		FROM book
		WHERE book_id = $1`

	rows, err := dbRunner.Query(ctx, query, bookID)
	if err != nil {
		return
	}

	defer rows.Close()

	rr, err := dbserver.GetRowReader(rows)
	if err != nil {
		return
	}

	if rr.ScanNext() {
		response = &BookDetails{}
		rr.ReadAllToStruct(response)
	}

	err = rr.Error()

	return
}

func getAllBooksForMember(ctx context.Context, searchTerm string, rowOffset, rowLimit int) (response []*BookInfoMember, err error) {
	dbRunner := ctx.Value(values.ContextKeyDbRunner).(dbserver.Runner)

	query := `
		SELECT
			book_id as "BookID",
			book_name as "BookName",
			author_name as "AuthorName",
			publisher as "Publisher"
		FROM book
		WHERE book_name like '%%' || $1 || '%%' and book_status = $2
		OFFSET $3
		LIMIT $4`

	rows, err := dbRunner.Query(ctx, query, searchTerm, values.BookStatusAvailable, rowOffset, rowLimit)

	if err != nil {
		return
	}

	response = make([]*BookInfoMember, 0)
	for rr.ScanNext() {
		book := &BookInfoMember{}
		rr.ReadAllToStruct(book)
		response = append(response, book)
	}

	err = rr.Error()

	return
}

func getAllBooksForLibrarian(
	ctx context.Context, 
	searchTerm string, 
	rowOffset, 
	rowLimit int) (response []*BookInfoLibrarian, err error) {
	dbRunner := ctx.Value(values.ContextKeyDbRunner).(dbserver.Runner)

	query := `
		SELECT
			b.book_id as "BookID",
			b.book_name as "BookName",
			b.author_name as "AuthorName",
			b.publisher as "Publisher",
			b.book_status as "Status",
			u.full_name as "Borrower"
		FROM book b
		LEFT JOIN library_user u on u.user_id = b.borrower_id
		WHERE b.book_name LIKE '%%' || $1 || '%%'
		OFFSET $2
		LIMIT $3`

	rows, err := dbRunner.Query(ctx, query, searchTerm, rowOffset, rowLimit)
	if err != nil {
		return
	}

	defer rows.Close()

	rr, err :+ dbserver.GetRowReader(rows)
	if err != nil {
		return
	}

	response = make([]*BookInfoLibrarian, 0)
	for rr.ScanNext() {
		book :+ &BookInfoLibrarian{}
		rr.ReadAllToStruct(book)
		response = append(response, book)
	}

	err = rr.Error()
	return
}

func updateBook(
		ctx context.Context, 
		bookID, 
		bookName, 
		authorName, 
		publisher string, 
		description util.NullString
	) (response time.Time, err error) {
	query := `
		UPDATE book
		SET
			book_name = $1,
			author_name = $2,
			publisher = $3,
			book_description = $4
		WHERE book_id = $5
		RETURNING updated_at`

	return executeQueryWithTimeResponse(
		ctx, 
		query, 
		bookName, 
		authorName, 
		publisher, 
		description, 
		bookID
	)

}

func deleteBook(ctx context.Context, bookID string) (response int64, err error) {
	query := `DELETE FROM book WHERE book_id = $1`
	return executeQueryWithRowsAffected(ctx, query, bookID)
}

func getBookStatus(ctx context.Context, bookID string) (response int64, err error) {
	query := `SELECT book_status FROM book WHERE book_id = $1`
	return executeQueryWithInt64Response(ctx, query, bookID)
}

func changeBookStatus(ctx context.Context, bookID string, status int, userID util.NullString) (err error) {
	dbRunner := ctx.Value(values.ContextKeyDbRunner).(dbserver.Runner)

	query := `
		UPDATE book
		SET
			book_status = $1,
			borrower_id = $2
		WHERE book_id = $3`

	_, err = dbRunner.Exec(ctx, query, status, userID, bookID)

	return
}

func getBorrower(ctx context.Context, bookID string) (response string, err error) {
	query := `SELECT borrower_id FROM book WHERE book_id = $1`
	return executeQueryWithStringResponse(ctx, query, bookID)
}