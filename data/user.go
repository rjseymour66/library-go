package data

import "context"

var (
	// Find the user with the provided username and password
	// and returns user token
	LoginUser = loginUser

	// AuthorizeUser returns user's role if the token exists
	// if token does not exist, returns empty string
	AuthorizeUser = authorizeUser

	// Returns userID for provided token
	GetUserID = getUserID
)

func loginUser(ctx context.Context, username, password string) (response string, err error) {
	query := `
		SELECT token
		FROM library_user
		WHERE
			username = $1
			and user_password = crypt($2, user_password)`

	return executeQueryWithStringResponse(ctx, query, username, password)
}

func authorizeUser(ctx context.Context, token string) (response int64, err error) {
	query := `
		SELECT	user_role
		FROM 	library_user
		WHERE 	token = $1`

	return executeQueryWithInt64Response(ctx, query, token)
}

func getUserID(ctx context.Context, token string) (response string, err error) {
	query := `
		SELECT	user_id
		FROM	library_user
		WHERE	token = $1`

	return executeQueryWithStringResponse(ctx, query, token)
}
