package values

// ContextKeyDbRunner is a key for context.Context to extract the db runner
var ContextKeyDbRunner = contextKeyDbRunner{}

type contextKeyDbRunner struct{}
