package auth

import (
	"context"
	"strings"
	"sync"

	"github.com/VincentChalnot/cafi/internal/db"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type contextKey string

const sourceIDKey contextKey = "source_id"

// Interceptor provides gRPC stream authentication via bearer tokens.
type Interceptor struct {
	db     *db.DB
	mu     sync.RWMutex
	tokens []db.SourceToken
}

// NewInterceptor creates a new auth interceptor.
func NewInterceptor(database *db.DB) *Interceptor {
	return &Interceptor{db: database}
}

// LoadTokens fetches all source tokens from the database into memory.
func (a *Interceptor) LoadTokens(ctx context.Context) error {
	tokens, err := a.db.GetAllSourceTokens(ctx)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.tokens = tokens
	a.mu.Unlock()
	return nil
}

// StreamInterceptor returns a gRPC stream server interceptor that validates bearer tokens.
func (a *Interceptor) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		sourceID, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}
		wrapped := &wrappedStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), sourceIDKey, sourceID)}
		return handler(srv, wrapped)
	}
}

func (a *Interceptor) authenticate(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeader := md.Get("authorization")
	if len(authHeader) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	if token == authHeader[0] {
		return "", status.Error(codes.Unauthenticated, "invalid authorization format")
	}

	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, st := range a.tokens {
		if err := bcrypt.CompareHashAndPassword([]byte(st.TokenHash), []byte(token)); err == nil {
			return st.SourceID, nil
		}
	}
	return "", status.Error(codes.Unauthenticated, "invalid token")
}

// SourceIDFromContext extracts the authenticated source ID from the context.
func SourceIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(sourceIDKey).(string)
	return id, ok
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
