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

const userIDKey contextKey = "user_id"
const sourceMapKey contextKey = "source_map"

// Interceptor provides gRPC stream authentication via bearer tokens.
type Interceptor struct {
	db     *db.DB
	mu     sync.RWMutex
	tokens []db.TokenInfo
}

// NewInterceptor creates a new auth interceptor.
func NewInterceptor(database *db.DB) *Interceptor {
	return &Interceptor{db: database}
}

// LoadTokens fetches all non-expired tokens from the database into memory.
func (a *Interceptor) LoadTokens(ctx context.Context) error {
	tokens, err := a.db.GetAllTokens(ctx)
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
		userID, sourceIDs, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}
		// Resolve source codes to IDs
		sourceMap, err := a.db.GetSourceCodeToID(ss.Context(), sourceIDs)
		if err != nil {
			return status.Error(codes.Internal, "failed to resolve source codes")
		}
		ctx := context.WithValue(ss.Context(), userIDKey, userID)
		ctx = context.WithValue(ctx, sourceMapKey, sourceMap)
		wrapped := &wrappedStream{ServerStream: ss, ctx: ctx}
		return handler(srv, wrapped)
	}
}

// UnaryInterceptor returns a gRPC unary server interceptor that validates bearer tokens.
func (a *Interceptor) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		userID, sourceIDs, err := a.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		// Resolve source codes to IDs
		sourceMap, err := a.db.GetSourceCodeToID(ctx, sourceIDs)
		if err != nil {
			return nil, status.Error(codes.Internal, "failed to resolve source codes")
		}
		ctx = context.WithValue(ctx, userIDKey, userID)
		ctx = context.WithValue(ctx, sourceMapKey, sourceMap)
		return handler(ctx, req)
	}
}

func (a *Interceptor) authenticate(ctx context.Context) (int, []int, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, nil, status.Error(codes.Unauthenticated, "missing metadata")
	}
	authHeader := md.Get("authorization")
	if len(authHeader) == 0 {
		return 0, nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}
	token := strings.TrimPrefix(authHeader[0], "Bearer ")
	if token == authHeader[0] {
		return 0, nil, status.Error(codes.Unauthenticated, "invalid authorization format")
	}

	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, ti := range a.tokens {
		if err := bcrypt.CompareHashAndPassword([]byte(ti.TokenHash), []byte(token)); err == nil {
			return ti.UserID, ti.SourceIDs, nil
		}
	}
	return 0, nil, status.Error(codes.Unauthenticated, "invalid token")
}

// UserIDFromContext extracts the authenticated user ID from the context.
func UserIDFromContext(ctx context.Context) (int, bool) {
	id, ok := ctx.Value(userIDKey).(int)
	return id, ok
}

// SourceMapFromContext extracts the source_code->source_id map from the context.
func SourceMapFromContext(ctx context.Context) (map[string]int, bool) {
	m, ok := ctx.Value(sourceMapKey).(map[string]int)
	return m, ok
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
