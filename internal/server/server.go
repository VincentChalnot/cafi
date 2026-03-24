package server

import (
	"context"
	"io"
	"log"
	"path"

	"github.com/VincentChalnot/cafi/internal/auth"
	"github.com/VincentChalnot/cafi/internal/db"
	cafiv1 "github.com/VincentChalnot/cafi/internal/proto/cafi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IndexerServer implements the Indexer gRPC service.
type IndexerServer struct {
	cafiv1.UnimplementedIndexerServer
	DB      *db.DB
	Verbose bool
}

// NewIndexerServer creates a new IndexerServer with the given database.
func NewIndexerServer(database *db.DB, verbose bool) *IndexerServer {
	return &IndexerServer{DB: database, Verbose: verbose}
}

// Sync handles the bidirectional streaming Sync RPC.
func (s *IndexerServer) Sync(stream cafiv1.Indexer_SyncServer) error {
	ctx := stream.Context()

	_, ok := auth.UserIDFromContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "user_id not found in context")
	}

	sourceMap, ok := auth.SourceMapFromContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "source_map not found in context")
	}

	// Expect Handshake as first message
	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	if s.Verbose {
		log.Printf("Received: %v", firstMsg)
	}
	hs := firstMsg.GetHandshake()
	if hs == nil {
		return status.Error(codes.InvalidArgument, "first message must be a Handshake")
	}

	// Validate the handshake source_code against authenticated sources
	hsSourceCode := hs.GetSourceCode()
	if _, ok := sourceMap[hsSourceCode]; !ok {
		return sendFatalSyncError(stream, "source_code not accessible via this token")
	}
	log.Printf("Sync started: source=%s version=%s", hsSourceCode, hs.GetClientVersion())

	// Process file events
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if s.Verbose {
			log.Printf("Received: %v", msg)
		}

		fe := msg.GetFileEvent()
		if fe == nil {
			if msg.GetEndOfQueue() != nil {
				// Strategy: remote will send pull requests here in the future.
				// For now, just send STOP.
				if s.Verbose {
					log.Printf("Received EndOfQueue, sending Stop")
				}
				stopMsg := &cafiv1.ServerMessage{
					Message: &cafiv1.ServerMessage_Stop{
						Stop: &cafiv1.Stop{},
					},
				}
				return stream.Send(stopMsg)
			}
			continue
		}

		// Resolve source_code for this event
		sourceCode := fe.GetSourceCode()
		if sourceCode == "" {
			sourceCode = hsSourceCode
		}
		sourceID, ok := sourceMap[sourceCode]
		if !ok {
			sendErr := stream.Send(&cafiv1.ServerMessage{
				Message: &cafiv1.ServerMessage_SyncError{
					SyncError: &cafiv1.SyncError{Message: "source_code not accessible via this token", Fatal: true},
				},
			})
			if sendErr != nil {
				return sendErr
			}
			return status.Error(codes.PermissionDenied, "source_code not accessible via this token")
		}

		switch fe.GetEventType() {
		case cafiv1.EventType_EVENT_TYPE_UPSERT:
			if err := s.handleUpsert(ctx, sourceID, fe); err != nil {
				log.Printf("Error handling upsert for %s: %v", fe.GetPath(), err)
				sendErr := stream.Send(&cafiv1.ServerMessage{
					Message: &cafiv1.ServerMessage_SyncError{
						SyncError: &cafiv1.SyncError{Message: err.Error(), Fatal: false},
					},
				})
				if sendErr != nil {
					return sendErr
				}
				if s.Verbose {
					log.Printf("Sent error for %s: %v", fe.GetPath(), err)
				}
				continue
			}
		case cafiv1.EventType_EVENT_TYPE_DELETED:
			if err := s.handleDelete(ctx, sourceID, fe); err != nil {
				log.Printf("Error handling delete for %s: %v", fe.GetPath(), err)
				sendErr := stream.Send(&cafiv1.ServerMessage{
					Message: &cafiv1.ServerMessage_SyncError{
						SyncError: &cafiv1.SyncError{Message: err.Error(), Fatal: false},
					},
				})
				if sendErr != nil {
					return sendErr
				}
				if s.Verbose {
					log.Printf("Sent error for %s: %v", fe.GetPath(), err)
				}
				continue
			}
		default:
			sendErr := stream.Send(&cafiv1.ServerMessage{
				Message: &cafiv1.ServerMessage_SyncError{
					SyncError: &cafiv1.SyncError{Message: "unknown event type", Fatal: false},
				},
			})
			if sendErr != nil {
				return sendErr
			}
			continue
		}

		// Send ACK
		serverMsg := &cafiv1.ServerMessage{
			Message: &cafiv1.ServerMessage_EventAck{
				EventAck: &cafiv1.EventAck{
					Blake3: fe.GetBlake3(),
					Path:   fe.GetPath(),
				},
			},
		}
		if err := stream.Send(serverMsg); err != nil {
			return err
		}
		if s.Verbose {
			log.Printf("Sent: %v", serverMsg)
		}
	}
}

func sendFatalSyncError(stream cafiv1.Indexer_SyncServer, message string) error {
	_ = stream.Send(&cafiv1.ServerMessage{
		Message: &cafiv1.ServerMessage_SyncError{
			SyncError: &cafiv1.SyncError{Message: message, Fatal: true},
		},
	})
	return status.Error(codes.PermissionDenied, message)
}

func (s *IndexerServer) handleUpsert(ctx context.Context, sourceID int, fe *cafiv1.FileEvent) error {
	blobID, err := s.DB.UpsertBlob(ctx, fe.GetBlake3(), fe.GetMimeType(), fe.GetSize())
	if err != nil {
		return err
	}
	folder, filename := path.Split(fe.GetPath())
	return s.DB.UpsertFilePath(ctx, sourceID, folder, filename, blobID, fe.GetMtime())
}

func (s *IndexerServer) handleDelete(ctx context.Context, sourceID int, fe *cafiv1.FileEvent) error {
	folder, filename := path.Split(fe.GetPath())
	return s.DB.MarkFileDeleted(ctx, sourceID, folder, filename)
}

// Ping verifies connectivity and authentication.
func (s *IndexerServer) Ping(ctx context.Context, _ *cafiv1.PingRequest) (*cafiv1.PingResponse, error) {
	if _, ok := auth.UserIDFromContext(ctx); !ok {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated")
	}
	// No payload needed, returning empty response indicates success
	return &cafiv1.PingResponse{}, nil
}
