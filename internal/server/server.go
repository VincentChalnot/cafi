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
	DB *db.DB
}

// NewIndexerServer creates a new IndexerServer with the given database.
func NewIndexerServer(database *db.DB) *IndexerServer {
	return &IndexerServer{DB: database}
}

// Sync handles the bidirectional streaming Sync RPC.
func (s *IndexerServer) Sync(stream cafiv1.Indexer_SyncServer) error {
	ctx := stream.Context()

	sourceID, ok := auth.SourceIDFromContext(ctx)
	if !ok {
		return status.Error(codes.Internal, "source_id not found in context")
	}

	// Expect Handshake as first message
	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	hs := firstMsg.GetHandshake()
	if hs == nil {
		return status.Error(codes.InvalidArgument, "first message must be a Handshake")
	}
	log.Printf("Sync started: source=%s version=%s", sourceID, hs.GetClientVersion())

	// Validate that the handshake source_id matches the authenticated source
	if hs.GetSourceId() != sourceID {
		return status.Error(codes.PermissionDenied, "source_id mismatch")
	}

	// Process file events
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		fe := msg.GetFileEvent()
		if fe == nil {
			continue
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
		if err := stream.Send(&cafiv1.ServerMessage{
			Message: &cafiv1.ServerMessage_EventAck{
				EventAck: &cafiv1.EventAck{
					Blake3: fe.GetBlake3(),
					Path:   fe.GetPath(),
				},
			},
		}); err != nil {
			return err
		}
	}
}

func (s *IndexerServer) handleUpsert(ctx context.Context, sourceID string, fe *cafiv1.FileEvent) error {
	blobID, err := s.DB.UpsertBlob(ctx, fe.GetBlake3(), fe.GetMimeType(), fe.GetSize())
	if err != nil {
		return err
	}
	folder, filename := path.Split(fe.GetPath())
	return s.DB.UpsertFilePath(ctx, sourceID, folder, filename, blobID, fe.GetMtime())
}

func (s *IndexerServer) handleDelete(ctx context.Context, sourceID string, fe *cafiv1.FileEvent) error {
	folder, filename := path.Split(fe.GetPath())
	return s.DB.MarkFileDeleted(ctx, sourceID, folder, filename)
}
