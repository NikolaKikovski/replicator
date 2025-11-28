package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/internal/pipeline"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

type Source struct {
	cfg        config.SourceConfig
	conn       *pgconn.PgConn
	relations  map[uint32]*pglogrepl.RelationMessage
	typeMap    *pgtype.Map
	checkpoint *pipeline.CheckpointManager
	outCh      chan<- *types.Event
}

func NewSource(cfg config.SourceConfig, cm *pipeline.CheckpointManager, out chan<- *types.Event) *Source {
	return &Source{
		cfg:        cfg,
		checkpoint: cm,
		outCh:      out,
		relations:  make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:    pgtype.NewMap(),
	}
}

func (s *Source) Start(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, s.cfg.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	s.conn = conn
	defer conn.Close(ctx)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed: %w", err)
	}
	slog.Info("System identified", "system_id", sysident.SystemID, "xlogpos", sysident.XLogPos)

	startLSN := sysident.XLogPos
	safeLSN := s.checkpoint.GetSafeLSN()
	if safeLSN > 0 {
		startLSN = pglogrepl.LSN(safeLSN)
	}

	slog.Info("Starting replication", "slot", s.cfg.SlotName, "start_lsn", startLSN)
	err = pglogrepl.StartReplication(ctx, conn, s.cfg.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + s.cfg.Publication + "'"},
	})
	if err != nil {
		return fmt.Errorf("StartReplication failed: %w", err)
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.sendStandbyStatus(ctx); err != nil {
				slog.Error("Failed to send heartbeat", "error", err)
			}
		default:
			ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
			msg, err := conn.ReceiveMessage(ctxTimeout)
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				return fmt.Errorf("ReceiveMessage failed: %w", err)
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						slog.Error("ParsePrimaryKeepaliveMessage failed", "error", err)
						continue
					}
					if pkm.ReplyRequested {
						s.sendStandbyStatus(ctx)
					}
				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						slog.Error("ParseXLogData failed", "error", err)
						continue
					}
					if err := s.handleLogicalMsg(xld); err != nil {
						slog.Error("Handle logical msg failed", "error", err)
					}
				}
			default:
				slog.Debug("Received unexpected message", "type", fmt.Sprintf("%T", msg))
			}
		}
	}
}

func (s *Source) sendStandbyStatus(ctx context.Context) error {
	safeLSN := s.checkpoint.GetSafeLSN()
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: pglogrepl.LSN(safeLSN),
		WALFlushPosition: pglogrepl.LSN(safeLSN),
		WALApplyPosition: pglogrepl.LSN(safeLSN),
		ClientTime:       time.Now(),
		ReplyRequested:   false,
	})
}

func (s *Source) handleLogicalMsg(xld pglogrepl.XLogData) error {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return err
	}

	s.checkpoint.Track(types.LSN(xld.WALStart))

	switch logicalMsg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		s.relations[logicalMsg.RelationID] = logicalMsg
	case *pglogrepl.InsertMessage:
		rel, ok := s.relations[logicalMsg.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		vals, err := decodeTuple(logicalMsg.Tuple, rel, s.typeMap)
		if err != nil {
			return err
		}
		s.outCh <- &types.Event{
			Type:      types.EventInsert,
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			Columns:   vals,
			LSN:       types.LSN(xld.WALStart),
			Timestamp: xld.ServerTime,
		}
	case *pglogrepl.UpdateMessage:
		rel, ok := s.relations[logicalMsg.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		vals, err := decodeTuple(logicalMsg.NewTuple, rel, s.typeMap)
		if err != nil {
			return err
		}
		s.outCh <- &types.Event{
			Type:      types.EventUpdate,
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			Columns:   vals,
			LSN:       types.LSN(xld.WALStart),
			Timestamp: xld.ServerTime,
		}
	case *pglogrepl.DeleteMessage:
		rel, ok := s.relations[logicalMsg.RelationID]
		if !ok {
			return fmt.Errorf("unknown relation ID %d", logicalMsg.RelationID)
		}
		// OLD tuple is usually in logicalMsg.OldTuple, but depends on REPLICA IDENTITY
		// For now, assume we have it.
		vals, err := decodeTuple(logicalMsg.OldTuple, rel, s.typeMap)
		if err != nil {
			return err
		}
		s.outCh <- &types.Event{
			Type:      types.EventDelete,
			Schema:    rel.Namespace,
			Table:     rel.RelationName,
			Identity:  vals,
			LSN:       types.LSN(xld.WALStart),
			Timestamp: xld.ServerTime,
		}
	}
	return nil
}
