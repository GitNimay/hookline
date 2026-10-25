package app

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	pb "github.com/GitNimay/hookline/gen/hookline/v1"
	"github.com/GitNimay/hookline/internal/domain"
	"github.com/GitNimay/hookline/internal/queue"
)

// jobRef identifies a delivery to publish.
type jobRef struct {
	ID, TenantID, EventID, EndpointID uuid.UUID
	AttemptCount                      int32
}

// publishJob publishes the job for the delivery's next attempt. nonce makes the NATS
// message ID unique per schedule, so a legitimate republish is not deduplicated away.
func publishJob(ctx context.Context, q *queue.Queue, r jobRef, nonce string) error {
	dlv := domain.FormatID(domain.PrefixDelivery, r.ID)
	tenant := domain.FormatID(domain.PrefixTenant, r.TenantID)
	job := &pb.DeliveryJob{
		DeliveryId: dlv,
		EventId:    domain.FormatID(domain.PrefixEvent, r.EventID),
		EndpointId: domain.FormatID(domain.PrefixEndpoint, r.EndpointID),
		TenantId:   tenant,
		Attempt:    r.AttemptCount + 1,
	}
	return q.PublishDelivery(ctx, tenant, job, domain.JobMsgID(dlv, job.Attempt, nonce))
}

func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}
