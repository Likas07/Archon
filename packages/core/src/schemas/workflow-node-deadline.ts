/**
 * Zod schema for the persisted absolute deadline of a workflow node.
 *
 * One row exists per (workflow_run_id, node_key). The start and deadline are
 * written once when the node first enters running; a later expiry transition
 * only records expiry_reason.
 */
import { z } from '@hono/zod-openapi';

// PostgreSQL hydrates TIMESTAMPTZ as Date; SQLite returns its TEXT value.
const dbTimestampSchema = z.union([z.date(), z.string()]);

export const workflowNodeDeadlineRowSchema = z.object({
  workflow_run_id: z.string(),
  node_key: z.string(),
  started_at: dbTimestampSchema,
  deadline_at: dbTimestampSchema,
  expiry_reason: z.string().nullable(),
});

export type WorkflowNodeDeadlineRow = z.infer<typeof workflowNodeDeadlineRowSchema>;
