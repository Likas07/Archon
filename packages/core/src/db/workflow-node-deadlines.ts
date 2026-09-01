/**
 * Database operations for per-node absolute deadlines.
 *
 * The row is created once when a node first enters running. A duplicate create
 * returns the existing row without changing its original start or deadline, so
 * restart and resume keep the same wall-clock budget.
 */
import { createLogger } from '@archon/paths';
import type { WorkflowNodeDeadlineRow } from '../schemas/workflow-node-deadline';
import { pool } from './connection';

type WorkflowNodeDeadlineKey = Pick<WorkflowNodeDeadlineRow, 'workflow_run_id' | 'node_key'>;
type CreateWorkflowNodeDeadlineParams = Pick<
  WorkflowNodeDeadlineRow,
  'workflow_run_id' | 'node_key' | 'started_at' | 'deadline_at'
>;

let cachedLog: ReturnType<typeof createLogger> | undefined;
function getLog(): ReturnType<typeof createLogger> {
  if (!cachedLog) cachedLog = createLogger('db.workflow-node-deadlines');
  return cachedLog;
}

function serializeTimestamp(timestamp: Date | string): string {
  return timestamp instanceof Date ? timestamp.toISOString() : timestamp;
}

export async function getWorkflowNodeDeadline(
  key: WorkflowNodeDeadlineKey
): Promise<WorkflowNodeDeadlineRow | null> {
  const result = await pool.query<WorkflowNodeDeadlineRow>(
    `SELECT * FROM remote_agent_workflow_node_deadlines
     WHERE workflow_run_id = $1 AND node_key = $2`,
    [key.workflow_run_id, key.node_key]
  );
  return result.rows[0] ?? null;
}

export async function createWorkflowNodeDeadline(
  params: CreateWorkflowNodeDeadlineParams
): Promise<WorkflowNodeDeadlineRow> {
  try {
    const result = await pool.query<WorkflowNodeDeadlineRow>(
      `INSERT INTO remote_agent_workflow_node_deadlines
         (workflow_run_id, node_key, started_at, deadline_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (workflow_run_id, node_key) DO NOTHING
       RETURNING *`,
      [
        params.workflow_run_id,
        params.node_key,
        serializeTimestamp(params.started_at),
        serializeTimestamp(params.deadline_at),
      ]
    );
    const inserted = result.rows[0];
    if (inserted) return inserted;

    const existing = await getWorkflowNodeDeadline(params);
    if (!existing) {
      throw new Error(
        `Workflow node deadline disappeared after conflict: ${params.workflow_run_id}/${params.node_key}`
      );
    }
    return existing;
  } catch (error) {
    getLog().error(
      {
        err: error as Error,
        workflowRunId: params.workflow_run_id,
        nodeKey: params.node_key,
      },
      'db.workflow_node_deadline_create_failed'
    );
    throw error;
  }
}

export async function expireWorkflowNodeDeadline(
  params: WorkflowNodeDeadlineKey & { expiry_reason: string }
): Promise<void> {
  try {
    // Update only if expiry_reason is currently NULL — this is first-wins behavior.
    // The first expiry records the true reason; later resume attempts preserve it.
    const result = await pool.query(
      `UPDATE remote_agent_workflow_node_deadlines
       SET expiry_reason = $3
       WHERE workflow_run_id = $1 AND node_key = $2 AND expiry_reason IS NULL`,
      [params.workflow_run_id, params.node_key, params.expiry_reason]
    );

    // If the update affected no rows, it means either the row doesn't exist at all
    // (an error) or it already has an expiry_reason set (a no-op on resume). Check which.
    if (result.rowCount === 0) {
      const existing = await getWorkflowNodeDeadline(params);
      if (!existing) {
        throw new Error(
          `Workflow node deadline not found: ${params.workflow_run_id}/${params.node_key}`
        );
      }
      // Row exists and expiry_reason is already set — this is a normal resume path
      // where the node was already expired. No-op, no error.
    }
  } catch (error) {
    getLog().error(
      {
        err: error as Error,
        workflowRunId: params.workflow_run_id,
        nodeKey: params.node_key,
      },
      'db.workflow_node_deadline_expire_failed'
    );
    throw error;
  }
}
