import { afterAll, beforeAll, beforeEach, describe, expect, mock, test } from 'bun:test';
import { SqliteAdapter } from './adapters/sqlite';
import { workflowNodeDeadlineRowSchema } from '../schemas/workflow-node-deadline';

const db = new SqliteAdapter(':memory:');

mock.module('./connection', () => ({
  pool: { query: db.query.bind(db) },
}));

const { createWorkflowNodeDeadline, expireWorkflowNodeDeadline, getWorkflowNodeDeadline } =
  await import('./workflow-node-deadlines');

const key = { workflow_run_id: 'run-1', node_key: 'planner' };
const startedAt = '2026-09-01T12:00:00.000Z';
const deadlineAt = '2026-09-01T12:30:00.000Z';

beforeAll(async () => {
  await db.query(
    `INSERT INTO remote_agent_conversations
       (id, platform_type, platform_conversation_id)
     VALUES ($1, $2, $3)`,
    ['conv-1', 'web', 'deadline-test']
  );
  await db.query(
    `INSERT INTO remote_agent_workflow_runs
       (id, workflow_name, conversation_id, user_message)
     VALUES ($1, $2, $3, $4)`,
    [key.workflow_run_id, 'deadline-test', 'conv-1', 'test']
  );
});

beforeEach(async () => {
  await db.query('DELETE FROM remote_agent_workflow_node_deadlines');
});

afterAll(async () => {
  await db.close();
});

describe('workflow-node-deadlines', () => {
  test('create then read returns the original start and absolute deadline', async () => {
    const created = await createWorkflowNodeDeadline({
      ...key,
      started_at: startedAt,
      deadline_at: deadlineAt,
    });
    const read = await getWorkflowNodeDeadline(key);

    expect(read).toEqual(created);
    expect(read?.started_at).toBe(startedAt);
    expect(read?.deadline_at).toBe(deadlineAt);
    expect(workflowNodeDeadlineRowSchema.safeParse(read).success).toBe(true);
  });

  test('a second create returns the existing row without moving the deadline', async () => {
    const original = await createWorkflowNodeDeadline({
      ...key,
      started_at: startedAt,
      deadline_at: deadlineAt,
    });
    const repeated = await createWorkflowNodeDeadline({
      ...key,
      started_at: '2026-09-01T12:10:00.000Z',
      deadline_at: '2026-09-01T12:40:00.000Z',
    });

    expect(repeated).toEqual(original);
    expect(repeated.started_at).toBe(startedAt);
    expect(repeated.deadline_at).toBe(deadlineAt);
    expect(await getWorkflowNodeDeadline(key)).toEqual(original);
  });

  test('recording an expiry reason persists it', async () => {
    await createWorkflowNodeDeadline({
      ...key,
      started_at: startedAt,
      deadline_at: deadlineAt,
    });

    await expireWorkflowNodeDeadline({ ...key, expiry_reason: 'absolute_deadline_exceeded' });

    expect((await getWorkflowNodeDeadline(key))?.expiry_reason).toBe('absolute_deadline_exceeded');
  });

  test('an unexpired row reads expiry_reason as null, never undefined', async () => {
    await createWorkflowNodeDeadline({
      ...key,
      started_at: startedAt,
      deadline_at: deadlineAt,
    });

    const read = await getWorkflowNodeDeadline(key);
    expect(read).not.toBeNull();
    expect(read?.expiry_reason).toBeNull();
    expect(read && 'expiry_reason' in read).toBe(true);
  });

  test('expiring an already-expired deadline preserves the original expiry_reason', async () => {
    // Setup: create a deadline and expire it once with a reason
    await createWorkflowNodeDeadline({
      ...key,
      started_at: startedAt,
      deadline_at: deadlineAt,
    });

    const originalReason = 'absolute_deadline_exceeded';
    await expireWorkflowNodeDeadline({ ...key, expiry_reason: originalReason });

    // Verify it was set
    expect((await getWorkflowNodeDeadline(key))?.expiry_reason).toBe(originalReason);

    // Now try to expire it again with a different reason (as happens on resume)
    const newReason = 'resumed_deadline_exceeded';
    await expireWorkflowNodeDeadline({ ...key, expiry_reason: newReason });

    // The original reason should still be there, not overwritten
    const result = await getWorkflowNodeDeadline(key);
    expect(result?.expiry_reason).toBe(originalReason);
    expect(result?.expiry_reason).not.toBe(newReason);
  });

  test('expiring a missing deadline still throws', async () => {
    // Don't create the deadline, just try to expire a non-existent one
    const missingKey = { workflow_run_id: 'run-missing', node_key: 'missing_node' };

    try {
      await expireWorkflowNodeDeadline({ ...missingKey, expiry_reason: 'test_reason' });
      throw new Error('Should have thrown for missing deadline');
    } catch (err: unknown) {
      // Expected: the error message should indicate the deadline was not found
      const error = err as Error;
      expect(error.message).toContain('not found');
      expect(error.message).toContain(missingKey.workflow_run_id);
    }
  });
});
