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
});
