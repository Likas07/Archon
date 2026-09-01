import { describe, expect, mock, test } from 'bun:test';
import { NODE_DEADLINE_EXCEEDED, runNodeRetryLoop } from './dag-executor';
import { parseWorkflow } from './loader';
import type { WorkflowClock, WorkflowClockWaitResult } from './clock';
import type { IWorkflowPlatform } from './deps';
import type { DagNode, NodeOutput, WorkflowRun } from './schemas';
import type { IWorkflowStore } from './store';

interface ScheduledWait {
  at: number;
  signal?: AbortSignal;
  resolve(result: WorkflowClockWaitResult): void;
  onAbort?: () => void;
}

class FakeWorkflowClock implements WorkflowClock {
  private currentMs: number;
  private readonly waits = new Set<ScheduledWait>();
  nowCalls = 0;

  constructor(nowMs: number) {
    this.currentMs = nowMs;
  }

  now(): number {
    this.nowCalls++;
    return this.currentMs;
  }

  wait(ms: number, signal?: AbortSignal): Promise<WorkflowClockWaitResult> {
    if (signal?.aborted) return Promise.resolve('aborted');
    if (ms <= 0) return Promise.resolve('elapsed');

    return new Promise(resolve => {
      const scheduled: ScheduledWait = {
        at: this.currentMs + ms,
        signal,
        resolve,
      };
      const onAbort = (): void => {
        this.finishWait(scheduled, 'aborted');
      };
      scheduled.onAbort = onAbort;
      signal?.addEventListener('abort', onAbort, { once: true });
      this.waits.add(scheduled);
    });
  }

  advanceBy(ms: number): void {
    this.currentMs += ms;
    const due = [...this.waits]
      .filter(wait => wait.at <= this.currentMs)
      .sort((a, b) => a.at - b.at);
    for (const wait of due) this.finishWait(wait, 'elapsed');
  }

  pendingWaitCount(): number {
    return this.waits.size;
  }

  private finishWait(wait: ScheduledWait, result: WorkflowClockWaitResult): void {
    if (!this.waits.delete(wait)) return;
    if (wait.onAbort) wait.signal?.removeEventListener('abort', wait.onAbort);
    wait.resolve(result);
  }
}

type DeadlineRow = Awaited<ReturnType<IWorkflowStore['createWorkflowNodeDeadline']>>;
type DeadlineStore = Pick<
  IWorkflowStore,
  'createWorkflowNodeDeadline' | 'expireWorkflowNodeDeadline' | 'createWorkflowEvent'
>;

function createDeadlineStore(initialRow?: DeadlineRow): {
  store: DeadlineStore;
  createWorkflowNodeDeadline: ReturnType<typeof mock>;
  expireWorkflowNodeDeadline: ReturnType<typeof mock>;
  createWorkflowEvent: ReturnType<typeof mock>;
  getRow(): DeadlineRow | undefined;
} {
  let row = initialRow;
  const createWorkflowNodeDeadline = mock(
    (params: Parameters<IWorkflowStore['createWorkflowNodeDeadline']>[0]) => {
      row ??= { ...params, expiry_reason: null };
      return Promise.resolve(row);
    }
  );
  const expireWorkflowNodeDeadline = mock(
    (params: Parameters<IWorkflowStore['expireWorkflowNodeDeadline']>[0]) => {
      if (row) row = { ...row, expiry_reason: params.expiry_reason };
      return Promise.resolve();
    }
  );
  const createWorkflowEvent = mock(
    (_params: Parameters<IWorkflowStore['createWorkflowEvent']>[0]) => Promise.resolve()
  );
  return {
    store: {
      createWorkflowNodeDeadline,
      expireWorkflowNodeDeadline,
      createWorkflowEvent,
    },
    createWorkflowNodeDeadline,
    expireWorkflowNodeDeadline,
    createWorkflowEvent,
    getRow: () => row,
  };
}

const platform: IWorkflowPlatform = {
  sendMessage: mock(() => Promise.resolve()),
  getStreamingMode: () => 'batch',
  getPlatformType: () => 'test',
};

const workflowRun: WorkflowRun = {
  id: 'run-deadline',
  workflow_name: 'deadline-test',
  conversation_id: 'conversation',
  parent_conversation_id: null,
  codebase_id: null,
  user_id: null,
  parent_run_id: null,
  status: 'running',
  user_message: 'test',
  metadata: {},
  started_at: new Date(0),
  completed_at: null,
  last_activity_at: null,
  working_path: null,
  output_root: null,
};

const deadlineOutput: NodeOutput = {
  state: 'failed',
  output: '',
  error: NODE_DEADLINE_EXCEEDED,
};
const initialOutput: NodeOutput = {
  state: 'failed',
  output: '',
  error: 'Node did not execute',
};
const noRetries = { maxRetries: 0, delayMs: 0, onError: 'transient' as const };

function promptNode(timeout?: number): DagNode {
  return timeout === undefined
    ? { id: 'prompt', prompt: 'do it' }
    : { id: 'prompt', prompt: 'do it', timeout };
}

function loopNode(timeout: number): DagNode {
  return {
    id: 'loop',
    timeout,
    loop: { prompt: 'iterate', until: 'DONE', max_iterations: 2 },
  };
}

function loopGroupNode(timeout: number): DagNode {
  return {
    id: 'group',
    timeout,
    loop_group: {
      until: 'DONE',
      max_iterations: 2,
      nodes: [{ id: 'body', prompt: 'iterate' }],
    },
  };
}

async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe('absolute node deadline schedules', () => {
  test('expiry mid-attempt aborts in-flight work and returns deadline_exceeded', async () => {
    const clock = new FakeWorkflowClock(0);
    const deadlineStore = createDeadlineStore();
    let abortObserved = false;
    let attempts = 0;

    const execution = runNodeRetryLoop(
      promptNode(100),
      platform,
      'conversation',
      workflowRun,
      noRetries,
      signal => {
        attempts++;
        signal?.addEventListener(
          'abort',
          () => {
            abortObserved = signal.reason === NODE_DEADLINE_EXCEEDED;
          },
          { once: true }
        );
        return new Promise<NodeOutput>(() => undefined);
      },
      initialOutput,
      {
        store: deadlineStore.store,
        nodeKey: 'prompt',
        clock,
        deadlineOutput,
      }
    );

    await flushMicrotasks();
    expect(attempts).toBe(1);
    clock.advanceBy(100);
    const output = await execution;

    expect(output).toEqual(deadlineOutput);
    expect(abortObserved).toBe(true);
    expect(deadlineStore.expireWorkflowNodeDeadline).toHaveBeenCalledWith({
      workflow_run_id: workflowRun.id,
      node_key: 'prompt',
      expiry_reason: NODE_DEADLINE_EXCEEDED,
    });
    expect(deadlineStore.createWorkflowEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        event_type: 'node_failed',
        data: expect.objectContaining({
          reason: NODE_DEADLINE_EXCEEDED,
          state: 'timed_out',
        }),
      })
    );
  });

  test('expiry during retry backoff prevents the next attempt', async () => {
    const clock = new FakeWorkflowClock(0);
    const deadlineStore = createDeadlineStore();
    let attempts = 0;

    const execution = runNodeRetryLoop(
      promptNode(100),
      platform,
      'conversation',
      workflowRun,
      { maxRetries: 1, delayMs: 200, onError: 'all' },
      () => {
        attempts++;
        return Promise.resolve({ state: 'failed', output: '', error: 'temporary failure' });
      },
      initialOutput,
      {
        store: deadlineStore.store,
        nodeKey: 'prompt',
        clock,
        deadlineOutput,
      }
    );

    await flushMicrotasks();
    expect(attempts).toBe(1);
    clock.advanceBy(100);
    const output = await execution;

    expect(output).toEqual(deadlineOutput);
    expect(attempts).toBe(1);
  });

  test('completion observed at the deadline is rejected before accepting the output', async () => {
    const clock = new FakeWorkflowClock(0);
    const deadlineStore = createDeadlineStore();

    const output = await runNodeRetryLoop(
      promptNode(100),
      platform,
      'conversation',
      workflowRun,
      noRetries,
      () => {
        // Simulate an event-loop turn where the provider completion callback is
        // delivered before the already-due timer callback.
        clock.advanceBy(100);
        return Promise.resolve({ state: 'completed', output: 'too late' });
      },
      initialOutput,
      {
        store: deadlineStore.store,
        nodeKey: 'prompt',
        clock,
        deadlineOutput,
      }
    );

    expect(output).toEqual(deadlineOutput);
    expect(deadlineStore.expireWorkflowNodeDeadline).toHaveBeenCalledTimes(1);
  });

  test('resume with remaining budget uses the original persisted deadline', async () => {
    const originalRow: DeadlineRow = {
      workflow_run_id: workflowRun.id,
      node_key: 'loop',
      started_at: new Date(0),
      deadline_at: new Date(100),
      expiry_reason: null,
    };
    const clock = new FakeWorkflowClock(40);
    const deadlineStore = createDeadlineStore(originalRow);
    let attempts = 0;

    const output = await runNodeRetryLoop(
      loopNode(100),
      platform,
      'conversation',
      workflowRun,
      noRetries,
      () => {
        attempts++;
        return Promise.resolve({ state: 'completed', output: 'done' });
      },
      initialOutput,
      {
        store: deadlineStore.store,
        nodeKey: 'loop',
        clock,
        deadlineOutput,
      }
    );

    expect(output).toEqual({ state: 'completed', output: 'done' });
    expect(attempts).toBe(1);
    expect(deadlineStore.getRow()?.deadline_at).toEqual(new Date(100));
    expect(deadlineStore.createWorkflowNodeDeadline).toHaveBeenCalledWith(
      expect.objectContaining({ deadline_at: new Date(140) })
    );
    expect(clock.pendingWaitCount()).toBe(0);
  });

  test('resume after expiry transitions to timed_out without invoking the provider', async () => {
    const originalRow: DeadlineRow = {
      workflow_run_id: workflowRun.id,
      node_key: 'group',
      started_at: new Date(0),
      deadline_at: new Date(100),
      expiry_reason: null,
    };
    const clock = new FakeWorkflowClock(101);
    const deadlineStore = createDeadlineStore(originalRow);
    const providerAttempt = mock(() =>
      Promise.resolve<NodeOutput>({ state: 'completed', output: 'too late' })
    );

    const output = await runNodeRetryLoop(
      loopGroupNode(100),
      platform,
      'conversation',
      workflowRun,
      noRetries,
      providerAttempt,
      initialOutput,
      {
        store: deadlineStore.store,
        nodeKey: 'group',
        clock,
        deadlineOutput,
      }
    );

    expect(output).toEqual(deadlineOutput);
    expect(providerAttempt).not.toHaveBeenCalled();
    expect(deadlineStore.createWorkflowEvent).toHaveBeenCalledWith(
      expect.objectContaining({ data: expect.objectContaining({ state: 'timed_out' }) })
    );
  });
});

test('a prompt node without timeout does not touch the deadline store or clock', async () => {
  const clock = new FakeWorkflowClock(0);
  const deadlineStore = createDeadlineStore();

  const output = await runNodeRetryLoop(
    promptNode(),
    platform,
    'conversation',
    workflowRun,
    noRetries,
    () => Promise.resolve({ state: 'completed', output: 'done' }),
    initialOutput,
    {
      store: deadlineStore.store,
      nodeKey: 'prompt',
      clock,
      deadlineOutput,
    }
  );

  expect(output).toEqual({ state: 'completed', output: 'done' });
  expect(deadlineStore.createWorkflowNodeDeadline).not.toHaveBeenCalled();
  expect(deadlineStore.expireWorkflowNodeDeadline).not.toHaveBeenCalled();
  expect(clock.nowCalls).toBe(0);
});

test('loader-admitted prompt timeout reaches the executor deadline store', async () => {
  const parsed = parseWorkflow(
    [
      'name: deadline',
      'description: deadline',
      'nodes:',
      '  - id: prompt',
      '    prompt: do it',
      '    timeout: 75',
    ].join('\n'),
    'deadline.yaml'
  );
  if (!parsed.workflow) throw new Error(parsed.error?.error ?? 'workflow did not parse');
  const node = parsed.workflow.nodes[0];
  if (!node) throw new Error('parsed workflow has no node');

  const clock = new FakeWorkflowClock(10);
  const deadlineStore = createDeadlineStore();
  await runNodeRetryLoop(
    node,
    platform,
    'conversation',
    workflowRun,
    noRetries,
    () => Promise.resolve({ state: 'completed', output: 'done' }),
    initialOutput,
    {
      store: deadlineStore.store,
      nodeKey: node.id,
      clock,
      deadlineOutput,
    }
  );

  expect(deadlineStore.createWorkflowNodeDeadline).toHaveBeenCalledWith({
    workflow_run_id: workflowRun.id,
    node_key: 'prompt',
    started_at: new Date(10),
    deadline_at: new Date(85),
  });
});
