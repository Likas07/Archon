import { describe, expect, mock, test } from 'bun:test';
import { existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileAsync } from '@archon/git';
import { executeDagWorkflow, NODE_DEADLINE_EXCEEDED, runNodeRetryLoop } from './dag-executor';
import { parseWorkflow } from './loader';
import type { WorkflowClock, WorkflowClockWaitResult } from './clock';
import type {
  IAgentProvider,
  IWorkflowPlatform,
  ProviderCapabilities,
  SendQueryOptions,
  WorkflowConfig,
  WorkflowDeps,
} from './deps';
import type {
  ChildWorkflowOutcome,
  RunChildWorkflowArgs,
  RunChildWorkflowFn,
} from './dag-executor';
import type { DagNode, NodeOutput, WorkflowNode, WorkflowRun } from './schemas';
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

function workflowNode(timeout?: number): WorkflowNode {
  return timeout === undefined
    ? { id: 'wrapper', workflow: 'child' }
    : { id: 'wrapper', workflow: 'child', timeout };
}

function workflowDeadlineUnconfirmedMessageForTest(childRunId: string): string {
  return (
    `Sub-run ${childRunId} may still be running. ` +
    `Check with \`archon workflow status ${childRunId}\` and abandon it with ` +
    `\`archon workflow abandon ${childRunId}\` if it is orphaned.`
  );
}

function childRun(id: string, status: WorkflowRun['status']): WorkflowRun {
  return {
    ...workflowRun,
    id,
    workflow_name: 'child',
    parent_run_id: workflowRun.id,
    status,
    metadata: { parent_node_id: 'wrapper' },
  };
}

interface WrapperStoreHarness {
  store: IWorkflowStore;
  createWorkflowNodeDeadline: ReturnType<typeof mock>;
  expireWorkflowNodeDeadline: ReturnType<typeof mock>;
  cancelWorkflowRun: ReturnType<typeof mock>;
  findChildRuns: ReturnType<typeof mock>;
  events: Parameters<IWorkflowStore['createWorkflowEvent']>[0][];
  setChildren(children: WorkflowRun[]): void;
  getDeadlineRow(): DeadlineRow | undefined;
}

function createWrapperStoreHarness(params: {
  parentRun: WorkflowRun;
  initialDeadline?: DeadlineRow;
  cancel?: (id: string) => Promise<{ cancelled: boolean }>;
}): WrapperStoreHarness {
  let deadlineRow = params.initialDeadline;
  let children: WorkflowRun[] = [];
  const events: Parameters<IWorkflowStore['createWorkflowEvent']>[0][] = [];
  const createWorkflowNodeDeadline = mock(
    (deadline: Parameters<IWorkflowStore['createWorkflowNodeDeadline']>[0]) => {
      deadlineRow ??= { ...deadline, expiry_reason: null };
      return Promise.resolve(deadlineRow);
    }
  );
  const expireWorkflowNodeDeadline = mock(
    (expiry: Parameters<IWorkflowStore['expireWorkflowNodeDeadline']>[0]) => {
      if (deadlineRow) deadlineRow = { ...deadlineRow, expiry_reason: expiry.expiry_reason };
      return Promise.resolve();
    }
  );
  const cancelWorkflowRun = mock(
    params.cancel ?? ((_id: string) => Promise.resolve({ cancelled: true }))
  );
  const findChildRuns = mock((_parentRunId: string) => Promise.resolve([...children]));

  const store = {
    findChildRuns,
    getRunAncestry: mock((_runId: string) => Promise.resolve([])),
    createWorkflowRun: mock((_data: Parameters<IWorkflowStore['createWorkflowRun']>[0]) =>
      Promise.resolve(childRun('unexpected-created-child', 'pending'))
    ),
    getWorkflowRun: mock((id: string) =>
      Promise.resolve(
        id === params.parentRun.id
          ? params.parentRun
          : (children.find(child => child.id === id) ?? null)
      )
    ),
    getActiveWorkflowRunByPath: mock(() => Promise.resolve(null)),
    findResumableRun: mock(() => Promise.resolve(null)),
    failOrphanedRuns: mock(() => Promise.resolve({ count: 0 })),
    resumeWorkflowRun: mock((_id: string) => Promise.resolve(params.parentRun)),
    updateWorkflowRun: mock(
      (_id: string, updates: Parameters<IWorkflowStore['updateWorkflowRun']>[1]) => {
        if (updates.status !== undefined) params.parentRun.status = updates.status;
        if (updates.metadata !== undefined) {
          params.parentRun.metadata = { ...params.parentRun.metadata, ...updates.metadata };
        }
        return Promise.resolve();
      }
    ),
    updateWorkflowActivity: mock(() => Promise.resolve()),
    getWorkflowRunStatus: mock((_id: string) => Promise.resolve(params.parentRun.status)),
    completeWorkflowRun: mock((_id: string, _metadata?: Record<string, unknown>) => {
      params.parentRun.status = 'completed';
      return Promise.resolve();
    }),
    failWorkflowRun: mock((_id: string, error: string) => {
      params.parentRun.status = 'failed';
      params.parentRun.metadata = { ...params.parentRun.metadata, error };
      return Promise.resolve();
    }),
    pauseWorkflowRun: mock(
      (
        _id: string,
        approvalContext: Parameters<IWorkflowStore['pauseWorkflowRun']>[1],
        extraMetadata?: Record<string, unknown>
      ) => {
        params.parentRun.status = 'paused';
        params.parentRun.metadata = {
          ...params.parentRun.metadata,
          approval: { ...approvalContext, resolved: null },
          ...extraMetadata,
        };
        return Promise.resolve();
      }
    ),
    claimWriteback: mock(() => Promise.resolve({ claimed: true })),
    releaseWritebackClaim: mock(() => Promise.resolve()),
    cancelWorkflowRun,
    createWorkflowEvent: mock((event: Parameters<IWorkflowStore['createWorkflowEvent']>[0]) => {
      events.push(event);
      return Promise.resolve();
    }),
    getDagResumeSnapshot: mock(() =>
      Promise.resolve({
        completedNodeOutputs: new Map<string, string>(),
        tokens: { input: 0, output: 0 },
      })
    ),
    getCodebaseEnvVars: mock(() => Promise.resolve({})),
    getCodebase: mock(() => Promise.resolve(null)),
    getWorkflowNodeSession: mock(() => Promise.resolve(null)),
    upsertWorkflowNodeSession: mock(() => Promise.resolve()),
    deleteWorkflowNodeSessions: mock(() => Promise.resolve({ deleted: 0 })),
    createWorkflowNodeDeadline,
    getWorkflowNodeDeadline: mock(() => Promise.resolve(deadlineRow ?? null)),
    expireWorkflowNodeDeadline,
  } satisfies IWorkflowStore;

  return {
    store,
    createWorkflowNodeDeadline,
    expireWorkflowNodeDeadline,
    cancelWorkflowRun,
    findChildRuns,
    events,
    setChildren(nextChildren): void {
      children = nextChildren;
    },
    getDeadlineRow: () => deadlineRow,
  };
}

const wrapperConfig: WorkflowConfig = {
  assistant: 'claude',
  assistants: { claude: {}, codex: {} },
  commands: {},
  defaults: { loadDefaultCommands: false, loadDefaultWorkflows: false },
};

const wrapperTestProviderCapabilities: ProviderCapabilities = {
  sessionResume: true,
  mcp: false,
  hooks: false,
  skills: false,
  agents: false,
  toolRestrictions: false,
  structuredOutput: false,
  envInjection: false,
  costControl: false,
  effortControl: false,
  thinkingControl: false,
  fallbackModel: false,
  sandbox: false,
  settingSources: false,
  nativeTools: false,
  containerExec: false,
};

/**
 * A provider whose iteration completes immediately without emitting the loop's
 * completion signal, so the loop always proceeds to its `until_bash` check --
 * which is the subprocess the test is actually watching.
 */
function stubLoopProvider(): IAgentProvider {
  return {
    // eslint-disable-next-line @typescript-eslint/require-await -- an async generator needs the async modifier
    sendQuery: async function* () {
      // Non-empty output that does NOT contain the loop's until signal: an empty
      // iteration fails the loop outright, which would end the node before the
      // until_bash check this test exists to reach.
      yield { type: 'assistant', content: 'iteration output' };
      yield { type: 'result', sessionId: 'loop-deadline-sess' };
    },
    getType: () => 'claude',
    getCapabilities: () => wrapperTestProviderCapabilities,
  } satisfies IAgentProvider;
}

function wrapperPlatform(): IWorkflowPlatform & { sendMessage: ReturnType<typeof mock> } {
  return {
    sendMessage: mock(() => Promise.resolve()),
    getStreamingMode: () => 'batch',
    getPlatformType: () => 'test',
  };
}

function executeWrapper(params: {
  node: WorkflowNode;
  parentRun: WorkflowRun;
  store: IWorkflowStore;
  clock: WorkflowClock;
  platform: IWorkflowPlatform;
  runChildWorkflow: RunChildWorkflowFn;
}): Promise<string | undefined> {
  const deps: WorkflowDeps = {
    store: params.store,
    getAgentProvider: () => {
      throw new Error('wrapper test must not resolve an agent provider');
    },
    loadConfig: () => Promise.resolve(wrapperConfig),
    clock: params.clock,
  };
  return executeDagWorkflow(
    deps,
    params.platform,
    'conversation',
    '/tmp',
    { name: 'wrapper-deadline', nodes: [params.node] },
    params.parentRun,
    'claude',
    undefined,
    '/tmp',
    '/tmp',
    '/tmp',
    'main',
    'docs',
    wrapperConfig,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    { kind: 'host' },
    undefined,
    params.runChildWorkflow
  );
}

async function waitForScheduledDeadline(clock: FakeWorkflowClock): Promise<void> {
  for (let attempt = 0; attempt < 20 && clock.pendingWaitCount() === 0; attempt++) {
    await flushMicrotasks();
  }
  expect(clock.pendingWaitCount()).toBe(1);
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

describe('workflow wrapper deadlines', () => {
  test('a wrapper deadline expiring mid-child cancels that exact child run id', async () => {
    const { registerBuiltinProviders } = await import('@archon/providers');
    registerBuiltinProviders();
    const clock = new FakeWorkflowClock(0);
    const parentRun: WorkflowRun = { ...workflowRun, metadata: {}, status: 'running' };
    const harness = createWrapperStoreHarness({ parentRun });
    const exactChild = childRun('child-exact', 'running');
    const childHarness = createWrapperStoreHarness({ parentRun: exactChild });
    let childProviderAborted = false;
    let resolveChildProviderStarted: (() => void) | undefined;
    const childProviderStarted = new Promise<void>(resolve => {
      resolveChildProviderStarted = resolve;
    });
    const childSendQuery = mock(async function* (
      _prompt: string,
      _cwd: string,
      _resumeSessionId?: string,
      options?: SendQueryOptions
    ) {
      const signal = options?.abortSignal;
      if (!signal) throw new Error('child provider did not receive an abort signal');
      resolveChildProviderStarted?.();
      await new Promise<void>(resolve => {
        const finish = (): void => {
          childProviderAborted = signal.reason === NODE_DEADLINE_EXCEEDED;
          resolve();
        };
        if (signal.aborted) finish();
        else signal.addEventListener('abort', finish, { once: true });
      });
    });
    const childProvider = {
      sendQuery: childSendQuery,
      getType: () => 'claude',
      getCapabilities: () => wrapperTestProviderCapabilities,
    } satisfies IAgentProvider;
    const childDeps: WorkflowDeps = {
      store: childHarness.store,
      getAgentProvider: () => childProvider,
      loadConfig: () => Promise.resolve(wrapperConfig),
    };
    const runChildWorkflow = mock(async (_args: RunChildWorkflowArgs) => {
      harness.setChildren([exactChild]);
      await executeDagWorkflow(
        childDeps,
        wrapperPlatform(),
        'conversation',
        '/tmp',
        { name: 'child', nodes: [{ id: 'child-provider', prompt: 'wait' }] },
        exactChild,
        'claude',
        undefined,
        '/tmp',
        '/tmp',
        '/tmp',
        'main',
        'docs',
        wrapperConfig
      );
      return {
        childRunId: exactChild.id,
        status: 'failed',
        error: NODE_DEADLINE_EXCEEDED,
      } satisfies ChildWorkflowOutcome;
    });

    const execution = executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock,
      platform: wrapperPlatform(),
      runChildWorkflow,
    });

    await waitForScheduledDeadline(clock);
    await childProviderStarted;
    expect(runChildWorkflow).toHaveBeenCalledTimes(1);
    expect(childSendQuery).toHaveBeenCalledTimes(1);
    clock.advanceBy(100);
    await execution;

    expect(childProviderAborted).toBe(true);
    expect(harness.cancelWorkflowRun).toHaveBeenCalledTimes(1);
    expect(harness.cancelWorkflowRun).toHaveBeenCalledWith('child-exact');
    expect(harness.expireWorkflowNodeDeadline).toHaveBeenCalledWith({
      workflow_run_id: parentRun.id,
      node_key: 'wrapper',
      expiry_reason: NODE_DEADLINE_EXCEEDED,
    });
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        event_type: 'node_failed',
        data: expect.objectContaining({
          state: 'timed_out',
          child_run_id: 'child-exact',
        }),
      })
    );
  });

  test("a wrapper deadline kills the child's bash subprocess, not just its database row", async () => {
    // The AI-child test above passes on a build where this one fails, which is
    // how the gap survived: `abortInFlightProviderWork` reaches PROVIDER abort
    // controllers, and a bash node registers none. The child run was marked
    // `cancelled` while its subprocess ran on to completion, still mutating the
    // checkout it was handed -- the run reported over, the work not over.
    //
    // Observed live before the fix: a `sleep 60` under a 3s wrapper deadline ran
    // the full 60 seconds with the child row already `cancelled`.
    //
    // A real subprocess and a real marker file, following the pattern below: the
    // only way to know a process died is to look for the side effect it would
    // have had, after the time it would have had it.
    const { registerBuiltinProviders } = await import('@archon/providers');
    registerBuiltinProviders();
    const marker = join(
      tmpdir(),
      `archon-wrapper-deadline-${String(Date.now())}-${String(process.pid)}`
    );
    const clock = new FakeWorkflowClock(0);
    const parentRun: WorkflowRun = { ...workflowRun, metadata: {}, status: 'running' };
    const harness = createWrapperStoreHarness({ parentRun });
    const exactChild = childRun('child-exact', 'running');
    const childHarness = createWrapperStoreHarness({ parentRun: exactChild });
    const childDeps: WorkflowDeps = {
      store: childHarness.store,
      getAgentProvider: () => {
        throw new Error('the child runs a bash node and must never reach a provider');
      },
      loadConfig: () => Promise.resolve(wrapperConfig),
    };

    let childStarted: (() => void) | undefined;
    const childRunning = new Promise<void>(resolve => {
      childStarted = resolve;
    });

    const runChildWorkflow = mock(async (_args: RunChildWorkflowArgs) => {
      harness.setChildren([exactChild]);
      childStarted?.();
      await executeDagWorkflow(
        childDeps,
        wrapperPlatform(),
        'conversation',
        tmpdir(),
        { name: 'child', nodes: [{ id: 'child-bash', bash: `sleep 5; touch ${marker}` }] },
        exactChild,
        'claude',
        undefined,
        tmpdir(),
        tmpdir(),
        tmpdir(),
        'main',
        'docs',
        wrapperConfig
      );
      return {
        childRunId: exactChild.id,
        status: 'failed',
        error: NODE_DEADLINE_EXCEEDED,
      } satisfies ChildWorkflowOutcome;
    });

    const execution = executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock,
      platform: wrapperPlatform(),
      runChildWorkflow,
    });

    await waitForScheduledDeadline(clock);
    await childRunning;
    // Let the subprocess actually spawn before the deadline fires; aborting a
    // signal nobody is listening to yet would prove nothing.
    await new Promise(resolve => setTimeout(resolve, 300));
    clock.advanceBy(100);
    await execution;

    expect(harness.cancelWorkflowRun).toHaveBeenCalledWith('child-exact');

    // Outlive the command's own 5s, then confirm it never got to write.
    await new Promise(resolve => setTimeout(resolve, 5500));
    expect(existsSync(marker)).toBe(false);
  }, 20_000);

  test("a wrapper deadline kills a child loop's until_bash subprocess", async () => {
    // The bash-node test above passes on a build where this one fails. The run
    // signal reached the bash and script dispatch sites but not the two loop
    // dispatch sites, and a loop's `until_bash` is a subprocess of exactly the
    // same kind -- it runs between iterations, in the checkout, under the same
    // shell. Killing the row and leaving that shell running is the same defect
    // wearing a different node type.
    const { registerBuiltinProviders } = await import('@archon/providers');
    registerBuiltinProviders();
    const marker = join(
      tmpdir(),
      `archon-loop-deadline-${String(Date.now())}-${String(process.pid)}`
    );
    const clock = new FakeWorkflowClock(0);
    const parentRun: WorkflowRun = { ...workflowRun, metadata: {}, status: 'running' };
    const harness = createWrapperStoreHarness({ parentRun });
    const exactChild = childRun('child-loop', 'running');
    const childHarness = createWrapperStoreHarness({ parentRun: exactChild });
    const childDeps: WorkflowDeps = {
      store: childHarness.store,
      // The loop's AI iteration returns immediately so the test spends its time
      // in until_bash, which is the subprocess under examination.
      getAgentProvider: () => stubLoopProvider(),
      loadConfig: () => Promise.resolve(wrapperConfig),
    };

    let childStarted: (() => void) | undefined;
    const childRunning = new Promise<void>(resolve => {
      childStarted = resolve;
    });

    const runChildWorkflow = mock(async (_args: RunChildWorkflowArgs) => {
      harness.setChildren([exactChild]);
      childStarted?.();
      await executeDagWorkflow(
        childDeps,
        wrapperPlatform(),
        'conversation',
        tmpdir(),
        {
          name: 'child',
          nodes: [
            {
              id: 'child-loop',
              loop: {
                prompt: 'iterate',
                until: 'NEVER_EMITTED',
                until_bash: `sleep 5; touch ${marker}`,
                max_iterations: 2,
              },
            },
          ],
        },
        exactChild,
        'claude',
        undefined,
        tmpdir(),
        tmpdir(),
        tmpdir(),
        'main',
        'docs',
        wrapperConfig
      );
      return {
        childRunId: exactChild.id,
        status: 'failed',
        error: NODE_DEADLINE_EXCEEDED,
      } satisfies ChildWorkflowOutcome;
    });

    const execution = executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock,
      platform: wrapperPlatform(),
      runChildWorkflow,
    });

    await waitForScheduledDeadline(clock);
    await childRunning;
    // The AI iteration has to finish and until_bash has to spawn before the
    // deadline fires; aborting a signal nobody is listening to yet proves nothing.
    await new Promise(resolve => setTimeout(resolve, 600));
    clock.advanceBy(100);
    await execution;

    expect(harness.cancelWorkflowRun).toHaveBeenCalledWith('child-loop');

    await new Promise(resolve => setTimeout(resolve, 5500));
    expect(existsSync(marker)).toBe(false);
  }, 20_000);

  test('a wrapper node without timeout never touches the clock or deadline store', async () => {
    const clock = new FakeWorkflowClock(0);
    const parentRun: WorkflowRun = { ...workflowRun, metadata: {}, status: 'running' };
    const harness = createWrapperStoreHarness({ parentRun });
    const runChildWorkflow = mock((_args: RunChildWorkflowArgs) =>
      Promise.resolve<ChildWorkflowOutcome>({
        childRunId: 'child-completed',
        status: 'completed',
        output: 'done',
      })
    );

    await executeWrapper({
      node: workflowNode(),
      parentRun,
      store: harness.store,
      clock,
      platform: wrapperPlatform(),
      runChildWorkflow,
    });

    expect(runChildWorkflow).toHaveBeenCalledTimes(1);
    expect(harness.createWorkflowNodeDeadline).not.toHaveBeenCalled();
    expect(harness.expireWorkflowNodeDeadline).not.toHaveBeenCalled();
    expect(harness.cancelWorkflowRun).not.toHaveBeenCalled();
    expect(clock.nowCalls).toBe(0);
  });

  test("cancelWorkflowRun returning false marks wrapper cleanup 'unconfirmed'", async () => {
    const childRunId = 'child-unconfirmed';
    const parentRun: WorkflowRun = {
      ...workflowRun,
      status: 'running',
      metadata: {
        approval: {
          type: 'child_workflow',
          nodeId: 'wrapper',
          childRunId,
          message: 'blocked',
        },
      },
    };
    const harness = createWrapperStoreHarness({
      parentRun,
      initialDeadline: {
        workflow_run_id: parentRun.id,
        node_key: 'wrapper',
        started_at: new Date(0),
        deadline_at: new Date(100),
        expiry_reason: null,
      },
      cancel: (_id: string) => Promise.resolve({ cancelled: false }),
    });
    const runChildWorkflow = mock((_args: RunChildWorkflowArgs) =>
      Promise.resolve<ChildWorkflowOutcome>({ childRunId, status: 'completed' })
    );
    const platform = wrapperPlatform();

    await executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock: new FakeWorkflowClock(101),
      platform,
      runChildWorkflow,
    });

    expect(harness.getDeadlineRow()?.expiry_reason).toBe('deadline_exceeded_cleanup_unconfirmed');
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        event_type: 'node_failed',
        data: expect.objectContaining({
          state: 'timed_out',
          cleanup: 'unconfirmed',
          child_run_id: childRunId,
          message: workflowDeadlineUnconfirmedMessageForTest(childRunId),
        }),
      })
    );
    expect(platform.sendMessage).toHaveBeenCalledWith(
      'conversation',
      workflowDeadlineUnconfirmedMessageForTest(childRunId),
      undefined
    );
  });

  test("cancelWorkflowRun throwing marks wrapper cleanup 'unconfirmed' without propagating", async () => {
    const childRunId = 'child-cancel-error';
    const parentRun: WorkflowRun = {
      ...workflowRun,
      status: 'running',
      metadata: {
        approval: {
          type: 'child_workflow',
          nodeId: 'wrapper',
          childRunId,
          message: 'blocked',
        },
      },
    };
    const harness = createWrapperStoreHarness({
      parentRun,
      initialDeadline: {
        workflow_run_id: parentRun.id,
        node_key: 'wrapper',
        started_at: new Date(0),
        deadline_at: new Date(100),
        expiry_reason: null,
      },
      cancel: (_id: string) => {
        throw new Error('cancel store unavailable');
      },
    });
    const runChildWorkflow = mock((_args: RunChildWorkflowArgs) =>
      Promise.resolve<ChildWorkflowOutcome>({ childRunId, status: 'completed' })
    );

    await expect(
      executeWrapper({
        node: workflowNode(100),
        parentRun,
        store: harness.store,
        clock: new FakeWorkflowClock(101),
        platform: wrapperPlatform(),
        runChildWorkflow,
      })
    ).resolves.toBeUndefined();

    expect(harness.getDeadlineRow()?.expiry_reason).toBe('deadline_exceeded_cleanup_unconfirmed');
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        event_type: 'node_failed',
        data: expect.objectContaining({
          cleanup: 'unconfirmed',
          child_run_id: childRunId,
          message: workflowDeadlineUnconfirmedMessageForTest(childRunId),
        }),
      })
    );
  });

  test('a child paused at a gate is cancelled when the wrapper deadline expires', async () => {
    const childRunId = 'child-paused';
    const clock = new FakeWorkflowClock(0);
    const parentRun: WorkflowRun = { ...workflowRun, metadata: {}, status: 'running' };
    const harness = createWrapperStoreHarness({ parentRun });
    const pausedChild = childRun(childRunId, 'paused');
    const runChildWorkflow = mock((_args: RunChildWorkflowArgs) => {
      harness.setChildren([pausedChild]);
      return Promise.resolve<ChildWorkflowOutcome>({ childRunId, status: 'paused' });
    });

    await executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock,
      platform: wrapperPlatform(),
      runChildWorkflow,
    });
    expect(parentRun.status).toBe('paused');
    expect(harness.cancelWorkflowRun).not.toHaveBeenCalled();

    clock.advanceBy(101);
    parentRun.status = 'running';
    await executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock,
      platform: wrapperPlatform(),
      runChildWorkflow,
    });

    expect(harness.cancelWorkflowRun).toHaveBeenCalledTimes(1);
    expect(harness.cancelWorkflowRun).toHaveBeenCalledWith(childRunId);
  });

  test('resume after wrapper expiry fails immediately without re-spawning a child', async () => {
    const childRunId = 'child-resume-expired';
    const parentRun: WorkflowRun = {
      ...workflowRun,
      status: 'running',
      metadata: {
        approval: {
          type: 'child_workflow',
          nodeId: 'wrapper',
          childRunId,
          message: 'blocked',
        },
      },
    };
    const harness = createWrapperStoreHarness({
      parentRun,
      initialDeadline: {
        workflow_run_id: parentRun.id,
        node_key: 'wrapper',
        started_at: new Date(0),
        deadline_at: new Date(100),
        expiry_reason: null,
      },
    });
    const runChildWorkflow = mock((_args: RunChildWorkflowArgs) =>
      Promise.resolve<ChildWorkflowOutcome>({ childRunId, status: 'completed' })
    );

    await executeWrapper({
      node: workflowNode(100),
      parentRun,
      store: harness.store,
      clock: new FakeWorkflowClock(101),
      platform: wrapperPlatform(),
      runChildWorkflow,
    });

    expect(runChildWorkflow).not.toHaveBeenCalled();
    expect(harness.findChildRuns).not.toHaveBeenCalled();
    expect(harness.cancelWorkflowRun).toHaveBeenCalledWith(childRunId);
  });
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

describe('deadline termination of deterministic subprocesses', () => {
  // These exercise the real execFile path rather than a fake clock. A deadline
  // that reports a stop without stopping the work is worse than no deadline:
  // the audit trail then asserts something false. The only way to know the
  // subprocess actually died is to look for its side effect afterwards.
  const abortAfter = (ms: number): AbortSignal => {
    const controller = new AbortController();
    setTimeout(() => {
      controller.abort(NODE_DEADLINE_EXCEEDED);
    }, ms);
    return controller.signal;
  };

  test('an aborted subprocess is killed before its side effect runs', async () => {
    const marker = join(tmpdir(), `archon-deadline-${String(Date.now())}-${String(process.pid)}`);
    const started = Date.now();

    await expect(
      execFileAsync('bash', ['-c', `sleep 5; touch ${marker}`], {
        timeout: 60_000,
        signal: abortAfter(200),
      })
    ).rejects.toMatchObject({ code: 'ABORT_ERR' });

    // Bounded well below the command's own 5s: the kill happened, the timeout did not.
    expect(Date.now() - started).toBeLessThan(2000);

    // Outlive the command's own duration, then confirm it never got to write.
    await new Promise(resolve => setTimeout(resolve, 5500));
    expect(existsSync(marker)).toBe(false);
  }, 20_000);

  test('an unaborted subprocess still runs to completion', async () => {
    // Guards the assertion above from passing for the wrong reason: if the
    // signal plumbing broke such that every subprocess died, the test above
    // would still be green.
    const { stdout } = await execFileAsync('bash', ['-c', 'echo alive'], { timeout: 10_000 });
    expect(stdout.trim()).toBe('alive');
  }, 15_000);
});

describe('governance deadline stops layer dispatch inside a loop_group body', () => {
  // A loop_group body is a sub-DAG, and its layers are dispatched by the same
  // runLayers that drives the top level. Aborting the governance signal does not
  // change the run's status, so the between-layer status check cannot see an
  // expired deadline — only a direct signal check can. Without one, a body whose
  // deadline fires during an early layer still dispatches every later layer, and
  // the deadline reports a stop it did not perform.
  function twoLayerLoopGroupNode(timeout: number): DagNode {
    return {
      id: 'group',
      timeout,
      loop_group: {
        until: 'DONE',
        max_iterations: 2,
        nodes: [
          { id: 'first', prompt: 'first-body' },
          // A bash node, deliberately. An AI body node routes through
          // runNodeRetryLoop, which returns on the aborted parent signal before
          // reaching the provider — so a later AI layer looks un-dispatched even
          // with the layer guard removed, and the test would prove nothing. A
          // deterministic node without `retry:` skips that loop entirely and
          // announces itself the moment it is dispatched.
          //
          // `all_done`, also deliberately: under the default `all_success` this
          // node would skip because its upstream FAILED on the deadline.
          {
            id: 'second',
            bash: 'echo second-body',
            depends_on: ['first'],
            trigger_rule: 'all_done',
          },
        ],
      },
    };
  }

  test('a later body layer is never dispatched once the group deadline has fired', async () => {
    const { registerBuiltinProviders } = await import('@archon/providers');
    registerBuiltinProviders();
    const clock = new FakeWorkflowClock(0);
    const parentRun: WorkflowRun = { ...workflowRun, metadata: {}, status: 'running' };
    const harness = createWrapperStoreHarness({ parentRun });

    let resolveFirstStarted: (() => void) | undefined;
    const firstStarted = new Promise<void>(resolve => {
      resolveFirstStarted = resolve;
    });
    const sendQuery = mock(async function* (
      _prompt: string,
      _cwd: string,
      _resumeSessionId?: string,
      options?: SendQueryOptions
    ) {
      const signal = options?.abortSignal;
      if (!signal) throw new Error('body node did not receive an abort signal');
      resolveFirstStarted?.();
      await new Promise<void>(resolve => {
        if (signal.aborted) resolve();
        else signal.addEventListener('abort', () => resolve(), { once: true });
      });
    });
    const provider = {
      sendQuery,
      getType: () => 'claude',
      getCapabilities: () => wrapperTestProviderCapabilities,
    } satisfies IAgentProvider;
    const deps: WorkflowDeps = {
      store: harness.store,
      getAgentProvider: () => provider,
      loadConfig: () => Promise.resolve(wrapperConfig),
      clock,
    };

    const execution = executeDagWorkflow(
      deps,
      wrapperPlatform(),
      'conversation',
      '/tmp',
      { name: 'group-deadline', nodes: [twoLayerLoopGroupNode(100)] },
      parentRun,
      'claude',
      undefined,
      '/tmp',
      '/tmp',
      '/tmp',
      'main',
      'docs',
      wrapperConfig
    );

    await waitForScheduledDeadline(clock);
    await firstStarted;
    clock.advanceBy(100);
    await execution;

    // The assertion that matters: the SECOND layer's node was never dispatched.
    // Asserting on the group's failure alone would stay green with the guard
    // removed — the group already returned deadline_exceeded once the body
    // finished running every layer.
    const startedSteps = harness.events
      .filter(event => event.event_type === 'node_started')
      .map(event => event.step_name);
    expect(startedSteps).toContain('group.first');
    expect(startedSteps).not.toContain('group.second');
    expect(harness.expireWorkflowNodeDeadline).toHaveBeenCalledWith({
      workflow_run_id: parentRun.id,
      node_key: 'group',
      expiry_reason: NODE_DEADLINE_EXCEEDED,
    });
  }, 15_000);
});

test('an event-persistence failure still returns deadline_exceeded and notifies', async () => {
  // The failure this guards: an unguarded await on createWorkflowEvent rejects
  // the whole deadline handler, so runLayers reports the database error instead
  // of deadline_exceeded — the node is recorded as failing for the wrong reason
  // and the operator is never told the timeout fired.
  const clock = new FakeWorkflowClock(101);
  const deadlineStore = createDeadlineStore({
    workflow_run_id: workflowRun.id,
    node_key: 'prompt',
    started_at: new Date(0),
    deadline_at: new Date(100),
    expiry_reason: null,
  });
  deadlineStore.store.createWorkflowEvent = mock(() =>
    Promise.reject(new Error('event store unavailable'))
  );
  const notifications: string[] = [];
  const capturingPlatform: IWorkflowPlatform = {
    sendMessage: mock((_conversationId: string, message: string) => {
      notifications.push(message);
      return Promise.resolve();
    }),
    getStreamingMode: () => 'batch',
    getPlatformType: () => 'test',
  };

  const output = await runNodeRetryLoop(
    promptNode(100),
    capturingPlatform,
    'conversation',
    workflowRun,
    noRetries,
    () => Promise.resolve({ state: 'completed', output: 'too late' }),
    initialOutput,
    {
      store: deadlineStore.store,
      nodeKey: 'prompt',
      clock,
      deadlineOutput,
    }
  );

  expect(output).toEqual(deadlineOutput);
  expect(notifications).toHaveLength(1);
  expect(notifications[0]).toContain('exceeded its absolute timeout');
});
