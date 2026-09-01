/** Result of waiting on the workflow clock. */
export type WorkflowClockWaitResult = 'elapsed' | 'aborted';

/**
 * Narrow clock used by absolute node deadlines.
 *
 * Production uses wall-clock time and real timers. Tests inject a fake clock so
 * deadline and retry-backoff schedules advance without sleeping.
 */
export interface WorkflowClock {
  now(): number;
  wait(ms: number, signal?: AbortSignal): Promise<WorkflowClockWaitResult>;
}

export const systemWorkflowClock: WorkflowClock = {
  now: (): number => Date.now(),
  wait: (ms, signal): Promise<WorkflowClockWaitResult> => {
    if (signal?.aborted) return Promise.resolve('aborted');

    return new Promise(resolve => {
      let settled = false;
      const finish = (result: WorkflowClockWaitResult): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        signal?.removeEventListener('abort', onAbort);
        resolve(result);
      };
      const onAbort = (): void => {
        finish('aborted');
      };
      const timer = setTimeout(
        () => {
          finish('elapsed');
        },
        Math.max(0, ms)
      );
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  },
};
