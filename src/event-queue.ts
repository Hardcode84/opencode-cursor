import { logWarn } from "./logger";

export const MAX_QUEUE_DEPTH = 10_000;

export class EventQueue<T> {
  private buffer: T[] = [];
  private waiters: Array<(value: T) => void> = [];
  private overflowCb?: () => void;

  constructor(opts?: { onOverflow?: () => void }) {
    this.overflowCb = opts?.onOverflow;
  }

  get length(): number {
    return this.buffer.length;
  }

  /** Returns false if the event was dropped due to overflow. */
  push(event: T): boolean {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(event);
      return true;
    }
    if (this.buffer.length >= MAX_QUEUE_DEPTH) {
      logWarn("EventQueue overflow", { depth: this.buffer.length });
      this.overflowCb?.();
      return false;
    }
    this.buffer.push(event);
    return true;
  }

  /** Push unconditionally (bypasses high-water mark). Used for terminal events. */
  pushForce(event: T): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(event);
    } else {
      this.buffer.push(event);
    }
  }

  next(): Promise<T> {
    const head = this.buffer.shift();
    if (head !== undefined) return Promise.resolve(head);
    return new Promise((resolve) => {
      this.waiters.push(resolve);
    });
  }
}
