import { logWarn } from "./logger";

export const MAX_QUEUE_DEPTH = 10_000;

export class EventQueue<T> {
  private buffer: T[] = [];
  private waiters: Array<(value: T) => void> = [];

  get length(): number {
    return this.buffer.length;
  }

  push(event: T): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(event);
    } else {
      if (this.buffer.length >= MAX_QUEUE_DEPTH) {
        logWarn("EventQueue overflow, dropping event", { depth: this.buffer.length });
        return;
      }
      this.buffer.push(event);
    }
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
