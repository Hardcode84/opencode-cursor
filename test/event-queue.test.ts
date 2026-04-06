import { describe, expect, test } from "bun:test";
import { EventQueue, MAX_QUEUE_DEPTH } from "../src/event-queue";

describe("EventQueue", () => {
  test("push then next: FIFO order", async () => {
    const q = new EventQueue<number>();
    q.push(1);
    q.push(2);
    q.push(3);
    expect(await q.next()).toBe(1);
    expect(await q.next()).toBe(2);
    expect(await q.next()).toBe(3);
  });

  test("next() before push resolves when item arrives", async () => {
    const q = new EventQueue<string>();
    const promise = q.next();
    q.push("delayed");
    expect(await promise).toBe("delayed");
  });

  test("multiple waiters resolve in order", async () => {
    const q = new EventQueue<number>();
    const p1 = q.next();
    const p2 = q.next();
    q.push(10);
    q.push(20);
    expect(await p1).toBe(10);
    expect(await p2).toBe(20);
  });

  test("waiter gets direct delivery, skips buffer", async () => {
    const q = new EventQueue<number>();
    const promise = q.next();
    expect(q.length).toBe(0);
    q.push(42);
    expect(q.length).toBe(0);
    expect(await promise).toBe(42);
  });

  test("length tracks buffered items", () => {
    const q = new EventQueue<number>();
    expect(q.length).toBe(0);
    q.push(1);
    expect(q.length).toBe(1);
    q.push(2);
    expect(q.length).toBe(2);
  });

  test("length decreases after next()", async () => {
    const q = new EventQueue<number>();
    q.push(1);
    q.push(2);
    expect(q.length).toBe(2);
    await q.next();
    expect(q.length).toBe(1);
    await q.next();
    expect(q.length).toBe(0);
  });

  test("drops events at high-water mark", () => {
    const q = new EventQueue<number>();
    for (let i = 0; i < MAX_QUEUE_DEPTH; i++) {
      q.push(i);
    }
    expect(q.length).toBe(MAX_QUEUE_DEPTH);

    q.push(99999);
    expect(q.length).toBe(MAX_QUEUE_DEPTH);
  });

  test("pushForce bypasses high-water mark", () => {
    const q = new EventQueue<number>();
    for (let i = 0; i < MAX_QUEUE_DEPTH; i++) {
      q.push(i);
    }

    q.pushForce(-1);
    expect(q.length).toBe(MAX_QUEUE_DEPTH + 1);
  });

  test("pushForce delivers directly to waiter", async () => {
    const q = new EventQueue<string>();
    const promise = q.next();
    q.pushForce("terminal");
    expect(await promise).toBe("terminal");
    expect(q.length).toBe(0);
  });

  test("interleaved push/next", async () => {
    const q = new EventQueue<string>();
    q.push("a");
    expect(await q.next()).toBe("a");
    q.push("b");
    q.push("c");
    expect(await q.next()).toBe("b");
    expect(await q.next()).toBe("c");
  });
});
