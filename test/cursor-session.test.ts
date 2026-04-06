import { describe, expect, test } from "bun:test";
import { classifyConnectError } from "../src/cursor-session";

describe("classifyConnectError", () => {
  test("blob not found → blob_not_found", () => {
    expect(classifyConnectError("Blob not found for key xyz")).toBe("blob_not_found");
  });

  test("resource_exhausted → resource_exhausted", () => {
    expect(classifyConnectError("RESOURCE_EXHAUSTED: rate limit")).toBe("resource_exhausted");
  });

  test("case insensitive matching", () => {
    expect(classifyConnectError("blob NOT FOUND")).toBe("blob_not_found");
    expect(classifyConnectError("Resource_Exhausted")).toBe("resource_exhausted");
  });

  test("unrecognized error → undefined", () => {
    expect(classifyConnectError("some random error")).toBeUndefined();
  });

  test("empty string → undefined", () => {
    expect(classifyConnectError("")).toBeUndefined();
  });

  test("timeout hint comes from inactivity timer, not from error classification", () => {
    expect(classifyConnectError("timeout")).toBeUndefined();
    expect(classifyConnectError("request timed out")).toBeUndefined();
  });
});
