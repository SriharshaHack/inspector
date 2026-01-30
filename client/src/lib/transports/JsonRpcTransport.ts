/**
 * Simple JSON-RPC Transport for generic HTTP endpoints.
 * This transport sends JSON-RPC requests via HTTP POST and returns responses directly,
 * without requiring the full MCP Streamable HTTP protocol (sessions, SSE, etc.).
 */

import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";

export interface JsonRpcTransportOptions {
  /** Custom fetch function for making requests */
  fetch?: typeof globalThis.fetch;
  /** Headers to include in requests */
  headers?: HeadersInit;
  /** Request timeout in milliseconds */
  timeout?: number;
}

export class JsonRpcTransport implements Transport {
  private _url: URL;
  private _options: JsonRpcTransportOptions;
  private _closed: boolean = false;
  private _pendingRequests: Map<
    string | number,
    {
      resolve: (response: JSONRPCMessage) => void;
      reject: (error: Error) => void;
    }
  > = new Map();

  onclose?: () => void;
  onerror?: (error: Error) => void;
  onmessage?: (message: JSONRPCMessage) => void;
  sessionId?: string;

  constructor(url: URL, options: JsonRpcTransportOptions = {}) {
    this._url = url;
    this._options = options;
  }

  async start(): Promise<void> {
    if (this._closed) {
      throw new Error("Transport has been closed");
    }
    // No initialization needed for simple JSON-RPC
  }

  async close(): Promise<void> {
    if (this._closed) {
      return;
    }
    this._closed = true;

    // Reject any pending requests
    for (const [, { reject }] of this._pendingRequests) {
      reject(new Error("Transport closed"));
    }
    this._pendingRequests.clear();

    this.onclose?.();
  }

  async send(message: JSONRPCMessage): Promise<void> {
    if (this._closed) {
      throw new Error("Transport has been closed");
    }

    const fetchFn = this._options.fetch ?? globalThis.fetch;
    const headers = new Headers(this._options.headers);
    headers.set("Content-Type", "application/json");
    headers.set("Accept", "application/json");

    // Extract request ID for correlation
    const requestId = "id" in message ? message.id : undefined;

    try {
      const controller = new AbortController();
      const timeoutId = this._options.timeout
        ? setTimeout(() => controller.abort(), this._options.timeout)
        : undefined;

      const response = await fetchFn(this._url.toString(), {
        method: "POST",
        headers,
        body: JSON.stringify(message),
        signal: controller.signal,
      });

      if (timeoutId) {
        clearTimeout(timeoutId);
      }

      // Handle 204 No Content (for notifications)
      if (response.status === 204) {
        return;
      }

      if (!response.ok) {
        const errorText = await response.text().catch(() => "Unknown error");
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }

      const contentType = response.headers.get("content-type") || "";

      // Handle JSON response
      let jsonResponse: JSONRPCMessage;
      if (contentType.includes("application/json")) {
        jsonResponse = (await response.json()) as JSONRPCMessage;
      } else {
        // Try to parse as JSON anyway (some servers don't set content-type properly)
        const text = await response.text();
        try {
          jsonResponse = JSON.parse(text) as JSONRPCMessage;
        } catch {
          throw new Error(
            `Unexpected response content-type: ${contentType}. Body: ${text.substring(0, 200)}`,
          );
        }
      }

      // Ensure response ID matches request ID (some servers may return different IDs)
      const responseId = "id" in jsonResponse ? jsonResponse.id : undefined;
      if (requestId !== undefined && responseId !== requestId) {
        (jsonResponse as { id?: unknown }).id = requestId;
      }

      // Deliver the response through onmessage asynchronously
      // This is needed because the SDK sets up response handlers after send() returns
      if (this.onmessage) {
        queueMicrotask(() => {
          this.onmessage!(jsonResponse);
        });
      }
    } catch (error) {
      const wrappedError =
        error instanceof Error ? error : new Error(String(error));
      this.onerror?.(wrappedError);
      throw wrappedError;
    }
  }
}
