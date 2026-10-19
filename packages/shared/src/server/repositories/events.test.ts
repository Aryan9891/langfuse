import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

const mockQueryClickhouse = vi.hoisted(() => vi.fn());
const mockQueryClickhouseWithProgress = vi.hoisted(() => vi.fn());
let charCounter = 0;

vi.mock("./clickhouse", async () => {
  const { isRow, isProgressRow, isException } =
    await vi.importActual<typeof import("@clickhouse/client")>(
      "@clickhouse/client",
    );

  return {
    ClickHouseResourceError: class extends Error {
      static wrapIfResourceError(error: Error) {
        return error;
      }
    },
    ClickHouseQueryAbortedError: class extends Error {},
    queryClickhouse: mockQueryClickhouse,
    queryClickhouseWithProgress: mockQueryClickhouseWithProgress,
    isRow,
    isProgressRow,
    isException,
    commandClickhouse: vi.fn(),
    queryClickhouseStream: vi.fn(),
    queryClickhouseStreamRawText: vi.fn(),
    queryClickhouseExecRaw: vi.fn(),
    parseClickhouseUTCDateTimeFormat: vi.fn(
      (value: string) => new Date(value.replace(" ", "T") + "Z"),
    ),
    BLOB_EXPORT_PARQUET_CLICKHOUSE_SETTINGS: {},
    // Return unique names per call so filter parameter variables don't collide
    clickhouseCompliantRandomCharacters: vi.fn(() => `x${++charCounter}`),
  };
});

import {
  getObservationsV2FromEventsTableForPublicApi,
  getObservationsV2PageFromEventsTableForPublicApi,
} from "./events";
import { ClickHouseQueryAbortedError } from "./clickhouse";
import { type EventsTableFilterState } from "../../types";

const inputContainsFilter: EventsTableFilterState = [
  { type: "string", column: "input", operator: "contains", value: "needle" },
];

const captureQuery = () => {
  const call = mockQueryClickhouse.mock.calls.at(0);
  return (call?.[0] as { query: string }).query;
};

const baseOpts = { projectId: "proj-1", page: 1, limit: 50 } as const;
const boundedOpts = {
  ...baseOpts,
  fields: ["core" as const],
  fromStartTime: "2026-10-01T00:00:00.000Z",
  toStartTime: "2026-10-19T00:00:00.000Z",
};
const observation = (id: string, startTime: string) => ({
  id,
  trace_id: "trace-1",
  project_id: "proj-1",
  start_time: startTime,
  type: "SPAN",
});

const rowEvent = <T>(row: T) => ({ row });
const progressEvent = (readRows: number, readBytes = 0) => ({
  progress: {
    read_rows: String(readRows),
    read_bytes: String(readBytes),
    elapsed_ns: "0",
  },
});
const mockProgressQuery = (...events: unknown[]) => {
  mockQueryClickhouseWithProgress.mockImplementationOnce(async function* () {
    for (const event of events) yield event;
  });
};
const mockProgressError = (error: Error) => {
  mockQueryClickhouseWithProgress.mockImplementationOnce(async function* () {
    yield progressEvent(0);
    throw error;
  });
};

describe("getObservationsV2FromEventsTableForPublicApi query shape", () => {
  beforeEach(() => {
    charCounter = 0;
    vi.clearAllMocks();
    mockQueryClickhouseWithProgress.mockReset();
    mockQueryClickhouse.mockResolvedValue([]);
  });

  afterEach(() => vi.restoreAllMocks());

  it("skips the io-lane split when a content filter forces base onto events_full", async () => {
    await getObservationsV2FromEventsTableForPublicApi(
      {
        ...baseOpts,
        fields: ["core", "io"],
        advancedFilters: inputContainsFilter,
      },
      { allowUnindexedIoFilters: true },
    );

    const query = captureQuery();
    // Single-pass scan on events_full: no base/io CTE split, io read inline.
    expect(query).not.toContain("FROM base");
    expect(query).not.toContain("_io_start_time");
    expect(query).toContain("e.input");
    expect(query).toContain("e.output");
    // Exactly one events_full read (the split would reference it twice).
    expect((query.match(/events_full/g) ?? []).length).toBe(1);
    expect(query).not.toContain("events_core");
  });

  it("keeps the io-lane split when no content filter forces events_full", async () => {
    await getObservationsV2FromEventsTableForPublicApi({
      ...baseOpts,
      fields: ["core", "io"],
    });

    const query = captureQuery();
    // Base filters on the cheap truncated table; io lane reads full columns
    // only for the matched, page-sized rows.
    expect(query).toContain("FROM events_core");
    expect(query).toContain("FROM base");
    expect(query).toContain("_io_start_time");
  });

  it("returns a continuation after a completed empty window exhausts the scan budget", async () => {
    // Progress counters are cumulative within one query. The delta between
    // these events is 1M, so the first window must not exhaust the 5M budget.
    mockProgressQuery(progressEvent(1_000_000), progressEvent(2_000_000));
    mockProgressQuery(progressEvent(2_000_000));

    const page = await getObservationsV2PageFromEventsTableForPublicApi({
      ...baseOpts,
      fromStartTime: "2026-10-01T00:00:00.000Z",
      toStartTime: "2026-10-19T00:00:00.000Z",
    });

    expect(page.items).toEqual([]);
    expect(page.cursor).toEqual({
      lastStartTimeTo: new Date("2026-10-05T00:00:00.000Z"),
      lastTraceId: "CUR00254FG5s",
      lastId: "",
    });
    expect(mockQueryClickhouseWithProgress).toHaveBeenCalledTimes(2);
    expect(
      mockQueryClickhouseWithProgress.mock.calls[0][0].params,
    ).toMatchObject({
      scanFrom: "2026-10-12 00:00:00.000",
      scanTo: "2026-10-19 00:00:00.000",
    });
    expect(
      mockQueryClickhouseWithProgress.mock.calls[1][0].params,
    ).toMatchObject({
      scanFrom: "2026-10-05 00:00:00.000",
      scanTo: "2026-10-12 00:00:00.000",
    });
  });

  it("walks past empty windows, keeps the lookahead, and resumes with a row cursor", async () => {
    mockProgressQuery(
      rowEvent(observation("newer", "2026-10-12 00:00:00.000")),
    );
    mockProgressQuery();
    mockProgressQuery(
      rowEvent(observation("older", "2026-10-04 00:00:00.000")),
      rowEvent(observation("lookahead", "2026-10-02 00:00:00.000")),
    );

    const page = await getObservationsV2PageFromEventsTableForPublicApi({
      ...boundedOpts,
      fields: ["core"],
      limit: 2,
    });

    expect(page.items.map((row) => row.id)).toEqual(["newer", "older"]);
    expect(page.cursor).toEqual({
      lastStartTimeTo: new Date("2026-10-04T00:00:00.000Z"),
      lastTraceId: "trace-1",
      lastId: "older",
    });
    const calls = mockQueryClickhouseWithProgress.mock.calls.map(
      ([opts]) => opts,
    );
    expect(calls.map((opts) => opts.params.scanFrom)).toEqual([
      "2026-10-12 00:00:00.000",
      "2026-10-05 00:00:00.000",
      "2026-10-01 00:00:00.000",
    ]);
    expect(calls.map((opts) => opts.params.limit)).toEqual([3, 2, 2]);
  });

  it("returns no cursor once the effective structured time range is exhausted", async () => {
    mockProgressQuery();
    const page = await getObservationsV2PageFromEventsTableForPublicApi({
      ...boundedOpts,
      fields: ["core", "io"],
      advancedFilters: [
        {
          type: "datetime",
          column: "startTime",
          operator: ">=",
          value: new Date("2026-10-17T00:00:00Z"),
        },
        {
          type: "datetime",
          column: "startTime",
          operator: "<=",
          value: new Date("2026-10-18T00:00:00Z"),
        },
      ],
    });

    expect(page).toEqual({ items: [], cursor: undefined });
    expect(mockQueryClickhouseWithProgress).toHaveBeenCalledTimes(1);
    const call = mockQueryClickhouseWithProgress.mock.calls[0][0];
    expect(call.params).toMatchObject({
      scanFrom: "2026-10-17 00:00:00.000",
      scanTo: "2026-10-18 00:00:00.000",
    });
    expect(call.query).toContain("e.start_time <= {scanTo: DateTime64(6)}");
    expect(call.query).toContain("FROM base");
  });

  it("stops before another window when the elapsed time approaches the deadline", async () => {
    const clock = vi.spyOn(performance, "now").mockReturnValue(0);
    mockQueryClickhouseWithProgress.mockImplementationOnce(async function* () {
      clock.mockReturnValue(24_500);
      yield rowEvent(observation("newer", "2026-10-13 00:00:00.000"));
    });

    const page = await getObservationsV2PageFromEventsTableForPublicApi({
      ...boundedOpts,
      fields: ["core"],
    });

    expect(page.items.map((row) => row.id)).toEqual(["newer"]);
    expect(page.cursor?.lastStartTimeTo).toEqual(
      new Date("2026-10-12T00:00:00Z"),
    );
    expect(mockQueryClickhouseWithProgress).toHaveBeenCalledTimes(1);
  });

  it("returns the last completed boundary when the shared deadline expires", async () => {
    const clock = vi.spyOn(performance, "now").mockReturnValue(0);
    mockQueryClickhouseWithProgress.mockImplementationOnce(async function* () {
      clock.mockReturnValue(5_000);
      yield progressEvent(0);
    });
    mockQueryClickhouseWithProgress.mockImplementationOnce(async function* () {
      yield rowEvent(observation("unfinished", "2026-10-11 00:00:00.000"));
      throw new ClickHouseQueryAbortedError();
    });
    const page =
      await getObservationsV2PageFromEventsTableForPublicApi(boundedOpts);
    expect(page.items).toEqual([]);
    expect(page.cursor?.lastStartTimeTo).toEqual(
      new Date("2026-10-12T00:00:00Z"),
    );
    expect(mockQueryClickhouseWithProgress).toHaveBeenCalledTimes(2);
    expect(
      mockQueryClickhouseWithProgress.mock.calls[1][0].clickhouseSettings
        .max_execution_time,
    ).toBe(20);
  });

  it("bounds an unbounded request by the project's oldest row and preserves future timestamps", async () => {
    vi.spyOn(Date, "now").mockReturnValue(Date.parse("2026-10-19T00:00:00Z"));
    mockProgressQuery(rowEvent({ start_time: "2026-10-17 00:00:00.000" }));
    mockProgressQuery(
      rowEvent(observation("future", "2026-10-20 00:00:00.000")),
    );

    const page = await getObservationsV2PageFromEventsTableForPublicApi({
      ...baseOpts,
      fields: ["core"],
    });

    expect(page.items.map((row) => row.id)).toEqual(["future"]);
    expect(page.cursor).toBeUndefined();
    expect(mockQueryClickhouseWithProgress).toHaveBeenCalledTimes(2);
    const [probe, scan] = mockQueryClickhouseWithProgress.mock.calls.map(
      ([opts]) => opts,
    );
    expect(probe.query).toContain("e.start_time ASC");
    expect(probe.params.limit).toBe(1);
    expect(scan.params.scanFrom).toBe("2026-10-17 00:00:00.000");
    expect(scan.params.scanTo).toBeUndefined();
  });

  it.each([
    {
      name: "typed cancellation",
      setup: () => mockProgressError(new ClickHouseQueryAbortedError()),
      assert: (error: unknown) =>
        expect(error).toBeInstanceOf(ClickHouseQueryAbortedError),
    },
    {
      name: "exception event",
      setup: () => mockProgressQuery({ exception: "Syntax error" }),
      assert: (error: unknown) =>
        expect(error).toMatchObject({ message: "Syntax error" }),
    },
  ])(
    "fails without inventing a continuation when no window completes: $name",
    async ({ setup, assert }) => {
      setup();
      let caught: unknown;
      try {
        await getObservationsV2PageFromEventsTableForPublicApi(boundedOpts);
      } catch (error) {
        caught = error;
      }
      expect(caught).toBeDefined();
      assert(caught);
      expect(mockQueryClickhouseWithProgress).toHaveBeenCalledTimes(1);
    },
  );
});
