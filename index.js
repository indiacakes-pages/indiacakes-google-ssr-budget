import { DurableObject } from "cloudflare:workers";

/*
  =====================================================================
  IndiaCakes - Phase 6.06A
  Dedicated Google SSR Budget Durable Object Worker
  =====================================================================

  Worker name:
    indiacakes-google-ssr-budget

  Durable Object class:
    GoogleSsrBudget

  Purpose:
    Maintain strongly-consistent, persistent SSR admission counters for
    Google crawler traffic.

  IMPORTANT:
    - This Worker does NOT proxy traffic to IndiaCakes or Emergent.
    - This Worker does NOT use the IndiaCakes origin protection token.
    - This Worker should have NO custom domain / route.
    - workers.dev should be disabled in wrangler.jsonc.
    - The production proxy Worker talks to this class through a Durable
      Object binding named GOOGLE_SSR_BUDGET.
    - The production proxy remains responsible for:
        * bot classification
        * 403 BOT_BLOCKED
        * 429 BOT_RATE_LIMITED
        * X-IndiaCakes-Proxy-Token injection
        * proxying admitted requests to Emergent
        * preserving ORIGIN_429 / ORIGIN_503 semantics
*/

const SERVICE_VERSION =
  "phase-6.06a-google-ssr-budget-do-20260819-v1";

const DEFAULT_ERROR_RETRY_AFTER_SECONDS = 30;

const MIN_LIMIT = 1;
const MAX_LIMIT = 100000;


/*
  =====================================================================
  DURABLE OBJECT
  =====================================================================

  Each named instance represents one budget shard, for example:

    google-ssr-0
    google-ssr-1
    google-ssr-2
    google-ssr-3

  The calling proxy Worker chooses the shard name with getByName().

  Each shard stores:
    - current 10-second fixed window
    - request count in that 10-second window
    - current 60-second fixed window
    - request count in that 60-second window

  SQLite persistence means an Object eviction/restart does NOT reset
  the budget counters.
*/
export class GoogleSsrBudget extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    this.sql = ctx.storage.sql;

    /*
      One row is enough for each shard.

      CREATE TABLE / INSERT OR IGNORE are idempotent and safe every time
      the Durable Object instance is constructed.
    */
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS budget_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        ten_second_start INTEGER NOT NULL,
        ten_second_count INTEGER NOT NULL,
        minute_start INTEGER NOT NULL,
        minute_count INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );

      INSERT OR IGNORE INTO budget_state (
        id,
        ten_second_start,
        ten_second_count,
        minute_start,
        minute_count,
        updated_at
      )
      VALUES (1, 0, 0, 0, 0, 0);
    `);
  }


  /*
    ===================================================================
    RPC METHOD: consume()
    ===================================================================

    The production proxy may call:

      const stub =
        env.GOOGLE_SSR_BUDGET.getByName("google-ssr-0");

      const result =
        await stub.consume({
          tenSecondLimit: 2,
          minuteLimit: 10
        });

    Result example:

      {
        allowed: true,
        reason: "ADMITTED",
        retryAfter: 0,
        tenSecondLimit: 2,
        tenSecondCount: 1,
        tenSecondRemaining: 1,
        minuteLimit: 10,
        minuteCount: 4,
        minuteRemaining: 6,
        serviceVersion: "..."
      }
  */
  consume(input = {}) {
    const tenSecondLimit =
      parseLimit(input.tenSecondLimit);

    const minuteLimit =
      parseLimit(input.minuteLimit);

    if (
      tenSecondLimit === null ||
      minuteLimit === null
    ) {
      return {
        allowed: false,
        reason: "INVALID_LIMITS",
        retryAfter:
          DEFAULT_ERROR_RETRY_AFTER_SECONDS,
        serviceVersion:
          SERVICE_VERSION,
      };
    }

    const now = Date.now();

    const tenSecondStart =
      Math.floor(now / 10000) * 10000;

    const minuteStart =
      Math.floor(now / 60000) * 60000;


    /*
      Use a synchronous SQLite transaction so the read, window reset,
      decision and increment are one atomic operation.
    */
    return this.ctx.storage.transactionSync(() => {
      const state =
        this.sql.exec(
          `
            SELECT
              ten_second_start,
              ten_second_count,
              minute_start,
              minute_count
            FROM budget_state
            WHERE id = 1
          `
        ).one();


      let storedTenSecondStart =
        Number(state.ten_second_start);

      let tenSecondCount =
        Number(state.ten_second_count);

      let storedMinuteStart =
        Number(state.minute_start);

      let minuteCount =
        Number(state.minute_count);


      // -------------------------------------------------------------
      // Reset expired windows.
      // -------------------------------------------------------------

      if (
        storedTenSecondStart !==
        tenSecondStart
      ) {
        storedTenSecondStart =
          tenSecondStart;

        tenSecondCount = 0;
      }

      if (
        storedMinuteStart !==
        minuteStart
      ) {
        storedMinuteStart =
          minuteStart;

        minuteCount = 0;
      }


      // -------------------------------------------------------------
      // Check both budgets BEFORE incrementing.
      // -------------------------------------------------------------

      const tenSecondExceeded =
        tenSecondCount >=
        tenSecondLimit;

      const minuteExceeded =
        minuteCount >=
        minuteLimit;


      if (
        tenSecondExceeded ||
        minuteExceeded
      ) {
        const retryAfter10 =
          tenSecondExceeded
            ? Math.max(
                1,
                Math.ceil(
                  (
                    tenSecondStart +
                    10000 -
                    now
                  ) / 1000
                )
              )
            : 0;

        const retryAfter60 =
          minuteExceeded
            ? Math.max(
                1,
                Math.ceil(
                  (
                    minuteStart +
                    60000 -
                    now
                  ) / 1000
                )
              )
            : 0;

        const retryAfter =
          Math.max(
            1,
            retryAfter10,
            retryAfter60
          );


        /*
          Persist any window reset that happened above even when this
          request is denied.
        */
        this.sql.exec(
          `
            UPDATE budget_state
            SET
              ten_second_start = ?,
              ten_second_count = ?,
              minute_start = ?,
              minute_count = ?,
              updated_at = ?
            WHERE id = 1
          `,
          storedTenSecondStart,
          tenSecondCount,
          storedMinuteStart,
          minuteCount,
          now
        );


        return {
          allowed: false,
          reason:
            tenSecondExceeded &&
            minuteExceeded
              ? "TEN_SECOND_AND_MINUTE_LIMIT"
              : tenSecondExceeded
                ? "TEN_SECOND_LIMIT"
                : "MINUTE_LIMIT",

          retryAfter,

          tenSecondLimit,
          tenSecondCount,
          tenSecondRemaining:
            Math.max(
              0,
              tenSecondLimit -
              tenSecondCount
            ),

          minuteLimit,
          minuteCount,
          minuteRemaining:
            Math.max(
              0,
              minuteLimit -
              minuteCount
            ),

          tenSecondWindowEndsAt:
            tenSecondStart + 10000,

          minuteWindowEndsAt:
            minuteStart + 60000,

          serviceVersion:
            SERVICE_VERSION,
        };
      }


      // -------------------------------------------------------------
      // Admit and increment both counters atomically.
      // -------------------------------------------------------------

      tenSecondCount += 1;
      minuteCount += 1;


      this.sql.exec(
        `
          UPDATE budget_state
          SET
            ten_second_start = ?,
            ten_second_count = ?,
            minute_start = ?,
            minute_count = ?,
            updated_at = ?
          WHERE id = 1
        `,
        storedTenSecondStart,
        tenSecondCount,
        storedMinuteStart,
        minuteCount,
        now
      );


      return {
        allowed: true,
        reason: "ADMITTED",
        retryAfter: 0,

        tenSecondLimit,
        tenSecondCount,
        tenSecondRemaining:
          Math.max(
            0,
            tenSecondLimit -
            tenSecondCount
          ),

        minuteLimit,
        minuteCount,
        minuteRemaining:
          Math.max(
            0,
            minuteLimit -
            minuteCount
          ),

        tenSecondWindowEndsAt:
          tenSecondStart + 10000,

        minuteWindowEndsAt:
          minuteStart + 60000,

        serviceVersion:
          SERVICE_VERSION,
      };
    });
  }


  /*
    ===================================================================
    RPC METHOD: getStatus()
    ===================================================================

    Optional diagnostic method.

    This is callable only through a Durable Object binding. It does not
    create a public Internet endpoint.
  */
  getStatus() {
    const state =
      this.sql.exec(
        `
          SELECT
            ten_second_start,
            ten_second_count,
            minute_start,
            minute_count,
            updated_at
          FROM budget_state
          WHERE id = 1
        `
      ).one();

    return {
      tenSecondStart:
        Number(state.ten_second_start),

      tenSecondCount:
        Number(state.ten_second_count),

      minuteStart:
        Number(state.minute_start),

      minuteCount:
        Number(state.minute_count),

      updatedAt:
        Number(state.updated_at),

      serviceVersion:
        SERVICE_VERSION,
    };
  }


  /*
    ===================================================================
    fetch() compatibility interface
    ===================================================================

    This keeps the Durable Object compatible with a proxy implementation
    that uses stub.fetch() instead of RPC.

    Accepted internal paths:

      POST /consume
      GET  /status

    Durable Objects do not receive Internet requests directly. Requests
    to this fetch() arrive through a configured Durable Object binding.
  */
  async fetch(request) {
    const url =
      new URL(request.url);


    if (
      request.method === "POST" &&
      url.pathname === "/consume"
    ) {
      let input;

      try {
        input =
          await request.json();
      } catch {
        return jsonResponse(
          {
            allowed: false,
            reason: "INVALID_JSON",
            retryAfter:
              DEFAULT_ERROR_RETRY_AFTER_SECONDS,
            serviceVersion:
              SERVICE_VERSION,
          },
          400
        );
      }

      const result =
        this.consume(input);

      return jsonResponse(
        result,
        200
      );
    }


    if (
      request.method === "GET" &&
      url.pathname === "/status"
    ) {
      return jsonResponse(
        this.getStatus(),
        200
      );
    }


    return jsonResponse(
      {
        error: "NOT_FOUND",
        serviceVersion:
          SERVICE_VERSION,
      },
      404
    );
  }
}


/*
  =====================================================================
  DEFAULT WORKER ENTRYPOINT
  =====================================================================

  The dedicated Worker is not intended to serve Internet traffic.

  wrangler.jsonc sets:
    "workers_dev": false
    "preview_urls": false

  Therefore the Worker has no public workers.dev/preview endpoint.

  This handler is intentionally defensive in case a route is ever added
  accidentally in the future.
*/
export default {
  async fetch() {
    return new Response(
      "Not Found",
      {
        status: 404,

        headers: {
          "Content-Type":
            "text/plain; charset=UTF-8",

          "Cache-Control":
            "no-store, max-age=0",

          "X-IndiaCakes-Service":
            "google-ssr-budget",

          "X-IndiaCakes-Service-Version":
            SERVICE_VERSION,
        },
      }
    );
  },
};


/*
  =====================================================================
  HELPERS
  =====================================================================
*/

function parseLimit(value) {
  const parsed =
    Number(value);

  if (
    !Number.isInteger(parsed) ||
    parsed < MIN_LIMIT ||
    parsed > MAX_LIMIT
  ) {
    return null;
  }

  return parsed;
}


function jsonResponse(
  body,
  status = 200
) {
  return new Response(
    JSON.stringify(body),
    {
      status,

      headers: {
        "Content-Type":
          "application/json; charset=UTF-8",

        "Cache-Control":
          "no-store, max-age=0",

        "X-IndiaCakes-Service":
          "google-ssr-budget",

        "X-IndiaCakes-Service-Version":
          SERVICE_VERSION,
      },
    }
  );
}
