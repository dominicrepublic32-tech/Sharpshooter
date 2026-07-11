const express   = require('express');
const WebSocket = require('ws');
const crypto    = require('crypto');
const path      = require('path');
const fs        = require('fs');
const Database  = require('better-sqlite3');

// ─── DataEngine (added) ────────────────────────────────────────────────────────
// Historical downloads, tick storage, and candle persistence for future
// backtesting. Entirely separate from trading state (state.json) and from
// the existing in-memory CandleManager below (which feeds live strategy
// execution). Nothing in this block or its wiring further down changes any
// existing trading behavior. Merged into this single file (previously
// separate lib/dataengine/*.js modules) for simpler one-file deployment.
const DATAENGINE_MARKETS = ['1HZ100V', '1HZ75V']; // Volatility 100 (1s), Volatility 75 (1s)
const DATAENGINE_CANDLE_INTERVAL_SECONDS = 60;
const DATAENGINE_DB_PATH = process.env.DATAENGINE_DB_PATH || path.join(__dirname, 'data', 'dataengine.db');

// ─── DataEngine: schema (merged from lib/dataengine/schema.js) ─────────────────
/**
 * lib/dataengine/schema.js
 *
 * Schema for the DataEngine's persistent market database. Completely
 * separate concern from the bot's state.json (slot configs/stats/history):
 * this only ever stores raw ticks (source of truth) and derived candles.
 * No trading state, no account state, no slot state lives here.
 */

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS ticks (
  symbol TEXT    NOT NULL,
  epoch  INTEGER NOT NULL,
  quote  REAL    NOT NULL,
  PRIMARY KEY (symbol, epoch)
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_epoch
  ON ticks (symbol, epoch);

CREATE TABLE IF NOT EXISTS candles (
  symbol TEXT    NOT NULL,
  epoch  INTEGER NOT NULL, -- candle OPEN epoch (start of interval)
  open   REAL    NOT NULL,
  high   REAL    NOT NULL,
  low    REAL    NOT NULL,
  close  REAL    NOT NULL,
  PRIMARY KEY (symbol, epoch)
);

CREATE INDEX IF NOT EXISTS idx_candles_symbol_epoch
  ON candles (symbol, epoch);
`;

function applySchema(db) {
  db.exec(SCHEMA_SQL);
}

// ─── DataEngine: database access layer (merged from lib/dataengine/db.js) ──────
/**
 * lib/dataengine/db.js
 *
 * All SQLite access for the DataEngine lives here. Ticks are the source of
 * truth and are never discarded; candles are derived, queryable convenience
 * data. Every insert uses INSERT OR IGNORE against a composite primary key
 * of (symbol, epoch), which makes duplicate prevention automatic.
 */
class DataEngineDB {
  constructor(dbPath) {
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    this.dbPath = dbPath;
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');

    applySchema(this.db);
    this._prepare();
  }

  _prepare() {
    this.stmts = {
      insertTick: this.db.prepare(`INSERT OR IGNORE INTO ticks (symbol, epoch, quote) VALUES (?, ?, ?)`),
      getLatestTick: this.db.prepare(`SELECT symbol, epoch, quote FROM ticks WHERE symbol = ? ORDER BY epoch DESC LIMIT 1`),
      getOldestTick: this.db.prepare(`SELECT symbol, epoch, quote FROM ticks WHERE symbol = ? ORDER BY epoch ASC LIMIT 1`),
      countTicksForSymbol: this.db.prepare(`SELECT COUNT(*) AS c FROM ticks WHERE symbol = ?`),
      getTicksBetween: this.db.prepare(`SELECT symbol, epoch, quote FROM ticks WHERE symbol = ? AND epoch >= ? AND epoch <= ? ORDER BY epoch ASC`),
      deleteTicksBetween: this.db.prepare(`DELETE FROM ticks WHERE symbol = ? AND epoch >= ? AND epoch <= ?`),

      insertCandle: this.db.prepare(`INSERT OR IGNORE INTO candles (symbol, epoch, open, high, low, close) VALUES (?, ?, ?, ?, ?, ?)`),
      getLatestCandle: this.db.prepare(`SELECT symbol, epoch, open, high, low, close FROM candles WHERE symbol = ? ORDER BY epoch DESC LIMIT 1`),
      countCandlesForSymbol: this.db.prepare(`SELECT COUNT(*) AS c FROM candles WHERE symbol = ?`),
      getCandlesBetween: this.db.prepare(`SELECT symbol, epoch, open, high, low, close FROM candles WHERE symbol = ? AND epoch >= ? AND epoch <= ? ORDER BY epoch ASC`),
      deleteCandlesBetween: this.db.prepare(`DELETE FROM candles WHERE symbol = ? AND epoch >= ? AND epoch <= ?`),

      distinctSymbols: this.db.prepare(`SELECT DISTINCT symbol FROM ticks`),
    };

    this._insertTicksBatch = this.db.transaction((rows) => {
      const inserted = [];
      for (const row of rows) {
        const info = this.stmts.insertTick.run(row.symbol, row.epoch, row.quote);
        if (info.changes > 0) inserted.push(row);
      }
      return inserted;
    });
  }

  // ---- Ticks ----
  saveTick(symbol, epoch, quote) {
    return this.stmts.insertTick.run(symbol, epoch, quote).changes > 0;
  }

  saveTicksBatch(rows) {
    if (!rows || rows.length === 0) return [];
    return this._insertTicksBatch(rows);
  }

  getLatestTick(symbol) {
    return this.stmts.getLatestTick.get(symbol) || null;
  }

  getOldestTick(symbol) {
    return this.stmts.getOldestTick.get(symbol) || null;
  }

  getTicksBetween(symbol, start, end) {
    return this.stmts.getTicksBetween.all(symbol, start, end);
  }

  countTicks(symbol) {
    return this.stmts.countTicksForSymbol.get(symbol).c;
  }

  deleteTicksBetween(symbol, start, end) {
    return this.stmts.deleteTicksBetween.run(symbol, start, end).changes;
  }

  // ---- Candles ----
  saveCandle(symbol, epoch, open, high, low, close) {
    return this.stmts.insertCandle.run(symbol, epoch, open, high, low, close).changes > 0;
  }

  /**
   * FIX: CandleBuilder (used during live incremental tick processing) assumes
   * ticks always arrive in forward chronological order — it closes a candle
   * bucket the instant it sees a LATER timestamp. That assumption is true for
   * live streaming, but historical downloads paginate BACKWARD (newest page
   * first, then progressively older pages) — so every tick from any page
   * after the first looked "out of order" to CandleBuilder and was silently
   * dropped from candle-building (the raw tick was still saved fine, just
   * never turned into a candle). Net effect: only the most recent ~1 page's
   * worth of candles ever got built correctly for any multi-page download.
   *
   * This rebuilds candles for a range directly from the ticks table (which
   * was never affected by the bug — only candle-building was), reading them
   * back out in guaranteed ascending order via getTicksBetween's ORDER BY,
   * so the result is correct regardless of what order they were originally
   * downloaded/inserted in. Existing candles in the range are replaced.
   */
  rebuildCandlesForRange(symbol, start, end) {
    const ticks = this.getTicksBetween(symbol, start, end); // already ORDER BY epoch ASC
    this.deleteCandlesBetween(symbol, start, end);
    if (!ticks.length) return 0;
    let built = 0;
    const rebuild = this.db.transaction((rows) => {
      let cur = null;
      for (const t of rows) {
        const bucketEpoch = Math.floor(t.epoch / 60) * 60;
        if (!cur) { cur = { bucketEpoch, open: t.quote, high: t.quote, low: t.quote, close: t.quote }; continue; }
        if (bucketEpoch === cur.bucketEpoch) {
          cur.high = Math.max(cur.high, t.quote); cur.low = Math.min(cur.low, t.quote); cur.close = t.quote;
          continue;
        }
        this.stmts.insertCandle.run(symbol, cur.bucketEpoch, cur.open, cur.high, cur.low, cur.close); built++;
        cur = { bucketEpoch, open: t.quote, high: t.quote, low: t.quote, close: t.quote };
      }
      if (cur) { this.stmts.insertCandle.run(symbol, cur.bucketEpoch, cur.open, cur.high, cur.low, cur.close); built++; }
    });
    rebuild(ticks);
    return built;
  }

  getLatestCandle(symbol) {
    return this.stmts.getLatestCandle.get(symbol) || null;
  }

  getCandlesBetween(symbol, start, end) {
    return this.stmts.getCandlesBetween.all(symbol, start, end);
  }

  countCandles(symbol) {
    return this.stmts.countCandlesForSymbol.get(symbol).c;
  }

  deleteCandlesBetween(symbol, start, end) {
    return this.stmts.deleteCandlesBetween.run(symbol, start, end).changes;
  }

  // ---- Management ----
  listSymbols() {
    return this.stmts.distinctSymbols.all().map((r) => r.symbol);
  }

  /**
   * One-time self-heal for data downloaded before rebuildCandlesForRange()
   * existed — candle-building during backward-paginated downloads was
   * corrupted (see rebuildCandlesForRange's comment), and that corruption
   * sits permanently in the candles table until something happens to
   * rebuild that exact range. Rather than leave it to chance (only fixed
   * when a backtest or new download happens to touch that range), this
   * rebuilds EVERY symbol's full tick-covered range once, so anything
   * downloaded before this fix gets corrected on the next server start
   * regardless of whether it's ever backtested.
   */
  rebuildAllCandles(logger = console) {
    const symbols = this.listSymbols();
    let totalRebuilt = 0;
    for (const symbol of symbols) {
      const oldest = this.getOldestTick(symbol);
      const newest = this.getLatestTick(symbol);
      if (!oldest || !newest) continue;
      const built = this.rebuildCandlesForRange(symbol, oldest.epoch, newest.epoch);
      totalRebuilt += built;
      logger.info && logger.info(`[startup] rebuilt ${built} candles for ${symbol} [${oldest.epoch}..${newest.epoch}]`);
    }
    return totalRebuilt;
  }

  /** Full stats block used by the Database Manager panel. */
  getStats(configuredSymbols) {
    const symbols = Array.from(new Set([...(configuredSymbols || []), ...this.listSymbols()]));
    const perSymbol = symbols.map((symbol) => {
      const tickCount = this.countTicks(symbol);
      const candleCount = this.countCandles(symbol);
      const oldest = this.getOldestTick(symbol);
      const newest = this.getLatestTick(symbol);
      return {
        symbol,
        tickCount,
        candleCount,
        oldestEpoch: oldest ? oldest.epoch : null,
        newestEpoch: newest ? newest.epoch : null,
      };
    });
    return {
      symbols: perSymbol,
      dbSizeBytes: this.getDatabaseSizeBytes(),
      dbPath: this.dbPath,
    };
  }

  /**
   * Cheap integrity check: confirms the SQLite file itself is not corrupt,
   * and reports any symbol whose tick coverage has gaps larger than
   * `gapThresholdSeconds` (informational only — large gaps are expected
   * around weekends/maintenance and are not necessarily an error).
   */
  verifyIntegrity(configuredSymbols, gapThresholdSeconds = 300) {
    const pragmaResult = this.db.pragma('integrity_check');
    const sqliteOk = Array.isArray(pragmaResult) && pragmaResult.length === 1 && pragmaResult[0].integrity_check === 'ok';

    const gapReports = [];
    for (const symbol of configuredSymbols || this.listSymbols()) {
      const rows = this.db
        .prepare(`SELECT epoch FROM ticks WHERE symbol = ? ORDER BY epoch ASC`)
        .all(symbol);
      let gaps = 0;
      let largestGap = 0;
      for (let i = 1; i < rows.length; i++) {
        const diff = rows[i].epoch - rows[i - 1].epoch;
        if (diff > gapThresholdSeconds) {
          gaps += 1;
          if (diff > largestGap) largestGap = diff;
        }
      }
      gapReports.push({ symbol, tickCount: rows.length, gapsOverThreshold: gaps, largestGapSeconds: largestGap });
    }

    return { sqliteIntegrityOk: sqliteOk, gapThresholdSeconds, symbols: gapReports };
  }

  getDatabaseSizeBytes() {
    let total = 0;
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        total += fs.statSync(this.dbPath + suffix).size;
      } catch (_) {
        /* file may not exist yet */
      }
    }
    return total;
  }

  close() {
    try {
      this.db.pragma('wal_checkpoint(TRUNCATE)');
    } catch (_) {}
    this.db.close();
  }
}

// ─── DataEngine: candle builder (merged from lib/dataengine/candleBuilder.js) ──
/**
 * lib/dataengine/candleBuilder.js
 *
 * Builds persisted OHLC candles from ticks for the DataEngine's own storage.
 * Completely separate from server.js's existing in-memory CandleManager
 * (which feeds live strategy execution) — this one writes durable candles
 * to SQLite for future backtesting. Historical backfill and live collection
 * both funnel through this exact same class so candle construction is
 * identical regardless of source, per spec.
 */
class CandleBuilder {
  constructor(db, intervalSeconds, logger = console) {
    this.db = db;
    this.intervalSeconds = intervalSeconds;
    this.logger = logger;
    this.openCandles = new Map(); // symbol -> { bucketEpoch, open, high, low, close }
  }

  _bucketStart(epoch) {
    return Math.floor(epoch / this.intervalSeconds) * this.intervalSeconds;
  }

  processTick(symbol, epoch, quote) {
    const bucketEpoch = this._bucketStart(epoch);
    let current = this.openCandles.get(symbol);

    if (!current) {
      this.openCandles.set(symbol, { bucketEpoch, open: quote, high: quote, low: quote, close: quote });
      return;
    }

    if (bucketEpoch === current.bucketEpoch) {
      current.high = Math.max(current.high, quote);
      current.low = Math.min(current.low, quote);
      current.close = quote;
      return;
    }

    if (bucketEpoch > current.bucketEpoch) {
      this.db.saveCandle(symbol, current.bucketEpoch, current.open, current.high, current.low, current.close);
      this.openCandles.set(symbol, { bucketEpoch, open: quote, high: quote, low: quote, close: quote });
      return;
    }
    // out-of-order tick for an already-closed bucket — tick itself is still
    // safely stored by the caller; only candle aggregation skips it.
  }

  flushOpenCandle(symbol) {
    const current = this.openCandles.get(symbol);
    if (current) {
      this.db.saveCandle(symbol, current.bucketEpoch, current.open, current.high, current.low, current.close);
    }
  }

  flushAll() {
    for (const symbol of this.openCandles.keys()) this.flushOpenCandle(symbol);
  }
}

// ─── DataEngine: historical downloader (merged from lib/dataengine/historyDownloader.js) ──
/**
 * lib/dataengine/historyDownloader.js
 *
 * Downloads historical TICKS from Deriv's ticks_history endpoint and
 * manages user-triggered download jobs (pause/resume/cancel/progress).
 *
 * PAGINATION — backward via `end`+`count`, WITH an explicit `start` on every
 * page (fixed after confirming with Deriv support — see chat transcript).
 * Earlier assumption here was that `start` is always ignored by Deriv and
 * only `end`+`count` matter; that was wrong. What was actually happening:
 * without an explicit `start`, Deriv silently defaults `start` to ~1 day
 * ago and returns only recent ticks regardless of how far back `end`
 * points — it does NOT error, so it looked like `start` was "ignored".
 * Deriv confirmed raw tick data is retained for about a month, but only
 * reachable by sending `start` explicitly. So every page now sends the
 * segment's lower bound as `start`, and still walks backward within that
 * bounded range: request a page ending "now" (or the current cursor), note
 * the OLDEST epoch received, set the next request's `end` to (that epoch -
 * 1), and repeat until the segment's `start` is reached.
 *
 * CONNECTION HANDLING — fixed after a real stall observed on a 7-day
 * download (progress would repeatedly stick around ~10% and not
 * continue). Root cause: the previous version opened a BRAND NEW
 * WebSocket connection for every single page — for a multi-day download
 * needing 100+ pages, that's 100+ rapid connect/disconnect cycles with no
 * delay between them, which very likely triggered connection throttling
 * or drops on Deriv's side. Fixed by:
 *   - Reusing ONE persistent connection for the life of a download job
 *     (opened once, kept alive, reconnected automatically if it drops).
 *   - Adding a small delay between page requests (PAGE_DELAY_MS) so the
 *     downloader isn't hammering the server back-to-back.
 *   - Correlating requests/responses by req_id instead of just "the next
 *     message", so a dropped/reconnected socket can't cause a response
 *     mismatch.
 *   - Logging every page fetch and every connection event via the
 *     provided logger, so a stall shows up clearly in the server console
 *     instead of silently freezing the dashboard's progress bar.
 *
 * NOTE: a ping/pong keepalive heartbeat was attempted here (per Deriv's
 * own stated best practice) but was found via testing to cause a severe
 * reconnect storm under certain timing conditions and was reverted. The
 * connection is instead kept alive implicitly by the natural cadence of
 * page requests every PAGE_DELAY_MS; genuine drops are still caught by
 * the socket's native 'close'/'error' events and trigger a clean
 * reconnect on the next request.
 *
 * Historical downloads NEVER start automatically — this module only ever
 * runs when explicitly triggered via the dashboard's Download Manager.
 */

const PAGE_DELAY_MS = 250;
const REQUEST_TIMEOUT_MS = 20000;
const RECONNECT_DELAY_MS = 2000;

class HistoryDownloader {
  /**
   * @param {DataEngineDB} db
   * @param {CandleBuilder} candleBuilder
   * @param {string} wsUrl e.g. wss://ws.binaryws.com/websockets/v3?app_id=1089
   * @param {object} logger
   * @param {number} pageSize ticks requested per call via `count` (default 5000, confirmed accepted by Deriv)
   */
  constructor(db, candleBuilder, wsUrl, logger = console, pageSize = 5000) {
    this.db = db;
    this.candleBuilder = candleBuilder;
    this.wsUrl = wsUrl;
    this.logger = logger;
    this.pageSize = pageSize;
    this.jobs = new Map();
    this._jobCounter = 1;

    // Single shared, persistent, reconnecting connection reused across
    // every download job (historical fetches only — never touches the
    // trading account socket).
    this.ws = null;
    this.wsConnecting = null; // in-flight connect promise, if any
    this.reqIdCounter = 1;
    this.pending = new Map(); // req_id -> {resolve, reject, timeout}
  }

  listJobs() {
    return Array.from(this.jobs.values()).map((j) => this._publicJob(j));
  }

  getJob(jobId) {
    const j = this.jobs.get(jobId);
    return j ? this._publicJob(j) : null;
  }

  _publicJob(j) {
    const elapsedMs = Date.now() - j.startedAt;
    const wallClockSecElapsed = Math.max(0.001, elapsedMs / 1000);

    let secondsCovered = j.completedSegmentSeconds;
    if (j.currentSegment) {
      const [segStart, segEnd] = j.currentSegment;
      const covered = j.cursor != null ? Math.max(0, segEnd - j.cursor) : 0;
      secondsCovered += Math.min(covered, segEnd - segStart);
    }
    const totalSeconds = Math.max(1, j.totalRequestedSeconds);
    const percent = j.status === 'completed' ? 100 : Math.min(100, (secondsCovered / totalSeconds) * 100);

    const rateMultiplier = secondsCovered > 0 ? secondsCovered / wallClockSecElapsed : 0;
    const remainingSeconds = Math.max(0, totalSeconds - secondsCovered);
    const etaSeconds = rateMultiplier > 0 ? Math.round(remainingSeconds / rateMultiplier) : null;
    const ticksPerSecond = j.ticksDownloaded > 0 ? +(j.ticksDownloaded / wallClockSecElapsed).toFixed(1) : 0;

    return {
      id: j.id,
      symbol: j.symbol,
      status: j.status,
      ticksDownloaded: j.ticksDownloaded,
      pagesDownloaded: j.pagesDownloaded,
      percent: +percent.toFixed(2),
      currentPeriod: j.cursor ? new Date(j.cursor * 1000).toISOString() : null,
      fromPeriod: new Date(j.targetStart * 1000).toISOString(),
      toPeriod: new Date(j.targetEnd * 1000).toISOString(),
      etaSeconds,
      downloadSpeedTicksPerSec: ticksPerSecond,
      error: j.error || null,
      note: j.note || null,
    };
  }

  startDownload(symbol, seconds, onProgress) {
    const existing = Array.from(this.jobs.values()).find(j => j.symbol === symbol && (j.status === 'running' || j.status === 'paused'));
    if (existing) {
      throw new Error(`A download for ${symbol} is already ${existing.status} (job ${existing.id}) — wait for it to finish or cancel it first.`);
    }
    const now = Math.floor(Date.now() / 1000);
    const targetStart = now - seconds;
    const targetEnd = now;
    const oldest = this.db.getOldestTick(symbol);
    const newest = this.db.getLatestTick(symbol);

    const segments = [];
    if (!oldest) {
      segments.push([targetStart, targetEnd]);
    } else {
      if (newest.epoch < targetEnd) segments.push([newest.epoch + 1, targetEnd]);
      if (oldest.epoch > targetStart) segments.push([targetStart, oldest.epoch - 1]);
    }

    const totalRequestedSeconds = segments.reduce((sum, [s, e]) => sum + Math.max(0, e - s + 1), 0);

    const job = {
      id: `dl${this._jobCounter++}`,
      symbol,
      targetStart,
      targetEnd,
      segments,
      segmentQueueIndex: 0,
      currentSegment: null,
      completedSegmentSeconds: 0,
      totalRequestedSeconds,
      cursor: null,
      status: 'running',
      ticksDownloaded: 0,
      pagesDownloaded: 0,
      startedAt: Date.now(),
      error: null,
      note: segments.length === 0 ? 'Requested range is already fully covered by existing data.' : null,
      _paused: false,
      _cancelled: false,
      _resumeWaiters: [],
    };
    this.jobs.set(job.id, job);
    this.logger.info && this.logger.info(`[download:${job.id}] starting: symbol=${symbol} segments=${JSON.stringify(segments)} totalRequestedSeconds=${totalRequestedSeconds}`);

    this._runJob(job, onProgress).catch((err) => {
      job.status = 'error';
      job.error = err.message;
      this.logger.error && this.logger.error(`[download:${job.id}] fatal error: ${err.message}`);
      if (onProgress) onProgress(this._publicJob(job));
    });

    return this._publicJob(job);
  }

  pauseJob(jobId) {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== 'running') return null;
    job._paused = true;
    job.status = 'paused';
    this.logger.info && this.logger.info(`[download:${job.id}] paused by user`);
    return this._publicJob(job);
  }

  resumeJob(jobId, onProgress) {
    const job = this.jobs.get(jobId);
    if (!job || job.status !== 'paused') return null;
    job._paused = false;
    job.status = 'running';
    this.logger.info && this.logger.info(`[download:${job.id}] resumed by user`);
    const waiters = job._resumeWaiters;
    job._resumeWaiters = [];
    waiters.forEach((resolve) => resolve());
    if (onProgress) onProgress(this._publicJob(job));
    return this._publicJob(job);
  }

  cancelJob(jobId) {
    const job = this.jobs.get(jobId);
    if (!job) return null;
    job._cancelled = true;
    job.status = 'cancelled';
    this.logger.info && this.logger.info(`[download:${job.id}] cancelled by user`);
    if (job._paused) {
      job._paused = false;
      const waiters = job._resumeWaiters;
      job._resumeWaiters = [];
      waiters.forEach((resolve) => resolve());
    }
    return this._publicJob(job);
  }

  async _waitIfPaused(job) {
    if (!job._paused) return;
    await new Promise((resolve) => job._resumeWaiters.push(resolve));
  }

  async _runJob(job, onProgress) {
    if (job.segments.length === 0) {
      job.status = 'completed';
      if (onProgress) onProgress(this._publicJob(job));
      return;
    }

    for (job.segmentQueueIndex = 0; job.segmentQueueIndex < job.segments.length; job.segmentQueueIndex++) {
      if (job._cancelled) break;
      const [segStart, segEnd] = job.segments[job.segmentQueueIndex];
      job.currentSegment = [segStart, segEnd];
      job.cursor = segEnd;

      this.logger.info && this.logger.info(
        `[download:${job.id}] === starting segment ${job.segmentQueueIndex + 1}/${job.segments.length}: ` +
        `[${segStart}..${segEnd}] (span=${segEnd - segStart}s, ${new Date(segStart*1000).toISOString()} .. ${new Date(segEnd*1000).toISOString()}) ===`
      );

      const ok = await this._runSegment(job, segStart, segEnd, onProgress);
      job.completedSegmentSeconds += Math.max(0, segEnd - segStart + 1);
      this.logger.info && this.logger.info(
        `[download:${job.id}] === segment ${job.segmentQueueIndex + 1}/${job.segments.length} finished (ok=${ok}, cursor ended at ${job.cursor}) ===`
      );
      job.currentSegment = null;
      if (!ok) break;
    }

    this.candleBuilder.flushOpenCandle(job.symbol);
    // FIX: candles built incrementally DURING a backward-paginated download
    // get corrupted (see rebuildCandlesForRange() for the full explanation) —
    // so once the job's done, throw away whatever candles the incremental
    // builder produced for this range and rebuild them properly from the
    // ticks table, which was never affected by the ordering bug.
    if (job.ticksDownloaded > 0) {
      const rebuilt = this.db.rebuildCandlesForRange(job.symbol, job.targetStart, job.targetEnd);
      this.logger.info && this.logger.info(`[download:${job.id}] rebuilt ${rebuilt} candles for [${job.targetStart}..${job.targetEnd}] from raw ticks`);
    }
    if (job._cancelled && job.status !== 'error') job.status = 'cancelled';
    else if (!job._cancelled && job.status !== 'error') job.status = 'completed';
    this.logger.info && this.logger.info(`[download:${job.id}] finished: status=${job.status} ticks=${job.ticksDownloaded} pages=${job.pagesDownloaded}`);
    if (onProgress) onProgress(this._publicJob(job));
  }

  async _runSegment(job, segStart, segEnd, onProgress) {
    const segmentSpanSeconds = Math.max(1, segEnd - segStart);
    const maxPages = Math.max(500, Math.ceil(segmentSpanSeconds / this.pageSize) + 100);

    while (job.cursor >= segStart) {
      if (job._cancelled) return false;
      await this._waitIfPaused(job);
      if (job._cancelled) return false;

      if (job.pagesDownloaded >= maxPages * (job.segmentQueueIndex + 1)) {
        job.status = 'error';
        job.error = 'Safety limit reached without completing — investigate before retrying.';
        this.logger.error && this.logger.error(`[download:${job.id}] ${job.error}`);
        return false;
      }

      let page;
      const requestedEnd = job.cursor;
      try {
        page = await this._fetchPage(job.symbol, requestedEnd, segStart);
      } catch (err) {
        job.status = 'error';
        job.error = err.message;
        this.logger.error && this.logger.error(`[download:${job.id}] page fetch failed at cursor=${requestedEnd}: ${err.message}`);
        return false;
      }

      job.pagesDownloaded += 1;

      if (page.times.length === 0) {
        const note = `No ticks available at or before ${new Date(requestedEnd * 1000).toISOString()} — ` +
          `this is likely the retention limit for raw tick data on this symbol.`;
        job.note = job.note ? job.note : note;
        this.logger.info && this.logger.info(`[download:${job.id}] page ${job.pagesDownloaded}: empty response — ${note}`);
        return true;
      }

      const earliestEpoch = page.times[0];
      const latestEpoch = page.times[page.times.length - 1];

      // CRITICAL: detect a real, confirmed Deriv API quirk found via live
      // testing — when `end` is requested further back than the server's
      // raw-tick retention window, it does NOT return empty or an error.
      // It silently ignores our `end` and returns its most recent ticks
      // instead (as if `end` were "latest"). Without this check, that looks
      // like a normal page and the loop keeps walking back to the same
      // wall, getting redirected to "now" again, forever — which is
      // exactly the "stuck at ~24h" symptom this was built to catch.
      // Detected by: the newest tick in the page is newer than the `end`
      // we actually asked for (with a small tolerance for tick-rate jitter).
      const RETENTION_TOLERANCE_SECONDS = 5;
      if (latestEpoch > requestedEnd + RETENTION_TOLERANCE_SECONDS) {
        const note = `Reached Deriv's raw-tick retention limit at ${new Date(requestedEnd * 1000).toISOString()} — ` +
          `requests for data further back than that are being silently answered with the most recent ticks ` +
          `instead of the requested range (a confirmed Deriv API quirk), so no further historical data is ` +
          `retrievable for this symbol beyond this point.`;
        job.note = job.note ? job.note : note;
        this.logger.warn && this.logger.warn(
          `[download:${job.id}] seg${job.segmentQueueIndex + 1}[${segStart}..${segEnd}] page ${job.pagesDownloaded}: ` +
          `RETENTION WALL detected — requested end=${requestedEnd} but got latestEpoch=${latestEpoch} (newer than requested). ${note}`
        );
        return true;
      }

      const ticks = page.times.map((t, i) => ({ symbol: job.symbol, epoch: t, quote: page.prices[i] }));
      const insertedRows = this.db.saveTicksBatch(ticks);
      for (const row of insertedRows) this.candleBuilder.processTick(row.symbol, row.epoch, row.quote);
      job.ticksDownloaded += insertedRows.length;

      this.logger.info && this.logger.info(
        `[download:${job.id}] seg${job.segmentQueueIndex + 1}[${segStart}..${segEnd}] page ${job.pagesDownloaded}: received=${page.times.length} new=${insertedRows.length} ` +
        `range=[${earliestEpoch}..${latestEpoch}] cursor->${earliestEpoch - 1}`
      );

      if (onProgress) onProgress(this._publicJob(job));

      if (earliestEpoch <= segStart) return true;

      job.cursor = earliestEpoch - 1;

      if (PAGE_DELAY_MS > 0) {
        await this._sleep(PAGE_DELAY_MS);
      }
    }
    return true;
  }

  _sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  // =======================================================================
  // Persistent, reused, auto-reconnecting connection
  // =======================================================================

  /** Ensures a live, open WebSocket exists, connecting/reconnecting as needed. */
  async _ensureConnected() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    if (this.wsConnecting) return this.wsConnecting;

    this.wsConnecting = new Promise((resolve, reject) => {
      this.logger.info && this.logger.info(`[history-socket] connecting to ${this.wsUrl}`);
      const ws = new WebSocket(this.wsUrl);

      const onOpen = () => {
        this.logger.info && this.logger.info('[history-socket] connected');
        this.ws = ws;
        this.wsConnecting = null;
        resolve();
      };
      const onError = (err) => {
        this.logger.warn && this.logger.warn(`[history-socket] connection error: ${err.message}`);
      };
      const onClose = (code) => {
        this.logger.warn && this.logger.warn(`[history-socket] connection closed (code=${code}) — will reconnect on next request`);
        if (this.ws === ws) this.ws = null;
        this.wsConnecting = null;
        // Reject any requests that were pending on this now-dead socket.
        for (const [reqId, pending] of this.pending) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('Connection closed before response arrived'));
          this.pending.delete(reqId);
        }
      };

      ws.once('open', onOpen);
      ws.once('error', (err) => { onError(err); reject(err); });
      ws.on('close', onClose);
      ws.on('message', (raw) => this._handleMessage(raw));
    });

    return this.wsConnecting;
  }

  _handleMessage(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (msg.req_id != null && this.pending.has(msg.req_id)) {
      const pending = this.pending.get(msg.req_id);
      clearTimeout(pending.timeout);
      this.pending.delete(msg.req_id);
      if (msg.error) pending.reject(new Error(msg.error.message));
      else pending.resolve(msg);
    }
  }

  async _send(payload) {
    // Reconnect with a short backoff if the connection is currently down.
    let attempts = 0;
    while (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      attempts += 1;
      try {
        await this._ensureConnected();
      } catch (err) {
        if (attempts >= 3) throw new Error(`Unable to connect to Deriv after ${attempts} attempts: ${err.message}`);
        await this._sleep(RECONNECT_DELAY_MS);
      }
    }

    const req_id = this.reqIdCounter++;
    const body = { ...payload, req_id };

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(req_id);
        reject(new Error(`Request timed out after ${REQUEST_TIMEOUT_MS}ms`));
      }, REQUEST_TIMEOUT_MS);

      this.pending.set(req_id, { resolve, reject, timeout });

      try {
        this.ws.send(JSON.stringify(body));
      } catch (err) {
        clearTimeout(timeout);
        this.pending.delete(req_id);
        reject(err);
      }
    });
  }

  /**
   * Backward-pagination request: `end` + `count`, PLUS an explicit `start`.
   *
   * FIX (confirmed with Deriv support): requests using only `count` + `end`
   * are silently capped to roughly the last 24 hours — Deriv defaults
   * `start` to ~1 day ago internally and ignores how far back `end` points,
   * which is exactly the "retention wall" behavior detected below. Deriv
   * confirmed raw tick data is actually available for about a month, but
   * only if every request carries an explicit `start` epoch. So every page
   * request now also sends `start` (the lower bound of the segment being
   * downloaded) — `count` + `end` still control which page within that
   * bounded range comes back, but `start` is what unlocks data older than
   * ~24h instead of it being silently discarded.
   */
  async _fetchPage(symbol, end, start) {
    const payload = { ticks_history: symbol, end, count: this.pageSize, style: 'ticks' };
    if (start != null) payload.start = start;
    const resp = await this._send(payload);
    if (!resp.history) {
      throw new Error(`Unexpected ticks_history response shape: ${JSON.stringify(resp).slice(0, 200)}`);
    }
    return { times: resp.history.times || [], prices: resp.history.prices || [] };
  }
}

// ─── DataEngine: live tick collector (merged from lib/dataengine/liveTickCollector.js) ──
/**
 * lib/dataengine/liveTickCollector.js
 *
 * Persists live ticks for the DataEngine's configured markets. Deliberately
 * does NOT open its own WebSocket — it taps the bot's existing, already
 * ref-counted `candleManager.subscribeTicks(symbol, cb)` tick multiplexer
 * (see server.js), so whether zero, one, or ten slots are also watching the
 * same symbol for trading, there is still only ever one live connection per
 * symbol to Deriv. This satisfies "one shared market-data pipeline."
 *
 * Runs continuously once started (unlike historical downloads, which are
 * always user-triggered) — this is the DataEngine's ongoing background
 * collection, so backtestable history keeps accumulating even when no
 * slot is actively trading a given market.
 */
class LiveTickCollector {
  /**
   * @param {DataEngineDB} db
   * @param {CandleBuilder} candleBuilder
   * @param {{subscribeTicks: Function}} candleManager the bot's existing shared tick source
   * @param {object} logger
   */
  constructor(db, candleBuilder, candleManager, logger = console) {
    this.db = db;
    this.candleBuilder = candleBuilder;
    this.candleManager = candleManager;
    this.logger = logger;
    this.unsubscribers = new Map(); // symbol -> unsubscribe fn
    this.stats = new Map(); // symbol -> { ticksPersisted }
  }

  start(symbols) {
    for (const symbol of symbols) this.startSymbol(symbol);
  }

  startSymbol(symbol) {
    if (this.unsubscribers.has(symbol)) return;
    this.stats.set(symbol, { ticksPersisted: 0 });
    const unsub = this.candleManager.subscribeTicks(symbol, ({ epoch, quote }) => {
      const inserted = this.db.saveTick(symbol, epoch, quote);
      if (inserted) {
        this.candleBuilder.processTick(symbol, epoch, quote);
        this.stats.get(symbol).ticksPersisted += 1;
      }
    });
    this.unsubscribers.set(symbol, unsub);
    this.logger.info && this.logger.info(`[dataengine] live tick collection started for ${symbol}`);
  }

  stopSymbol(symbol) {
    const unsub = this.unsubscribers.get(symbol);
    if (!unsub) return false;
    try { unsub(); } catch (_) {}
    this.unsubscribers.delete(symbol);
    this.candleBuilder.flushOpenCandle(symbol);
    this.logger.info && this.logger.info(`[dataengine] live tick collection stopped for ${symbol}`);
    return true;
  }

  isRunning(symbol) {
    return this.unsubscribers.has(symbol);
  }

  stopAll() {
    for (const [symbol, unsub] of this.unsubscribers) {
      try { unsub(); } catch (_) {}
      this.logger.info && this.logger.info(`[dataengine] live tick collection stopped for ${symbol}`);
    }
    this.unsubscribers.clear();
    this.candleBuilder.flushAll();
  }

  getStats(symbol) {
    return this.stats.get(symbol) || { ticksPersisted: 0 };
  }
}


const app = express();
app.use(express.json());

const PORT         = process.env.PORT         || 3000;
const APP_ID_LIVE  = process.env.DERIV_APP_ID || '33HTyuiZsviXpxyNSLIrE';
const REDIRECT_URI = process.env.REDIRECT_URI || 'https://sharpshooter-vfb2.onrender.com/callback';
const WS_DEMO      = 'wss://api.derivws.com/trading/v1/options/ws/public';
const STATE_FILE   = path.join(__dirname, 'state.json');

const num = v => { const n = parseFloat(v); return isNaN(n) ? null : n; };
function serverLog(msg, data) { console.log(`[${new Date().toISOString()}] ${msg}`, data || ''); }

process.on('uncaughtException',  e => serverLog('uncaughtException',  { err: e && e.message }));
process.on('unhandledRejection', e => serverLog('unhandledRejection', { err: e && e.message ? e.message : String(e) }));

// ─── graceful shutdown (added) ─────────────────────────────────────────────────
// Flushes the DataEngine's SQLite WAL and any still-open persisted candle
// cleanly on exit. Does not touch existing trading/slot behavior — the
// process still terminates the same way it always did; this just runs
// first when a shutdown signal is received.
let _shuttingDown = false;
function gracefulShutdown(signal) {
  if (_shuttingDown) return;
  _shuttingDown = true;
  serverLog(`${signal} received — flushing DataEngine before exit`);
  try { liveTickCollector.stopAll(); } catch (e) { serverLog('liveTickCollector stop error', { err: e.message }); }
  try { dataDb.close(); } catch (e) { serverLog('dataDb close error', { err: e.message }); }
  process.exit(0);
}
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// ─── PKCE helpers ─────────────────────────────────────────────────────────────
function base64url(buf){ return buf.toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }
function generateCodeVerifier(){ return base64url(crypto.randomBytes(32)); }
function generateCodeChallenge(v){ return base64url(crypto.createHash('sha256').update(v).digest()); }
const oauthPending = new Map();
// Holds the access token + full account list between "logged in with Deriv"
// and "user picked which account to trade on". Single global slot is fine —
// this bot only ever drives one browser session at a time.
let pendingLiveAuth = null;

// ─── SSE ─────────────────────────────────────────────────────────────────────
const clients = [];
function push(data){
  const s = `data: ${JSON.stringify(data)}\n\n`;
  clients.forEach(r => { try { r.write(s); } catch {} });
}

// ─── Deriv account ────────────────────────────────────────────────────────────
class Account {
  constructor(){
    this.token = ''; this.ws = null; this.ready = false;
    this.loginid = null; this.balance = null; this.currency = null;
    this.reqs = new Map(); this.watchers = new Map(); this.rid = 1;
    this.pinger = null; this._reconnecting = false;
    this.liveAccessToken = null; this.liveAccountId = null; this.isLive = false;
  }

  setToken(t){
    if (!t || t === this.token) return;
    this.token = t; this.ready = false; this.isLive = false;
    if (this.ws) try { this.ws.close(); } catch {}
  }

  _attachHandlers(ws){
    // Guard: this.ws may be swapped to a NEW connection before this (older) socket's
    // own async events finish firing (e.g. after ws.close()). Without this check, a
    // stale socket's 'close'/'error'/'message' events would wrongly mutate shared
    // account state (this.ready, this.pinger, etc.) belonging to the CURRENT connection,
    // causing spurious disconnect/reconnect flapping. Every handler below first confirms
    // it's still reporting on the connection that is actually active.
    ws.on('message', raw => {
      if (ws !== this.ws) return;
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.req_id && this.reqs.has(m.req_id)) { this.reqs.get(m.req_id)(m); this.reqs.delete(m.req_id); }
      if (m.msg_type === 'proposal_open_contract') {
        const c = m.proposal_open_contract;
        if (c && this.watchers.has(c.contract_id)) this.watchers.get(c.contract_id)(m);
      }
      if (m.msg_type === 'balance') {
        this.balance = m.balance?.balance;
        if (m.balance?.currency) this.currency = m.balance.currency; // live path never got this before — connectLive() has no authorize step to set it from
        push({ type:'account', data:this.info() });
      }
    });
    ws.on('error', e => { if (ws !== this.ws) return; serverLog('WS error', { err: e.message }); });
    ws.on('close', () => {
      if (ws !== this.ws) return; // stale socket — a newer connection already replaced it, ignore
      this.ready = false;
      if (this.pinger) clearInterval(this.pinger);
      if (this.balancePoller) clearInterval(this.balancePoller);
      push({ type:'account', data:this.info() });
      if (this.isLive && this.liveAccessToken && this.liveAccountId && !this._reconnecting) {
        this._reconnecting = true;
        serverLog('Live disconnected — refreshing OTP in 5s');
        setTimeout(() => this.refreshOTP(), 5000);
      } else if (!this.isLive && this.token && !this._reconnecting) {
        this._reconnecting = true;
        serverLog('Demo disconnected — reconnecting in 4s');
        setTimeout(() => { this.connect().catch(e => { this._reconnecting = false; serverLog('Reconnect failed', { err: e.message }); }); }, 4000);
      }
    });
  }

  connect(){
    return new Promise((resolve, reject) => {
      if (!this.token) return reject(new Error('No token — enter your demo API token'));
      if (this.ws) try { this.ws.close(); } catch {}
      const ws = new WebSocket(WS_DEMO);
      this.ws = ws; this.isLive = false;
      ws.on('open', () => {
        this.send({ authorize: this.token }).then(r => {
          if (r.error) return reject(new Error('Auth failed: ' + r.error.message));
          this.ready = true;
          this.loginid = r.authorize?.loginid;
          this.balance = r.authorize?.balance;
          this.currency = r.authorize?.currency;
          this._reconnecting = false;
          serverLog('Demo account connected', { loginid: this.loginid });
          this.pinger = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 })); }, 25000);
          this.send({ balance: 1, subscribe: 1 }).catch(() => {});
          push({ type:'account', data:this.info() });
          resolve();
        }).catch(e => { this._reconnecting = false; reject(e); });
      });
      this._attachHandlers(ws);
    });
  }

  connectLive(wssUrl, accountId){
    return new Promise((resolve, reject) => {
      if (this.ws) try { this.ws.close(); } catch {}
      if (this.pinger) clearInterval(this.pinger);
      if (this.balancePoller) clearInterval(this.balancePoller);
      const ws = new WebSocket(wssUrl);
      this.ws = ws; this.isLive = true; this.loginid = accountId;
      ws.on('open', () => {
        this.ready = true; this._reconnecting = false;
        serverLog('Live account connected', { loginid: accountId });
        this.pinger = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: 1 })); }, 25000);
        this.send({ balance: 1, subscribe: 1 }).catch(() => {});
        // Fallback balance refresh via REST: we don't yet have confirmation
        // that the WS balance push above uses the same message shape on
        // this newer Options API as it did on the legacy one (proposal/buy
        // turned out to need different field names, so balance may too).
        // Polling the account endpoint directly guarantees the dashboard
        // shows a real, current balance either way.
        this.refreshBalanceFromRest();
        this.balancePoller = setInterval(() => this.refreshBalanceFromRest(), 15000);
        push({ type:'account', data:this.info() });
        resolve();
      });
      this._attachHandlers(ws);
      ws.on('error', e => reject(e));
    });
  }

  async refreshBalanceFromRest(){
    if (!this.liveAccountId || !this.liveAccessToken) return;
    try {
      const res = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${this.liveAccountId}`, {
        headers:{ 'Authorization':`Bearer ${this.liveAccessToken}`, 'Deriv-App-ID':APP_ID_LIVE },
      });
      const data = await res.json();
      const acctData = Array.isArray(data.data) ? data.data[0] : data.data;
      if (acctData?.balance != null) {
        this.balance = acctData.balance;
        if (acctData.currency) this.currency = acctData.currency;
        push({ type:'account', data:this.info() });
      }
    } catch (err) {
      serverLog('Balance refresh failed', { err: err.message });
    }
  }

  async refreshOTP(){
    try {
      const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${this.liveAccountId}/otp`, {
        method:'POST', headers:{ 'Authorization':`Bearer ${this.liveAccessToken}`, 'Deriv-App-ID':APP_ID_LIVE },
      });
      const otpData = await otpRes.json();
      if (!otpData.data?.url) throw new Error('No URL in OTP response');
      await this.connectLive(otpData.data.url, this.liveAccountId);
      this._reconnecting = false;
    } catch (err) {
      this._reconnecting = false; this.liveAccessToken = null; this.liveAccountId = null;
      serverLog('OTP refresh failed', { err: err.message });
      push({ type:'live_status', status:'expired', msg:'⚠ Session expired — please login again' });
    }
  }

  send(obj){
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return reject(new Error('Account socket not open'));
      const req_id = this.rid++;
      this.reqs.set(req_id, resolve);
      this.ws.send(JSON.stringify({ ...obj, req_id }));
      setTimeout(() => { if (this.reqs.has(req_id)) { this.reqs.delete(req_id); reject(new Error('Request timed out')); } }, 15000);
    });
  }

  // Live trades (new Options API, connected via OTP) require the two-step
  // proposal→buy flow Deriv support confirmed: get a proposal id + ask_price
  // first, then buy using exactly those two values. Demo trades stay on the
  // old direct buy:1 flow (legacy WS, already working, untouched).
  //
  // Confirmed schema (Deriv support, live chat):
  //   proposal: { proposal:1, contract_type, currency, underlying_symbol,
  //               amount, basis, duration, duration_unit, barrier (string),
  //               barrier2 (string, optional) }
  //   buy:      { buy: <proposal.id>, price: <proposal.ask_price> }
  //   response: { buy: { contract_id, buy_price, payout, transaction_id, ... } }
  async buy(params){
    if (!this.isLive) {
      return this.send({ buy: 1, price: params.amount, parameters: params });
    }

    const proposalReq = {
      proposal: 1,
      contract_type: params.contract_type,
      currency: params.currency,
      underlying_symbol: params.symbol,
      amount: params.amount,
      basis: params.basis,
      duration: params.duration,
      duration_unit: params.duration_unit,
    };
    if (params.barrier != null) proposalReq.barrier = String(params.barrier);
    if (params.barrier2 != null) proposalReq.barrier2 = String(params.barrier2);

    const proposalRes = await this.send(proposalReq);
    if (proposalRes.error) return proposalRes; // runContract() checks res.error and throws — surface it the same way
    const proposalId = proposalRes.proposal?.id;
    const askPrice = proposalRes.proposal?.ask_price;
    if (!proposalId || askPrice == null) {
      return { error: { message: 'Proposal response missing id/ask_price — cannot buy' } };
    }

    return this.send({ buy: proposalId, price: askPrice });
  }

  watchContract(id){
    return new Promise((resolve, reject) => {
      let maxSpot = null, minSpot = null, entrySpot = null, barrier = null;
      const t = setTimeout(() => { this.watchers.delete(id); reject(new Error('Watch timed out')); }, 5 * 60 * 1000);
      this.watchers.set(id, msg => {
        const c = msg.proposal_open_contract; if (!c) return;
        const cs = parseFloat(c.current_spot);
        if (!isNaN(cs)) { if (maxSpot === null || cs > maxSpot) maxSpot = cs; if (minSpot === null || cs < minSpot) minSpot = cs; }
        if (c.entry_spot != null) entrySpot = parseFloat(c.entry_spot);
        if (c.barrier != null)    barrier   = parseFloat(c.barrier);
        if (c.is_sold || c.is_expired) {
          clearTimeout(t); this.watchers.delete(id);
          this.send({ forget: msg.subscription?.id }).catch(() => {});
          c._maxSpot = maxSpot; c._minSpot = minSpot; c._entrySpot = entrySpot; c._barrier = barrier;
          resolve(c);
        }
      });
      this.send({ proposal_open_contract: 1, contract_id: id, subscribe: 1 })
        .catch(e => { clearTimeout(t); this.watchers.delete(id); reject(e); });
    });
  }

  openTickStream(symbol, cb){
    const ws = new WebSocket(WS_DEMO);
    ws.on('open', () => ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'tick' && m.tick) cb(parseFloat(m.tick.quote));
    });
    ws.on('error', () => {}); return ws;
  }

  info(){ return { ready:this.ready, loginid:this.loginid, balance:this.balance, currency:this.currency, isLive:this.isLive }; }
}

const acct = new Account();

// ─── Candle Manager ────────────────────────────────────────────────────────────
// Deriv's API no longer streams OHLC/candles — only raw ticks. This is now the
// SINGLE source of truth for candles in the bot: it owns one shared tick
// WebSocket per symbol (ref-counted — 3 slots watching the same symbol still
// means only one live connection), builds candles itself from tick epoch/price,
// and only ever hands out FULLY CLOSED candles. No strategy builds its own
// candles or talks to ticks directly for candle purposes; everything (Golden
// Logic, Super Logic, trend detection, and any future logic) subscribes here.
//
// Candle-close detection: a tick's epoch maps to a window
// (Math.floor(epoch/granularity)*granularity). While ticks keep landing in the
// same window, that window's open/high/low/close is updated. The moment a tick
// lands in a NEW window, the previous window's candle is complete and is
// emitted exactly once — matching Deriv's own recommended approach.
//
// On first subscribe for a symbol+granularity, a one-time ticks_history
// (style=candles) fetch backfills recent completed candles so strategies don't
// have to wait up to 15 minutes for the first live M15 candle to close. After
// that, everything is built from live ticks — no further polling, ever,
// unless the connection drops and needs to resync.
class CandleManager {
  constructor(){
    this.symbols = new Map(); // symbol -> { ws, grans: Map(granularity -> GranState), tickSubs: Set(cb), attempt }
  }

  // Subscribe to completed candles for a symbol at a given granularity (seconds).
  // cb(candle) fires once per fully-closed candle: { epoch, open, high, low, close }.
  // Returns an unsubscribe function.
  subscribe(symbol, granularity, cb){
    const entry = this._entry(symbol);
    let g = entry.grans.get(granularity);
    if (!g) { g = { current:null, subs:new Set(), backfilled:false }; entry.grans.set(granularity, g); }
    g.subs.add(cb);

    this._ensureConnected(symbol);
    if (!g.backfilled) this._backfill(symbol, granularity);

    return () => {
      g.subs.delete(cb);
      if (g.subs.size === 0) entry.grans.delete(granularity);
      this._maybeTeardown(symbol);
    };
  }

  // Subscribe to raw ticks for a symbol — used by Demo Mode to track entry price
  // and barrier touches live, without opening a second connection to the same feed.
  // cb({epoch, quote}) fires on every tick. Returns an unsubscribe function.
  subscribeTicks(symbol, cb){
    const entry = this._entry(symbol);
    entry.tickSubs.add(cb);
    this._ensureConnected(symbol);
    return () => {
      entry.tickSubs.delete(cb);
      this._maybeTeardown(symbol);
    };
  }

  _entry(symbol){
    let entry = this.symbols.get(symbol);
    if (!entry) { entry = { ws:null, grans:new Map(), tickSubs:new Set(), attempt:0 }; this.symbols.set(symbol, entry); }
    return entry;
  }

  _maybeTeardown(symbol){
    const entry = this.symbols.get(symbol);
    if (entry && entry.grans.size === 0 && entry.tickSubs.size === 0) this._teardown(symbol);
  }

  _ensureConnected(symbol){
    const entry = this.symbols.get(symbol);
    if (!entry || entry.ws) return;
    const ws = new WebSocket(WS_DEMO);
    entry.ws = ws;
    ws.on('open', () => { if (ws !== entry.ws) return; entry.attempt = 0; ws.send(JSON.stringify({ ticks: symbol, subscribe: 1 })); });
    ws.on('message', raw => {
      if (ws !== entry.ws) return;
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'tick' && m.tick) this._onTick(symbol, m.tick);
    });
    ws.on('error', () => {});
    ws.on('close', () => {
      if (ws !== entry.ws) return; // stale socket, already replaced — ignore
      entry.ws = null;
      if (!this.symbols.has(symbol) || (entry.grans.size === 0 && entry.tickSubs.size === 0)) return; // no subscribers left, don't reconnect
      const delay = Math.min(30000, 2000 * Math.pow(2, entry.attempt++));
      serverLog(`Candle Manager: tick stream for ${symbol} dropped — reconnecting in ${Math.round(delay/1000)}s`);
      setTimeout(() => {
        if (!this.symbols.has(symbol)) return;
        // Resync every active granularity via backfill so no gap corrupts the in-progress candle
        entry.grans.forEach((g, gran) => { g.backfilled = false; g.current = null; this._backfill(symbol, gran); });
        this._ensureConnected(symbol);
      }, delay);
    });
  }

  _onTick(symbol, tick){
    const entry = this.symbols.get(symbol);
    if (!entry) return;
    const price = parseFloat(tick.quote);
    const epoch = Number(tick.epoch);
    if (isNaN(price) || isNaN(epoch)) return;
    entry.tickSubs.forEach(cb => { try { cb({ epoch, quote:price }); } catch (e) { serverLog('Candle Manager tick callback error', { err:e.message }); } });
    entry.grans.forEach((g, granularity) => {
      const windowStart = Math.floor(epoch / granularity) * granularity;
      if (!g.current) { g.current = { epoch:windowStart, open:price, high:price, low:price, close:price }; return; }
      if (windowStart === g.current.epoch) {
        if (price > g.current.high) g.current.high = price;
        if (price < g.current.low)  g.current.low  = price;
        g.current.close = price;
        return;
      }
      if (windowStart > g.current.epoch) {
        const completed = g.current; // window rolled over — previous candle is done, emit once
        g.subs.forEach(cb => { try { cb(completed); } catch (e) { serverLog('Candle Manager callback error', { err:e.message }); } });
        g.current = { epoch:windowStart, open:price, high:price, low:price, close:price };
      }
      // windowStart < g.current.epoch would mean an out-of-order tick — ignore it
    });
  }

  async _backfill(symbol, granularity){
    const entry = this.symbols.get(symbol);
    const g = entry && entry.grans.get(granularity);
    if (!g || g.backfilled) return;
    g.backfilled = true; // mark up-front so concurrent subscribers don't double-backfill
    try {
      const raw = await fetchCandleHistory(symbol, granularity);
      const candles = raw.map(c => ({ epoch:Number(c.epoch), open:parseFloat(c.open), high:parseFloat(c.high), low:parseFloat(c.low), close:parseFloat(c.close) }));
      if (!candles.length) return;
      const last = candles[candles.length - 1];
      candles.slice(0, -1).forEach(c => g.subs.forEach(cb => { try { cb(c); } catch (e) {} }));
      // Seed the in-progress candle with the most recent one from history so the very
      // next live tick continues it correctly instead of starting a fresh, empty window.
      if (!g.current) g.current = last;
    } catch (e) {
      g.backfilled = false; // allow a retry on the next subscribe/reconnect
      serverLog('Candle Manager backfill failed', { symbol, granularity, err:e.message });
    }
  }

  _teardown(symbol){
    const entry = this.symbols.get(symbol);
    if (!entry) return;
    if (entry.ws) closeWs(entry.ws);
    this.symbols.delete(symbol);
  }
}

// One-time historical candle fetch used only for backfill (not for streaming).
function fetchCandleHistory(symbol, granularity){
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_DEMO);
    const t = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('Backfill timed out')); }, 10000);
    ws.on('open', () => ws.send(JSON.stringify({ ticks_history:symbol, adjust_start_time:1, count:50, end:'latest', granularity, style:'candles' })));
    ws.on('message', raw => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.msg_type === 'candles') {
        clearTimeout(t); try { ws.close(); } catch {}
        if (m.error) return reject(new Error(m.error.message));
        resolve(m.candles || []);
      }
    });
    ws.on('error', e => { clearTimeout(t); reject(e); });
  });
}

const candleManager = new CandleManager();

// ─── DataEngine wiring (added) ─────────────────────────────────────────────────
// Uses candleManager.subscribeTicks() (already ref-counted/reconnect-safe,
// defined above) instead of opening any new WebSocket, so live tick
// collection for backtesting never adds a duplicate connection beyond
// what the bot already maintains for trading.
const dataDb = new DataEngineDB(DATAENGINE_DB_PATH);
// One-time self-heal, see rebuildAllCandles() — runs once at boot, before the
// server starts accepting traffic, so any data downloaded before the
// backward-pagination candle-corruption fix gets corrected automatically.
try {
  const rebuilt = dataDb.rebuildAllCandles({ info: serverLog, warn: serverLog, error: serverLog });
  if (rebuilt > 0) serverLog(`[startup] Candle self-heal: rebuilt ${rebuilt} candles across all symbols`);
} catch (e) {
  serverLog('[startup] Candle self-heal failed (non-fatal)', { err: e.message });
}
// FIX: historyDownloader and liveTickCollector used to share ONE CandleBuilder
// instance. CandleBuilder keeps its "candle currently being built" state in
// memory keyed only by symbol — it has no idea two different processes are
// feeding it. Live collection streams ticks in real, increasing time order
// for "now"; a historical download walks backward through old timestamps for
// the same symbol. Running both at once (completely normal — you'd usually
// leave live collection on for whatever you're actively trading while also
// backfilling more history for it) meant every tick from one process looked
// "out of order" to whichever candle the other process had just been
// building, silently corrupting BOTH streams' in-progress candles for the
// duration of the overlap. Downstream rebuild-from-ticks fixes (backtest,
// search, startup) mask this for anything that reads through them, but nothing
// should have to rely on that as the only safety net — giving each process
// its own CandleBuilder means their in-memory state can never collide,
// regardless of what future code reads the candles table.
const downloadCandleBuilder = new CandleBuilder(dataDb, DATAENGINE_CANDLE_INTERVAL_SECONDS, { info: serverLog, warn: serverLog, error: serverLog });
const liveCandleBuilder     = new CandleBuilder(dataDb, DATAENGINE_CANDLE_INTERVAL_SECONDS, { info: serverLog, warn: serverLog, error: serverLog });
const historyDownloader = new HistoryDownloader(dataDb, downloadCandleBuilder, WS_DEMO, { info: serverLog, warn: serverLog, error: serverLog });
const liveTickCollector = new LiveTickCollector(dataDb, liveCandleBuilder, candleManager, { info: serverLog, warn: serverLog, error: serverLog });
liveTickCollector.start(DATAENGINE_MARKETS);
serverLog('DataEngine ready', { dbPath: DATAENGINE_DB_PATH, markets: DATAENGINE_MARKETS });

// ─── contract builder ─────────────────────────────────────────────────────────
const CT_MAP = { CALL:'CALL', PUT:'PUT', RISE:'CALL', FALL:'PUT', HIGHER:'CALL', LOWER:'PUT', ONETOUCH:'ONETOUCH', NOTOUCH:'NOTOUCH', VANILLA_CALL:'VANILLALONGCALL', VANILLA_PUT:'VANILLALONGPUT' };
const NEEDS_BARRIER = new Set(['HIGHER','LOWER','ONETOUCH','NOTOUCH','VANILLA_CALL','VANILLA_PUT']);
function buildParams(symbol, type, barrier, durValue, durUnit, stake){
  const ct = CT_MAP[type] || type;
  // FIX: was hardcoded to 'USD' regardless of which account is actually
  // connected — if a selected live account isn't USD-denominated, proposals
  // built with the wrong currency get rejected/mispriced. Now uses the real
  // currency reported by the connected account, falling back to USD only
  // if we haven't heard a currency back yet (e.g. very first connect tick).
  const p = { amount:Number(stake), basis:'stake', contract_type:ct, currency:(acct.currency || 'USD'), duration:Number(durValue), duration_unit:durUnit, symbol };
  if (NEEDS_BARRIER.has(type) && barrier != null && String(barrier).trim() !== '') p.barrier = String(barrier).trim();
  return p;
}

// ─── Demo Mode — simulated trades on live price data ─────────────────────────
// When a slot's cfg.demo_mode is on, no real order is ever sent to Deriv. Instead
// this watches live ticks (via the same shared Candle Manager connection used for
// candles — no extra subscription) starting from the moment the trade would have
// opened, tracks whether/when the barrier is touched or where price lands at
// expiry, and settles exactly like the real contract type would. A real payout
// quote is pulled from Deriv's proposal endpoint (pricing only, never executes a
// trade) so the simulated win amount reflects real market odds, not a guess.
function resolveBarrier(entrySpot, barrierStr){
  if (barrierStr == null) return null;
  const s = String(barrierStr).trim();
  if (s === '') return null;
  if (s[0] === '+' || s[0] === '-') return parseFloat((entrySpot + parseFloat(s)).toFixed(5));
  return parseFloat(s);
}

async function estimatePayout(params){
  try {
    const r = await acct.send({
      proposal:1, amount:params.amount, basis:params.basis, contract_type:params.contract_type,
      currency:params.currency, duration:params.duration, duration_unit:params.duration_unit,
      symbol:params.symbol, ...(params.barrier != null ? { barrier:params.barrier } : {}),
    });
    if (r.error) throw new Error(r.error.message);
    const payout = parseFloat(r.proposal?.payout);
    if (!isNaN(payout) && payout > 0) return payout;
    throw new Error('No payout in proposal response');
  } catch (e) {
    serverLog('Demo mode: live payout quote failed, using estimate', { err:e.message });
    return parseFloat((params.amount * 1.85).toFixed(2)); // rough touch-contract fallback (~85% return)
  }
}

function simulateContract(slot, params){
  return new Promise((resolve) => {
    const type = params.contract_type;
    const isTouch       = type === 'ONETOUCH' || type === 'NOTOUCH';
    const isDirectional = type === 'CALL' || type === 'PUT';
    const isVanilla      = type === 'VANILLALONGCALL' || type === 'VANILLALONGPUT';

    let entrySpot = null, maxSpot = null, minSpot = null, ticksSeen = 0, barrierAbs = null, expireAt = null, done = false;
    let unsub = () => {};

    const finish = async (won, exitSpot) => {
      if (done) return; done = true;
      clearTimeout(safetyTimer);
      unsub();
      let profit;
      if (won) {
        const manualWin = parseFloat(slot.cfg.demo_win_amount);
        if (!isNaN(manualWin) && manualWin > 0) {
          profit = parseFloat(manualWin.toFixed(2)); // manual override — no live quote needed
        } else {
          const payout = await estimatePayout(params);
          profit = parseFloat((payout - params.amount).toFixed(2));
        }
      } else {
        profit = -params.amount; // loss always costs the full stake
      }
      resolve({ profit, _entrySpot:entrySpot, exit_spot:exitSpot, _barrier:barrierAbs, _maxSpot:maxSpot, _minSpot:minSpot });
    };

    // Hard safety cap so a simulated trade can never hang forever if ticks stop arriving
    const safetyTimer = setTimeout(() => finish(false, null), 6 * 60 * 1000);

    unsub = candleManager.subscribeTicks(params.symbol, tick => {
      const price = tick.quote;
      if (entrySpot === null) {
        entrySpot = price; maxSpot = price; minSpot = price;
        barrierAbs = resolveBarrier(entrySpot, params.barrier);
        if (params.duration_unit === 's') expireAt = Date.now() + params.duration * 1000;
        if (params.duration_unit === 'm') expireAt = Date.now() + params.duration * 60000;
        return; // first tick only establishes entry, matching how a real contract starts at the next quote
      }
      ticksSeen++;
      if (price > maxSpot) maxSpot = price;
      if (price < minSpot) minSpot = price;

      if (isTouch && barrierAbs != null) {
        const touched = barrierAbs >= entrySpot ? price >= barrierAbs : price <= barrierAbs;
        if (touched) { finish(type === 'ONETOUCH', price); return; }
      }

      const expired = params.duration_unit === 't' ? ticksSeen >= params.duration : Date.now() >= expireAt;
      if (!expired) return;

      if (isTouch) { finish(type === 'NOTOUCH', price); return; } // expired without ever touching
      if (isDirectional) { finish(type === 'CALL' ? price > entrySpot : price < entrySpot, price); return; }
      if (isVanilla) { finish(type === 'VANILLALONGCALL' ? price > barrierAbs : price < barrierAbs, price); return; }
      finish(false, price); // unrecognized type — settle as a loss rather than hang
    });
  });
}

// ─── Backtest contract resolution ──────────────────────────────────────────────
// Mirrors simulateContract()'s exact decision logic (touch detection, expiry,
// outcome determination) but operates on a historical tick ARRAY with real
// epochs instead of a live subscription + Date.now(). Kept as a fully separate
// function rather than modifying simulateContract() itself, since that function
// is live/demo production code and must not change. Both are tested to agree
// on identical scenarios (see accompanying test suite).
function findTickIndexAtOrAfter(ticks, targetEpoch, fromIdx = 0) {
  for (let i = fromIdx; i < ticks.length; i++) if (ticks[i].epoch >= targetEpoch) return i;
  return -1;
}

function resolveContractFromHistory(ticks, startIndex, params) {
  if (startIndex >= ticks.length) return null;
  const type = params.contract_type;
  const isTouch       = type === 'ONETOUCH' || type === 'NOTOUCH';
  const isDirectional = type === 'CALL' || type === 'PUT';
  const isVanilla      = type === 'VANILLALONGCALL' || type === 'VANILLALONGPUT';

  const entryTick = ticks[startIndex];
  const entrySpot = entryTick.quote;
  const barrierAbs = resolveBarrier(entrySpot, params.barrier);
  let maxSpot = entrySpot, minSpot = entrySpot;

  let expireEpoch = null;
  // FIX: only 's' and 'm' were handled — any other non-'t' unit left
  // expireEpoch as null, and `tick.epoch >= null` coerces null to 0 in JS,
  // which is always true for a real epoch. That silently resolved the
  // contract after just ONE tick instead of erroring or using the right
  // duration. The UI currently only offers t/s/m, so this was latent, not
  // reachable — hardened anyway so it fails loudly instead of silently
  // mis-pricing a trade if that ever changes.
  const DURATION_UNIT_SECONDS = { s: 1, m: 60, h: 3600, d: 86400 };
  if (params.duration_unit !== 't') {
    const mult = DURATION_UNIT_SECONDS[params.duration_unit];
    if (!mult) throw new Error(`resolveContractFromHistory: unsupported duration_unit "${params.duration_unit}"`);
    expireEpoch = entryTick.epoch + params.duration * mult;
  }

  let ticksSeen = 0;
  for (let i = startIndex + 1; i < ticks.length; i++) {
    const tick = ticks[i]; const price = tick.quote; ticksSeen++;
    if (price > maxSpot) maxSpot = price;
    if (price < minSpot) minSpot = price;

    if (isTouch && barrierAbs != null) {
      const touched = barrierAbs >= entrySpot ? price >= barrierAbs : price <= barrierAbs;
      if (touched) return finish(type === 'ONETOUCH', price, i);
    }
    const expired = params.duration_unit === 't' ? ticksSeen >= params.duration : tick.epoch >= expireEpoch;
    if (!expired) continue;

    if (isTouch) return finish(type === 'NOTOUCH', price, i);
    if (isDirectional) return finish(type === 'CALL' ? price > entrySpot : price < entrySpot, price, i);
    if (isVanilla) return finish(type === 'VANILLALONGCALL' ? price > barrierAbs : price < barrierAbs, price, i);
    return finish(false, price, i);
  }
  return null; // ran out of historical data before the contract could resolve

  function finish(won, exitSpot, exitIndex) {
    return { won, entrySpot, exitSpot, barrierAbs, maxSpot, minSpot, entryEpoch: entryTick.epoch, exitEpoch: ticks[exitIndex].epoch, exitIndex };
  }
}

/** Adapts resolveContractFromHistory()'s output into the exact `settled` shape
 *  runContract()'s shared bookkeeping expects (same fields simulateContract()
 *  produces), computing profit via the manual demo_win_amount override or the
 *  same 1.85x fallback estimatePayout() uses when no live quote is available
 *  (backtest can never call the live API — no connection required, per spec). */
function runContractBacktestResolve(slot, params) {
  const runner = slot.backtestRunner;
  const result = resolveContractFromHistory(runner.allTicks, runner.currentTickIndex, params);
  if (!result) return null;
  let profit;
  if (result.won) {
    const manualWin = parseFloat(slot.cfg.demo_win_amount);
    profit = (!isNaN(manualWin) && manualWin > 0) ? parseFloat(manualWin.toFixed(2)) : parseFloat((params.amount * 1.85 - params.amount).toFixed(2));
  } else {
    profit = -params.amount;
  }
  return { profit, _entrySpot: result.entrySpot, exit_spot: result.exitSpot, _barrier: result.barrierAbs, _maxSpot: result.maxSpot, _minSpot: result.minSpot, _exitEpoch: result.exitEpoch };
}


// ─── trade execution ──────────────────────────────────────────────────────────
async function runContract(slot, params, label, opts = {}){
  const backtest = !!slot.backtest;
  if (!backtest && !acct.ready) throw new Error('Account not connected');
  const demo = !!slot.cfg.demo_mode;
  const tag = backtest ? '[BACKTEST] ' : (demo ? '[DEMO] ' : '');
  slot.emit(`${tag}Opening ${label} | ${params.contract_type}${params.barrier ? ' @'+params.barrier : ''} | ${params.duration}${params.duration_unit} | $${params.amount}`);
  let cid, settled;
  if (backtest) {
    cid = `bt-${slot.backtestRunner.id}-${slot.backtestRunner.tradeCounter++}`;
    settled = runContractBacktestResolve(slot, params);
    if (!settled) {
      // Ran out of historical data before this contract could resolve
      // (e.g. it was opened too close to the end of the downloaded range).
      // Rather than returning null — which would break every existing
      // caller that does runContract(...).then(rec => rec.won), since
      // live/demo runContract() NEVER returns null — treat it as a
      // conservative loss, clearly flagged, so results stay honest
      // without touching any calling code's assumptions.
      slot.emit(`${tag}Insufficient historical data to resolve ${label} — counted as a loss`, 'err');
      settled = { profit: -params.amount, _entrySpot:null, exit_spot:null, _barrier:null, _maxSpot:null, _minSpot:null, _exitEpoch:null, _insufficientData:true };
    }
  } else if (demo) {
    cid = `demo-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
    settled = await simulateContract(slot, params);
  } else {
    const res = await acct.buy(params);
    if (res.error) throw new Error(res.error.message);
    cid = res.buy.contract_id;
    settled = await acct.watchContract(cid);
  }
  const profit = parseFloat(settled.profit || 0);
  const won = profit > 0;
  slot.stats.trades++;
  slot.stats[won ? 'wins' : 'losses']++;
  slot.stats.profit  = parseFloat((slot.stats.profit + profit).toFixed(2));
  slot.sessionProfit = parseFloat((slot.sessionProfit + profit).toFixed(2));
  slot.emit(`${tag}${won ? '✓ WIN' : '✗ LOSS'} ${label} $${Math.abs(profit).toFixed(2)} | session: ${slot.sessionProfit >= 0 ? '+' : ''}$${slot.sessionProfit}`, won ? 'win' : 'loss');
  const rec = {
    time: backtest && settled._exitEpoch ? new Date(settled._exitEpoch*1000).toISOString() : new Date().toISOString(),
    contract_id:cid, symbol:params.symbol, type:params.contract_type,
    barrier:params.barrier ?? null, stake:params.amount, profit, won, role:opts.role || 'primary', simulated:demo,
    entrySpot:num(settled._entrySpot), exitSpot:num(settled.exit_spot ?? settled.sell_spot),
    barrierAbs:num(settled._barrier), maxSpot:num(settled._maxSpot), minSpot:num(settled._minSpot),
    ...(backtest ? { insufficientData: !!settled._insufficientData } : {}),
  };
  slot.history.unshift(rec);
  if (slot.history.length > 200) slot.history.pop();
  slot.pushState();
  push({ type:'trade', slotId:slot.id, data:rec });
  persistSoon();
  return rec;
}

// ─── TP / SL ─────────────────────────────────────────────────────────────────
function checkTPSL(slot){
  const { take_profit, stop_loss } = slot.cfg;
  const p = slot.sessionProfit;
  if (take_profit > 0 && p >= take_profit)            { slot.emit(`🎯 Take profit $${take_profit} reached — stopping`, 'win');  stopSlot(slot); return true; }
  if (stop_loss   > 0 && p <= -Math.abs(stop_loss))   { slot.emit(`🛑 Stop loss $${stop_loss} reached — stopping`, 'loss');     stopSlot(slot); return true; }
  return false;
}

// ─── filters ─────────────────────────────────────────────────────────────────
function passMomentum(slot){
  const { cfg, buf } = slot;
  const n = Math.min(cfg.momentum_candles, buf.length);
  if (n < 2) return true;
  if (cfg.observe_mode === 'ticks') {
    const r = buf.slice(-n).map(Number);
    const up = r.every((p,i)=>i===0||p>=r[i-1]), dn = r.every((p,i)=>i===0||p<=r[i-1]);
    if (!up && !dn) { slot.emit('Momentum: mixed direction — skip','err'); return false; }
    if (slot.zoneHigh && slot.zoneLow) {
      const pos = (r[r.length-1] - slot.zoneLow) / ((slot.zoneHigh - slot.zoneLow) || 1);
      if (pos > 0.35 && pos < 0.65) { slot.emit('Momentum: price dead-center — skip','err'); return false; }
    }
  } else {
    const r = buf.slice(-n);
    const bull = r.every(c=>parseFloat(c.close)>=parseFloat(c.open)), bear = r.every(c=>parseFloat(c.close)<=parseFloat(c.open));
    if (!bull && !bear) { slot.emit('Momentum: mixed candles — skip','err'); return false; }
    if (cfg.momentum_body_mult > 0 && buf.length >= 3) {
      const slice = buf.slice(-10), avgBody = slice.reduce((s,c)=>s+Math.abs(parseFloat(c.close)-parseFloat(c.open)),0)/slice.length;
      const last = r[r.length-1], body = Math.abs(parseFloat(last.close)-parseFloat(last.open));
      if (body < avgBody * cfg.momentum_body_mult) { slot.emit('Momentum: breakout candle too weak — skip','err'); return false; }
    }
  }
  return true;
}

function passTrend(slot){
  const { cfg, trendBuf } = slot;
  const n = Math.min(cfg.trend_candles, trendBuf.length);
  if (n < 3) return true;
  const candles = trendBuf.slice(-n);
  const avgBody = candles.reduce((s,c)=>s+Math.abs(parseFloat(c.close)-parseFloat(c.open)),0)/n;
  if (avgBody < cfg.min_body_size) { slot.emit('Trend: body too small — choppy — skip','err'); return false; }
  let ov = 0;
  for (let i=1;i<candles.length;i++) {
    const a=candles[i-1], b=candles[i];
    const oH=Math.min(parseFloat(a.high),parseFloat(b.high)), oL=Math.max(parseFloat(a.low),parseFloat(b.low));
    const overlap=Math.max(0,oH-oL), range=Math.max(parseFloat(a.high)-parseFloat(a.low),parseFloat(b.high)-parseFloat(b.low))||1;
    ov += overlap/range;
  }
  if (ov/(candles.length-1) > cfg.max_overlap) { slot.emit('Trend: overlap too high — ranging — skip','err'); return false; }
  let maxRun=1, run=1;
  for (let i=1;i<candles.length;i++) {
    const pb=parseFloat(candles[i-1].close)>=parseFloat(candles[i-1].open), cb=parseFloat(candles[i].close)>=parseFloat(candles[i].open);
    if (pb===cb){run++;maxRun=Math.max(maxRun,run);} else run=1;
  }
  if (maxRun < cfg.min_dir_candles) { slot.emit('Trend: no trend — skip','err'); return false; }
  return true;
}

function passFilter(slot){
  if (slot.cfg.filter === 'momentum') return passMomentum(slot);
  if (slot.cfg.filter === 'trend')    return passTrend(slot);
  return true;
}

// ─── slot defaults ────────────────────────────────────────────────────────────
function defaultCfg(){
  return {
    symbol:'1HZ100V', logic:'golden', observe_mode:'ticks', observe_count:5, reaction_range:0.3,
    confirm_count:1, stake:1, duration_value:5, duration_unit:'t',
    barrier1:'+0.25', barrier2:'-0.25', contract_type:'ONETOUCH',
    rest_seconds:30, skip_rest_on_win:false, take_profit:0, stop_loss:0,
    // Demo Mode — when true, trades are simulated against live prices, no real
    // orders are ever sent. Defaults to on so a slot never risks real money
    // until this is explicitly switched off.
    demo_mode:true,
    // Manual override for Demo Mode's simulated win profit ($). When > 0, every
    // simulated win uses this exact dollar amount instead of a live Deriv quote.
    // 0/empty falls back to the live proposal-based estimate. Losses always cost
    // the full stake either way. Has zero effect on Real Mode — that path never
    // reads this field.
    demo_win_amount:0,
    // Golden Logic — one attached contract per reversal side (independent params)
    combo_high_on:false, combo_high_type:'FALL', combo_high_barrier:'+2', combo_high_dv:5, combo_high_du:'t', combo_high_stake:1,
    combo_low_on:false,  combo_low_type:'RISE', combo_low_barrier:'-2', combo_low_dv:5, combo_low_du:'t', combo_low_stake:1,
    filter:'none', momentum_candles:3, momentum_body_mult:1.0,
    trend_candles:5, min_body_size:0.05, max_overlap:0.5, min_dir_candles:3,
    // Golden Logic — optional M1/M5/M15 trend confirmation (same engine as Super
    // Logic's). HIGH-zone reversal needs a downtrend confirmed (rejection at the
    // top); LOW-zone reversal needs an uptrend confirmed (rejection at the bottom).
    // Off by default — existing Golden Logic behavior is unchanged unless enabled.
    golden_trend_enabled:false, golden_trend_mode:'m1_5_15', golden_trend_min_match:2,
    golden_trend_lookback_m1:10, golden_trend_lookback_m5:8, golden_trend_lookback_m15:6,
    // Trend STRENGTH threshold — how lopsided the green/red candle mix needs to be
    // to call a trend at all (separate from golden_trend_mode above, which is about
    // WHICH timeframes must agree, not how strict each one's own call is).
    // 'manual'    → always use golden_trend_threshold_pct.
    // 'automatic' → self-adjusting between golden_trend_auto_min_pct and
    //               golden_trend_auto_max_pct: every loss steps it up by 10
    //               (stricter), every golden_trend_auto_win_streak consecutive
    //               wins steps it back down by 10 (looser). Never leaves the
    //               min/max range. Resets to the minimum whenever the slot starts.
    golden_trend_strictness_mode:'manual', golden_trend_threshold_pct:60,
    golden_trend_auto_min_pct:60, golden_trend_auto_max_pct:80, golden_trend_auto_win_streak:1,
    // Win-streak easing (the only easing mechanism now — price-drift easing
    // was tried and removed per user testing) — on by default, tightening on
    // a loss always happens regardless of this; only easing is optional.
    golden_trend_auto_win_ease_enabled:true,
    // ─── Real Trend Pause (structure-based, added) ─────────────────────────
    // Separate from the color-based trend confirmation above entirely. After
    // N consecutive losses (configurable), checks actual market STRUCTURE —
    // swing highs/lows, not candle color — on the configured timeframe(s).
    // If price is ranging (no confirmed higher-highs/higher-lows or
    // lower-highs/lower-lows), the bot pauses taking new setups until a real
    // trend (either direction) reappears. See detectSwingTrend().
    golden_structure_pause_enabled:false,
    golden_structure_pause_loss_threshold:1,     // how many consecutive losses trigger a check
    golden_structure_pause_timeframes:'m1',      // 'm1' | 'm5' | 'm1_and_m5' — which must show a real trend
    golden_structure_swing_strength:2,           // candles required on each side to confirm a swing point
    golden_structure_swing_confirm_count:3,      // consecutive swing highs/lows required to confirm a trend
    // Golden Logic consolidation filter — same mechanism as Super Logic's. Off by default.
    golden_consolidation_filter_enabled:false, golden_consolidation_lookback:10, golden_consolidation_min_range:2.5,
    // Super Logic settings — all configurable, nothing hardcoded
    super_same_color_count:3, super_opposite_count:2,
    super_trend_enabled:true, super_trend_mode:'m1_5_15', super_trend_min_match:2,
    // How many recent candles each timeframe's trend detector looks at. Lower = more
    // reactive to recent moves, higher = smoother but slower to recognize a new trend.
    super_trend_lookback_m1:10, super_trend_lookback_m5:8, super_trend_lookback_m15:6,
    // Same manual/automatic trend-strength threshold as Golden Logic above, kept as
    // a fully separate setting per user request (Golden and Super each get their own).
    super_trend_strictness_mode:'manual', super_trend_threshold_pct:60,
    super_trend_auto_min_pct:60, super_trend_auto_max_pct:80, super_trend_auto_win_streak:1,
    // Win-streak easing only now (price-drift easing was tried and removed).
    super_trend_auto_win_ease_enabled:true,
    // Real Trend Pause — same mechanism as Golden Logic's, fully separate setting.
    super_structure_pause_enabled:false,
    super_structure_pause_loss_threshold:1,
    super_structure_pause_timeframes:'m1',
    super_structure_swing_strength:2,
    super_structure_swing_confirm_count:3,
    // Consolidation filter — blocks trading in tight/choppy ranges. Off by default.
    super_consolidation_filter_enabled:false, super_consolidation_lookback:10, super_consolidation_min_range:2.5,
    super_cooldown_seconds:30,
  };
}

// ─── slot ─────────────────────────────────────────────────────────────────────
class Slot {
  constructor(idx){
    this.idx=idx; this.id=`slot${idx}`; this.cfg=defaultCfg();
    this.running=false; this.busy=false;
    this.logs=[]; this.stats={trades:0,wins:0,losses:0,profit:0}; this.history=[];
    this.sessionProfit=0; this.liveCount=0;
    this.buf=[]; this.trendBuf=[]; this.dataWs=null;
    // Unsubscribe functions returned by candleManager.subscribe() — replaces the old
    // per-slot raw candle/trend WebSockets, since candles now come from the shared
    // Candle Manager instead of being opened directly by each slot.
    this.candleSubs=[];
    this.phase='warmup'; this.zoneHigh=null; this.zoneLow=null;
    this.confSide=null; this.confBuf=[];
    // Super Logic state
    this.superM5Buf=[]; this.superM15Buf=[];
    this.superState='scanning';
    this.superRun=[];
    this.superRunColor=null;
    this.superTriggerBuf=[];
    // Automatic trend-strictness state (separate per logic). Current active
    // threshold level and consecutive-win counter — see updateAutoTrendLevel().
    // Starts at null so getEffectiveTrendThreshold() initializes it to the
    // configured minimum the first time it's read.
    this.goldenTrendAutoLevel=null; this.goldenTrendAutoWinStreak=0;
    this.superTrendAutoLevel=null;  this.superTrendAutoWinStreak=0;
    // Real Trend Pause runtime state — separate counters/flags per logic.
    this.goldenStructureLossStreak=0; this.goldenStructurePaused=false;
    this.superStructureLossStreak=0;  this.superStructurePaused=false;
  }
  emit(msg, level='info'){
    const e={ time:new Date().toISOString(), msg, level };
    this.logs.push(e); if (this.logs.length>400) this.logs.shift();
    push({ type:'log', slotId:this.id, entry:e });
  }
  pushState(){
    push({ type:'state', slotId:this.id, data:{ running:this.running, busy:this.busy, stats:this.stats, phase:this.phase, liveCount:this.liveCount, sessionProfit:this.sessionProfit, superState:this.superState, superTriggerCount:this.superTriggerBuf.length, superOppositeCount:this.cfg.super_opposite_count } });
  }
  clearLog(){ this.logs=[]; push({ type:'log_cleared', slotId:this.id }); }
  clearStats(){ this.stats={trades:0,wins:0,losses:0,profit:0}; this.sessionProfit=0; this.history=[]; push({ type:'stats_cleared', slotId:this.id }); persistSoon(); }
  softReset(){
    this.phase='warmup'; this.zoneHigh=null; this.zoneLow=null; this.confSide=null; this.confBuf=[]; this.buf=[]; this.liveCount=0;
    this.superState='scanning'; this.superRun=[]; this.superRunColor=null; this.superTriggerBuf=[];
  }
  snap(){ return { id:this.id, idx:this.idx, running:this.running, busy:this.busy, stats:this.stats, cfg:this.cfg, phase:this.phase, liveCount:this.liveCount, sessionProfit:this.sessionProfit, superState:this.superState, superTriggerCount:this.superTriggerBuf.length, superOppositeCount:this.cfg.super_opposite_count }; }
}

const slots  = Array.from({length:10},(_,i)=>new Slot(i));
const slotOf = new Map(slots.map(s=>[s.id,s]));

// ─── Backtest engine ────────────────────────────────────────────────────────────
// Replays historical ticks/candles (from the DataEngine's SQLite store) through
// the EXACT SAME dispatch()/evalGolden_watch()/superOnCandle() functions used by
// live and demo trading. The only things that differ are: (1) where ticks come
// from, and (2) how a triggered contract's outcome gets resolved (see
// runContractBacktestResolve() and the `slot.backtest` branch inside
// runContract() above) — no strategy/entry/exit decision code is duplicated
// or altered for this to work, per the "same strategy engine" requirement.
function sleepMs(ms) { return new Promise(r => setTimeout(r, ms)); }

class BacktestRunner {
  constructor(id, baseCfg, symbol, fromEpoch, toEpoch, speed) {
    this.id = id;
    this.status = 'running'; // running|paused|completed|error|cancelled
    this.speed = speed; // number (multiplier of realtime) or 'max'
    this._paused = false;
    this._cancelled = false;
    this.tradeCounter = 1;
    this.allTicks = [];
    this.currentTickIndex = 0;
    this.processedTicks = 0;
    this.totalTicks = 0;
    this.currentEpoch = null;
    this.fromEpoch = fromEpoch;
    this.toEpoch = toEpoch;
    this.symbol = symbol;
    this.error = null;
    this.note = null;
    this.startedAt = Date.now();

    // Shadow slot — a REAL Slot instance (idx -1, never added to the global
    // `slots` array, so it's invisible to /api/slots, persistSoon(), and the
    // live dashboard's slot list) so every strategy function runs completely
    // unmodified. Only emit()/pushState() are overridden on THIS INSTANCE to
    // avoid flooding the SSE stream / log buffer during a fast replay — the
    // Slot class itself is untouched.
    this.slot = new Slot(-1);
    this.slot.id = `bt-${id}`;
    this.slot.cfg = { ...baseCfg, symbol };
    this.slot.running = true;
    this.slot.backtest = true;
    this.slot.backtestRunner = this;
    this.slot.emit = function(msg, level='info'){
      const e = { time:new Date().toISOString(), msg, level };
      this.logs.push(e); if (this.logs.length > 500) this.logs.shift();
    };
    this.slot.pushState = function(){};
    // "Rest"/"cooldown" periods between trades (afterTrade/superAfterTrade)
    // normally use a real setTimeout — fine for live trading, but would make
    // a backtest take as long in wall-clock time as the real rest period
    // per trade. Instead, those functions schedule resets HERE using
    // simulated time (historical tick epochs), and the replay loop below
    // fires them as it advances — so a backtest runs at its chosen replay
    // speed regardless of configured rest/cooldown seconds.
    this.pendingResets = [];
  }

  scheduleReset(resumeAtEpoch, fn) {
    this.pendingResets.push({ resumeAtEpoch, fn });
  }

  _firePendingResets(uptoEpoch) {
    if (!this.pendingResets.length) return;
    this.pendingResets = this.pendingResets.filter(r => {
      if (r.resumeAtEpoch <= uptoEpoch) { r.fn(); return false; }
      return true;
    });
  }

  publicState() {
    const totalSeconds = Math.max(1, this.toEpoch - this.fromEpoch);
    const coveredSeconds = this.currentEpoch != null ? Math.max(0, this.currentEpoch - this.fromEpoch) : 0;
    const percent = this.status === 'completed' ? 100 : Math.min(100, (coveredSeconds / totalSeconds) * 100);
    return {
      id: this.id, status: this.status, symbol: this.symbol,
      fromPeriod: new Date(this.fromEpoch * 1000).toISOString(), toPeriod: new Date(this.toEpoch * 1000).toISOString(),
      currentPeriod: this.currentEpoch ? new Date(this.currentEpoch * 1000).toISOString() : null,
      percent: +percent.toFixed(2), processedTicks: this.processedTicks, totalTicks: this.totalTicks,
      stats: this.slot.stats, history: this.slot.history.slice(0, 100),
      error: this.error, note: this.note,
    };
  }

  pause() { if (this.status === 'running') { this._paused = true; this.status = 'paused'; } }
  resume() { if (this.status === 'paused') { this._paused = false; this.status = 'running'; } }
  cancel() { this._cancelled = true; }

  async run() {
    try {
      this.allTicks = dataDb.getTicksBetween(this.symbol, this.fromEpoch, this.toEpoch);
      this.totalTicks = this.allTicks.length;
      if (this.totalTicks === 0) {
        this.status = 'error';
        this.error = 'No tick data available in this range — download history for this market/period first.';
        return;
      }
      // Transparency: silently using less data than requested (because the
      // download doesn't reach as far as `toEpoch`) would otherwise look
      // identical to a full, complete run — surface it instead of hiding it.
      const lastAvailableEpoch = this.allTicks[this.allTicks.length - 1].epoch;
      if (this.toEpoch - lastAvailableEpoch > 120) {
        this.note = `Requested up to ${new Date(this.toEpoch * 1000).toISOString()}, but downloaded data only reaches ` +
          `${new Date(lastAvailableEpoch * 1000).toISOString()} — the run stops there instead of the full requested range.`;
      }

      const cfg = this.slot.cfg;
      // FIX: this used to only fetch candles when `observe_mode==='candles'`
      // or `logic==='super'` — the same array driving dispatch() directly.
      // But live trading ALSO runs a second, independent M1/M5/M15 subscription
      // whose ONLY job is to fill slot.trendBuf/superM5Buf/superM15Buf for
      // detectTrend() — completely separate from whatever drives the strategy
      // state machine. That second subscription was never simulated here, so
      // trendBuf/superM5Buf/superM15Buf stayed empty for the whole backtest.
      // Since detectTrend() on an empty buffer always returns 'neutral', and
      // super_trend_enabled defaults to true, EVERY Super Logic setup failed
      // trend confirmation, silently, on every single candle — zero trades,
      // no matter how much history you fed it. Same for Golden Logic whenever
      // golden_trend_enabled/golden_consolidation_filter_enabled/filter==='trend'
      // is on. This is now split into two independent things, exactly mirroring
      // startWatcher(): what DRIVES the strategy (mainDriveIsCandles) vs what
      // FEEDS the trend buffers (needsTrendBuffers) — a slot can need one, the
      // other, both, or neither.
      const mainDriveIsCandles = cfg.observe_mode === 'candles' || cfg.logic === 'super';
      // Real Trend Pause needs M1 and/or M5 depending on its own timeframe
      // setting — folded into the same buffer-feed conditions below so it
      // gets the data it needs regardless of whether color-trend/consolidation
      // features are even on.
      const structureLogic = cfg.logic === 'super' ? 'super' : 'golden';
      const structurePauseOn = !!cfg[`${structureLogic}_structure_pause_enabled`];
      const structureTf = cfg[`${structureLogic}_structure_pause_timeframes`] || 'm1';
      const structureWantsM1 = structurePauseOn && structureTf !== 'm5';
      const structureWantsM5 = structurePauseOn && structureTf !== 'm1';
      const needsTrendBuffers  = cfg.logic === 'super' || cfg.golden_trend_enabled || cfg.golden_consolidation_filter_enabled || cfg.filter === 'trend' || structurePauseOn;
      const needsCandleQueue   = mainDriveIsCandles || needsTrendBuffers;

      // FIX: rebuild M1 candles fresh from the raw ticks we just confirmed
      // exist, before reading them — see rebuildCandlesForRange() for why
      // the candles table can't be trusted as-is for anything downloaded
      // before this fix. Ticks themselves were never affected, so this is
      // a full, correct recovery with no re-download needed.
      if (needsCandleQueue) dataDb.rebuildCandlesForRange(this.symbol, this.fromEpoch, this.toEpoch);

      const m1Queue  = needsCandleQueue ? dataDb.getCandlesBetween(this.symbol, this.fromEpoch, this.toEpoch) : [];
      // M5/M15 aren't stored — derive them from the same M1 series we just fetched.
      const m5Queue  = needsTrendBuffers ? aggregateCandles(m1Queue, 300) : [];
      const wantsM15 = cfg.logic === 'super'
        ? (cfg.super_trend_mode  !== 'm1_5' && cfg.super_trend_mode  !== 'm1_and_5')
        : (cfg.golden_trend_enabled && cfg.golden_trend_mode !== 'm1_5' && cfg.golden_trend_mode !== 'm1_and_5');
      const m15Queue = (needsTrendBuffers && wantsM15) ? aggregateCandles(m1Queue, 900) : [];

      // Golden Logic only feeds trendBuf (M1) when filter/trend/consolidation/
      // structure-pause actually needs it (see startWatcher) — Super Logic
      // always feeds it.
      const feedsM1Trend = cfg.logic === 'super' || cfg.filter === 'trend' || cfg.golden_trend_enabled || cfg.golden_consolidation_filter_enabled || structureWantsM1;
      // M5/M15 buffers: Super Logic always feeds M5 (M15 conditionally); Golden
      // Logic feeds either when golden_trend_enabled OR structure-pause needs M5.
      const feedsM5M15 = cfg.logic === 'super' || cfg.golden_trend_enabled || structureWantsM5;

      let m1Idx = 0, m5Idx = 0, m15Idx = 0;

      for (let i = 0; i < this.allTicks.length; i++) {
        if (this._cancelled) { this.status = 'cancelled'; return; }
        while (this._paused && !this._cancelled) await sleepMs(50);
        if (this._cancelled) { this.status = 'cancelled'; return; }

        this.currentTickIndex = i;
        const tick = this.allTicks[i];
        this.currentEpoch = tick.epoch;
        this._firePendingResets(tick.epoch);

        if (needsCandleQueue) {
          // Feed every candle that has fully closed by this tick's epoch —
          // mirrors live candle-mode dispatch, which only ever sees closed
          // candles (candleManager.subscribe fires on close, never mid-candle).
          while (m1Idx < m1Queue.length && m1Queue[m1Idx].epoch + 60 <= tick.epoch) {
            const candle = m1Queue[m1Idx];
            if (feedsM1Trend) pushM1TrendCandle(this.slot, structureLogic, candle);
            if (mainDriveIsCandles) dispatch(this.slot, candle);
            m1Idx++;
          }
          if (feedsM5M15) {
            while (m5Idx < m5Queue.length && m5Queue[m5Idx].epoch + 300 <= tick.epoch) {
              pushM5TrendCandle(this.slot, m5Queue[m5Idx]);
              m5Idx++;
            }
            if (wantsM15) {
              while (m15Idx < m15Queue.length && m15Queue[m15Idx].epoch + 900 <= tick.epoch) {
                this.slot.superM15Buf.push(m15Queue[m15Idx]); if (this.slot.superM15Buf.length > 100) this.slot.superM15Buf.shift();
                m15Idx++;
              }
            }
          }
        }
        if (!mainDriveIsCandles) {
          dispatch(this.slot, tick.quote);
        }

        this.processedTicks = i + 1;
        if (this.speed !== 'max') {
          const delayMs = 1000 / this.speed;
          if (delayMs > 0) await sleepMs(delayMs);
        } else if (i % 250 === 0) {
          // FIX: at 'max' speed there was previously NO await anywhere in this
          // loop unless paused — for a multi-day/255k-tick backtest that means
          // one giant synchronous block with zero chances for Node to handle
          // anything else. Since this bot's live trading, HTTP API, and SSE
          // pushes all run on the SAME event loop, that would freeze live
          // trading entirely for the whole backtest's run time. Yielding every
          // 250 ticks costs essentially nothing on throughput but lets live
          // ticks/requests/websocket messages interleave normally.
          await sleepMs(0);
        }
      }
      this.status = 'completed';
    } catch (err) {
      this.status = 'error';
      this.error = err.message;
      serverLog('Backtest error', { id: this.id, err: err.message });
    }
  }
}

const backtestRunners = new Map();
let backtestIdCounter = 1;

function bufPrices(buf,mode){ return mode==='candles' ? buf.flatMap(c=>[parseFloat(c.high),parseFloat(c.low)]) : buf.map(Number); }
function bufSpread(buf,mode){ const ps=bufPrices(buf,mode); return parseFloat((Math.max(...ps)-Math.min(...ps)).toFixed(5)); }

// ─── Golden Logic — reversal touch (was Logic 3) ───────────────────────────────
function evalGolden_watch(slot){
  if (slot.goldenStructurePaused) return; // Real Trend Pause — no new setups until a real trend reappears
  if (slot.busy || slot.buf.length < slot.cfg.observe_count) return;
  if (!passFilter(slot)) return;
  const sp = bufSpread(slot.buf, slot.cfg.observe_mode);
  if (sp > slot.cfg.reaction_range) return;
  const ps = bufPrices(slot.buf, slot.cfg.observe_mode);
  slot.zoneHigh = Math.max(...ps); slot.zoneLow = Math.min(...ps);
  slot.phase='confirming'; slot.confSide=null; slot.confBuf=[];
  slot.emit(`Zone locked — HIGH:${slot.zoneHigh} | LOW:${slot.zoneLow} | spread:${sp}`);
  slot.emit('Waiting for price to reach HIGH or LOW…');
  slot.pushState();
}

function onConfirmTick(slot, price){
  if (!slot.running || slot.busy || slot.phase !== 'confirming') return;
  if (!slot.confSide) {
    if (price >= slot.zoneHigh) { slot.confSide='high'; slot.confBuf=[price]; slot.emit(`HIGH touched (${slot.zoneHigh}) — waiting ${slot.cfg.confirm_count} back`); }
    else if (price <= slot.zoneLow) { slot.confSide='low'; slot.confBuf=[price]; slot.emit(`LOW touched (${slot.zoneLow}) — waiting ${slot.cfg.confirm_count} back`); }
    return;
  }
  slot.confBuf.push(price);
  if (slot.confSide==='high' && (slot.confBuf.filter(p=>p<slot.zoneHigh).length>=slot.cfg.confirm_count || slot.cfg.confirm_count===0)) fireReversal(slot,'high');
  if (slot.confSide==='low'  && (slot.confBuf.filter(p=>p>slot.zoneLow ).length>=slot.cfg.confirm_count || slot.cfg.confirm_count===0)) fireReversal(slot,'low');
}

function fireReversal(slot, side){
  const c = slot.cfg;

  // Trend confirmation — HIGH touch needs a downtrend confirmed (rejection at the
  // top), LOW touch needs an uptrend confirmed (rejection at the bottom). No-op if disabled.
  if (!goldenTrendConfirmed(slot, side)) {
    slot.emit('Golden: trend FAILED — discarding setup, scanning fresh','err');
    slot.phase='watching'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
    slot.pushState();
    return;
  }

  // Consolidation filter — reject tight, choppy ranges even if the zone touch was valid
  if (!goldenConsolidationOk(slot)) {
    slot.phase='watching'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
    slot.pushState();
    return;
  }

  slot.phase='trading'; slot.busy=true; slot.pushState();
  const barrier = side==='high' ? c.barrier1 : c.barrier2;

  // ── Golden Logic: PRIMARY one touch + ONE optional attached contract, fired together ──
  const on      = side==='high' ? c.combo_high_on      : c.combo_low_on;
  const aType   = side==='high' ? c.combo_high_type    : c.combo_low_type;
  const aBar    = side==='high' ? c.combo_high_barrier : c.combo_low_barrier;
  const aDV     = side==='high' ? c.combo_high_dv      : c.combo_low_dv;
  const aDU     = side==='high' ? c.combo_high_du      : c.combo_low_du;
  const aStake  = side==='high' ? c.combo_high_stake   : c.combo_low_stake;

  const primaryParams = buildParams(c.symbol,'ONETOUCH',barrier,c.duration_value,c.duration_unit,c.stake);
  slot.emit(`Confirmed ${side.toUpperCase()} reversal — PRIMARY ONETOUCH @${barrier}`);
  const primaryP = runContract(slot, primaryParams, `PRIMARY ONETOUCH ${side.toUpperCase()}`, { role:'primary' })
    .catch(e => { slot.emit('Primary error: '+e.message,'err'); return null; });

  let comboP = Promise.resolve(null);
  if (on && aType && aType !== 'none') {
    const ap = buildParams(c.symbol, aType, aBar, aDV, aDU, aStake);
    slot.emit(`Combo attach — ${aType}${ap.barrier ? ' @'+ap.barrier : ''} | ${ap.duration}${ap.duration_unit} | $${ap.amount}`);
    comboP = runContract(slot, ap, `COMBO ${aType}`, { role:'combo' })
      .catch(e => { slot.emit('Combo error: '+e.message,'err'); return null; });
  }
  // rest / momentum decision is based on the PRIMARY one touch ONLY
  Promise.all([primaryP, comboP]).then(([primaryRec]) => afterTrade(slot, primaryRec ? primaryRec.won : false));
}

function afterTrade(slot, won){
  if (!slot.running) return;
  updateAutoTrendLevel(slot, 'golden', won);
  if (won) slot.goldenStructureLossStreak = 0;
  else checkStructurePauseTrigger(slot, 'golden');
  if (checkTPSL(slot)) return;
  if (won && slot.cfg.skip_rest_on_win) {
    slot.busy=false; slot.phase='watching'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
    slot.pushState(); slot.emit('WIN — no rest, riding momentum with current data','win'); return;
  }
  const rest = Math.max(0, Number(slot.cfg.rest_seconds)) * 1000;
  if (rest > 0) slot.emit(`Resting ${slot.cfg.rest_seconds}s…`);
  const resetFn = () => {
    if (!slot.running) return;
    slot.busy=false; slot.softReset(); slot.pushState();
    slot.emit(`Collecting fresh data 0/${slot.cfg.observe_count}…`);
  };
  if (slot.backtest) {
    slot.backtestRunner.scheduleReset(slot.backtestRunner.currentEpoch + Math.max(0, Number(slot.cfg.rest_seconds)), resetFn);
  } else {
    setTimeout(resetFn, rest);
  }
}

// ─── Super Logic engine ────────────────────────────────────────────────────────

// Candle color: green = close > open, red = close < open (doji treated as same-color reset)
function candleColor(c){
  const cl = parseFloat(c.close), op = parseFloat(c.open);
  if (cl > op) return 'green';
  if (cl < op) return 'red';
  return 'doji';
}

// Aggregate a series of M1 (60s) candles into higher-timeframe OHLC candles —
// the Data Engine only ever stores M1, so M5/M15 are derived on demand from it
// (both for backtesting's trend buffers and any live M5/M15 needs). Candles
// are bucketed by epoch floor-aligned to bucketSeconds, and a bucket is only
// emitted once a LATER m1 candle proves it has closed (mirrors how the live
// Candle Manager only fires on close, never mid-candle) — the caller drives
// this via nextClosedBucket()/consume() below rather than getting a finished
// array up front, so bucket-close timing lines up with tick-by-tick replay.
function aggregateCandles(m1Candles, bucketSeconds) {
  const buckets = [];
  let cur = null;
  for (const c of m1Candles) {
    const bucketStart = Math.floor(c.epoch / bucketSeconds) * bucketSeconds;
    if (!cur || cur.epoch !== bucketStart) {
      if (cur) buckets.push(cur);
      cur = { epoch: bucketStart, open: c.open, high: c.high, low: c.low, close: c.close };
    } else {
      cur.high = Math.max(cur.high, c.high);
      cur.low = Math.min(cur.low, c.low);
      cur.close = c.close;
    }
  }
  if (cur) buckets.push(cur);
  return buckets;
}

// Trend detection on a candle buffer: majority candle color over the lookback
// window. Needs >=thresholdPct% green to call uptrend, >=thresholdPct% red to
// call downtrend — otherwise neutral. Pure color count, not high/low structure.
// thresholdPct is a plain percent number (e.g. 60 for 60%), not a fraction.
function detectTrend(buf, lookback, thresholdPct){
  const win = lookback ? buf.slice(-lookback) : buf;
  if (win.length < 3) return 'neutral';
  let green = 0, red = 0;
  win.forEach(c => {
    const o = parseFloat(c.open), cl = parseFloat(c.close);
    if (cl > o) green++; else if (cl < o) red++;
  });
  const total = win.length;
  const threshold = (Number(thresholdPct) || 60) / 100;
  if (green / total >= threshold) return 'uptrend';
  if (red   / total >= threshold) return 'downtrend';
  return 'neutral';
}

// ─── Trend strictness: manual vs automatic (added) ────────────────────────────
// Manual mode: always use the configured fixed % (golden_trend_threshold_pct /
// super_trend_threshold_pct).
// Automatic mode: self-adjusting between the configured min/max, stepping by
// TREND_AUTO_STEP_PCT (fixed 10%) — any loss tightens one step immediately,
// N consecutive wins (configurable) eases back down one step. Separate state
// is tracked per logic (golden/super) per slot.
const TREND_AUTO_STEP_PCT = 10;

function getEffectiveTrendThreshold(slot, logic){
  const cfg = slot.cfg;
  const mode = cfg[`${logic}_trend_strictness_mode`] || 'manual';
  if (mode !== 'automatic') return Number(cfg[`${logic}_trend_threshold_pct`]) || 60;

  const min = Number(cfg[`${logic}_trend_auto_min_pct`]) || 60;
  const max = Number(cfg[`${logic}_trend_auto_max_pct`]) || 80;
  const levelKey = logic === 'golden' ? 'goldenTrendAutoLevel' : 'superTrendAutoLevel';
  let level = slot[levelKey];
  if (level == null || isNaN(level) || level < min || level > max) level = min; // init/clamp if cfg changed underneath it
  slot[levelKey] = level;
  return level;
}

// Called once a trade's win/loss is known for that logic — moves the
// automatic level accordingly. No-op in manual mode.
function updateAutoTrendLevel(slot, logic, won){
  const cfg = slot.cfg;
  if ((cfg[`${logic}_trend_strictness_mode`] || 'manual') !== 'automatic') return;

  const min = Number(cfg[`${logic}_trend_auto_min_pct`]) || 60;
  const max = Number(cfg[`${logic}_trend_auto_max_pct`]) || 80;
  const winEaseEnabled = cfg[`${logic}_trend_auto_win_ease_enabled`] !== false; // default true — preserves prior behavior
  const winStreakNeeded = Math.max(1, Number(cfg[`${logic}_trend_auto_win_streak`]) || 1);
  const levelKey  = logic === 'golden' ? 'goldenTrendAutoLevel'     : 'superTrendAutoLevel';
  const streakKey = logic === 'golden' ? 'goldenTrendAutoWinStreak' : 'superTrendAutoWinStreak';
  const label = logic === 'golden' ? 'Golden' : 'Super';

  if (slot[levelKey] == null) slot[levelKey] = min;

  if (won) {
    if (!winEaseEnabled) { slot[streakKey] = 0; return; } // tightening-on-loss is always on; only easing is optional
    slot[streakKey] = (slot[streakKey] || 0) + 1;
    if (slot[streakKey] >= winStreakNeeded) {
      const before = slot[levelKey];
      slot[levelKey] = Math.max(min, slot[levelKey] - TREND_AUTO_STEP_PCT);
      slot[streakKey] = 0;
      if (slot[levelKey] !== before) slot.emit(`${label} auto-trend: ${winStreakNeeded>1?winStreakNeeded+'-win streak':'win'} → easing to ${slot[levelKey]}%`);
    }
  } else {
    slot[streakKey] = 0;
    const before = slot[levelKey];
    slot[levelKey] = Math.min(max, slot[levelKey] + TREND_AUTO_STEP_PCT);
    if (slot[levelKey] !== before) slot.emit(`${label} auto-trend: loss → tightening to ${slot[levelKey]}%`, 'err');
  }
  slot.pushState();
}

/**
 * REAL TREND PAUSE (structure-based) — added per user request, replacing an
 * earlier price-drift-easing attempt that didn't test well and was removed.
 *
 * This is a completely separate concept from the color-based trend
 * confirmation above. It's market STRUCTURE: swing highs/lows, not candle
 * color, not majority-of-candles. A swing high is a candle whose high is
 * higher than `swingStrength` candles on BOTH sides of it (a local peak); a
 * swing low is the mirror (a local trough). An uptrend needs a run of
 * successively HIGHER swing highs AND higher swing lows; a downtrend needs
 * successively LOWER swing highs and lower swing lows. Anything else —
 * including a mix, or not enough swings found yet — is "ranging", i.e. not
 * a real trend.
 *
 * Returns 'uptrend' | 'downtrend' | 'ranging'.
 */
function detectSwingTrend(candleBuf, swingStrength, confirmCount){
  const strength = Math.max(1, Number(swingStrength) || 2);
  const need = Math.max(2, Number(confirmCount) || 3);
  if (!candleBuf || candleBuf.length < strength * 2 + 1) return 'ranging'; // not enough candles to find even one swing

  const swingHighs = []; // chronological list of {epoch, value}
  const swingLows  = [];
  for (let i = strength; i < candleBuf.length - strength; i++) {
    const c = candleBuf[i];
    const high = parseFloat(c.high), low = parseFloat(c.low);
    let isSwingHigh = true, isSwingLow = true;
    for (let k = 1; k <= strength; k++) {
      const left = candleBuf[i - k], right = candleBuf[i + k];
      if (!(high > parseFloat(left.high)) || !(high > parseFloat(right.high))) isSwingHigh = false;
      if (!(low  < parseFloat(left.low))  || !(low  < parseFloat(right.low)))  isSwingLow  = false;
    }
    if (isSwingHigh) swingHighs.push(high);
    if (isSwingLow)  swingLows.push(low);
  }

  if (swingHighs.length < need || swingLows.length < need) return 'ranging'; // not enough confirmed swings yet

  const recentHighs = swingHighs.slice(-need);
  const recentLows  = swingLows.slice(-need);
  let allHigherHighs = true, allLowerHighs = true, allHigherLows = true, allLowerLows = true;
  for (let i = 1; i < recentHighs.length; i++) {
    if (!(recentHighs[i] > recentHighs[i - 1])) allHigherHighs = false;
    if (!(recentHighs[i] < recentHighs[i - 1])) allLowerHighs = false;
  }
  for (let i = 1; i < recentLows.length; i++) {
    if (!(recentLows[i] > recentLows[i - 1])) allHigherLows = false;
    if (!(recentLows[i] < recentLows[i - 1])) allLowerLows = false;
  }

  if (allHigherHighs && allHigherLows) return 'uptrend';
  if (allLowerHighs && allLowerLows) return 'downtrend';
  return 'ranging';
}

// Evaluates whether the configured timeframe(s) currently show a real
// (structural) trend, per golden_/super_structure_pause_timeframes.
function structureTrendConfirmed(slot, logic){
  const cfg = slot.cfg;
  const strength = cfg[`${logic}_structure_swing_strength`];
  const confirmCount = cfg[`${logic}_structure_swing_confirm_count`];
  const timeframes = cfg[`${logic}_structure_pause_timeframes`] || 'm1';

  const m1Trend = timeframes !== 'm5' ? detectSwingTrend(slot.trendBuf, strength, confirmCount) : null;
  const m5Trend = timeframes !== 'm1' ? detectSwingTrend(slot.superM5Buf, strength, confirmCount) : null;

  if (timeframes === 'm1') return m1Trend !== 'ranging';
  if (timeframes === 'm5') return m5Trend !== 'ranging';
  return m1Trend !== 'ranging' && m5Trend !== 'ranging'; // 'm1_and_m5' — both must show SOME real trend (not necessarily the same direction)
}

// Called from afterTrade/superAfterTrade on every LOSS. Counts consecutive
// losses; once the configured threshold is hit, checks structure right away
// — if the market's ranging, pauses new setups until a real trend reappears
// (checked continuously via maybeResumeFromStructurePause, on every M1 close).
function checkStructurePauseTrigger(slot, logic){
  const cfg = slot.cfg;
  if (!cfg[`${logic}_structure_pause_enabled`]) return;

  const streakKey = logic === 'golden' ? 'goldenStructureLossStreak' : 'superStructureLossStreak';
  const pauseKey  = logic === 'golden' ? 'goldenStructurePaused'     : 'superStructurePaused';
  const label = logic === 'golden' ? 'Golden' : 'Super';
  const threshold = Math.max(1, Number(cfg[`${logic}_structure_pause_loss_threshold`]) || 1);

  slot[streakKey] = (slot[streakKey] || 0) + 1;
  if (slot[streakKey] < threshold) return;

  slot[streakKey] = 0; // consumed — next pause-check needs another full run of losses
  if (structureTrendConfirmed(slot, logic)) {
    slot.emit(`${label} structure check: real trend already present — continuing normally`);
    return;
  }
  slot[pauseKey] = true;
  slot.emit(`${label} paused — market structure is ranging (no confirmed higher-highs/lows or lower-highs/lows). Holding until a real trend appears.`, 'err');
  slot.pushState();
}

// Called on every M1 candle close while paused (see pushM1TrendCandle) — the
// ONLY thing that can lift the pause, regardless of what triggered it.
function maybeResumeFromStructurePause(slot, logic){
  const pauseKey = logic === 'golden' ? 'goldenStructurePaused' : 'superStructurePaused';
  if (!slot[pauseKey]) return;
  if (!structureTrendConfirmed(slot, logic)) return;
  slot[pauseKey] = false;
  const label = logic === 'golden' ? 'Golden' : 'Super';
  slot.emit(`${label} resumed — real trend confirmed, no longer ranging.`, 'win');
  slot.pushState();
}

// Trend confirmation gate — returns true if the trade direction is confirmed
function superTrendConfirmed(slot, direction){
  const cfg = slot.cfg;
  if (!cfg.super_trend_enabled) return true;

  const threshold = getEffectiveTrendThreshold(slot, 'super');
  const m1Trend  = detectTrend(slot.trendBuf,    Number(cfg.super_trend_lookback_m1)  || 10, threshold);
  const m5Trend  = detectTrend(slot.superM5Buf,  Number(cfg.super_trend_lookback_m5)  || 8,  threshold);
  const m15Trend = detectTrend(slot.superM15Buf, Number(cfg.super_trend_lookback_m15) || 6,  threshold);

  const required = direction === 'buy' ? 'uptrend' : 'downtrend';

  if (cfg.super_trend_mode === 'm1_5') {
    // Mode: M1 or M5 must match — M15 ignored
    const match = (m1Trend === required) || (m5Trend === required);
    slot.emit(`Super trend (M1-5 mode): M1=${m1Trend} M5=${m5Trend} | need=${required} | ${match ? 'PASS' : 'FAIL'}`);
    return match;
  } else if (cfg.super_trend_mode === 'm1_and_5') {
    // Mode: M1 AND M5 must both match — stricter than M1-5, M15 ignored
    const match = (m1Trend === required) && (m5Trend === required);
    slot.emit(`Super trend (M1+M5 mode): M1=${m1Trend} M5=${m5Trend} | need=${required} on BOTH | ${match ? 'PASS' : 'FAIL'}`);
    return match;
  } else {
    // Mode 1: M1-5-15, require minimum matching count
    const minMatch = Number(cfg.super_trend_min_match) || 2;
    const matches  = [m1Trend, m5Trend, m15Trend].filter(t => t === required).length;
    const pass     = matches >= minMatch;
    slot.emit(`Super trend (M1-5-15 mode): M1=${m1Trend} M5=${m5Trend} M15=${m15Trend} | need ${minMatch} x ${required} | got ${matches} | ${pass ? 'PASS' : 'FAIL'}`);
    return pass;
  }
}

// Push live trend to client
function superPushTrend(slot){
  if (!slot.running || slot.cfg.logic !== 'super') return;
  const cfg = slot.cfg;
  const threshold = getEffectiveTrendThreshold(slot, 'super');
  const m1  = detectTrend(slot.trendBuf,    Number(cfg.super_trend_lookback_m1)  || 10, threshold);
  const m5  = detectTrend(slot.superM5Buf,  Number(cfg.super_trend_lookback_m5)  || 8,  threshold);
  const m15 = detectTrend(slot.superM15Buf, Number(cfg.super_trend_lookback_m15) || 6,  threshold);
  push({ type:'trend', slotId:slot.id, data:{ m1, m5, m15 } });
}

// Full Super Logic reset — discard setup, start fresh
function superReset(slot){
  slot.superState     = 'scanning';
  slot.superRun       = [];
  slot.superRunColor  = null;
  slot.superTriggerBuf = [];
}

// Fire a Super Logic trade (primary ONETOUCH only, no combo)
function superFireTrade(slot, direction){
  if (slot.busy) return;
  slot.busy = true; slot.phase = 'trading'; slot.pushState();
  const c       = slot.cfg;
  const barrier = direction === 'buy' ? c.barrier1 : c.barrier2;
  const label   = direction === 'buy' ? 'BUY +Barrier' : 'SELL -Barrier';
  slot.emit(`Super Logic signal: ${label} ONETOUCH @${barrier}`);
  const params  = buildParams(c.symbol, 'ONETOUCH', barrier, c.duration_value, c.duration_unit, c.stake);
  runContract(slot, params, `SUPER ${label}`, { role:'primary' })
    .then(rec  => superAfterTrade(slot, rec.won))
    .catch(e   => { slot.emit('Super trade error: '+e.message,'err'); superAfterTrade(slot, false); });
}

// Post-trade cooldown then clean restart
function superAfterTrade(slot, won){
  if (!slot.running) return;
  updateAutoTrendLevel(slot, 'super', won);
  if (won) slot.superStructureLossStreak = 0;
  else checkStructurePauseTrigger(slot, 'super');
  if (checkTPSL(slot)) return;
  superReset(slot);
  const cooldown = Math.max(0, Number(slot.cfg.super_cooldown_seconds)) * 1000;
  slot.emit(`Super cooldown ${slot.cfg.super_cooldown_seconds}s — pausing all analysis…`);
  const resetFn = () => {
    if (!slot.running) return;
    slot.busy  = false;
    slot.phase = 'watching';
    superReset(slot);
    slot.pushState();
    slot.emit('Super cooldown done — scanning for fresh candle sequence…');
  };
  if (slot.backtest) {
    slot.backtestRunner.scheduleReset(slot.backtestRunner.currentEpoch + Math.max(0, Number(slot.cfg.super_cooldown_seconds)), resetFn);
  } else {
    setTimeout(resetFn, cooldown);
  }
}




// Called on every newly closed M1 candle for Super Logic
function superOnCandle(slot, candle){
  if (!slot.running || slot.busy) return;
  if (slot.superStructurePaused) return; // Real Trend Pause — no new setups until a real trend reappears

  const n    = Number(slot.cfg.super_same_color_count) || 3;
  const m    = Number(slot.cfg.super_opposite_count)   || 1;
  const size = n + m;

  // Rolling buffer — keep last (size) candles
  slot.superRun.push(candle);
  if (slot.superRun.length > size) slot.superRun = slot.superRun.slice(-size);

  // Not enough candles yet
  if (slot.superRun.length < size){
    slot.emit(`Super: collecting ${slot.superRun.length}/${size} candles…`);
    return;
  }

  // Evaluate the window: first N = same-color, last M = opposite
  const win      = slot.superRun.slice(-size);
  const samePart = win.slice(0, n);
  const oppPart  = win.slice(n);

  const sameColor = candleColor(samePart[0]);
  const oppColor  = sameColor === 'green' ? 'red' : 'green';

  // All N candles must be the same color (no doji)
  if (sameColor === 'doji' || !samePart.every(c => candleColor(c) === sameColor)) return;

  // All M opposite candles must be the opposite color (no doji, no same color)
  if (!oppPart.every(c => candleColor(c) === oppColor)) return;

  // Direction check on same-color candles only
  // Green: each close must be strictly higher than previous
  // Red:   each close must be strictly lower than previous
  let dirValid = true;
  for (let i = 1; i < samePart.length; i++){
    const prev = parseFloat(samePart[i-1].close);
    const curr = parseFloat(samePart[i].close);
    if (sameColor === 'green' ? curr <= prev : curr >= prev){ dirValid = false; break; }
  }

  if (!dirValid){
    slot.emit(`Super: ${sameColor.toUpperCase()}x${n} found but direction not progressive — skipping`);
    return;
  }

  // Pattern valid — determine trade direction
  // Green ascending → price was rising → expect reversal → SELL (−barrier)
  // Red descending  → price was falling → expect reversal → BUY (+barrier)
  const direction = sameColor === 'green' ? 'sell' : 'buy';
  slot.emit(`Super: ✓ ${sameColor.toUpperCase()}x${n} + ${oppColor.toUpperCase()}x${m} → ${direction.toUpperCase()} setup — checking trend…`);

  // Trend confirmation
  if (!superTrendConfirmed(slot, direction)){
    slot.emit('Super: trend FAILED — discarding setup, scanning fresh','err');
    slot.superRun = []; // discard all candles from this setup
    return;
  }

  // Consolidation filter — reject tight, choppy ranges even if pattern+trend passed
  if (!superConsolidationOk(slot)){
    slot.superRun = []; // discard all candles from this setup
    return;
  }

  // All conditions met — fire
  slot.emit(`Super: trend confirmed → firing ${direction.toUpperCase()}!`,'ok');
  slot.superRun = [];
  slot.superState = 'scanning';
  superFireTrade(slot, direction);
}

// Blocks trading when recent price action is too tight/choppy to trust a reversal
// signal. Measures (highest high − lowest low) over the last N M1 candles; if that
// range is smaller than the configured minimum, it's consolidation — skip the trade.
function superConsolidationOk(slot){
  const cfg = slot.cfg;
  if (!cfg.super_consolidation_filter_enabled) return true;
  const lookback = Number(cfg.super_consolidation_lookback) || 10;
  const minRange = Number(cfg.super_consolidation_min_range) || 0;
  const win = slot.trendBuf.slice(-lookback);
  if (win.length < 3) return true; // not enough data yet — don't block on startup
  const highs = win.map(c => parseFloat(c.high));
  const lows  = win.map(c => parseFloat(c.low));
  const range = Math.max(...highs) - Math.min(...lows);
  if (range < minRange) {
    slot.emit(`Super: consolidation filter — range ${range.toFixed(2)} < min ${minRange} over last ${win.length} candles — skipping`,'err');
    return false;
  }
  return true;
}

// Golden Logic's trend confirmation — same M1/M5/M15 engine as Super Logic, but
// direction is mapped from the zone side rather than a color pattern: a HIGH-zone
// touch is a rejection at the top (needs downtrend confirmed), a LOW-zone touch is
// a rejection at the bottom (needs uptrend confirmed).
function goldenTrendConfirmed(slot, side){
  const cfg = slot.cfg;
  if (!cfg.golden_trend_enabled) return true;

  const required = side === 'high' ? 'downtrend' : 'uptrend';
  const threshold = getEffectiveTrendThreshold(slot, 'golden');
  const m1Trend  = detectTrend(slot.trendBuf,    Number(cfg.golden_trend_lookback_m1)  || 10, threshold);
  const m5Trend  = detectTrend(slot.superM5Buf,  Number(cfg.golden_trend_lookback_m5)  || 8,  threshold);
  const m15Trend = detectTrend(slot.superM15Buf, Number(cfg.golden_trend_lookback_m15) || 6,  threshold);

  if (cfg.golden_trend_mode === 'm1_5') {
    const match = (m1Trend === required) || (m5Trend === required);
    slot.emit(`Golden trend (M1-5 mode): M1=${m1Trend} M5=${m5Trend} | need=${required} | ${match ? 'PASS' : 'FAIL'}`);
    return match;
  } else if (cfg.golden_trend_mode === 'm1_and_5') {
    const match = (m1Trend === required) && (m5Trend === required);
    slot.emit(`Golden trend (M1+M5 mode): M1=${m1Trend} M5=${m5Trend} | need=${required} on BOTH | ${match ? 'PASS' : 'FAIL'}`);
    return match;
  } else {
    const minMatch = Number(cfg.golden_trend_min_match) || 2;
    const matches  = [m1Trend, m5Trend, m15Trend].filter(t => t === required).length;
    const pass     = matches >= minMatch;
    slot.emit(`Golden trend (M1-5-15 mode): M1=${m1Trend} M5=${m5Trend} M15=${m15Trend} | need ${minMatch} x ${required} | got ${matches} | ${pass ? 'PASS' : 'FAIL'}`);
    return pass;
  }
}

// Golden Logic's consolidation filter — identical mechanism to Super Logic's.
function goldenConsolidationOk(slot){
  const cfg = slot.cfg;
  if (!cfg.golden_consolidation_filter_enabled) return true;
  const lookback = Number(cfg.golden_consolidation_lookback) || 10;
  const minRange = Number(cfg.golden_consolidation_min_range) || 0;
  const win = slot.trendBuf.slice(-lookback);
  if (win.length < 3) return true;
  const highs = win.map(c => parseFloat(c.high));
  const lows  = win.map(c => parseFloat(c.low));
  const range = Math.max(...highs) - Math.min(...lows);
  if (range < minRange) {
    slot.emit(`Golden: consolidation filter — range ${range.toFixed(2)} < min ${minRange} over last ${win.length} candles — skipping`,'err');
    return false;
  }
  return true;
}

// ─── data dispatch — guaranteed fresh warm-up before any trade ──────────────────
// Centralized M1/M5 buffer feeds — used by both live (startWatcher) and
// backtest (BacktestRunner) so behavior is identical in both. M1 is also the
// ONLY trigger for re-checking Real Trend Pause resumption (per design: only
// re-check on M1 close, regardless of which timeframe(s) the pause itself
// watches) — M5 pushes never trigger a resume check directly.
function pushM1TrendCandle(slot, logic, candle){
  slot.trendBuf.push(candle); if (slot.trendBuf.length > 100) slot.trendBuf.shift();
  maybeResumeFromStructurePause(slot, logic);
}
function pushM5TrendCandle(slot, candle){
  slot.superM5Buf.push(candle); if (slot.superM5Buf.length > 100) slot.superM5Buf.shift();
}

function dispatch(slot, val){
  if (!slot.running) return;

  // ── Super Logic: only uses M1 closed candles ─────────────────────────────
  if (slot.cfg.logic === 'super'){
    if (typeof val === 'object' && val.open != null){
      // val is a candle — only process closed ones (isLive=false from history, or ohlc with epoch matching)
      // The stream is set up so dispatch is only called with closed candles (isLive flag filtered in startWatcher)
      superOnCandle(slot, val);
    }
    return;
  }

  // ── Golden Logic ──────────────────────────────────────────────────────────
  const price = slot.cfg.observe_mode==='candles' ? parseFloat(val.close) : Number(val);
  if (slot.phase === 'confirming') { onConfirmTick(slot, price); return; }
  if (slot.phase === 'trading' || slot.busy) return;

  slot.liveCount++;
  slot.buf.push(val); if (slot.buf.length > slot.cfg.observe_count) slot.buf.shift();

  if (slot.liveCount < slot.cfg.observe_count) {
    slot.emit(`Collecting fresh ${slot.cfg.observe_mode==='candles'?'candles':'ticks'} ${slot.liveCount}/${slot.cfg.observe_count}…`);
    slot.pushState(); return;
  }
  if (slot.liveCount === slot.cfg.observe_count && slot.phase === 'warmup') {
    slot.phase = 'watching';
    slot.emit(`Warm-up complete — ${slot.cfg.observe_count} fresh ${slot.cfg.observe_mode==='candles'?'candles':'ticks'} collected — monitoring`);
    slot.pushState();
  }
  if (slot.phase === 'watching') evalGolden_watch(slot);
}

// ─── watchers ─────────────────────────────────────────────────────────────────
function closeWs(ws){ if (ws) try { ws.close(); } catch {} }

function clearCandleSubs(slot){ slot.candleSubs.forEach(unsub => { try { unsub(); } catch {} }); slot.candleSubs = []; }

function startWatcher(slot){
  closeWs(slot.dataWs); slot.dataWs=null;
  clearCandleSubs(slot);

  const cfg = slot.cfg;

  if (cfg.logic === 'super'){
    // All candles now come from the shared Candle Manager — one subscription per
    // granularity needed. Same-symbol slots share the underlying tick connection.
    slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 60, candle => {
      if (!slot.running) return;
      pushM1TrendCandle(slot, 'super', candle);
      superPushTrend(slot);
      dispatch(slot, candle);
    }));
    slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 300, candle => {
      if (!slot.running) return;
      pushM5TrendCandle(slot, candle);
      superPushTrend(slot);
    }));
    if (cfg.super_trend_mode !== 'm1_5' && cfg.super_trend_mode !== 'm1_and_5') {
      slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 900, candle => {
        if (!slot.running) return;
        slot.superM15Buf.push(candle); if (slot.superM15Buf.length > 100) slot.superM15Buf.shift();
        superPushTrend(slot);
      }));
    }
    slot.phase = 'watching'; slot.pushState();
    slot.emit(`Super Logic started — M1 candles (core) | M5 + M15 (trend) | Symbol: ${cfg.symbol}`);
    return;
  }

  // Golden Logic streams
  // M1 candles feed the existing 'trend' filter, the new multi-timeframe trend
  // confirmation, the consolidation filter, and Real Trend Pause — one shared
  // subscription for all of them.
  if (cfg.filter === 'trend' || cfg.golden_trend_enabled || cfg.golden_consolidation_filter_enabled || (cfg.golden_structure_pause_enabled && cfg.golden_structure_pause_timeframes !== 'm5')) {
    slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 60, candle => { if (!slot.running) return; pushM1TrendCandle(slot, 'golden', candle); }));
  }
  // M5/M15 are needed for the trend confirmation feature and/or Real Trend Pause
  // when it's set to watch M5 — mirrors Super Logic's setup.
  if (cfg.golden_trend_enabled || (cfg.golden_structure_pause_enabled && cfg.golden_structure_pause_timeframes !== 'm1')) {
    slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 300, candle => { if (!slot.running) return; pushM5TrendCandle(slot, candle); }));
    if (cfg.golden_trend_enabled && cfg.golden_trend_mode !== 'm1_5' && cfg.golden_trend_mode !== 'm1_and_5') {
      slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 900, candle => { if (!slot.running) return; slot.superM15Buf.push(candle); if (slot.superM15Buf.length>100) slot.superM15Buf.shift(); }));
    }
  }
  if (cfg.observe_mode === 'candles') {
    slot.candleSubs.push(candleManager.subscribe(cfg.symbol, 60, candle => dispatch(slot, candle)));
  } else {
    const attachClose = ws => {
      ws.on('error', e => slot.emit('Stream error: '+e.message,'err'));
      ws.on('close', () => {
        if (!slot.running) return;
        slot.emit('Stream dropped — reconnecting in 4s…','err');
        setTimeout(() => { if (slot.running) { slot.buf=[]; slot.liveCount=0; slot.phase='warmup'; startWatcher(slot); } }, 4000);
      });
    };
    slot.dataWs = acct.openTickStream(cfg.symbol, price => dispatch(slot, price));
    attachClose(slot.dataWs);
  }
  slot.emit(`Stream open — ${cfg.observe_mode==='candles'?'M1 Candles':'Ticks'} | Golden Logic | Filter: ${cfg.filter}`);
}

async function startSlot(slot){
  if (slot.running) return { error:'Already running' };
  if (!acct.ready) { try { await acct.connect(); } catch(e) { return { error:e.message }; } }
  slot.running=true; slot.busy=false; slot.sessionProfit=0;
  slot.buf=[]; slot.trendBuf=[]; slot.liveCount=0;
  slot.phase='warmup'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
  slot.superM5Buf=[]; slot.superM15Buf=[];
  // Automatic trend-strictness always restarts at the configured minimum —
  // "if bot starts, it starts with the minimum".
  slot.goldenTrendAutoLevel=Number(slot.cfg.golden_trend_auto_min_pct)||60; slot.goldenTrendAutoWinStreak=0;
  slot.superTrendAutoLevel=Number(slot.cfg.super_trend_auto_min_pct)||60;   slot.superTrendAutoWinStreak=0;
  slot.goldenStructureLossStreak=0; slot.goldenStructurePaused=false;
  slot.superStructureLossStreak=0;  slot.superStructurePaused=false;
  superReset(slot);
  slot.pushState(); startWatcher(slot);
  if (slot.cfg.logic !== 'super') slot.emit(`Bot started — collecting fresh data 0/${slot.cfg.observe_count} (no trade until warm-up done)…`);
  return { success:true };
}

function stopSlot(slot){
  slot.running=false; slot.busy=false;
  closeWs(slot.dataWs); slot.dataWs=null;
  clearCandleSubs(slot);
  slot.buf=[]; slot.trendBuf=[]; slot.liveCount=0;
  slot.phase='warmup'; slot.zoneHigh=null; slot.zoneLow=null; slot.confSide=null; slot.confBuf=[];
  slot.superM5Buf=[]; slot.superM15Buf=[];
  superReset(slot);
  slot.emit('Bot stopped'); slot.pushState(); return { success:true };
}

// ─── health monitor — keep all running bots alive forever ───────────────────────
setInterval(() => {
  if (acct.token && !acct.ready && !acct._reconnecting && !acct.isLive) { acct.connect().catch(() => {}); }
  slots.forEach(slot => {
    if (!slot.running) return;
    // Candle-based logic (Golden candle-mode, Super Logic, trend filter) is fed by the
    // Candle Manager, which owns its own reconnect/backoff — don't duplicate that here,
    // or it just recreates the same kind of restart-loop bug as the old account socket.
    if (slot.cfg.logic === 'super' || slot.cfg.observe_mode === 'candles') return;
    // Golden Logic tick-mode still uses a raw per-slot WebSocket — that one we do watch.
    const ws = slot.dataWs;
    if (!ws || ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
      slot.emit('Health check: stream dead — restarting','err');
      slot.buf=[]; slot.liveCount=0; slot.phase='warmup';
      startWatcher(slot);
    }
  });
}, 10000);

// ─── symbols ──────────────────────────────────────────────────────────────────
let symsCache = null;
function fetchSymbols(){
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_DEMO);
    const t = setTimeout(() => { try { ws.close(); } catch {} reject(new Error('Timed out')); }, 10000);
    ws.on('open', () => ws.send(JSON.stringify({ active_symbols:'full', product_type:'basic' })));
    ws.on('message', raw => { let m; try { m=JSON.parse(raw); } catch { return; } if (m.msg_type==='active_symbols') { clearTimeout(t); try { ws.close(); } catch {} if (m.error) return reject(new Error(m.error.message)); resolve(m.active_symbols); } });
    ws.on('error', e => { clearTimeout(t); reject(e); });
  });
}

// ─── persistence (survives restarts) ─────────────────────────────────────────
let persistTimer = null;
function persistSoon(){ if (persistTimer) return; persistTimer = setTimeout(persistNow, 3000); }
function persistNow(){
  persistTimer = null;
  try {
    const data = slots.map(s => ({ id:s.id, cfg:s.cfg, stats:s.stats, history:s.history, sessionProfit:s.sessionProfit }));
    fs.writeFile(STATE_FILE, JSON.stringify(data), () => {});
  } catch {}
}
function loadState(){
  try {
    if (!fs.existsSync(STATE_FILE)) return;
    const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    data.forEach(d => {
      const s = slotOf.get(d.id); if (!s) return;
      if (d.cfg)   s.cfg   = { ...s.cfg, ...d.cfg };
      if (d.stats) s.stats = d.stats;
      if (d.history) s.history = d.history;
      if (typeof d.sessionProfit === 'number') s.sessionProfit = d.sessionProfit;
    });
    serverLog('State restored from disk');
  } catch (e) { serverLog('State load failed', { err:e.message }); }
}
setInterval(persistNow, 30000);

// ─── OAuth routes ────────────────────────────────────────────────────────────
app.get('/api/oauth/url', (req, res) => {
  const verifier = generateCodeVerifier(), challenge = generateCodeChallenge(verifier), state = base64url(crypto.randomBytes(16));
  oauthPending.set(state, { verifier });
  setTimeout(() => oauthPending.delete(state), 10*60*1000);
  const params = new URLSearchParams({ response_type:'code', client_id:APP_ID_LIVE, redirect_uri:REDIRECT_URI, scope:'trade', state, code_challenge:challenge, code_challenge_method:'S256' });
  res.json({ url:`https://auth.deriv.com/oauth2/auth?${params.toString()}` });
});

app.get('/callback', async (req, res) => {
  const { code, state, error } = req.query;
  if (error) return res.send(`<script>window.location.href='/';</script><p>Login failed: ${error}</p>`);
  if (!code || !state) return res.send(`<script>window.location.href='/';</script><p>Missing params</p>`);
  const pending = oauthPending.get(state);
  if (!pending) return res.send(`<script>window.location.href='/';</script><p>Session expired — try again</p>`);
  oauthPending.delete(state);

  const page = (bodyHtml) => `<!DOCTYPE html><html><head><title>Zone Touch — Connecting</title>
  <style>body{background:#060910;color:#C8D0DC;font-family:'JetBrains Mono',monospace;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column;gap:16px;padding:20px;text-align:center;}
  .spin{width:40px;height:40px;border:3px solid #192030;border-top-color:#A78BFA;border-radius:50%;animation:s .8s linear infinite;}
  @keyframes s{to{transform:rotate(360deg);}}
  .acctbtn{background:#0C1220;color:#C8D0DC;border:1px solid #192030;border-radius:8px;padding:12px 16px;font-family:'JetBrains Mono',monospace;font-size:13px;cursor:pointer;min-width:280px;text-align:left;}
  .acctbtn:hover{border-color:#A78BFA;color:#E8EDF3;}
  .acctbtn b{color:#E8EDF3;}
  .err{color:#EF4444;}
  </style></head><body>${bodyHtml}</body></html>`;

  // FIX: this used to respond immediately with a spinner that auto-redirected
  // to '/' after 12s, while the actual token exchange + account lookup ran
  // afterward and tried to hand results to the dashboard over SSE. But the
  // spinner page has no SSE connection, and the redirect fires on its own
  // timer regardless of whether that work finished — so the account list
  // (or any result) was frequently lost and the login just stalled with
  // nothing connected. Now we await everything BEFORE responding, and the
  // picker (or error) is rendered directly into this page — no SSE, no
  // race with a timed redirect.
  try {
    const tokenRes = await fetch('https://auth.deriv.com/oauth2/token', {
      method:'POST', headers:{ 'Content-Type':'application/x-www-form-urlencoded' },
      body:new URLSearchParams({ grant_type:'authorization_code', client_id:APP_ID_LIVE, code, code_verifier:pending.verifier, redirect_uri:REDIRECT_URI }).toString(),
    });
    const tokenData = await tokenRes.json();
    if (!tokenRes.ok || !tokenData.access_token) throw new Error(tokenData.error_description || tokenData.error || 'Token exchange failed');
    const accessToken = tokenData.access_token;

    let accountsRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', { headers:{ 'Authorization':`Bearer ${accessToken}`, 'Deriv-App-ID':APP_ID_LIVE } });
    let accountsData = await accountsRes.json();
    let accounts = accountsData.data || [];

    if (!accounts.length) {
      const createRes = await fetch('https://api.derivws.com/trading/v1/options/accounts', { method:'POST', headers:{ 'Authorization':`Bearer ${accessToken}`, 'Deriv-App-ID':APP_ID_LIVE, 'Content-Type':'application/json' }, body:JSON.stringify({ currency:'USD', group:'row', account_type:'real' }) });
      const createData = await createRes.json();
      accounts = createData.data || [];
      if (!accounts.length) throw new Error('Could not find or create a live account');
    }

    pendingLiveAuth = { accessToken, accounts, expires: Date.now() + 10 * 60 * 1000 };

    const buttons = accounts.map(a => `
      <button class="acctbtn" onclick="pick('${a.account_id}', this)">
        <b>${a.account_id}</b><br>${a.currency || '—'}${a.balance != null ? ` · ${a.balance}` : ''}${(a.account_type || a.type) ? ` · ${a.account_type || a.type}` : ''}
      </button>`).join('');

    res.send(page(`
      <p>Choose which account to connect:</p>
      <div style="display:flex;flex-direction:column;gap:8px">${buttons}</div>
      <p id="status" style="min-height:16px;font-size:12px;"></p>
      <script>
        async function pick(accountId, btn){
          document.querySelectorAll('.acctbtn').forEach(b=>b.disabled=true);
          document.getElementById('status').textContent='Connecting…';
          try {
            const r = await fetch('/api/oauth/select-account', {
              method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ accountId })
            }).then(r=>r.json());
            if (r.error) { document.getElementById('status').textContent = '❌ ' + r.error; document.querySelectorAll('.acctbtn').forEach(b=>b.disabled=false); return; }
            window.location.href = '/';
          } catch (e) {
            document.getElementById('status').textContent = '❌ ' + e.message;
            document.querySelectorAll('.acctbtn').forEach(b=>b.disabled=false);
          }
        }
      </script>
    `));
  } catch (err) {
    serverLog('Live login error', { err:err.message });
    res.send(page(`<div class="err">❌ ${err.message}</div><p><a href="/" style="color:#A78BFA">Back to dashboard</a></p>`));
  }
});

// User picks which of the accounts returned above to actually trade on.
app.post('/api/oauth/select-account', async (req, res) => {
  try {
    const { accountId } = req.body;
    if (!pendingLiveAuth || Date.now() > pendingLiveAuth.expires) {
      pendingLiveAuth = null;
      return res.status(400).json({ error: 'Login session expired — please log in with Deriv again' });
    }
    const chosen = pendingLiveAuth.accounts.find(a => a.account_id === accountId);
    if (!chosen) return res.status(400).json({ error: 'Unknown account_id — not in the list returned at login' });

    const accessToken = pendingLiveAuth.accessToken;
    push({ type:'live_status', status:'working', msg:`Account: ${accountId} — getting WebSocket token…` });

    const otpRes = await fetch(`https://api.derivws.com/trading/v1/options/accounts/${accountId}/otp`, { method:'POST', headers:{ 'Authorization':`Bearer ${accessToken}`, 'Deriv-App-ID':APP_ID_LIVE } });
    const otpData = await otpRes.json();
    if (!otpData.data?.url) throw new Error('OTP endpoint did not return a WebSocket URL');

    acct.liveAccessToken = accessToken; acct.liveAccountId = accountId;
    if (chosen.currency) acct.currency = chosen.currency; // set immediately — don't wait on the balance push round-trip
    if (chosen.balance != null) acct.balance = chosen.balance; // same fix, applied to balance — see connectLive()'s periodic refresh too
    push({ type:'live_status', status:'working', msg:'Connecting live WebSocket…' });
    await acct.connectLive(otpData.data.url, accountId);
    pendingLiveAuth = null;
    push({ type:'live_status', status:'ready', msg:`✅ Connected — ${accountId}` });
    push({ type:'account', data:acct.info() });
    res.json({ success:true, accountId });
  } catch (err) {
    serverLog('Account selection error', { err:err.message });
    push({ type:'live_status', status:'error', msg:`❌ ${err.message}` });
    res.status(500).json({ error: err.message });
  }
});

// ─── app routes ──────────────────────────────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.get('/api/stream', (req, res) => {
  res.set({ 'Content-Type':'text/event-stream', 'Cache-Control':'no-cache', Connection:'keep-alive' });
  res.flushHeaders(); clients.push(res);
  res.write(`data: ${JSON.stringify({ type:'init', account:acct.info(), slots:slots.map(s=>s.snap()), appId:APP_ID_LIVE })}\n\n`);
  req.on('close', () => { const i = clients.indexOf(res); if (i !== -1) clients.splice(i,1); });
});

app.get('/api/account', (req, res) => res.json(acct.info()));

app.post('/api/token', async (req, res) => {
  const { token } = req.body; if (!token) return res.status(400).json({ error:'No token' });
  acct.setToken(token);
  try { await acct.connect(); res.json({ success:true, ...acct.info() }); }
  catch (e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/symbols', async (req, res) => {
  try {
    if (!symsCache) {
      const raw = await fetchSymbols();
      symsCache = raw.filter(s => s.market==='synthetic_index' && (/^1HZ/.test(s.symbol) || /\(1s\)/i.test(s.display_name)))
        .map(s => ({ symbol:s.symbol, display_name:s.display_name }))
        .sort((a,b) => a.display_name.localeCompare(b.display_name));
    }
    res.json(symsCache);
  } catch (e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/slots', (req, res) => res.json(slots.map(s=>s.snap())));
app.get('/api/slot/:id', (req, res) => { const s=slotOf.get(req.params.id); s ? res.json(s.snap()) : res.status(404).json({ error:'Not found' }); });
app.get('/api/slot/:id/logs', (req, res) => { const s=slotOf.get(req.params.id); s ? res.json(s.logs) : res.status(404).json({ error:'Not found' }); });
app.get('/api/slot/:id/history', (req, res) => { const s=slotOf.get(req.params.id); s ? res.json(s.history) : res.status(404).json({ error:'Not found' }); });

app.post('/api/slot/:id/start', async (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); res.json(await startSlot(s)); });
app.post('/api/slot/:id/stop', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); res.json(stopSlot(s)); });
app.post('/api/slot/:id/cfg', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); s.cfg={ ...s.cfg, ...req.body }; persistNow(); res.json(s.cfg); });
app.post('/api/slot/:id/clearlogs', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); s.clearLog(); res.json({ success:true }); });
app.post('/api/slot/:id/clearstats', (req, res) => { const s=slotOf.get(req.params.id); if (!s) return res.status(404).json({ error:'Not found' }); s.clearStats(); res.json({ success:true }); });
app.post('/api/stopall', (req, res) => { slots.forEach(s => { if (s.running) stopSlot(s); }); res.json({ success:true }); });

// ─── DataEngine routes (added) ─────────────────────────────────────────────────
// Historical downloads are ALWAYS user-triggered from the dashboard — nothing
// here runs automatically. Live tick collection (started above) is the only
// part of the DataEngine that runs continuously in the background.
function toEpochSeconds(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return Math.floor(v);
  const n = Number(v);
  if (!isNaN(n) && String(v).trim() !== '') return Math.floor(n);
  const parsed = Date.parse(v);
  return isNaN(parsed) ? null : Math.floor(parsed / 1000);
}

const UNIT_SECONDS = { days: 86400, weeks: 7 * 86400, months: 30 * 86400 }; // ASSUMPTION: a "month" is treated as 30 days for duration math — no calendar-month ambiguity.

app.get('/api/data/markets', (req, res) => res.json(DATAENGINE_MARKETS));

app.get('/api/data/live/status', (req, res) => {
  res.json(DATAENGINE_MARKETS.map(symbol => ({ symbol, running: liveTickCollector.isRunning(symbol) })));
});

app.post('/api/data/live/:symbol/pause', (req, res) => {
  const { symbol } = req.params;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  const stopped = liveTickCollector.stopSymbol(symbol);
  res.json({ symbol, running: false, changed: stopped });
});

app.post('/api/data/live/:symbol/resume', (req, res) => {
  const { symbol } = req.params;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  liveTickCollector.startSymbol(symbol);
  res.json({ symbol, running: true });
});

app.get('/api/data/stats', (req, res) => {
  try { res.json(dataDb.getStats(DATAENGINE_MARKETS)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/data/integrity', (req, res) => {
  try { res.json(dataDb.verifyIntegrity(DATAENGINE_MARKETS)); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/data/downloads', (req, res) => res.json(historyDownloader.listJobs()));

app.post('/api/data/download', (req, res) => {
  const { symbol, amount, unit } = req.body;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  const unitSeconds = UNIT_SECONDS[unit];
  const amt = Number(amount);
  if (!unitSeconds || !amt || amt <= 0) return res.status(400).json({ error: 'Invalid amount/unit — unit must be days, weeks, or months' });
  const seconds = Math.round(amt * unitSeconds);
  try {
    const job = historyDownloader.startDownload(symbol, seconds, (progress) => push({ type: 'download_progress', data: progress }));
    res.json(job);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/data/download/:jobId/pause', (req, res) => {
  const job = historyDownloader.pauseJob(req.params.jobId);
  job ? res.json(job) : res.status(404).json({ error: 'Job not found or not running' });
});

app.post('/api/data/download/:jobId/resume', (req, res) => {
  const job = historyDownloader.resumeJob(req.params.jobId, (progress) => push({ type: 'download_progress', data: progress }));
  job ? res.json(job) : res.status(404).json({ error: 'Job not found or not paused' });
});

app.post('/api/data/download/:jobId/cancel', (req, res) => {
  const job = historyDownloader.cancelJob(req.params.jobId);
  job ? res.json(job) : res.status(404).json({ error: 'Job not found' });
});

app.get('/api/data/search', (req, res) => {
  const symbol = req.query.symbol;
  const start = toEpochSeconds(req.query.start);
  const end = toEpochSeconds(req.query.end);
  const limit = Math.min(5000, Number(req.query.limit) || 1000);
  if (!DATAENGINE_MARKETS.includes(symbol) || start == null || end == null) {
    return res.status(400).json({ error: 'symbol, start, and end are required (start/end may be epoch seconds or ISO date strings)' });
  }
  try {
    const ticks = dataDb.getTicksBetween(symbol, start, end).slice(0, limit);
    dataDb.rebuildCandlesForRange(symbol, start, end); // guarantee correctness — cheap, same range already being scanned above
    const candles = dataDb.getCandlesBetween(symbol, start, end).slice(0, limit);
    res.json({ symbol, start, end, ticks, candles, ticksTruncated: ticks.length >= limit, candlesTruncated: candles.length >= limit });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/data/range', (req, res) => {
  const { symbol, start, end } = req.body;
  const s = toEpochSeconds(start), e2 = toEpochSeconds(end);
  if (!DATAENGINE_MARKETS.includes(symbol) || s == null || e2 == null || s > e2) {
    return res.status(400).json({ error: 'symbol, start, and end are required and start must be <= end' });
  }
  try {
    const deletedTicks = dataDb.deleteTicksBetween(symbol, s, e2);
    const deletedCandles = dataDb.deleteCandlesBetween(symbol, s, e2);
    res.json({ success: true, deletedTicks, deletedCandles });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── Backtest routes (added) ────────────────────────────────────────────────────
// Backtest NEVER requires OAuth/a live connection — it only ever reads from the
// local SQLite tick/candle store already downloaded via the Data Engine panel.
app.get('/api/backtest/list', (req, res) => {
  res.json(Array.from(backtestRunners.values()).map(r => r.publicState()));
});

app.get('/api/backtest/:id', (req, res) => {
  const runner = backtestRunners.get(req.params.id);
  runner ? res.json(runner.publicState()) : res.status(404).json({ error: 'Backtest run not found' });
});

app.post('/api/backtest/start', (req, res) => {
  const { slotId, symbol, start, end, speed } = req.body;
  if (!DATAENGINE_MARKETS.includes(symbol)) return res.status(400).json({ error: 'Unsupported symbol' });
  const baseSlot = slotOf.get(slotId);
  if (!baseSlot) return res.status(400).json({ error: 'Unknown slotId — must be an existing slot to clone its strategy config from' });

  const fromEpoch = toEpochSeconds(start);
  const toEpoch = toEpochSeconds(end);
  if (fromEpoch == null || toEpoch == null || fromEpoch >= toEpoch) {
    return res.status(400).json({ error: 'Invalid start/end range' });
  }

  const oldest = dataDb.getOldestTick(symbol);
  if (!oldest || oldest.epoch > fromEpoch) {
    return res.status(400).json({ error: `No downloaded history reaches back to the requested start date for ${symbol}. Download more history first.` });
  }

  const speedVal = speed === 'max' ? 'max' : Math.max(1, Number(speed) || 1);
  const id = `bt${backtestIdCounter++}`;
  const runner = new BacktestRunner(id, JSON.parse(JSON.stringify(baseSlot.cfg)), symbol, fromEpoch, toEpoch, speedVal);
  backtestRunners.set(id, runner);
  runner.run().catch(err => { runner.status = 'error'; runner.error = err.message; });
  res.json(runner.publicState());
});

app.post('/api/backtest/:id/pause', (req, res) => {
  const runner = backtestRunners.get(req.params.id);
  if (!runner) return res.status(404).json({ error: 'Backtest run not found' });
  runner.pause();
  res.json(runner.publicState());
});

app.post('/api/backtest/:id/resume', (req, res) => {
  const runner = backtestRunners.get(req.params.id);
  if (!runner) return res.status(404).json({ error: 'Backtest run not found' });
  runner.resume();
  res.json(runner.publicState());
});

app.post('/api/backtest/:id/cancel', (req, res) => {
  const runner = backtestRunners.get(req.params.id);
  if (!runner) return res.status(404).json({ error: 'Backtest run not found' });
  runner.cancel();
  res.json(runner.publicState());
});

app.post('/api/backtest/:id/speed', (req, res) => {
  const runner = backtestRunners.get(req.params.id);
  if (!runner) return res.status(404).json({ error: 'Backtest run not found' });
  const { speed } = req.body;
  runner.speed = speed === 'max' ? 'max' : Math.max(1, Number(speed) || 1);
  res.json(runner.publicState());
});

loadState();
app.listen(PORT, () => serverLog(`Zone Touch 10-in-1 v6 running on port ${PORT}`));


