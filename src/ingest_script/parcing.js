// File: drive_month_xlsx_only_shared_drive.gs
//
// PURPOSE
// =======
// Robust Google Apps Script to parse very large, pipe-delimited CSV audit logs that live in a
// Shared Drive, normalize them, and export the results **only as XLSX files** in deterministic,
// bounded-size parts. The script supports two modes:
//
//   v1 (month/range runner):
//     - Manually process a single month or a contiguous range of months.
//     - Streams rows into temporary Google Sheets in bounded blocks, exports each to XLSX,
//       then deletes the temporary Sheet to avoid Google Sheets cell limits.
//     - Fully resumable via Script Properties + a time-based trigger.
//
//   v2 (folder-wide runner):
//     - Queues every CSV in the folder and processes newest first.
//     - Adds a persistent “bad-lines” sink for unparseable rows and a live “execution log” workbook.
//     - Self-heals via a watchdog trigger that re-arms the resume trigger if needed.
//     - Idempotent per-part export: if a part’s XLSX already exists, that part is skipped.
//
// KEY DESIGN CONSTRAINTS
// ======================
// • Shared Drive compatible (uses DriveApp with supportsAllDrives where needed).
// • No single Google Sheet can exceed Google’s cell limits; all writing is chunked.
// • Export target is strictly XLSX; no Google Sheets kept except the logbooks and transient temps.
// • Resumability: job and queue states stored in Script Properties.
// • Idempotency: v2 will not re-create an XLSX part if it already exists.
// • Robustness: converts CSV via Drive API first; falls back to ranged downloads if too large.
//
// FOLDER STRUCTURE (under DRIVE_FOLDER_ID)
// =======================================
// • CSVs are read directly from the parent folder.
// • v1 outputs XLSX parts into OUTPUT_SUBFOLDER.
// • v2 outputs:
//     - XLSX parts into OUTPUT_SUBFOLDER_V2
//     - bad/unprocessed lines into BADLINES_SUBFOLDER_V2 (rolling workbooks)
//     - execution logbook into LOGS_SUBFOLDER_V2 (sheets: 'runs', 'progress')
//
// TRIGGERS
// ========
// • v1: scheduleResume_() creates a time-based trigger to call resume_current_job().
// • v2: scheduleResumeAt_() creates a time-based trigger to call resume_all_v2_safe().
// • Watchdog: install_watchdog_v2() installs watchdog_v2() every 10 minutes.
//
// PARSER OVERVIEW
// ===============
// • Input lines are pipe-delimited with CSV-like quoting and "" escaping.
// • Records are normalized to exactly 9 base columns, then enriched with derived fields:
//   recipe_id, recipe_name, material_name, material_id, name1, name2, username, action,
//   session_start, session_end, session_duration.
// • Session pairing: “checked out recipe …” starts a session; “checked in … / discarded checkout /
//   updated recipe / auto-check in” ends a session. Durations are bounded for outliers.
//
// =================================================================================================
// ======================================= CONFIG & CONSTANTS =======================================
// =================================================================================================

// ========= REQUIRED CONFIG (v1) =========
const DRIVE_FOLDER_ID = '1GDJifQIErKsKwUuQta2ILXmVyvOSiEk6';
const OUTPUT_SUBFOLDER = 'parced';

// Auto-splitting rules (constants, not defaults)
const AUTO_SPLIT_THRESHOLD_ROWS = 100000; // split if data rows > this
const AUTO_SPLIT_CHUNK_ROWS     = 100000; // data rows per XLSX part (each part also has the header)

// CSV download chunk size (bytes) for huge CSVs
const DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024; // 8 MB

// Apps Script time guards
const WRITE_CHUNK_ROWS        = 10000;   // write to temp Sheet in 10k-row blocks
const MAX_WRITE_ROWS_PER_RUN  = 300000;  // max data rows to export per execution

// Google Sheets hard cap
const SHEET_CELL_LIMIT = 10000000;       // absolute worksheet cell limit

// Auto-resume
const ENABLE_AUTORETRY   = true;
const AUTORETRY_MINUTES  = 1;

// Job state key (Script Properties)
const JOB_KEY = 'LOGS_XLSX_JOB_STREAM_V1';

// ========= V2 CONFIG (folder-wide) =========
const OUTPUT_SUBFOLDER_V2   = 'parced2';
const BADLINES_SUBFOLDER_V2 = 'parced2_unprocessed';
const LOGS_SUBFOLDER_V2     = 'parced2_logs';
const LOG_BOOK_TITLE_V2     = 'Parse v2 — Execution Log';
const BADLINES_MAX_ROWS     = 200000;      // rollover threshold (rows per bad-lines GSheet)
const BETWEEN_PARTS_DELAY_MIN  = 1;        // schedule delay between parts (same file)
const BETWEEN_FILES_DELAY_MIN  = 2;        // schedule delay between files

// V2 job keys
const JOB_KEY_V2       = 'LOGS_XLSX_JOB_STREAM_V2';
const JOB_QUEUE_KEY_V2 = 'LOGS_XLSX_JOB_QUEUE_V2';

// ========= INTERNAL (used by parser) =========
var GLOBAL_CURRENT_FILE_NAME = '';  // set by v2 processor to stamp bad-lines

// ---------- Time formatting (CET/CEST) ----------
/**
 * Formats a Date into CET/CEST string "yyyy-MM-dd HH:mm:ss z".
 * Used for logs so all timestamps are local to Europe/Zurich.
 */
function formatCET(d) {
  return Utilities.formatDate(d, 'Europe/Zurich', "yyyy-MM-dd HH:mm:ss z");
}

// =================================================================================================
// ======================================= PUBLIC: v1 RUNNERS =======================================
// =================================================================================================

/**
 * v1 entry: process a single month (YYYY, M) by delegating to run_range().
 * @param {number} year
 * @param {number} month 1–12
 */
function run_month(year, month) {
  const ym = String(year) + '-' + ('0' + Number(month)).slice(-2);
  run_range(ym, ym);
}

// Convenience wrappers for fixed months (handy to use as trigger handlers)
function run_2021_08(){ run_month(2021,8); }
function run_2021_09(){ run_month(2021,9); }
function run_2021_10(){ run_month(2021,10); }
function run_2021_11(){ run_month(2021,11); }
function run_2021_12(){ run_month(2021,12); }

function run_2022_01(){ run_month(2022,1); }
function run_2022_02(){ run_month(2022,2); }
function run_2022_03(){ run_month(2022,3); }
function run_2022_04(){ run_month(2022,4); }
function run_2022_05(){ run_month(2022,5); }
function run_2022_06(){ run_month(2022,6); }
function run_2022_07(){ run_month(2022,7); }
function run_2022_08(){ run_month(2022,8); }
function run_2022_09(){ run_month(2022,9); }
function run_2022_10(){ run_month(2022,10); }
function run_2022_11(){ run_month(2022,11); }
function run_2022_12(){ run_month(2022,12); }

function run_2023_01(){ run_month(2023,1); }
function run_2023_02(){ run_month(2023,2); }
function run_2023_03(){ run_month(2023,3); }
function run_2023_04(){ run_month(2023,4); }
function run_2023_05(){ run_month(2023,5); }
function run_2023_06(){ run_month(2023,6); }
function run_2023_07(){ run_month(2023,7); }
function run_2023_08(){ run_month(2023,8); }
function run_2023_09(){ run_month(2023,9); }
function run_2023_10(){ run_month(2023,10); }
function run_2023_11(){ run_month(2023,11); }
function run_2023_12(){ run_month(2023,12); }

function run_2024_01(){ run_month(2024,1); }
function run_2024_02(){ run_month(2024,2); }
function run_2024_03(){ run_month(2024,3); }
function run_2024_04(){ run_month(2024,4); }
function run_2024_05(){ run_month(2024,5); }
function run_2024_06(){ run_month(2024,6); }
function run_2024_07(){ run_month(2024,7); }
function run_2024_08(){ run_month(2024,8); }
function run_2024_09(){ run_month(2024,9); }
function run_2024_10(){ run_month(2024,10); }
function run_2024_11(){ run_month(2024,11); }
function run_2024_12(){ run_month(2024,12); }

function run_2025_01(){ run_month(2025,1); }
function run_2025_02(){ run_month(2025,2); }
function run_2025_03(){ run_month(2025,3); }
function run_2025_04(){ run_month(2025,4); }
function run_2025_05(){ run_month(2025,5); }
function run_2025_06(){ run_month(2025,6); }
function run_2025_07(){ run_month(2025,7); }
function run_2025_08(){ run_month(2025,8); }

/**
 * v1 manual resume entry point. Used by v1’s time-based trigger.
 * Loads job from Script Properties and calls processJobStreaming_().
 */
function resume_current_job() {
  const job = loadJob_();
  if (!job) { Logger.log('No active job.'); return; }
  Logger.log('Resuming job for ' + job.outputBase);
  processJobStreaming_(job);
}

// =================================================================================================
// ======================================= PUBLIC: v2 RUNNERS =======================================
// =================================================================================================

/**
 * Attempts to parse a year-month from a filename for sorting priority.
 * Accepts "Month YYYY" or "YYYY[-_ ]MM" variants. Case-insensitive months.
 * @param {string} name CSV filename
 * @return {{y:number,m:number}|null}
 */
function parseYearMonthFromName_(name) {
  const MONTHS = {january:1,february:2,march:3,april:4,may:5,june:6,
                  july:7,august:8,september:9,october:10,november:11,december:12};
  const base = name.replace(/\.csv$/i,'');
  let m = base.match(/(january|february|march|april|may|june|july|august|september|october|november|december)\s*([0-9]{4})/i);
  if (m) return { y: Number(m[2]), m: MONTHS[m[1].toLowerCase()] };
  m = base.match(/([0-9]{4})[-_ ]?([0-9]{2})\b/);
  if (m) return { y: Number(m[1]), m: Number(m[2]) };
  return null;
}

/**
 * v2 entry: builds a queue of all CSV files in the folder and starts processing newest first.
 * Stores the queue in Script Properties, clears any stale v2 job, and calls resume_all_v2().
 */
function run_all_v2() {
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const items = [];
  const it = parent.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    const name = f.getName();
    if (!/\.csv$/i.test(name)) continue;
    const ym = parseYearMonthFromName_(name);
    items.push({id:f.getId(), name:name, ym:ym});
  }
  // Newest first: by year, then month, then name desc. Unknown dates go last.
  items.sort(function(a,b){
    if (a.ym && b.ym) {
      if (a.ym.y !== b.ym.y) return b.ym.y - a.ym.y;
      if (a.ym.m !== b.ym.m) return b.ym.m - a.ym.m;
      return b.name.localeCompare(a.name);
    } else if (a.ym && !b.ym) return -1;
    else if (!a.ym && b.ym) return 1;
    return b.name.localeCompare(a.name);
  });

  PropertiesService.getScriptProperties().setProperty(JOB_QUEUE_KEY_V2, JSON.stringify({idx:0, items:items}));
  PropertiesService.getScriptProperties().deleteProperty(JOB_KEY_V2);
  resume_all_v2();
}

/**
 * v2 main loop (resumable). Ensures output/log/badlines folders exist,
 * acquires current queue item or creates a new job, runs processJobStreamingV2_(),
 * then advances to the next file with a delay. Writes live logs to the logbook.
 * Safe to call manually or via the resume_all_v2_safe() trigger.
 */
function resume_all_v2() {
  // heartbeat so we see every wake-up
  logRun_('resume_tick', '', '', '', '', 'ok', formatCET(new Date()));

  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  ensureFolder(parent, OUTPUT_SUBFOLDER_V2);
  ensureFolder(parent, BADLINES_SUBFOLDER_V2);
  ensureFolder(parent, LOGS_SUBFOLDER_V2);

  const rawQ = PropertiesService.getScriptProperties().getProperty(JOB_QUEUE_KEY_V2);
  if (!rawQ) { Logger.log('No queue.'); return; }
  const q = JSON.parse(rawQ);
  if (q.idx >= q.items.length) {
    Logger.log('Queue finished.');
    logProgress_('', '', '', '', 'idle','all complete');
    return;
  }

  let job = loadJobV2_();
  if (!job) {
    const cur = q.items[q.idx];
    precleanChunkArtifacts_(ensureFolder(parent, OUTPUT_SUBFOLDER_V2), makeOutputBaseV2_(cur.name));
    job = {
      fileId: cur.id,
      fileName: cur.name,
      outputBase: makeOutputBaseV2_(cur.name),
      totalRows:0, totalCols:0, dataCursor:0, partIndex:1
    };
    saveJobV2_(job);
  }

  processJobStreamingV2_(job);

  // If file finished, advance queue and schedule next file
  if (!loadJobV2_()) {
    q.idx += 1;
    PropertiesService.getScriptProperties().setProperty(JOB_QUEUE_KEY_V2, JSON.stringify(q));
    if (q.idx < q.items.length) {
      scheduleResumeAt_(BETWEEN_FILES_DELAY_MIN, 'resume_all_v2_safe');
      const nextAt = new Date(Date.now()+BETWEEN_FILES_DELAY_MIN*60*1000);
      logRun_('file_done', job.fileName, '', '', job.totalRows, 'ok', formatCET(nextAt));
      logProgress_(q.items[q.idx].name, 0, 0, 0, 'queued next file', '');
    } else {
      logRun_('all_done','', '', '', '', 'ok','');
      logProgress_('', '', '', '', 'idle','all complete');
    }
  }
}

// =================================================================================================
// ======================================= BACKFILL HELPERS (v1) ====================================
// =================================================================================================

/**
 * Scans OUTPUT_SUBFOLDER for legacy ".parced" Google Sheets and exports each to XLSX
 * if an XLSX doesn’t already exist. Useful when migrating older artifacts to XLSX-only.
 * Requires Advanced Drive.
 */
function export_all_existing_parced_to_xlsx() {
  assertAdvancedDrive_();
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const outFolder = ensureSubfolder(parent, OUTPUT_SUBFOLDER);

  const files = outFolder.getFiles();
  let made = 0, skipped = 0;

  while (files.hasNext()) {
    const f = files.next();
    const name = f.getName();
    if (!name.endsWith('.parced')) continue;
    if (f.getMimeType() !== MimeType.GOOGLE_SHEETS) { skipped++; continue; }

    const xlsxName = name + '.xlsx';
    const already = outFolder.getFilesByName(xlsxName);
    if (already.hasNext()) { skipped++; continue; }

    exportSpreadsheetToXlsx_(f.getId(), outFolder, xlsxName);
    Logger.log('Exported XLSX: ' + xlsxName);
    made++;
  }
  Logger.log('Done. Created XLSX: ' + made + ', skipped: ' + skipped);
}

/**
 * For each legacy ".parced" Google Sheet, exports in chunked XLSX parts of `chunkSize` rows.
 * Ensures temp artifacts are cleaned per part.
 * @param {number} chunkSize number of data rows per part
 */
function export_all_existing_parced_to_xlsx_in_chunks(chunkSize) {
  assertAdvancedDrive_();
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const outFolder = ensureSubfolder(parent, OUTPUT_SUBFOLDER);

  const files = outFolder.getFiles();
  let made = 0;

  while (files.hasNext()) {
    const f = files.next();
    const name = f.getName();
    if (!name.endsWith('.parced')) continue;
    if (f.getMimeType() !== MimeType.GOOGLE_SHEETS) continue;

    const base = name;
    precleanChunkArtifacts_(outFolder, base);
    exportSheetToXlsxInChunks_(f.getId(), outFolder, base, chunkSize);
    Logger.log('Exported chunked XLSX: ' + base + '__partNN.xlsx');
    made++;
  }
  Logger.log('Done. Chunk-exported sheets: ' + made);
}

// =================================================================================================
/**
 * v1 core range runner: finds CSVs whose inferred YYYY-MM are between [fromYMonth, toYMonth],
 * sorted ascending, then streams each into XLSX parts via processJobStreaming_().
 * Resumable mid-file via Script Properties.
 * @param {string} fromYMonth "YYYY-MM"
 * @param {string} toYMonth   "YYYY-MM"
 */
function run_range(fromYMonth, toYMonth) {
  const t0 = new Date();
  Logger.log('Range start: ' + fromYMonth + ' → ' + toYMonth);

  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const outFolder = ensureSubfolder(parent, OUTPUT_SUBFOLDER);

  function ymKey(y, m) { return y + '-' + ('0' + m).slice(-2); }

  const MONTHS = {january:'01',february:'02',march:'03',april:'04',may:'05',june:'06',
                  july:'07',august:'08',september:'09',october:'10',november:'11',december:'12'};
  function parseYM(name){
    const base = name.replace(/\.csv$/i,'');
    let m = base.match(/(january|february|march|april|may|june|july|august|september|october|november|december)\s*([0-9]{4})/i);
    if (m) return { y: m[2], M: MONTHS[m[1].toLowerCase()] };
    m = base.match(/([0-9]{4})[-_ ]?([0-9]{2})\b/);
    if (m) return { y: m[1], M: m[2] };
    return null;
  }

  const files = [];
  const it = parent.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    const name = f.getName();
    if (!/\.csv$/i.test(name)) continue;
    const ym = parseYM(name);
    if (!ym) continue;
    const key = ymKey(ym.y, ym.M);
    if (key < fromYMonth || key > toYMonth) continue;
    files.push({ f: f, name: name, key: key });
  }
  files.sort(function(a,b){ return a.key < b.key ? -1 : a.key > b.key ? 1 : 0; });

  Logger.log('Files in range: ' + files.length);

  for (let i = 0; i < files.length; i++) {
    const f      = files[i].f;
    const name   = files[i].name;
    const ym     = files[i].key;
    const base   = name.replace(/\.csv$/i, '');
    const outputBase = ym + '_' + base + '.parced';

    Logger.log('Processing: ' + name + ' → ' + outputBase + ' (' + f.getId() + ')');

    // If another job is in progress (maybe same file), process that first
    let job = loadJob_();
    if (!job || job.outputBase !== outputBase) {
      // Clean any stale artifacts from prior runs
      precleanChunkArtifacts_(outFolder, outputBase);

      job = {
        fileId: f.getId(),
        outputBase: outputBase,
        // streaming state
        totalRows: 0,        // table rows incl header
        totalCols: 0,        // columns
        dataCursor: 0,       // how many DATA rows (excl header) already exported
        partIndex: 1         // next __partNN to write
      };
      saveJob_(job);
    }

    processJobStreaming_(job);

    if (loadJob_()) { // not finished; stop loop and let resume handle it
      Logger.log('Job not finished yet; stopping this pass.');
      break;
    }
  }

  const t1 = new Date();
  Logger.log('Range handler finished. Elapsed: ' + Math.round((t1 - t0)/1000) + 's');
}

// =================================================================================================
// ============================= STREAMING PROCESSOR (v1, XLSX-only) ===============================
// =================================================================================================

/**
 * v1 streaming writer for a single CSV file. Converts CSV→table, then repeatedly:
 * - creates a temp Sheet for the next part
 * - writes header + a bounded number of data rows in WRITE_CHUNK_ROWS blocks
 * - exports to XLSX
 * - deletes temp artifacts
 * Persists job state after each part for resumability.
 * @param {{fileId:string,outputBase:string,totalRows:number,totalCols:number,dataCursor:number,partIndex:number}} job
 */
function processJobStreaming_(job) {
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const outFolder = ensureSubfolder(parent, OUTPUT_SUBFOLDER);

  const file = DriveApp.getFileById(job.fileId);
  const lines = readCsvLinesReliably_(file);
  if (lines.length === 0) { Logger.log('Empty file, skip.'); clearJob_(); return; }

  Logger.log('Input lines (incl header): ' + lines.length);

  const table = parseLinesToTable(lines, /*strictBadSink*/null);
  Logger.log('Parsed rows (incl header): ' + table.length);

  if (!job.totalRows || !job.totalCols) {
    job.totalRows = table.length;
    job.totalCols = table[0].length;
  }
  const header = [table[0]];
  const totalDataRows = job.totalRows - 1;

  const rowsAllowed = Math.floor(SHEET_CELL_LIMIT / job.totalCols);
  const dataRowsAllowedByCellCap = Math.max(1, rowsAllowed - 1);
  const dataRowsAllowedByPolicy = AUTO_SPLIT_CHUNK_ROWS;
  let remainingThisRun = MAX_WRITE_ROWS_PER_RUN;

  while (job.dataCursor < totalDataRows && remainingThisRun > 0) {
    const dataLeft = totalDataRows - job.dataCursor;
    const partDataRows = Math.min(dataLeft, remainingThisRun, dataRowsAllowedByPolicy, dataRowsAllowedByCellCap);

    const tempName = job.outputBase + '__part' + String(job.partIndex).padStart(2, '0');
    precleanChunkArtifacts_(outFolder, tempName);

    const tmp = SpreadsheetApp.create(tempName);
    moveFileToFolder(DriveApp.getFileById(tmp.getId()), outFolder);
    const sh = tmp.getSheets()[0];
    sh.setName('Parsed');

    let written = 0;
    while (written < partDataRows) {
      const size = Math.min(WRITE_CHUNK_ROWS, partDataRows - written);
      const startIdxInTable = 1 + job.dataCursor + written;
      const slice = table.slice(startIdxInTable, startIdxInTable + size);
      sh.getRange(2 + written, 1, slice.length, job.totalCols).setValues(slice);
      written += size;
      Logger.log('Part ' + job.partIndex + ' — wrote ' + written + ' / ' + partDataRows + ' rows');
      SpreadsheetApp.flush();
    }

    exportSpreadsheetToXlsx_(tmp.getId(), outFolder, tempName + '.xlsx');
    Logger.log('Exported XLSX: ' + tempName + '.xlsx');

    trashFile_(tmp.getId());
    forceTrashIfExistsByName_(outFolder, tempName);
    Logger.log('Trashed temp Sheet: ' + tempName);

    job.dataCursor += partDataRows;
    job.partIndex += 1;
    remainingThisRun -= partDataRows;
    saveJob_(job);
  }

  if (job.dataCursor >= totalDataRows) {
    // Finished all parts. Single-file XLSX export disabled.
    clearJob_();
    Logger.log('All parts completed for ' + job.outputBase);
    return;
  }

  if (ENABLE_AUTORETRY) {
    scheduleResume_();
    Logger.log('Scheduled auto-resume in ' + AUTORETRY_MINUTES + ' minute(s).');
  } else {
    Logger.log('Partial stream saved. Run "resume_current_job" to continue.');
  }
}

// =================================================================================================
// ==================================== HELPERS (folders, IO) ======================================
// =================================================================================================

/**
 * Get or create a subfolder by name under a parent folder.
 * @param {GoogleAppsScript.Drive.Folder} parent
 * @param {string} name
 * @return {GoogleAppsScript.Drive.Folder}
 */
function ensureSubfolder(parent, name) {
  const it = parent.getFoldersByName(name);
  return it.hasNext() ? it.next() : parent.createFolder(name);
}
/** Alias kept for readability parity. */
function ensureFolder(parent, name) { return ensureSubfolder(parent, name); }

/**
 * Moves a file into the given folder, removing any other parents.
 * This is Shared Drive safe (DriveApp).
 */
function moveFileToFolder(file, folder) {
  folder.addFile(file);
  const parents = file.getParents();
  while (parents.hasNext()) {
    const p = parents.next();
    if (p.getId() !== folder.getId()) p.removeFile(file);
  }
}

/**
 * Normalizes newlines and splits to non-empty lines.
 * Note: not used in main flow (we rely on Drive conversion / ranged fetch).
 */
function splitLines_(s) {
  return s.replace(/\r\n/g, '\n').replace(/\r/g, '\n')
          .split('\n').filter(function (x) { return x !== '' });
}

/**
 * Ensures the Advanced Drive service is enabled for this script project.
 * Throws a descriptive error if not enabled.
 */
function assertAdvancedDrive_() {
  if (typeof Drive === 'undefined' || !Drive.Files) {
    throw new Error('Advanced Drive Service not enabled. In Script Editor: Services → “+” → add Drive API.');
  }
}

/**
 * Soft-delete a file by ID with retries. Uses DriveApp.setTrashed(true),
 * which works in Shared Drives. Swallows errors after max retries, but logs.
 */
function trashFile_(fileId) {
  const maxTries = 3;
  let wait = 300;
  for (let i = 1; i <= maxTries; i++) {
    try {
      DriveApp.getFileById(fileId).setTrashed(true);
      return;
    } catch (e) {
      if (i === maxTries) {
        Logger.log('Trash failed for ' + fileId + ': ' + e);
        return;
      }
      Utilities.sleep(wait);
      wait *= 2;
    }
  }
}

/**
 * Force-delete any file(s) in the folder that exactly match the given name.
 * Used to clean temp artifacts that may have been left if an execution aborted.
 */
function forceTrashIfExistsByName_(folder, exactName) {
  const it = folder.getFilesByName(exactName);
  while (it.hasNext()) {
    const f = it.next();
    try { f.setTrashed(true); }
    catch (e) { Logger.log('Force-trash failed for ' + exactName + ': ' + e); }
  }
}

/**
 * Pre-cleans any temporary Google Sheets whose names start with outBaseName+"__part".
 * This avoids collisions before a new export attempt.
 */
function precleanChunkArtifacts_(folder, outBaseName) {
  forceTrashIfExistsByName_(folder, outBaseName);
  const it = folder.getFiles();
  while (it.hasNext()) {
    const f = it.next();
    const n = f.getName();
    if (n.indexOf(outBaseName + '__part') === 0 && f.getMimeType() === MimeType.GOOGLE_SHEETS) {
      try { f.setTrashed(true); }
      catch (e) { Logger.log('Preclean failed for ' + n + ': ' + e); }
    }
  }
}

// =================================================================================================
// ========================= CSV READING (convert-first, ranged fallback) ===========================
// =================================================================================================

/**
 * Reads a CSV (Drive file) into an array of raw text lines robustly.
 * Preferred path: Drive.Files.copy(..., convert:true) to Google Sheet, then read column A.
 * If file is too large (413) or conversion fails after retries, falls back to byte-range downloads.
 * Always cleans up the temp converted Sheet (trashed).
 * Requires Advanced Drive.
 * @param {GoogleAppsScript.Drive.File} file
 * @return {string[]} lines
 */
function readCsvLinesReliably_(file) {
  assertAdvancedDrive_();

  const mimeSheet = MimeType.GOOGLE_SHEETS;
  const title = 'TMP_IMPORT_' + file.getName() + '_' + new Date().getTime();

  const maxTries = 5;
  let delayMs = 500;
  let tmpId = null;

  for (let attempt = 1; attempt <= maxTries; attempt++) {
    try {
      const copied = Drive.Files.copy(
        { title: title, mimeType: mimeSheet },
        file.getId(),
        { convert: true, supportsAllDrives: true }
      );
      tmpId = copied.id;

      Utilities.sleep(300);

      const ss = SpreadsheetApp.openById(tmpId);
      const sh = ss.getSheets()[0];

      let last = sh.getLastRow();
      for (let i = 0; i < 5 && last === 0; i++) {
        Utilities.sleep(200);
        last = sh.getLastRow();
      }
      if (last < 1) return [];

      const vals = sh.getRange(1, 1, last, 1).getDisplayValues();
      const lines = [];
      for (let r = 0; r < vals.length; r++) {
        const v = String(vals[r][0] || '');
        if (v !== '') lines.push(v);
      }
      return lines;

    } catch (e) {
      const msg = String(e && e.message || '');
      if (msg.indexOf('Request Too Large') !== -1 || msg.indexOf('413') !== -1) {
        Logger.log('Drive convert too large; falling back to chunked download.');
        break;
      }
      if (attempt === maxTries) {
        Logger.log('Drive convert failed after retries; falling back to chunked download.');
        break;
      }
      Utilities.sleep(delayMs);
      delayMs *= 2;
    } finally {
      if (tmpId) { trashFile_(tmpId); }
      tmpId = null;
    }
  }
  return readCsvByChunkedDownload_(file.getId());
}

/**
 * Byte-range downloads a CSV file and reassembles clean lines.
 * Preserves a final carry-over between chunks to avoid splitting lines mid-byte range.
 * @param {string} fileId
 * @return {string[]} lines
 */
function readCsvByChunkedDownload_(fileId) {
  const token = ScriptApp.getOAuthToken();
  const base = 'https://www.googleapis.com/drive/v3/files/' + encodeURIComponent(fileId) + '?alt=media&supportsAllDrives=true';

  let offset = 0;
  let carry = '';
  const pieces = [];

  while (true) {
    const end = offset + DOWNLOAD_CHUNK_BYTES - 1;
    const resp = UrlFetchApp.fetch(base, {
      method: 'get',
      headers: { Authorization: 'Bearer ' + token, Range: 'bytes=' + offset + '-' + end },
      muteHttpExceptions: false
    });

    const bytes = resp.getContent();
    if (!bytes || bytes.length === 0) break;

    const chunkText = Utilities.newBlob(bytes).getDataAsString('UTF-8');
    const text = carry + chunkText;
    const parts = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');

    for (let i = 0; i < parts.length - 1; i++) pieces.push(parts[i]);
    carry = parts[parts.length - 1];

    if (bytes.length < DOWNLOAD_CHUNK_BYTES) break;
    offset += bytes.length;
  }

  if (carry !== '') pieces.push(carry);

  const lines = [];
  for (let i = 0; i < pieces.length; i++) {
    const s = pieces[i];
    if (s !== '') lines.push(s);
  }
  return lines;
}

// =================================================================================================
// ===================================== XLSX EXPORT HELPERS =======================================
// =================================================================================================

/**
 * Exports a Google Spreadsheet (by ID) to XLSX into the given folder, overwriting by name.
 * Uses Drive.Files.export first; falls back to Drive v3 REST URL if needed.
 * Requires Advanced Drive.
 * @param {string} ssId Spreadsheet ID
 * @param {GoogleAppsScript.Drive.Folder} outFolder
 * @param {string} xlsxName
 */
function exportSpreadsheetToXlsx_(ssId, outFolder, xlsxName) {
  assertAdvancedDrive_();

  const mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  let blob;

  try {
    blob = Drive.Files.export(ssId, mime).getBlob();
  } catch (e) {
    const url = 'https://www.googleapis.com/drive/v3/files/' + encodeURIComponent(ssId) +
                '/export?mimeType=' + encodeURIComponent(mime) +
                '&supportsAllDrives=true&alt=media';
    const resp = UrlFetchApp.fetch(url, {
      method: 'get',
      headers: { Authorization: 'Bearer ' + ScriptApp.getOAuthToken() },
      muteHttpExceptions: false
    });
    blob = Utilities.newBlob(resp.getContent(), mime, xlsxName);
  }

  const old = outFolder.getFilesByName(xlsxName);
  while (old.hasNext()) old.next().setTrashed(true);
  outFolder.createFile(blob).setName(xlsxName);
}

/**
 * Exports a single Google Sheet into multiple XLSX parts of `chunkSize` rows.
 * Writes header to each part, then appends a slice of rows, exports, and cleans temp.
 * @param {string} ssId
 * @param {GoogleAppsScript.Drive.Folder} outFolder
 * @param {string} outBaseName
 * @param {number} chunkSize
 */
function exportSheetToXlsxInChunks_(ssId, outFolder, outBaseName, chunkSize) {
  assertAdvancedDrive_();

  const ss = SpreadsheetApp.openById(ssId);
  const sh = ss.getSheets()[0];

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();

  if (lastRow < 2) {
    exportSpreadsheetToXlsx_(ssId, outFolder, outBaseName + '.xlsx');
    return;
  }

  const header = sh.getRange(1, 1, 1, lastCol).getValues();

  let fromRow = 2;
  let part = 1;

  while (fromRow <= lastRow) {
    const toRow = Math.min(fromRow + chunkSize - 1, lastRow);
    const rows = toRow - fromRow + 1;

    const tempName = outBaseName + '__part' + String(part).padStart(2, '0');
    precleanChunkArtifacts_(outFolder, tempName);

    const temp = SpreadsheetApp.create(tempName);
    moveFileToFolder(DriveApp.getFileById(temp.getId()), outFolder);

    const tsh = temp.getSheets()[0];
    tsh.setName('Parsed');

    tsh.getRange(1, 1, 1, lastCol).setValues(header);
    const data = sh.getRange(fromRow, 1, rows, lastCol).getValues();
    tsh.getRange(2, 1, rows, lastCol).setValues(data);
    SpreadsheetApp.flush();

    exportSpreadsheetToXlsx_(temp.getId(), outFolder, tempName + '.xlsx');

    trashFile_(temp.getId());
    forceTrashIfExistsByName_(outFolder, tempName);

    fromRow = toRow + 1;
    part += 1;
  }
}

// =================================================================================================
// ================================== PARSING & SESSION LOGIC ======================================
// =================================================================================================

/**
 * Converts raw CSV lines to a normalized 2D table with derived fields and session metadata.
 * If `badSink` is provided, malformed rows are appended to the bad-lines workbook and skipped.
 * Output schema:
 *   base 9 fields: [User ID, ID, Subseq ID, Message, Audit Time, Action, Type, Label, Version]
 *   + derived fields:
 *     recipe_id, recipe_name, material_name, material_id,
 *     name1, name2, username, action,
 *     session_start, session_end, session_duration
 * Session pairing is applied in a second pass.
 * @param {string[]} lines
 * @param {{fileName:string}|null} badSink
 * @return {Array<Array<string>>} table (first row is header)
 */
function parseLinesToTable(lines, badSink) {
  const rawHeader = lines[0].split('|').map(function (s) { return s.trim(); });
  const header = rawHeader.map(function (h) { return h.replace(/\s+/g, '_').replace(/[^\w]/g, '').toLowerCase(); });

  header.push(
    'recipe_id','recipe_name','material_name','material_id',
    'name1','name2','username','action',
    'session_start','session_end','session_duration'
  );

  const out = [header];
  const rowMeta = [];
  const startsByKey = {};
  let nextParseLog = 10000;

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;

    const raw = String(line);
    const cells0 = splitPipesQuoted(raw).map(function (s) { return String(s).trim(); });
    const norm = tryToNine_(cells0);

    if (!norm.ok) {
      if (badSink) appendBadLine_(badSink.fileName, i + 1, raw, norm.reason);
      continue;
    }

    // [User ID, ID, Subseq ID, Message, Audit Time, Action, Type, Label, Version]
    const cols = norm.cols;
    const userIdRaw = cols[0];
    const message   = cols[3];
    const auditRaw  = cols[4];
    const typeVal   = cols[6];
    const labelVal  = cols[7];

    const ts = safeParseTs_(auditRaw);

    const ridRaw = extractRecipeId_(message);
    const rid = ridRaw.split('|')[0].trim();

    const rname = /^recipe$/i.test(String(typeVal)) ? String(labelVal) : '';

    const mname = extractMaterialName_(message);
    const mid   = extractMaterialId_(message);
    const u     = splitUser_(userIdRaw);
    const action= message ? message.trim().split(/\s+/).slice(0, 4).join(' ') : '';

    const base = cols.slice();
    base.push(rid, rname, mname, mid, u.name1, u.name2, u.username, action, '', '', '');

    const outIndex = out.length;
    out.push(base);

    const key = makeKey_(u.username, userIdRaw, rid, rname);
    const meta = { key: key, isStart: isStart_(message), isEnd: isEnd_(message), isAutoEnd: isAutoCheckIn_(message), ts: ts, outIndex: outIndex };
    rowMeta.push(meta);

    if (meta.isStart) {
      if (ts) out[outIndex][out[0].length - 3] = ts.toISOString();
      if (!startsByKey[key]) startsByKey[key] = [];
      startsByKey[key].push({ rowIndex: outIndex, startTs: ts });
    }

    if (i >= nextParseLog) {
      Logger.log('Parsed ' + i + ' rows of ' + lines.length);
      nextParseLog += 10000;
    }
  }

  // PASS 2: close sessions
  for (let j = 0; j < rowMeta.length; j++) {
    const meta = rowMeta[j];
    if (!meta.isEnd || !meta.ts) continue;

    const stack = startsByKey[meta.key];
    if (!stack || stack.length === 0) continue;

    const start = stack.pop();
    const startMs = start.startTs ? start.startTs.getTime() : meta.ts.getTime();
    const rawSec = Math.max(0, Math.floor((meta.ts.getTime() - startMs) / 1000));

    let durationSec;
    if (meta.isAutoEnd) {
      const eightHours = 8 * 3600;
      durationSec = rawSec < eightHours ? rawSec : eightHours;
    } else if (rawSec > 28000) {
      const days = Math.floor(rawSec / 86400);
      durationSec = days <= 1 ? 28000 : days * 28000;
    } else {
      durationSec = rawSec;
    }

    const startRow = out[start.rowIndex];
    const colEnd = out[0].length - 2; // session_end
    const colDur = out[0].length - 1; // session_duration
    startRow[colEnd] = meta.ts.toISOString();
    startRow[colDur] = String(durationSec);
  }

  return out;
}

// ---------------------------------- Parser helpers ----------------------------------

/**
 * Splits "Firstname Lastname (username)" into structured parts.
 * Returns {name1, name2, username}. If no space, name2 is empty.
 */
function splitUser_(userIdRaw) {
  const m = String(userIdRaw).match(/^(.*?)(?:\s*\(([^)]+)\))?\s*$/);
  const namePart = m && m[1] ? m[1].trim() : String(userIdRaw).trim();
  const username = m && m[2] ? m[2].trim() : '';
  const i = namePart.indexOf(' ');
  const name1 = i === -1 ? namePart : namePart.slice(0, i).trim();
  const name2 = i === -1 ? ''       : namePart.slice(i + 1).trim();
  return { name1: name1, name2: name2, username: username };
}

/**
 * Extracts Recipe ID from message with several tolerant patterns.
 * Returns empty string if not found.
 */
function extractRecipeId_(msg) {
  const s = String(msg);
  let m = s.match(/Recipe ID:\s*'([^']+)'/i);
  if (m) return m[1].trim();
  m = s.match(/Recipe ID:\s*([^|,'"]+)/i);
  if (m) return m[1].trim();
  m = s.match(/\brecipe_[^|,\s]+/i);
  if (m) return m[0].trim();
  return '';
}

/** Extracts "Material Name = X" from message (up to next pipe). */
function extractMaterialName_(msg) {
  const m = String(msg).match(/Material Name\s*=\s*([^|]+)/i);
  return m ? m[1].trim() : '';
}
/** Extracts numeric "Material ID = 123" from message. */
function extractMaterialId_(msg) {
  const m = String(msg).match(/Material ID\s*=\s*([0-9]+)/i);
  return m ? m[1].trim() : '';
}

/** True if message denotes a session start (checkout). */
function isStart_(msg) { return /(^|\s)checked out recipe\b/i.test(String(msg)); }

/** True if message denotes a session end. Includes multiple end-like events. */
function isEnd_(msg) {
  const s = String(msg);
  return /(^|\s)checked in recipe\b/i.test(s)
      || /(^|\s)discarded checkout\b/i.test(s)
      || /(^|\s)updated recipe\b/i.test(s)
      || /(^|\s)auto-check in\b/i.test(s);
}

/** True if message denotes an automatic check-in (caps session duration). */
function isAutoCheckIn_(msg) { return /(^|\s)auto-check in\b/i.test(String(msg)); }

/**
 * Builds a session key combining user identity and recipe identity.
 * The key groups starts/ends for pairing in pass 2.
 */
function makeKey_(username, userIdRaw, recipeId, recipeName) {
  const who = username || String(userIdRaw);
  const what = recipeId || recipeName || '';
  return who + '|' + what;
}

/**
 * Splits a pipe-delimited line with CSV-like quotes and "" escaping.
 * Ensures that pipes inside quotes do not split the field.
 */
function splitPipesQuoted(line) {
  const out = []; let cur = ''; let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && i + 1 < line.length && line[i + 1] === '"') { cur += '"'; i++; }
      else { inQ = !inQ; }
    } else if (ch === '|' && !inQ) {
      out.push(cur); cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

/**
 * Coerces a row to exactly 9 fields.
 * If >9 fields, merges the extras back into the Message field.
 * If <9 fields, pads with empty strings.
 * @param {string[]} cells
 * @return {{ok:boolean, cols:string[], reason:string}}
 */
function tryToNine_(cells) {
  if (cells.length === 9) return { ok:true, cols:cells, reason:'' };
  if (cells.length > 9) {
    if (cells.length >= 10) {
      const head = cells.slice(0,3);
      const msg  = cells.slice(3, cells.length - 5).join('|');
      const tail = cells.slice(cells.length - 5);
      return { ok:true, cols: head.concat([msg]).concat(tail), reason:'COERCED' };
    }
    return { ok:false, reason:'TOO_MANY_FIELDS' };
  }
  const padded = cells.slice();
  while (padded.length < 9) padded.push('');
  return { ok:true, cols:padded, reason:'PADDED' };
}

// =================================================================================================
// ============================== JOB PERSISTENCE & AUTO-RESUME (v1) ===============================
// =================================================================================================

/** Persists v1 job to Script Properties. */
function saveJob_(job) {
  PropertiesService.getScriptProperties().setProperty(JOB_KEY, JSON.stringify(job));
}
/** Loads v1 job from Script Properties, or null. */
function loadJob_() {
  const raw = PropertiesService.getScriptProperties().getProperty(JOB_KEY);
  return raw ? JSON.parse(raw) : null;
}
/** Clears v1 job from Script Properties. */
function clearJob_() {
  PropertiesService.getScriptProperties().deleteProperty(JOB_KEY);
}
/**
 * Ensures a single time-based trigger exists for v1 resume.
 * Deletes stale triggers for the same handler before creating a fresh one.
 */
function scheduleResume_() {
  const triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    const t = triggers[i];
    if (t.getHandlerFunction && t.getHandlerFunction() === 'resume_current_job') {
      ScriptApp.deleteTrigger(t);
    }
  }
  ScriptApp.newTrigger('resume_current_job').timeBased().after(AUTORETRY_MINUTES * 60 * 1000).create();
}

// =================================================================================================
// ==================== V2 PERSISTENCE, SCHEDULING, LOGGING, BAD-LINES ==============================
// =================================================================================================

/** Persists v2 job. */
function saveJobV2_(job){ PropertiesService.getScriptProperties().setProperty(JOB_KEY_V2, JSON.stringify(job)); }
/** Loads v2 job or null. */
function loadJobV2_(){ const raw=PropertiesService.getScriptProperties().getProperty(JOB_KEY_V2); return raw?JSON.parse(raw):null; }
/** Clears v2 job. */
function clearJobV2_(){ PropertiesService.getScriptProperties().deleteProperty(JOB_KEY_V2); }

/**
 * Creates a time-based trigger to run a handler at a future time (or after delay),
 * first removing any existing triggers for that handler. Logs the action in the logbook.
 * @param {number} minutes delay in minutes
 * @param {string} handler handler function name
 */
function scheduleResumeAt_(minutes, handler) {
  const whenMs = minutes * 60 * 1000;
  const eta = new Date(Date.now() + whenMs);

  // remove stale triggers for this handler
  const triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    var t = triggers[i];
    if (t.getHandlerFunction && (t.getHandlerFunction() === handler)) {
      ScriptApp.deleteTrigger(t);
    }
  }

  try {
    ScriptApp.newTrigger(handler).timeBased().at(eta).create();
    logRun_('trigger_set', '', '', '', '', 'ok', formatCET(eta));
  } catch (e) {
    try {
      ScriptApp.newTrigger(handler).timeBased().after(whenMs).create();
      logRun_('trigger_set_after', '', '', '', '', 'ok', formatCET(eta));
    } catch (e2) {
      logRun_('trigger_error', '', '', '', '', 'error: ' + e2, '');
      throw e2;
    }
  }
}

/**
 * Returns (or creates) the execution logbook spreadsheet with sheets:
 *  - 'runs'     : append-only event log
 *  - 'progress' : single-row current status for easy monitoring
 */
function getLogBook_() {
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const logsFolder = ensureFolder(parent, LOGS_SUBFOLDER_V2);
  const it = logsFolder.getFilesByName(LOG_BOOK_TITLE_V2);
  if (it.hasNext()) return SpreadsheetApp.open(it.next());
  const ss = SpreadsheetApp.create(LOG_BOOK_TITLE_V2);
  moveFileToFolder(DriveApp.getFileById(ss.getId()), logsFolder);
  const runs = ss.getSheets()[0]; runs.setName('runs');
  runs.appendRow(['ts','phase','file','part','rows_written','total_rows','status','next_resume_at']);
  const progress = ss.insertSheet('progress');
  progress.getRange(1,1,1,7).setValues([['ts','current_file','part','cursor','total_rows','status','note']]);
  return ss;
}

/**
 * Appends a structured log entry to the 'runs' sheet of the logbook.
 */
function logRun_(phase, fileName, part, rowsWritten, totalRows, status, nextResumeAt) {
  const ss = getLogBook_();
  ss.getSheetByName('runs').appendRow([
    formatCET(new Date()), phase, fileName, String(part),
    String(rowsWritten), String(totalRows), status || '',
    nextResumeAt ? (typeof nextResumeAt === 'string' ? nextResumeAt : formatCET(nextResumeAt)) : ''
  ]);
}

/**
 * Updates the single-line 'progress' sheet with current status.
 */
function logProgress_(fileName, part, cursor, total, status, note) {
  const ss = getLogBook_();
  const sh = ss.getSheetByName('progress');
  sh.getRange(2,1,1,7).setValues([[
    formatCET(new Date()), fileName, String(part),
    String(cursor), String(total), status||'', note||''
  ]]);
}

/**
 * Returns (or creates) the "bad lines" workbook. Rolls over when max rows is reached,
 * appending a timestamp suffix to the workbook name.
 * @param {string} rollSuffix optional timestamp suffix (managed internally)
 */
function getBadLinesBook_(rollSuffix) {
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const badFolder = ensureFolder(parent, BADLINES_SUBFOLDER_V2);
  const name = 'unprocessed_lines' + (rollSuffix ? ('__' + rollSuffix) : '');
  const it = badFolder.getFilesByName(name);
  if (it.hasNext()) return SpreadsheetApp.open(it.next());
  const ss = SpreadsheetApp.create(name);
  moveFileToFolder(DriveApp.getFileById(ss.getId()), badFolder);
  const sh = ss.getSheets()[0]; sh.setName('raw');
  sh.appendRow(['file','orig_line_no','raw_line','reason']);
  return ss;
}

/**
 * Appends a single malformed input line into the current bad-lines workbook,
 * rolling over to a new workbook when BADLINES_MAX_ROWS is reached.
 */
function appendBadLine_(fileName, lineNo, raw, reason) {
  let ss = getBadLinesBook_('');
  let sh = ss.getSheets()[0];
  if (sh.getLastRow() >= BADLINES_MAX_ROWS) {
    const stamp = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
    ss = getBadLinesBook_(stamp);
    sh = ss.getSheets()[0];
  }
  sh.appendRow([fileName, String(lineNo), raw, reason || 'NOT_PROCESSED']);
}

// ---------------------------------- v2 debug helpers ----------------------------------

/** Lists current time-based triggers for quick inspection in Logs. */
function v2_list_triggers() {
  const ts = ScriptApp.getProjectTriggers().map(function(t){ return {fn:t.getHandlerFunction(), type:'timeBased'}; });
  Logger.log(JSON.stringify(ts));
}

/** Dumps current queue and job JSON from Script Properties into Logs. */
function v2_debug_status() {
  const rawQ = PropertiesService.getScriptProperties().getProperty(JOB_QUEUE_KEY_V2);
  const rawJ = PropertiesService.getScriptProperties().getProperty(JOB_KEY_V2);
  Logger.log('QUEUE: ' + (rawQ ? rawQ : 'null'));
  Logger.log('JOB:   ' + (rawJ ? rawJ : 'null'));
}

// =================================================================================================
// ============================ V2 STREAMING PROCESSOR (idempotent) ================================
// =================================================================================================

/**
 * Builds the base output name for a CSV in v2 (keeps ".parced" for familiarity).
 * @param {string} csvName
 * @return {string}
 */
function makeOutputBaseV2_(csvName) {
  const base = csvName.replace(/\.csv$/i,'');
  return base + '.parced'; // keep familiar suffix
}

/**
 * v2 streaming writer with idempotent per-part XLSX export and live logging.
 * If an XLSX part already exists, that part is skipped and the cursor advances accordingly.
 * Writes progress and run logs, and schedules next part/file via scheduleResumeAt_().
 * @param {{fileId:string,fileName:string,outputBase:string,totalRows:number,totalCols:number,dataCursor:number,partIndex:number}} job
 */
function processJobStreamingV2_(job) {
  const parent = DriveApp.getFolderById(DRIVE_FOLDER_ID);
  const outFolder = ensureFolder(parent, OUTPUT_SUBFOLDER_V2);

  const file = DriveApp.getFileById(job.fileId);
  const lines = readCsvLinesReliably_(file);
  if (lines.length === 0) {
    clearJobV2_();
    logRun_('empty', job.fileName, '', 0, 0, 'skip','');
    return;
  }

  GLOBAL_CURRENT_FILE_NAME = job.fileName; // for bad-line stamping
  Logger.log('Input lines (incl header): ' + lines.length);

  // optional visibility
  logRun_('part_start', job.fileName, job.partIndex, '', '', 'ok', formatCET(new Date()));

  const table = parseLinesToTable(lines, { fileName: job.fileName });
  Logger.log('Parsed rows (incl header): ' + table.length);

  if (!job.totalRows || !job.totalCols) {
    job.totalRows = table.length;
    job.totalCols = table[0].length;
  }
  const header = [table[0]];
  const totalDataRows = job.totalRows - 1;

  const rowsAllowed = Math.floor(SHEET_CELL_LIMIT / job.totalCols);
  const dataRowsAllowedByCellCap = Math.max(1, rowsAllowed - 1);
  const dataRowsAllowedByPolicy = AUTO_SPLIT_CHUNK_ROWS;

  let remainingThisRun = MAX_WRITE_ROWS_PER_RUN;

  while (job.dataCursor < totalDataRows && remainingThisRun > 0) {
    const dataLeft = totalDataRows - job.dataCursor;
    const partDataRows = Math.min(dataLeft, remainingThisRun, dataRowsAllowedByPolicy, dataRowsAllowedByCellCap);

    // ---- Idempotent guard: skip if this part already exists ----
    const xlsxName = job.outputBase + '__part' + String(job.partIndex).padStart(2, '0') + '.xlsx';
    if (outFolder.getFilesByName(xlsxName).hasNext()) {
      job.dataCursor += partDataRows;
      logRun_('part_skip_exists', job.fileName, job.partIndex, partDataRows, totalDataRows, 'ok', '');
      job.partIndex += 1;
      remainingThisRun -= partDataRows;
      saveJobV2_(job);
      continue;
    }
    // ------------------------------------------------------------

    const tempName = job.outputBase + '__part' + String(job.partIndex).padStart(2, '0');
    precleanChunkArtifacts_(outFolder, tempName);

    const tmp = SpreadsheetApp.create(tempName);
    moveFileToFolder(DriveApp.getFileById(tmp.getId()), outFolder);
    const sh = tmp.getSheets()[0]; sh.setName('Parsed');
    sh.getRange(1,1,1,job.totalCols).setValues(header);

    let written = 0;
    while (written < partDataRows) {
      const size = Math.min(WRITE_CHUNK_ROWS, partDataRows - written);
      const startIdx = 1 + job.dataCursor + written;
      const slice = table.slice(startIdx, startIdx + size);
      sh.getRange(2 + written, 1, slice.length, job.totalCols).setValues(slice);
      written += size;
      SpreadsheetApp.flush();
    }

    exportSpreadsheetToXlsx_(tmp.getId(), outFolder, tempName + '.xlsx');
    trashFile_(tmp.getId());
    forceTrashIfExistsByName_(outFolder, tempName);

    job.dataCursor += partDataRows;
    logRun_('part_done', job.fileName, job.partIndex, partDataRows, totalDataRows, 'ok','');
    logProgress_(job.fileName, job.partIndex, job.dataCursor, totalDataRows, 'running','');

    job.partIndex += 1;
    remainingThisRun -= partDataRows;
    saveJobV2_(job);
  }

  if (job.dataCursor >= totalDataRows) {
    clearJobV2_();
    logRun_('file_complete', job.fileName, '', job.dataCursor, totalDataRows, 'ok','');
    logProgress_(job.fileName, 0, totalDataRows, totalDataRows, 'completed','');
    return;
  }

  // schedule next part (same file)
  scheduleResumeAt_(BETWEEN_PARTS_DELAY_MIN, 'resume_all_v2_safe');
  const nextAt = new Date(Date.now()+BETWEEN_PARTS_DELAY_MIN*60*1000);
  logRun_('part_pause', job.fileName, job.partIndex, job.dataCursor, totalDataRows, 'pause', formatCET(nextAt));
  logProgress_(job.fileName, job.partIndex, job.dataCursor, totalDataRows, 'paused','next part scheduled');
}

/**
 * Parses a variety of timestamp shapes into a Date or returns null.
 * Supports ISO-like strings and "YYYY-MM-DD HH:MM:SS" (treated as UTC).
 */
function safeParseTs_(s) {
  if (!s) return null;
  var t = String(s).trim();
  var d = new Date(t);
  if (isNaN(d.getTime())) {
    // common fallback: "YYYY-MM-DD HH:MM:SS" → ISO
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(t)) {
      d = new Date(t.replace(' ', 'T') + 'Z');
    }
  }
  return isNaN(d.getTime()) ? null : d;
}

/**
 * Safe trigger entrypoint for v2: wraps resume_all_v2() with error logging and backoff re-arm.
 * Use this function name as the time-based trigger handler.
 */
function resume_all_v2_safe() {
  try {
    logRun_('resume_tick', '', '', '', '', 'ok', formatCET(new Date()));
    resume_all_v2();
  } catch (e) {
    logRun_('resume_error', '', '', '', '', 'error: ' + (e && e.message || e), formatCET(new Date()));
    scheduleResumeAt_(3, 'resume_all_v2_safe');
  }
}

// =================================================================================================
// ========================================== WATCHDOG =============================================
// =================================================================================================

/**
 * One-time installer for the v2 watchdog trigger (every 10 minutes).
 * Removes any existing watchdog_v2 triggers to avoid duplicates.
 */
function install_watchdog_v2() {
  // remove existing watchdog triggers to avoid duplicates
  const ts = ScriptApp.getProjectTriggers();
  for (let i = 0; i < ts.length; i++) {
    if (ts[i].getHandlerFunction && ts[i].getHandlerFunction() === 'watchdog_v2') {
      ScriptApp.deleteTrigger(ts[i]);
    }
  }
  ScriptApp.newTrigger('watchdog_v2').timeBased().everyMinutes(10).create();
  logRun_('watchdog_installed', '', '', '', '', 'ok', formatCET(new Date()));
}

/**
 * Runs every 10 minutes. If the v2 queue still has work and no resume trigger is present,
 * arms a resume in 1 minute and logs. Otherwise logs “ok”.
 * Errors are caught and logged, and do not throw.
 */
function watchdog_v2() {
  try {
    const rawQ = PropertiesService.getScriptProperties().getProperty(JOB_QUEUE_KEY_V2);
    const rawJ = PropertiesService.getScriptProperties().getProperty(JOB_KEY_V2);
    const queue = rawQ ? JSON.parse(rawQ) : null;
    const job   = rawJ ? JSON.parse(rawJ) : null;

    if (!queue || !queue.items || queue.idx >= queue.items.length) {
      logRun_('watchdog_ok', '', '', '', '', 'idle', formatCET(new Date()));
      return;
    }

    // check if a resume trigger exists
    let hasResume = false;
    const ts = ScriptApp.getProjectTriggers();
    for (let i = 0; i < ts.length; i++) {
      const fn = ts[i].getHandlerFunction && ts[i].getHandlerFunction();
      if (fn === 'resume_all_v2_safe') { hasResume = true; break; }
    }

    if (!hasResume) {
      scheduleResumeAt_(1, 'resume_all_v2_safe');
      logRun_('watchdog_rescheduled', (job && job.fileName) || '', (job && job.partIndex) || '', '', '', 'ok', formatCET(new Date()));
    } else {
      logRun_('watchdog_ok', (job && job.fileName) || '', (job && job.partIndex) || '', '', '', 'ok', formatCET(new Date()));
    }
  } catch (e) {
    logRun_('watchdog_error', '', '', '', '', 'error: ' + (e && e.message || e), formatCET(new Date()));
  }
}

/**
 * Utility to clean up v2-related triggers ("resume_all_v2_safe" and "watchdog_v2").
 * Keeps none by default; edit the `keep` Set to preserve specific handlers.
 * Writes an entry to the run log with how many were removed.
 */
function cleanup_triggers_v2() {
  const keep = new Set(); // keep none; or add names to keep: keep.add('watchdog_v2')
  const ts = ScriptApp.getProjectTriggers();
  let removed = 0;
  for (let i = 0; i < ts.length; i++) {
    const fn = ts[i].getHandlerFunction && ts[i].getHandlerFunction();
    if (!keep.has(fn) && (fn === 'resume_all_v2_safe' || fn === 'watchdog_v2')) {
      ScriptApp.deleteTrigger(ts[i]);
      removed++;
    }
  }
  logRun_('cleanup_triggers', '', '', '', '', 'ok (removed ' + removed + ')', formatCET(new Date()));
}
