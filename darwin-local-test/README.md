# Darwin Local Test

Isolated Node.js scripts that explore the National Rail **Darwin Push Port**
feed (Confluent Cloud Kafka) and the daily **Push Port Timetable** files
(XML.gz). Has its own `package.json`; does **not** touch the website source or
its dependencies.

---

## ⚡ Quick start — run the website + daemon together

```bash
# from the repo root (one terminal):
npm install               # only needed once, or after dependency changes
npm run devdarwin   # starts Vite (web) + the daemon side-by-side
```

Then open <http://localhost:3000/departures/LDS> in your browser. You'll see
live Leeds departures; change the CRS in the URL for any station
(`/departures/KGX`, `/departures/MAN`, `/departures/STP`, `/departures/DEW`).
Click any departure card to see the full calling pattern at `/services/:rid`.

### If port 4001 is already taken

Most likely a previous daemon didn't shut down cleanly:

```bash
lsof -ti :4001 | xargs kill -9
```

then re-run `npm run devdarwin`.

### Just the daemon, no website

```bash
# from the repo root:
npm run darwin:daemon

# or from this folder:
cd darwin-local-test
npm run daemon
```

Then hit it directly:

```bash
curl http://localhost:4001/api/health | jq
curl 'http://localhost:4001/api/departures/LDS?hours=1' | jq '.counts, .departures[0]'
```

### One-shot scripts (no long-running process)

```bash
cd darwin-local-test
npm run leeds:full        # Leeds departure board, prints once and exits
DARWIN_LEEDS_TIPLOC=KNGX  npm run leeds:full   # any station
AT=DWBY TIME=15:34        npm run service      # detailed view of one service
```

### First-time setup checklist

1. `npm install` from the repo root, then `npm install` from this folder if you'll run the one-shot scripts directly.
2. `cp .env.example .env` from this folder and fill in `DARWIN_USERNAME` / `DARWIN_PASSWORD` — credentials live in `docs/Darwin guides/Credentials.md` (gitignored).
3. Put today's timetable files under `darwin-local-test/tt/YYYYMMDD/` (the fetch script does this automatically). The daemon picks today's highest-version file automatically.

Full setup details are in [§1](#1-one-time-setup) below.

### Automate daily file fetch (04:05)

You can auto-download today's `v8` + `ref_v99` files from GCS into `darwin-local-test/tt/YYYYMMDD/`.

Manual run:

```bash
# from repo root
npm run darwin:fetch-daily-files
```

Daily schedule with cron (runs at 04:05 local time):

```bash
crontab -e
```

Add:

```cron
5 4 * * * cd /Users/jackwingate/Documents/Rail\ Statistics/CODEBASES/RailStatisticsWebsite && /usr/bin/env npm run darwin:fetch-daily-files >> /tmp/darwin-fetch.log 2>&1
```

What this does:
- Picks today's latest `PPTimetable_YYYYMMDD..._v8.xml.gz`
- Picks today's latest `PPTimetable_YYYYMMDD..._ref_v99.xml.gz`
- Downloads both to `darwin-local-test/tt/YYYYMMDD/` if missing

---

Four scripts, one shared timetable loader. Pick whichever fits the question.

| script                      | npm command         | what it does                                              |
|-----------------------------|---------------------|-----------------------------------------------------------|
| `index.mjs`                 | `npm run leeds`     | Kafka smoke test — prints live Leeds matches for ~60 s.   |
| `leeds-next-hour.mjs`       | `npm run leeds:hour`| Kafka-only departure board (no timetable file needed).    |
| `leeds-departures.mjs`      | `npm run leeds:full`| **Recommended.** Timetable + live Kafka + cancellation reasons. |
| `service-detail.mjs`        | `npm run service`   | Full per-service detail (every calling point + live).     |
| `departures-daemon.mjs`     | `npm run daemon`    | **Long-running HTTP API.** Loads the whole UK timetable, streams Kafka, serves any-station departure boards on `:4001`. |
| `probe-topic.mjs`           | `node probe-topic.mjs` | Prints partition count / leaders. Diagnostic only.    |
| `probe-cancellations.mjs`   | `node probe-cancellations.mjs` | Prints every cancellation observed across the whole feed. |

---

## 1. One-time setup

From this folder (`darwin-local-test/`):

```bash
npm install
cp .env.example .env
```

Fill in `.env`:

| variable             | source                                                         |
|----------------------|----------------------------------------------------------------|
| `DARWIN_BOOTSTRAP`   | from `docs/Darwin guides/Credentials.md`                       |
| `DARWIN_TOPIC`       | the JSON topic — pre-filled in `.env.example`                  |
| `DARWIN_GROUP_ID`    | issued by RDM with your subscription                           |
| `DARWIN_USERNAME`    | Confluent API key (a.k.a. "Consumer username")                 |
| `DARWIN_PASSWORD`    | Confluent API secret (a.k.a. "Consumer password")              |

`.env` is gitignored. **Never commit credentials.**

### Timetable files

The board and service-detail scripts need today's PPTimetable XML.gz file.
Drop it into one of these folders (the loader checks in this order):

```
darwin-local-test/tt/YYYYMMDD/  ← preferred (daily folder, used by auto-fetch)
docs/V8s/                       ← fallback (legacy/manual)
docs/timetablefiles/            ← fallback (older v5/v6/v7 schemas also work)
```

File naming: `PPTimetable_YYYYMMDDhhmmss_v{N}.xml.gz`. The loader auto-picks
today's file with the **highest version number**.

---

## 2. Daily usage

### Live departure board for any TIPLOC

```bash
# Default: Leeds, next 60 minutes from now, with 30 s of live overlay
npm run leeds:full

# Different station
DARWIN_LEEDS_TIPLOC=YORK npm run leeds:full
DARWIN_LEEDS_TIPLOC=KNGX npm run leeds:full

# Different time window today (HH:MM, 24 h)
DARWIN_LEEDS_TIPLOC=DWBY WINDOW_START=15:00 WINDOW_END=16:00 npm run leeds:full

# Wider "next N minutes" instead of fixed window
LEEDS_WINDOW_MIN=120 npm run leeds:full

# Skip Kafka entirely (timetable only — instant, useful offline)
LIVE_SEC=0 npm run leeds:full

# Longer live overlay (catches more services that haven't been forecast in last 30 s)
LIVE_SEC=120 npm run leeds:full

# Pin to a specific timetable file
LEEDS_TIMETABLE=/full/path/to/PPTimetable_20260428020459_v8.xml.gz npm run leeds:full

# Replay the last 6 hours of retained Kafka messages (catches cancellations
# announced earlier today that wouldn't appear in a short live window).
REPLAY_MIN=360 LIVE_SEC=30 npm run leeds:full
```

Output is a sorted board: `time · plat · trainId · TOC · status · origin → destination · calling pattern`.

**Cancellations** appear on a second indented line with the decoded reason, e.g.

```
19:39  1  2V61  GN  CANCELLED  WLWYNGC → MRGT
       ↳ reason: This service has been cancelled because of a fault on this train  [schedule]
```

Reason sources (the `[...]` tag at the end):

| source            | where the reason came from                                                      |
|-------------------|---------------------------------------------------------------------------------|
| `[schedule]`      | a `schedule` message with top-level `cancelReason` (most common)                |
| `[schedule-loc]`  | per-location `can="true"` in a schedule message (partial cancel)                |
| `[ts]`            | a `TS` message with top-level `cancelReason`/`lateReason`                       |
| `[ts-loc]`        | a `TS` message location-level cancel/late reason                                |
| `[ts-loc-can]`    | a `TS` location with `can="true"` (in-running partial cancel at this stop)      |
| `[deactivated]`   | the whole schedule was deactivated — no reason code carried by the message     |

### Detail view for a single service

```bash
# By RID (fastest if you know it — copy from the board output)
RID=202604287192109 npm run service

# By TIPLOC + scheduled departure time
AT=DWBY TIME=15:34 npm run service
AT=KNGX TIME=18:30 npm run service
AT=YORK TIME=09:15 LIVE_SEC=60 npm run service
```

Prints:
1. Header (rid, uid, trainId, TOC, category, passenger flag).
2. Full chronological calling pattern — every `OR / IP / PP / DT` location
   with public + working times and activity codes.
3. Live overlay table (if the service is currently active or near-future):
   scheduled vs live arrivals/departures, with a `delta` column showing
   on-time / early / late by minute.

Live overlay is auto-skipped when the service is fully in the past (no
forecasts to merge).

### Pure Kafka smoke tests (no timetable file needed)

```bash
npm run leeds         # 60 s of live messages, prints Leeds-matching TS rows
npm run leeds:hour    # next-60-min board built ONLY from Kafka history+live
```

These are useful when you want to verify the Kafka pipe is healthy independently
of the timetable file workflow.

### Long-running daemon for the website (`npm run daemon`)

The one-shot board scripts exit after a single listen. For the **live website**
the daemon keeps a rolling in-memory state for ALL UK stations and serves
any-station departure boards over HTTP on port 4001.

```bash
# from this folder:
npm run daemon

# OR from the repo root, alongside the website dev server (recommended):
cd ..
npm run devdarwin
```

On startup it:
1. Loads today's timetable file from `darwin-local-test/tt/YYYYMMDD/` (or legacy `docs/V8s/` / `docs/timetablefiles/`).
2. Builds two indexes: `byRid` (RID → full journey) and `byTiploc`
   (TIPLOC → list of services calling there).
3. Loads the `_ref_v*.xml.gz` reference file for late/cancel reasons,
   TIPLOC ↔ CRS ↔ station-name lookup, and TOC code ↔ operator name.
4. Connects to the Kafka Push Port and replays `INITIAL_REPLAY_MIN` minutes
   (default 360) so today's earlier cancellations and forecasts are already
   in state before the first HTTP request.
5. Listens forever. Day rollover detected automatically at midnight.

For a single laptop running the full UK timetable: ~5 s startup, ~290 MB heap
at rest, ~430 MB while replaying, ~420 MB steady-state.

#### HTTP endpoints (default port 4001)

| method | path                              | purpose                                                |
|--------|-----------------------------------|--------------------------------------------------------|
| GET    | `/api/health`                     | daemon stats: journeys loaded, kafka, overlay sizes    |
| GET    | `/api/station/:code`              | resolve CRS or TIPLOC → `{ tiploc, name, crs, matchedAs }` |
| GET    | `/api/departures/:code?hours=N`   | live departure board (rows + NRCC station messages)    |
| GET    | `/api/messages/:crs`              | currently-known NRCC station messages for a CRS        |
| GET    | `/api/service/:rid`               | full calling pattern + formation + associations + alerts + **consist** |
| GET    | `/api/unit/:resourceGroupId`      | physical unit detail + today's diagram (PTAC)          |

### Network Rail PTAC (Passenger Train Allocation and Consist) — second feed

The daemon also runs a second Kafka consumer for Network Rail's
`prod-1033-Passenger-Train-Allocation-and-Consist-1_0` topic when
`PTAC_USERNAME`, `PTAC_PASSWORD` and `PTAC_GROUP_ID` are set in `.env`.
This is an XML feed (parsed by `consist-parser.mjs`) that gives us the
*physical reality* view of every passenger train: actual unit numbers,
fleet/class identification, individual vehicle IDs, seat counts, max
speed, brake type, weight, length, livery, registered category, **and
open defects per coach**.

PTAC messages are joined to Darwin RIDs via the 4-tuple
`(StartDate, OperationalTrainNumber, TrainOriginLocation, TrainOriginDateTime)`
— ~99 % match rate observed. Unmatched messages are stashed and retried
when the timetable reloads.

**Participating operators in this contract** (refreshed daily as we observe joins):
SE (Southeastern), XR (Elizabeth Line), SW (South Western), NT (Northern),
TP (TransPennine Express), c2c, GTR (Thameslink/Southern/GN), Greater
Anglia, Northern, Heathrow Express, EMR, and more — ~21 distinct TOC
codes seen, ~100+ fleet classes covered.

Service-detail responses now include a `consist` field with the full
allocation tree. Unit-tracking is surfaced via `/api/unit/:id`, e.g.

```bash
curl -s http://localhost:4001/api/unit/158756 | jq '.fleetId, .services'
```

### Beyond schedules: extra data exposed

In addition to live arrival/departure forecasts and platforms, the daemon
consumes and exposes the following Push Port elements:

| Element              | Where it surfaces |
|----------------------|-------------------|
| `serviceLoading`     | `loadingPercentage` (0–100) on each board row and service-detail stop |
| `formationLoading`   | `coachLoading[]` (0–100% per coach) on each row and stop |
| `scheduleFormations` | `formation` ({ fid, coaches[] }) on service detail |
| `association`        | `associations[]` on service detail (joins / divides / next-portion) |
| `OW` (NRCC)          | `messages[]` on each board response, also via `/api/messages/:crs` |
| `trainAlert`         | `alerts[]` on service detail |
| `TS.isReverseFormation` | `reverseFormation: true` on service detail |
| Per-stop `cancelReason` | `cancelReasonAtStop` on each stop in service detail |

`/api/health` reports a counter for each overlay store (`live`, `cancelled`,
`formations`, `messages`, `ridsWithAssociations`, etc.) so you can verify
data is actually flowing without grepping the heartbeat log.

`code` accepts either CRS (`LDS`) or TIPLOC (`LEEDS`). Where a CRS maps to
multiple TIPLOCs (e.g. multi-platform stations) the daemon picks the one
with the most services that day and lists the others in `alternates`.

#### Env vars (all optional except DARWIN_*)

| var                      | default                       | meaning                                               |
|--------------------------|-------------------------------|-------------------------------------------------------|
| `DAEMON_PORT`            | `4001`                        | HTTP API port                                         |
| `CORS_ORIGIN`            | `http://localhost:3000`       | `Access-Control-Allow-Origin` for browser clients     |
| `DEFAULT_WINDOW_HOURS`   | `3`                           | look-ahead used when `?hours` is omitted              |
| `INITIAL_REPLAY_MIN`     | `360`                         | minutes of Kafka to replay on startup                 |
| `HEARTBEAT_SEC`          | `60`                          | stats log interval                                    |
| `DARWIN_AUTO_FETCH_FILES`| `true`                        | auto-fetch daily `v8` + `ref_v99` files while daemon runs |
| `DARWIN_AUTO_FETCH_TIME` | `04:05`                       | local HH:MM time to run auto-fetch once per day       |
| `DARWIN_STATIONS_REF_XML`| `darwin-local-test/StationsRefData_v1.2.xml` | optional extra TIPLOC→name fallback for passing points |

#### Smoke-test from a terminal

```bash
curl -s http://localhost:4001/api/health | jq
curl -s http://localhost:4001/api/station/LDS | jq
curl -s 'http://localhost:4001/api/departures/LDS?hours=1' | jq '.counts, .departures[0]'
```

### Website integration (local dev)

The React app (Vite, port 3000) talks to the daemon via a transparent proxy:

```
browser →  http://localhost:3000/api/darwin/departures/LDS
  Vite proxy rewrites →  http://localhost:4001/api/departures/LDS
```

The proxy is configured in `vite.config.js`. Browser code uses
`fetch('/api/darwin/...')` everywhere — the same path will work in
production once the daemon has a public URL (the Netlify rewrite stays the
same; only its target changes).

Start everything with one command from the repo root:

```bash
npm run devdarwin     # starts vite (web) + daemon concurrently
```

Visit <http://localhost:3000/departures/LDS> for live Leeds, change the
CRS in the URL for any other station (`/departures/KGX`, `/departures/MAN`,
`/departures/DEW`, etc.).

Shared TypeScript types live in `src/types/darwin.ts` (single source of
truth, mirrors the daemon's JSON shape exactly).

Schema for each row in `departures[]`:

```ts
{
  rid: string;
  trainId: string;              // 4-char headcode, e.g. "1A28"
  uid: string;
  toc: string;                  // 2-char operator code
  scheduledTime: string;        // "HH:MM"
  scheduledAt: string;          // ISO timestamp anchored to today
  liveTime: string;             // latest best-known departure time
  liveKind: 'scheduled'|'working'|'est'|'actual';
  platform: string|null;        // scheduled platform
  livePlatform: string|null;    // overlaid platform if changed
  origin: string;               // TIPLOC
  destination: string;          // TIPLOC
  callingAfter: string[];       // TIPLOCs in order after this stop

  // Human-readable names and CRS codes (from ref_v*.xml.gz).
  // Parallel arrays align with callingAfter; null for junctions/sidings.
  tocName:              string|null;
  originName:           string|null;
  originCrs:            string|null;
  destinationName:      string|null;
  destinationCrs:       string|null;
  callingAfterNames:    Array<string|null>;
  callingAfterCrs:      Array<string|null>;

  isPassenger: boolean;
  cancelled: boolean;
  cancellation: { code?, source, reason }|null;
  delayReason:  { code?, source, reason }|null;
  status: string;               // convenience: "on time"|"CANCELLED"|"est HH:MM"|...
}
```

---

## 3. Common modifications — recipes

### Filter to a different station

Everything is keyed on **TIPLOC** (Darwin's internal location codes — *not*
the 3-letter CRS codes the public sees). Common ones:

| TIPLOC    | station                       |
|-----------|-------------------------------|
| `LEEDS`   | Leeds                         |
| `YORK`    | York                          |
| `MNCRPIC` | Manchester Piccadilly         |
| `MNCRVIC` | Manchester Victoria           |
| `KNGX`    | London Kings Cross            |
| `EUSTON`  | London Euston                 |
| `BRGHTN`  | Brighton                      |
| `EDINBUR` | Edinburgh                     |
| `LVRPLSH` | Liverpool Lime Street         |
| `BHMNWS`  | Birmingham New Street         |

Full list lives in the **`_ref_v*.xml.gz` reference files** in `docs/timetablefiles/`
(TIPLOC ↔ CRS ↔ name). If you want human-readable names in the output, that's
the lookup source.

```bash
DARWIN_LEEDS_TIPLOC=BHMNWS npm run leeds:full
```

### Change the time window

```bash
# next 2 hours
LEEDS_WINDOW_MIN=120 npm run leeds:full

# fixed window today
WINDOW_START=07:00 WINDOW_END=09:00 npm run leeds:full   # peak inbound
WINDOW_START=16:30 WINDOW_END=19:00 npm run leeds:full   # peak outbound
```

### Use a different schedule activity filter

`leeds-departures.mjs` line ~140 currently excludes `act === 'TF'`
(terminating arrivals). Open it and tweak if you want to include arrivals,
freight (`isPassengerSvc === 'false'`), or empty-stock moves (`trainCat === 'EE'`):

```javascript
// drop these lines to include terminating arrivals
if (sc.activity === 'TF') continue;

// or filter to passenger services only
if (!sc.isPassenger) continue;
```

### Fetch a different topic (XML or AVRO)

```bash
# .env or one-off:
DARWIN_TOPIC=prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-XML  npm run leeds
```

XML decoding is built into `index.mjs` (auto-detects gzipped XML payloads).
AVRO requires Confluent Schema Registry credentials; not implemented here.

### Replay older Kafka messages (catch earlier cancellations)

Most cancellations are announced by a **single `schedule` message** emitted
when the decision is made — sometimes hours before the train was due. A short
live window won't catch them. Seek backward to replay retained messages:

```bash
# main board: rewind 6 hours before listening
REPLAY_MIN=360 LIVE_SEC=60 npm run leeds:full

# full 24-hour replay (catches everything retained on the broker)
REPLAY_MIN=1440 LIVE_SEC=10 npm run leeds:full

# Kafka-only board (older script, uses LEEDS_REPLAY_MIN instead of REPLAY_MIN)
LEEDS_REPLAY_MIN=120 npm run leeds:hour
```

Trade-off: `REPLAY_MIN=360` typically pulls ~100-150 k messages in 60-90 s
(~1,500 msg/s on the local link). The script handles this fine but it is
noisy. For a long-running production consumer you'd want a stateful process
that keeps rolling state instead of replaying on each launch.

Kafka retention on Confluent Cloud is typically 24-72 h.

### Verify cancellation decoding end-to-end

```bash
CANCEL_SEC=120 node probe-cancellations.mjs
```

Listens across the **whole feed** (any TIPLOC) for 120 s and prints every
cancellation / late-reason event it observes, with the code decoded against
the local reference tables. Useful when the main board says 0 cancellations
and you want to confirm the decoder works.

### Inspect a raw message

```bash
DARWIN_DUMP_FIRST_XML=1 npm run leeds
```

The first decoded payload is written to `first-message.xml` (or `.json`) for
offline inspection. Useful when fields don't look right.

### Add a new derived field to the board

The per-service object built by `leeds-departures.mjs` lives in
`departureRows`. Add fields in the loop around line ~150 and surface them in
the printed table around line ~250. Same pattern in `service-detail.mjs`.

---

## 4. How the data flows (mental model)

```
   ┌─────────────────────────────┐         ┌──────────────────────────────┐
   │ daily PPTimetable_*_v8.xml.gz │       │ Kafka Push Port (live JSON)  │
   │ source of truth: schedule    │       │ source: forecasts + actuals  │
   └────────────┬────────────────┘         └─────────────┬────────────────┘
                │                                         │
                ▼                                         ▼
          timetable-loader.mjs                       index.mjs decoder
                │                                         │
                ▼                                         ▼
        Map<rid, schedule>     ◀── merge by rid ──▶ Map<rid, liveState>
                │
                ▼
      leeds-departures.mjs / service-detail.mjs
```

Key points:

- **Schedule** (timetable file) gives every train a structure: TOC, trainId,
  origin, destination, all calling points with times and activity codes.
  Static for the day (snapshotted around 02:04 UTC).
- **Push Port** is *event-driven* — it broadcasts a `TS` (Train Status)
  message every time Darwin updates its forecast or records an actual time at
  a sensor. It does NOT broadcast a service unless something changes.
- **`schedule` messages** on the Push Port are emitted only when a service's
  schedule **changes** (new train, retiming, cancellation). Most services'
  full schedules are already in the timetable file, not the live stream.
- This is why the **timetable file + live overlay** model is the right one.
  Pure-Kafka scripts can replay history but can't reconstruct a daily
  schedule completely.

---

## 5. JSON wire format gotchas (verified 2026-04-28)

The JSON Push Port topic differs from the XML XSD in non-obvious ways. The
decoders here already handle these — listed for future maintainers:

| XML XSD                     | JSON wire form                |
|-----------------------------|-------------------------------|
| `<SC>` (schedule)           | `e.schedule`                  |
| `<AS>` (association)        | `e.association`               |
| `<DeactivatedSchedule>`     | `e.deactivated`               |
| `<LO>` (loading)            | `e.formationLoading`          |
| `<TS>` (train status)       | `e.TS` (case preserved)       |
| Calling points (passenger)  | `OR / IP / PP / DT`           |
| Calling points (freight/ECS)| `OPOR / OPIP / OPPP / OPDT`   |
| `<plat>3A</plat>`           | `{ platsrc, conf, "": "3A" }` |

All messages are wrapped in an envelope (`{ destination, messageID, bytes, ...}`)
where the inner Pport JSON is in the **stringified** `bytes` field. Decoders
in `index.mjs` and `service-detail.mjs` unwrap this transparently.

---

## 6. Troubleshooting

| symptom                                          | likely cause / fix                                                                  |
|--------------------------------------------------|-------------------------------------------------------------------------------------|
| `SASL authentication failed`                     | Wrong key/secret, or your subscription expired. Re-check RDM.                       |
| `topic authorization failed`                     | Group ID mismatch (`DARWIN_GROUP_ID` must match what RDM issued you).               |
| Stuck on `subscribed` with `memberAssignment:{}` | Another consumer in the same group is holding the partition. Identify with `lsof -nP -i TCP:9092` and stop it. |
| `Schedule data captured for 0`                   | Group has committed offsets at "latest". Use `LEEDS_REPLAY_MIN=...` for `leeds:hour`, or use `leeds:full` (timetable). |
| 0 services in the board                          | Timetable file for today missing — drop into `docs/V8s/`. Or window is empty.       |
| `[object Object]` in plat column                 | Use the latest `service-detail.mjs` — early version had a bug, fixed.               |
| `no timetable file for today`                    | Loader looked in `docs/V8s/` and `docs/timetablefiles/`. Add a file there or pass `LEEDS_TIMETABLE=`. |
| TLS hostname mismatch when listing legacy bucket | Pre-RDM `darwin.xmltimetable` bucket — IP-whitelisted, likely retired. Use the v8 files RDM provides instead. |

---

## 7. Topic partitioning (current state)

Probed via `node probe-topic.mjs` on 2026-04-28: **all three Darwin topics
(`-JSON`, `-XML`, `-AVRO`) have exactly 1 partition.**

Historical note: the openraildata-talk thread `oth6M9vnsKU` (Jun-Oct 2025)
discussed 2-partition out-of-order delivery and per-partition sequence-gap
tracking. That issue no longer applies with the current 1-partition layout —
ordering by `Pport@ts` is global, and a single sequence-number counter is
sufficient to detect missed messages.

Re-run `node probe-topic.mjs` if you suspect this has changed again.

---

## 8. File reference

| file                         | purpose                                                                  |
|------------------------------|--------------------------------------------------------------------------|
| `index.mjs`                  | Pure Kafka smoke test (Leeds matches over a fixed window).               |
| `leeds-next-hour.mjs`        | Kafka-only board, with manual replay-from-N-minutes-ago seek.            |
| `leeds-departures.mjs`       | **Main board.** Timetable + live overlay.                                |
| `service-detail.mjs`         | Single-service detail, all calling points, scheduled vs live diff.       |
| `departures-daemon.mjs`      | **Long-running** rolling snapshot writer for the website. Writes `state/<tiploc>-departures.json` every few seconds. |
| `timetable-loader.mjs`       | Schema-aware (v5/v6/v7/v8) regex-prefiltered XML parser.                 |
| `reasons-loader.mjs`         | Loads LateRunningReasons + CancellationReasons from `_ref_v*.xml.gz`.    |
| `locations-loader.mjs`       | Loads LocationRef (TIPLOC↔CRS↔name) + TocRef (operator code↔name).         |
| `probe-topic.mjs`            | Prints partition count and leaders. Diagnostic.                          |
| `probe-cancellations.mjs`    | Prints every cancellation observed feed-wide for N seconds. Diagnostic.  |
| `sample-types.mjs`           | (debug) Captures one example of each Pport message type.                 |
| `dump-raw.mjs`               | (debug) Dumps first N raw Kafka envelopes to `raw-samples.json`.         |
| `package.json`               | Isolated deps: `kafkajs`, `fast-xml-parser`, `dotenv`.                   |
| `.env.example`               | Copy → `.env` and fill in credentials.                                   |
| `first-message.json/.xml`    | Generated when `DARWIN_DUMP_FIRST_*=1`. Inspect in your editor.          |
| `type-samples.json`          | Generated by `sample-types.mjs`. One example of every observed type.     |
| `raw-samples.json`           | Generated by `dump-raw.mjs`. Raw envelopes for low-level debugging.      |

---

## 9. Future work

- **Historical service performance** — separate RDM product (subscription
  pending). Once approved, will let `leeds:full` and `service` answer
  retrospective queries (e.g. "how late did the 15:34 from Dewsbury actually
  arrive at Wigan?") instead of just showing scheduled times.
- ~~**CRS / human-name resolution**~~ — **done.** `locations-loader.mjs` loads 12 k locations + 43 TOCs from `ref_v*.xml.gz`. The daemon snapshot now includes `stationName`, `originName`/`originCrs`, `destinationName`/`destinationCrs`, `callingAfterNames[]`/`callingAfterCrs[]`, and `tocName` (e.g. `"Northern"`, `"TransPennine Express"`).

> **Note on ref file versions**: reference files have their own version scheme
> (`_ref_v1` / `_ref_v2` / `_ref_v3` / `_ref_v4` / `_ref_v99`) that is
> **unrelated** to the timetable file's v5/v6/v7/v8. As of 2026-04-28 `ref_v4`
> is the current production version. No `ref_v8` exists.
- ~~**Continuous JSON output**~~ — **done.** Replaced by an **HTTP API** in `departures-daemon.mjs` serving any-station boards on port 4001. The website consumes it via `fetch('/api/darwin/...')` (Vite proxies in dev).

---

## 10. Production hosting

The daemon is a **stateful long-running process** with ~420 MB RSS that needs a
persistent connection to Kafka. **Netlify Functions cannot host this** — they
have short timeouts and no persistent memory between invocations. Options to
choose between later:

| host                  | fit for this workload                                                        |
|-----------------------|------------------------------------------------------------------------------|
| **Fly.io machine**    | Single small VM (1 GB), simplest, keeps RAM warm, scale-to-zero unsupported. |
| **Railway worker**    | Easy GitHub-attached deploys, fixed monthly cost.                            |
| **Bare VPS** (Hetzner/DO) | Cheapest at scale (~£4/mo), needs systemd unit + manual updates.        |
| **Cloud Run** (GCP)   | Container, can stay warm with `min-instances >= 1`. Good if already on GCP.  |

Production deploy contract:
- Daemon exposes `/api/health`, `/api/station/:code`, `/api/departures/:code`,
  `/api/messages/:crs`, `/api/service/:rid` on whatever port the host gives
  it (`PORT` env var convention; daemon already accepts `DAEMON_PORT`).
- Website's Netlify config gets a redirect from `/api/darwin/*` to that
  public URL — no React changes needed because the browser already uses
  the relative `/api/darwin/...` path in dev.
- Daemon needs the same `DARWIN_*` env vars + access to today's timetable
  files. Shipping the `docs/V8s/` directory with the container is fine for
  v1 (~12 MB/day); a tiny cron that pulls from RDM's S3 bucket is the
  follow-up.

