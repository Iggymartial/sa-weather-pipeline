# Design Decisions Log

The reasoning behind key choices, written as I build rather than after.

## Step 1: Extractor, raw storage and tests

**Data source: Open-Meteo.** Free, no API key, and it returns data that
genuinely changes, which is what justifies scheduling a pipeline at all.
The terms shape the design: free use is non-commercial and capped (under
10,000 calls/day), and the data is CC BY 4.0, so the README carries an
attribution line. One request per run, covering every location, keeps usage
a tiny fraction of the cap.

**I poll "current conditions" rather than pulling a forecast.** Each run
adds one observation per location, so the database becomes a time series
built by repeated polling. That is the same shape of problem as any
monitoring pipeline, and it gives the schedule a real purpose: skip a run
and there is a permanent gap.

**One request for all nine locations.** Open-Meteo accepts comma-separated
coordinates. Nine requests would mean nine chances to fail and nine
round-trips; one request is either all-or-nothing, which is simpler to
reason about and to retry. A quirk to remember: with several locations the
API returns a list, but with exactly one location it returns a plain object.
Config can legitimately shrink to one city, so the transform step must
handle both shapes.

**Results are matched to locations by position, never by coordinates.**
Open-Meteo snaps requested coordinates to the centre of its model grid
(asking for 40.7128, -74.0060 returns 40.710335, -73.99235). Comparing
coordinates would therefore never match. Order is preserved by the API, so
the raw file records the locations in request order and the transform step
pairs them up by index. The transform must also check the counts agree.

**Raw payload stored untouched, inside a small envelope.** The `response`
field is exactly what the API sent. Around it the envelope records when it
was fetched, the full request, and the locations requested. This makes each
file self-describing: if `config/locations.json` changes next month, an old
raw file can still be interpreted correctly, because it carries its own copy
of what was asked. Cleaning is deliberately not done here, so a bug in the
transform step can always be fixed by reprocessing the originals.

**Timestamps stay in UTC, and the assumption is checked rather than
trusted.** I do not send a `timezone` parameter, relying on the documented
default of GMT. Sending my own value risked a mismatch between a single
timezone and nine coordinates that I could not verify. Instead the transform
step will assert each response reports GMT and a zero UTC offset, turning
the assumption into a validated invariant.

**Retry policy.** Timeouts, connection errors, HTTP 429 and 5xx responses
(and a 200 with an unreadable body) are retried up to three attempts with
exponential backoff (2s, then 4s, with no pointless sleep after the last
attempt). Other 4xx responses are not retried: a request that is wrong stays
wrong, and the API's own `reason` message is surfaced in the error so the
cause is visible. This is the first of two layers. Airflow's task-level
retries (Step 6) are the second, covering longer outages than a few seconds
of backoff can ride out.

**Raw files are never overwritten, and never left half-written.** If two
runs land in the same second, the second file gets a `-1` suffix instead of
replacing the first. Files are written to a `.tmp` name and renamed into
place, so a crash mid-write cannot leave a truncated `.json` for the
transform step to trip over.

**Configuration, not constants.** Locations live in `config/locations.json`
and are validated on load (required fields, coordinate ranges, unique ids),
so a typo stops the run instead of quietly fetching the wrong place. The
API base URL can be overridden by environment variable, so a future API
version change is an environment edit, not a code change.

**Bulk data is not committed to git.** An hourly schedule produces about 24
files a day, which would bury the commit history this project is meant to
demonstrate. The folders are kept with `.gitkeep`, and a few curated real
files will live in `data/samples/`.

**A known duplicate problem, deliberately left for the transform step.**
The `current` block refreshes every 15 minutes (`interval: 900`). An hourly
schedule, a manual trigger, or an Airflow retry can therefore return an
observation already stored. Observations will be unique on
(location, observed_at), and the loader must be idempotent against it.

**Testing approach.** The 38 tests use no network and no real sleeping: HTTP
is replaced by a scripted fake and retry delays are recorded rather than
waited out. To check the tests can actually fail, I deliberately broke the
code three ways (stopped retrying 503s, allowed overwriting raw files, swapped
latitude and longitude in the request) and confirmed each was caught. The
fake API responses are modelled on Open-Meteo's documented format; once the
first live run succeeds, a real raw file will be added to `data/samples/` as
an additional fixture.
