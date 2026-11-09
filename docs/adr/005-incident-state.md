# ADR-005: Modelling incidents from a chain of CAP messages

**Status:** Accepted
**Date:** 2026-11-09

## Context
An NWS hazard is not one record. A Tornado Warning is issued (Alert), then narrowed or extended
(Update), then ended early (Cancel), or simply expires. Each message has its own id and lists the
earlier messages it updates in `references`. Dashboards need the *current* state of each hazard.

## Decision
- **Store every message** (`alert_messages`) and every reference (`alert_references`): the full
  history stays queryable.
- **Resolve the incident** a message belongs to by following its references to already-stored
  messages; fall back to the producer's incident key (the earliest referenced id). This survives
  long chains where NWS drops the earliest reference from later updates.
- **`incidents` holds the current state**, taken from the message with the latest `sent` time.
  `state` is `cancelled` after a Cancel; otherwise `active`. Expiry is not stored as a state,
  because time passes without any message arriving: views compare `expires` and `ends` with
  `now()` at query time.
- **Only `status = 'Actual'` counts.** Exercises and tests are stored but never shown as active.
- **People affected** = population of the incident's counties (SAME codes → county FIPS → Census
  ACS 5-year). For "people under a warning", each county is counted once even when several
  warnings cover it.

## Consequences
- Counts and maps are correct regardless of the order messages arrive in (tested).
- Population is an estimate of people *in covered counties*, not people inside the warning
  polygon; polygon-level estimates would need block-level population and spatial joins (PostGIS).
- County codes that do not match the Census table are counted and shown (`unmatched_counties`).
  Connecticut is the known case: its eight counties were replaced by planning regions in 2022, so
  alerts using old county codes do not match newer Census data.
