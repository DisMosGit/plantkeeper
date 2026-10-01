# care-journal Specification

## Purpose

The append-only record of what was actually done to each plant, kept as an event-sourced stream so it can be replayed to any past date.

## Requirements

### Requirement: One stream per plant

Every journal entry SHALL belong to exactly one plant, and a plant's entries SHALL form a single ordered stream identified by the plant's identifier.

#### Scenario: Entries are appended for a plant

- **WHEN** entries are recorded for a plant
- **THEN** they appear in that plant's stream in the order they were appended, each with a position in the stream

#### Scenario: A plant with no entries is read

- **WHEN** a plant's journal is read before any entry was recorded
- **THEN** the journal is empty rather than an error

### Requirement: The journal is append-only

A recorded entry SHALL NOT be modified or deleted, and the same entry SHALL NOT appear twice in a stream.

#### Scenario: An entry is recorded

- **WHEN** an entry is recorded for a plant
- **THEN** it exists in the stream permanently and no operation offered by the platform changes or removes it

### Requirement: Concurrent appends cannot fork a stream

An append SHALL be written at the position after the stream's current end, and an append that loses a race SHALL be refused rather than written, leaving no partial state behind.

#### Scenario: Two writers append at the same position

- **WHEN** two appends target the same stream position
- **THEN** one is written, the other is refused as a conflict, and the stream has no gap and no duplicate position

#### Scenario: A refused append is retried

- **WHEN** the refused append is retried against the stream's new end
- **THEN** it succeeds and the stream stays contiguous

### Requirement: A replay detects a damaged stream instead of skipping it

Replaying a stream SHALL fail when a position is missing, when an entry names a type the journal does not know, or when an entry's payload no longer validates. A damaged stream SHALL NOT be replayed partially with a warning, because the stream is the source of truth for that plant.

#### Scenario: A position is missing from the stream

- **WHEN** a stream is replayed and a position has no entry
- **THEN** the replay fails as corrupt rather than returning a state built from the surviving entries

#### Scenario: An entry's payload no longer validates

- **WHEN** a stream is replayed and one entry's payload fails validation
- **THEN** the replay fails as corrupt

### Requirement: Replay is deterministic

Replaying the same stream SHALL produce the same state every time, and reading a plant's journal twice SHALL return the same entries in the same order.

#### Scenario: A stream is replayed twice

- **WHEN** a plant's stream is replayed twice
- **THEN** both replays produce the same state

### Requirement: A completed watering becomes exactly one journal entry

A completed watering SHALL produce exactly one journal entry recording what was done, and the entry's identity SHALL be derived from the event so that a redelivery or a rebuilt consumer appends nothing further.

#### Scenario: A watering is completed

- **WHEN** a watering-completed fact is consumed
- **THEN** one entry of the watering kind is appended, carrying the moment the care happened as distinct from the moment it was recorded

#### Scenario: The same fact is delivered twice

- **WHEN** the same watering fact is delivered again
- **THEN** no second entry is appended, even if the consumer's own ledger has been lost

### Requirement: Snapshots bound the cost of replay without replacing the stream

A plant's state SHALL be checkpointed periodically as the stream grows, and a replay SHALL start from the newest checkpoint and read only the entries after it.

#### Scenario: A long stream is read

- **WHEN** a plant's stream has grown past a checkpoint boundary and its journal is read
- **THEN** the read starts from the newest checkpoint rather than from the first entry

#### Scenario: The checkpoint is missing or stale

- **WHEN** a stream is read and no checkpoint exists
- **THEN** the whole stream is replayed and the result is identical

### Requirement: The journal answers what was true on a date

A plant's journal SHALL be readable in full and as of a date, where the date selects entries by the moment the care happened rather than the moment the entry was recorded, and the end of the named day SHALL be included.

#### Scenario: The journal is read as of a date

- **WHEN** a plant's journal is requested as of a date
- **THEN** the response contains the entries whose care happened on or before the end of that day

#### Scenario: A backdated entry is added later

- **WHEN** an entry describing care from an earlier day is added afterwards, and that earlier day is requested
- **THEN** the backdated entry is included in that day's result

### Requirement: The journal records what happened, not what did not

Skipping, missing and rescheduling a watering SHALL NOT create journal entries, and recording an entry by hand SHALL NOT be part of this capability.

#### Scenario: A watering is skipped

- **WHEN** a household skips a watering
- **THEN** no journal entry is recorded for it
