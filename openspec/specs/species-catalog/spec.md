# species-catalog Specification

## Purpose

The catalogue of plant species and the external source it is synchronised from, including how the external vocabulary is translated and how the catalogue survives that source being unavailable.

## Requirements

### Requirement: The external source's vocabulary stops at the boundary

Species SHALL be stored in the catalogue's own terms, and no value from the external source SHALL reach the domain untranslated. The translation SHALL map the source's ecological indicator scales onto the catalogue's light requirement and watering interval through fixed bands, and SHALL clamp a value outside the expected range rather than trust it.

#### Scenario: A record is translated

- **WHEN** an upstream record is imported
- **THEN** its light and soil indicators become the catalogue's own light requirement and watering interval

#### Scenario: An upstream value falls outside the expected range

- **WHEN** an upstream indicator carries a value beyond the mapped range
- **THEN** the value is clamped into the range instead of being accepted as-is

#### Scenario: A record has no scientific name

- **WHEN** an upstream record carries no scientific name
- **THEN** the record is skipped with a warning and the rest of the run continues

#### Scenario: A record has no common name

- **WHEN** an upstream record carries no common name
- **THEN** its scientific name is used in its place

### Requirement: A species keeps one identity across synchronisations

A species' identifier SHALL be derived from its upstream slug under a fixed namespace, so that a later synchronisation recognises an existing species rather than creating a duplicate. That namespace SHALL never change.

#### Scenario: The same species is synchronised twice

- **WHEN** the same upstream species is imported by two separate synchronisations
- **THEN** the second run updates the existing catalogue entry rather than adding a second one

### Requirement: Requests to the external source are rate limited, retried and circuit broken

Every upstream request SHALL pass through a rate limit below the source's allowance. A transient failure — a transport error, a rate-limit response or a server error — SHALL be retried a bounded number of times with backoff; any other client error SHALL NOT be retried. A circuit breaker SHALL open after a threshold of consecutive logical failures, refuse calls without touching the network while open, allow a single trial after its reset period, and close again on success. Retries SHALL NOT inflate the breaker's failure count.

#### Scenario: The source returns a server error

- **WHEN** an upstream request fails with a server error
- **THEN** it is retried with backoff up to the configured attempts

#### Scenario: The source rejects the request

- **WHEN** an upstream request fails with a client error other than a rate-limit response
- **THEN** it is not retried and the failure is reported

#### Scenario: The source keeps failing

- **WHEN** consecutive failures reach the breaker's threshold
- **THEN** the breaker opens, further calls are refused without a network request, and one trial is allowed once the reset period has elapsed

#### Scenario: A retried call eventually succeeds

- **WHEN** a call succeeds after retries
- **THEN** the breaker counts one success, not one failure per attempt

### Requirement: An unavailable source degrades instead of failing the synchronisation

When the source is unavailable or its breaker is open, the last successful fetch SHALL be reused as a fallback, and a run with no cached fetch SHALL find no changes rather than failing. An authentication failure SHALL NOT be masked by the fallback.

#### Scenario: The source is unavailable after a successful fetch

- **WHEN** a synchronisation runs while the source is unavailable
- **THEN** the last successful fetch is used and the run proceeds

#### Scenario: The source is unavailable with nothing cached

- **WHEN** a synchronisation runs with no cached fetch and an unavailable source
- **THEN** the run completes having found no changes

#### Scenario: The source rejects the credentials

- **WHEN** the source answers that the credentials are not accepted
- **THEN** the failure is reported rather than replaced with cached data

### Requirement: A run imports a whole fetch or nothing

A synchronisation SHALL either work from a complete fetch or fail; an availability failure SHALL abort the fetch rather than import a partial one. A single record that cannot be read or parsed SHALL be skipped with a warning without failing the run.

#### Scenario: One record in the fetch is unreadable

- **WHEN** a fetch contains a record whose detail cannot be read or parsed
- **THEN** that record is skipped, the run continues and the remaining species are imported

#### Scenario: The fetch fails part way

- **WHEN** the source becomes unavailable part way through a fetch
- **THEN** the run fails rather than importing the part it managed to read

### Requirement: A synchronisation is requested manually or on a schedule and reports its changes

A synchronisation SHALL be startable on demand and on a recurring schedule, and both routes SHALL announce the same sync request. A run SHALL create species the catalogue does not know and update those that changed, publishing an event for each. A synchronisation with no configured credentials SHALL complete having done nothing and SHALL NOT fail.

#### Scenario: A synchronisation is requested by hand

- **WHEN** a client requests a synchronisation
- **THEN** the request is accepted and a sync request is announced

#### Scenario: A new species is found

- **WHEN** the upstream source carries a species the catalogue does not know
- **THEN** it is created in the catalogue and a species-added event is published

#### Scenario: A known species changed upstream

- **WHEN** a species already in the catalogue differs from its upstream record
- **THEN** it is updated and a species-updated event is published

#### Scenario: No credentials are configured

- **WHEN** a synchronisation runs without configured credentials
- **THEN** it completes having made no changes

### Requirement: A rolled-back synchronisation is itself a change consumers can see

When a synchronisation fails after applying changes, the created species SHALL be deleted and the changed ones restored to their previous values, and a restore SHALL be published as an ordinary update rather than applied silently. An event already published for a created species SHALL NOT be recalled.

#### Scenario: A later step fails after species were applied

- **WHEN** a synchronisation fails after creating and updating species
- **THEN** the created species are deleted and the changed ones are restored to their previous values

#### Scenario: The catalogue is restored

- **WHEN** a species is restored to its previous values
- **THEN** a species-updated event is published for the restore

### Requirement: The catalogue read path is cached and invalidated by event

A species lookup SHALL be served from a cache with a bounded lifetime, falling back to the catalogue on a miss and populating the cache on the way back. Invalidation SHALL be delivered as an event consumed after the change has committed, and SHALL NOT be performed inside the transaction that changed the species. A cache failure SHALL degrade to a miss or be logged, and SHALL NOT fail the request or the change.

#### Scenario: A species is read twice

- **WHEN** the same species is requested twice within the cache's lifetime
- **THEN** the second request is answered from the cache without reading the catalogue

#### Scenario: A species changes

- **WHEN** a species is updated
- **THEN** a cache invalidation is announced after the change commits, and the cache entry is dropped when the announcement is consumed

#### Scenario: The cache is unreachable

- **WHEN** the cache cannot be reached during a read
- **THEN** the answer comes from the catalogue and the request still succeeds

### Requirement: The catalogue never removes a species the source no longer carries

A synchronisation SHALL NOT delete a species that has disappeared upstream, because the source provides no signal that a species is gone.

#### Scenario: A species disappears upstream

- **WHEN** a species present in the catalogue is absent from a later fetch
- **THEN** it remains in the catalogue unchanged

### Requirement: Onboarding does not depend on the external source

Resolving a plant's species during onboarding SHALL read the local catalogue only, so that onboarding never waits on an external request. A plant whose species the catalogue has never seen SHALL NOT receive a care schedule.

#### Scenario: Onboarding a plant whose species is unknown

- **WHEN** a plant is onboarded with a species the catalogue does not hold
- **THEN** onboarding completes without contacting the external source and the plant receives no care schedule
