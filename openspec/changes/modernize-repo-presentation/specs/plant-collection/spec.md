# Spec Delta

## Purpose

Households and the plants they own: how a plant is registered, how it is identified and located, and the invariants the Garden context refuses to break.

## ADDED Requirements

### Requirement: A plant belongs to exactly one household

Registering a plant SHALL record its household, its species, a name and a location together, and a plant SHALL belong to exactly one household for its whole life.

#### Scenario: A plant is registered

- **WHEN** a household registers a plant with a species, a name and a location
- **THEN** the plant exists with that household, species, name and location, and it is visible in the household's plant list

#### Scenario: A plant is moved

- **WHEN** a registered plant's location changes
- **THEN** the plant keeps its identity and household, and only its location changes

### Requirement: Identifiers are typed

Every aggregate SHALL be identified by a typed identifier — its own, or that of the aggregate it belongs to — and an identifier of one type SHALL never compare equal to, or be accepted in place of, an identifier of another type.

#### Scenario: An aggregate shares its parent's identifier

- **WHEN** an aggregate is identified by the identifier of the aggregate it belongs to
- **THEN** that identifier is a typed value rather than a bare string, and it is the same type its parent uses

#### Scenario: A sensor id is used where a plant id belongs

- **WHEN** a sensor identifier is supplied to an operation that expects a plant identifier
- **THEN** the value is rejected rather than silently coerced

### Requirement: A plant's name and location are non-empty and bounded

A plant's name SHALL NOT be blank, and its location SHALL be a non-blank value of at most 100 characters after trimming surrounding whitespace.

#### Scenario: A blank name is supplied

- **WHEN** a plant is registered with an empty or whitespace-only name
- **THEN** the registration is refused

#### Scenario: A location is over-long

- **WHEN** a location longer than 100 characters is supplied
- **THEN** the value is refused at the boundary

### Requirement: A household owns at most fifty plants

A household SHALL NOT hold more than fifty plants at one time.

#### Scenario: The fifty-first plant is added

- **WHEN** a household that already holds fifty plants registers another
- **THEN** the registration is refused

#### Scenario: The fiftieth plant is added

- **WHEN** a household that holds forty-nine plants registers one more
- **THEN** the registration succeeds, boundaries being inclusive

### Requirement: Watering twice within an hour is refused

The Garden context SHALL refuse a second watering of the same plant within one hour of the previous one, and SHALL allow one exactly one hour later.

#### Scenario: A plant is watered twice in quick succession

- **WHEN** a plant that was watered ten minutes ago is watered again
- **THEN** the second watering is refused

#### Scenario: A plant is watered exactly an hour later

- **WHEN** a plant is watered exactly one hour after its previous watering
- **THEN** the watering is allowed

### Requirement: Repotting more often than every 182 days is refused

The Garden context SHALL refuse a repotting within 182 days of the previous one, and SHALL allow one exactly 182 days later.

#### Scenario: A plant is repotted too soon

- **WHEN** a plant repotted thirty days ago is repotted again
- **THEN** the repotting is refused

### Requirement: Garden records what a plant is, not what was done to it

Registering, moving and removing a plant SHALL each publish exactly one event, and the Garden context SHALL NOT publish an event for watering or repotting, because those facts belong to the contexts that own care and the journal.

#### Scenario: A plant is registered

- **WHEN** a plant is registered
- **THEN** a plant-added event is published carrying the plant, household, species, name, location and the moment it was added

#### Scenario: A plant is repotted

- **WHEN** a plant is repotted
- **THEN** no Garden event is published, and the fact is recorded by the journal instead

### Requirement: A removed plant stops being part of the household

Removing a plant SHALL publish a plant-removed event carrying the plant and the moment, and the plant SHALL no longer appear as an active plant of the household.

#### Scenario: A plant is removed

- **WHEN** a plant is removed
- **THEN** a plant-removed event is published and the household's active plant list no longer contains it
