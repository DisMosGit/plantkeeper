# Spec Delta

## MODIFIED Requirements

### Requirement: Notification delivery does not depend on an external channel

Notifications SHALL be delivered only to clients connected to the API — whether by stream or by request and wait — and the platform SHALL NOT send them through electronic mail, a chat service or a mobile push provider.

#### Scenario: A household has no client connected

- **WHEN** a notification is created while nobody is waiting for that household
- **THEN** it is stored and delivered when a client next asks or reconnects

## ADDED Requirements

### Requirement: Notifications are delivered by stream, with request and wait as a fallback

A client SHALL be able to open a stream of a household's unread notifications over the API and receive each notification as it appears, without holding a database session while nothing happens, and SHALL be able to resume the stream from the last notification it saw. The request-and-wait form SHALL remain available: it SHALL answer immediately when something is to deliver, SHALL answer with an empty result once the wait has elapsed with nothing to deliver, and SHALL reject a wait length outside the supported bounds.

#### Scenario: Something is already waiting

- **WHEN** a client requests pending notifications and unread ones exist
- **THEN** they are returned without waiting

#### Scenario: A notification appears on an open stream

- **WHEN** a notification is created for a household whose stream is open
- **THEN** the client receives it without re-requesting, and the content still comes from the application's own tables

#### Scenario: A stream reconnects

- **WHEN** a client reconnects and names the last notification it saw
- **THEN** it receives what it missed and nothing it already saw

#### Scenario: Nothing appears during the wait

- **WHEN** a client waits for the permitted maximum and nothing is created
- **THEN** the request answers with an empty result rather than hanging

#### Scenario: An unsupported wait is requested

- **WHEN** a client requests a wait length outside the supported bounds
- **THEN** the request is rejected

## REMOVED Requirements

### Requirement: Notifications are delivered by long polling

**Reason**: Long polling as the only delivery channel spends a request and a database session per waiting client to simulate a push the platform's own broker could carry, and caps every waiter at the endpoint's maximum wait.

**Migration**: The stream endpoint becomes the primary delivery channel over the same payload-free household signal; the request-and-wait behaviour of the removed requirement is preserved verbatim as the fallback form in the replacement requirement.
