# repository-presentation Specification

## Purpose

Defines what this repository shows a visitor: how the project presents itself, how a contributor is guided into the workflow, and where release history is kept.

## Requirements

### Requirement: The repository root holds no roadmap or changelog document

The repository root SHALL NOT contain a roadmap document or a changelog document. Planned work SHALL live in OpenSpec changes, and release history SHALL live in GitHub Releases.

#### Scenario: The root is listed

- **WHEN** the repository root is listed
- **THEN** there is no `ROADMAP.md` and no `CHANGELOG.md`

#### Scenario: A visitor looks for planned work

- **WHEN** a visitor wants to know what is planned or in flight
- **THEN** the in-flight work is visible through the OpenSpec change list rather than a roadmap file

### Requirement: Nothing links to a removed root document

Every reference to the removed roadmap and changelog SHALL be deleted or repointed, and no tracked file SHALL link to a path that does not exist.

#### Scenario: The repository is searched for the removed names

- **WHEN** the tracked content of the repository is searched for `ROADMAP.md` or `CHANGELOG.md`
- **THEN** no tracked file references either path

#### Scenario: A source docstring cites roadmap history

- **WHEN** a source docstring or test docstring cites a roadmap phase or task as its rationale
- **THEN** the citation points at the document or capability spec that now carries that information, or the citation is removed

### Requirement: The README presents the project

The README SHALL state the project title, a short plain-language description, badges, the key features, a quick start, how OpenSpec is used in this repository, the repository layout, how to contribute, the license, and where release notes live.

#### Scenario: A newcomer reads the README

- **WHEN** a visitor reads the README from top to bottom
- **THEN** they can say what the project is, what it demonstrates, how to run it, how changes are proposed, and under which license, without opening another file

#### Scenario: A visitor looks for release notes

- **WHEN** a visitor wants to know what changed in a released version
- **THEN** the README points them at the repository's GitHub Releases

#### Scenario: A badge claims something untrue

- **WHEN** the README renders its badges
- **THEN** every badge states a fact that is true of this repository, and no badge claims a continuous-integration status the project does not have

### Requirement: The contributing guide describes the OpenSpec lifecycle

The contributing guide SHALL describe the change lifecycle as propose, review, implement, archive, and SHALL NOT instruct contributors to maintain a changelog.

#### Scenario: A contributor reads the contributing guide

- **WHEN** a contributor wants to change something
- **THEN** they are told to create an OpenSpec change with its planning artifacts before writing code

#### Scenario: The pull-request checklist is read

- **WHEN** the contributing guide's pull-request checklist is read
- **THEN** it contains no step that updates a changelog file

### Requirement: Issue and pull-request templates reference OpenSpec

The repository SHALL provide a pull-request template and at least one issue template under `.github/`, and each SHALL reference the OpenSpec change the work belongs to. The templates SHALL stay minimal and SHALL NOT add process the project does not enforce.

#### Scenario: A pull request is opened

- **WHEN** a contributor opens a pull request
- **THEN** the template asks which OpenSpec change it implements and whether the lint and test gates pass

#### Scenario: An issue is opened

- **WHEN** a contributor opens an issue
- **THEN** a template is offered that asks whether the work needs an OpenSpec change

### Requirement: Release history is kept in GitHub Releases

User-facing release history SHALL be recorded by publishing a GitHub Release for a tagged version, and SHALL NOT be recorded by editing a file in the repository.

#### Scenario: A version is released

- **WHEN** a maintainer publishes a version
- **THEN** the release notes are written on the GitHub Release and no repository file is edited to record them

#### Scenario: A contributor finishes a user-facing change

- **WHEN** a contributor completes a change that a user would notice
- **THEN** no step asks them to add an entry to a changelog file

### Requirement: Agent commit tooling describes this repository

The commit skill at `.agents/skills/commit/SKILL.md` SHALL describe this repository. The workspace layout, commit scopes, secret sources, ignored paths, generated artifacts and build gates it names SHALL all be true of PlantKeeper, and it SHALL follow the OpenSpec commit sequencing this repository uses.

#### Scenario: The skill is read

- **WHEN** the commit skill is read
- **THEN** it names no package, tool, secret source, coverage floor or build gate belonging to a different project

#### Scenario: A scope is chosen for a commit

- **WHEN** a contributor picks a commit scope
- **THEN** the scopes the skill lists are scopes this repository actually uses

#### Scenario: The skill's safety gate is followed

- **WHEN** an agent follows the skill's pre-commit gate
- **THEN** the commands it names exist in this repository's `Makefile` and the paths it warns about are the paths this repository ignores
