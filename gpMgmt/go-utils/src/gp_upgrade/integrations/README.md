## Naming

We want to optimize for find-ability, so we should have integration tests
corresponding to modules or command functionalities

If we want to optimize for "how much setup is needed", we should write setup
steps that are idempotent and focused to the areas that need them

## "Vanilla" Test

Most tests will differ in their setup in a clear way for a clear reason. Besides that, they should try to be as "vanilla" as possible to isolate the use case / user flow.

The integration test setup is currently such that the "vanilla" test:

 - Is cool with a hub being up already. If it needs a hub up, it can ensure that it's up
 - Is cool with an empty home / config directory, because we currently clean that between runs.

## Gotchas

Leaving a process open in a Makefile is not a good thing and will cause the make to wait for it's forked processes to finish.
Integration tests should be careful about cleaning up after themselves, make sure to kill and hub or agent that you started.
