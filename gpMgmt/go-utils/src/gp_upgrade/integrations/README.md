## Naming

If we want to optimize for find-ability, we should have integration tests
corresponding to modules or command functionalities

But if we want to optimize for "how much setup is needed", we should group them
according to that categorization.

Up for debate -- feel free to reorganize.

## Gotchas

Leaving a process open in a Makefile is not a good thing and will cause the make to wait for it's forked processes to finish.
Integration tests should be careful about cleaning up after themselves, make sure to kill and hub or agent that you started.
