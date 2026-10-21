"""Allow ``python -m logplane`` as an alternative to the ``logplane`` command."""

from logplane.cli import main

raise SystemExit(main())
