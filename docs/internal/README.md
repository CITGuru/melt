# `docs/internal/` — gitignore exclude rule

The repository's top-level `.gitignore` contains:

```gitignore
/docs/internal/*
!/docs/internal/KNOWN_ISSUES.md
```

That rule ignores every file dropped here **except** `KNOWN_ISSUES.md`. So if you add a new file to this directory and want it tracked, add a matching `!/docs/internal/<your-file>` exception alongside the existing one. Otherwise `git add` will silently skip it and the file will look untracked locally without ever reaching review.

`KNOWN_ISSUES.md` is the operator-visible punch list — kept inside `docs/internal/` because the rest of this directory is for low-traffic engineering scratch (POWA design docs, in-flight notes, etc.) but the `KI-*` entries themselves are the public reference for proxy bugs. See `docs/SEED_MODE.md` for the operator-facing version of the seed-mode fix.

This file itself is force-included via the same exception so it doesn't get silently dropped.
