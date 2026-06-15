# Releasing

tern is **not yet published to Hex**. This file is the checklist for when it is.

## Prerequisites (blockers)

1. **wren must be on Hex first.** `tern/consumer` depends on
   [wren](https://github.com/cargopete/wren) via a **git** dependency, and Hex packages
   may not have git dependencies. So either:
   - publish wren to Hex and switch `gleam.toml` to a version requirement, **or**
   - move `tern/consumer` (and `consume_demo`) into a separate `tern_wren` package, so the
     core `tern` package has no wren dependency and can publish independently.
2. **Permission.** tern is a clean-room, generic reimplementation of ideas from a
   production lineage service — no proprietary code or data. Even so, get a written OK
   from the relevant employer before publishing publicly (the same nod wren needs).
   Publishing to Hex is **public and permanent**.

## Checklist

- [ ] Resolve the wren dependency (see above)
- [ ] Written permission obtained
- [ ] `gleam format --check src test` clean
- [ ] `TERN_IT=1 gleam test` green (CI does this against real AGE + RabbitMQ)
- [ ] `gleam docs build` produces clean docs
- [ ] Bump the version in `gleam.toml`, update `CHANGELOG.md`
- [ ] `gleam publish`
