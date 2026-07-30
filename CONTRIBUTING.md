# Contributing to joule

## Project constraints

- **Language:** Rust only for first-party source and dependencies (strict).
- **Versioning:** [Semantic Versioning 2.0.0](https://semver.org/).
- **Commits:** [Conventional Commits](https://www.conventionalcommits.org/).
- **License:** MIT.
- **User-facing docs:** Follow the project language stack (clear, plain, consistent terms).

## Development

1. Use the declared toolchain and lockfiles.
2. Format with the language standard formatter before commit.
3. Install git hooks: `pre-commit install --hook-type pre-commit --hook-type commit-msg` (requires [pre-commit](https://pre-commit.com)).
4. Run the test suite and fix failures before you open a pull request.
5. Keep changes focused. Do not mix unrelated refactors.

See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) if you expected a corporate CoC — there is not one. Short version: do not be a dick for no reason; ship good code.

## Pull requests

- Use the PR template.
- State SemVer impact (patch / minor / major / none).
- Update `CHANGELOG.md` for user-visible changes.
- Update user docs (README, man page(s), GitHub Pages under `docs/`, and help text) in the same PR when behavior changes. Keep those surfaces in sync.
- On a SemVer **release**: refresh root `file_id.diz` (ACiD / 16colo.rs-style scene card), sync the README and Pages previews, list it in the man page, and attach `file_id.diz` to the GitHub Release with the changelog.
- Install paths: every method installs man pages; keep the curl-from-releases install script accurate; document only package channels this project maintains.

## Issues

- Use the bug or feature issue templates.
- Search existing issues before opening a new one.

## Security

See [SECURITY.md](SECURITY.md). Do not file public issues for vulnerabilities.
