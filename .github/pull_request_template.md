<!-- markdownlint-disable MD041 -->
## Summary

<!-- What does this PR change, and why? Reference any related issue,
     e.g. "Closes #123". -->

## How was this verified?

<!-- Describe how you tested the change and paste the relevant command output. -->

## Checklist

- [ ] The diff is focused -- one logical change.
- [ ] `just quality` passes locally (formatting, clippy `-D warnings`
  across profiles, tests, builder-path audit, dependency policy,
  audits, OSV scan).
- [ ] New or changed behavior is covered by a test that would fail
  without this change.
- [ ] Documentation changes: `just docs` (book + embedded rustdoc) and
  `just lint-md` pass, and the touched pages match their source of
  truth (router for `api.md`, clap structs for `cli.md`, skill
  registry for `harnesses.md`).
- [ ] Release-pipeline changes: DISTRIBUTION.md, the
  installation/distribution/gates pages, and CHANGELOG.md are swept;
  the workflow files pass actionlint and zizmor.
- [ ] Commit messages have an imperative subject and explain *why*,
  with no AI attribution or co-author trailers.
- [ ] I have read the contributing guidelines.
