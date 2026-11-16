# Accessibility

Accessibility matters to wrkspc because the operator UI and the
documentation are how every user — including people using assistive
technology — participates in a shared workspace. This document
records our accessibility commitment, what contributors are expected
to do, and how to report a barrier. It is written for someone who has
just hit a wall, not for a standards audit.

## Priorities

- **Accessibility target:** we work toward WCAG 2.2 Level AA. This
  target guides our work but is an aspiration, not a claim of
  verified conformance — no evaluation has been completed yet.
- **Keyboard support** — interactive elements must be reachable and
  operable by keyboard alone, with a visible focus indicator and a
  logical tab order.
- **Screen reader support** — content uses semantic HTML and a
  logical heading hierarchy so it can be navigated with VoiceOver,
  NVDA, and similar tools.
- **Readable content** — plain language, descriptive link text,
  sufficient color contrast, and meaningful alternative text.

## Contributor expectations

- **For UI changes:** run an automated accessibility check (such as
  axe DevTools or the GitHub Accessibility Scanner) and do at least
  one keyboard-only pass: logical tab order, visible focus, no
  keyboard traps, every action reachable by keyboard.
- **For content:** keep a logical heading hierarchy, use real list
  elements, write unique descriptive link text, provide alternative
  text for images, add column and row headers to tables, and never
  use color as the only way to convey meaning.
- **CI today:** `just lint-md` enforces markdown structure but is not
  an accessibility scanner. No automated accessibility gate exists
  yet; the expectations above are the gate for now, and a scanner is
  an open improvement.

## Reporting accessibility issues

If you hit an accessibility barrier, report it — we treat these
reports as expertise, not complaints. You never need to disclose a
diagnosis or personal information.

1. Open an issue with the
   [accessibility issue template](https://github.com/beardedeagle/wrkspc/issues/new?template=accessibility.yml).
2. Include, when possible: what you were trying to do, the page or
   route, steps to reproduce, your operating system, browser, and
   assistive technology with versions, and a screenshot or recording
   if you're comfortable sharing one.

### Severity

- **Critical:** prevents completing a core task (for example, the
  conversation view is unusable).
- **Serious:** significant difficulty, but a workaround exists.
- **Moderate:** annoyance or inconsistent experience.
- **Minor:** minor issue with minimal impact on usability.

### How we respond

- We acknowledge the report promptly and respectfully, and thank the
  reporter.
- We never require disclosure of a diagnosis or other personal
  information.
- We explain next steps, known limitations, and relevant
  dependencies.
- Where possible, we provide a workaround while a fix is in progress.
- We may ask you to confirm a fix resolves the barrier before closing
  the issue.

### Resolution expectations

This is a small, single-maintainer project. We aim to resolve
critical barriers within 30 days, serious within 60, and moderate or
minor within 90, counting from triage. When we cannot meet a target,
we will say so, explain the delay, share any workaround, and give a
revised date.

## Ownership and maintenance

Accessibility is owned by the maintainer, reachable by email at
[randy@heroictek.com](mailto:randy@heroictek.com), who
triages accessibility reports, tracks known barriers, and keeps this
statement current. If ownership changes, this section is updated in
the same change.

## Supported environments

The operator UI is a desktop-browser dashboard and the documentation
is static HTML. We intend them to work in the latest two versions of
Chrome, Edge, Firefox, and Safari on desktop, with VoiceOver (macOS),
NVDA (Windows), and JAWS (Windows).

Partial-support notes, stated plainly:

- No screen-reader verification pass has been completed on any
  surface.
- The UI assumes a pointer and a full-size viewport; no responsive or
  touch layout exists.
- Color carries information in places (peer colors, state badges);
  it is paired with text labels, but the pairing has not been
  independently verified.

## Known limitations

- No dedicated screen-reader audit (VoiceOver/NVDA/JAWS) has been
  run against the operator UI or the documentation.
- No automated accessibility testing is wired into the test suite.
- The operator UI has no responsive or touch layout.
- The embedded rustdoc API reference uses rustdoc's stock template;
  its accessibility posture is upstream's, not something wrkspc has
  independently evaluated.

If you find a barrier that is not listed here, please report it —
that is how this section grows.

## Feedback

Accessibility is ongoing practice, not a one-time fix. Suggestions
for this statement or our practices are welcome as issues or pull
requests. Thank you for helping make wrkspc usable by everyone.
