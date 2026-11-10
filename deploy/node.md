# Node deploy (macOS)

A macOS node runs `~/.local/bin/wrkspc node run …` under one of two
supervisors, both per-machine setup outside this repository:

- the launcher app `~/Applications/wrkspc-node.app`, which starts the
  node again whenever it exits;
- a launchd LaunchAgent labelled `io.beardedeagle.wrkspc-node`.

`just deploy-node` installs a new build under whichever one the node
has; when both are present it uses the launcher.

## Deploy

```bash
just quality                      # builds target/release/wrkspc
just deploy-node                  # this Mac
just deploy-node your-node.local  # another Mac, over ssh
```

For another Mac the binary is copied with `scp` and the install runs
there through `ssh -o BatchMode=yes`, so key-based login must already
work. On the node, the recipe:

1. refuses unless the launcher is running or the launchd job is loaded —
   otherwise nothing would restart the node, so nothing is changed;
2. ad-hoc signs the new binary with identifier `io.beardedeagle.wrkspc`;
3. copies the installed binary to `~/.local/bin/wrkspc.prev`, unless the
   new build is identical (a repeat deploy keeps the last real rollback
   point);
4. moves the new binary into place and restarts the node — under the
   launcher by stopping it (the launcher starts it again), under launchd
   with `launchctl kickstart -k` — then waits up to 150 s for a NEW node
   process to own its UI listener (default port 8081; this pilot's
   nodes bind 8083, so both are checked, and the listener owner is
   verified with lsof — an unrelated process on the port never
   certifies the deploy).

## Keychain prompt

The node keeps its operator UI token in the login Keychain. An ad-hoc
signature identifies the binary only by its code hash, so the Keychain
treats every new build as a different program and asks before handing
it the token. After a deploy, expect a dialog for `wrkspc` on that node.
The node waits 10 seconds for **Allow** and then exits: the launcher
starts it again and the dialog returns, while a launchd job without
`KeepAlive` leaves it stopped. When the recipe times out on another Mac,
the dialog is usually on that machine's screen — answer it, and rerun
the deploy if the node stopped. Redeploying an identical build keeps
the code hash and does not prompt.

## Rollback

On the node, restore the previous binary, then restart the node through
its supervisor:

```bash
mv -f ~/.local/bin/wrkspc.prev ~/.local/bin/wrkspc
pkill -f 'wrkspc node run'                                         # launcher
launchctl kickstart -k "gui/$(id -u)/io.beardedeagle.wrkspc-node"  # launchd
```
