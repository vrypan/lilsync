# Running lilsync as a systemd service

`lilsync` manages its own background daemon via `start`/`stop`, but running
it under systemd instead gets you automatic restarts, boot-time startup, and
`journalctl` logging. The units here use `lilsync watch` (foreground) as
`ExecStart`, since `start` forks into the background and would leave systemd
unable to track the process.

## Single folder

Use `lilsync.service`:

1. Edit `User=` and the `ExecStart` folder path.
2. Install and enable it:

   ```bash
   sudo cp lilsync.service /etc/systemd/system/lilsync.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now lilsync
   ```

## Multiple folders (named instances)

To sync more than one folder, use the template unit `lilsync@.service` and
run one instance per folder, identified by the `%i` instance name.

1. Edit `User=` in `lilsync@.service`, then install it:

   ```bash
   sudo cp lilsync@.service /etc/systemd/system/lilsync@.service
   ```

2. For each folder, create a config file under `/etc/lilsync/<name>.conf`
   (see `photos.conf.example`):

   ```bash
   sudo mkdir -p /etc/lilsync
   sudo install -m 0644 photos.conf.example /etc/lilsync/photos.conf
   # edit /etc/lilsync/photos.conf to point LILSYNC_FOLDER at the real path
   ```

3. Enable and start each named instance — the part after `@` (e.g. `photos`)
   selects which `.conf` file is loaded and becomes the instance name for
   `systemctl`/`journalctl`:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now lilsync@photos
   sudo systemctl enable --now lilsync@docs
   ```

4. Check status and logs per instance:

   ```bash
   systemctl status lilsync@photos
   journalctl -u lilsync@photos -f
   ```

5. Stop or disable a single instance without affecting the others:

   ```bash
   sudo systemctl stop lilsync@photos
   sudo systemctl disable lilsync@photos
   ```

Pass any extra flags (`--name`, `--poll`, `--interval-ms`,
`--announce-interval-secs`) directly in `ExecStart`, or add them to the
per-instance `.conf` file and reference them the same way as
`${LILSYNC_FOLDER}`.
