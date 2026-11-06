//! A program a node starts on the computer and reaches from outside it
//! (an app): the computer hands `/apps/<name>/...` on to the loopback
//! port the app wrote into its port file, and at each start of the
//! computer runs every app's start script again. A package using this
//! one calls it as `computer::apps`.
//!
//! An app's name holds only letters, digits, `-` and `_`. In `DIR`, the
//! file `<name>` holds the app's port, `<name>.start` is the bash script
//! that starts the app unless it is up, `<name>.remove` the bash script
//! that stops it and deletes everything it keeps, which the computer
//! runs when `DELETE /apps/<name>` removes the app (weft terminating the
//! node that started it), and `<name>.lock` is the file whoever runs
//! either script holds an flock on, so two starts at once start one app
//! and a start never runs beside a removal.

// SYNC: the computer's apps <-> catalog/computer/computer/images/computer/computer.py (APPS_PREFIX, APPS_DIR,
//       START_SUFFIX, REMOVE_SUFFIX, LOCK_SUFFIX, start_app, remove_app)
/// The path an app answers under on the computer's endpoint
/// (`/apps/<name>`).
pub const PREFIX: &str = "/apps";
/// The folder of the apps' files, as a shell command in the computer
/// writes it.
pub const DIR: &str = "$HOME/.weft/apps";
pub const START_SUFFIX: &str = ".start";
pub const REMOVE_SUFFIX: &str = ".remove";
pub const LOCK_SUFFIX: &str = ".lock";
