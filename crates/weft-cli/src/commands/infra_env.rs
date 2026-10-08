//! `weft infra env <node> --into <file> --set NAME=Label ... [--as NAME]`:
//! write what an infra node's card shows into an env file, every name in
//! one go, a secret without its value ever passing through a screen.
//!
//! Generic over nodes: the card is whatever the node's display (`/live`)
//! serves. `--set NAME=Label` takes the item under that label, whatever
//! its kind; `--as NAME` takes the card's only `secret` item. A node that
//! hands its secret out once replaces that item with a line carrying a
//! button (the Postgres database's "Reset password"); this refuses then
//! and points at `weft infra press`, which presses it.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use weft_core::live::{LiveAction, LiveFeed, LiveItem, LiveItemKind};

use super::infra_card::{item_text, Card};
use super::Ctx;

pub struct EnvArgs {
    pub node: String,
    pub into: PathBuf,
    /// `NAME=Label` pairs, as typed.
    pub set: Vec<String>,
    /// The name for the card's only secret.
    pub secret_as: Option<String>,
    pub instance: Option<weft_core::instance::InstanceId>,
}

/// Which item a name takes its value from.
#[derive(Debug, PartialEq)]
enum Want {
    /// The item under this label.
    Label(String),
    /// The card's only secret.
    OnlySecret,
}

/// Every `(NAME, what it takes)` the command line asks for, names
/// checked and none asked twice.
fn wanted(set: &[String], secret_as: Option<&str>) -> Result<Vec<(String, Want)>> {
    let mut out: Vec<(String, Want)> = Vec::new();
    for pair in set {
        let Some((name, label)) = pair.split_once('=') else {
            bail!("--set '{pair}' names no card item: write it NAME=Label, e.g. DATABASE_USER=User");
        };
        out.push((name.trim().to_string(), Want::Label(label.trim().to_string())));
    }
    if let Some(name) = secret_as {
        out.push((name.to_string(), Want::OnlySecret));
    }
    if out.is_empty() {
        bail!("nothing to write: name what goes in with --set NAME=Label (repeatable), or --as NAME for the card's only secret");
    }
    for (i, (name, _)) in out.iter().enumerate() {
        check_env_name(name)?;
        if out[..i].iter().any(|(n, _)| n == name) {
            bail!("{name} is asked for twice; each name takes one value");
        }
    }
    Ok(out)
}

/// What the card says about the item asked for.
#[derive(Debug, PartialEq)]
enum Pick<'a> {
    /// The value is there to take.
    Value(&'a LiveItem),
    /// The node handed a secret over already; this item's button gets a
    /// new one.
    HandedOver(&'a LiveItem, &'a LiveAction),
}

/// Find an item in a card.
///
/// A secret handed over already shows as another kind of item under the
/// same label, carrying the button that makes it readable again, so a
/// non-secret line with a button is that state, never a value to write.
/// With no label, that item is recognized as the only one with a button,
/// so a node with one secret works with `--as` whichever state it is in.
fn pick<'a>(feed: &'a LiveFeed, want: &Want) -> Result<Pick<'a>> {
    let labels = |items: &[&LiveItem]| {
        items.iter().map(|i| format!("'{}'", i.label)).collect::<Vec<_>>().join(", ")
    };
    let all: Vec<&LiveItem> = feed.items.iter().collect();
    match want {
        Want::Label(label) => match feed.items.iter().find(|i| &i.label == label) {
            Some(item) => match (&item.kind, &item.action) {
                (LiveItemKind::Secret, _) | (_, None) => Ok(Pick::Value(item)),
                (_, Some(action)) => Ok(Pick::HandedOver(item, action)),
            },
            None if all.is_empty() => bail!("the node shows nothing, so there is no '{label}' to write"),
            None => bail!("the node shows no '{label}'; it shows {}", labels(&all)),
        },
        Want::OnlySecret => {
            let secrets: Vec<&LiveItem> = feed.items.iter().filter(|i| i.kind == LiveItemKind::Secret).collect();
            match secrets.as_slice() {
                [only] => Ok(Pick::Value(only)),
                [] => {
                    let buttons: Vec<(&LiveItem, &LiveAction)> =
                        feed.items.iter().filter_map(|i| Some((i, i.action.as_ref()?))).collect();
                    match buttons.as_slice() {
                        [(item, action)] => Ok(Pick::HandedOver(item, action)),
                        _ if all.is_empty() => bail!("the node shows nothing, so there is no secret to write"),
                        _ => bail!(
                            "the node shows no secret right now; name the line with --set NAME=Label (it shows {})",
                            labels(&all)
                        ),
                    }
                }
                many => bail!("the node shows several secrets; name each with --set NAME=Label ({})", labels(many)),
            }
        }
    }
}

/// `NAME` as an env file and a shell both accept it.
fn check_env_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if !first_ok || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("'{name}' is not an environment variable name: use letters, digits and '_', not starting with a digit");
    }
    Ok(())
}

/// The value as it goes after `NAME=` in a `.env` file, read back the
/// same by dotenv, Vite, Next.js, Docker Compose and a shell `source`.
///
/// Bare when nothing in it needs quoting; single quotes otherwise, which
/// every reader takes literally; double quotes only for a value holding a
/// `'`. A value no quoting carries the same way everywhere (a line break,
/// or both a `'` and a `$`) is refused rather than written so that one
/// reader gets something else.
fn quote_env_value(value: &str) -> Result<String> {
    if value.contains(['\n', '\r']) {
        bail!("the value spans several lines, which an env file cannot hold the same way for every reader");
    }
    let bare = !value.is_empty()
        && value.chars().all(|c| c.is_ascii_alphanumeric() || "_-./:@%+,=".contains(c));
    if bare {
        return Ok(value.to_string());
    }
    if !value.contains('\'') {
        return Ok(format!("'{value}'"));
    }
    if value.contains(['$', '`']) {
        bail!("the value holds both a quote (') and a '$' or '`', which no env file quoting reads back the same everywhere");
    }
    Ok(format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
}

/// Does this line set `name` (`NAME=...` or `export NAME=...`)?
fn sets(line: &str, name: &str) -> bool {
    let line = line.trim_start();
    let line = line.strip_prefix("export ").map(str::trim_start).unwrap_or(line);
    line.strip_prefix(name).is_some_and(|rest| rest.trim_start().starts_with('='))
}

/// The file's contents with each name set to its already quoted value:
/// the first line setting it replaced in place (an `export` kept), any
/// later line setting it again dropped so none of them overrides this
/// one, and a new line at the end, in the order given, for a name
/// nothing set yet. Every other line stays as it was.
fn merge_env(existing: &str, pairs: &[(String, String)]) -> String {
    let mut out = Vec::new();
    let mut written = vec![false; pairs.len()];
    for line in existing.lines() {
        match pairs.iter().position(|(name, _)| sets(line, name)) {
            None => out.push(line.to_string()),
            Some(i) if !written[i] => {
                let (name, quoted) = &pairs[i];
                let export = if line.trim_start().starts_with("export ") { "export " } else { "" };
                out.push(format!("{export}{name}={quoted}"));
                written[i] = true;
            }
            Some(_) => {}
        }
    }
    for ((name, quoted), done) in pairs.iter().zip(written) {
        if !done {
            out.push(format!("{name}={quoted}"));
        }
    }
    let mut text = out.join("\n");
    text.push('\n');
    text
}

/// Where the value goes: `--into` checked to be a file on disk. A
/// symlink is followed, so the file it points at is the one updated.
fn target(into: &Path) -> Result<PathBuf> {
    if into.as_os_str() == "-" {
        bail!("--into - would print the values; name a file");
    }
    match std::fs::metadata(into) {
        Ok(meta) if meta.is_file() => {
            std::fs::canonicalize(into).with_context(|| format!("resolve {}", into.display()))
        }
        Ok(_) => bail!("{} is not a regular file (a terminal, a directory, a device); name a file", into.display()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(into.to_path_buf()),
        Err(e) => Err(e).with_context(|| format!("read {}", into.display())),
    }
}

/// Write every `(name, value)` into the env file at `path`, all at once:
/// a temp file beside it, renamed over it, so a reader never sees half a
/// file. A new file is readable by its owner only; an existing one keeps
/// its mode.
fn write_env(path: &Path, pairs: &[(String, String)]) -> Result<()> {
    let quoted = pairs
        .iter()
        .map(|(name, value)| Ok((name.clone(), quote_env_value(value).with_context(|| format!("write {name}"))?)))
        .collect::<Result<Vec<_>>>()?;
    let (existing, mode) = match std::fs::read_to_string(path) {
        Ok(text) => {
            let mode = std::fs::metadata(path)?.permissions();
            (text, Some(mode))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (String::new(), None),
        Err(e) => return Err(e).with_context(|| format!("read {}", path.display())),
    };
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let mut tmp = tempfile::NamedTempFile::new_in(&dir)
        .with_context(|| format!("create a temp file in {}", dir.display()))?;
    let perms = match mode {
        Some(mode) => mode,
        None => {
            use std::os::unix::fs::PermissionsExt;
            std::fs::Permissions::from_mode(0o600)
        }
    };
    std::fs::set_permissions(tmp.path(), perms)?;
    tmp.write_all(merge_env(&existing, &quoted).as_bytes())?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

pub async fn run(ctx: Ctx, args: EnvArgs) -> Result<()> {
    let wanted = wanted(&args.set, args.secret_as.as_deref())?;
    let path = target(&args.into)?;
    let card = Card::open(&ctx, &args.node, args.instance.as_ref()).await?;
    let feed = card.read().await?;
    let mut pairs = Vec::new();
    let mut written = Vec::new();
    for (name, want) in &wanted {
        let item = match pick(&feed, want)? {
            Pick::Value(item) => item,
            Pick::HandedOver(item, action) => bail!("{}", handed_over(&card.place, item, action, ctx.on())),
        };
        let value = item_text(item);
        written.push(match item.kind {
            LiveItemKind::Secret => (name.clone(), None),
            _ => (name.clone(), Some(value.clone())),
        });
        pairs.push((name.clone(), value));
    }
    write_env(&path, &pairs)?;
    let shown = args.into.display().to_string();
    let report = serde_json::json!({
        "file": shown,
        "node": card.place,
        "env": written
            .iter()
            .map(|(name, value)| match value {
                Some(value) => serde_json::json!({ "name": name, "value": value }),
                None => serde_json::json!({ "name": name, "secret": true }),
            })
            .collect::<Vec<_>>(),
    });
    if !ctx.json_out(&report)? {
        let names = written
            .iter()
            .map(|(name, value)| match value {
                Some(value) => format!("{name}={value}"),
                None => format!("{name} (secret, not shown)"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        println!("wrote {names} to {shown}");
    }
    Ok(())
}

/// The refusal for a secret the node handed over already: what its card
/// says there, and the button that gets a new one.
fn handed_over(place: &str, item: &LiveItem, action: &LiveAction, on: Option<&str>) -> String {
    format!(
        "{place}'s card shows no secret under '{}' right now; it says \"{}\". To get a new one, press the card's \
         button (`{}` lists them, `{}` presses '{}'), then run this again",
        item.label,
        item_text(item),
        super::weft_on(on, &format!("infra show {place}")),
        super::weft_on(on, &format!("infra press {place} {}", action.action_kind)),
        action.label,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reset_button() -> LiveAction {
        LiveAction::new("Reset password", "reset_password")
    }

    fn fresh() -> LiveFeed {
        LiveFeed::new(vec![
            LiveItem::text("User", "weft"),
            LiveItem::secret("Password", "s3cret").with_action(reset_button()),
        ])
    }

    fn handed_over() -> LiveFeed {
        LiveFeed::new(vec![
            LiveItem::text("User", "weft"),
            LiveItem::text("Password", "handed over").with_action(reset_button()),
        ])
    }

    fn label(l: &str) -> Want {
        Want::Label(l.to_string())
    }

    #[test]
    fn the_only_secret_is_picked_without_a_label() {
        let feed = fresh();
        assert_eq!(pick(&feed, &Want::OnlySecret).unwrap(), Pick::Value(&feed.items[1]));
        assert_eq!(pick(&feed, &label("Password")).unwrap(), Pick::Value(&feed.items[1]));
    }

    #[test]
    fn a_plain_line_is_picked_by_its_label() {
        let feed = fresh();
        assert_eq!(pick(&feed, &label("User")).unwrap(), Pick::Value(&feed.items[0]));
    }

    #[test]
    fn the_command_line_is_read_into_names_and_items() {
        let set = vec!["DATABASE_USER=User".to_string(), "DB_PW = Password".to_string()];
        assert_eq!(
            wanted(&set, Some("PG")).unwrap(),
            vec![
                ("DATABASE_USER".to_string(), label("User")),
                ("DB_PW".to_string(), label("Password")),
                ("PG".to_string(), Want::OnlySecret),
            ]
        );
        assert!(wanted(&[], None).unwrap_err().to_string().contains("--set NAME=Label"));
        assert!(wanted(&["USER".to_string()], None).unwrap_err().to_string().contains("NAME=Label"));
        assert!(wanted(&["1X=User".to_string()], None).is_err());
        let twice = wanted(&["PG=User".to_string()], Some("PG")).unwrap_err().to_string();
        assert!(twice.contains("twice"), "{twice}");
    }

    #[test]
    fn a_handed_over_secret_is_the_item_with_the_button() {
        let feed = handed_over();
        assert_eq!(pick(&feed, &Want::OnlySecret).unwrap(), Pick::HandedOver(&feed.items[1], feed.items[1].action.as_ref().unwrap()));
        assert_eq!(pick(&feed, &label("Password")).unwrap(), Pick::HandedOver(&feed.items[1], feed.items[1].action.as_ref().unwrap()));
    }

    #[test]
    fn a_handed_over_refusal_says_what_the_card_says_and_names_press() {
        let feed = handed_over();
        let err = handed_over_text(&feed);
        assert!(err.contains("handed over"), "{err}");
        assert!(err.contains("weft infra show db"), "{err}");
        assert!(err.contains("weft infra press db reset_password"), "{err}");
    }

    fn handed_over_text(feed: &LiveFeed) -> String {
        match pick(feed, &Want::OnlySecret).unwrap() {
            Pick::HandedOver(item, action) => super::handed_over("db", item, action, None),
            other => panic!("expected handed over, got {other:?}"),
        }
    }

    #[test]
    fn several_secrets_need_a_label_and_the_refusal_lists_them() {
        let feed = LiveFeed::new(vec![LiveItem::secret("A", "1"), LiveItem::secret("B", "2")]);
        let err = pick(&feed, &Want::OnlySecret).unwrap_err().to_string();
        assert!(err.contains("'A', 'B'"), "{err}");
        assert_eq!(pick(&feed, &label("B")).unwrap(), Pick::Value(&feed.items[1]));
    }

    #[test]
    fn an_unknown_label_lists_the_ones_there_are() {
        let feed = fresh();
        let err = pick(&feed, &label("Nope")).unwrap_err().to_string();
        assert!(err.contains("'User', 'Password'"), "{err}");
    }

    #[test]
    fn values_are_quoted_so_every_reader_gets_them_back() {
        assert_eq!(quote_env_value("abc-_.XYZ09").unwrap(), "abc-_.XYZ09");
        assert_eq!(quote_env_value("a b#c$d\"").unwrap(), "'a b#c$d\"'");
        assert_eq!(quote_env_value("it's \"x\"\\").unwrap(), "\"it's \\\"x\\\"\\\\\"");
        assert_eq!(quote_env_value("").unwrap(), "''");
        assert!(quote_env_value("a\nb").is_err());
        assert!(quote_env_value("it's $HOME").is_err());
    }

    #[test]
    fn merging_replaces_only_that_key() {
        let before = "# db\nA=1\nexport PG=old\nB=2\nPG=dup\nPGX=keep";
        assert_eq!(merge_env(before, &one("PG", "new")), "# db\nA=1\nexport PG=new\nB=2\nPGX=keep\n");
        assert_eq!(merge_env("A=1", &one("PG", "new")), "A=1\nPG=new\n");
        assert_eq!(merge_env("", &one("PG", "new")), "PG=new\n");
        assert_eq!(merge_env("PG =x\n", &one("PG", "y")), "PG=y\n");
    }

    fn one(name: &str, value: &str) -> Vec<(String, String)> {
        vec![(name.to_string(), value.to_string())]
    }

    #[test]
    fn several_names_go_in_at_once_each_in_its_place() {
        let pairs = vec![
            ("USER".to_string(), "weft".to_string()),
            ("PW".to_string(), "s3".to_string()),
            ("DB".to_string(), "app".to_string()),
        ];
        assert_eq!(merge_env("A=1\nPW=old\nUSER=x\nPW=dup", &pairs), "A=1\nPW=s3\nUSER=weft\nDB=app\n");
    }

    #[test]
    fn env_names_are_checked() {
        assert!(check_env_name("DATABASE_PASSWORD").is_ok());
        assert!(check_env_name("_x1").is_ok());
        assert!(check_env_name("1X").is_err());
        assert!(check_env_name("A-B").is_err());
        assert!(check_env_name("").is_err());
    }

    #[test]
    fn a_new_file_is_private_and_an_old_one_keeps_its_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join(".env");
        write_env(&fresh, &one("PG", "v1")).unwrap();
        assert_eq!(std::fs::metadata(&fresh).unwrap().permissions().mode() & 0o777, 0o600);
        std::fs::set_permissions(&fresh, std::fs::Permissions::from_mode(0o640)).unwrap();
        std::fs::write(&fresh, "A=1\nPG=v1\n").unwrap();
        write_env(&fresh, &one("PG", "v2")).unwrap();
        assert_eq!(std::fs::read_to_string(&fresh).unwrap(), "A=1\nPG=v2\n");
        assert_eq!(std::fs::metadata(&fresh).unwrap().permissions().mode() & 0o777, 0o640);
    }

    #[test]
    fn a_dash_or_a_device_is_refused() {
        assert!(target(Path::new("-")).is_err());
        assert!(target(Path::new("/dev/null")).is_err());
    }
}
