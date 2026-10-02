//! A list a person may write either as one comma-separated string or as
//! a list of strings (and each list entry may itself hold commas):
//! `"a, b"`, `["a", "b"]` and `["a, b", "c"]` all read as entries.

/// Flatten `entries` into its items: split every entry on commas, trim
/// each piece, and drop the blank ones (an unfilled text input arrives
/// as "" and means "none given"). Feed it `ValueBag::list::<String>`.
pub fn comma_list<S: AsRef<str>>(entries: impl IntoIterator<Item = S>) -> Vec<String> {
    entries
        .into_iter()
        .flat_map(|entry| {
            entry
                .as_ref()
                .split(',')
                .map(str::trim)
                .filter(|piece| !piece.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::comma_list;

    #[test]
    fn one_string_splits_on_commas_and_trims() {
        assert_eq!(comma_list(["a@x.com, b@y.com"]), vec!["a@x.com", "b@y.com"]);
    }

    #[test]
    fn list_entries_each_split_and_keep_order() {
        assert_eq!(comma_list(["a, b", "c", " d ,e"]), vec!["a", "b", "c", "d", "e"]);
    }

    #[test]
    fn blanks_are_dropped() {
        assert!(comma_list([""]).is_empty());
        assert!(comma_list([" , ,", "  "]).is_empty());
        assert!(comma_list(Vec::<String>::new()).is_empty());
        assert_eq!(comma_list(["a,,b,"]), vec!["a", "b"]);
    }
}
