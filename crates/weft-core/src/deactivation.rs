//! What `POST /projects/{id}/deactivate` answers, and how a person reads
//! whose triggers a lifecycle verb touched. ONE definition for the
//! dispatcher that writes the answer and the CLI that reads it.

use serde::{Deserialize, Serialize};

use crate::member::{MemberId, Owner};

/// Whose triggers a deactivate took down, in order, and which members
/// still have a trigger on afterwards, so a plain deactivate (the
/// program's own triggers) can say that members' are still listening
/// and how to take them down too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeactivateResponse {
    pub deactivated: Vec<Owner>,
    pub members_still_on: Vec<MemberId>,
}

/// How a person reads one owner's triggers: "the program's triggers" or
/// "member 'ada''s triggers".
pub fn whose_triggers(owner: &Owner) -> String {
    match owner {
        Owner::Shared => "the program's triggers".to_string(),
        Owner::Member(member) => format!("member '{member}''s triggers"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_owner_reads_as_whose_triggers() {
        assert_eq!(whose_triggers(&Owner::Shared), "the program's triggers");
        let ada = Owner::Member(MemberId::new("ada").unwrap());
        assert_eq!(whose_triggers(&ada), "member 'ada''s triggers");
    }
}
