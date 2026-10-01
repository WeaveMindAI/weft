//! The wire-enum helper.
//!
//! Every enum that crosses a process boundary as a TEXT column or a JSON
//! string field uses [`wire_enum!`]. The variant <-> string mapping is
//! written once, on the variant, and everything else follows from it.
//!
//! Generates:
//! - the enum itself with `#[derive(...Serialize, Deserialize)]`, each
//!   variant renamed to its string
//! - `as_str(self) -> &'static str`
//! - `parse(s: &str) -> Option<Self>`
//! - `pub const VARIANTS: &[Self]` for iteration (the round-trip tests
//!   [`wire_enum_roundtrip_tests!`] generates walk it)
//! - `accepted() -> String`, every string comma-separated, for the error
//!   a caller raises when `parse` refuses a value
//! - `Display`, writing `as_str`
//!
//! It lives here, in the lowest crate, so a wire enum sits beside the
//! messages that carry it whichever crate those are in.

/// Declare a wire enum: see the module docs.
#[macro_export]
macro_rules! wire_enum {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $str:literal
            ),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash,
            ::serde::Serialize, ::serde::Deserialize,
        )]
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                #[serde(rename = $str)]
                $variant,
            )+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $str, )+
                }
            }
            pub fn parse(s: &str) -> ::core::option::Option<Self> {
                match s {
                    $( $str => ::core::option::Option::Some(Self::$variant), )+
                    _ => ::core::option::Option::None,
                }
            }
            pub const VARIANTS: &'static [Self] = &[ $( Self::$variant ),+ ];
            pub fn accepted() -> ::std::string::String {
                [ $( $str ),+ ].join(", ")
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

/// Generate one round-trip `#[test]` per [`wire_enum!`]. Each test walks
/// `T::VARIANTS` and asserts `parse(as_str) == Some(v)`, the serde wire
/// form equals `"<as_str>"`, and decode round-trips. New variants are
/// covered automatically; a new wire enum means one more name in the
/// invocation of the crate that declares it.
#[macro_export]
macro_rules! wire_enum_roundtrip_tests {
    ( $( $name:ident ),+ $(,)? ) => {
        $(
            #[allow(non_snake_case)]
            #[test]
            fn $name() {
                for v in $name::VARIANTS {
                    assert_eq!($name::parse(v.as_str()), Some(*v), "parse(as_str) {v:?}");
                    let json = ::serde_json::to_string(v).expect("serialize");
                    assert_eq!(json, format!("\"{}\"", v.as_str()), "wire form {v:?}");
                    let back: $name = ::serde_json::from_str(&json).expect("deserialize");
                    assert_eq!(back, *v, "round-trip {v:?}");
                }
            }
        )+
    };
}
