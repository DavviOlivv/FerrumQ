use crate::error::{DomainError, DomainResult};

const MAX_TEXT_LEN: usize = 255;

pub(crate) fn bounded_text(value: &str, field: &'static str) -> DomainResult<String> {
    let trimmed = value.trim();
    let actual = trimmed.chars().count();

    if actual == 0 {
        return Err(DomainError::Empty { field });
    }

    if actual > MAX_TEXT_LEN {
        return Err(DomainError::TooLong {
            field,
            max: MAX_TEXT_LEN,
            actual,
        });
    }

    Ok(trimmed.to_owned())
}

pub(crate) fn topic_name(value: &str, field: &'static str) -> DomainResult<String> {
    let value = bounded_text(value, field)?;

    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(DomainError::InvalidCharacters {
            field,
            allowed: "ASCII letters, digits, '.', '_', '-'",
        });
    }

    if value.starts_with('.') || value.ends_with('.') {
        return Err(DomainError::InvalidDotBoundary { field });
    }

    if value.contains("..") {
        return Err(DomainError::ConsecutiveDots { field });
    }

    Ok(value)
}

pub(crate) fn consumer_group_id(value: &str, field: &'static str) -> DomainResult<String> {
    let value = bounded_text(value, field)?;

    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(DomainError::InvalidCharacters {
            field,
            allowed: "ASCII letters, digits, '.', '_', '-'",
        });
    }

    Ok(value)
}

macro_rules! validated_string_type {
    ($name:ident, $field:literal, $validator:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl AsRef<str>) -> $crate::error::DomainResult<Self> {
                let value = $crate::validation::$validator(value.as_ref(), $field)?;
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $crate::error::DomainError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = $crate::error::DomainError;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $crate::error::DomainError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl ::serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::Deserializer<'de>,
            {
                let value = <String as ::serde::Deserialize>::deserialize(deserializer)?;
                Self::new(value).map_err(::serde::de::Error::custom)
            }
        }
    };
}

pub(crate) use validated_string_type;
