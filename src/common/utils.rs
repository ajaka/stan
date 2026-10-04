use crate::core::types::AppError;

pub const MAX_TOKEN_LEN: usize = 1024;

pub fn token_len_is_valid(len: usize) -> bool {
    len <= MAX_TOKEN_LEN
}

pub enum Wildcards {
    Allow,
    Forbid,
}

pub fn validate_topic(segments: &[&str], wildcards: Wildcards) -> Result<(), AppError> {
    if segments.is_empty() {
        return Err(AppError::InvalidTopic {
            context: String::new(),
        });
    }

    let context = || segments.join(".");

    let last = segments.len() - 1;
    for (i, seg) in segments.iter().enumerate() {
        match *seg {
            "*" => {
                if matches!(wildcards, Wildcards::Forbid) {
                    return Err(AppError::WildcardInPublish { context: context() });
                }
            }

            ">" => {
                if matches!(wildcards, Wildcards::Forbid) {
                    return Err(AppError::WildcardInPublish { context: context() });
                }
                if i != last {
                    return Err(AppError::InvalidTopic { context: context() });
                }
            }
            _ => {}
        }
    }

    Ok(())
}

pub fn split(path: &str) -> Vec<&str> {
    path.split('.').filter(|seg| !seg.is_empty()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(topic: &str) -> Result<(), AppError> {
        validate_topic(&split(topic), Wildcards::Allow)
    }

    fn forbid(topic: &str) -> Result<(), AppError> {
        validate_topic(&split(topic), Wildcards::Forbid)
    }

    // ------------------------------------------------------------------ split

    // ------------------------------------------------------------ token limit

    #[test]
    fn token_len_at_the_limit_is_accepted() {
        assert!(token_len_is_valid(MAX_TOKEN_LEN));
    }

    #[test]
    fn token_len_over_the_limit_is_rejected() {
        assert!(!token_len_is_valid(MAX_TOKEN_LEN + 1));
    }

    #[test]
    fn a_huge_claimed_len_is_rejected_without_allocating() {
        // This is the value a hostile client puts in the length prefix.
        assert!(!token_len_is_valid(u32::MAX as usize));
    }

    #[test]
    fn empty_token_len_is_accepted() {
        assert!(token_len_is_valid(0));
    }

    #[test]
    fn split_divides_on_dots() {
        assert_eq!(split("foo.bar.baz"), vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn split_drops_empty_segments() {
        assert_eq!(split("foo..bar"), vec!["foo", "bar"]);
        assert_eq!(split(".foo."), vec!["foo"]);
    }

    #[test]
    fn split_of_an_empty_topic_yields_nothing() {
        assert!(split("").is_empty());
        assert!(split("...").is_empty());
    }

    // -------------------------------------------------------------- accepted

    #[test]
    fn plain_topic_is_accepted() {
        assert!(allow("foo").is_ok());
        assert!(forbid("foo").is_ok());
    }

    #[test]
    fn multi_segment_topic_is_accepted() {
        assert!(allow("foo.bar.baz").is_ok());
    }

    #[test]
    fn a_topic_named_like_a_wildcard_segment_is_rejected() {
        // `*` and `>` are reserved, so a literal segment with those bytes can
        // never be addressed.
        assert_eq!(
            forbid("foo.*"),
            Err(AppError::WildcardInPublish {
                context: "foo.*".into()
            })
        );
    }

    // ------------------------------------------------------------ empty topic

    #[test]
    fn empty_topic_is_invalid() {
        assert_eq!(
            allow(""),
            Err(AppError::InvalidTopic {
                context: String::new()
            })
        );
        assert_eq!(
            forbid(""),
            Err(AppError::InvalidTopic {
                context: String::new()
            })
        );
    }

    #[test]
    fn topic_of_only_separators_is_invalid() {
        assert_eq!(
            forbid("..."),
            Err(AppError::InvalidTopic {
                context: String::new()
            })
        );
    }

    #[test]
    fn empty_segments_around_a_real_one_are_tolerated() {
        assert!(allow("foo..bar").is_ok());
    }

    // -------------------------------------------------------------------- star

    #[test]
    fn star_is_allowed_in_a_subscription() {
        assert!(allow("foo.*").is_ok());
        assert!(allow("*").is_ok());
    }

    #[test]
    fn star_is_rejected_in_a_publish() {
        assert_eq!(
            forbid("foo.*"),
            Err(AppError::WildcardInPublish {
                context: "foo.*".into()
            })
        );
        assert_eq!(
            forbid("*"),
            Err(AppError::WildcardInPublish {
                context: "*".into()
            })
        );
    }

    #[test]
    fn star_in_a_later_segment_is_rejected_in_a_publish() {
        assert!(matches!(
            forbid("a.b.*"),
            Err(AppError::WildcardInPublish { .. })
        ));
    }

    // ---------------------------------------------------------------------- >

    #[test]
    fn trailing_gt_is_allowed_in_a_subscription() {
        assert!(allow("foo.>").is_ok());
        assert!(allow(">").is_ok());
        assert!(allow("a.b.>").is_ok());
    }

    #[test]
    fn gt_is_rejected_in_a_publish() {
        assert_eq!(
            forbid("foo.>"),
            Err(AppError::WildcardInPublish {
                context: "foo.>".into()
            })
        );
        assert_eq!(
            forbid(">"),
            Err(AppError::WildcardInPublish {
                context: ">".into()
            })
        );
    }

    #[test]
    fn non_trailing_gt_is_invalid_even_in_a_subscription() {
        // `foo.>.bar` has no meaning: `>` swallows everything after it.
        assert_eq!(
            allow("foo.>.bar"),
            Err(AppError::InvalidTopic {
                context: "foo.>.bar".into()
            })
        );
        assert_eq!(
            allow(">.bar"),
            Err(AppError::InvalidTopic {
                context: ">.bar".into()
            })
        );
    }

    #[test]
    fn gt_may_appear_only_once() {
        assert_eq!(
            allow("foo.>.>"),
            Err(AppError::InvalidTopic {
                context: "foo.>.>".into()
            })
        );
    }

    // -------------------------------------------------------------- combined

    #[test]
    fn both_wildcards_together_are_accepted_in_a_subscription() {
        assert!(allow("foo.*.bar.>").is_ok());
    }

    #[test]
    fn context_is_the_rejoined_topic() {
        // Error context is what the client sees, so it should read as the
        // topic it tried to use.
        assert_eq!(
            forbid("foo.bar.*"),
            Err(AppError::WildcardInPublish {
                context: "foo.bar.*".into()
            })
        );
    }

    #[test]
    fn context_is_normalised_not_verbatim() {
        // `split` drops empty segments, so the context is the cleaned form.
        assert_eq!(
            forbid("foo..*.bar"),
            Err(AppError::WildcardInPublish {
                context: "foo.*.bar".into()
            })
        );
    }
}
