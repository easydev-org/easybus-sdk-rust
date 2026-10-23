//! Channel subjects and matching.
//!
//! A channel is a PUBLISH/SUBSCRIBE topic, distinct from the mailbox, which is a point-to-point
//! command queue for one session. Keeping them separate matters: a mailbox message drives one
//! session, while a channel message may interest none or many.
//!
//!     easybus.<tenant>.channel.<channel>.<topic...>
//!
//! The tenant segment is not optional and not caller-supplied: publishers name a channel, never a
//! tenant. That is what makes cross-tenant delivery structurally impossible rather than merely
//! forbidden by a check.

/// The KV bucket holding channel subscriptions.
pub const CHANNEL_KV_BUCKET: &str = "subs";

/// Matches every channel message in every tenant (the agent's fan-in consumer uses this).
pub const CH_CHANNEL_ALL: &str = "easybus.*.channel.>";

/// Builds the subject for one channel message.
///
/// Channel and topic are validated rather than escaped: a caller that smuggles a wildcard into
/// either would turn one publish into a broadcast.
pub fn ch_channel(tenant: &str, channel: &str, topic: &str) -> Result<String, String> {
    for v in [channel, topic] {
        let valid = !v.is_empty()
            && v.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
            && !v.starts_with('.')
            && !v.ends_with('.');
        if !valid {
            return Err(format!(
                "channel and topic must be literal subject segments: {v:?}"
            ));
        }
    }
    Ok(format!("easybus.{tenant}.channel.{channel}.{topic}"))
}

/// Splits a channel subject into (tenant, channel, topic).
///
/// The consumer derives the tenant from the SUBJECT rather than the payload: a publisher that
/// lied in its payload must not be able to deliver into another tenant's channel. The subject is
/// the one thing a publisher cannot forge, because the broker routes on it.
pub fn parse_channel_subject(subject: &str) -> Option<(String, String, String)> {
    let rest = subject.strip_prefix("easybus.")?;
    let i = rest.find('.')?;
    if i == 0 {
        return None;
    }
    let tenant = &rest[..i];
    let after_tenant = &rest[i + 1..];
    let tail = after_tenant.strip_prefix("channel.")?;
    let j = tail.find('.')?;
    if j == 0 || j == tail.len() - 1 {
        return None;
    }
    Some((
        tenant.to_string(),
        tail[..j].to_string(),
        tail[j + 1..].to_string(),
    ))
}

/// Reports whether a topic matches a subscription pattern, with NATS subject semantics.
///
/// `*` matches exactly one segment; `>` matches the rest and must be last, covering at least one
/// segment. The `>` rule was verified against a real broker rather than assumed: subscribing to
/// `tag.>` delivers `tag.v1` but NOT `tag`.
pub fn match_subject(pattern: &str, topic: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }
    let p: Vec<&str> = pattern.split('.').collect();
    let t: Vec<&str> = topic.split('.').collect();
    for (i, seg) in p.iter().enumerate() {
        if *seg == ">" {
            return t.len() > i;
        }
        if i >= t.len() {
            return false;
        }
        if *seg != "*" && *seg != t[i] {
            return false;
        }
    }
    p.len() == t.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_round_trip() {
        let s = ch_channel("alice", "gitea", "tag.agentdev/mrflow.v1.2.3").unwrap();
        assert_eq!(s, "easybus.alice.channel.gitea.tag.agentdev/mrflow.v1.2.3");
        let (t, c, top) = parse_channel_subject(&s).unwrap();
        assert_eq!((t.as_str(), c.as_str(), top.as_str()),
            ("alice", "gitea", "tag.agentdev/mrflow.v1.2.3"));
    }

    #[test]
    fn rejects_wildcards_in_a_publish() {
        // A wildcard here would turn one publish into a broadcast.
        assert!(ch_channel("alice", "gitea*", "tag.v1").is_err());
        assert!(ch_channel("alice", "gitea", "tag.>").is_err());
        assert!(ch_channel("alice", "", "tag.v1").is_err());
    }

    #[test]
    fn rejects_malformed_subjects() {
        for bad in [
            "",
            "not-easybus.alice.channel.gitea.tag",
            "easybus.channel.gitea.tag",
            "easybus.alice.mailbox.abc",
            "easybus.alice.channel.gitea",
            "easybus.alice.channel.gitea.",
            "easybus.alice.channel..tag",
        ] {
            assert!(parse_channel_subject(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn match_semantics() {
        let cases = [
            ("tag.v1", "tag.v1", true),
            ("tag.*", "tag.v1", true),
            ("tag.*", "tag.v1.2", false),
            // ">" must cover at least one segment: verified against a real broker.
            ("tag.>", "tag.v1", true),
            ("tag.>", "tag.v1.2.3", true),
            ("tag.>", "tag", false),
            (">", "anything.at.all", true),
            ("gitea.tag.>", "gitea.push.v1", false),
            ("", "anything", false),
        ];
        for (pattern, topic, want) in cases {
            assert_eq!(match_subject(pattern, topic), want, "{pattern} vs {topic}");
        }
    }
}
