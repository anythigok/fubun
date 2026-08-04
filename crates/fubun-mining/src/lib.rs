//! Pure, deterministic Phase 4A discovery logic.
//!
//! This crate deliberately contains no persistence, runtime, filesystem,
//! browser, operating-system, or network code. It consumes validated semantic
//! events and returns deterministic sessions and prefix evidence.

use fubun_domain::{
    Actor, Event, EventData, EventType, ObservationScope, ObservationSource, ObservationStatus,
    Resource, ResourceKind,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use time::OffsetDateTime;
use uuid::Uuid;

pub const ALGORITHM_VERSION: &str = "workspace-browser-start/v1";
pub const SESSION_KIND: &str = "workspace_start";
pub const STARTUP_WINDOW_SECONDS: i64 = 600;
pub const ANCHOR_MERGE_SECONDS: i64 = 300;
pub const MIN_ACTIONS: usize = 2;
pub const MAX_ACTIONS: usize = 5;
pub const MIN_SUPPORT: usize = 3;
pub const MIN_CONFIDENCE_BPS: u32 = 7_000;
pub const MIN_SPAN_SECONDS: i64 = 18 * 60 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryRunStatus {
    Running,
    Succeeded,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum SuggestionStatus {
    Pending,
    Snoozed,
    Dismissed,
    Accepted,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryRun {
    pub id: Uuid,
    pub algorithm_version: String,
    pub status: DiscoveryRunStatus,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub started_at: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    #[schemars(with = "Option<String>")]
    pub finished_at: Option<OffsetDateTime>,
    pub input_event_count: u32,
    pub sessions_upserted: u32,
    pub candidates_evaluated: u32,
    pub suggestions_created: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryInput {
    pub events: Vec<Event>,
    pub resources: Vec<Resource>,
    pub scopes: Vec<ObservationScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionEvent {
    pub event_id: Uuid,
    pub resource_id: Uuid,
    pub ordinal: u32,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub received_at: OffsetDateTime,
    pub is_anchor: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveredSession {
    pub id: Uuid,
    pub algorithm_version: String,
    pub kind: String,
    pub workspace_resource_id: Uuid,
    pub anchor_event_id: Uuid,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub started_at: OffsetDateTime,
    #[serde(default, with = "time::serde::rfc3339::option")]
    #[schemars(with = "Option<String>")]
    pub finished_at: Option<OffsetDateTime>,
    pub event_count: u32,
    pub eligible: bool,
    pub events: Vec<SessionEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveredSuggestion {
    pub id: Uuid,
    pub algorithm_version: String,
    pub workspace_resource_id: Uuid,
    pub pattern_fingerprint: String,
    pub action_resource_ids: Vec<Uuid>,
    pub supporting_session_ids: Vec<Uuid>,
    pub support_sessions: u32,
    pub eligible_sessions: u32,
    pub confidence_basis_points: u32,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub first_seen_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    #[schemars(with = "String")]
    pub last_seen_at: OffsetDateTime,
    pub observation_span_seconds: i64,
    pub median_completion_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryOutput {
    pub algorithm_version: String,
    pub sessions: Vec<DiscoveredSession>,
    pub suggestions: Vec<DiscoveredSuggestion>,
    pub candidates_evaluated: u32,
}

fn active_scope(resource_id: Uuid, source: ObservationSource, scopes: &[ObservationScope]) -> bool {
    scopes.iter().any(|scope| {
        scope.resource_id == resource_id
            && scope.source == source
            && scope.status == ObservationStatus::Active
    })
}

fn eligible_resource(
    resources: &[Resource],
    scopes: &[ObservationScope],
    id: Uuid,
    kind: ResourceKind,
    source: ObservationSource,
) -> bool {
    resources
        .iter()
        .any(|resource| resource.id == id && resource.kind == kind)
        && active_scope(id, source, scopes)
}

fn event_sort_key(event: &Event) -> (OffsetDateTime, Uuid) {
    (event.received_at, event.id)
}

fn delta_seconds(later: OffsetDateTime, earlier: OffsetDateTime) -> i64 {
    (later - earlier).whole_seconds()
}

/// Run the complete deterministic Phase 4A discovery pass.
pub fn discover(input: DiscoveryInput) -> DiscoveryOutput {
    let mut events = input.events;
    events.sort_by_key(event_sort_key);
    let workspace_ids: BTreeSet<Uuid> = input
        .resources
        .iter()
        .filter(|resource| resource.kind == ResourceKind::Directory)
        .filter(|resource| {
            active_scope(
                resource.id,
                ObservationSource::VscodeWorkspace,
                &input.scopes,
            )
        })
        .map(|resource| resource.id)
        .collect();

    let anchors: Vec<(Uuid, Event)> = events
        .iter()
        .filter_map(|event| {
            if event.actor != Actor::User || event.event_type != EventType::VscodeWorkspaceOpenedV1
            {
                return None;
            }
            let EventData::VscodeWorkspaceOpened { resource_id } = event.data else {
                return None;
            };
            workspace_ids
                .contains(&resource_id)
                .then(|| (resource_id, event.clone()))
        })
        .collect();

    let mut sessions = Vec::new();
    for (workspace_id, anchor) in anchors {
        let merge = sessions
            .last_mut()
            .filter(|session: &&mut DiscoveredSession| {
                session.workspace_resource_id == workspace_id
                    && delta_seconds(anchor.received_at, session.started_at).abs()
                        <= ANCHOR_MERGE_SECONDS
            });
        let session = if let Some(existing) = merge {
            existing
        } else {
            let id = Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!("{ALGORITHM_VERSION}:{}", anchor.id).as_bytes(),
            );
            sessions.push(DiscoveredSession {
                id,
                algorithm_version: ALGORITHM_VERSION.to_owned(),
                kind: SESSION_KIND.to_owned(),
                workspace_resource_id: workspace_id,
                anchor_event_id: anchor.id,
                started_at: anchor.received_at,
                finished_at: Some(anchor.received_at),
                event_count: 1,
                eligible: false,
                events: vec![SessionEvent {
                    event_id: anchor.id,
                    resource_id: workspace_id,
                    ordinal: 0,
                    received_at: anchor.received_at,
                    is_anchor: true,
                }],
            });
            sessions.last_mut().expect("session was inserted")
        };
        for event in events.iter().filter(|candidate| {
            candidate.received_at >= session.started_at
                && delta_seconds(candidate.received_at, session.started_at)
                    <= STARTUP_WINDOW_SECONDS
                && candidate.event_type == EventType::BrowserResourceOpenedV1
                && candidate.actor == Actor::User
        }) {
            let EventData::BrowserResourceOpened { resource_id } = event.data else {
                continue;
            };
            if !eligible_resource(
                &input.resources,
                &input.scopes,
                resource_id,
                ResourceKind::WebPage,
                ObservationSource::BrowserChromium,
            ) {
                continue;
            }
            if session
                .events
                .iter()
                .any(|item| !item.is_anchor && item.resource_id == resource_id)
                || session.events.iter().filter(|item| !item.is_anchor).count() >= MAX_ACTIONS
            {
                continue;
            }
            let ordinal = session.events.len() as u32;
            session.events.push(SessionEvent {
                event_id: event.id,
                resource_id,
                ordinal,
                received_at: event.received_at,
                is_anchor: false,
            });
            session.finished_at = Some(event.received_at);
        }
        session.event_count = session.events.len() as u32;
        session.eligible = session.events.iter().any(|item| !item.is_anchor);
    }

    let mut groups: BTreeMap<(Uuid, Vec<Uuid>), Vec<&DiscoveredSession>> = BTreeMap::new();
    for session in sessions.iter().filter(|session| session.eligible) {
        let actions: Vec<Uuid> = session
            .events
            .iter()
            .filter(|event| !event.is_anchor)
            .map(|event| event.resource_id)
            .collect();
        for length in MIN_ACTIONS..=actions.len().min(MAX_ACTIONS) {
            groups
                .entry((session.workspace_resource_id, actions[..length].to_vec()))
                .or_default()
                .push(session);
        }
    }
    let eligible_by_workspace: BTreeMap<Uuid, usize> = sessions
        .iter()
        .filter(|session| session.eligible)
        .fold(BTreeMap::new(), |mut map, session| {
            *map.entry(session.workspace_resource_id).or_default() += 1;
            map
        });

    let mut candidates_evaluated = 0;
    let mut suggestions = Vec::new();
    for ((workspace, actions), supporters) in groups {
        candidates_evaluated += 1;
        let support = supporters.len();
        let eligible = eligible_by_workspace
            .get(&workspace)
            .copied()
            .unwrap_or_default();
        let confidence = if eligible == 0 {
            0
        } else {
            ((support * 10_000) / eligible) as u32
        };
        let first = supporters
            .iter()
            .map(|session| session.started_at)
            .min()
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let last = supporters
            .iter()
            .map(|session| session.started_at)
            .max()
            .unwrap_or(first);
        let span = delta_seconds(last, first);
        if actions.len() < MIN_ACTIONS
            || actions.len() > MAX_ACTIONS
            || support < MIN_SUPPORT
            || confidence < MIN_CONFIDENCE_BPS
            || span < MIN_SPAN_SECONDS
        {
            continue;
        }
        let pattern_fingerprint = fingerprint(workspace, &actions);
        let id = Uuid::new_v5(&Uuid::NAMESPACE_URL, pattern_fingerprint.as_bytes());
        let durations: Vec<i64> = supporters
            .iter()
            .filter_map(|session| {
                session
                    .finished_at
                    .map(|finish| (finish - session.started_at).whole_milliseconds() as i64)
            })
            .collect();
        suggestions.push(DiscoveredSuggestion {
            id,
            algorithm_version: ALGORITHM_VERSION.to_owned(),
            workspace_resource_id: workspace,
            pattern_fingerprint,
            action_resource_ids: actions,
            supporting_session_ids: supporters.iter().map(|session| session.id).collect(),
            support_sessions: support as u32,
            eligible_sessions: eligible as u32,
            confidence_basis_points: confidence,
            first_seen_at: first,
            last_seen_at: last,
            observation_span_seconds: span,
            median_completion_ms: median(&durations),
        });
    }

    // Keep one longest qualifying prefix per workspace. Ties are deterministic.
    suggestions.sort_by(|a, b| {
        b.action_resource_ids
            .len()
            .cmp(&a.action_resource_ids.len())
            .then_with(|| b.support_sessions.cmp(&a.support_sessions))
            .then_with(|| b.confidence_basis_points.cmp(&a.confidence_basis_points))
            .then_with(|| b.last_seen_at.cmp(&a.last_seen_at))
            .then_with(|| a.pattern_fingerprint.cmp(&b.pattern_fingerprint))
    });
    let mut selected_workspaces = BTreeSet::new();
    suggestions.retain(|suggestion| selected_workspaces.insert(suggestion.workspace_resource_id));
    DiscoveryOutput {
        algorithm_version: ALGORITHM_VERSION.to_owned(),
        sessions,
        suggestions,
        candidates_evaluated,
    }
}

fn median(values: &[i64]) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2]
}

/// Fingerprint uses length-prefixed canonical bytes and IDs only.
pub fn fingerprint(workspace: Uuid, actions: &[Uuid]) -> String {
    let mut bytes = Vec::with_capacity(64 + actions.len() * 16);
    bytes.extend_from_slice(&(ALGORITHM_VERSION.len() as u32).to_be_bytes());
    bytes.extend_from_slice(ALGORITHM_VERSION.as_bytes());
    bytes.extend_from_slice(workspace.as_bytes());
    bytes.extend_from_slice(&(actions.len() as u32).to_be_bytes());
    for action in actions {
        bytes.extend_from_slice(action.as_bytes());
    }
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use fubun_domain::{AdapterIdentity, PrivacyClass, ResourceScope, Sensitivity};

    #[test]
    fn fingerprint_is_stable_and_order_sensitive() {
        let workspace = Uuid::from_u128(1);
        let a = Uuid::from_u128(2);
        let b = Uuid::from_u128(3);
        assert_eq!(
            fingerprint(workspace, &[a, b]),
            fingerprint(workspace, &[a, b])
        );
        assert_ne!(
            fingerprint(workspace, &[a, b]),
            fingerprint(workspace, &[b, a])
        );
    }

    #[test]
    fn three_spread_workspace_starts_produce_one_prefix_suggestion() {
        let workspace = Uuid::from_u128(10);
        let browser_a = Uuid::from_u128(11);
        let browser_b = Uuid::from_u128(12);
        let base = OffsetDateTime::UNIX_EPOCH;
        let resources = [
            resource(workspace, ResourceKind::Directory),
            resource(browser_a, ResourceKind::WebPage),
            resource(browser_b, ResourceKind::WebPage),
        ];
        let scopes = [
            scope(workspace, ObservationSource::VscodeWorkspace),
            scope(browser_a, ObservationSource::BrowserChromium),
            scope(browser_b, ObservationSource::BrowserChromium),
        ];
        let mut events = Vec::new();
        for (offset, anchor_number) in [0_i64, 18 * 60 * 60, 36 * 60 * 60]
            .into_iter()
            .zip(1_u64..=3)
        {
            let start = base + time::Duration::seconds(offset);
            events.push(event(
                Uuid::from_u128(100 + u128::from(anchor_number)),
                start,
                anchor_number,
                EventType::VscodeWorkspaceOpenedV1,
                EventData::VscodeWorkspaceOpened {
                    resource_id: workspace,
                },
            ));
            events.push(event(
                Uuid::from_u128(200 + u128::from(anchor_number)),
                start + time::Duration::seconds(1),
                anchor_number * 2,
                EventType::BrowserResourceOpenedV1,
                EventData::BrowserResourceOpened {
                    resource_id: browser_a,
                },
            ));
            events.push(event(
                Uuid::from_u128(300 + u128::from(anchor_number)),
                start + time::Duration::seconds(2),
                anchor_number * 2 + 1,
                EventType::BrowserResourceOpenedV1,
                EventData::BrowserResourceOpened {
                    resource_id: browser_b,
                },
            ));
        }
        let output = discover(DiscoveryInput {
            events,
            resources: resources.to_vec(),
            scopes: scopes.to_vec(),
        });
        assert_eq!(output.sessions.len(), 3);
        assert_eq!(output.suggestions.len(), 1);
        assert_eq!(
            output.suggestions[0].action_resource_ids,
            vec![browser_a, browser_b]
        );
        assert_eq!(output.suggestions[0].support_sessions, 3);
        assert_eq!(output.suggestions[0].confidence_basis_points, 10_000);
    }

    fn resource(id: Uuid, kind: ResourceKind) -> Resource {
        let locator = if kind == ResourceKind::WebPage {
            "https://example.com/page"
        } else {
            "/tmp/fubun-fixture"
        };
        Resource {
            id,
            kind,
            label: "fixture".to_owned(),
            locator: locator.to_owned(),
            canonical_locator: locator.to_owned(),
            sensitivity: Sensitivity::Normal,
            scope: ResourceScope::Exact,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn scope(resource_id: Uuid, source: ObservationSource) -> ObservationScope {
        ObservationScope {
            id: Uuid::new_v4(),
            source,
            resource_id,
            status: ObservationStatus::Active,
            created_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn event(
        id: Uuid,
        received_at: OffsetDateTime,
        sequence_no: u64,
        event_type: EventType,
        data: EventData,
    ) -> Event {
        Event {
            spec_version: fubun_domain::EVENT_SPEC_VERSION.to_owned(),
            id,
            event_type,
            source: "fixture".to_owned(),
            occurred_at: received_at,
            received_at,
            actor: Actor::User,
            adapter: AdapterIdentity {
                id: "fixture".to_owned(),
                version: "1".to_owned(),
                instance_id: Uuid::from_u128(999),
                sequence_no,
            },
            context: None,
            privacy: PrivacyClass::Normal,
            data,
        }
    }
}
