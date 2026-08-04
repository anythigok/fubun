use std::time::Instant;

use fubun_domain::{
    Actor, AdapterIdentity, Event, EventData, EventType, ObservationScope, ObservationSource,
    ObservationStatus, PrivacyClass, Resource, ResourceKind, ResourceScope, Sensitivity,
    EVENT_SPEC_VERSION,
};
use fubun_mining::{discover, DiscoveryInput};
use time::OffsetDateTime;
use uuid::Uuid;

fn main() {
    let workspace = Uuid::from_u128(1);
    let actions = [Uuid::from_u128(2), Uuid::from_u128(3), Uuid::from_u128(4)];
    let resources = std::iter::once(resource(workspace, ResourceKind::Directory))
        .chain(
            actions
                .iter()
                .copied()
                .map(|id| resource(id, ResourceKind::WebPage)),
        )
        .collect::<Vec<_>>();
    let scopes = std::iter::once(scope(workspace, ObservationSource::VscodeWorkspace))
        .chain(
            actions
                .iter()
                .copied()
                .map(|id| scope(id, ObservationSource::BrowserChromium)),
        )
        .collect::<Vec<_>>();
    let mut events = Vec::with_capacity(100_000);
    let base = OffsetDateTime::UNIX_EPOCH;
    let mut sequence = 1_u64;
    for index in 0_u128..25_000 {
        let start = base + time::Duration::hours((index as i64) * 3);
        events.push(event(
            index * 4 + 10,
            start,
            sequence,
            EventType::VscodeWorkspaceOpenedV1,
            EventData::VscodeWorkspaceOpened {
                resource_id: workspace,
            },
        ));
        sequence += 1;
        for (offset, resource_id) in actions.iter().copied().enumerate() {
            events.push(event(
                index * 4 + 11 + offset as u128,
                start + time::Duration::seconds((offset + 1) as i64),
                sequence,
                EventType::BrowserResourceOpenedV1,
                EventData::BrowserResourceOpened { resource_id },
            ));
            sequence += 1;
        }
    }
    assert_eq!(events.len(), 100_000);
    let started = Instant::now();
    let output = discover(DiscoveryInput {
        events,
        resources,
        scopes,
    });
    println!(
        "elapsed_ms={} sessions={} candidates={} suggestions={}",
        started.elapsed().as_millis(),
        output.sessions.len(),
        output.candidates_evaluated,
        output.suggestions.len()
    );
}

fn resource(id: Uuid, kind: ResourceKind) -> Resource {
    let locator = if kind == ResourceKind::WebPage {
        "https://example.com/page"
    } else {
        "/tmp/fubun-perf"
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
    id: u128,
    received_at: OffsetDateTime,
    sequence_no: u64,
    event_type: EventType,
    data: EventData,
) -> Event {
    Event {
        spec_version: EVENT_SPEC_VERSION.to_owned(),
        id: Uuid::from_u128(id),
        event_type,
        source: "perf-fixture".to_owned(),
        occurred_at: received_at,
        received_at,
        actor: Actor::User,
        adapter: AdapterIdentity {
            id: "fixture".to_owned(),
            version: "1".to_owned(),
            instance_id: Uuid::from_u128(99),
            sequence_no,
        },
        context: None,
        privacy: PrivacyClass::Normal,
        data,
    }
}
