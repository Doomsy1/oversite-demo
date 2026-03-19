create or replace view derived.v_image_enriched as
with flag_rollup as (
    select
        f.derived_image_id,
        count(*) as flag_count,
        string_agg(f.flag_type || ':' || f.flag_value, ', ' order by f.flag_type, f.flag_value) as flag_summary
    from derived.flags f
    group by f.derived_image_id
)
select
    i.id as image_id,
    i.pipeline_run_id,
    i.derived_session_id,
    i.source_event_id,
    i.source_session_id,
    i.company_id,
    i.device_id,
    i.user_id,
    i.event_timestamp,
    i.event_type,
    i.event_summary,
    i.priority,
    i.blob_url,
    i.public_image_url,
    i.storage_bucket,
    i.storage_path,
    i.trade,
    i.location_hint,
    i.site_hint,
    i.is_flagged,
    i.image_fetch_status,
    i.image_sha256,
    i.image_mime_type,
    i.image_width,
    i.image_height,
    i.image_bytes,
    coalesce(fr.flag_count, 0) as flag_count,
    fr.flag_summary,
    s.started_at as session_started_at,
    s.ended_at as session_ended_at,
    s.status as session_status
from derived.images i
join derived.sessions s
    on s.id = i.derived_session_id
left join flag_rollup fr
    on fr.derived_image_id = i.id;

create or replace view derived.v_graph_edges_enriched as
select
    e.id as edge_id,
    e.pipeline_run_id,
    e.edge_type,
    src.node_type as src_node_type,
    src.node_key as src_node_key,
    src.display_label as src_node_label,
    dst.node_type as dst_node_type,
    dst.node_key as dst_node_key,
    dst.display_label as dst_node_label,
    e.evidence_image_id,
    e.evidence_session_id,
    e.source_kind,
    e.attributes,
    coalesce(
        (e.attributes ->> 'evidence_image_count')::integer,
        case when e.evidence_image_id is null then 0 else 1 end
    ) as evidence_image_count,
    coalesce(
        (e.attributes ->> 'evidence_session_count')::integer,
        case when e.evidence_session_id is null then 0 else 1 end
    ) as evidence_session_count,
    (e.attributes ->> 'first_seen_at')::timestamptz as first_seen_at,
    (e.attributes ->> 'last_seen_at')::timestamptz as last_seen_at
from derived.graph_edges e
join derived.graph_nodes src
    on src.id = e.src_node_id
join derived.graph_nodes dst
    on dst.id = e.dst_node_id;

create or replace view derived.v_entity_location_recurrence as
select
    edge_type,
    src_node_type as entity_type,
    src_node_label as entity_label,
    dst_node_label as location_hint,
    evidence_image_count,
    evidence_session_count,
    first_seen_at,
    last_seen_at
from derived.v_graph_edges_enriched
where edge_type in ('hazard_near_location_hint', 'equipment_at_location_hint')
order by
    evidence_session_count desc,
    evidence_image_count desc,
    edge_type,
    entity_label,
    location_hint;

create or replace view derived.v_hazard_location_recurrence as
select
    entity_label as hazard,
    location_hint,
    evidence_image_count,
    evidence_session_count,
    first_seen_at,
    last_seen_at
from derived.v_entity_location_recurrence
where edge_type = 'hazard_near_location_hint'
order by
    evidence_session_count desc,
    evidence_image_count desc,
    hazard,
    location_hint;

drop view if exists derived.v_equipment_location_recurrence;

create or replace view derived.v_trade_location_recurrence as
with trade_observations as (
    select
        o.derived_image_id,
        o.observation_label as trade_label,
        o.canonical_key as normalized_trade_key
    from derived.observations o
    where o.observation_family = 'trade'
),
location_observations as (
    select
        o.derived_image_id,
        o.observation_label as location_label,
        o.canonical_key as normalized_location_hint_key
    from derived.observations o
    where o.observation_family = 'location_hint'
)
select
    min(t.trade_label) as trade,
    min(l.location_label) as location_hint,
    count(distinct i.image_id) as image_count,
    count(distinct i.source_session_id) as session_count,
    count(*) filter (where i.is_flagged) as flagged_image_count,
    min(i.event_timestamp) as first_seen_at,
    max(i.event_timestamp) as last_seen_at,
    t.normalized_trade_key,
    l.normalized_location_hint_key
from derived.v_image_enriched i
join trade_observations t
    on t.derived_image_id = i.image_id
join location_observations l
    on l.derived_image_id = i.image_id
group by
    t.normalized_trade_key,
    l.normalized_location_hint_key
order by
    session_count desc,
    image_count desc,
    trade,
    location_hint;

create or replace view derived.v_session_image_summary as
select
    s.id as derived_session_id,
    s.source_session_id,
    s.company_id,
    s.device_id,
    count(i.id) as image_count,
    count(*) filter (where i.is_flagged) as flagged_image_count,
    s.started_at,
    s.ended_at,
    s.status
from derived.sessions s
left join derived.images i
    on i.derived_session_id = s.id
group by
    s.id,
    s.source_session_id,
    s.company_id,
    s.device_id,
    s.started_at,
    s.ended_at,
    s.status;

create or replace view derived.v_ml_dataset_manifest as
with observation_rollup as (
    select
        o.derived_image_id,
        jsonb_agg(
            jsonb_build_object(
                'family', o.observation_family,
                'label', o.observation_label,
                'canonical_key', o.canonical_key,
                'confidence', o.confidence,
                'source_kind', o.source_kind,
                'rule_id', o.rule_id
            )
            order by o.observation_family, o.observation_label
        ) as observations
    from derived.observations o
    group by o.derived_image_id
),
flag_rollup as (
    select
        f.derived_image_id,
        jsonb_agg(
            jsonb_build_object(
                'flag_type', f.flag_type,
                'flag_value', f.flag_value,
                'source_kind', f.source_kind
            )
            order by f.flag_type, f.flag_value
        ) as flags
    from derived.flags f
    group by f.derived_image_id
)
select
    i.id as image_id,
    i.source_event_id,
    i.source_session_id,
    i.company_id,
    i.device_id,
    i.user_id,
    i.event_timestamp,
    i.event_type,
    i.event_summary,
    i.priority,
    i.trade,
    i.location_hint,
    i.site_hint,
    i.blob_url,
    i.public_image_url,
    i.storage_bucket,
    i.storage_path,
    a.fetch_status,
    a.fetch_http_status,
    a.fetched_at,
    a.byte_size,
    a.sha256,
    a.mime_type,
    a.width,
    a.height,
    a.curated_bucket,
    a.curated_path,
    s.started_at as session_started_at,
    s.ended_at as session_ended_at,
    s.status as session_status,
    coalesce(fr.flags, '[]'::jsonb) as flags,
    coalesce(oroll.observations, '[]'::jsonb) as observations
from derived.images i
join derived.sessions s
    on s.id = i.derived_session_id
left join derived.image_assets a
    on a.derived_image_id = i.id
left join flag_rollup fr
    on fr.derived_image_id = i.id
left join observation_rollup oroll
    on oroll.derived_image_id = i.id;
