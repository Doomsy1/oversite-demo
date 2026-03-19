# Source Notes

## Important Source-Limit Details

- The extraction rule is `events.blob_url is not null`, not `type = issue_flagged`
- `issue_flagged` is the dominant current image event type, but it is not the only one
- `trade` is worth normalizing in v1 because it is repeated, relatively clean, and useful for grouping
- `location` is useful, but it behaves like free text rather than a canonical place model
- `sessions.event_count` should be treated as a source summary field, not as an authoritative count of raw `events` rows

## Why `location_hint` Stays A Hint

The live source can express the same place in multiple ways, for example:

- `east wall`
- `East wall, stairwell`
- `near the east wall`

That is useful signal, but not enough to claim canonical area modeling in v1. The repo therefore preserves it as `location_hint` and leaves place normalization for a later step.

## Similar Choice For `site`

`site` can be preserved when present, but it is treated as `site_hint` rather than a canonical site hierarchy for the same reason: the source does not yet support stronger semantics cleanly.

In the current live image-bearing set, `site` is not populated at all, so `site_hint` is a forward-compatible field rather than a populated canonical dimension.
