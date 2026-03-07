package remoterocketship

import "time"

func ParseDT(value any) *time.Time {
	raw, ok := value.(string)
	if !ok || raw == "" {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return &parsed
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return &parsed
	}
	return nil
}

func NormalizeDT(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func IsSourceOlderThanPostDate(sourceCreatedAt, postDate *time.Time) bool {
	source := NormalizeDT(sourceCreatedAt)
	post := NormalizeDT(postDate)
	if source == nil || post == nil {
		return false
	}
	return source.Before(*post)
}
