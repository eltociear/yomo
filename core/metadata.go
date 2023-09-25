package core

import (
	"github.com/yomorun/yomo/core/metadata"
	"golang.org/x/exp/slog"
)

const (
	MetadataSourceIDKey = "yomo-source-id"
	MetadataTIDKey      = "yomo-tid"
	MetadataSIDKey      = "yomo-sid"
	MetaTraced          = "yomo-traced"
	MetaStreamed        = "yomo-streamed"
)

// NewDefaultMetadata returns a default metadata.
func NewDefaultMetadata(sourceID string, tid string, sid string, traced bool, streamed bool) metadata.M {
	// streamed
	streamedString := "false"
	if streamed {
		streamedString = "true"
	}
	tracedString := "false"
	if traced {
		tracedString = "true"
	}
	return metadata.M{
		MetadataSourceIDKey: sourceID,
		MetadataTIDKey:      tid,
		MetadataSIDKey:      sid,
		MetaTraced:          tracedString,
		MetaStreamed:        streamedString,
	}
}

// GetSourceIDFromMetadata gets sourceID from metadata.
func GetSourceIDFromMetadata(m metadata.M) string {
	sourceID, _ := m.Get(MetadataSourceIDKey)
	return sourceID
}

// GetTIDFromMetadata gets TID from metadata.
func GetTIDFromMetadata(m metadata.M) string {
	tid, _ := m.Get(MetadataTIDKey)
	return tid
}

// GetSIDFromMetadata gets SID from metadata.
func GetSIDFromMetadata(m metadata.M) string {
	sid, _ := m.Get(MetadataSIDKey)
	return sid
}

// GetTracedFromMetadata gets traced from metadata.
func GetTracedFromMetadata(m metadata.M) bool {
	traced, _ := m.Get(MetaTraced)
	return traced == "true"
}

// GetStreamedFromMetadata gets streamed from metadata.
func GetStreamedFromMetadata(m metadata.M) bool {
	streamed, _ := m.Get(MetaStreamed)
	return streamed == "true"
}

// SetTIDToMetadata sets tid to metadata.
func SetTIDToMetadata(m metadata.M, tid string) {
	m.Set(MetadataTIDKey, tid)
}

// SetSIDToMetadata sets sid to metadata.
func SetSIDToMetadata(m metadata.M, sid string) {
	m.Set(MetadataSIDKey, sid)
}

// SetTracedToMetadata sets traced to metadata.
func SetTracedToMetadata(m metadata.M, traced bool) {
	tracedString := "false"
	if traced {
		tracedString = "true"
	}
	m.Set(MetaTraced, tracedString)
}

// SetStreamedToMetadata sets streamed to metadata.
func SetStreamedToMetadata(m metadata.M, streamed bool) {
	streamedString := "false"
	if streamed {
		streamedString = "true"
	}
	m.Set(MetaStreamed, streamedString)
}

// MetadataSlogAttr returns slog.Attr from metadata.
func MetadataSlogAttr(md metadata.M) slog.Attr {
	kvStrings := make([]any, len(md)*2)
	i := 0
	for k, v := range md {
		kvStrings[i] = k
		i++
		kvStrings[i] = v
		i++
	}

	return slog.Group("metadata", kvStrings...)
}
