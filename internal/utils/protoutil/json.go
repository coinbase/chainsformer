package protoutil

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MarshalJSON marshals the given proto.Message in the JSON format using options in
// MarshalOptions. Do not depend on the output being stable.
func MarshalJSON(m proto.Message) ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		UseProtoNames: true, // Use snake case.
	}

	return marshaler.Marshal(m)
}

// UnmarshalJSON reads the given []byte into the given proto.Message.
// The provided message must be mutable (e.g., a non-nil pointer to a message).
func UnmarshalJSON(data []byte, m proto.Message) error {
	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	return unmarshaler.Unmarshal(data, m)
}
