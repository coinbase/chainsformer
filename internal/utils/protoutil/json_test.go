package protoutil

import (
	"encoding/json"
	"testing"

	"github.com/coinbase/chainsformer/internal/utils/testutil"
	api "github.com/coinbase/chainsformer/protos/coinbase/chainsformer"
)

func TestMarshalBatchQueryJSON(t *testing.T) {
	require := testutil.Require(t)

	expected := `{
  "batch_query": {
    "start_height": "11000000",
    "end_height": "13000000",
    "blocks_per_partition": "1000",
    "blocks_per_record": "10"
  }
}`
	input := &api.GetFlightInfoCmd{
		Query: &api.GetFlightInfoCmd_BatchQuery_{
			BatchQuery: &api.GetFlightInfoCmd_BatchQuery{
				StartHeight:        11_000_000,
				EndHeight:          13_000_000,
				BlocksPerPartition: 1000,
				BlocksPerRecord:    10,
			},
		},
	}

	actual, err := MarshalJSON(input)
	require.NoError(err)

	// Output of MarshalJSON is unstable. Reformat to get a deterministic result.
	actualFormatted, err := json.MarshalIndent(json.RawMessage(actual), "", "  ")
	require.NoError(err)
	require.Equal(expected, string(actualFormatted))

	var output api.GetFlightInfoCmd
	err = UnmarshalJSON(actual, &output)
	require.NoError(err)
	require.Equal(input, &output)
}

func TestMarshalStreamQueryJSON(t *testing.T) {
	require := testutil.Require(t)

	expected := `{
  "stream_query": {
    "start_sequence": "11000001",
    "end_sequence": "13000001",
    "events_per_partition": "1000",
    "events_per_record": "10"
  }
}`
	input := &api.GetFlightInfoCmd{
		Query: &api.GetFlightInfoCmd_StreamQuery_{
			StreamQuery: &api.GetFlightInfoCmd_StreamQuery{
				StartSequence:      11_000_001,
				EndSequence:        13_000_001,
				EventsPerPartition: 1000,
				EventsPerRecord:    10,
			},
		},
	}

	actual, err := MarshalJSON(input)
	require.NoError(err)

	// Output of MarshalJSON is unstable. Reformat to get a deterministic result.
	actualFormatted, err := json.MarshalIndent(json.RawMessage(actual), "", "  ")
	require.NoError(err)
	require.Equal(expected, string(actualFormatted))

	var output api.GetFlightInfoCmd
	err = UnmarshalJSON(actual, &output)
	require.NoError(err)
	require.Equal(input, &output)
}
