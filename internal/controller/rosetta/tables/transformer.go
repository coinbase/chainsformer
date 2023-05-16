package tables

import (
	"encoding/json"

	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/golang/protobuf/proto"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainsformer/internal/utils/partition"

	rosettaType "github.com/coinbase/chainstorage/protos/coinbase/crypto/rosetta/types"

	"github.com/coinbase/chainsformer/internal/utils/xarrow"
)

func transformTransactions(recordBuilder *array.RecordBuilder, block *rosettaType.Block, partitionBySize uint64) error {
	transactions := block.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	for transactionIndex, transaction := range transactions {
		transactionMetadata, err := toMetadata(transaction.Metadata)
		if err != nil {
			return xerrors.New("failed to marshal transaction metadata to string")
		}

		xarrow.NewRecordAppender(recordBuilder).
			AppendString(transaction.GetTransactionIdentifier().Hash).
			AppendUint64(uint64(transactionIndex)).
			AppendString(block.GetBlockIdentifier().Hash).
			AppendUint64(uint64(block.GetBlockIdentifier().Index)).
			AppendUint64(uint64(block.GetTimestamp().Seconds)).
			AppendList(func(la *xarrow.ListAppender) {
				err = transformOperations(la, transaction)
			}).
			AppendUint64(uint64(len(transaction.GetOperations()))).
			AppendList(func(la *xarrow.ListAppender) {
				transformRelatedTransactions(la, transaction)
			}).
			AppendString(transactionMetadata).
			AppendUint64(partition.GetPartitionByNumber(uint64(block.GetBlockIdentifier().Index), partitionBySize)).
			AppendUint64(uint64(block.GetBlockIdentifier().Index)).
			Build()

		if err != nil {
			return err
		}
	}

	return nil
}

func transformBlocks(recordBuilder *array.RecordBuilder, block *rosettaType.Block, partitionBySize uint64) error {
	metadata, err := toMetadata(block.Metadata)
	if err != nil {
		return xerrors.New("failed to marshal block metadata to string")
	}

	xarrow.NewRecordAppender(recordBuilder).
		AppendString(block.GetBlockIdentifier().Hash).
		AppendString(block.GetParentBlockIdentifier().Hash).
		AppendUint64(uint64(block.GetBlockIdentifier().Index)).
		AppendUint64(uint64(block.GetParentBlockIdentifier().Index)).
		AppendUint64(uint64(block.GetTimestamp().Seconds)).
		AppendUint64(uint64(len(block.GetTransactions()))).
		AppendList(func(la *xarrow.ListAppender) {
			for _, transaction := range block.Transactions {
				la.AppendString(transaction.GetTransactionIdentifier().Hash)
			}
		}).
		AppendString(metadata).
		AppendUint64(partition.GetPartitionByNumber(uint64(block.GetBlockIdentifier().Index), partitionBySize)).
		AppendUint64(uint64(block.GetBlockIdentifier().Index)).
		Build()

	return nil
}

func transformRawRosettaStreamedTransactions(recordBuilder *array.RecordBuilder, block *rosettaType.Block, event *chainstorageapi.BlockchainEvent, partitionBySize uint64) error {
	transactions := block.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	for i, transaction := range transactions {
		data, err := proto.Marshal(transaction)
		if err != nil {
			return xerrors.New("transaction failed to marshal into protobuf")
		}

		xarrow.NewRecordAppender(recordBuilder).
			AppendInt64(event.GetSequenceNum()).
			AppendString(event.GetType().String()).
			AppendStruct(transformStreamedBlock(block)).
			AppendUint64(uint64(i)).
			AppendBinary(data).
			AppendUint64(partition.GetPartitionByNumber(uint64(event.GetSequenceNum()), partitionBySize)).
			AppendUint64(uint64(event.GetSequenceNum())).
			Build()
	}

	return nil
}

func transformStreamedBlock(block *rosettaType.Block) func(*xarrow.StructAppender) {
	return func(sa *xarrow.StructAppender) {
		sa.AppendStruct(transformBlockIdentifier(block.GetBlockIdentifier())).
			AppendStruct(transformBlockIdentifier(block.GetParentBlockIdentifier())).
			AppendUint64(uint64(block.GetTimestamp().GetSeconds()))
	}
}

func transformBlockIdentifier(id *rosettaType.BlockIdentifier) func(sa *xarrow.StructAppender) {
	return func(sa *xarrow.StructAppender) {
		sa.AppendUint64(uint64(id.GetIndex())).
			AppendString(id.GetHash())
	}
}

func transformOperations(la *xarrow.ListAppender, transaction *rosettaType.Transaction) error {
	for _, operation := range transaction.Operations {
		metadata, err := toMetadata(operation.Metadata)
		if err != nil {
			return xerrors.New("failed to marshal operation metadata to string")
		}

		value, err := xarrow.Decimal128FromString(operation.GetAmount().GetValue())
		if err != nil {
			value, err = xarrow.Decimal128FromString("-1")
			if err != nil {
				return xerrors.New("failed to convert operation amount to decimal")
			}
		}

		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendUint64(uint64(operation.OperationIdentifier.Index)).
				AppendUint64(uint64(operation.OperationIdentifier.NetworkIndex)).
				AppendList(func(la *xarrow.ListAppender) {
					transformRelatedOperations(la, operation)
				}).
				AppendString(operation.Type).
				AppendString(operation.Status).
				AppendString(operation.GetAccount().GetAddress()).
				AppendString(operation.GetAccount().GetSubAccount().GetAddress())

			if err != nil {
				sa.AppendDecimal128Null()
			} else {
				sa.AppendDecimal128(value)
			}

			sa.AppendString(operation.GetAmount().GetValue()).
				AppendString(operation.GetAmount().GetCurrency().GetSymbol()).
				AppendUint64(uint64(operation.GetAmount().GetCurrency().GetDecimals())).
				AppendString(operation.GetCoinChange().GetCoinIdentifier().GetIdentifier()).
				AppendString(operation.GetCoinChange().GetCoinAction().String()).
				AppendString(metadata)
		})
	}

	return nil
}

func transformRelatedOperations(la *xarrow.ListAppender, operation *rosettaType.Operation) {
	for _, relatedOperation := range operation.RelatedOperations {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendUint64(uint64(relatedOperation.Index)).
				AppendUint64(uint64(relatedOperation.NetworkIndex))
		})
	}
}

func transformRelatedTransactions(la *xarrow.ListAppender, transaction *rosettaType.Transaction) {
	for _, relatedTransaction := range transaction.RelatedTransactions {
		la.AppendStruct(func(sa *xarrow.StructAppender) {
			sa.AppendString(relatedTransaction.GetTransactionIdentifier().Hash).
				AppendString(relatedTransaction.GetDirection().String())
		})
	}
}

func toMetadata(metadata map[string]*anypb.Any) (string, error) {
	marshalledMetadata := make(map[string]json.RawMessage, len(metadata))
	for k, v := range metadata {
		if v == nil {
			continue
		}

		data, err := protojson.Marshal(v)
		if err != nil {
			return "", xerrors.Errorf("failed to marshal metadata at key %v: %w", k, err)
		}

		marshalledMetadata[k] = data
	}

	if len(marshalledMetadata) == 0 {
		return "", nil
	}

	res, err := json.Marshal(marshalledMetadata)
	if err != nil {
		return "", xerrors.Errorf("failed to marshal metadata: %w", err)
	}

	return string(res), nil
}
