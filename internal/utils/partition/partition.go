package partition

func GetPartitionByNumber(inputNumber uint64, partitionBySize uint64) (partitionByNumber uint64) {
	if partitionBySize > 0 {
		partitionByNumber = inputNumber / partitionBySize * partitionBySize
	}

	return partitionByNumber
}
