package database

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}
