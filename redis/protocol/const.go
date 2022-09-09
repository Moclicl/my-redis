package protocol

type EmptyMultiBulkReply struct {
}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return []byte("*0\r\n")
}
