package y3codec

import (
	"github.com/yomorun/y3"
	frame "github.com/yomorun/yomo/core/frame"
)

// encodeStreamFrame Streamframe to Y3 encoded bytes
func encodeStreamFrame(f *frame.StreamFrame) ([]byte, error) {
	id := y3.NewPrimitivePacketEncoder(tagStreamClientID)
	id.SetStringValue(f.ID)

	streamID := y3.NewPrimitivePacketEncoder(tagStreamID)
	streamID.SetInt64Value(f.StreamID)

	chunkSize := y3.NewPrimitivePacketEncoder(tagStreamChunkSize)
	chunkSize.SetUInt32Value(uint32(f.ChunkSize))

	node := y3.NewNodePacketEncoder(byte(f.Type()))
	node.AddPrimitivePacket(id)
	node.AddPrimitivePacket(streamID)
	node.AddPrimitivePacket(chunkSize)

	return node.Encode(), nil
}

// decodeStreamFrame decodes Y3 encoded bytes to StreamFrame.
func decodeStreamFrame(data []byte, f *frame.StreamFrame) error {
	nodeBlock := y3.NodePacket{}
	_, err := y3.DecodeToNodePacket(data, &nodeBlock)
	if err != nil {
		return err
	}
	// id
	if p, ok := nodeBlock.PrimitivePackets[tagStreamClientID]; ok {
		id, err := p.ToUTF8String()
		if err != nil {
			return err
		}
		f.ID = id
	}
	// stream id
	if p, ok := nodeBlock.PrimitivePackets[tagStreamID]; ok {
		steamID, err := p.ToInt64()
		if err != nil {
			return err
		}
		f.StreamID = steamID
	}
	// chunk size
	if p, ok := nodeBlock.PrimitivePackets[tagStreamChunkSize]; ok {
		chunkSize, err := p.ToInt32()
		if err != nil {
			return err
		}
		f.ChunkSize = uint(chunkSize)
	}

	return nil
}

var (
	tagStreamClientID  byte = 0x01
	tagStreamID        byte = 0x02
	tagStreamChunkSize byte = 0x03
)
