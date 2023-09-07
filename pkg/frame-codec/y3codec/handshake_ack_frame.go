package y3codec

import (
	"github.com/yomorun/y3"
	frame "github.com/yomorun/yomo/core/frame"
)

// encodeHandshakeAckFrame encodes HandshakeAckFrame to Y3 encoded bytes.
func encodeHandshakeAckFrame(f *frame.HandshakeAckFrame) ([]byte, error) {
	ack := y3.NewNodePacketEncoder(byte(f.Type()))
	// ID
	idBlock := y3.NewPrimitivePacketEncoder(tagHandshakeAckID)
	idBlock.SetStringValue(f.ID)
	// client ID
	clientIDBlock := y3.NewPrimitivePacketEncoder(tagHandshakeAckClientID)
	clientIDBlock.SetStringValue(f.ClientID)
	// streamID
	streamIDBlock := y3.NewPrimitivePacketEncoder(tagHandshakeAckStreamID)
	streamIDBlock.SetInt64Value(f.StreamID)

	ack.AddPrimitivePacket(idBlock)
	ack.AddPrimitivePacket(clientIDBlock)
	ack.AddPrimitivePacket(streamIDBlock)

	return ack.Encode(), nil
}

// decodeHandshakeAckFrame decodes Y3 encoded bytes to HandshakeAckFrame
func decodeHandshakeAckFrame(data []byte, f *frame.HandshakeAckFrame) error {
	node := y3.NodePacket{}
	_, err := y3.DecodeToNodePacket(data, &node)
	if err != nil {
		return err
	}
	// ID
	if idBlock, ok := node.PrimitivePackets[tagHandshakeAckID]; ok {
		id, err := idBlock.ToUTF8String()
		if err != nil {
			return err
		}
		f.ID = id
	}
	// client ID
	if clientIDBlock, ok := node.PrimitivePackets[tagHandshakeAckClientID]; ok {
		clientID, err := clientIDBlock.ToUTF8String()
		if err != nil {
			return err
		}
		f.ClientID = clientID
	}
	// streamID
	if streamIDBlock, ok := node.PrimitivePackets[tagHandshakeAckStreamID]; ok {
		streamID, err := streamIDBlock.ToInt64()
		if err != nil {
			return err
		}
		f.StreamID = streamID
	}
	return nil
}

var (
	tagHandshakeAckID       byte = 0x26
	tagHandshakeAckClientID byte = 0x27
	tagHandshakeAckStreamID byte = 0x28
)
