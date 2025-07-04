// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v3.21.12
// source: pb/placement.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Draw struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	X             int32                  `protobuf:"varint,1,opt,name=x,proto3" json:"x,omitempty"`
	Y             int32                  `protobuf:"varint,2,opt,name=y,proto3" json:"y,omitempty"`
	R             int32                  `protobuf:"varint,3,opt,name=r,proto3" json:"r,omitempty"`
	G             int32                  `protobuf:"varint,4,opt,name=g,proto3" json:"g,omitempty"`
	B             int32                  `protobuf:"varint,5,opt,name=b,proto3" json:"b,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Draw) Reset() {
	*x = Draw{}
	mi := &file_pb_placement_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Draw) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Draw) ProtoMessage() {}

func (x *Draw) ProtoReflect() protoreflect.Message {
	mi := &file_pb_placement_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Draw.ProtoReflect.Descriptor instead.
func (*Draw) Descriptor() ([]byte, []int) {
	return file_pb_placement_proto_rawDescGZIP(), []int{0}
}

func (x *Draw) GetX() int32 {
	if x != nil {
		return x.X
	}
	return 0
}

func (x *Draw) GetY() int32 {
	if x != nil {
		return x.Y
	}
	return 0
}

func (x *Draw) GetR() int32 {
	if x != nil {
		return x.R
	}
	return 0
}

func (x *Draw) GetG() int32 {
	if x != nil {
		return x.G
	}
	return 0
}

func (x *Draw) GetB() int32 {
	if x != nil {
		return x.B
	}
	return 0
}

var File_pb_placement_proto protoreflect.FileDescriptor

const file_pb_placement_proto_rawDesc = "" +
	"\n" +
	"\x12pb/placement.proto\"L\n" +
	"\x04Draw\x12\f\n" +
	"\x01x\x18\x01 \x01(\x05R\x01x\x12\f\n" +
	"\x01y\x18\x02 \x01(\x05R\x01y\x12\f\n" +
	"\x01r\x18\x03 \x01(\x05R\x01r\x12\f\n" +
	"\x01g\x18\x04 \x01(\x05R\x01g\x12\f\n" +
	"\x01b\x18\x05 \x01(\x05R\x01bB\x0eZ\fPlacement/pbb\x06proto3"

var (
	file_pb_placement_proto_rawDescOnce sync.Once
	file_pb_placement_proto_rawDescData []byte
)

func file_pb_placement_proto_rawDescGZIP() []byte {
	file_pb_placement_proto_rawDescOnce.Do(func() {
		file_pb_placement_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_pb_placement_proto_rawDesc), len(file_pb_placement_proto_rawDesc)))
	})
	return file_pb_placement_proto_rawDescData
}

var file_pb_placement_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_pb_placement_proto_goTypes = []any{
	(*Draw)(nil), // 0: Draw
}
var file_pb_placement_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_pb_placement_proto_init() }
func file_pb_placement_proto_init() {
	if File_pb_placement_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_pb_placement_proto_rawDesc), len(file_pb_placement_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_pb_placement_proto_goTypes,
		DependencyIndexes: file_pb_placement_proto_depIdxs,
		MessageInfos:      file_pb_placement_proto_msgTypes,
	}.Build()
	File_pb_placement_proto = out.File
	file_pb_placement_proto_goTypes = nil
	file_pb_placement_proto_depIdxs = nil
}
