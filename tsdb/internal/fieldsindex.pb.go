// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        (unknown)
// source: internal/fieldsindex.proto

package tsdb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Series struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key  string `protobuf:"bytes,1,opt,name=Key,json=key,proto3" json:"Key,omitempty"`
	Tags []*Tag `protobuf:"bytes,2,rep,name=Tags,json=tags,proto3" json:"Tags,omitempty"`
}

func (x *Series) Reset() {
	*x = Series{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_fieldsindex_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Series) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Series) ProtoMessage() {}

func (x *Series) ProtoReflect() protoreflect.Message {
	mi := &file_internal_fieldsindex_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Series.ProtoReflect.Descriptor instead.
func (*Series) Descriptor() ([]byte, []int) {
	return file_internal_fieldsindex_proto_rawDescGZIP(), []int{0}
}

func (x *Series) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Series) GetTags() []*Tag {
	if x != nil {
		return x.Tags
	}
	return nil
}

type Tag struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=Key,json=key,proto3" json:"Key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=Value,json=value,proto3" json:"Value,omitempty"`
}

func (x *Tag) Reset() {
	*x = Tag{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_fieldsindex_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Tag) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tag) ProtoMessage() {}

func (x *Tag) ProtoReflect() protoreflect.Message {
	mi := &file_internal_fieldsindex_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tag.ProtoReflect.Descriptor instead.
func (*Tag) Descriptor() ([]byte, []int) {
	return file_internal_fieldsindex_proto_rawDescGZIP(), []int{1}
}

func (x *Tag) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Tag) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type MeasurementFields struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name   []byte   `protobuf:"bytes,1,opt,name=Name,json=name,proto3" json:"Name,omitempty"`
	Fields []*Field `protobuf:"bytes,2,rep,name=Fields,json=fields,proto3" json:"Fields,omitempty"`
}

func (x *MeasurementFields) Reset() {
	*x = MeasurementFields{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_fieldsindex_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MeasurementFields) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MeasurementFields) ProtoMessage() {}

func (x *MeasurementFields) ProtoReflect() protoreflect.Message {
	mi := &file_internal_fieldsindex_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MeasurementFields.ProtoReflect.Descriptor instead.
func (*MeasurementFields) Descriptor() ([]byte, []int) {
	return file_internal_fieldsindex_proto_rawDescGZIP(), []int{2}
}

func (x *MeasurementFields) GetName() []byte {
	if x != nil {
		return x.Name
	}
	return nil
}

func (x *MeasurementFields) GetFields() []*Field {
	if x != nil {
		return x.Fields
	}
	return nil
}

type Field struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name []byte `protobuf:"bytes,1,opt,name=Name,json=name,proto3" json:"Name,omitempty"`
	Type int32  `protobuf:"varint,2,opt,name=Type,json=type,proto3" json:"Type,omitempty"`
}

func (x *Field) Reset() {
	*x = Field{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_fieldsindex_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Field) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Field) ProtoMessage() {}

func (x *Field) ProtoReflect() protoreflect.Message {
	mi := &file_internal_fieldsindex_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Field.ProtoReflect.Descriptor instead.
func (*Field) Descriptor() ([]byte, []int) {
	return file_internal_fieldsindex_proto_rawDescGZIP(), []int{3}
}

func (x *Field) GetName() []byte {
	if x != nil {
		return x.Name
	}
	return nil
}

func (x *Field) GetType() int32 {
	if x != nil {
		return x.Type
	}
	return 0
}

type MeasurementFieldSet struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Measurements []*MeasurementFields `protobuf:"bytes,1,rep,name=Measurements,json=measurements,proto3" json:"Measurements,omitempty"`
}

func (x *MeasurementFieldSet) Reset() {
	*x = MeasurementFieldSet{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_fieldsindex_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MeasurementFieldSet) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MeasurementFieldSet) ProtoMessage() {}

func (x *MeasurementFieldSet) ProtoReflect() protoreflect.Message {
	mi := &file_internal_fieldsindex_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MeasurementFieldSet.ProtoReflect.Descriptor instead.
func (*MeasurementFieldSet) Descriptor() ([]byte, []int) {
	return file_internal_fieldsindex_proto_rawDescGZIP(), []int{4}
}

func (x *MeasurementFieldSet) GetMeasurements() []*MeasurementFields {
	if x != nil {
		return x.Measurements
	}
	return nil
}

var File_internal_fieldsindex_proto protoreflect.FileDescriptor

var file_internal_fieldsindex_proto_rawDesc = []byte{
	0x0a, 0x1a, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x66, 0x69, 0x65, 0x6c, 0x64,
	0x73, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x74, 0x73,
	0x64, 0x62, 0x22, 0x39, 0x0a, 0x06, 0x53, 0x65, 0x72, 0x69, 0x65, 0x73, 0x12, 0x10, 0x0a, 0x03,
	0x4b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x1d,
	0x0a, 0x04, 0x54, 0x61, 0x67, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x09, 0x2e, 0x74,
	0x73, 0x64, 0x62, 0x2e, 0x54, 0x61, 0x67, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x22, 0x2d, 0x0a,
	0x03, 0x54, 0x61, 0x67, 0x12, 0x10, 0x0a, 0x03, 0x4b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x4c, 0x0a, 0x11,
	0x4d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x46, 0x69, 0x65, 0x6c, 0x64,
	0x73, 0x12, 0x12, 0x0a, 0x04, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52,
	0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x23, 0x0a, 0x06, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x18,
	0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x74, 0x73, 0x64, 0x62, 0x2e, 0x46, 0x69, 0x65,
	0x6c, 0x64, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x22, 0x2f, 0x0a, 0x05, 0x46, 0x69,
	0x65, 0x6c, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0c, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x22, 0x52, 0x0a, 0x13, 0x4d,
	0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x53,
	0x65, 0x74, 0x12, 0x3b, 0x0a, 0x0c, 0x4d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x6d, 0x65, 0x6e,
	0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x74, 0x73, 0x64, 0x62, 0x2e,
	0x4d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x46, 0x69, 0x65, 0x6c, 0x64,
	0x73, 0x52, 0x0c, 0x6d, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x42,
	0x08, 0x5a, 0x06, 0x2e, 0x3b, 0x74, 0x73, 0x64, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_internal_fieldsindex_proto_rawDescOnce sync.Once
	file_internal_fieldsindex_proto_rawDescData = file_internal_fieldsindex_proto_rawDesc
)

func file_internal_fieldsindex_proto_rawDescGZIP() []byte {
	file_internal_fieldsindex_proto_rawDescOnce.Do(func() {
		file_internal_fieldsindex_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_fieldsindex_proto_rawDescData)
	})
	return file_internal_fieldsindex_proto_rawDescData
}

var file_internal_fieldsindex_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_internal_fieldsindex_proto_goTypes = []interface{}{
	(*Series)(nil),              // 0: tsdb.Series
	(*Tag)(nil),                 // 1: tsdb.Tag
	(*MeasurementFields)(nil),   // 2: tsdb.MeasurementFields
	(*Field)(nil),               // 3: tsdb.Field
	(*MeasurementFieldSet)(nil), // 4: tsdb.MeasurementFieldSet
}
var file_internal_fieldsindex_proto_depIdxs = []int32{
	1, // 0: tsdb.Series.Tags:type_name -> tsdb.Tag
	3, // 1: tsdb.MeasurementFields.Fields:type_name -> tsdb.Field
	2, // 2: tsdb.MeasurementFieldSet.Measurements:type_name -> tsdb.MeasurementFields
	3, // [3:3] is the sub-list for method output_type
	3, // [3:3] is the sub-list for method input_type
	3, // [3:3] is the sub-list for extension type_name
	3, // [3:3] is the sub-list for extension extendee
	0, // [0:3] is the sub-list for field type_name
}

func init() { file_internal_fieldsindex_proto_init() }
func file_internal_fieldsindex_proto_init() {
	if File_internal_fieldsindex_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_fieldsindex_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Series); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_fieldsindex_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Tag); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_fieldsindex_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MeasurementFields); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_fieldsindex_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Field); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_fieldsindex_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MeasurementFieldSet); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_fieldsindex_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_fieldsindex_proto_goTypes,
		DependencyIndexes: file_internal_fieldsindex_proto_depIdxs,
		MessageInfos:      file_internal_fieldsindex_proto_msgTypes,
	}.Build()
	File_internal_fieldsindex_proto = out.File
	file_internal_fieldsindex_proto_rawDesc = nil
	file_internal_fieldsindex_proto_goTypes = nil
	file_internal_fieldsindex_proto_depIdxs = nil
}
