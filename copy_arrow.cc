// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

extern "C"
{
#include <postgres.h>

#include <access/table.h>
#include <access/tableam.h>
#include <commands/copy.h>
#include <executor/tuptable.h>
#include <nodes/makefuncs.h>
#include <nodes/value.h>
#include <storage/ipc.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>

#include <arpa/inet.h>
}

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/table_builder.h>
#include <arrow/util/logging.h>

namespace {
class Listener {
   public:
	Listener() = default;
	virtual ~Listener() = default;

	virtual void on_start() {}

	virtual void on_start_tuple() {}

	// null
	virtual void on_field_value(size_t nth_field) {}

	virtual void on_field_value(size_t nth_field, int32_t value) {}

	virtual void on_field_value(size_t nth_field, std::string_view value) {}

	virtual void on_finish_tuple() {}

	virtual void on_finish() {}
};

// See the "Binary Format" section in
// https://www.postgresql.org/docs/current/sql-copy.html for
// details.
class BinaryFormatParser {
   private:
	enum class State
	{
		kSignature,
		kHeaderFlags,
		kHeaderExtensionLength,
		kHeaderExtension,
		kTuple,
		kFieldSize,
		kFieldValue,
		kFinish,
	};

	// The last '\0' is also part of the signature.
	static constexpr char kSignature[11] = "PGCOPY\n\377\r\n";
	static constexpr size_t kSignatureSize = sizeof(kSignature);

	std::vector<Oid> field_types_;
	std::shared_ptr<Listener> listener_;
	std::string buffer_;
	State state_;
	uint32_t header_extension_length_;
	uint16_t i_field_;
	uint16_t n_fields_;
	uint32_t field_value_size_;

   public:
	class ParseException : public std::exception {
	   public:
		ParseException(std::string message) : message_(std::move(message)) {}
		const char* what() const noexcept override { return message_.c_str(); }

	   private:
		std::string message_;
	};

	BinaryFormatParser(std::vector<Oid> field_types, std::shared_ptr<Listener> listener)
		: field_types_(std::move(field_types)),
		  listener_(std::move(listener)),
		  buffer_(),
		  state_(State::kSignature),
		  header_extension_length_(0),
		  i_field_(0),
		  n_fields_(0)
	{
	}

	void emit(const void* data, size_t length)
	{
		buffer_.append(static_cast<const char*>(data), length);
		while (!buffer_.empty())
		{
			switch (state_)
			{
				case State::kSignature:
					if (buffer_.size() < kSignatureSize)
					{
						return;
					}
					if (memcmp(buffer_.data(), kSignature, kSignatureSize) != 0)
					{
						throw ParseException("Invalid signature");
					}
					buffer_.erase(0, kSignatureSize);
					state_ = State::kHeaderFlags;
					break;
				case State::kHeaderFlags:
				{
					auto flags_result = read_uint32();
					if (!flags_result)
					{
						return;
					}
					state_ = State::kHeaderExtensionLength;
					break;
				}
				case State::kHeaderExtensionLength:
				{
					auto length_result = read_uint32();
					if (!length_result)
					{
						return;
					}
					header_extension_length_ = length_result.value();
					state_ = State::kHeaderExtension;
					break;
				}
				case State::kHeaderExtension:
					if (buffer_.size() < header_extension_length_)
					{
						return;
					}
					buffer_.erase(0, header_extension_length_);
					listener_->on_start();
					state_ = State::kTuple;
					break;
				case State::kTuple:
				{
					auto n_fields_result = read_uint16();
					if (!n_fields_result)
					{
						return;
					}
					i_field_ = 0;
					n_fields_ = n_fields_result.value();
					if (n_fields_ == static_cast<uint16_t>(-1))
					{
						listener_->on_finish();
						state_ = State::kFinish;
					}
					else
					{
						listener_->on_start_tuple();
						state_ = State::kFieldSize;
					}
					break;
				}
				case State::kFieldSize:
				{
					auto size_result = read_uint32();
					if (!size_result)
					{
						return;
					}
					field_value_size_ = size_result.value();
					if (field_value_size_ == static_cast<uint32_t>(-1))
					{
						listener_->on_field_value(i_field_);
						++i_field_;
						if (i_field_ < n_fields_)
						{
							state_ = State::kFieldSize;
						}
						else
						{
							listener_->on_finish_tuple();
							state_ = State::kTuple;
						}
					}
					else if (field_value_size_ == 0)
					{
						// TODO: We assume that 0 size value is TEXTOID
						listener_->on_field_value(i_field_, std::string_view(""));
						++i_field_;
						if (i_field_ < n_fields_)
						{
							state_ = State::kFieldSize;
						}
						else
						{
							listener_->on_finish_tuple();
							state_ = State::kTuple;
						}
					}
					else
					{
						state_ = State::kFieldValue;
					}
					break;
				}
				case State::kFieldValue:
					switch (field_types_[i_field_])
					{
						case INT4OID:
						{
							auto value_result = read_uint32();
							if (!value_result)
							{
								return;
							}
							listener_->on_field_value(
								i_field_, static_cast<int32_t>(value_result.value()));
						}
						break;
						case TEXTOID:
							if (buffer_.size() < field_value_size_)
							{
								return;
							}
							listener_->on_field_value(
								i_field_,
								std::string_view(buffer_.data(), field_value_size_));
							buffer_.erase(0, field_value_size_);
							break;
						default:
							throw ParseException("Unsuppored type");
					}
					i_field_++;
					if (i_field_ < n_fields_)
					{
						state_ = State::kFieldSize;
					}
					else
					{
						listener_->on_finish_tuple();
						state_ = State::kTuple;
					}
					break;
				case State::kFinish:
					throw ParseException("Data after finish");
				default:
					throw ParseException("Invalid state");
			}
		}
	}

   private:
	std::optional<uint16_t> read_uint16()
	{
		if (buffer_.size() < sizeof(uint16_t))
		{
			return std::nullopt;
		}
		auto value = ntohs(*reinterpret_cast<uint16_t*>(buffer_.data()));
		buffer_.erase(0, sizeof(uint16_t));
		return value;
	}

	std::optional<uint32_t> read_uint32()
	{
		if (buffer_.size() < sizeof(uint32_t))
		{
			return std::nullopt;
		}
		auto value = ntohl(*reinterpret_cast<uint32_t*>(buffer_.data()));
		buffer_.erase(0, sizeof(uint32_t));
		return value;
	}
};

class ArrowStreamingFormatWriter : public Listener {
   public:
	ArrowStreamingFormatWriter(std::shared_ptr<arrow::io::OutputStream> output,
	                           std::shared_ptr<arrow::Schema> schema)
		: writer_(*arrow::ipc::MakeStreamWriter(std::move(output), schema)),
		  builder_(*arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool()))
	{
	}

	// null
	void on_field_value(size_t nth_field) override
	{
		ARROW_IGNORE_EXPR(builder_->GetField(nth_field)->AppendNull());
	}

	void on_field_value(size_t nth_field, int32_t value) override
	{
		ARROW_IGNORE_EXPR(
			builder_->GetFieldAs<arrow::Int32Builder>(nth_field)->Append(value));
	}

	void on_field_value(size_t nth_field, std::string_view value) override
	{
		ARROW_IGNORE_EXPR(
			builder_->GetFieldAs<arrow::StringBuilder>(nth_field)->Append(value));
	}

	void on_finish() override
	{
		ARROW_IGNORE_EXPR(writer_->WriteRecordBatch(**builder_->Flush()));
		ARROW_IGNORE_EXPR(writer_->Close());
	}

   private:
	std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_;
	std::shared_ptr<arrow::RecordBatchBuilder> builder_;
};

std::shared_ptr<arrow::io::BufferOutputStream> copy_arrow_output = nullptr;
std::shared_ptr<BinaryFormatParser> copy_arrow_binary_format_parser = nullptr;

void
copy_arrow_before_shmem_exit(int code, Datum arg)
{
	copy_arrow_output = nullptr;
	copy_arrow_binary_format_parser = nullptr;
}

void
copy_arrow_to_callback(void* data, int len)
{
	copy_arrow_binary_format_parser->emit(data, len);
}

std::shared_ptr<arrow::Schema>
build_schema(TupleDesc tuple_desc)
{
	arrow::SchemaBuilder schema_builder;
	for (int i = 0; i < tuple_desc->natts; ++i)
	{
		auto attribute = TupleDescAttr(tuple_desc, i);
		std::shared_ptr<arrow::Field> field;
		switch (attribute->atttypid)
		{
			case INT4OID:
				field = arrow::field(
					NameStr(attribute->attname), arrow::int32(), !attribute->attnotnull);
				break;
			case TEXTOID:
				field = arrow::field(
					NameStr(attribute->attname), arrow::utf8(), !attribute->attnotnull);
				break;
			default:
				ereport(ERROR,
				        errcode(ERRCODE_INTERNAL_ERROR),
				        errmsg("unsupported type: %u", attribute->atttypid));
				break;
		}
		ARROW_IGNORE_EXPR(schema_builder.AddField(std::move(field)));
	}
	return *schema_builder.Finish();
}

};  // namespace

extern "C"
{
	PG_MODULE_MAGIC;

	extern PGDLLEXPORT void _PG_init(void)
	{
		before_shmem_exit(copy_arrow_before_shmem_exit, 0);
	}

	PGDLLEXPORT PG_FUNCTION_INFO_V1(copy_to_arrow);
	Datum copy_to_arrow(PG_FUNCTION_ARGS)
	{
		auto table = table_open(PG_GETARG_OID(0), AccessShareLock);
		auto tuple_desc = RelationGetDescr(table);

		std::vector<Oid> field_types;
		for (int i = 0; i < tuple_desc->natts; ++i)
		{
			auto attribute = TupleDescAttr(tuple_desc, i);
			field_types.push_back(attribute->atttypid);
		}
		auto schema = build_schema(tuple_desc);
		copy_arrow_output = *arrow::io::BufferOutputStream::Create();
		auto writer =
			std::make_shared<ArrowStreamingFormatWriter>(copy_arrow_output, schema);
		copy_arrow_binary_format_parser = std::make_shared<BinaryFormatParser>(
			std::move(field_types), std::move(writer));

		CopyToState cstate;
		auto options = list_make1(
			makeDefElem(const_cast<char*>("format"),
		                reinterpret_cast<Node*>(makeString(const_cast<char*>("binary"))),
		                -1));

		cstate = BeginCopyTo(NULL,
		                     table,
		                     NULL,
		                     RelationGetRelid(table),
		                     NULL,
		                     NULL,
		                     copy_arrow_to_callback,
		                     NIL,
		                     options);
		DoCopyTo(cstate);
		EndCopyTo(cstate);

		table_close(table, NoLock);

		auto arrow_buffer = *copy_arrow_output->Finish();
		auto arrow_data = static_cast<bytea*>(palloc0(VARHDRSZ + arrow_buffer->size()));
		SET_VARSIZE(arrow_data, VARHDRSZ + arrow_buffer->size());
		memcpy(VARDATA(arrow_data), arrow_buffer->data(), arrow_buffer->size());
		PG_RETURN_BYTEA_P(arrow_data);
	}

	PGDLLEXPORT PG_FUNCTION_INFO_V1(scan_to_arrow);
	Datum scan_to_arrow(PG_FUNCTION_ARGS)
	{
		auto table = table_open(PG_GETARG_OID(0), AccessShareLock);
		auto tuple_desc = RelationGetDescr(table);
		auto schema = build_schema(tuple_desc);
		auto builder =
			*arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool());
		auto scan = table_beginscan(table, GetActiveSnapshot(), 0, nullptr);
		auto slot = table_slot_create(table, nullptr);
		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			for (int i = 0; i < tuple_desc->natts; ++i)
			{
				bool is_null;
				auto datum = slot_getattr(slot, i + 1, &is_null);
				if (is_null)
				{
					ARROW_IGNORE_EXPR(builder->GetField(i)->AppendNull());
					continue;
				}

				auto attribute = TupleDescAttr(tuple_desc, i);
				switch (attribute->atttypid)
				{
					case INT4OID:
						ARROW_IGNORE_EXPR(
							builder->GetFieldAs<arrow::Int32Builder>(i)->Append(
								DatumGetInt32(datum)));
						break;
					case TEXTOID:
						ARROW_IGNORE_EXPR(
							builder->GetFieldAs<arrow::StringBuilder>(i)->Append(
								VARDATA_ANY(datum), VARSIZE_ANY_EXHDR(datum)));
						break;
					default:
						ereport(ERROR,
						        errcode(ERRCODE_INTERNAL_ERROR),
						        errmsg("unsupported type: %u", attribute->atttypid));
						break;
				}
			}
		}
		ExecDropSingleTupleTableSlot(slot);
		table_endscan(scan);
		table_close(table, NoLock);

		copy_arrow_output = *arrow::io::BufferOutputStream::Create();
		{
			auto writer = *arrow::ipc::MakeStreamWriter(copy_arrow_output, schema);
			ARROW_IGNORE_EXPR(writer->WriteRecordBatch(**builder->Flush()));
			ARROW_IGNORE_EXPR(writer->Close());
		}
		auto arrow_buffer = *copy_arrow_output->Finish();
		auto arrow_data = static_cast<bytea*>(palloc0(VARHDRSZ + arrow_buffer->size()));
		SET_VARSIZE(arrow_data, VARHDRSZ + arrow_buffer->size());
		memcpy(VARDATA(arrow_data), arrow_buffer->data(), arrow_buffer->size());
		PG_RETURN_BYTEA_P(arrow_data);
	}

	PGDLLEXPORT PG_FUNCTION_INFO_V1(format_arrow);
	Datum format_arrow(PG_FUNCTION_ARGS)
	{
		auto arrow_data = PG_GETARG_BYTEA_P(0);
		auto arrow_buffer = std::make_shared<arrow::Buffer>(
			reinterpret_cast<const uint8_t*>(VARDATA_ANY(arrow_data)),
			VARSIZE_ANY_EXHDR(arrow_data));
		arrow::io::BufferReader input(arrow_buffer);
		auto reader = *arrow::ipc::RecordBatchStreamReader::Open(&input);
		auto table = *reader->ToTable();
		auto table_string = table->ToString();
		PG_RETURN_TEXT_P(
			cstring_to_text_with_len(table_string.data(), table_string.length()));
	}
}
