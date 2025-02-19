////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "index/comparer.hpp"
#include "index/norm.hpp"
#include "index_tests.hpp"
#include "search/term_filter.hpp"
#include "store/memory_directory.hpp"
#include "store/mmap_directory.hpp"
#include "tests_shared.hpp"
#include "utils/index_utils.hpp"

namespace {

struct EmptyField : tests::ifield {
  std::string_view name() const final {
    EXPECT_FALSE(true);
    throw irs::not_impl_error{};
  }

  irs::token_stream& get_tokens() const final {
    EXPECT_FALSE(true);
    throw irs::not_impl_error{};
  }

  irs::features_t features() const final {
    EXPECT_FALSE(true);
    throw irs::not_impl_error{};
  }

  irs::IndexFeatures index_features() const final {
    EXPECT_FALSE(true);
    throw irs::not_impl_error{};
  }

  bool write(irs::data_output&) const final { return false; }

  mutable irs::null_token_stream stream_;
};

const EmptyField kEmpty;

auto MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::by_term>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

class SortedEuroparlDocTemplate : public tests::europarl_doc_template {
 public:
  explicit SortedEuroparlDocTemplate(
    std::string field, std::vector<irs::type_info::type_id> field_features)
    : field_{std::move(field)}, field_features_{std::move(field_features)} {}

  void init() final {
    indexed.push_back(std::make_shared<tests::string_field>(
      "title", irs::IndexFeatures::ALL, field_features_));
    indexed.push_back(
      std::make_shared<text_ref_field>("title_anl", false, field_features_));
    indexed.push_back(
      std::make_shared<text_ref_field>("title_anl_pay", true, field_features_));
    indexed.push_back(
      std::make_shared<text_ref_field>("body_anl", false, field_features_));
    indexed.push_back(
      std::make_shared<text_ref_field>("body_anl_pay", true, field_features_));
    {
      insert(std::make_shared<tests::long_field>());
      auto& field = static_cast<tests::long_field&>(indexed.back());
      field.name("date");
    }
    insert(std::make_shared<tests::string_field>(
      "datestr", irs::IndexFeatures::ALL, field_features_));
    insert(std::make_shared<tests::string_field>(
      "body", irs::IndexFeatures::ALL, field_features_));
    {
      insert(std::make_shared<tests::int_field>());
      auto& field = static_cast<tests::int_field&>(indexed.back());
      field.name("id");
    }
    insert(std::make_shared<tests::string_field>(
      "idstr", irs::IndexFeatures::ALL, field_features_));

    auto fields = indexed.find(field_);

    if (!fields.empty()) {
      sorted = fields[0];
    }
  }

 private:
  std::string field_;  // sorting field
  std::vector<irs::type_info::type_id> field_features_;
};

class StringComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    const auto lhs_value = irs::to_string<irs::bytes_view>(lhs.data());
    const auto rhs_value = irs::to_string<irs::bytes_view>(rhs.data());

    return rhs_value.compare(lhs_value);
  }
};

class LongComparer final : public irs::Comparer {
  int CompareImpl(irs::bytes_view lhs, irs::bytes_view rhs) const final {
    EXPECT_FALSE(irs::IsNull(lhs));
    EXPECT_FALSE(irs::IsNull(rhs));

    auto* plhs = lhs.data();
    const auto lhs_value = irs::zig_zag_decode64(irs::vread<uint64_t>(plhs));
    auto* prhs = rhs.data();
    const auto rhs_value = irs::zig_zag_decode64(irs::vread<uint64_t>(prhs));

    if (lhs_value < rhs_value) {
      return -1;
    }

    if (rhs_value < lhs_value) {
      return 1;
    }

    return 0;
  }
};

struct CustomFeature {
  struct header {
    explicit header(std::span<const irs::bytes_view> headers) noexcept {
      for (const auto header : headers) {
        update(header);
      }
    }

    void write(irs::bstring& out) const {
      EXPECT_TRUE(out.empty());
      out.resize(sizeof(size_t));

      auto* p = out.data();
      irs::write<size_t>(p, count);
    }

    void update(irs::bytes_view in) {
      EXPECT_EQ(sizeof(count), in.size());
      auto* p = in.data();
      count += irs::read<decltype(count)>(p);
    }

    size_t count{0};
  };

  struct writer : irs::FeatureWriter {
    explicit writer(std::span<const irs::bytes_view> headers) noexcept
      : hdr{{}} {
      if (!headers.empty()) {
        init_header.emplace(headers);
      }
    }

    void write(const irs::field_stats& stats, irs::doc_id_t doc,
               // cppcheck-suppress constParameter
               irs::column_output& writer) final {
      ++hdr.count;

      // We intentionally call `writer(doc)` multiple
      // times to check concatenation logic.
      writer(doc).write_int(stats.len);
      writer(doc).write_int(stats.max_term_freq);
      writer(doc).write_int(stats.num_overlap);
      writer(doc).write_int(stats.num_unique);
    }

    virtual void write(irs::data_output& out, irs::bytes_view payload) {
      if (!payload.empty()) {
        ++hdr.count;
        out.write_bytes(payload.data(), payload.size());
      }
    }

    void finish(irs::bstring& out) final {
      if (init_header.has_value()) {
        // <= due to removals
        EXPECT_LE(hdr.count, init_header.value().count);
      }
      hdr.write(out);
    }

    header hdr;
    std::optional<header> init_header;
    std::optional<size_t> expected_count;
  };

  static irs::FeatureWriter::ptr make_writer(
    std::span<const irs::bytes_view> payload) {
    return irs::memory::make_managed<writer>(payload);
  }
};

REGISTER_ATTRIBUTE(CustomFeature);

class SortedIndexTestCase : public tests::index_test_base {
 protected:
  bool supports_pluggable_features() const noexcept {
    // old formats don't support pluggable features
    constexpr std::string_view kOldFormats[]{"1_0", "1_1", "1_2", "1_3",
                                             "1_3simd"};

    return std::end(kOldFormats) == std::find(std::begin(kOldFormats),
                                              std::end(kOldFormats),
                                              codec()->type()().name());
  }

  irs::FeatureInfoProvider features() {
    return [this](irs::type_info::type_id id) {
      if (id == irs::type<irs::Norm>::id()) {
        return std::make_pair(
          irs::ColumnInfo{irs::type<irs::compression::lz4>::get(), {}, false},
          &irs::Norm::MakeWriter);
      }

      if (supports_pluggable_features()) {
        if (irs::type<irs::Norm2>::id() == id) {
          return std::make_pair(
            irs::ColumnInfo{
              irs::type<irs::compression::none>::get(), {}, false},
            &irs::Norm2::MakeWriter);
        } else if (irs::type<CustomFeature>::id() == id) {
          return std::make_pair(
            irs::ColumnInfo{
              irs::type<irs::compression::none>::get(), {}, false},
            &CustomFeature::make_writer);
        }
      }

      return std::make_pair(
        irs::ColumnInfo{irs::type<irs::compression::none>::get(), {}, false},
        irs::FeatureWriterFactory{});
    };
  }

  std::vector<irs::type_info::type_id> field_features() {
    return supports_pluggable_features()
             ? std::vector<
                 irs::type_info::type_id>{irs::type<irs::Norm>::id(),
                                          irs::type<irs::Norm2>::id(),
                                          irs::type<CustomFeature>::id()}
             : std::vector<irs::type_info::type_id>{irs::type<irs::Norm>::id()};
  }

  void assert_index(size_t skip = 0,
                    irs::automaton_table_matcher* matcher = nullptr) const {
    index_test_base::assert_index(irs::IndexFeatures::NONE, skip, matcher);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ, skip, matcher);
    index_test_base::assert_index(irs::IndexFeatures::NONE |
                                    irs::IndexFeatures::FREQ |
                                    irs::IndexFeatures::POS,
                                  skip, matcher);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ |
        irs::IndexFeatures::POS | irs::IndexFeatures::OFFS,
      skip, matcher);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ |
        irs::IndexFeatures::POS | irs::IndexFeatures::PAY,
      skip, matcher);
    index_test_base::assert_index(
      irs::IndexFeatures::NONE | irs::IndexFeatures::FREQ |
        irs::IndexFeatures::POS | irs::IndexFeatures::OFFS |
        irs::IndexFeatures::PAY,
      skip, matcher);
    index_test_base::assert_columnstore();
  }

  void check_feature_header(const irs::SubReader& segment,
                            const irs::field_meta& field,
                            irs::type_info::type_id type,
                            irs::bytes_view header) {
    ASSERT_TRUE(supports_pluggable_features());
    auto feature = field.features.find(type);
    ASSERT_NE(feature, field.features.end());
    ASSERT_TRUE(irs::field_limits::valid(feature->second));
    auto* column = segment.column(feature->second);
    ASSERT_NE(nullptr, column);
    ASSERT_FALSE(irs::IsNull(column->payload()));
    ASSERT_EQ(header, column->payload());
  }

  void check_empty_feature(const irs::SubReader& segment,
                           const irs::field_meta& field,
                           irs::type_info::type_id type) {
    ASSERT_TRUE(supports_pluggable_features());
    auto feature = field.features.find(type);
    ASSERT_NE(feature, field.features.end());
    ASSERT_FALSE(irs::field_limits::valid(feature->second));
    auto* column = segment.column(feature->second);
    ASSERT_EQ(nullptr, column);
  }

  void check_features(const irs::SubReader& segment,
                      std::string_view field_name, size_t count,
                      bool after_consolidation) {
    auto* field_reader = segment.field(field_name);
    ASSERT_NE(nullptr, field_reader);
    auto& field = field_reader->meta();
    ASSERT_EQ(3, field.features.size());

    // irs::norm, nothing is written since all values are equal to 1
    check_empty_feature(segment, field, irs::type<irs::Norm>::id());

    // custom_feature
    {
      irs::byte_type buf[sizeof(count)];
      auto* p = buf;
      irs::write<size_t>(p, count);

      check_feature_header(segment, field, irs::type<CustomFeature>::id(),
                           {buf, sizeof buf});
    }

    // irs::Norm2
    {
      irs::Norm2Header hdr{after_consolidation ? irs::Norm2Encoding::Byte
                                               : irs::Norm2Encoding::Int};
      hdr.Reset(1);

      irs::bstring buf;
      irs::Norm2Header::Write(hdr, buf);

      check_feature_header(segment, field, irs::type<irs::Norm2>::id(), buf);
    }
  }
};

TEST_P(SortedIndexTestCase, simple_sequential) {
  constexpr std::string_view sorted_column = "name";

  // Build index
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [&sorted_column, this](tests::document& doc, const std::string& name,
                           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == sorted_column) {
          doc.sorted = field;
        }
      } else if (data.is_number()) {
        auto field = std::make_shared<tests::long_field>();
        field->name(name);
        field->value(data.ui);

        doc.insert(field);
      }
    });

  StringComparer compare;

  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = features();

  add_segment(gen, irs::OM_CREATE, opts);  // add segment

  // Check index
  assert_index();

  // Check columns
  {
    auto reader = irs::DirectoryReader(dir(), codec());

    if (dynamic_cast<irs::memory_directory*>(&dir()) == nullptr) {
      auto name = codec()->type()().name();
      EXPECT_EQ(GetResourceManager().file_descriptors.counter_, 5) << name;
    }
#ifdef __linux__
    if (dynamic_cast<irs::MMapDirectory*>(&dir()) != nullptr) {
      EXPECT_GT(reader->CountMappedMemory(), 0);
    }
#endif

    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
    ASSERT_NE(nullptr, segment.sort());

    // check sorted column
    {
      std::vector<irs::bstring> column_payload;
      gen.reset();

      while (auto* doc = gen.next()) {
        auto* field = doc->stored.get(sorted_column);
        ASSERT_NE(nullptr, field);

        column_payload.emplace_back();
        irs::bytes_output out(column_payload.back());
        field->write(out);
      }

      ASSERT_EQ(column_payload.size(), segment.docs_count());

      std::stable_sort(column_payload.begin(), column_payload.end(),
                       [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                         return compare.Compare(lhs, rhs) < 0;
                       });

      auto& sorted_column = *segment.sort();
      ASSERT_EQ(segment.docs_count(), sorted_column.size());

      auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, sorted_column_it);

      auto* payload = irs::get<irs::payload>(*sorted_column_it);
      ASSERT_TRUE(payload);

      auto expected_doc = irs::doc_limits::min();
      for (auto& expected_payload : column_payload) {
        ASSERT_TRUE(sorted_column_it->next());
        ASSERT_EQ(expected_doc, sorted_column_it->value());
        ASSERT_EQ(expected_payload, payload->value);
        ++expected_doc;
      }
      ASSERT_FALSE(sorted_column_it->next());
    }

    // Check regular columns
    constexpr std::string_view column_names[]{"seq", "value", "duplicated",
                                              "prefix"};

    for (auto& column_name : column_names) {
      struct doc {
        irs::doc_id_t id{irs::doc_limits::eof()};
        irs::bstring order;
        irs::bstring value;
      };

      std::vector<doc> column_docs;
      column_docs.reserve(segment.docs_count());

      gen.reset();
      irs::doc_id_t id{irs::doc_limits::min()};
      while (auto* doc = gen.next()) {
        auto* sorted = doc->stored.get(sorted_column);
        ASSERT_NE(nullptr, sorted);

        column_docs.emplace_back();

        auto* column = doc->stored.get(column_name);

        auto& value = column_docs.back();
        irs::bytes_output order_out(value.order);
        sorted->write(order_out);

        if (column) {
          value.id = id++;
          irs::bytes_output value_out(value.value);
          column->write(value_out);
        }
      }

      std::stable_sort(column_docs.begin(), column_docs.end(),
                       [&](const doc& lhs, const doc& rhs) {
                         return compare.Compare(lhs.order, rhs.order) < 0;
                       });

      auto* column_meta = segment.column(column_name);
      ASSERT_NE(nullptr, column_meta);
      auto* column = segment.column(column_meta->id());
      ASSERT_NE(nullptr, column);

      ASSERT_EQ(id - 1, column->size());

      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, column_it);

      auto* payload = irs::get<irs::payload>(*column_it);
      ASSERT_TRUE(payload);

      irs::doc_id_t doc = 0;
      for (auto& expected_value : column_docs) {
        ++doc;

        if (irs::doc_limits::eof(expected_value.id)) {
          // skip empty values
          continue;
        }

        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(doc, column_it->value());
        EXPECT_EQ(expected_value.value, payload->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // Check pluggable features
    if (supports_pluggable_features()) {
      check_features(segment, "name", 32, false);
      check_features(segment, "same", 32, false);
      check_features(segment, "duplicated", 13, false);
      check_features(segment, "prefix", 10, false);
    }
  }
}

TEST_P(SortedIndexTestCase, reader_components) {
  StringComparer comparer;

  tests::json_doc_generator gen{
    resource("simple_sequential.json"),
    [](tests::document& doc, const std::string& name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name") {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.insert(field);
        doc.sorted = field;
      }
    }};

  const tests::document* doc1 = gen.next();
  const tests::document* doc2 = gen.next();

  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;

  auto query_doc1 = MakeByTerm("name", "A");
  auto writer = irs::IndexWriter::Make(dir(), codec(), irs::OM_CREATE, opts);
  ASSERT_TRUE(insert(*writer, *doc1, 1, true));
  ASSERT_TRUE(insert(*writer, *doc2, 1, true));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  auto check_reader = [](irs::DirectoryReader reader, irs::doc_id_t live_docs,
                         bool has_columnstore, bool has_index) {
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(2, segment.docs_count());
    ASSERT_EQ(live_docs, segment.live_docs_count());
    ASSERT_EQ(has_index, nullptr != segment.field("name"));
    ASSERT_EQ(has_columnstore, nullptr != segment.column("name"));
    ASSERT_EQ(has_columnstore, nullptr != segment.sort());
  };

  auto default_reader = irs::DirectoryReader(dir(), codec());
  auto no_cs_mask_reader = irs::DirectoryReader(
    dir(), codec(),
    irs::IndexReaderOptions{.columnstore = false, .doc_mask = false});
  auto no_index_reader = irs::DirectoryReader(
    dir(), codec(), irs::IndexReaderOptions{.index = false});
  auto empty_index_reader = irs::DirectoryReader(
    dir(), codec(),
    irs::IndexReaderOptions{
      .index = false, .columnstore = false, .doc_mask = false});

  check_reader(default_reader, 2, true, true);
  check_reader(no_cs_mask_reader, 2, false, true);
  check_reader(no_index_reader, 2, true, false);
  check_reader(empty_index_reader, 2, false, false);

  writer->GetBatch().Remove(*query_doc1);
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  default_reader = default_reader.Reopen();
  no_cs_mask_reader = no_cs_mask_reader.Reopen();
  no_index_reader = no_index_reader.Reopen();
  empty_index_reader = empty_index_reader.Reopen();

  check_reader(default_reader, 1, true, true);
  check_reader(no_cs_mask_reader, 2, false, true);
  check_reader(no_index_reader, 1, true, false);
  check_reader(empty_index_reader, 2, false, false);
}

TEST_P(SortedIndexTestCase, simple_sequential_consolidate) {
  constexpr std::string_view sorted_column = "name";

  // Build index
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [&sorted_column, this](tests::document& doc, const std::string& name,
                           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == sorted_column) {
          doc.sorted = field;
        }
      } else if (data.is_number()) {
        auto field = std::make_shared<tests::long_field>();
        field->name(name);
        field->value(data.i64);

        doc.insert(field);
      }
    });

  constexpr std::pair<size_t, size_t> segment_offsets[]{{0, 15}, {15, 17}};

  StringComparer compare;

  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&compare, writer->Comparator());

  // Add segment 0
  {
    auto& offset = segment_offsets[0];
    tests::limiting_doc_generator segment_gen(gen, offset.first, offset.second);
    add_segment(*writer, segment_gen);
  }

  // Add segment 1
  add_segment(*writer, gen);

  // Check index
  assert_index();

  // Check columns
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segments
    size_t i = 0;
    for (auto& segment : *reader) {
      auto& offset = segment_offsets[i++];
      tests::limiting_doc_generator segment_gen(gen, offset.first,
                                                offset.second);

      ASSERT_EQ(offset.second, segment.docs_count());
      ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
      ASSERT_NE(nullptr, segment.sort());

      // Check sorted column
      {
        segment_gen.reset();
        std::vector<irs::bstring> column_payload;

        while (auto* doc = segment_gen.next()) {
          auto* field = doc->stored.get(sorted_column);
          ASSERT_NE(nullptr, field);

          column_payload.emplace_back();
          irs::bytes_output out(column_payload.back());
          field->write(out);
        }

        ASSERT_EQ(column_payload.size(), segment.docs_count());

        std::stable_sort(column_payload.begin(), column_payload.end(),
                         [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                           return compare.Compare(lhs, rhs) < 0;
                         });

        auto& sorted_column = *segment.sort();
        ASSERT_EQ(segment.docs_count(), sorted_column.size());
        ASSERT_TRUE(irs::IsNull(sorted_column.name()));
        ASSERT_TRUE(sorted_column.payload().empty());

        auto sorted_column_it =
          sorted_column.iterator(irs::ColumnHint::kNormal);
        ASSERT_NE(nullptr, sorted_column_it);

        auto* payload = irs::get<irs::payload>(*sorted_column_it);
        ASSERT_TRUE(payload);

        auto expected_doc = irs::doc_limits::min();
        for (auto& expected_payload : column_payload) {
          ASSERT_TRUE(sorted_column_it->next());
          ASSERT_EQ(expected_doc, sorted_column_it->value());
          ASSERT_EQ(expected_payload, payload->value);
          ++expected_doc;
        }
        ASSERT_FALSE(sorted_column_it->next());
      }

      // Check stored columns
      constexpr std::string_view column_names[]{"seq", "value", "duplicated",
                                                "prefix"};

      for (auto& column_name : column_names) {
        struct doc {
          irs::doc_id_t id{irs::doc_limits::eof()};
          irs::bstring order;
          irs::bstring value;
        };

        std::vector<doc> column_docs;
        column_docs.reserve(segment.docs_count());

        segment_gen.reset();
        irs::doc_id_t id{irs::doc_limits::min()};
        while (auto* doc = segment_gen.next()) {
          auto* sorted = doc->stored.get(sorted_column);
          ASSERT_NE(nullptr, sorted);

          column_docs.emplace_back();

          auto* column = doc->stored.get(column_name);

          auto& value = column_docs.back();
          irs::bytes_output order_out(value.order);
          sorted->write(order_out);

          if (column) {
            value.id = id++;
            irs::bytes_output value_out(value.value);
            column->write(value_out);
          }
        }

        std::stable_sort(column_docs.begin(), column_docs.end(),
                         [&](const doc& lhs, const doc& rhs) {
                           return compare.Compare(lhs.order, rhs.order) < 0;
                         });

        auto* column_meta = segment.column(column_name);
        ASSERT_NE(nullptr, column_meta);
        auto* column = segment.column(column_meta->id());
        ASSERT_NE(nullptr, column);
        ASSERT_EQ(column_meta, column);
        ASSERT_TRUE(column->payload().empty());

        ASSERT_EQ(id - 1, column->size());

        auto column_it = column->iterator(irs::ColumnHint::kNormal);
        ASSERT_NE(nullptr, column_it);

        auto* payload = irs::get<irs::payload>(*column_it);
        ASSERT_TRUE(payload);

        irs::doc_id_t doc = 0;
        for (auto& expected_value : column_docs) {
          ++doc;

          if (irs::doc_limits::eof(expected_value.id)) {
            // skip empty values
            continue;
          }

          ASSERT_TRUE(column_it->next());
          ASSERT_EQ(doc, column_it->value());
          EXPECT_EQ(expected_value.value, payload->value);
        }
        ASSERT_FALSE(column_it->next());
      }

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", offset.second, false);
        check_features(segment, "same", offset.second, false);

        {
          constexpr std::string_view kColumnName = "duplicated";
          auto* column = segment.column(kColumnName);
          ASSERT_NE(nullptr, column);
          check_features(segment, kColumnName, column->size(), false);
        }

        {
          constexpr std::string_view kColumnName = "prefix";
          auto* column = segment.column(kColumnName);
          ASSERT_NE(nullptr, column);
          check_features(segment, kColumnName, column->size(), false);
        }
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);

    // simulate consolidation
    index().clear();
    index().emplace_back(writer->FeatureInfo());
    auto& segment = index().back();

    gen.reset();
    while (auto* doc = gen.next()) {
      segment.insert(*doc);
    }

    for (auto& column : segment.columns()) {
      column.rewrite();
    }

    ASSERT_NE(nullptr, writer->Comparator());
    segment.sort(*writer->Comparator());
  }

  assert_index();

  // Check columns in consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(segment_offsets[0].second + segment_offsets[1].second,
              segment.docs_count());
    ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
    ASSERT_NE(nullptr, segment.sort());

    // Check sorted column
    {
      gen.reset();
      std::vector<irs::bstring> column_payload;

      while (auto* doc = gen.next()) {
        auto* field = doc->stored.get(sorted_column);
        ASSERT_NE(nullptr, field);

        column_payload.emplace_back();
        irs::bytes_output out(column_payload.back());
        field->write(out);
      }

      ASSERT_EQ(column_payload.size(), segment.docs_count());

      std::stable_sort(column_payload.begin(), column_payload.end(),
                       [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                         return compare.Compare(lhs, rhs) < 0;
                       });

      auto& sorted_column = *segment.sort();
      ASSERT_EQ(segment.docs_count(), sorted_column.size());
      ASSERT_TRUE(sorted_column.payload().empty());
      ASSERT_TRUE(irs::IsNull(sorted_column.name()));

      auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, sorted_column_it);

      auto* payload = irs::get<irs::payload>(*sorted_column_it);
      ASSERT_TRUE(payload);

      auto expected_doc = irs::doc_limits::min();
      for (auto& expected_payload : column_payload) {
        ASSERT_TRUE(sorted_column_it->next());
        ASSERT_EQ(expected_doc, sorted_column_it->value());
        ASSERT_EQ(expected_payload, payload->value);
        ++expected_doc;
      }
      ASSERT_FALSE(sorted_column_it->next());
    }

    // Check stored columns
    constexpr std::string_view column_names[]{"seq", "value", "duplicated",
                                              "prefix"};

    for (auto& column_name : column_names) {
      struct doc {
        irs::doc_id_t id{irs::doc_limits::eof()};
        irs::bstring order;
        irs::bstring value;
      };

      std::vector<doc> column_docs;
      column_docs.reserve(segment.docs_count());

      gen.reset();
      irs::doc_id_t id{irs::doc_limits::min()};
      while (auto* doc = gen.next()) {
        auto* sorted = doc->stored.get(sorted_column);
        ASSERT_NE(nullptr, sorted);

        column_docs.emplace_back();

        auto* column = doc->stored.get(column_name);

        auto& value = column_docs.back();
        irs::bytes_output order_out(value.order);
        sorted->write(order_out);

        if (column) {
          value.id = id++;
          irs::bytes_output value_out(value.value);
          column->write(value_out);
        }
      }

      std::stable_sort(column_docs.begin(), column_docs.end(),
                       [&](const doc& lhs, const doc& rhs) {
                         return compare.Compare(lhs.order, rhs.order) < 0;
                       });

      auto* column_meta = segment.column(column_name);
      ASSERT_NE(nullptr, column_meta);
      auto* column = segment.column(column_meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column_meta, column);
      ASSERT_TRUE(column->payload().empty());

      ASSERT_EQ(id - 1, column->size());

      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, column_it);

      auto* payload = irs::get<irs::payload>(*column_it);
      ASSERT_TRUE(payload);

      irs::doc_id_t doc = 0;
      for (auto& expected_value : column_docs) {
        ++doc;

        if (irs::doc_limits::eof(expected_value.id)) {
          // skip empty values
          continue;
        }

        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(doc, column_it->value());
        EXPECT_EQ(expected_value.value, payload->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // Check pluggable features in consolidated segment
    if (supports_pluggable_features()) {
      check_features(segment, "name", 32, true);
      check_features(segment, "same", 32, true);
      check_features(segment, "duplicated", 13, true);
      check_features(segment, "prefix", 10, true);
    }
  }
}

TEST_P(SortedIndexTestCase, simple_sequential_already_sorted) {
  constexpr std::string_view sorted_column = "seq";

  // Build index
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [&sorted_column, this](tests::document& doc, const std::string& name,
                           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

      } else if (data.is_number()) {
        auto field = std::make_shared<tests::long_field>();
        field->name(name);
        field->value(data.i64);

        doc.insert(field);

        if (name == sorted_column) {
          doc.sorted = field;
        }
      }
    });

  LongComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();

  add_segment(gen, irs::OM_CREATE, opts);  // add segment

  assert_index();

  // Check columns
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    auto& segment = reader[0];
    ASSERT_EQ(segment.docs_count(), segment.live_docs_count());
    ASSERT_NE(nullptr, segment.sort());

    // Check sorted column
    {
      std::vector<irs::bstring> column_payload;
      gen.reset();

      while (auto* doc = gen.next()) {
        auto* field = doc->stored.get(sorted_column);
        ASSERT_NE(nullptr, field);

        column_payload.emplace_back();
        irs::bytes_output out(column_payload.back());
        field->write(out);
      }

      ASSERT_EQ(column_payload.size(), segment.docs_count());

      std::stable_sort(column_payload.begin(), column_payload.end(),
                       [&](const irs::bstring& lhs, const irs::bstring& rhs) {
                         return comparer.Compare(lhs, rhs) < 0;
                       });

      auto& sorted_column = *segment.sort();
      ASSERT_EQ(segment.docs_count(), sorted_column.size());
      ASSERT_TRUE(irs::IsNull(sorted_column.name()));
      ASSERT_TRUE(sorted_column.payload().empty());

      auto sorted_column_it = sorted_column.iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, sorted_column_it);

      auto* payload = irs::get<irs::payload>(*sorted_column_it);
      ASSERT_TRUE(payload);

      auto expected_doc = irs::doc_limits::min();
      for (auto& expected_payload : column_payload) {
        ASSERT_TRUE(sorted_column_it->next());
        ASSERT_EQ(expected_doc, sorted_column_it->value());
        ASSERT_EQ(expected_payload, payload->value);
        ++expected_doc;
      }
      ASSERT_FALSE(sorted_column_it->next());
    }

    // Check stored columns
    constexpr std::string_view column_names[]{"name", "value", "duplicated",
                                              "prefix"};

    for (auto& column_name : column_names) {
      struct doc {
        irs::doc_id_t id{irs::doc_limits::eof()};
        irs::bstring order;
        irs::bstring value;
      };

      std::vector<doc> column_docs;
      column_docs.reserve(segment.docs_count());

      gen.reset();
      irs::doc_id_t id{irs::doc_limits::min()};
      while (auto* doc = gen.next()) {
        auto* sorted = doc->stored.get(sorted_column);
        ASSERT_NE(nullptr, sorted);

        column_docs.emplace_back();

        auto* column = doc->stored.get(column_name);

        auto& value = column_docs.back();
        irs::bytes_output order_out(value.order);
        sorted->write(order_out);

        if (column) {
          value.id = id++;
          irs::bytes_output value_out(value.value);
          column->write(value_out);
        }
      }

      std::stable_sort(column_docs.begin(), column_docs.end(),
                       [&](const doc& lhs, const doc& rhs) {
                         return comparer.Compare(lhs.order, rhs.order) < 0;
                       });

      auto* column_meta = segment.column(column_name);
      ASSERT_NE(nullptr, column_meta);
      auto* column = segment.column(column_meta->id());
      ASSERT_NE(nullptr, column);
      ASSERT_EQ(column_meta, column);
      ASSERT_EQ(0, column->payload().size());

      ASSERT_EQ(id - 1, column->size());

      auto column_it = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, column_it);

      auto* payload = irs::get<irs::payload>(*column_it);
      ASSERT_TRUE(payload);

      irs::doc_id_t doc = 0;
      for (auto& expected_value : column_docs) {
        ++doc;

        if (irs::doc_limits::eof(expected_value.id)) {
          // skip empty values
          continue;
        }

        ASSERT_TRUE(column_it->next());
        ASSERT_EQ(doc, column_it->value());
        EXPECT_EQ(expected_value.value, payload->value);
      }
      ASSERT_FALSE(column_it->next());
    }

    // Check pluggable features
    if (supports_pluggable_features()) {
      check_features(segment, "name", 32, false);
      check_features(segment, "same", 32, false);
      check_features(segment, "duplicated", 13, false);
      check_features(segment, "prefix", 10, false);
    }
  }
}

TEST_P(SortedIndexTestCase, europarl) {
  SortedEuroparlDocTemplate doc("date", field_features());
  tests::delim_doc_generator gen(resource("europarl.subset.txt"), doc);

  LongComparer comparer;

  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();

  add_segment(gen, irs::OM_CREATE, opts);

  assert_index();
}

TEST_P(SortedIndexTestCase, multi_valued_sorting_field) {
  struct {
    bool write(irs::data_output& out) {
      out.write_bytes(reinterpret_cast<const irs::byte_type*>(value.data()),
                      value.size());
      return true;
    }

    std::string_view value;
  } field;

  tests::string_view_field same("same");
  same.value("A");

  // Open writer
  StringComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&comparer, writer->Comparator());

  // Write documents
  {
    auto docs = writer->GetBatch();

    {
      auto doc = docs.Insert();

      // Compound sorted field
      field.value = "A";
      doc.Insert<irs::Action::STORE_SORTED>(field);
      field.value = "B";
      doc.Insert<irs::Action::STORE_SORTED>(field);

      // Indexed field
      doc.Insert<irs::Action::INDEX>(same);
    }

    {
      auto doc = docs.Insert();

      // Compound sorted field
      field.value = "C";
      doc.Insert<irs::Action::STORE_SORTED>(field);
      field.value = "D";
      doc.Insert<irs::Action::STORE_SORTED>(field);

      // Indexed field
      doc.Insert<irs::Action::INDEX>(same);
    }
  }

  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);

      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("CD", irs::ViewCast<char>(actual_value->value));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("AB", irs::ViewCast<char>(actual_value->value));
      ASSERT_FALSE(docsItr->next());
    }
  }
}

TEST_P(SortedIndexTestCase, check_document_order_after_consolidation_dense) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [this](tests::document& doc, const std::string& name,
           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  StringComparer comparer;

  // open writer
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&comparer, writer->Comparator());

  // Segment 0
  ASSERT_TRUE(insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end(), doc2->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Segment 1
  ASSERT_TRUE(insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end(), doc3->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("C",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 2, false);
        check_features(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("D",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 1, false);
        check_features(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("D",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("C",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features in consolidated segment
      if (supports_pluggable_features()) {
        check_features(segment, "name", 4, true);
        check_features(segment, "same", 4, true);
        check_features(segment, "duplicated", 3, true);
        check_features(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), doc2->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), doc3->sorted.get());
  segment.sort(*writer->Comparator());
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  assert_index();
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_dense_with_removals) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [this](tests::document& doc, const std::string& name,
           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  tests::string_field empty_field{"", irs::IndexFeatures::NONE};
  ASSERT_FALSE(irs::IsNull(empty_field.value()));
  ASSERT_TRUE(empty_field.value().empty());

  StringComparer comparer;

  // open writer
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();
  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_EQ(&comparer, writer->Comparator());

  // segment 0
  ASSERT_TRUE(insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end(), doc2->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // segment 1
  ASSERT_TRUE(insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end(), doc3->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_TRUE(column->payload().empty());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("C",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 2, false);
        check_features(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("D",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 1, false);
        check_features(segment, "prefix", 1, false);
      }
    }
  }

  // Remove document
  {
    auto query_doc1 = MakeByTerm("name", "C");
    writer->GetBatch().Remove(*query_doc1);
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = segment.mask(termItr->postings(irs::IndexFeatures::NONE));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 2, false);
        check_features(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = segment.mask(termItr->postings(irs::IndexFeatures::NONE));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("D",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 1, false);
        check_features(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("D",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features in consolidated segment
      if (supports_pluggable_features()) {
        check_features(segment, "name", 3, true);
        check_features(segment, "same", 3, true);
        check_features(segment, "duplicated", 2, true);
        check_features(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), doc3->sorted.get());
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });

  const tests::document* doc1 = gen.next();
  const tests::document* doc2 = gen.next();
  const tests::document* doc3 = gen.next();

  auto query_doc1 = MakeByTerm("name", "A");
  auto query_doc2 = MakeByTerm("name", "B");

  {
    StringComparer comparer;

    // open writer
    irs::IndexWriterOptions opts;
    opts.comparator = &comparer;
    opts.features = features();
    auto writer = open_writer(irs::OM_CREATE, opts);
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(&comparer, writer->Comparator());

    ASSERT_TRUE(insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
    writer->GetBatch().Remove(*(query_doc1));
    ASSERT_TRUE(insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end(), doc2->sorted));
    writer->GetBatch().Remove(*(query_doc2));
    ASSERT_TRUE(insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                       doc3->stored.begin(), doc3->stored.end(), doc3->sorted));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(3, reader->docs_count());
    ASSERT_EQ(1, reader->live_docs_count());

    // Check segment 0
    auto& segment = reader[0];
    const auto* column = segment.sort();
    ASSERT_NE(nullptr, column);
    ASSERT_TRUE(irs::IsNull(column->name()));
    ASSERT_EQ(0, column->payload().size());
    auto values = column->iterator(irs::ColumnHint::kNormal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::payload>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("name");
    ASSERT_NE(nullptr, terms);
    auto docs = segment.docs_iterator();
    ASSERT_TRUE(docs->next());
    ASSERT_EQ(docs->value(), values->seek(docs->value()));
    ASSERT_EQ("C",
              irs::to_string<std::string_view>(actual_value->value.data()));
    ASSERT_FALSE(docs->next());
  }
}

bool Insert(irs::IndexWriter::Transaction& ctx, const tests::document* d) {
  auto doc = ctx.Insert();
  if (d->sorted && !doc.Insert<irs::Action::STORE_SORTED>(*d->sorted)) {
    return false;
  }
  return doc.Insert<irs::Action::INDEX>(d->indexed.begin(), d->indexed.end()) &&
         doc.Insert<irs::Action::STORE>(d->stored.begin(), d->stored.end());
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx_flush) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });

  const tests::document* doc1 = gen.next();
  const tests::document* doc2 = gen.next();
  const tests::document* doc3 = gen.next();
  const tests::document* doc4 = gen.next();
  const tests::document* doc5 = gen.next();
  const tests::document* doc6 = gen.next();
  const tests::document* doc7 = gen.next();

  auto query_doc1 = MakeByTerm("name", "A");
  auto query_doc2 = MakeByTerm("name", "B");

  {
    StringComparer comparer;

    // open writer
    irs::IndexWriterOptions opts;
    opts.comparator = &comparer;
    opts.features = features();
    auto writer = open_writer(irs::OM_CREATE, opts);
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(&comparer, writer->Comparator());
    writer->Options({.segment_docs_max = 6});
    {
      auto batch = writer->GetBatch();
      ASSERT_TRUE(Insert(batch, doc1));
      batch.Remove(*(query_doc1));
      ASSERT_TRUE(Insert(batch, doc2));
      batch.Remove(*(query_doc2));
      ASSERT_TRUE(Insert(batch, doc3));
      ASSERT_TRUE(batch.Commit());
    }
    {
      auto batch = writer->GetBatch();
      ASSERT_TRUE(Insert(batch, doc4));
      ASSERT_TRUE(Insert(batch, doc5));
      ASSERT_TRUE(Insert(batch, doc6));
      ASSERT_TRUE(Insert(batch, doc7));  // Flush triggered here, before insert
      ASSERT_TRUE(batch.Commit());
    }
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    EXPECT_EQ(2, reader.size());
    EXPECT_EQ(7, reader->docs_count());
    EXPECT_EQ(5, reader->live_docs_count());

    // Check segment 0
    auto& segment = reader[0];
    const auto* column = segment.sort();
    ASSERT_NE(nullptr, column);
    ASSERT_TRUE(irs::IsNull(column->name()));
    ASSERT_EQ(0, column->payload().size());
    auto values = column->iterator(irs::ColumnHint::kNormal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::payload>(*values);
    ASSERT_NE(nullptr, actual_value);
    auto terms = segment.field("name");
    ASSERT_NE(nullptr, terms);
    auto docs = segment.docs_iterator();
    for (const char expected_char : std::string_view{"FEDC", 4}) {
      ASSERT_TRUE(docs->next());
      ASSERT_EQ(docs->value(), values->seek(docs->value()));
      std::string_view expected_str{&expected_char, 1};
      EXPECT_EQ(expected_str,
                irs::to_string<std::string_view>(actual_value->value.data()));
    }
    ASSERT_FALSE(docs->next());
  }
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_sparse_already_sorted) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [this](tests::document& doc, const std::string& name,
           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'

  StringComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(1, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 2, false);
        check_features(segment, "prefix", 1, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(1, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 2, false);
        check_features(segment, "same", 2, false);
        check_features(segment, "duplicated", 1, false);
        check_features(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(4, segment.docs_count());
      ASSERT_EQ(4, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_EQ(2, column->size());
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto termItr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(termItr->next());
      auto docsItr = termItr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value() + 1, values->seek(docsItr->value()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_EQ(docsItr->value(), values->seek(docsItr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docsItr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docsItr->next());

      // Check pluggable features in consolidated segment
      if (supports_pluggable_features()) {
        check_features(segment, "name", 4, true);
        check_features(segment, "same", 4, true);
        check_features(segment, "duplicated", 3, true);
        check_features(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), &kEmpty);
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), &kEmpty);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase, check_document_order_after_consolidation_sparse) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [this](tests::document& doc, const std::string& name,
           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'
  auto* doc4 = gen.next();  // name == 'E'
  auto* doc5 = gen.next();  // name == 'F'
  auto* doc6 = gen.next();  // name == 'G'

  StringComparer comparer;
  irs::IndexWriterOptions opts;
  opts.comparator = &comparer;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end(), doc4->sorted));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  ASSERT_TRUE(insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                     doc5->stored.begin(), doc5->stored.end(), doc5->sorted));
  ASSERT_TRUE(insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                     doc6->stored.begin(), doc6->stored.end()));
  writer->Commit();
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0: E - <empty> - A
    {
      auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 3, false);
        check_features(segment, "same", 3, false);
        check_features(segment, "duplicated", 3, false);
        check_features(segment, "prefix", 1, false);
      }
    }

    // Check segment 1:
    // <empty> - F - B - <empty>
    {
      auto& segment = reader[1];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 4, false);
        check_features(segment, "same", 4, false);
        check_features(segment, "duplicated", 1, false);
        check_features(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    writer->Commit();
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment:
  // <empty> - F - E - B - <empty> - A - <empty>
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(7, segment.docs_count());
      ASSERT_EQ(7, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_EQ(4, column->size());
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (supports_pluggable_features()) {
        check_features(segment, "name", 7, true);
        check_features(segment, "same", 7, true);
        check_features(segment, "duplicated", 4, true);
        check_features(segment, "prefix", 2, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), &kEmpty);
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc4->indexed.begin(), doc4->indexed.end(),
                 doc4->stored.begin(), doc4->stored.end(), doc4->sorted.get());
  segment.insert(doc1->indexed.begin(), doc1->indexed.end(),
                 doc1->stored.begin(), doc1->stored.end(), doc1->sorted.get());
  segment.insert(doc3->indexed.begin(), doc3->indexed.end(),
                 doc3->stored.begin(), doc3->stored.end(), &kEmpty);
  segment.insert(doc5->indexed.begin(), doc5->indexed.end(),
                 doc5->stored.begin(), doc5->stored.end(), doc5->sorted.get());
  segment.insert(doc6->indexed.begin(), doc6->indexed.end(),
                 doc6->stored.begin(), doc6->stored.end(), &kEmpty);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_sparse_with_removals) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [this](tests::document& doc, const std::string& name,
           const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == "name") {
          doc.sorted = field;
        }
      }
    });

  auto* doc0 = gen.next();  // name == 'A'
  auto* doc1 = gen.next();  // name == 'B'
  auto* doc2 = gen.next();  // name == 'C'
  auto* doc3 = gen.next();  // name == 'D'
  auto* doc4 = gen.next();  // name == 'E'
  auto* doc5 = gen.next();  // name == 'F'
  auto* doc6 = gen.next();  // name == 'G'

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                     doc2->stored.begin(), doc2->stored.end()));
  ASSERT_TRUE(insert(*writer, doc0->indexed.begin(), doc0->indexed.end(),
                     doc0->stored.begin(), doc0->stored.end(), doc0->sorted));
  ASSERT_TRUE(insert(*writer, doc4->indexed.begin(), doc4->indexed.end(),
                     doc4->stored.begin(), doc4->stored.end(), doc4->sorted));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                     doc1->stored.begin(), doc1->stored.end(), doc1->sorted));
  ASSERT_TRUE(insert(*writer, doc3->indexed.begin(), doc3->indexed.end(),
                     doc3->stored.begin(), doc3->stored.end()));
  ASSERT_TRUE(insert(*writer, doc5->indexed.begin(), doc5->indexed.end(),
                     doc5->stored.begin(), doc5->stored.end(), doc5->sorted));
  ASSERT_TRUE(insert(*writer, doc6->indexed.begin(), doc6->indexed.end(),
                     doc6->stored.begin(), doc6->stored.end()));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Remove docs from segment 1
  writer->GetBatch().Remove(irs::filter::ptr{MakeByTerm("name", "B")});  // doc1
  writer->GetBatch().Remove(irs::filter::ptr{MakeByTerm("name", "D")});  // doc3
  // Remove docs from segment 0
  writer->GetBatch().Remove(irs::filter::ptr{MakeByTerm("name", "E")});  // doc4
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Read documents
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0: E - <empty> - A
    {
      auto& segment = reader[0];
      ASSERT_EQ(3, segment.docs_count());
      ASSERT_EQ(2, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("E",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 3, false);
        check_features(segment, "same", 3, false);
        check_features(segment, "duplicated", 3, false);
        check_features(segment, "prefix", 1, false);
      }
    }

    // Check segment 1:
    // <empty> - F - B - <empty>
    {
      auto& segment = reader[1];
      ASSERT_EQ(4, segment.docs_count());
      ASSERT_EQ(2, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(2, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("B",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 4, false);
        check_features(segment, "same", 4, false);
        check_features(segment, "duplicated", 1, false);
        check_features(segment, "prefix", 1, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment:
  // F - <empty> - A - <empty>
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(4, segment.docs_count());
      ASSERT_EQ(4, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_EQ(2, column->size());
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      auto terms = segment.field("same");
      ASSERT_NE(nullptr, terms);
      auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
      ASSERT_TRUE(term_itr->next());
      auto docs_itr = term_itr->postings(irs::IndexFeatures::NONE);
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("F",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value() + 1, values->seek(docs_itr->value()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ("A",
                irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(docs_itr->next());
      ASSERT_FALSE(values->next());
      ASSERT_FALSE(docs_itr->next());

      // Check pluggable features in consolidated segment
      if (supports_pluggable_features()) {
        check_features(segment, "name", 4, true);
        check_features(segment, "same", 4, true);
        check_features(segment, "duplicated", 2, true);
        check_features(segment, "prefix", 1, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(doc2->indexed.begin(), doc2->indexed.end(),
                 doc2->stored.begin(), doc2->stored.end(), &kEmpty);
  segment.insert(doc0->indexed.begin(), doc0->indexed.end(),
                 doc0->stored.begin(), doc0->stored.end(), doc0->sorted.get());
  segment.insert(doc5->indexed.begin(), doc5->indexed.end(),
                 doc5->stored.begin(), doc5->stored.end(), doc5->sorted.get());
  segment.insert(doc6->indexed.begin(), doc6->indexed.end(),
                 doc6->stored.begin(), doc6->stored.end(), &kEmpty);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_consolidation_sparse_with_gaps) {
  constexpr std::string_view kName = "name";

  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [&](tests::document& doc, const std::string& name,
        const tests::json_doc_generator::json_value& data) {
      if (data.is_string()) {
        auto field = std::make_shared<tests::string_field>(
          name, data.str, irs::IndexFeatures::ALL, field_features());

        doc.insert(field);

        if (name == kName) {
          doc.sorted = field;
        }
      }
    });

  using DocAndFilter =
    std::pair<const tests::document*, std::unique_ptr<irs::by_term>>;
  constexpr size_t kCount = 14;
  std::array<DocAndFilter, kCount> docs;
  for (auto& [doc, filter] : docs) {
    doc = gen.next();
    ASSERT_NE(nullptr, doc);
    auto* field = dynamic_cast<tests::string_field*>(doc->indexed.get(kName));
    ASSERT_NE(nullptr, field);
    filter = MakeByTerm(kName, field->value());
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = features();

  auto writer = open_writer(irs::OM_CREATE, opts);
  ASSERT_NE(nullptr, writer);
  ASSERT_NE(nullptr, writer->Comparator());

  // Create segment 0
  ASSERT_TRUE(insert(*writer, *docs[0].first, 5, false));
  ASSERT_TRUE(insert(*writer, *docs[1].first, 1, true));
  ASSERT_TRUE(insert(*writer, *docs[2].first, 3, false));
  ASSERT_TRUE(insert(*writer, *docs[3].first, 1, true));
  ASSERT_TRUE(insert(*writer, *docs[12].first, 2, false));
  ASSERT_TRUE(insert(*writer, *docs[13].first, 1, true));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Create segment 1
  ASSERT_TRUE(insert(*writer, *docs[6].first, 1, false));
  ASSERT_TRUE(insert(*writer, *docs[7].first, 1, true));
  ASSERT_TRUE(insert(*writer, *docs[9].first, 1, true));
  ASSERT_TRUE(insert(*writer, *docs[4].first, 7, false));
  ASSERT_TRUE(insert(*writer, *docs[5].first, 1, true));
  ASSERT_TRUE(insert(*writer, *docs[10].first, 8, false));
  ASSERT_TRUE(insert(*writer, *docs[11].first, 1, true));
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  // Remove docs
  writer->GetBatch().Remove(*docs[2].second);
  writer->GetBatch().Remove(*docs[3].second);

  writer->GetBatch().Remove(*docs[4].second);
  writer->GetBatch().Remove(*docs[5].second);

  writer->GetBatch().Remove(*docs[9].second);
  ASSERT_TRUE(writer->Commit());
  AssertSnapshotEquality(*writer);

  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(2, reader.size());

    // Check segment 0
    {
      auto& segment = reader[0];
      ASSERT_EQ(13, segment.docs_count());
      ASSERT_EQ(9, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(3, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_TRUE(values->next());
      ASSERT_EQ(3, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[13].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(7, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[3].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(13, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[1].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(values->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 13, false);
        check_features(segment, "same", 13, false);
        check_features(segment, "duplicated", 10, false);
        check_features(segment, "prefix", 6, false);
      }
    }

    // Check segment 1
    {
      auto& segment = reader[1];
      ASSERT_EQ(20, segment.docs_count());
      ASSERT_EQ(11, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(4, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_TRUE(values->next());
      ASSERT_EQ(9, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[11].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(10, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[9].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(12, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[7].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(20, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[5].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(values->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 20, false);
        check_features(segment, "same", 20, false);
        check_features(segment, "duplicated", 16, false);
      }
    }
  }

  // Consolidate segments
  {
    irs::index_utils::ConsolidateCount consolidate_all;
    ASSERT_TRUE(
      writer->Consolidate(irs::index_utils::MakePolicy(consolidate_all)));
    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // Check consolidated segment
  {
    auto reader = irs::DirectoryReader(dir(), codec());
    ASSERT_TRUE(reader);
    ASSERT_EQ(1, reader.size());
    ASSERT_EQ(reader->live_docs_count(), reader->docs_count());

    {
      auto& segment = reader[0];
      ASSERT_EQ(20, segment.docs_count());
      ASSERT_EQ(20, segment.live_docs_count());
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      ASSERT_EQ(4, column->size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      ASSERT_TRUE(values->next());
      ASSERT_EQ(3, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[13].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(12, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[11].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(14, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[7].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_TRUE(values->next());
      ASSERT_EQ(20, values->value());
      ASSERT_EQ(
        irs::ViewCast<char>(irs::bytes_view{docs[1].second->options().term}),
        irs::to_string<std::string_view>(actual_value->value.data()));
      ASSERT_FALSE(values->next());

      // Check pluggable features
      if (supports_pluggable_features()) {
        check_features(segment, "name", 20, true);
        check_features(segment, "same", 20, true);
        check_features(segment, "duplicated", 16, true);
        check_features(segment, "prefix", 5, true);
      }
    }
  }

  // Create expected index
  auto& expected_index = index();
  auto& segment = expected_index.emplace_back(writer->FeatureInfo());
  segment.insert(*docs[0].first, 5, false);
  segment.insert(*docs[1].first, 1, true);
  segment.insert(*docs[12].first, 2, false);
  segment.insert(*docs[13].first, 1, true);
  segment.insert(*docs[6].first, 1, false);
  segment.insert(*docs[7].first, 1, true);
  segment.insert(*docs[10].first, 8, false);
  segment.insert(*docs[11].first, 1, true);
  for (auto& column : segment.columns()) {
    column.rewrite();
  }
  segment.sort(*writer->Comparator());
  assert_index();
}

// Separate definition as MSVC parser fails to do conditional defines in macro
// expansion
#ifdef IRESEARCH_SSE2
const auto kSortedIndexTestCaseValues = ::testing::Values(
  tests::format_info{"1_1", "1_0"}, tests::format_info{"1_2", "1_0"},
  tests::format_info{"1_3", "1_0"}, tests::format_info{"1_4", "1_0"},
  tests::format_info{"1_5", "1_0"}, tests::format_info{"1_3simd", "1_0"},
  tests::format_info{"1_4simd", "1_0"}, tests::format_info{"1_5simd", "1_0"});
#else
const auto kSortedIndexTestCaseValues = ::testing::Values(
  tests::format_info{"1_1", "1_0"}, tests::format_info{"1_2", "1_0"},
  tests::format_info{"1_3", "1_0"}, tests::format_info{"1_4", "1_0"},
  tests::format_info{"1_5", "1_0"});
#endif

static constexpr auto kTestDirs = tests::getDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(SortedIndexTest, SortedIndexTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs),
                                            kSortedIndexTestCaseValues),
                         SortedIndexTestCase::to_string);

struct SortedIndexStressTestCase : SortedIndexTestCase {};

TEST_P(SortedIndexStressTestCase, doc_removal_same_key_within_trx) {
#if !GTEST_OS_LINUX
  GTEST_SKIP();  // too long for our CI
#endif
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });

  static constexpr size_t kLen = 5;
  std::array<std::pair<size_t, const tests::document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::by_term>>, kLen>
    remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::string_field&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }
  std::array<bool, kLen> in_store;
  std::array<char, kLen> results{'A', 'B', 'C', 'D', 'E'};
  for (size_t reset = 0; reset < 32; ++reset) {
    std::sort(insert_docs.begin(), insert_docs.end(),
              [](auto& a, auto& b) { return a.first < b.first; });
    do {
      std::sort(remove_docs.begin(), remove_docs.end(),
                [](auto& a, auto& b) { return a.first < b.first; });
      do {
        in_store.fill(false);
        // open writer
        StringComparer compare;
        irs::IndexWriterOptions opts;
        opts.comparator = &compare;
        opts.features = features();
        auto writer = open_writer(irs::OM_CREATE, opts);
        ASSERT_NE(nullptr, writer);
        ASSERT_EQ(&compare, writer->Comparator());
        for (size_t i = 0; i < kLen; ++i) {
          {
            auto ctx = writer->GetBatch();
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::STORE_SORTED>(
              *insert_docs[i].second->sorted));
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(
              insert_docs[i].second->indexed.begin(),
              insert_docs[i].second->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(
              insert_docs[i].second->stored.begin(),
              insert_docs[i].second->stored.end()));
            if (((reset >> i) & 1U) == 1U) {
              ctx.Reset();
            } else {
              in_store[insert_docs[i].first] = true;
            }
          }
          writer->GetBatch().Remove(*(remove_docs[i].second));
          in_store[remove_docs[i].first] = false;
        }
        writer->Commit();
        AssertSnapshotEquality(*writer);
        writer = nullptr;
        // Check consolidated segment
        auto reader = irs::DirectoryReader(dir(), codec());
        ASSERT_TRUE(reader);
        size_t in_store_count = 0;
        for (auto v : in_store) {
          in_store_count += static_cast<size_t>(v);
        }
        if (in_store_count == 0) {
          ASSERT_EQ(0, reader.size());
          ASSERT_EQ(0, reader->docs_count());
          ASSERT_EQ(0, reader->live_docs_count());
          continue;
        }
        ASSERT_EQ(1, reader.size());
        // less possible when Reset/Rollback is first
        ASSERT_LE(reader->docs_count(), kLen);
        ASSERT_EQ(in_store_count, reader->live_docs_count());
        const auto& segment = reader[0];
        const auto* column = segment.sort();
        ASSERT_NE(nullptr, column);
        ASSERT_TRUE(irs::IsNull(column->name()));
        ASSERT_EQ(0, column->payload().size());
        auto values = column->iterator(irs::ColumnHint::kNormal);
        ASSERT_NE(nullptr, values);
        const auto* actual_value = irs::get<irs::payload>(*values);
        ASSERT_NE(nullptr, actual_value);
        const auto* terms = segment.field("name");
        ASSERT_NE(nullptr, terms);
        auto docs = segment.docs_iterator();
        for (size_t i = kLen; i > 0; --i) {
          if (!in_store[i - 1]) {
            continue;
          }
          ASSERT_TRUE(docs->next());
          ASSERT_EQ(docs->value(), values->seek(docs->value()));
          ASSERT_EQ(results[i - 1], irs::to_string<std::string_view>(
                                      actual_value->value.data())[0]);
        }
        ASSERT_FALSE(docs->next());
      } while (std::next_permutation(remove_docs.begin(), remove_docs.end()));
    } while (std::next_permutation(insert_docs.begin(), insert_docs.end()));
  }
}

TEST_P(SortedIndexStressTestCase, commit_on_tick) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });

  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<bool, kLen> in_store;
  std::array<char, kLen> results{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'};
  for (size_t commit = 0; commit < (1 << kLen); ++commit) {
    for (size_t reset = 0; reset < (1 << kLen); ++reset) {
      in_store.fill(false);
      uint64_t insert_time = irs::writer_limits::kMinTick + 1;
      uint64_t commit_time = insert_time;
      // open writer
      StringComparer compare;
      irs::IndexWriterOptions opts;
      opts.segment_docs_max = 2;
      opts.comparator = &compare;
      opts.features = features();
      auto writer = open_writer(irs::OM_CREATE, opts);
      ASSERT_NE(nullptr, writer);
      ASSERT_EQ(&compare, writer->Comparator());
      {
        auto ctx = writer->GetBatch();
        for (size_t i = 0; i < kLen; ++i) {
          {
            auto doc = ctx.Insert();
            ASSERT_TRUE(doc.Insert<irs::Action::STORE_SORTED>(
              *insert_docs[i].second->sorted));
            ASSERT_TRUE(doc.Insert<irs::Action::INDEX>(
              insert_docs[i].second->indexed.begin(),
              insert_docs[i].second->indexed.end()));
            ASSERT_TRUE(doc.Insert<irs::Action::STORE>(
              insert_docs[i].second->stored.begin(),
              insert_docs[i].second->stored.end()));
          }
          if (((reset >> i) & 1U) == 1U) {
            ctx.Abort();
          } else {
            ctx.Commit(++insert_time);
            in_store[insert_docs[i].first] = true;
          }
          if (((commit >> i) & 1U) == 1U) {
            writer->Commit({.tick = commit_time});
            commit_time = insert_time;
            AssertSnapshotEquality(*writer);
          }
        }
      }
      size_t in_store_count = 0;
      for (auto v : in_store) {
        in_store_count += static_cast<size_t>(v);
      }
      writer->Commit({.tick = insert_time});
      AssertSnapshotEquality(*writer);

      auto reader = writer->GetSnapshot();
      ASSERT_TRUE(reader);
      EXPECT_EQ(in_store_count, reader->live_docs_count());
      EXPECT_LE(in_store_count, reader->docs_count());
      EXPECT_LE(reader->docs_count(), kLen);

      writer->Consolidate(MakePolicy(irs::index_utils::ConsolidateCount{}));
      writer->Commit({.tick = insert_time});
      AssertSnapshotEquality(*writer);

      writer = nullptr;

      // Check consolidated segment
      reader = irs::DirectoryReader(dir(), codec());
      ASSERT_TRUE(reader);
      if (in_store_count == 0) {
        ASSERT_EQ(0, reader.size());
        ASSERT_EQ(0, reader->docs_count());
        ASSERT_EQ(0, reader->live_docs_count());
        continue;
      }
      ASSERT_EQ(1, reader.size());
      EXPECT_EQ(in_store_count, reader->docs_count());
      EXPECT_EQ(in_store_count, reader->live_docs_count());
      const auto& segment = reader[0];
      const auto* column = segment.sort();
      ASSERT_NE(nullptr, column);
      ASSERT_TRUE(irs::IsNull(column->name()));
      ASSERT_EQ(0, column->payload().size());
      auto values = column->iterator(irs::ColumnHint::kNormal);
      ASSERT_NE(nullptr, values);
      const auto* actual_value = irs::get<irs::payload>(*values);
      ASSERT_NE(nullptr, actual_value);
      const auto* terms = segment.field("name");
      ASSERT_NE(nullptr, terms);
      auto docs = segment.docs_iterator();
      for (size_t i = kLen; i > 0; --i) {
        if (!in_store[i - 1]) {
          continue;
        }
        ASSERT_TRUE(docs->next());
        ASSERT_EQ(docs->value(), values->seek(docs->value()));
        EXPECT_EQ(results[i - 1], irs::to_string<std::string_view>(
                                    actual_value->value.data())[0]);
      }
      ASSERT_FALSE(docs->next());
    }
  }
}

TEST_P(SortedIndexStressTestCase, split_empty_commit) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });
  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::by_term>>, kLen>
    remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::string_field&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = features();
  auto writer = open_writer(irs::OM_CREATE, opts);
  auto segment1 = writer->GetBatch();
  auto insert_doc = [&](size_t i) {
    auto doc = segment1.Insert();
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE_SORTED>(*insert_docs[i].second->sorted));
    ASSERT_TRUE(
      doc.Insert<irs::Action::INDEX>(insert_docs[i].second->indexed.begin(),
                                     insert_docs[i].second->indexed.end()));
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE>(insert_docs[i].second->stored.begin(),
                                     insert_docs[i].second->stored.end()));
  };
  auto remove_doc = [&](size_t i) { segment1.Remove(*remove_docs[i].second); };
  insert_doc(0);
  insert_doc(1);
  insert_doc(2);
  remove_doc(0);
  segment1.Commit(10);
  insert_doc(3);
  remove_doc(1);
  remove_doc(2);
  remove_doc(3);
  segment1.Commit(20);
  writer->Commit({.tick = 10});
  auto reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 4);
  EXPECT_EQ(reader->live_docs_count(), 2);
  EXPECT_EQ(reader->size(), 1);
  writer->Commit({.tick = 20});
  reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 0);
  EXPECT_EQ(reader->live_docs_count(), 0);
  EXPECT_EQ(reader->size(), 0);
}

TEST_P(SortedIndexStressTestCase, remove_tick) {
  tests::json_doc_generator gen(
    resource("simple_sequential.json"),
    [](tests::document& doc, std::string_view name,
       const tests::json_doc_generator::json_value& data) {
      if (name == "name" && data.is_string()) {
        auto field = std::make_shared<tests::string_field>(name, data.str);
        doc.sorted = field;
        doc.insert(field);
      }
    });
  static constexpr size_t kLen = 8;
  std::array<std::pair<size_t, const tests::document*>, kLen> insert_docs;
  for (size_t i = 0; i < kLen; ++i) {
    insert_docs[i] = {i, gen.next()};
  }
  std::array<std::pair<size_t, std::unique_ptr<irs::by_term>>, kLen>
    remove_docs;
  for (size_t i = 0; i < kLen; ++i) {
    remove_docs[i] = {i, MakeByTerm("name", static_cast<tests::string_field&>(
                                              *insert_docs[i].second->sorted)
                                              .value())};
  }

  StringComparer compare;
  irs::IndexWriterOptions opts;
  opts.comparator = &compare;
  opts.features = features();
  auto writer = open_writer(irs::OM_CREATE, opts);
  auto segment1 = writer->GetBatch();
  auto insert_doc = [&](size_t i) {
    auto doc = segment1.Insert();
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE_SORTED>(*insert_docs[i].second->sorted));
    ASSERT_TRUE(
      doc.Insert<irs::Action::INDEX>(insert_docs[i].second->indexed.begin(),
                                     insert_docs[i].second->indexed.end()));
    ASSERT_TRUE(
      doc.Insert<irs::Action::STORE>(insert_docs[i].second->stored.begin(),
                                     insert_docs[i].second->stored.end()));
  };
  auto remove_doc = [&](size_t i) { segment1.Remove(*remove_docs[i].second); };
  insert_doc(0);
  insert_doc(1);
  insert_doc(2);
  remove_doc(0);
  segment1.Commit(10);
  insert_doc(3);
  insert_doc(4);
  remove_doc(4);
  segment1.Commit(20);
  writer->Commit({.tick = 10});
  auto reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 5);
  EXPECT_EQ(reader->live_docs_count(), 2);
  EXPECT_EQ(reader->size(), 1);
  writer->Commit({.tick = 20});
  reader = writer->GetSnapshot();
  EXPECT_EQ(reader->docs_count(), 5);
  EXPECT_EQ(reader->live_docs_count(), 3);
  EXPECT_EQ(reader->size(), 1);
}

INSTANTIATE_TEST_SUITE_P(
  SortedIndexStressTest, SortedIndexStressTestCase,
  ::testing::Combine(
    ::testing::Values(&tests::directory<&tests::memory_directory>),
    ::testing::Values(tests::format_info{"1_5", "1_0"})),
  SortedIndexStressTestCase::to_string);

}  // namespace
