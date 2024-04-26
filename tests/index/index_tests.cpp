////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "index_tests.hpp"

#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "index/field_meta.hpp"
#include "index/norm.hpp"
#include "search/boolean_filter.hpp"
#include "search/term_filter.hpp"
#include "store/fs_directory.hpp"
#include "store/memory_directory.hpp"
#include "store/mmap_directory.hpp"
#include "tests_shared.hpp"
#include "utils/delta_compression.hpp"
#include "utils/file_utils.hpp"
#include "utils/fstext/fst_table_matcher.hpp"
#include "utils/index_utils.hpp"
#include "utils/lz4compression.hpp"
#include "utils/wildcard_utils.hpp"

using namespace std::literals;

namespace {

bool visit(const irs::column_reader& reader,
           const std::function<bool(irs::doc_id_t, irs::bytes_view)>& visitor) {
  auto it = reader.iterator(irs::ColumnHint::kConsolidation);

  irs::payload dummy;
  auto* doc = irs::get<irs::document>(*it);
  if (!doc) {
    return false;
  }
  auto* payload = irs::get<irs::payload>(*it);
  if (!payload) {
    payload = &dummy;
  }

  while (it->next()) {
    if (!visitor(doc->value, payload->value)) {
      return false;
    }
  }

  return true;
}

irs::filter::ptr MakeByTerm(std::string_view name, std::string_view value) {
  auto filter = std::make_unique<irs::by_term>();
  *filter->mutable_field() = name;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(value);
  return filter;
}

irs::filter::ptr MakeByTermOrByTerm(std::string_view name0,
                                    std::string_view value0,
                                    std::string_view name1,
                                    std::string_view value1) {
  auto filter = std::make_unique<irs::Or>();
  filter->add<irs::by_term>() =
    std::move(static_cast<irs::by_term&>(*MakeByTerm(name0, value0)));
  filter->add<irs::by_term>() =
    std::move(static_cast<irs::by_term&>(*MakeByTerm(name1, value1)));
  return filter;
}

irs::filter::ptr MakeOr(
  const std::vector<std::pair<std::string_view, std::string_view>>& parts) {
  auto filter = std::make_unique<irs::Or>();
  for (const auto& [name, value] : parts) {
    filter->add<irs::by_term>() =
      std::move(static_cast<irs::by_term&>(*MakeByTerm(name, value)));
  }
  return filter;
}

class SubReaderMock final : public irs::SubReader {
 public:
  virtual uint64_t CountMappedMemory() const { return 0; }

  const irs::SegmentInfo& Meta() const final { return meta_; }

  // Live & deleted docs

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  // Returns an iterator over live documents in current segment.
  irs::doc_iterator::ptr docs_iterator() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  irs::doc_iterator::ptr mask(irs::doc_iterator::ptr&& it) const final {
    EXPECT_FALSE(true);
    return std::move(it);
  }

  // Inverted index

  irs::field_iterator::ptr fields() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Returns corresponding term_reader by the specified field name.
  const irs::term_reader* field(std::string_view) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  // Columnstore

  irs::column_iterator::ptr columns() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::column_reader* column(irs::field_id) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::column_reader* column(std::string_view) const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

  const irs::column_reader* sort() const final {
    EXPECT_FALSE(true);
    return nullptr;
  }

 private:
  irs::SegmentInfo meta_;
};

}  // namespace

namespace tests {

void AssertSnapshotEquality(irs::DirectoryReader lhs,
                            irs::DirectoryReader rhs) {
  ASSERT_EQ(lhs.size(), rhs.size());
  ASSERT_EQ(lhs.docs_count(), rhs.docs_count());
  ASSERT_EQ(lhs.live_docs_count(), rhs.live_docs_count());
  ASSERT_EQ(lhs.Meta(), rhs.Meta());
  auto rhs_segment = rhs.begin();
  for (auto& lhs_segment : lhs.GetImpl()->GetReaders()) {
    ASSERT_EQ(lhs_segment.size(), rhs_segment->size());
    ASSERT_EQ(lhs_segment.docs_count(), rhs_segment->docs_count());
    ASSERT_EQ(lhs_segment.live_docs_count(), rhs_segment->live_docs_count());
    ASSERT_EQ(lhs_segment.Meta(), rhs_segment->Meta());
    ASSERT_TRUE(!lhs_segment.docs_mask() && !rhs_segment->docs_mask() ||
                (lhs_segment.docs_mask() && rhs_segment->docs_mask() &&
                 *lhs_segment->docs_mask() == *rhs_segment->docs_mask()));
    ++rhs_segment;
  }
}

struct incompatible_attribute : irs::attribute {
  incompatible_attribute() noexcept;
};

REGISTER_ATTRIBUTE(incompatible_attribute);

incompatible_attribute::incompatible_attribute() noexcept {}

std::string index_test_base::to_string(
  const testing::TestParamInfo<index_test_context>& info) {
  auto [factory, codec] = info.param;

  std::string str = (*factory)(nullptr).second;
  if (codec.codec) {
    str += "___";
    str += codec.codec;
  }

  return str;
}

std::shared_ptr<irs::directory> index_test_base::get_directory(
  const test_base& ctx) const {
  dir_param_f factory;
  std::tie(factory, std::ignore) = GetParam();

  return (*factory)(&ctx).first;
}

irs::format::ptr index_test_base::get_codec() const {
  tests::format_info info;
  std::tie(std::ignore, info) = GetParam();

  return irs::formats::get(info.codec, info.module);
}

void index_test_base::AssertSnapshotEquality(const irs::IndexWriter& writer) {
  tests::AssertSnapshotEquality(writer.GetSnapshot(), open_reader());
}

void index_test_base::write_segment(irs::IndexWriter& writer,
                                    tests::index_segment& segment,
                                    tests::doc_generator_base& gen) {
  // add segment
  const document* src;

  while ((src = gen.next())) {
    segment.insert(*src);

    ASSERT_TRUE(insert(writer, src->indexed.begin(), src->indexed.end(),
                       src->stored.begin(), src->stored.end(), src->sorted));
  }

  if (writer.Comparator()) {
    segment.sort(*writer.Comparator());
  }
}

void index_test_base::add_segment(irs::IndexWriter& writer,
                                  tests::doc_generator_base& gen) {
  index_.emplace_back(writer.FeatureInfo());
  write_segment(writer, index_.back(), gen);
  writer.Commit();
}

void index_test_base::add_segments(irs::IndexWriter& writer,
                                   std::vector<doc_generator_base::ptr>& gens) {
  for (auto& gen : gens) {
    index_.emplace_back(writer.FeatureInfo());
    write_segment(writer, index_.back(), *gen);
  }
  writer.Commit();
}

void index_test_base::add_segment(
  tests::doc_generator_base& gen, irs::OpenMode mode /*= irs::OM_CREATE*/,
  const irs::IndexWriterOptions& opts /*= {}*/) {
  auto writer = open_writer(mode, opts);
  add_segment(*writer, gen);
}

}  // namespace tests

