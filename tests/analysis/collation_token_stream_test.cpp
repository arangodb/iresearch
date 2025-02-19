////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#ifndef IRESEARCH_ICU_NAMESPACE
#define IRESEARCH_ICU_NAMESPACE icu
#endif

#include "analysis/collation_token_stream.hpp"

#include <unicode/coll.h>
#include <unicode/locid.h>
#include <unicode/sortkey.h>

#include "analysis/collation_token_stream_encoder.hpp"
#include "tests_shared.hpp"
#include "velocypack/Parser.h"
#include "velocypack/velocypack-aliases.h"

namespace {

std::vector<uint8_t> encode(uint8_t b) {
  std::vector<uint8_t> res;
  if (0x80 & b) {
    res.push_back(0xc0 | (b >> 3));
    res.push_back(0x80 | (b & 0x7));
  } else {
    res.push_back(b);
  }
  return res;
}

class CollationEncoder {
 public:
  void encode(irs::bytes_view src) {
    buffer_.clear();
    buffer_.reserve(src.size() * 2);
    for (auto b : src) {
      auto enc = ::encode(b);
      buffer_.insert(buffer_.end(), enc.begin(), enc.end());
    }
  }

  irs::bytes_view getByteArray() noexcept {
    return {buffer_.data(), buffer_.size()};
  }

 private:
  std::vector<uint8_t> buffer_;
};

}  // namespace

TEST(collation_token_stream_test, consts) {
  static_assert("collation" ==
                irs::type<irs::analysis::collation_token_stream>::name());
}

TEST(collation_token_stream_test, empty_analyzer) {
  irs::analysis::collation_token_stream stream{{}};
  ASSERT_FALSE(stream.next());
}

TEST(collation_token_stream_test, test_byte_encoder) {
  uint8_t target{0x0};
  ASSERT_EQ(256, kRecalcMap.size());
  do {
    --target;
    const auto expected = encode(target);
    const auto actual = kRecalcMap[target];
    ASSERT_EQ(expected.size(), actual.second);
    for (size_t i = 0; i < expected.size(); ++i) {
      ASSERT_EQ(expected[i], kBytesRecalcMap[actual.first + i]);
    }
  } while (target != 0);
}

TEST(collation_token_stream_test, construct_from_str) {
  // json
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "ru.koi8.r"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "en-US"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "en-US.utf-8"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de_DE_phonebook"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "C"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de_DE.utf-8@phonebook"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de_DE.UTF-8@collation=phonebook"})");
    ASSERT_NE(nullptr, stream);
    ASSERT_EQ(irs::type<irs::analysis::collation_token_stream>::id(),
              stream->type());
  }

  // invalid
  {
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get(
                         "collation", irs::type<irs::text_format::json>::get(),
                         std::string_view{}));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::get(
                "collation", irs::type<irs::text_format::json>::get(), "1"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::get(
                "collation", irs::type<irs::text_format::json>::get(), "[]"));
    ASSERT_EQ(nullptr,
              irs::analysis::analyzers::get(
                "collation", irs::type<irs::text_format::json>::get(), "{}"));
    ASSERT_EQ(nullptr, irs::analysis::analyzers::get(
                         "collation", irs::type<irs::text_format::json>::get(),
                         "{\"locale\":1}"));
  }
}

TEST(collation_token_stream_test, check_collation) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view locale_name = R"(en)";
  const IRESEARCH_ICU_NAMESPACE::Locale icu_locale =
    IRESEARCH_ICU_NAMESPACE::Locale::createFromName(locale_name.data());

  CollationEncoder encodedKey;
  std::unique_ptr<IRESEARCH_ICU_NAMESPACE::Collator> coll{
    IRESEARCH_ICU_NAMESPACE::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    IRESEARCH_ICU_NAMESPACE::CollationKey key;
    coll->getCollationKey(IRESEARCH_ICU_NAMESPACE::UnicodeString::fromUTF8(
                            IRESEARCH_ICU_NAMESPACE::StringPiece{
                              data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encodedKey.encode({p, static_cast<size_t>(size - 1)});
    return encodedKey.getByteArray();
  };

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "en"})");
    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"å b z a"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "sv"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"a å b z"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);

      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

// collation defined by keywords
TEST(collation_token_stream_test, check_collation_with_variant1) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view locale_name = R"(de@collation=phonebook)";
  const IRESEARCH_ICU_NAMESPACE::Locale icu_locale =
    IRESEARCH_ICU_NAMESPACE::Locale::createFromName(locale_name.data());

  CollationEncoder encodedKey;
  std::unique_ptr<IRESEARCH_ICU_NAMESPACE::Collator> coll{
    IRESEARCH_ICU_NAMESPACE::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    IRESEARCH_ICU_NAMESPACE::CollationKey key;
    coll->getCollationKey(IRESEARCH_ICU_NAMESPACE::UnicodeString::fromUTF8(
                            IRESEARCH_ICU_NAMESPACE::StringPiece{
                              data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encodedKey.encode({p, static_cast<size_t>(size - 1)});
    return encodedKey.getByteArray();
  };

  // locale defined as object
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de__pinyin"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de_pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de@pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de@collation=pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de__phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de_phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, but collation is ignored, because input as old string.
  // should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de_phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, check_collation_with_variant2) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view locale_name = "de_phonebook";
  const IRESEARCH_ICU_NAMESPACE::Locale icu_locale =
    IRESEARCH_ICU_NAMESPACE::Locale::createFromName(locale_name.data());

  CollationEncoder encodedKey;
  std::unique_ptr<IRESEARCH_ICU_NAMESPACE::Collator> coll{
    IRESEARCH_ICU_NAMESPACE::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    IRESEARCH_ICU_NAMESPACE::CollationKey key;
    coll->getCollationKey(IRESEARCH_ICU_NAMESPACE::UnicodeString::fromUTF8(
                            IRESEARCH_ICU_NAMESPACE::StringPiece{
                              data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encodedKey.encode({p, static_cast<size_t>(size - 1)});
    return encodedKey.getByteArray();
  };

  // locale defined as object
  // different variants, should be NOT EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de__pinyan"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_NE(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de__phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as object
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale": "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  // locale defined as string
  // same variants, should be EQUAL
  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de_phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({"locale" : "de@collation=phonebook"})");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      constexpr std::string_view data{"Ärger Ast Aerosol Abbruch Aqua Afrika"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, check_tokens_utf8) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view locale_name = "en-EN.UTF-8";

  const auto icu_locale =
    IRESEARCH_ICU_NAMESPACE::Locale::createFromName(locale_name.data());

  CollationEncoder encodedKey;
  std::unique_ptr<IRESEARCH_ICU_NAMESPACE::Collator> coll{
    IRESEARCH_ICU_NAMESPACE::Collator::createInstance(icu_locale, err)};
  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    err = UErrorCode::U_ZERO_ERROR;
    IRESEARCH_ICU_NAMESPACE::CollationKey key;
    coll->getCollationKey(IRESEARCH_ICU_NAMESPACE::UnicodeString::fromUTF8(
                            IRESEARCH_ICU_NAMESPACE::StringPiece{
                              data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encodedKey.encode({p, static_cast<size_t>(size - 1)});
    return encodedKey.getByteArray();
  };

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({ "locale" : "en" })");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      const std::string_view data{};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      const std::string_view data{""};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr std::string_view data{"quick"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr std::string_view data{"foo"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }

    {
      constexpr std::string_view data{
        "the quick Brown fox jumps over the lazy dog"};
      ASSERT_TRUE(stream->reset(data));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(data.size(), offset->end);
      ASSERT_EQ(get_collation_key(data), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, check_tokens) {
  auto err = UErrorCode::U_ZERO_ERROR;

  constexpr std::string_view locale_name = "de-DE";

  const auto icu_locale =
    IRESEARCH_ICU_NAMESPACE::Locale::createFromName(locale_name.data());

  CollationEncoder encodedKey;
  std::unique_ptr<IRESEARCH_ICU_NAMESPACE::Collator> coll{
    IRESEARCH_ICU_NAMESPACE::Collator::createInstance(icu_locale, err)};

  ASSERT_NE(nullptr, coll);
  ASSERT_TRUE(U_SUCCESS(err));

  auto get_collation_key = [&](std::string_view data) -> irs::bytes_view {
    IRESEARCH_ICU_NAMESPACE::CollationKey key;
    err = UErrorCode::U_ZERO_ERROR;
    coll->getCollationKey(IRESEARCH_ICU_NAMESPACE::UnicodeString::fromUTF8(
                            IRESEARCH_ICU_NAMESPACE::StringPiece{
                              data.data(), static_cast<int32_t>(data.size())}),
                          key, err);
    EXPECT_TRUE(U_SUCCESS(err));

    int32_t size = 0;
    const irs::byte_type* p = key.getByteArray(size);
    EXPECT_NE(nullptr, p);
    EXPECT_NE(0, size);
    encodedKey.encode({p, static_cast<size_t>(size - 1)});
    return encodedKey.getByteArray();
  };

  {
    auto stream = irs::analysis::analyzers::get(
      "collation", irs::type<irs::text_format::json>::get(),
      R"({ "locale" : "de_DE" })");

    ASSERT_NE(nullptr, stream);

    auto* offset = irs::get<irs::offset>(*stream);
    ASSERT_NE(nullptr, offset);
    auto* term = irs::get<irs::term_attribute>(*stream);
    ASSERT_NE(nullptr, term);
    auto* inc = irs::get<irs::increment>(*stream);
    ASSERT_NE(nullptr, inc);

    {
      std::string unicodeData = "\xE2\x82\xAC";

      ASSERT_TRUE(stream->reset(unicodeData));
      ASSERT_TRUE(stream->next());
      ASSERT_EQ(0, offset->start);
      ASSERT_EQ(unicodeData.size(), offset->end);
      ASSERT_EQ(get_collation_key(unicodeData), term->value);
      ASSERT_EQ(1, inc->value);
      ASSERT_FALSE(stream->next());
    }
  }
}

TEST(collation_token_stream_test, normalize) {
  {
    std::string config = R"({ "locale" : "de_DE_phonebook" })";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(
      actual, "collation", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(
      VPackParser::fromJson(R"({ "locale" : "de_DE_PHONEBOOK" })")->toString(),
      actual);
  }

  {
    std::string config = R"({ "locale" : "de_DE.utf-8" })";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(
      actual, "collation", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(VPackParser::fromJson(R"({ "locale" : "de_DE"})")->toString(),
              actual);
  }

  {
    std::string config = R"({ "locale" : "de_DE@collation=phonebook" })";
    std::string actual;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(
      actual, "collation", irs::type<irs::text_format::json>::get(), config));
    ASSERT_EQ(
      VPackParser::fromJson(R"({ "locale" : "de_DE@collation=phonebook" })")
        ->toString(),
      actual);
  }

  {
    std::string config = R"({ "locale" : "de_DE@phonebook" })";
    auto in_vpack = VPackParser::fromJson(config.c_str(), config.size());
    std::string in_str;
    in_str.assign(in_vpack->slice().startAs<char>(),
                  in_vpack->slice().byteSize());
    std::string out_str;
    ASSERT_TRUE(irs::analysis::analyzers::normalize(
      out_str, "collation", irs::type<irs::text_format::vpack>::get(), in_str));
    VPackSlice out_slice(reinterpret_cast<const uint8_t*>(out_str.c_str()));
    ASSERT_EQ(
      VPackParser::fromJson(R"({ "locale" : "de_DE_PHONEBOOK"})")->toString(),
      out_slice.toString());
  }
}
