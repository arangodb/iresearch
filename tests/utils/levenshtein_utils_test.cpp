﻿////////////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////////////

#include "utils/levenshtein_utils.hpp"

#include "store/memory_directory.hpp"
#include "store/store_utils.hpp"
#include "tests_shared.hpp"
#include "utils/automaton_utils.hpp"
#include "utils/fstext/fst_table_matcher.hpp"
#include "utils/utf8_utils.hpp"

using namespace std::literals;

namespace {

void assert_description(
  const irs::parametric_description& description, const irs::bytes_view& prefix,
  const irs::bytes_view& term,
  const std::vector<std::tuple<irs::bytes_view, size_t, size_t, size_t>>&
    candidates) {
  auto a = irs::make_levenshtein_automaton(description, prefix, term);

  irs::bstring target(prefix.data(), prefix.size());
  target += term;

  // ensure only invalid state has no outbound connections
  ASSERT_GE(a.NumStates(), 1);
  ASSERT_EQ(0, a.NumArcs(0));
  for (irs::automaton::StateId state = 1; state < a.NumStates(); ++state) {
    ASSERT_GT(a.NumArcs(state), 0);
  }

  for (auto& entry : candidates) {
    const auto candidate = std::get<0>(entry);
    const size_t expected_distance = std::get<1>(entry);
    const size_t expected_distance_automaton = std::get<2>(entry);
    const size_t expected_distance_precise = std::get<3>(entry);
    SCOPED_TRACE(testing::Message("Target: '")
                 << irs::ViewCast<char>(irs::bytes_view{target})
                 << "', Candidate: '"
                 << irs::ViewCast<char>(irs::bytes_view{candidate})
                 << "' , Distance: " << expected_distance
                 << " , Precise distance: " << expected_distance_precise);

    std::vector<uint32_t> utf8_target, utf8_candidate;
    irs::utf8_utils::ToUTF32<true>(target, std::back_inserter(utf8_target));
    irs::utf8_utils::ToUTF32<true>(candidate,
                                   std::back_inserter(utf8_candidate));
    const irs::basic_string_view<uint32_t> utf8_target_ref(utf8_target.data(),
                                                           utf8_target.size());
    const irs::basic_string_view<uint32_t> utf8_candidate_ref(
      utf8_candidate.data(), utf8_candidate.size());

    ASSERT_EQ(expected_distance_precise,
              irs::edit_distance(utf8_candidate_ref, utf8_target_ref));
    ASSERT_EQ(expected_distance_precise,
              irs::edit_distance(utf8_target_ref, utf8_candidate_ref));

    {
      size_t actual_distance;
      if (irs::edit_distance(actual_distance, description, candidate, target)) {
        ASSERT_EQ(expected_distance, actual_distance);
        ASSERT_EQ(expected_distance,
                  irs::edit_distance(description, candidate, target));
      }
    }

    {
      size_t actual_distance;
      if (irs::edit_distance(actual_distance, description, target, candidate)) {
        ASSERT_EQ(expected_distance, actual_distance);
        ASSERT_EQ(expected_distance,
                  irs::edit_distance(description, target, candidate));
      }
    }

    const auto state = irs::Accept(a, candidate);
    ASSERT_EQ(expected_distance_automaton <= description.max_distance(),
              bool(state));
    if (state) {
      // every final state contains valid edit distance
      ASSERT_EQ(expected_distance_automaton, state.Payload());
    }
  }
}

void assert_description(
  const irs::parametric_description& description, const irs::bytes_view& target,
  const std::vector<std::tuple<irs::bytes_view, size_t, size_t, size_t>>&
    candidates) {
  return assert_description(description, irs::kEmptyStringView<irs::byte_type>,
                            target, candidates);
}

void assert_read_write(const irs::parametric_description& description) {
  irs::bstring buf;
  {
    irs::bytes_output out(buf);
    irs::write(description, static_cast<irs::data_output&>(out));
    ASSERT_FALSE(buf.empty());
  }
  {
    irs::bytes_view_input in(buf);
    ASSERT_EQ(description, irs::read(in));
  }
}

}  // namespace

TEST(levenshtein_utils_test, test_distance) {
  {
    const std::string_view lhs = "aec";
    const std::string_view rhs = "abcd";

    ASSERT_EQ(
      2, irs::edit_distance(lhs.data(), lhs.size(), rhs.data(), rhs.size()));
    ASSERT_EQ(
      2, irs::edit_distance(rhs.data(), rhs.size(), lhs.data(), lhs.size()));
  }

  {
    const std::string_view lhs = "elephant";
    const std::string_view rhs = "relevant";

    ASSERT_EQ(
      3, irs::edit_distance(lhs.data(), lhs.size(), rhs.data(), rhs.size()));
    ASSERT_EQ(
      3, irs::edit_distance(rhs.data(), rhs.size(), lhs.data(), lhs.size()));
  }

  {
    const std::string_view lhs = "\xD0\xBF\xD1\x83\xD1\x82\xD0\xB8\xD0\xBD";
    const std::string_view rhs = "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBB\xD0\xBE";

    std::vector<uint32_t> lhs_utf8, rhs_utf8;
    irs::utf8_utils::ToUTF32<false>(irs::ViewCast<irs::byte_type>(lhs),
                                    std::back_inserter(lhs_utf8));
    irs::utf8_utils::ToUTF32<false>(irs::ViewCast<irs::byte_type>(rhs),
                                    std::back_inserter(rhs_utf8));

    ASSERT_EQ(4, irs::edit_distance(lhs_utf8.data(), lhs_utf8.size(),
                                    rhs_utf8.data(), rhs_utf8.size()));
    ASSERT_EQ(4, irs::edit_distance(rhs_utf8.data(), rhs_utf8.size(),
                                    lhs_utf8.data(), lhs_utf8.size()));
  }

  {
    const std::string_view lhs = "";
    const std::string_view rhs = "aec";

    ASSERT_EQ(
      3, irs::edit_distance(lhs.data(), lhs.size(), rhs.data(), rhs.size()));
    ASSERT_EQ(
      3, irs::edit_distance(rhs.data(), rhs.size(), lhs.data(), lhs.size()));
  }

  {
    const std::string_view lhs = "";
    const std::string_view rhs = "";

    ASSERT_EQ(
      0, irs::edit_distance(lhs.data(), lhs.size(), rhs.data(), rhs.size()));
    ASSERT_EQ(
      0, irs::edit_distance(rhs.data(), rhs.size(), lhs.data(), lhs.size()));
  }

  {
    const std::string_view lhs;
    const std::string_view rhs;

    ASSERT_EQ(
      0, irs::edit_distance(lhs.data(), lhs.size(), rhs.data(), rhs.size()));
    ASSERT_EQ(
      0, irs::edit_distance(rhs.data(), rhs.size(), lhs.data(), lhs.size()));
  }
}

TEST(levenshtein_utils_test, test_static_const) {
  ASSERT_EQ(31, decltype(irs::parametric_description::MAX_DISTANCE)(
                  irs::parametric_description::MAX_DISTANCE));
}

TEST(levenshtein_utils_test, test_description_0) {
  auto assert_distance = [](const irs::parametric_description& d) {
    // state 0
    ASSERT_EQ(1, d.distance(0, 0));
    // state 1
    ASSERT_EQ(0, d.distance(1, 0));
  };

  auto assert_transitions = [](const irs::parametric_description& d) {
    // state 0
    ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
              d.transition(0, 0));
    ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
              d.transition(0, 1));
    // state 1
    ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
              d.transition(1, 0));
    ASSERT_EQ(irs::parametric_description::transition_t(1, 1),
              d.transition(1, 1));
  };

  // no transpositions
  {
    auto description = irs::make_parametric_description(0, false);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(2, description.size());
    ASSERT_EQ(1, description.chi_size());
    ASSERT_EQ(2, description.chi_max());
    ASSERT_EQ(0, description.max_distance());
    ASSERT_EQ(description, irs::make_parametric_description(0, true));
    ASSERT_EQ(description, irs::make_parametric_description(0, false));
    assert_distance(description);
    assert_transitions(description);
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 1, 1, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 1, 1, 3},
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\x68\xD1\xFE\x62\x65\x74"sv),
         1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xF0\x9F\x98\x81\x6c\x70\x68\xD1\xFE\x62\x65\x74"sv),
         1, 1, 2},
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\xE2\xFF\xFF\xD1\xFE\x62\x65\x74"sv),
         1, 1, 2},
      });
  }

  // no transpositions with prefix
  {
    auto description = irs::make_parametric_description(0, false);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(2, description.size());
    ASSERT_EQ(1, description.chi_size());
    ASSERT_EQ(2, description.chi_max());
    ASSERT_EQ(0, description.max_distance());
    ASSERT_EQ(description, irs::make_parametric_description(0, true));
    ASSERT_EQ(description, irs::make_parametric_description(0, false));
    assert_distance(description);
    assert_transitions(description);
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alpha"sv),
      irs::ViewCast<irs::byte_type>("bet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 1, 1, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 1, 1, 2},
      });
  }

  // transpositions
  {
    auto description = irs::make_parametric_description(0, true);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(2, description.size());
    ASSERT_EQ(1, description.chi_size());
    ASSERT_EQ(2, description.chi_max());
    ASSERT_EQ(0, description.max_distance());
    ASSERT_EQ(description, irs::make_parametric_description(0, true));
    ASSERT_EQ(description, irs::make_parametric_description(0, false));
    assert_distance(description);
    assert_transitions(description);
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 1, 1, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 1, 1, 3},
      });
  }

  // transpositions with prefix
  {
    auto description = irs::make_parametric_description(0, true);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(2, description.size());
    ASSERT_EQ(1, description.chi_size());
    ASSERT_EQ(2, description.chi_max());
    ASSERT_EQ(0, description.max_distance());
    ASSERT_EQ(description, irs::make_parametric_description(0, true));
    ASSERT_EQ(description, irs::make_parametric_description(0, false));
    assert_distance(description);
    assert_transitions(description);
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("al"sv),
      irs::ViewCast<irs::byte_type>("phabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 1, 1, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 1, 1, 2},
      });
  }
}

TEST(levenshtein_utils_test, test_description_1) {
  // no transpositions
  {
    auto assert_distance = [](const irs::parametric_description& d) {
      // state 0
      ASSERT_EQ(2, d.distance(0, 0));
      ASSERT_EQ(2, d.distance(0, 1));
      ASSERT_EQ(2, d.distance(0, 2));
      // state 1
      ASSERT_EQ(0, d.distance(1, 0));
      ASSERT_EQ(1, d.distance(1, 1));
      ASSERT_EQ(2, d.distance(1, 2));
      // state 2
      ASSERT_EQ(1, d.distance(2, 0));
      ASSERT_EQ(1, d.distance(2, 1));
      ASSERT_EQ(2, d.distance(2, 2));
      // state 3
      ASSERT_EQ(1, d.distance(3, 0));
      ASSERT_EQ(1, d.distance(3, 1));
      ASSERT_EQ(1, d.distance(3, 2));
      // state 4
      ASSERT_EQ(1, d.distance(4, 0));
      ASSERT_EQ(2, d.distance(4, 1));
      ASSERT_EQ(2, d.distance(4, 2));
      // state 5
      ASSERT_EQ(1, d.distance(5, 0));
      ASSERT_EQ(2, d.distance(5, 1));
      ASSERT_EQ(1, d.distance(5, 2));
    };

    auto assert_transitions = [](const irs::parametric_description& d) {
      // state 0
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 0));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 1));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 2));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 3));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 4));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 5));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 6));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(0, 7));
      // state 1
      ASSERT_EQ(irs::parametric_description::transition_t(2, 0),
                d.transition(1, 0));
      ASSERT_EQ(irs::parametric_description::transition_t(1, 1),
                d.transition(1, 1));
      ASSERT_EQ(irs::parametric_description::transition_t(3, 0),
                d.transition(1, 2));
      ASSERT_EQ(irs::parametric_description::transition_t(1, 1),
                d.transition(1, 3));
      ASSERT_EQ(irs::parametric_description::transition_t(2, 0),
                d.transition(1, 4));
      ASSERT_EQ(irs::parametric_description::transition_t(1, 1),
                d.transition(1, 5));
      ASSERT_EQ(irs::parametric_description::transition_t(3, 0),
                d.transition(1, 6));
      ASSERT_EQ(irs::parametric_description::transition_t(1, 1),
                d.transition(1, 7));
      // state 2
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(2, 0));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(2, 1));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 2),
                d.transition(2, 2));
      ASSERT_EQ(irs::parametric_description::transition_t(2, 1),
                d.transition(2, 3));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(2, 4));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(2, 5));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 2),
                d.transition(2, 6));
      ASSERT_EQ(irs::parametric_description::transition_t(2, 1),
                d.transition(2, 7));
      // state 3
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(3, 0));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(3, 1));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 2),
                d.transition(3, 2));
      ASSERT_EQ(irs::parametric_description::transition_t(2, 1),
                d.transition(3, 3));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 3),
                d.transition(3, 4));
      ASSERT_EQ(irs::parametric_description::transition_t(5, 1),
                d.transition(3, 5));
      ASSERT_EQ(irs::parametric_description::transition_t(2, 2),
                d.transition(3, 6));
      ASSERT_EQ(irs::parametric_description::transition_t(3, 1),
                d.transition(3, 7));
      // state 4
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(4, 0));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(4, 1));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(4, 2));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(4, 3));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(4, 4));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(4, 5));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(4, 6));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(4, 7));
      // state 5
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(5, 0));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(5, 1));
      ASSERT_EQ(irs::parametric_description::transition_t(0, 0),
                d.transition(5, 2));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 1),
                d.transition(5, 3));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 3),
                d.transition(5, 4));
      ASSERT_EQ(irs::parametric_description::transition_t(5, 1),
                d.transition(5, 5));
      ASSERT_EQ(irs::parametric_description::transition_t(4, 3),
                d.transition(5, 6));
      ASSERT_EQ(irs::parametric_description::transition_t(5, 1),
                d.transition(5, 7));
    };

    auto description = irs::make_parametric_description(1, false);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(6, description.size());
    ASSERT_EQ(3, description.chi_size());
    ASSERT_EQ(8, description.chi_max());
    ASSERT_EQ(1, description.max_distance());
    ASSERT_NE(description, irs::make_parametric_description(1, true));
    ASSERT_EQ(description, irs::make_parametric_description(1, false));
    assert_distance(description);
    assert_transitions(description);
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("a"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 2, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 2, 2,
         2},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 2,
         2, 2},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 1, 1,
         1},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x83"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 1, 1, 1},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 0, 0, 0},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\x9E"sv), 2, 2, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 2, 2,
         2},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 2,
         2, 2},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 1, 1,
         1},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // invalid 2-byte utf8 char - not accepted by automaton
        {irs::ViewCast<irs::byte_type>("\xD1\xFF"sv), 1, 2, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\x9E"sv), 1, 1, 1},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 0, 0, 0},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 1, 1,
         1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 2,
         2, 2},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 1, 1,
         1},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 2, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 1, 1,
         1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 1,
         1, 1},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 0, 0,
         0},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("alpha"sv),
      irs::ViewCast<irs::byte_type>("bet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x84\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xF0\x9F\x98\x82\xD0\xB9"sv), 1,
         1, 1},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD1\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xF0\xB9\xB9\xB9"sv), 1,
         1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 3},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85"sv),
      irs::ViewCast<irs::byte_type>("\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x84\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xF0\x9F\x98\x82\xD0\xB9"sv), 1,
         1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD1\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xF0\xB9\xB9\xB9"sv), 1,
         1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
      });
  }

  // transpositions
  {
    auto description = irs::make_parametric_description(1, true);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(8, description.size());
    ASSERT_EQ(3, description.chi_size());
    ASSERT_EQ(8, description.chi_max());
    ASSERT_EQ(1, description.max_distance());
    ASSERT_EQ(description, irs::make_parametric_description(1, true));
    ASSERT_NE(description, irs::make_parametric_description(1, false));
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("alph"sv),
      irs::ViewCast<irs::byte_type>("abet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("a"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 2, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 2, 2,
         2},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 2,
         2, 2},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 1, 1,
         1},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x83"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 1, 1, 1},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 0, 0, 0},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 2, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 2, 2,
         2},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 2,
         2, 2},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 1, 1,
         1},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\x9E"sv), 1, 1, 1},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 1, 2, 1},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 0, 0, 0},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 1, 1,
         1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 2,
         2, 2},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 1, 1,
         1},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 2, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 1, 1,
         1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 1,
         1, 1},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 0, 0,
         0},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 3},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85"sv),
      irs::ViewCast<irs::byte_type>("\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
      });
  }
}

TEST(levenshtein_utils_test, test_description_2) {
  // no transpositions
  {
    auto description = irs::make_parametric_description(2, false);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(31, description.size());
    ASSERT_EQ(5, description.chi_size());
    ASSERT_EQ(32, description.chi_max());
    ASSERT_EQ(2, description.max_distance());
    ASSERT_NE(description, irs::make_parametric_description(2, true));
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\x9E"sv), 2, 2, 2},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 3, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 1, 1,
         1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 1,
         1, 1},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 0, 0,
         0},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 3, 3, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\x68\xD1\x96\x62\x65\x74"sv),
         1, 1, 1},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\x68\xD1\xFE\x62\x65\x74"sv),
         1, 3, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xF0\x9F\x98\x81\x6c\x70\x68\xD1\x96\x62\x65\x74"sv),
         2, 2, 2},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>(
           "\xF0\x9F\x98\x81\x6c\x70\x68\xD1\xFE\x62\x65\x74"sv),
         2, 3, 2},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\xE2\xFF\xFF\xD1\xFE\x62\x65\x74"sv),
         2, 3, 2},
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\xD1\x95\x70\xD1\x96\x62\x65\x74"sv),
         3, 3, 3},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("a"sv),
      irs::ViewCast<irs::byte_type>("lphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 3, 3, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\x68\xD1\x96\x62\x65\x74"sv),
         1, 1, 1},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\x68\xD1\xFE\x62\x65\x74"sv),
         1, 3, 1},
        // not accepted by automaton due to invalid utf8 characters
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\xE2\xFF\xFF\xD1\xFE\x62\x65\x74"sv),
         2, 3, 2},
        {irs::ViewCast<irs::byte_type>(
           "\x61\x6c\x70\xD1\x95\x70\xD1\x96\x62\x65\x74"sv),
         3, 3, 3},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         3, 3, 3},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85"sv),
      irs::ViewCast<irs::byte_type>("\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
      });
  }

  // transpositions
  {
    auto description = irs::make_parametric_description(2, true);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(68, description.size());
    ASSERT_EQ(5, description.chi_size());
    ASSERT_EQ(32, description.chi_max());
    ASSERT_EQ(2, description.max_distance());
    ASSERT_NE(description, irs::make_parametric_description(2, false));
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv),
      {
        {irs::kEmptyStringView<irs::byte_type>, 1, 1, 1},
        // 1-byte sequence
        {irs::ViewCast<irs::byte_type>("a"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("b"sv), 1, 1, 1},
        // 2-byte sequence
        {irs::ViewCast<irs::byte_type>("\xD1\x83"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1\x83"sv), 2, 2, 2},
        // incomplete 2-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xD1"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xD1\x83\xD1"sv), 1, 1, 1},
        // 3-byte sequence
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\x96"sv), 2, 2, 2},
        // not accepted by automaton due to invalid utf8 characters
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E\xE2"sv), 2, 3, 2},
        // incomplete 3-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xE2\x9E"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xE2\x9E\x96\xE2\x9E"sv), 1, 1, 1},
        // 4-byte sequence
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xE2\x9E\x96"sv), 1, 1,
         1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98\x81"sv), 1,
         1, 1},
        // incomplete 4-byte utf8 char - treat symbol as non-existent
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xF0\x9F\x98\x81\xF0\x9F\x98"sv), 0, 0,
         0},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("alp"sv),
      irs::ViewCast<irs::byte_type>("habet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 2, 2, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 3, 3, 3},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         3, 3, 3},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85"sv),
      irs::ViewCast<irs::byte_type>("\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
      });
  }
}

TEST(levenshtein_utils_test, test_description_3) {
  // no transpositions
  {
    auto description = irs::make_parametric_description(3, false);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(197, description.size());
    ASSERT_EQ(7, description.chi_size());
    ASSERT_EQ(128, description.chi_max());
    ASSERT_EQ(3, description.max_distance());
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("alhpeabt"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("laphaebt"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 3, 3, 3},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("a"sv),
      irs::ViewCast<irs::byte_type>("lphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("alhpeabt"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         3, 3, 3},
      });

    // "aec" vs "abc"
    ASSERT_EQ(
      1, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("aec"sv),
                            irs::ViewCast<irs::byte_type>("abc"sv)));
    ASSERT_EQ(
      1, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("abc"sv),
                            irs::ViewCast<irs::byte_type>("aec"sv)));
    // "aec" vs "ac"
    ASSERT_EQ(
      1, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("aec"sv),
                            irs::ViewCast<irs::byte_type>("ac"sv)));
    ASSERT_EQ(
      1, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("ac"sv),
                            irs::ViewCast<irs::byte_type>("aec"sv)));
    // "aec" vs "zaec"
    ASSERT_EQ(
      1, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("aec"sv),
                            irs::ViewCast<irs::byte_type>("zaec"sv)));
    ASSERT_EQ(1, irs::edit_distance(description,
                                    irs::ViewCast<irs::byte_type>("zaec"sv),
                                    irs::ViewCast<irs::byte_type>("aec"sv)));
    // "aec" vs "abcd"
    ASSERT_EQ(
      2, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("aec"sv),
                            irs::ViewCast<irs::byte_type>("abcd"sv)));
    ASSERT_EQ(2, irs::edit_distance(description,
                                    irs::ViewCast<irs::byte_type>("abcd"sv),
                                    irs::ViewCast<irs::byte_type>("aec"sv)));
    // "aec" vs "abcdz"
    ASSERT_EQ(
      3, irs::edit_distance(description, irs::ViewCast<irs::byte_type>("aec"sv),
                            irs::ViewCast<irs::byte_type>("abcdz"sv)));
    ASSERT_EQ(3, irs::edit_distance(description,
                                    irs::ViewCast<irs::byte_type>("abcdz"sv),
                                    irs::ViewCast<irs::byte_type>("aec"sv)));
    // "aec" vs "abcdefasdfasdf", can differentiate distances up to
    // 'desc.max_distance'
    ASSERT_EQ(
      description.max_distance() + 1,
      irs::edit_distance(description, irs::ViewCast<irs::byte_type>("aec"sv),
                         irs::ViewCast<irs::byte_type>("abcdefasdfasdf"sv)));
    ASSERT_EQ(description.max_distance() + 1,
              irs::edit_distance(
                description, irs::ViewCast<irs::byte_type>("abcdefasdfasdf"sv),
                irs::ViewCast<irs::byte_type>("aec"sv)));
  }

  // transpositions
  {
    auto description = irs::make_parametric_description(3, true);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(769, description.size());
    ASSERT_EQ(7, description.chi_size());
    ASSERT_EQ(128, description.chi_max());
    ASSERT_EQ(3, description.max_distance());
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 2, 2, 4},
        {irs::ViewCast<irs::byte_type>("alhpeabt"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("laphaebt"sv), 2, 2, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 1, 1, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("lpzazez"sv), 4, 4, 4},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         3, 3, 3},
      });
  }
}

TEST(levenshtein_utils_test, test_description_4) {
  // no transpositions
  {
    auto description = irs::make_parametric_description(4, false);
    ASSERT_TRUE(bool(description));
    ASSERT_EQ(1354, description.size());
    ASSERT_EQ(9, description.chi_size());
    ASSERT_EQ(512, description.chi_max());
    ASSERT_EQ(4, description.max_distance());
    ASSERT_EQ(description, description);
    assert_read_write(description);

    assert_description(
      description, irs::ViewCast<irs::byte_type>("alphabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("alhpeabt"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("laphaebt"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("lphazez"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("phazez"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("phzez"sv), 5, 5, 5},
        {irs::ViewCast<irs::byte_type>("hzez"sv), 5, 5, 6},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("al"sv),
      irs::ViewCast<irs::byte_type>("phabet"sv),
      {
        {irs::ViewCast<irs::byte_type>("alphabet"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("alphabez"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alphaet"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>("alhpaebt"sv), 4, 4, 4},
        {irs::ViewCast<irs::byte_type>("alhpeabt"sv), 3, 3, 3},
        {irs::ViewCast<irs::byte_type>("alphaebt"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2},
        {irs::ViewCast<irs::byte_type>("alphazez"sv), 2, 2, 2},
      });

    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>("\xD0\xBF\xD1\x83\xD0\xB9"sv), 1, 1, 1},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
        {irs::ViewCast<irs::byte_type>(
           "\xD0\xBF\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         3, 3, 3},
      });

    // with prefix
    assert_description(
      description, irs::ViewCast<irs::byte_type>("\xD1\x85"sv),
      irs::ViewCast<irs::byte_type>("\xD1\x83\xD0\xB9"sv),
      {
        {irs::ViewCast<irs::byte_type>("\xD1\x85\xD1\x83\xD0\xB9"sv), 0, 0, 0},
        {irs::ViewCast<irs::byte_type>(
           "\xD1\x85\xD1\x83\xD0\xB9\xD0\xBD\xD1\x8F"sv),
         2, 2, 2},
      });
  }

  // Commented out since it takes ~10 min to pass
  // #ifndef IRESEARCH_DEBUG
  //  // with transpositions
  //  {
  //    auto description = irs::make_parametric_description(4, true);
  //    ASSERT_TRUE(bool(description));
  //    ASSERT_EQ(9628, description.size());
  //    ASSERT_EQ(9, description.chi_size());
  //    ASSERT_EQ(512, description.chi_max());
  //    ASSERT_EQ(4, description.max_distance());
  //
  //    assert_description(
  //      description,
  //      irs::ViewCast<irs::byte_type>("alphabet"sv),
  //      {
  //        { irs::ViewCast<irs::byte_type>("alphabet"sv),  0, 0, 0 },
  //        { irs::ViewCast<irs::byte_type>("alphabez"sv),  1, 1, 1 },
  //        { irs::ViewCast<irs::byte_type>("alphaet"sv),   1, 1, 1 },
  //        { irs::ViewCast<irs::byte_type>("alhpaebt"sv),  2, 2, 4 },
  //        { irs::ViewCast<irs::byte_type>("alhpeabt"sv),  3, 3, 3 },
  //        { irs::ViewCast<irs::byte_type>("laphaebt"sv),  2, 2, 4 },
  //        { irs::ViewCast<irs::byte_type>("alphaebt"sv),  1, 1, 2 },
  //        { irs::ViewCast<irs::byte_type>("alphabezz"sv), 2, 2, 2 },
  //        { irs::ViewCast<irs::byte_type>("alphazez"sv),  2, 2, 2 },
  //        { irs::ViewCast<irs::byte_type>("lphazez"sv),   3, 3, 3 },
  //        { irs::ViewCast<irs::byte_type>("phazez"sv),    4, 4, 4 },
  //        { irs::ViewCast<irs::byte_type>("phzez"sv),     5, 5, 5 },
  //        { irs::ViewCast<irs::byte_type>("hzez"sv),      5, 5, 6 },
  //      }
  //    );
  //  }
  // #endif
}

TEST(levenshtein_utils_test, test_description_invalid) {
  ASSERT_FALSE(irs::parametric_description());

  // no transpositions
  {
    auto description = irs::make_parametric_description(
      irs::parametric_description::MAX_DISTANCE + 1, false);
    ASSERT_FALSE(bool(description));
    ASSERT_EQ(0, description.size());
    ASSERT_EQ(0, description.chi_size());
    ASSERT_EQ(0, description.chi_max());
    ASSERT_EQ(0, description.max_distance());
    ASSERT_EQ(irs::parametric_description(), description);
    ASSERT_NE(description, irs::make_parametric_description(0, false));
  }

  // transpositions
  {
    auto description = irs::make_parametric_description(
      irs::parametric_description::MAX_DISTANCE + 1, true);
    ASSERT_FALSE(bool(description));
    ASSERT_EQ(0, description.size());
    ASSERT_EQ(0, description.chi_size());
    ASSERT_EQ(0, description.chi_max());
    ASSERT_EQ(0, description.max_distance());
  }
}
