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

#pragma once

#include "index/index_writer.hpp"
#include "index/index_reader.hpp"

namespace {

  // Returns percentage of live documents
  inline double FillFactor(const irs::SegmentInfo& segment) noexcept {
    return static_cast<double>(segment.live_docs_count) /
           static_cast<double>(segment.docs_count);
  }
  
  // Returns approximated size of a segment in the absence of removals
  inline size_t SizeWithoutRemovals(const irs::SegmentInfo& segment) noexcept {
    return size_t(static_cast<double>(segment.byte_size) * FillFactor(segment));
  }
}

namespace tier {

  struct SegmentStats {
    SegmentStats() = default;

    // cppcheck-suppress noExplicitConstructor
    SegmentStats(const irs::SubReader& reader) noexcept
      : reader{&reader},
        meta{&reader.Meta()},
        size{SizeWithoutRemovals(*meta)},
        fill_factor{FillFactor(*meta)} {}
  
    bool operator<(const SegmentStats& rhs) const noexcept {
      // cppcheck-suppress constVariable
      auto& lhs = *this;
  
      if (lhs.size == rhs.size) {
        if (lhs.fill_factor > rhs.fill_factor) {
          return true;
        } else if (lhs.fill_factor < rhs.fill_factor) {
          return false;
        }
  
        return lhs.meta->name < rhs.meta->name;
      }
  
      return lhs.size < rhs.size;
    }
  
    operator const irs::SubReader*() const noexcept { return reader; }
  
    const irs::SubReader* reader;
    const irs::SegmentInfo* meta;
    size_t size;  // approximate size of segment without removals
    double_t fill_factor; //  live_docs_count / docs_count - meaning that fill_factor is
                          //  inversely proportional to the no. of deletions.
  };

    struct ConsolidationConfig {
      static constexpr size_t candidate_size { 4 };   //  candidate selection window size: 4
      static constexpr double maxMergeScore { 1.5 };  //  max score allowed for candidates consolidation.
                                                      //  Skip consolidation if candidate score is greater
                                                      //  than this value.
    };

    //  interface to fetch the required attributes from
    //  SegmentStats struct.
    //  We use this function in struct ConsolidationCandidate
    //  to fetch the segment dimensions from the SegmentStats
    //  struct.
    //
    void getSegmentDimensions(
        std::vector<tier::SegmentStats>::const_iterator itr,
        uint64_t& byte_size,
        uint64_t& docs_count,
        uint64_t& live_docs_count);

    template<typename Segment>
    struct ConsolidationCandidate {
      using iterator_t = std::vector<Segment>::const_iterator;
      using range_t = std::pair<iterator_t, iterator_t>;

      ConsolidationCandidate() = default;

      ConsolidationCandidate(
        iterator_t start,
        iterator_t end,
        std::function<void(
            iterator_t /* segment iterator */,
            uint64_t& /* byte_size */,
            uint64_t& /* docs_count */,
            uint64_t& /* live_docs_count */)> accessor
      ) noexcept : segments(start, end), accessor_(accessor) {

        initialized = true;

        //  Calculate initial cost
        uint64_t byte_size;
        uint64_t docs_count;
        uint64_t live_docs_count;

        auto itr = start;
        do
        {
          accessor_(itr, byte_size, docs_count, live_docs_count);

          mergeBytes += byte_size;
          delCount += (docs_count - live_docs_count);

        } while (itr++ != end);

        skew = static_cast<double>(byte_size) / mergeBytes;
        mergeScore = skew + (1.0 / (1 + delCount));
        cost = mergeBytes * mergeScore;
      }

      void advance() noexcept {
        if (!initialized)
          return;

        const auto& removeSegment = segments.first;
        const auto& addSegment = segments.second + 1;

        std::advance(segments.first, 1);
        std::advance(segments.second, 1);

        //  Segment to be removed
        uint64_t rem_byte_size;
        uint64_t rem_docs_count;
        uint64_t rem_live_docs_count;
        accessor_(removeSegment, rem_byte_size, rem_docs_count, rem_live_docs_count);
        auto rem_del_count = rem_docs_count - rem_live_docs_count;

        //  Segment to be added
        uint64_t add_byte_size;
        uint64_t add_docs_count;
        uint64_t add_live_docs_count;
        accessor_(addSegment, add_byte_size, add_docs_count, add_live_docs_count);
        auto add_del_count = add_docs_count - add_live_docs_count;

        mergeBytes = mergeBytes - rem_byte_size + add_byte_size;
        skew = static_cast<double>(add_byte_size) / mergeBytes;
        delCount = delCount - rem_del_count + add_del_count;
        mergeScore = skew + (1.0 / (1 + delCount));
        cost = mergeBytes * mergeScore;
      }

      iterator_t first() const noexcept { return segments.first; }
      iterator_t last() const noexcept { return segments.second; }

      size_t mergeBytes { 0 };
      double skew { 0.0 };
      size_t delCount { 0 };
      double mergeScore { 0.0 };
      double cost { 0.0 };
      bool initialized { false };

      range_t segments;
      std::function<void(iterator_t, uint64_t&, uint64_t&, uint64_t&)> accessor_;
    };

    //
    //  This function receives a sorted vector of segments
    //  and finds the best contiguous subset of segments to
    //  merge together.
    //  The best subset is defined as the one with the lowest
    //  merge cost and the merge cost is computed inside the
    //  ConslidateCandidate struct.
    //
    //  findBestConsolidationCandidate merely sets a rolling
    //  window of size 4 on a subset of segments, it lets
    //  ConsolidationCandidate compute the cost of merge for that
    //  subset, repeats this process for all contiguous subsets
    //  and identifies the best candidate.
    //
    template<typename Segment>
    bool findBestConsolidationCandidate(
      const std::vector<Segment>& sorted_segments,
      const std::function<
        void(const typename std::vector<Segment>::const_iterator,
             uint64_t& /* byte_size */,
             uint64_t& /* docs_count */,
             uint64_t& /* live_docs_count */)>& getSegmentAttributes,
      tier::ConsolidationCandidate<Segment>& best) {
      if (sorted_segments.size() < tier::ConsolidationConfig::candidate_size)
        return false;

      //  set consolidation window size
      auto front = sorted_segments.begin();
      auto rear = front + (tier::ConsolidationConfig::candidate_size - 1);
      tier::ConsolidationCandidate<Segment> candidate(front, rear,
                                                        getSegmentAttributes);

      while (candidate.last() != sorted_segments.end()) {
        if (!best.initialized || candidate.mergeScore < best.mergeScore)
          best = candidate;

        if (sorted_segments.end() == (candidate.last() + 1))
          break;

        candidate.advance();
      }

      return (best.initialized &&
              best.mergeScore <= tier::ConsolidationConfig::maxMergeScore);
    }
}

namespace irs::index_utils {

// merge segment if:
//   {threshold} > segment_bytes / (all_segment_bytes / #segments)
struct ConsolidateBytes {
  float threshold = 0;
};

ConsolidationPolicy MakePolicy(const ConsolidateBytes& options);

// merge segment if:
//   {threshold} >= (segment_bytes + sum_of_merge_candidate_segment_bytes) /
//   all_segment_bytes
struct ConsolidateBytesAccum {
  float threshold = 0;
};

ConsolidationPolicy MakePolicy(const ConsolidateBytesAccum& options);

// merge first {threshold} segments
struct ConsolidateCount {
  size_t threshold = std::numeric_limits<size_t>::max();
};

ConsolidationPolicy MakePolicy(const ConsolidateCount& options);

// merge segment if:
//   {threshold} >= segment_docs{valid} / (all_segment_docs{valid} / #segments)
struct ConsolidateDocsLive {
  float threshold = 0;
};

ConsolidationPolicy MakePolicy(const ConsolidateDocsLive& options);

// merge segment if:
//   {threshold} > #segment_docs{valid} / (#segment_docs{valid} +
//   #segment_docs{removed})
struct ConsolidateDocsFill {
  float threshold = 0;
};

ConsolidationPolicy MakePolicy(const ConsolidateDocsFill& options);

struct ConsolidateTier {
  // minimum allowed number of segments to consolidate at once
  size_t min_segments = 1;
  // maximum allowed number of segments to consolidate at once
  size_t max_segments = 10;
  // maxinum allowed size of all consolidated segments
  size_t max_segments_bytes = size_t(5) * (1 << 30);
  // treat all smaller segments as equal for consolidation selection
  size_t floor_segment_bytes = size_t(2) * (1 << 20);
  // filter out candidates with score less than min_score
  double_t min_score = 0.;
};

ConsolidationPolicy MakePolicy(const ConsolidateTier& options);

void ReadDocumentMask(DocumentMask& docs_mask, const directory& dir,
                      const SegmentMeta& meta);

// Writes segment_meta to the supplied directory
void FlushIndexSegment(directory& dir, IndexSegment& segment);

}  // namespace irs::index_utils
