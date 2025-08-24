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
      static constexpr size_t candidate_size { 2 };   //  candidate selection window size: 4
      static constexpr double maxMergeScore { 0.4 };  //  max score allowed for candidates consolidation.
                                                      //  Skip consolidation if candidate score is greater
                                                      //  than this value.
      static constexpr double maxLivePercentage { 0.5 };  //  Max live docs % of a segment to consider it
                                                          //  for cleanup during consolidation.
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

        } while (itr++ != end);

        skew = static_cast<double>(byte_size) / mergeBytes;
        mergeScore = skew;
      }

      bool pop_front() {
        if (!initialized)
          return false;

        const auto removeSegment = first();
        const auto lastSegment = last();

        std::advance(segments.first, 1);

        //  Segment to be removed
        uint64_t rem_byte_size;
        uint64_t rem_docs_count;
        uint64_t rem_live_docs_count;
        accessor_(removeSegment, rem_byte_size, rem_docs_count, rem_live_docs_count);

        uint64_t last_seg_byte_size;
        uint64_t ignore;
        accessor_(lastSegment, last_seg_byte_size, ignore, ignore);

        mergeBytes -= rem_byte_size;
        skew = static_cast<double>(last_seg_byte_size) / mergeBytes;
        mergeScore = skew;

        return true;
      }

      bool push_back() noexcept {
        if (!initialized)
          return false;

        const auto addSegment = segments.second + 1;

        std::advance(segments.second, 1);

        //  Segment to be added
        uint64_t add_byte_size;
        uint64_t add_docs_count;
        uint64_t add_live_docs_count;
        accessor_(addSegment, add_byte_size, add_docs_count, add_live_docs_count);

        mergeBytes += add_byte_size;
        skew = static_cast<double>(add_byte_size) / mergeBytes;
        mergeScore = skew;

        return true;
      }

      iterator_t first() const noexcept { return segments.first; }
      iterator_t last() const noexcept { return segments.second; }

      size_t mergeBytes { 0 };
      double skew { 0.0 };
      double mergeScore { 0.0 };
      bool initialized { false };

      range_t segments;
      std::function<void(iterator_t, uint64_t&, uint64_t&, uint64_t&)> accessor_;
    };

    template<typename Segment>
    bool findBestCleanupCandidate(
      std::vector<Segment>& segments,
      const std::function<
        void(const typename std::vector<Segment>::const_iterator,
             uint64_t& /* byte_size */,
             uint64_t& /* docs_count */,
             uint64_t& /* live_docs_count */)>& getSegmentAttributes,
      tier::ConsolidationCandidate<Segment>& best) {

        auto segmentSortFunc = [](const Segment& left, const Segment& right) {

          auto lMeta = left.meta;
          auto rMeta = right.meta;
          auto lLivePerc = static_cast<double>(lMeta->live_docs_count) / lMeta->docs_count;
          auto rLivePerc = static_cast<double>(rMeta->live_docs_count) / rMeta->docs_count;

          return (lLivePerc < rLivePerc);
        };

        std::sort(segments.begin(), segments.end(), segmentSortFunc);

        auto count = 0;
        auto total_docs_count = 0;
        auto total_live_docs_count = 0;
        double livePerc;

        for (auto itr = segments.begin(); itr != segments.end(); itr++) {
          auto meta = itr->meta;
          total_docs_count += meta->docs_count;
          total_live_docs_count += meta->live_docs_count;

          livePerc = static_cast<double>(total_live_docs_count) / total_docs_count;
          if (livePerc > tier::ConsolidationConfig::maxLivePercentage)
            break;

          ++count;
        }

        if (count < 1)
          return false;

        best = ConsolidationCandidate<Segment>(segments.begin(), segments.begin() + count - 1, getSegmentAttributes);
        return true;
    }

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
      std::vector<Segment>& sorted_segments,
      size_t max_segments_bytes,
      const std::function<
        void(const typename std::vector<Segment>::const_iterator,
             uint64_t& /* byte_size */,
             uint64_t& /* docs_count */,
             uint64_t& /* live_docs_count */)>& getSegmentAttributes,
      tier::ConsolidationCandidate<Segment>& best) {

        //  sort segments in increasing order of the segment byte size
        std::sort(sorted_segments.begin(), sorted_segments.end());

        //  We start with a min. window size of 2
        //  since a window of size 1 will always
        //  give us a skew of 1.0.
        uint64_t minWindowSize { tier::ConsolidationConfig::candidate_size };
        auto front = sorted_segments.begin();
        auto rear = front + minWindowSize - 1;
        tier::ConsolidationCandidate<Segment> candidate(front, rear, getSegmentAttributes);

        double prev_score { 1.0 };

        while ((candidate.first() + 1) < sorted_segments.end()) {

          if (!best.initialized || (best.mergeScore > candidate.mergeScore && candidate.mergeBytes <= max_segments_bytes))
            best = candidate;

          if (std::distance(candidate.first(), candidate.last()) < (minWindowSize - 1)) {
            candidate.push_back();
            continue;
          }

          if (candidate.mergeScore > prev_score ||
              candidate.mergeBytes > max_segments_bytes ||
              candidate.last() == (sorted_segments.end() - 1)) {
            prev_score = candidate.mergeScore;
            candidate.pop_front();
          }
          else if (candidate.mergeScore <= prev_score && candidate.last() < (sorted_segments.end() - 1) &&
            candidate.mergeBytes <= max_segments_bytes) {
            prev_score = candidate.mergeScore;
            candidate.push_back();
          }
        }

        return (best.initialized &&
                best.mergeScore <= tier::ConsolidationConfig::maxMergeScore);
    }

    template<typename Segment>
    bool findBestConsolidationCandidate1(
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
  size_t min_segments = 50;
  // maximum allowed number of segments to consolidate at once
  size_t max_segments = 200;
  // maxinum allowed size of all consolidated segments
  size_t max_segments_bytes = size_t(8) * (1 << 30);
  // treat all smaller segments as equal for consolidation selection
  size_t floor_segment_bytes = size_t(24) * (1 << 20);
  // filter out candidates with score less than min_score
  double_t min_score = 0.;
};

ConsolidationPolicy MakePolicy(const ConsolidateTier& options);

void ReadDocumentMask(DocumentMask& docs_mask, const directory& dir,
                      const SegmentMeta& meta);

// Writes segment_meta to the supplied directory
void FlushIndexSegment(directory& dir, IndexSegment& segment);

}  // namespace irs::index_utils
