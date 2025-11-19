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

    operator const irs::SubReader*() const noexcept { return reader; }
  
    const irs::SubReader* reader;
    const irs::SegmentInfo* meta;
    size_t size;  // approximate size of segment without removals
    double_t fill_factor; //  live_docs_count / docs_count - meaning that fill_factor is
                          //  inversely proportional to the no. of deletions.
  };

  struct ConsolidationConfig {
    static constexpr size_t min_merge_window_size { 2 };   //  min window size for segments merge 
    static constexpr double maxMergeScore { 0.4 };  //  max score allowed for candidates consolidation.
                                                    //  Skip consolidation if candidate score is greater
                                                    //  than this value.
    static constexpr double maxLivePercentage { 0.5 };  //  Max live docs % of a segment to consider it
                                                        //  for cleanup during consolidation.
  };

  struct SegmentAttributes {
    uint64_t byteSize { 0 };
    uint64_t docsCount { 0 };
    uint64_t liveDocsCount { 0 };

    SegmentAttributes() = default;
    SegmentAttributes(uint64_t b, uint64_t d, uint64_t l) :
      byteSize(b), docsCount(d), liveDocsCount(l) {}
  };

  //  interface to fetch the required attributes from
  //  SegmentStats struct.
  //  We use this function in struct ConsolidationCandidate
  //  to fetch the segment dimensions from the SegmentStats
  //  struct.
  //
  void getSegmentDimensions(
      const tier::SegmentStats& segment,
      tier::SegmentAttributes& attrs);

  template<typename Segment>
  struct ConsolidationCandidate {
    using SegmentIterator = typename std::vector<Segment>::const_iterator;
    using range_t = typename std::pair<SegmentIterator, SegmentIterator>;

    ConsolidationCandidate() = default;

    ConsolidationCandidate(
      SegmentIterator start,
      SegmentIterator end,
      std::function<void(
          const Segment&,
          SegmentAttributes&
      )> accessor
    ) noexcept : segments(start, end), accessor_(accessor) {

      initialized = true;

      //  Calculate initial cost
      SegmentAttributes attrs;

      auto itr = start;
      do
      {
        accessor_(*itr, attrs);
        mergeBytes += attrs.byteSize;

      } while (itr++ != end);

      skew = static_cast<double>(attrs.byteSize) / mergeBytes;
      mergeScore = skew;
    }

    //  It is the caller's responsibility to ensure that
    //  a std::advance() operation is possible on the segments
    //  range.
    bool pop_front() {
      if (!initialized)
        return false;

      const auto removeSegment = first();
      const auto lastSegment = last();

      std::advance(segments.first, 1);

      //  Segment to be removed
      SegmentAttributes remSegAttrs;
      accessor_(*removeSegment, remSegAttrs);

      SegmentAttributes lastSegAttrs;
      accessor_(*lastSegment, lastSegAttrs);

      mergeBytes -= remSegAttrs.byteSize;
      skew = static_cast<double>(lastSegAttrs.byteSize) / mergeBytes;
      mergeScore = skew;

      return true;
    }

    //  It is the caller's responsibility to ensure that
    //  a std::advance() operation is possible on the segments
    //  range.
    bool push_back() noexcept {
      if (!initialized)
        return false;

      const auto addSegment = segments.second + 1;

      std::advance(segments.second, 1);

      //  Segment to be added
      SegmentAttributes attrs;
      accessor_(*addSegment, attrs);

      mergeBytes += attrs.byteSize;
      skew = static_cast<double>(attrs.byteSize) / mergeBytes;
      mergeScore = skew;

      return true;
    }

    SegmentIterator first() const noexcept { return segments.first; }
    SegmentIterator last() const noexcept { return segments.second; }

    size_t mergeBytes { 0 };
    double skew { 0.0 };
    double mergeScore { 0.0 };
    bool initialized { false };

    range_t segments;
    std::function<void(const Segment&, SegmentAttributes&)> accessor_;
  };

  template<typename Segment>
  bool findBestCleanupCandidate(
    std::vector<Segment>& segments,
    const std::function<
      void(const Segment&,
            tier::SegmentAttributes&
          )>& getSegmentAttributes,
    tier::ConsolidationCandidate<Segment>& best) {

      if (segments.empty())
        return false;

      auto segmentSortFunc = [&](const Segment& left, const Segment& right) {

        tier::SegmentAttributes attrs;
        getSegmentAttributes(left, attrs);
        double lLivePerc;
        lLivePerc = (0 == attrs.docsCount ? 0 : static_cast<double>(attrs.liveDocsCount) / attrs.docsCount);

        getSegmentAttributes(right, attrs);
        auto rLivePerc = (0 == attrs.docsCount ? 0 : static_cast<double>(attrs.liveDocsCount) / attrs.docsCount);

        return lLivePerc < rLivePerc;
      };

      std::sort(segments.begin(), segments.end(), segmentSortFunc);

      uint32_t count = 0;
      uint64_t totalDocsCount = 0;
      uint64_t totalLiveDocsCount = 0;
      double livePerc;

      for (auto itr = segments.begin(); itr != segments.end(); itr++) {

        tier::SegmentAttributes attrs;
        getSegmentAttributes(*itr, attrs);

        totalDocsCount += attrs.docsCount;
        totalLiveDocsCount += attrs.liveDocsCount;

        livePerc = static_cast<double>(totalLiveDocsCount) / totalDocsCount;
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
    //  This function receives a set of segments and finds
    //  the best subset to merge together.
    //  The best subset is defined as the one with the lowest
    //  merge cost (i.e. skew). The merge cost is computed inside
    //  the ConslidationCandidate struct upon candidate init,
    //  push_back() and pop_front() operations.
    //
    //  findBestConsolidationCandidate sorts the set of segments
    //  in the increasing order of the segment sizes and then finds
    //  the largest possible subset of segments whose consolidated
    //  size is within the maxSegmentsBytes range and has the
    //  lowest skew.
    //
    //  Currently it is only executed with struct tier::SegmentStats
    //  as the template argument in ArangoSearch. However we leverage
    //  this templatized design for writing unit tests.
    //
    //  findBestConsolidationCandidate does not use the live %
    //  to find the best candidate. It only needs the segment
    //  byte size.
    //
    template<typename Segment>
    bool findBestConsolidationCandidate(
      std::vector<Segment>& segments,
      size_t maxSegmentsBytes,
      const std::function<
        void(const Segment&,
             SegmentAttributes&
            )>& getSegmentAttributes,
      tier::ConsolidationCandidate<Segment>& best) {

        if (segments.size() < tier::ConsolidationConfig::min_merge_window_size)
          return false;

        //  sort segments by segment size
        auto comp = [&](const Segment& lhs, const Segment& rhs) {

          SegmentAttributes lAttrs;
          SegmentAttributes rAttrs;

          getSegmentAttributes(lhs, lAttrs);
          getSegmentAttributes(rhs, rAttrs);

          if (lAttrs.byteSize == rAttrs.byteSize && lAttrs.docsCount > 0 && rAttrs.docsCount > 0) {

            double lfill_factor = static_cast<double>(lAttrs.liveDocsCount) / lAttrs.docsCount;
            double rfill_factor = static_cast<double>(rAttrs.liveDocsCount) / rAttrs.docsCount;
            return lfill_factor > rfill_factor;
          }

          return lAttrs.byteSize < rAttrs.byteSize;
        };
  
        //  sort segments in increasing order of the segment byte size
        std::sort(segments.begin(), segments.end(), comp);

        //  We start with a min. window size of 2
        //  since a window of size 1 will always
        //  give us a skew of 1.0.
        uint64_t minWindowSize { tier::ConsolidationConfig::min_merge_window_size };
        auto front = segments.begin();
        auto rear = front + minWindowSize - 1;
        tier::ConsolidationCandidate<Segment> candidate(front, rear, getSegmentAttributes);

        //  Algorithm:
        //  We start by setting the smallest possible window on the list of
        //  sorted segments. We move the right end ahead to add more segments to
        //  the window and we incrementally compute the merge cost for each subset.
        //  We move the left end ahead to remove segments from the window and we
        //  only do this when we're over the maxSegmentsBytes limit.
        while ((candidate.first() + minWindowSize - 1) <= candidate.last() &&
                candidate.last() < segments.end()) {

          if (candidate.mergeBytes > maxSegmentsBytes) {
            candidate.pop_front();
            continue;
          }

          if (candidate.mergeScore <= tier::ConsolidationConfig::maxMergeScore &&
              (!best.initialized || best.mergeScore > candidate.mergeScore))
            best = candidate;

          if (candidate.last() == (segments.end() - 1))
            break;

          candidate.push_back();
        }

        return best.initialized;
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

//  [TODO] Currently unused as the new algorithm uses a different
//  approach. Only max_segments_bytes is in use.
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
