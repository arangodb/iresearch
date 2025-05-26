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

  //  Consolidation configuration constants
  struct ConsolidationConfig {
    static const size_t candidate_size; //  candidate selection window size: 4
    static const size_t tier1;          //  First tier group: default - 4 MB
    static const double maxMergeScore;  //  max score allowed for candidates consolidation.
                                        //  Skip consolidation if candidate score is greater
                                        //  than this value.
  };

  struct ConsolidationCandidate {
    using iterator_t = std::vector<SegmentStats>::const_iterator;
    using range_t = std::pair<iterator_t, iterator_t>;
  
    ConsolidationCandidate() = default;
    explicit ConsolidationCandidate(iterator_t start, iterator_t end) noexcept;
  
    iterator_t first() const noexcept { return segments.first; }
    iterator_t last() const noexcept { return segments.second; }
  
    void advance() noexcept;
  
    size_t mergeBytes { 0 };
    double skew { 0.0 };
    size_t delCount { 0 };
    double mergeScore { 0.0 };
    double cost { 0.0 };
    bool initialized { false };
  
    range_t segments;
  };
  
  size_t getConsolidationTier(size_t num);


  template<typename Type>
  using Iterator = std::vector<Type>::const_iterator;

  //  A tier is nothing but a pair of (start, end) iterator
  //  pointing to segments in a sorted array of segments. This pair
  //  denotes a range of segments that are to be considered as part
  //  of the same group.
  template<typename TierElementType>
  using Tier = std::pair<Iterator<TierElementType>, Iterator<TierElementType>>;
  
  //  mapToTiers() goes over a sorted (by size) list of segments
  //  and groups these segments into different tiers (representing different
  //  size groups). We use powers of 4 for tiers with the first tier
  //  being 0-4M followed by 4-16M, 16-64M, 64-256M and so on.
  //
  //  The first argument is a vector of segments.
  //  This function only uses the size of a segment to make the decision
  //  as to which tier a segment should be a part of. That is what brings
  //  us to the 2nd argument. GetSegmentSize is a function/lambda that takes
  //  as argument a segment and returns its size.
  //  From the perspective of the mapToTiers() function, the segment is an
  //  opaque object.
  //
  //  The return type is a vector<Tier> where Tier which in turn is a
  //  pair of iterators pointing to a range of segments that are part of
  //  a tier.
  //  Eg.
  //      Segments: 10K 20K 500K 3.5M 200M 250M 255M 300M 310M 500M 1024M
  //      Tiers:   |<---- 0-4M ----->|<-- 64-256M ->|<---- 256-1024M ---->|
  //
  template<typename Segment, typename GetSegmentSize>
  requires std::invocable<GetSegmentSize, const Segment&>
  std::vector<Tier<Segment>> mapToTiers(
    const std::vector<Segment>& sorted_segments,   //  assumed sorted in asc order
    GetSegmentSize getSize
    )
  {
    if (sorted_segments.empty())
      return {};

    size_t currentTier;
    
    auto start = sorted_segments.begin();
    currentTier = getConsolidationTier(getSize(*start));

    std::vector<Tier<Segment>> tiers;

    for (auto idx = start; idx != sorted_segments.end();) {

        if (getSize(*idx) <= currentTier) {
            idx++;
            continue;
        }

        tiers.emplace_back(start, idx - 1);

        //  The next tier may not necessarily be in the
        //  next power of 4.
        //  Consider this example,
        //     [2, 4, 6, 8, 900]
        //  While the 2, 4 fall in the 0-4 tier and 6, 8 fall
        //  in the 4-16 tier, the last segment falls in
        //  the [256-1024] tier.

        currentTier = getConsolidationTier(getSize(*idx));
        start = idx++;
    }

    if (tiers.empty() || tiers.back().second != sorted_segments.end())
      tiers.emplace_back(start, sorted_segments.end() - 1);

    return tiers;
  }

  template<typename Segment>
  bool findBestConsolidationCandidate(
    std::vector<Tier<Segment>>& tiers,
    size_t max_segment_bytes,
    tier::ConsolidationCandidate& best
  )
  {
    best.initialized = false;
  
    for (const auto& tr : tiers) {
  
      //  skip tiers where we don't have enough segments.
      if (std::distance(tr.first, tr.second) < (tier::ConsolidationConfig::candidate_size - 1))
        continue;
  
      tier::ConsolidationCandidate candidate(tr.first, tr.first + (tier::ConsolidationConfig::candidate_size - 1));
  
      while (true) {
  
        //  merge score - smaller the better
        //  Also ensure that the consolidated segment doesn't
        //  grow beyond threshold.
        if (candidate.mergeBytes <= max_segment_bytes &&
            (!best.initialized || (candidate.mergeScore < best.mergeScore)))
          best = candidate;
  
        //  Check if we've arrived at the last segment
        //  in the current tier.
        if (candidate.last() == tr.second)
          break;
  
        candidate.advance();
      }
    }
  
    return (best.initialized && best.mergeScore <= tier::ConsolidationConfig::maxMergeScore);
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
