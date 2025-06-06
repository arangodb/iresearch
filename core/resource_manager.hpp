////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <atomic>
#include <vector>

#include "shared.hpp"
#include "utils/managed_allocator.hpp"

namespace arangodb::iresearch
{
  class IResearchFeature;
};

namespace irs {

struct IResourceManager {
  static IResourceManager kNoop;
#ifdef IRESEARCH_DEBUG
  static IResourceManager kForbidden;
#endif

  IResourceManager() = default;
  virtual ~IResourceManager() = default;

  IResourceManager(const IResourceManager&) = delete;
  IResourceManager operator=(const IResourceManager&) = delete;

  virtual void Increase([[maybe_unused]] size_t v) {
    IRS_ASSERT(this != &kForbidden);
    IRS_ASSERT(v != 0);
  }

  virtual void Decrease([[maybe_unused]] size_t v) noexcept {
    IRS_ASSERT(this != &kForbidden);
    IRS_ASSERT(v != 0);
  }

  IRS_FORCE_INLINE void DecreaseChecked(size_t v) noexcept {
    if (v != 0) {
      Decrease(v);
    }
  }
};

struct ResourceManagementOptions {
  static ResourceManagementOptions kDefault;

  IResourceManager* transactions{&IResourceManager::kNoop};
  IResourceManager* readers{&IResourceManager::kNoop};
  IResourceManager* consolidations{&IResourceManager::kNoop};
  IResourceManager* file_descriptors{&IResourceManager::kNoop};
  IResourceManager* cached_columns{&IResourceManager::kNoop};
};

struct IResearchMemoryManager : public IResourceManager {
protected:
  IResearchMemoryManager() = default;

public:
  virtual ~IResearchMemoryManager() = default;

  virtual void Increase([[maybe_unused]] uint64_t value) override {

    IRS_ASSERT(this != &kForbidden);
    IRS_ASSERT(value >= 0);

    if (0 == IResearchMemoryManager::_memory_limit) {
      // since we have no limit, we can simply use fetch-add for the increment
      IResearchMemoryManager::_current.fetch_add(value, std::memory_order_relaxed);
    } else {
      // we only want to perform the update if we don't exceed the limit!
      std::uint64_t cur = IResearchMemoryManager::_current.load(std::memory_order_relaxed);
      std::uint64_t next;
      do {
        next = cur + value;
        if (IRS_UNLIKELY(next > IResearchMemoryManager::_memory_limit.load(std::memory_order_relaxed))) {
          throw std::bad_alloc();
        }
      } while (!IResearchMemoryManager::_current.compare_exchange_weak(
        cur, next, std::memory_order_relaxed));
    }
  }

  virtual void Decrease([[maybe_unused]] uint64_t value) noexcept override {
    IRS_ASSERT(this != &kForbidden);
    IRS_ASSERT(value >= 0);
    _current.fetch_sub(value, std::memory_order_relaxed);
  }

protected:
  //  This limit can be set only by IResearchFeature.
  //  During IResearchFeature::prepare() it is set to a pre-defined
  //  percentage of the total available physical memory or to the value
  //  of ARANGODB_OVERRIDE_DETECTED_TOTAL_MEMORY option if specified.
  static inline std::atomic<std::uint64_t> _memory_limit = { 0 };

  //  Singleton
  static inline std::shared_ptr<IResourceManager> _instance;

private:
  static inline std::atomic<std::uint64_t> _current = { 0 };

public:
  static std::shared_ptr<IResourceManager> GetInstance() {
    if (!_instance.get())
      _instance.reset(new IResearchMemoryManager());

    return _instance;
  }

  friend class arangodb::iresearch::IResearchFeature;
};

template<typename T>
struct ManagedTypedAllocator
  : ManagedAllocator<std::allocator<T>, IResourceManager> {
  using Base = ManagedAllocator<std::allocator<T>, IResourceManager>;
  explicit ManagedTypedAllocator()
    : Base(
#if !defined(_MSC_VER) && defined(IRESEARCH_DEBUG)
        IResourceManager::kForbidden
#else
        *IResearchMemoryManager::GetInstance()
#endif
      ) {
  }
  using Base::Base;
};

template<typename T>
using ManagedVector = std::vector<T, ManagedTypedAllocator<T>>;

}  // namespace irs
