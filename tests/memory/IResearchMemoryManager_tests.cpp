////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2025 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Koushal Kawade
////////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <memory>
#include "tests_shared.hpp"
#include "utils/managed_allocator.hpp"
#include "resource_manager.hpp"

using namespace irs;

//  Wrapper class around IResearchMemoryManager.
//  We use it to track memory increase/decrease.
//  To be used mostly for debugging purposes.
struct IResearchMemoryManagerAccessor : public irs::IResearchMemoryManager {

    IResearchMemoryManagerAccessor() = default;
    virtual ~IResearchMemoryManagerAccessor() = default;

    virtual void Increase([[maybe_unused]] uint64_t value) override {

        // std::cout << "Increase: " << value << std::endl;
        IResearchMemoryManager::Increase(value);
    }

    virtual void Decrease([[maybe_unused]] uint64_t value) noexcept override {
        // std::cout << "Decrease: " << value << std::endl;
        IResearchMemoryManager::Decrease(value);
    }

    static std::shared_ptr<IResearchMemoryManagerAccessor> GetInstance() {
        if (!_accessor)
            _accessor.reset(new IResearchMemoryManagerAccessor());

        return _accessor;
    }

    static void setMemoryLimit(uint64_t limit) {
      irs::IResearchMemoryManager::_memory_limit.store(limit);
    }

private:
    static inline std::shared_ptr<IResearchMemoryManagerAccessor> _accessor;
};

//  An allocator using IResearchMemoryManagerAccessor
//  so that we can track the allocations/deallocations
//  during the test.
template<typename T>
struct ManagedTypedAllocatorTest
  : ManagedAllocator<std::allocator<T>, IResearchMemoryManagerAccessor> {
  using Base = ManagedAllocator<std::allocator<T>, IResearchMemoryManagerAccessor>;
  explicit ManagedTypedAllocatorTest()
    : Base(
        *IResearchMemoryManagerAccessor::GetInstance()
      ) {
  }
  using Base::Base;
};

TEST(IResearch_memory_limit_test, managed_allocator_smoke_test) {

    IResearchMemoryManagerAccessor::setMemoryLimit(3);
    auto memoryMgr = IResearchMemoryManagerAccessor::GetInstance();

    using type = char;
    std::vector<type, ManagedTypedAllocatorTest<type>> arr(*memoryMgr.get());

    arr.push_back('a');
    arr.push_back('b');
    ASSERT_THROW(arr.push_back('c'), std::bad_alloc);
}

TEST(IResearch_memory_limit_test, memory_manager_smoke_test) {

    //  set limit
    IResearchMemoryManagerAccessor::setMemoryLimit(47);

    //  allocate vector
    ManagedVector<uint64_t> vec;
    vec.push_back(10);  //  Allocate 8 bytes
    vec.push_back(11);  //  Allocate 16, Free previous 8, Copy both elements in the new array.

    ASSERT_THROW(vec.push_back(12), std::bad_alloc);    //  Allocate 32 while holding previous 16, total 48 (bad_alloc)

    //  Increase memory limit to accommodate the 3rd element.
    IResearchMemoryManagerAccessor::setMemoryLimit(48);

    ASSERT_NO_THROW(vec.push_back(12));
}
