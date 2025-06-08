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

TEST(IResearchMemoryLimitTest, managed_allocator_smoke_test) {

    auto memoryMgr = IResearchMemoryManager::GetInstance();
    memoryMgr->SetMemoryLimit(3);

    using type = char;
    std::vector<type, ManagedTypedAllocator<type>> arr(*memoryMgr);

    arr.push_back('a');
    arr.push_back('b');
    ASSERT_THROW(arr.push_back('c'), std::bad_alloc);
}

TEST(IResearchMemoryLimitTest, memory_manager_smoke_test) {

    //  set limit
    auto memoryMgr = IResearchMemoryManager::GetInstance();
    memoryMgr->SetMemoryLimit(47);

    //  allocate vector
    ManagedVector<uint64_t> vec;
    vec.push_back(10);  //  Allocate 8 bytes
    vec.push_back(11);  //  Allocate 16, Free previous 8, Copy both elements in the new array.

    ASSERT_THROW(vec.push_back(12), std::bad_alloc);    //  Allocate 32 while holding previous 16, total 48 (bad_alloc)

    //  Increase memory limit to accommodate the 3rd element.
    memoryMgr->SetMemoryLimit(48);

    ASSERT_NO_THROW(vec.push_back(12));
}
