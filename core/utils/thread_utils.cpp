////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include "thread_utils.hpp"

#include <cstring>

#if defined(__linux__)

#include <sys/prctl.h>

#include <memory>

namespace irs {

// posix restricts name length to 16 bytes
constexpr size_t MAX_THREAD_NAME_LENGTH = 16;

bool set_thread_name(const thread_name_t name) noexcept {
  return 0 == prctl(PR_SET_NAME, name, 0, 0, 0);
}

bool get_thread_name(
  irs::basic_string<std::remove_pointer_t<thread_name_t>>& name) {
  name.resize(MAX_THREAD_NAME_LENGTH, 0);
  if (0 == prctl(PR_GET_NAME, const_cast<char*>(name.data()), 0, 0, 0)) {
    name.resize(std::strlen(name.c_str()));
    return true;
  }
  return false;
}

}  // namespace irs

#elif defined(_WIN32)

#include <windows.h>

#include <absl/base/call_once.h>

namespace {

using name_set_ptr = HRESULT(WINAPI*)(HANDLE, PCWSTR);

using name_get_ptr = HRESULT(WINAPI*)(HANDLE, PWSTR*);

HRESULT WINAPI set_thread_description_noop(HANDLE, PCWSTR) { return E_NOTIMPL; }

HRESULT WINAPI get_thread_description_noop(HANDLE, PWSTR*) { return E_NOTIMPL; }

ABSL_CONST_INIT absl::once_flag flag;

/// @brief tries to find out if the current host supports thread namings
void init_thread_name_api() {
  absl::call_once(flag, [] {
    auto kernel32 = LoadLibraryW(L"KERNEL32.DLL");
    if (kernel32) {
      get_thread_description = reinterpret_cast<name_get_ptr>(
        GetProcAddress(kernel32, "GetThreadDescription"));
      set_thread_description = reinterpret_cast<name_set_ptr>(
        GetProcAddress(kernel32, "SetThreadDescription"));
    }
    if (!get_thread_description) {
      get_thread_description = get_thread_description_noop;
    }
    if (!set_thread_description) {
      set_thread_description = set_thread_description_noop;
    }
  });
}

}  // namespace

namespace irs {

struct local_deleter {
  void operator()(void* p) const noexcept { LocalFree(p); }
};

bool set_thread_name(const thread_name_t name) noexcept {
  init_thread_name_api();
  return SUCCEEDED(set_thread_description.load()(GetCurrentThread(), name));
}

bool get_thread_name(
  irs::basic_string<std::remove_pointer_t<thread_name_t>>& name) {
  init_thread_name_api();
  std::unique_ptr<void, local_deleter> guard;
  thread_name_t tmp;
  const auto res = get_thread_description.load()(GetCurrentThread(), &tmp);
  guard.reset(tmp);
  if (SUCCEEDED(res)) {
    name = tmp;
    return true;
  }
  return false;
}

}  // namespace irs

#elif defined(__APPLE__)

#include <pthread.h>
#include <sys/proc_info.h>

namespace irs {

// OSX as of 10.6 restricts name length to 64 bytes
constexpr size_t MAX_THREAD_NAME_LENGTH = 64;

bool set_thread_name(const thread_name_t name) noexcept {
  return 0 == pthread_setname_np(name);
}

bool get_thread_name(
  irs::basic_string<std::remove_pointer_t<thread_name_t>>& name) {
  name.resize(MAX_THREAD_NAME_LENGTH, 0);
  if (0 == pthread_getname_np(pthread_self(), const_cast<char*>(name.data()),
                              name.size())) {
    name.resize(std::strlen(name.c_str()));
    return true;
  }
  return false;
}

}  // namespace irs

#else

namespace irs {

bool set_thread_name(const thread_name_t name) noexcept { return false; }

bool get_thread_name(irs::basic_string<std::remove_pointer_t<thread_name_t>>&) {
  return false;
}

}  // namespace irs

#endif
