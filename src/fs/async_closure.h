/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created: 2020-09-18
 * Author: qinyi
 */

#ifndef SRC_FS_ASYNC_CLOSURE_H_
#define SRC_FS_ASYNC_CLOSURE_H_

#include <brpc/closure_guard.h>
#include <iomanip>
#include <utility>
#include <tuple>
#include <array>

namespace curve {
namespace fs {

class ReqClosure : public ::google::protobuf::Closure {
public:
    int res_;
    butil::Timer timer_;
};

template<typename Function, typename argType, typename Tuple, size_t... I>
auto call(Function f, argType arg, Tuple t, std::index_sequence<I...>) {
    return f(arg, std::get<I>(t)...);
}

template<typename Function, typename argType, typename Tuple>
auto call(Function f, argType arg, Tuple t) {
    static constexpr auto size = std::tuple_size<Tuple>::value;
    return call(f, arg, t, std::make_index_sequence<size>{});
}

template<typename Func, class... Types>
class AsyncClosure : public ReqClosure {
 public:
    AsyncClosure(Func callback, Types... args)
        : callback_(callback), args_(args...){
        res_ = 0;
    }

    void Run() {
        std::unique_ptr<AsyncClosure> selfGuard(this);
        call(callback_, res_, args_);
    }

 private:
    Func callback_;
    std::tuple<Types...> args_;
};

}  // namespace chunkserver
}  // namespace curve
#endif  // SRC_FS_ASYNC_CLOSURE_H_
