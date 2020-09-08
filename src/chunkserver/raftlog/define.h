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
 * Created Date: 2020-09-03
 * Author: charisu
 */

// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

namespace curve {
namespace chunkserver {

#define CURVE_SEGMENT_OPEN_PATTERN "curve_log_inprogress_%020" PRId64
#define CURVE_SEGMENT_CLOSED_PATTERN "curve_log_%020" PRId64 "_%020" PRId64
#define BRAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020" PRId64
#define BRAFT_SEGMENT_CLOSED_PATTERN "log_%020" PRId64 "_%020" PRId64
#define BRAFT_SEGMENT_META_FILE  "log_meta"

enum CheckSumType {
    CHECKSUM_MURMURHASH32 = 0,
    CHECKSUM_CRC32 = 1,   
};

const uint32_t kPageSize = 4096;

}  // namespace chunkserver
}  // namespace curve
