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
 * Created Date: 2020-09-02
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

#ifndef  SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_H_
#define  SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_H_

#include <vector>
#include <butil/memory/ref_counted.h>
#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <braft/log_entry.h>
#include <braft/storage.h>
#include <braft/util.h>
#include "src/chunkserver/datastore/chunkfile_pool.h"

namespace curve {
namespace chunkserver {

class BAIDU_CACHELINE_ALIGNMENT CurveSegment
        : public butil::RefCountedThreadSafe<CurveSegment> {
public:
    CurveSegment(const std::string& path, const int64_t first_index,
                 int checksum_type, bool from_pool)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(true),
        _first_index(first_index), _last_index(first_index - 1),
        _checksum_type(checksum_type),
        _from_pool(from_pool)
    {}
    CurveSegment(const std::string& path, const int64_t first_index,
            const int64_t last_index, int checksum_type, bool from_pool)
        : _path(path), _bytes(0),
        _fd(-1), _is_open(false),
        _first_index(first_index), _last_index(last_index),
        _checksum_type(checksum_type),
        _from_pool(from_pool)
    {}

    struct EntryHeader;

    // create open segment
    int create();

    // load open or closed segment
    // open fd, load index, truncate uncompleted entry
    int load(braft::ConfigurationManager* configuration_manager);

    // serialize entry, and append to open segment
    int append(const braft::LogEntry* entry);

    // get entry by index
    braft::LogEntry* get(const int64_t index) const;

    // get entry's term by index
    int64_t get_term(const int64_t index) const;

    // close open segment
    int close(bool will_sync = true);

    // sync open segment
    int sync(bool will_sync);

    // unlink segment
    int unlink();

    // truncate segment to last_index_kept
    int truncate(const int64_t last_index_kept);

    bool is_open() const {
        return _is_open;
    }

    int64_t bytes() const {
        return _bytes;
    }

    int64_t first_index() const {
        return _first_index;
    }

    int64_t last_index() const {
        return _last_index.load(butil::memory_order_consume);
    }

    std::string file_name();
private:
friend class butil::RefCountedThreadSafe<CurveSegment>;
    ~CurveSegment() {
        if (_fd >= 0) {
            ::close(_fd);
            _fd = -1;
        }
    }

    struct LogMeta {
        off_t offset;
        size_t length;
        int64_t term;
    };

    int _load_entry(off_t offset, EntryHeader *head, butil::IOBuf *body, 
                    size_t size_hint) const;

    int _get_meta(int64_t index, LogMeta* meta) const;

    int _load_meta();

    int _update_meta_page();

    std::string _path;
    int64_t _bytes;
    mutable braft::raft_mutex_t _mutex;
    int _fd;
    bool _is_open;
    const int64_t _first_index;
    butil::atomic<int64_t> _last_index;
    int _checksum_type;
    std::vector<std::pair<int64_t/*offset*/, int64_t/*term*/> > _offset_and_term;
    bool _from_pool;
};

}  // namespace chunkserver
}  // namespace curve

#endif  // SRC_CHUNKSERVER_RAFTLOG_CURVE_SEGMENT_H_
