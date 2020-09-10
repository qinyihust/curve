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

#include <fcntl.h>
#include <butil/fd_utility.h>
#include <butil/raw_pack.h>
#include <braft/local_storage.pb.h>
#include <braft/fsync.h>
#include "src/chunkserver/raftlog/curve_segment.h"
#include "src/chunkserver/raftlog/define.h"

DEFINE_uint32(walPageSize, 4096, "page size of wal");
DEFINE_bool(raftSyncSegments, false, "call fsync when a segment is closed");

namespace curve {
namespace chunkserver {

// Format of Header, all fields are in network order
// | -------------------- term (64bits) -------------------------  |
// | entry-type (8bits) | checksum_type (8bits) | reserved(16bits) |
// | ------------------ data len (32bits) -----------------------  |
// | --------------- data real len (32bits) ---------------------  |
// | data_checksum (32bits) | header checksum (32bits)             |

const static size_t ENTRY_HEADER_SIZE = 28;

int CurveSegment::create() {
    if (!_is_open) {
        CHECK(false) << "Create on a closed segment at first_index=" 
                     << _first_index << " in " << _path;
        return -1;
    }

    std::string path(_path);
    butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN, _first_index);
    char metaPage[FLAGS_walPageSize];
    memset(metaPage, 0, sizeof(metaPage));
    int res = ChunkfilePool::GetInstance()->GetChunk(path, metaPage);
    if (res != 0) {
        LOG(ERROR) << "Get segment from chunk file pool fail!";
        return -1;
    }
    _fd = ::open(path.c_str(), O_RDWR|O_NOATIME, 0644);
    if (_fd >= 0) {
        butil::make_close_on_exec(_fd);
    }
    res = ::lseek(_fd, FLAGS_walPageSize, SEEK_SET);
    if (res != FLAGS_walPageSize) {
        LOG(ERROR) << "lseek fail!";
        return -1;
    }
    LOG_IF(INFO, _fd >= 0) << "Created new segment `" << path 
                           << "' with fd=" << _fd ;
    _bytes += FLAGS_walPageSize;
    LOG(INFO) << "QQQ create segmetn with _bytes=" << _bytes;
    return _fd >= 0 ? 0 : -1;
}

struct CurveSegment::EntryHeader {
    int64_t term;
    int type;
    int checksum_type;
    uint32_t data_len;
    uint32_t data_real_len;
    uint32_t data_checksum;
};

std::ostream& operator<<(std::ostream& os, const CurveSegment::EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", data_real_len=" << h.data_real_len << ", checksum_type="
       << h.checksum_type << ", data_checksum=" << h.data_checksum << '}';
    return os;
}

int ftruncate_uninterrupted(int fd, off_t length) {
    int rc = 0;
    do {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

int CurveSegment::load(braft::ConfigurationManager* configuration_manager) {
    int ret = 0;

    std::string path(_path);
    if (_is_open) {
        if (_from_pool) {
            butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                              _first_index);
        } else {
            butil::string_appendf(&path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                              _first_index);
        }
    } else {
        if (_from_pool) {
            butil::string_appendf(&path, "/" CURVE_SEGMENT_CLOSED_PATTERN, 
                             _first_index, _last_index.load());
        } else {
            butil::string_appendf(&path, "/" BRAFT_SEGMENT_CLOSED_PATTERN, 
                             _first_index, _last_index.load());
        }
    }
    _fd = ::open(path.c_str(), O_RDWR);
    if (_fd < 0) {
        LOG(ERROR) << "Fail to open " << path << ", " << berror();
        return -1;
    }
    butil::make_close_on_exec(_fd);

    // get file size
    struct stat st_buf;
    if (fstat(_fd, &st_buf) != 0) {
        LOG(ERROR) << "Fail to get the stat of " << path << ", " << berror();
        ::close(_fd);
        _fd = -1;
        return -1;
    }

    // load meta page
    if (_from_pool && _load_meta() != 0) {
        LOG(ERROR) << "Load wal meta page fail";
        return -1;
    }

    // load entry index
    int64_t load_size = _from_pool ? _bytes : st_buf.st_size;
    int64_t entry_off = _from_pool ? FLAGS_walPageSize : 0;
    int64_t actual_last_index = _first_index - 1;
    for (int64_t i = _first_index; entry_off < load_size; i++) {
        EntryHeader header;
        LOG(INFO) << "QQQ load entry header at " << entry_off;
        const int rc = _load_entry(entry_off, &header, NULL, ENTRY_HEADER_SIZE);
        if (rc > 0) {
            // The last log was not completely written, which should be truncated
            break;
        }
        if (rc < 0) {
            ret = rc;
            break;
        }
        // rc == 0
        const int64_t skip_len = ENTRY_HEADER_SIZE + header.data_len;
        if (entry_off + skip_len > load_size) {
            // The last log was not completely written and it should be
            // truncated
            break;
        }
        if (header.type == braft::ENTRY_TYPE_CONFIGURATION) {
            butil::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            LOG(INFO) << "QQQ load configuration entry at " << entry_off;
            if (_load_entry(entry_off, NULL, &data, ENTRY_HEADER_SIZE + header.data_real_len) != 0) {
                break;
            }
            scoped_refptr<braft::LogEntry> entry = new braft::LogEntry();
            entry->id.index = i;
            entry->id.term = header.term;
            butil::Status status = parse_configuration_meta(data, entry);
            if (status.ok()) {
                braft::ConfigurationEntry conf_entry(*entry);
                configuration_manager->add(conf_entry); 
            } else {
                LOG(ERROR) << "fail to parse configuration meta, path: " << _path
                    << " entry_off " << entry_off;
                ret = -1;
                break;
            }
        }
        if (header.type == braft::ENTRY_TYPE_CONFIGURATION)
            LOG(INFO) << "QQQ push configuration entry on term " << header.term 
                      << " at offset " << entry_off;
        else if (header.type == braft::ENTRY_TYPE_DATA)
            LOG(INFO) << "QQQ push data entry on term " << header.term 
                      << " at offset " << entry_off;
        else if (header.type == braft::ENTRY_TYPE_NO_OP)
            LOG(INFO) << "QQQ push noop entry on term " << header.term 
                      << " at offset " << entry_off;

        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        ++actual_last_index;
        entry_off += skip_len;
    }

    const int64_t last_index = _last_index.load(butil::memory_order_relaxed);
    if (ret == 0 && !_is_open) {
        if (actual_last_index < last_index) {
            LOG(ERROR) << "data lost in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        } else if (actual_last_index > last_index) {
            // FIXME(zhengpengfei): should we ignore garbage entries silently
            LOG(ERROR) << "found garbage in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        }
    }

    if (ret != 0) {
        return ret;
    }

    if (_is_open) {
        _last_index = actual_last_index;
    }

    // truncate last uncompleted entry
    if (!_from_pool && entry_off != st_buf.st_size) {
        LOG(INFO) << "truncate last uncompleted write entry, path: " << _path
            << " first_index: " << _first_index << " old_size: " << st_buf.st_size
            << " new_size: " << entry_off;
        ret = ftruncate_uninterrupted(_fd, entry_off);
    }

    // seek to end, for opening segment
    ::lseek(_fd, entry_off, SEEK_SET);

    _bytes = entry_off;
    return ret;
}

int CurveSegment::_load_meta() {
    char metaPage[FLAGS_walPageSize];
    int res = ::pread(_fd, metaPage, FLAGS_walPageSize, 0);
    if (res != FLAGS_walPageSize) {
        return -1;
    }
    memcpy(&_bytes, metaPage, sizeof(_bytes));
    LOG(INFO) << "loaded bytes: " << _bytes;
    return 0;
}

inline bool verify_checksum(int checksum_type,
                            const char* data, size_t len, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == braft::murmurhash32(data, len));
    case CHECKSUM_CRC32:
        return (value == braft::crc32(data, len));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline bool verify_checksum(int checksum_type, 
                            const butil::IOBuf& data, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == braft::murmurhash32(data));
    case CHECKSUM_CRC32:
        return (value == braft::crc32(data));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline uint32_t get_checksum(int checksum_type, const char* data, size_t len) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return braft::murmurhash32(data, len);
    case CHECKSUM_CRC32:
        return braft::crc32(data, len);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

inline uint32_t get_checksum(int checksum_type, const butil::IOBuf& data) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return braft::murmurhash32(data);
    case CHECKSUM_CRC32:
        return braft::crc32(data);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

std::string CurveSegment::file_name() {
    if (!_is_open) {
        if (_from_pool) {
            return butil::string_printf(CURVE_SEGMENT_CLOSED_PATTERN, _first_index,
                                    _last_index.load());
        } else {
            return butil::string_printf(BRAFT_SEGMENT_CLOSED_PATTERN, _first_index,
                                    _last_index.load());
        }
    } else {
        if (_from_pool) {
            return butil::string_printf(CURVE_SEGMENT_OPEN_PATTERN, _first_index);
        } else {
            return butil::string_printf(BRAFT_SEGMENT_OPEN_PATTERN, _first_index);
        }
    }
}

int CurveSegment::_load_entry(off_t offset, EntryHeader* head,
                              butil::IOBuf* data, size_t size_hint) const {
    butil::IOPortal buf;
    size_t to_read = std::max(size_hint, ENTRY_HEADER_SIZE);
    const ssize_t n = braft::file_pread(&buf, _fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }
    char header_buf[ENTRY_HEADER_SIZE];
    const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_real_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    butil::RawUnpacker(p).unpack64((uint64_t&)term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_real_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    LOG(INFO) << "load log at offset " << offset << ": term=" << term
              << ", meta_field=" << meta_field << ", data_len=" << data_len
              << ", real_len=" << data_real_len;
    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.data_len = data_len;
    tmp.data_real_len = data_real_len;
    tmp.data_checksum = data_checksum;
    if (!verify_checksum(tmp.checksum_type, 
                        p, ENTRY_HEADER_SIZE - 4, header_checksum)) {
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << ", header=" << tmp << ", path: " << _path;
        return -1;
    }
    if (head != NULL) {
        *head = tmp;
    }
    if (data != NULL) {
        if (buf.length() < ENTRY_HEADER_SIZE + data_real_len) {
            const size_t to_read = ENTRY_HEADER_SIZE + data_real_len - buf.length();
            const ssize_t n = braft::file_pread(&buf, _fd, offset + buf.length(), to_read);
            if (n != (ssize_t)to_read) {
                return n < 0 ? -1 : 1;
            }
        } else if (buf.length() > ENTRY_HEADER_SIZE + data_real_len) {
            buf.pop_back(buf.length() - ENTRY_HEADER_SIZE - data_real_len);
        }
        CHECK_EQ(buf.length(), ENTRY_HEADER_SIZE + data_real_len);
        buf.pop_front(ENTRY_HEADER_SIZE);
        if (!verify_checksum(tmp.checksum_type, buf, tmp.data_checksum)) {
            LOG(ERROR) << "Found corrupted data at offset=" 
                       << offset + ENTRY_HEADER_SIZE
                       << " header=" << tmp
                       << " path: " << _path;
            // TODO: abort()?
            return -1;
        }
        data->swap(buf);
    }
    return 0;
}

int CurveSegment::append(const braft::LogEntry* entry) {
    if (BAIDU_UNLIKELY(!entry || !_is_open)) {
        return EINVAL;
    } else if (entry->id.index != 
                    _last_index.load(butil::memory_order_consume) + 1) {
        CHECK(false) << "entry->index=" << entry->id.index
                  << " _last_index=" << _last_index
                  << " _first_index=" << _first_index;
        return ERANGE;
    }
    butil::IOBuf data;
    switch (entry->type) {
    case braft::ENTRY_TYPE_DATA:
        data.append(entry->data);
        break;
    case braft::ENTRY_TYPE_NO_OP:
        break;
    case braft::ENTRY_TYPE_CONFIGURATION: 
        {
            butil::Status status = serialize_configuration_meta(entry, data);
            if (!status.ok()) {
                LOG(ERROR) << "Fail to serialize ConfigurationPBMeta, path: " 
                           << _path;
                return -1; 
            }
        }
        break;
    default:
        LOG(FATAL) << "unknow entry type: " << entry->type
                   << ", path: " << _path;
        return -1;
    }
    uint32_t data_check_sum = get_checksum(_checksum_type, data);
    uint32_t real_length = data.length();
    size_t to_write = ENTRY_HEADER_SIZE + data.length();
    uint32_t zero_bytes_num = 0;
    if (to_write % kPageSize != 0) {
        zero_bytes_num = (to_write / kPageSize + 1) * kPageSize - to_write;
    }
    data.resize(data.length() + zero_bytes_num);
    to_write = ENTRY_HEADER_SIZE + data.length();
    CHECK_LE(data.length(), 1ul << 56ul);
    char header_buf[ENTRY_HEADER_SIZE];
    const uint32_t meta_field = (entry->type << 24 ) | (_checksum_type << 16);
    butil::RawPacker packer(header_buf);
    packer.pack64(entry->id.term)
          .pack32(meta_field)
          .pack32((uint32_t)data.length())
          .pack32(real_length)
          .pack32(data_check_sum);
    packer.pack32(get_checksum(
                  _checksum_type, header_buf, ENTRY_HEADER_SIZE - 4));
    butil::IOBuf header;
    header.append(header_buf, ENTRY_HEADER_SIZE);
    // 4KB alignment
    butil::IOBuf* pieces[2] = { &header, &data };
    size_t start = 0;
    ssize_t written = 0;
    butil::Timer timer;
    timer.start();
    LOG(INFO) << "QQQ write entry at offset " << _bytes << "+" << to_write;
    while (written < (ssize_t)to_write) {
        butil::Timer timer2;
        timer2.start();
        const ssize_t n = butil::IOBuf::cut_multiple_into_file_descriptor(
                _fd, pieces + start, ARRAY_SIZE(pieces) - start);
        timer2.stop();
        if (n < 0) {
            LOG(ERROR) << "Fail to write to fd=" << _fd 
                       << ", path: " << _path << berror();
            return -1;
        }
        written += n;

        for (;start < ARRAY_SIZE(pieces) && pieces[start]->empty(); ++start) {}
    }
    timer.stop();
    {
        BAIDU_SCOPED_LOCK(_mutex);
        LOG(INFO) << "QQQ push term " << entry->id.term << " at offset " << _bytes;
        _offset_and_term.push_back(std::make_pair(_bytes, entry->id.term));
        _last_index.fetch_add(1, butil::memory_order_relaxed);
        _bytes += to_write;
    }
    timer.start();
    int res = _update_meta_page();
    timer.stop();
    return res;
}

int CurveSegment::_update_meta_page() {
    char metaPage[FLAGS_walPageSize];
    memset(metaPage, 0, sizeof(metaPage));
    memcpy(metaPage, &_bytes, sizeof(_bytes));
    int res = ::pwrite(_fd, metaPage, FLAGS_walPageSize, 0);
    if (res != FLAGS_walPageSize) {
        LOG(ERROR) << "Fail to write meta page into fd=" << _fd
                   << ", path: " << _path << berror();
        return -1;
    }
    return 0;
}

braft::LogEntry* CurveSegment::get(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return NULL;
    }

    bool ok = true;
    braft::LogEntry* entry = NULL;
    do {
        braft::ConfigurationPBMeta configuration_meta;
        EntryHeader header;
        butil::IOBuf data;
        if (_load_entry(meta.offset, &header, &data, 
                        meta.length) != 0) {
            ok = false;
            break;
        }
        CHECK_EQ(meta.term, header.term);
        entry = new braft::LogEntry();
        entry->AddRef();
        switch (header.type) {
        case braft::ENTRY_TYPE_DATA:
            entry->data.swap(data);
            break;
        case braft::ENTRY_TYPE_NO_OP:
            CHECK(data.empty()) << "Data of NO_OP must be empty";
            break;
        case braft::ENTRY_TYPE_CONFIGURATION:
            {
                butil::Status status = parse_configuration_meta(data, entry); 
                if (!status.ok()) {
                    LOG(WARNING) << "Fail to parse ConfigurationPBMeta, path: "
                                 << _path;
                    ok = false;
                    break;
                }
            }
            break;
        default:
            CHECK(false) << "Unknown entry type, path: " << _path;
            break;
        }

        if (!ok) { 
            break;
        }
        entry->id.index = index;
        entry->id.term = header.term;
        entry->type = (braft::EntryType)header.type;
    } while (0);

    if (!ok && entry != NULL) {
        entry->Release();
        entry = NULL;
    }
    return entry;
}

int CurveSegment::_get_meta(int64_t index, LogMeta* meta) const {
    BAIDU_SCOPED_LOCK(_mutex);
    if (index > _last_index.load(butil::memory_order_relaxed) 
                    || index < _first_index) {
        // out of range
        BRAFT_VLOG << "_last_index=" << _last_index.load(butil::memory_order_relaxed)
                  << " _first_index=" << _first_index;
        return -1;
    } else if (_last_index == _first_index - 1) {
        BRAFT_VLOG << "_last_index=" << _last_index.load(butil::memory_order_relaxed)
                  << " _first_index=" << _first_index;
        // empty
        return -1;
    }
    int64_t meta_index = index - _first_index;
    int64_t entry_cursor = _offset_and_term[meta_index].first;
    int64_t next_cursor = (index < _last_index.load(butil::memory_order_relaxed))
                          ? _offset_and_term[meta_index + 1].first : _bytes;
    DCHECK_LT(entry_cursor, next_cursor);
    meta->offset = entry_cursor;
    meta->term = _offset_and_term[meta_index].second;
    meta->length = next_cursor - entry_cursor;
    return 0;
}

int64_t CurveSegment::get_term(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return 0;
    }
    return meta.term;
}

int CurveSegment::close(bool will_sync) {
    CHECK(_is_open);
    
    std::string old_path(_path);
    if (_from_pool) {
        butil::string_appendf(&old_path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                         _first_index);
    } else {
        butil::string_appendf(&old_path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                         _first_index);
    }
    std::string new_path(_path);
    if (_from_pool) {
        butil::string_appendf(&new_path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                         _first_index, _last_index.load());
    } else {
        butil::string_appendf(&new_path, "/" BRAFT_SEGMENT_CLOSED_PATTERN, 
                         _first_index, _last_index.load());
    }

    // TODO: optimize index memory usage by reconstruct vector
    LOG(INFO) << "close a full segment. Current first_index: " << _first_index 
              << " last_index: " << _last_index 
              << " raft_sync_segments: " << FLAGS_raftSyncSegments
              << " will_sync: " << will_sync 
              << " path: " << new_path;
    int ret = 0;
    if (_last_index > _first_index) {
        if (FLAGS_raftSyncSegments && will_sync) {
            ret = braft::raft_fsync(_fd);
        }
    }
    if (ret == 0) {
        _is_open = false;
        const int rc = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, rc == 0) << "Renamed `" << old_path
                              << "' to `" << new_path <<'\'';
        LOG_IF(ERROR, rc != 0) << "Fail to rename `" << old_path
                               << "' to `" << new_path <<"\', "
                               << berror();
        return rc;
    }
    return ret;
}

int CurveSegment::sync(bool will_sync) {
    if (_last_index > _first_index) {
        //CHECK(_is_open);
        if (braft::FLAGS_raft_sync && will_sync) {
            return braft::raft_fsync(_fd);
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}

static void* run_unlink(void* arg) {
    std::string* file_path = (std::string*) arg;
    butil::Timer timer;
    timer.start();
    int ret = ::unlink(file_path->c_str());
    timer.stop();
    BRAFT_VLOG << "unlink " << *file_path << " ret " << ret << " time: " << timer.u_elapsed();
    delete file_path;

    return NULL;
}

int CurveSegment::unlink() {
    int ret = 0;
    do {
        std::string path(_path);
        if (_from_pool) {
            if (_is_open) {
                butil::string_appendf(&path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                                 _first_index);
            } else {
                butil::string_appendf(&path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                                _first_index, _last_index.load());
            }
            int res = ChunkfilePool::GetInstance()->RecycleChunk(path);
            if (res != 0) {
                LOG(ERROR) << "Return segment to chunk file pool fail!";
                return -1;
            }
            return 0;
        }

        if (_is_open) {
            butil::string_appendf(&path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                                 _first_index);
        } else {
            butil::string_appendf(&path, "/" BRAFT_SEGMENT_CLOSED_PATTERN,
                                _first_index, _last_index.load());
        }
        std::string tmp_path(path);
        tmp_path.append(".tmp");
        ret = ::rename(path.c_str(), tmp_path.c_str());
        if (ret != 0) {
            PLOG(ERROR) << "Fail to rename " << path << " to " << tmp_path;
            break;
        }

        // start bthread to unlink
        // TODO unlink follow control
        std::string* file_path = new std::string(tmp_path);
        bthread_t tid;
        if (bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, run_unlink, file_path) != 0) {
            run_unlink(file_path);
        }

        LOG(INFO) << "Unlinked segment `" << path << '\'';
    } while (0);

    return ret;
}

int CurveSegment::truncate(const int64_t last_index_kept) {
    int64_t truncate_size = 0;
    int64_t first_truncate_in_offset = 0;
    std::unique_lock<braft::raft_mutex_t> lck(_mutex);
    if (last_index_kept >= _last_index) {
        return 0;
    }
    first_truncate_in_offset = last_index_kept + 1 - _first_index;
    truncate_size = _offset_and_term[first_truncate_in_offset].first;
    BRAFT_VLOG << "Truncating " << _path << " first_index: " << _first_index
              << " last_index from " << _last_index << " to " << last_index_kept
              << " truncate size to " << truncate_size;
    lck.unlock();

    // Truncate on a full segment need to rename back to inprogess segment again,
    // because the node may crash before truncate.
    if (!_is_open) {
        std::string old_path(_path);
        if (_from_pool) {
            butil::string_appendf(&old_path, "/" CURVE_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());
        } else {
            butil::string_appendf(&old_path, "/" BRAFT_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());
        }
        std::string new_path(_path);
        if (_from_pool) {
            butil::string_appendf(&new_path, "/" CURVE_SEGMENT_OPEN_PATTERN,
                             _first_index);
        } else {
            butil::string_appendf(&new_path, "/" BRAFT_SEGMENT_OPEN_PATTERN,
                             _first_index);
        }
        int ret = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, ret == 0) << "Renamed `" << old_path << "' to `"
                               << new_path << '\'';
        LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                << new_path << "', " << berror();
        if (ret != 0) {
            return ret;
        }
        _is_open = true;
    }

    if (_from_pool) {
        _bytes = truncate_size;
        int ret = _update_meta_page();
        if (ret < 0) {
            return ret;
        }
    } else {
        // truncate fd
        int ret = ftruncate_uninterrupted(_fd, truncate_size);
        if (ret < 0) {
            return ret;
        }
    }

    // seek fd
    off_t ret_off = ::lseek(_fd, truncate_size, SEEK_SET);
    if (ret_off < 0) {
        PLOG(ERROR) << "Fail to lseek fd=" << _fd << " to size=" << truncate_size
                    << " path: " << _path;
        return -1;
    }

    lck.lock();
    // update memory var
    _offset_and_term.resize(first_truncate_in_offset);
    _last_index.store(last_index_kept, butil::memory_order_relaxed);
    _bytes = truncate_size;
    return 0;
}

}  // namespace chunkserver
}  // namespace curve
