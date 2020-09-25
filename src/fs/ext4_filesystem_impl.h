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
 * File Created: 18-10-31
 * Author: yangyaokai
 */

#ifndef SRC_FS_EXT4_FILESYSTEM_IMPL_H_
#define SRC_FS_EXT4_FILESYSTEM_IMPL_H_

#include <sys/eventfd.h>
#include <sys/epoll.h>
#include <bvar/bvar.h>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <thread>   //NOLINT
#include "bthread/butex.h"
#include "src/fs/local_filesystem.h"
#include "src/fs/wrap_posix.h"
#include "src/common/concurrent/concurrent.h"
#include "src/fs/async_closure.h"

const int MAX_RETYR_TIME = 3;

namespace curve {
namespace fs {
struct CoRoutineContext {
    butil::atomic<int>* waiter;
    volatile int64_t res;
    volatile int64_t res2;
    // common::Atomic<uint64_t> waitStartTime;
    uint64_t butexWakeTime;
    bool isWrite;
};

class Ext4FileSystemMetric {
 public:
    const std::string prefix = "curve lfs";
    bvar::LatencyRecorder totalWriteLatancy;
    bvar::LatencyRecorder totalReadLatancy;
    bvar::LatencyRecorder writePrepLatancy;
    bvar::LatencyRecorder writeSetEventfdLatancy;
    bvar::LatencyRecorder writeButexCreate;
    bvar::LatencyRecorder writeIosubmitLatancy;
    bvar::LatencyRecorder writeButexWaitLatancy;
    bvar::LatencyRecorder writeButexDestroyLatancy;
    bvar::LatencyRecorder writeFinishIoLatancy;
    bvar::LatencyRecorder writeCtxSwichLatancy;
    bvar::LatencyRecorder readPrepLatancy;
    bvar::LatencyRecorder readSetEventfdLatancy;
    bvar::LatencyRecorder readButexCreate;
    bvar::LatencyRecorder readIosubmitLatancy;
    bvar::LatencyRecorder readButexWaitLatancy;
    bvar::LatencyRecorder readButexDestroyLatancy;
    bvar::LatencyRecorder readFinishIoLatancy;
    bvar::LatencyRecorder readCtxSwichLatancy;
    bvar::LatencyRecorder getEventRecordCount;
    bvar::LatencyRecorder getEventLatancy;
    bvar::LatencyRecorder butexWakeLatancy;
    Ext4FileSystemMetric()
        : totalWriteLatancy(prefix, "totalWriteLatancy"),
          totalReadLatancy(prefix, "totalReadLatancy"),
          writePrepLatancy(prefix, "writePrepLatancy"),
          writeSetEventfdLatancy(prefix, "writeSetEventfdLatancy"),
          writeButexCreate(prefix, "writeButexCreate"),
          writeIosubmitLatancy(prefix, "writeIosubmitLatancy"),
          writeButexWaitLatancy(prefix, "writeButexWaitLatancy"),
          writeButexDestroyLatancy(prefix, "writeButexDestroyLatancy"),
          writeFinishIoLatancy(prefix, "writeFinishIoLatancy"),
          writeCtxSwichLatancy(prefix, "writeCtxSwichLatancy"),
          readPrepLatancy(prefix, "readPrepLatancy"),
          readSetEventfdLatancy(prefix, "readSetEventfdLatancy"),
          readButexCreate(prefix, "readButexCreate"),
          readIosubmitLatancy(prefix, "readIosubmitLatancy"),
          readButexWaitLatancy(prefix, "readButexWaitLatancy"),
          readButexDestroyLatancy(prefix, "readButexDestroyLatancy"),
          readFinishIoLatancy(prefix, "readFinishIoLatancy"),
          readCtxSwichLatancy(prefix, "readCtxSwichLatancy"),
          getEventRecordCount(prefix, "getEventRecordCount"),
          getEventLatancy(prefix, "getEventLatancy"),
          butexWakeLatancy(prefix, "butexWakeLatancy") {}
    void PrintMetric();

 private:
    void PrintOneMetric(bvar::LatencyRecorder& record);
};

class IoTask {
 public:
    IoTask(int fd, bool isRead, char* buf, off_t offset,
           size_t length, void* done)
        : fd_(fd), isRead_(isRead), buf_(buf),
          offset_(offset), length_(length), done_(done){};

    int fd_;
    bool isRead_;
    char* buf_;
    off_t offset_;
    size_t length_;
    void* done_;
};
class Ext4FileSystemImpl : public LocalFileSystem {
 public:
    virtual ~Ext4FileSystemImpl();
    static Ext4FileSystemImpl* getInstance();
    void SetPosixWrapper(PosixWrapper* wrapper);

    int Init(const LocalFileSystemOption& option) override;
    int Uninit() override;
    int Statfs(const string& path, struct FileSystemInfo* info) override;
    int Open(const string& path, int flags) override;
    int Close(int fd) override;
    int Delete(const string& path) override;
    int Mkdir(const string& dirPath) override;
    bool DirExists(const string& dirPath) override;
    bool FileExists(const string& filePath) override;
    int List(const string& dirPath, vector<std::string>* names) override;
    int Read(int fd, char* buf, uint64_t offset, int length) override;
    int Write(int fd, const char *buf, uint64_t offset, int length) override;
    int Append(int fd, const char* buf, int length) override;
    int Fallocate(int fd, int op, uint64_t offset, int length) override;
    int Fstat(int fd, struct stat* info) override;
    int Fsync(int fd) override;
    int WriteAsync(int fd, const char *buf, uint64_t offset,
                   int length, void *done);
 private:
    explicit Ext4FileSystemImpl(PosixWrapper *);
    int DoRename(const string& oldPath, const string& newPath,
                 unsigned int flags) override;
    bool CheckKernelVersion();
    void BatchSubmit(std::deque<IoTask *> *batchIo);
    void IoSubmitter();
    void ReapIo();
    void ReapIoWithoutEpoll();
    void ReapIoWithEpoll();
    int ReadCoroutine_(int fd, char* buf, uint64_t offset, int length);
    int WriteCoroutine_(int fd, const char* buf, uint64_t offset, int length);
    int ReadPread_(int fd, char* buf, uint64_t offset, int length);
    int WritePwrite_(int fd, const char* buf, uint64_t offset, int length);

 private:
    static Ext4FileSystemImpl* self_;
    static std::mutex mutex_;
    PosixWrapper* posixWrapper_;
    bool enableRenameat2_;
    bool enableCoroutine_;
    bool enableAio_;
    bool enableEpool_;
    int maxEvents_;
    io_context_t ctx_;
    std::thread th_;
    std::thread submitTh_;
    std::mutex submitMutex_;
    std::deque<IoTask*> tasks_;
    std::atomic<int> nrFlyingIo_;
    bool stop_;
    int efd_;
    int epfd_;
    struct epoll_event epevent_;
    Ext4FileSystemMetric metric_;
};

}  // namespace fs
}  // namespace curve

#endif  // SRC_FS_EXT4_FILESYSTEM_IMPL_H_
