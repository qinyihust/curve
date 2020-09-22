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

#include <butil/iobuf.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "src/fs/local_filesystem.h"
#include "src/fs/wrap_posix.h"

#include "src/common/concurrent/concurrent.h"
#include "src/fs/async_closure.h"

const int MAX_RETYR_TIME = 3;

namespace curve {
namespace fs {

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
    static std::shared_ptr<Ext4FileSystemImpl> getInstance();
    void SetPosixWrapper(std::shared_ptr<PosixWrapper> wrapper);

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
    int WriteAsync(int fd, const char *buf, uint64_t offset,
                   int length, void *done);
    int Append(int fd, const char* buf, int length) override;
    int Fallocate(int fd, int op, uint64_t offset, int length) override;
    int Fstat(int fd, struct stat* info) override;
    int Fsync(int fd) override;

 private:
    explicit Ext4FileSystemImpl(std::shared_ptr<PosixWrapper>);
    int DoRename(const string& oldPath, const string& newPath,
                 unsigned int flags) override;
    bool CheckKernelVersion();
    void BatchSubmit(std::deque<IoTask *> *batchIo);
    void IoSubmitter();
    void ReapIo();
    void ReapIoWithoutEpoll();
    void ReapIoWithEpoll();

 private:
    static std::shared_ptr<Ext4FileSystemImpl> self_;
    static std::mutex mutex_;
    std::shared_ptr<PosixWrapper> posixWrapper_;
    bool enableRenameat2_;
    bool enableEpoll_;
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
};

}  // namespace fs
}  // namespace curve

#endif  // SRC_FS_EXT4_FILESYSTEM_IMPL_H_
