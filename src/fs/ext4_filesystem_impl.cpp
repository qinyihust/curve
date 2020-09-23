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

#include <glog/logging.h>
#include <sys/vfs.h>
#include <sys/utsname.h>
#include <linux/version.h>
#include <dirent.h>
#include <brpc/server.h>
#include <sys/eventfd.h>

#include "src/common/string_util.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "src/fs/wrap_posix.h"

#define MIN_KERNEL_VERSION KERNEL_VERSION(3, 15, 0)
static bvar::LatencyRecorder g_fs_write_latency(
		                                   "fs_write");

namespace curve {
namespace fs {

std::shared_ptr<Ext4FileSystemImpl> Ext4FileSystemImpl::self_ = nullptr;
std::mutex Ext4FileSystemImpl::mutex_;

Ext4FileSystemImpl::Ext4FileSystemImpl(
    std::shared_ptr<PosixWrapper> posixWrapper)
    : posixWrapper_(posixWrapper)
    , enableRenameat2_(false) {
    CHECK(posixWrapper_ != nullptr) << "PosixWrapper is null";
}

Ext4FileSystemImpl::~Ext4FileSystemImpl() {
}

std::shared_ptr<Ext4FileSystemImpl> Ext4FileSystemImpl::getInstance() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (self_ == nullptr) {
        std::shared_ptr<PosixWrapper> wrapper =
            std::make_shared<PosixWrapper>();
        self_ = std::shared_ptr<Ext4FileSystemImpl>(
                new(std::nothrow) Ext4FileSystemImpl(wrapper));
        CHECK(self_ != nullptr) << "Failed to new ext4 local fs.";
    }
    return self_;
}

void Ext4FileSystemImpl::SetPosixWrapper(std::shared_ptr<PosixWrapper> wrapper) {  //NOLINT
    CHECK(wrapper != nullptr) << "PosixWrapper is null";
    posixWrapper_ = wrapper;
}

bool Ext4FileSystemImpl::CheckKernelVersion() {
    struct utsname kernel_info;
    int ret = 0;

    ret = posixWrapper_->uname(&kernel_info);
    if (ret != 0) {
         LOG(ERROR) << "Get kernel info failed.";
         return false;
    }

    LOG(INFO) << "Kernel version: " << kernel_info.release;
    LOG(INFO) << "System version: " << kernel_info.version;
    LOG(INFO) << "Machine: " << kernel_info.machine;

    // 通过uname获取的版本字符串格式可能为a.b.c-xxx
    // a为主版本号，b为此版本号，c为修正号
    vector<string> elements;
    ::curve::common::SplitString(kernel_info.release, "-", &elements);
    if (elements.size() == 0) {
        LOG(ERROR) << "parse kenel version failed.";
        return false;
    }

    vector<string> numbers;
    ::curve::common::SplitString(elements[0], ".", &numbers);
    // 有些系统可能版本格式前面部分是a.b.c.d，但是a.b.c是不变的
    if (numbers.size() < 3) {
        LOG(ERROR) << "parse kenel version failed.";
        return false;
    }

    int major = std::stoi(numbers[0]);
    int minor = std::stoi(numbers[1]);
    int revision = std::stoi(numbers[2]);
    LOG(INFO) << "major: " << major
              << ", minor: " << minor
              << ", revision: " << revision;

    // 内核版本必须大于3.15,用于支持renameat2
    if (KERNEL_VERSION(major, minor, revision) < MIN_KERNEL_VERSION) {
        LOG(ERROR) << "Kernel older than 3.15 is not supported.";
        return false;
    }
    return true;
}

int Ext4FileSystemImpl::Init(const LocalFileSystemOption& option) {
    enableRenameat2_ = option.enableRenameat2;
    if (enableRenameat2_) {
        if (!CheckKernelVersion())
            return -1;
    }

    maxEvents_ = option.maxEvents;
    enableEpoll_ = option.enableEpoll;
    LOG(INFO) << "Init libaio, maxEvents = " << option.maxEvents
              << ", enableEpoll = " << option.enableEpoll;

    if (maxEvents_ <= 0) {
        LOG(ERROR) << "enableaio enable coroutine but maxevent <= 0"
                    << ", maxEvents = " << maxEvents_;
        return -1;
    }
    memset(&ctx_, 0, sizeof(ctx_));
    int ret = posixWrapper_->iosetup(maxEvents_, &ctx_);
    if (ret != 0) {
        LOG(ERROR) << "iosetup fail :" << strerror(errno);
        return -errno;
    }

    if (enableEpoll_) {
        efd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (-1 == efd_) {
            LOG(ERROR) << "failed to eventfd ";
            return -1;
        }

        epfd_ = epoll_create(1);
        if (-1 == epfd_) {
            LOG(ERROR) << "failed to open epoll_create ";
            return -1;
        }

        epevent_.events = EPOLLIN | EPOLLET;
        epevent_.data.ptr = NULL;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, efd_, &epevent_) != 0) {
            LOG(ERROR) << "failed to open epoll_ctl ";
            return -1;
        }
    }

    stop_ = false;
    submitTh_ =
        std::move(std::thread(&Ext4FileSystemImpl::IoSubmitter, this));
    th_ = std::move(std::thread(&Ext4FileSystemImpl::ReapIo, this));

    LOG(INFO) << "ext4 filesystem use aio";

    return 0;
}

void Ext4FileSystemImpl::BatchSubmit(std::deque<IoTask *> *batchIo) {
    int size = batchIo->size();
    int i = 0;
    iocb *iocbs[size];

    for (i = 0; i < size; ++i) {
        iocbs[i] = new iocb;
        IoTask *task = batchIo->front();
        if (task->isRead_)
            io_prep_pread(iocbs[i], task->fd_, task->buf_, task->length_,
                          task->offset_);
        else
            io_prep_pwrite(iocbs[i], task->fd_, task->buf_, task->length_,
                           task->offset_);
        if (enableEpoll_)
	    io_set_eventfd(iocbs[i], efd_);

        iocbs[i]->data = (void *)task->done_;
        batchIo->pop_front();
        delete task;
    }

    int ret = posixWrapper_->iosubmit(ctx_, size, iocbs);
    while (ret == -EAGAIN) {
        LOG(ERROR) << "failed to submit IO with EAGAIN";
        usleep(100);
        ret = posixWrapper_->iosubmit(ctx_, size, iocbs);
    }
    if (ret < 0)
        LOG(FATAL) << "failed to submit libaio, errno: " << strerror(errno);
    nrFlyingIo_.fetch_add(size);
    //LOG(INFO) << size << " IO submitted, flying IO number=" << nrFlyingIo_;
}

void Ext4FileSystemImpl::IoSubmitter() {
    LOG(INFO) << "start IO submitter thread";
    while (!stop_) {
        // sleep if I/O depth is overload
        if (nrFlyingIo_ >= maxEvents_) {
            LOG(INFO) << "flying IO number=" << nrFlyingIo_ << ", go to sleep";
            usleep(100);
            continue;
        }

        int nrSubmit = 0;
        int nrFlying = nrFlyingIo_;
        std::deque<IoTask *> batchIo;
        submitMutex_.lock();
        if (tasks_.empty()) {
            submitMutex_.unlock();
            usleep(100);
            continue;
        }

        if (tasks_.size() + nrFlying > maxEvents_) {
            nrSubmit = maxEvents_ - nrFlying;
            while (nrSubmit > 0) {
                batchIo.push_back(tasks_.front());
                tasks_.pop_front();
                --nrSubmit;
            }
        } else {
            batchIo.swap(tasks_);
        }
        submitMutex_.unlock();

        BatchSubmit(&batchIo);
    }
}

void Ext4FileSystemImpl::ReapIoWithEpoll() {
    LOG(INFO) << "start reapio thread with epoll";
    io_event *events = new io_event[maxEvents_];
    while (!stop_) {
        int waitTimeMs = 100;
	int ret = epoll_wait(epfd_, &epevent_, 1, waitTimeMs);
        if (ret < 0) {
            LOG(ERROR) << "epoll_wait error: " << strerror(errno);
            continue;
        } else if (ret == 0) {
	    continue;
        }

        uint64_t finishIoCount = 0;
        ret = read(efd_, &finishIoCount, sizeof(finishIoCount));
        if (ret != sizeof(finishIoCount)) {
            LOG(ERROR) << "failed to efd read error, ret = " << ret;
            return;
        } else {
	    //LOG(INFO) << "going to reap " << finishIoCount << " IOs";
        }

        while (finishIoCount > 0) {
            timespec time = {0, 100000};  // 100us
            int reapNum = maxEvents_ > finishIoCount
                                    ? finishIoCount : maxEvents_;
            int eventNum = posixWrapper_->iogetevents(ctx_, 1, reapNum,
                                    events, &time);
            if (eventNum <= 0) {
                if (eventNum < 0) {
                    LOG(ERROR) << "iogetevents failed, ret = " << eventNum;
                }
                continue;
            }
               
            for (int i = 0; i < eventNum; i++) {
                delete events[i].obj;
                ReqClosure *done =
                    reinterpret_cast<ReqClosure *> (events[i].data);
                
                brpc::ClosureGuard doneGuard(done);
                done->res_ = events[i].res2;
                if (events[i].res2)
                    LOG(ERROR) << "res = " << events[i].res
                              << ", res2 = " << events[i].res2;
                nrFlyingIo_--;
            }
            //LOG(INFO) << eventNum << " IOs reaped, flying IO number=" << nrFlyingIo_;
            finishIoCount -= eventNum;
        }
    }
    free(events);
    LOG(INFO) << "end reapio thread";
}

void Ext4FileSystemImpl::ReapIoWithoutEpoll() {
    LOG(INFO) << "start reapio thread with out epoll";
    while (!stop_) {
        io_event event;
        timespec time = {0, 1000000};  // 1000us
        int ret = posixWrapper_->iogetevents(ctx_, 1, 1,
                                &event, &time);
        if (ret <= 0) {
            if (ret < 0) {
                LOG(ERROR) << "iogetevents failed, ret = " << ret;
            }
            continue;
        }

        delete event.obj;
        {
            ReqClosure *done = reinterpret_cast<ReqClosure *> (event.data);
            brpc::ClosureGuard doneGuard(done);
            done->res_ = event.res2;
            if (event.res2)
                LOG(ERROR) << "res = " << event.res << ", res2 = " << event.res2;
            // LOG(INFO) << "get event ctx = " << event.data << ", ret = " <<
            // ret;
            nrFlyingIo_--;
        }
        //LOG(INFO) << "1 IO reaped, cb run time=" <<  endTime - startTime 
        //          << ", flying IO number=" << nrFlyingIo_;
    }
    LOG(INFO) << "end reapio thread";
}

void Ext4FileSystemImpl::ReapIo() {
    if (enableEpoll_) {
        ReapIoWithEpoll();
    } else {
        ReapIoWithoutEpoll();
    }
}

int Ext4FileSystemImpl::Uninit() {
    stop_ = true;

    th_.join();
    submitTh_.join();
    if (enableEpoll_) {
        close(epfd_);
    }
    posixWrapper_->iodestroy(ctx_);

    return 0;
}

int Ext4FileSystemImpl::Statfs(const string& path,
                               struct FileSystemInfo *info) {
    struct statfs diskInfo;
    int rc = posixWrapper_->statfs(path.c_str(), &diskInfo);
    if (rc < 0) {
        LOG(WARNING) << "fstat failed: " << strerror(errno)
                     << ", file path = " << path.c_str();
        return -errno;
    }
    info->total = diskInfo.f_blocks * diskInfo.f_bsize;
    info->available = diskInfo.f_bavail * diskInfo.f_bsize;
    info->stored = info->total - diskInfo.f_bfree * diskInfo.f_bsize;
    info->allocated = info->stored;
    return 0;
}

int Ext4FileSystemImpl::Open(const string& path, int flags) {
    int fd = posixWrapper_->open(path.c_str(), flags, 0644);
    if (fd < 0) {
        LOG(WARNING) << "open failed: " << strerror(errno)
                     << ", file path = " << path.c_str();
        return -errno;
    }
    return fd;
}

int Ext4FileSystemImpl::Close(int fd) {
    int rc = posixWrapper_->close(fd);
    if (rc < 0) {
        LOG(ERROR) << "close failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Delete(const string& path) {
    int rc = 0;
    // 如果删除对象是目录的话，需要先删除目录下的子对象
    if (DirExists(path)) {
        vector<string> names;
        rc = List(path, &names);
        if (rc < 0) {
            LOG(WARNING) << "List " << path << " failed.";
            return rc;
        }
        for (auto &name : names) {
            string subPath = path + "/" + name;
            // 递归删除子对象
            rc = Delete(subPath);
            if (rc < 0) {
                LOG(WARNING) << "Delete " << subPath << " failed.";
                return rc;
            }
        }
    }
    rc = posixWrapper_->remove(path.c_str());
    if (rc < 0) {
        LOG(WARNING) << "remove failed: " << strerror(errno)
                     << ", file path = " << path.c_str();
        return -errno;
    }
    return rc;
}

int Ext4FileSystemImpl::Mkdir(const string& dirName) {
    vector<string> names;
    ::curve::common::SplitString(dirName, "/", &names);

    // root dir must exists
    if (0 == names.size())
        return 0;

    string path;
    for (size_t i = 0; i < names.size(); ++i) {
        if (0 == i && dirName[0] != '/')  // 相对路径
            path = path + names[i];
        else
            path = path + "/" + names[i];
        if (DirExists(path))
            continue;
        // 目录需要755权限，不然会出现“Permission denied”
        if (posixWrapper_->mkdir(path.c_str(), 0755) < 0) {
            LOG(WARNING) << "mkdir " << path << " failed. "<< strerror(errno);
            return -errno;
        }
    }

    return 0;
}

bool Ext4FileSystemImpl::DirExists(const string& dirName) {
    struct stat path_stat;
    if (0 == posixWrapper_->stat(dirName.c_str(), &path_stat))
        return S_ISDIR(path_stat.st_mode);
    else
        return false;
}

bool Ext4FileSystemImpl::FileExists(const string& filePath) {
    struct stat path_stat;
    if (0 == posixWrapper_->stat(filePath.c_str(), &path_stat))
        return S_ISREG(path_stat.st_mode);
    else
        return false;
}

int Ext4FileSystemImpl::DoRename(const string& oldPath,
                                 const string& newPath,
                                 unsigned int flags) {
    int rc = 0;
    if (enableRenameat2_) {
        rc = posixWrapper_->renameat2(oldPath.c_str(), newPath.c_str(), flags);
    } else {
        rc = posixWrapper_->rename(oldPath.c_str(), newPath.c_str());
    }
    if (rc < 0) {
        LOG(WARNING) << "rename failed: " << strerror(errno)
                     << ". old path: " << oldPath
                     << ", new path: " << newPath
                     << ", flag: " << flags;
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::List(const string& dirName,
                             vector<std::string> *names) {
    DIR *dir = posixWrapper_->opendir(dirName.c_str());
    if (nullptr == dir) {
        LOG(WARNING) << "opendir:" << dirName << " failed:" << strerror(errno);
        return -errno;
    }
    struct dirent *dirIter;
    errno = 0;
    while ((dirIter=posixWrapper_->readdir(dir)) != nullptr) {
        if (strcmp(dirIter->d_name, ".") == 0
                || strcmp(dirIter->d_name, "..") == 0)
            continue;
        names->push_back(dirIter->d_name);
    }
    // 可能存在其他携程改变了errno，但是只能通过此方式判断readdir是否成功
    if (errno != 0) {
        LOG(WARNING) << "readdir failed: " << strerror(errno);
    }
    posixWrapper_->closedir(dir);
    return -errno;
}

int Ext4FileSystemImpl::Read(int fd,
                             char *buf,
                             uint64_t offset,
                             int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = posixWrapper_->pread(fd,
                                       buf + relativeOffset,
                                       remainLength,
                                       offset);
        // 如果offset大于文件长度，pread会返回0
        if (ret == 0) {
            LOG(WARNING) << "pread returns zero."
                         << "offset: " << offset
                         << ", length: " << remainLength;
            break;
        }
        if (ret < 0) {
            if (errno == EINTR && retryTimes < MAX_RETYR_TIME) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pread failed: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length - remainLength;
}


int Ext4FileSystemImpl::WriteAsync(int fd, const char *buf, uint64_t offset,
                                  int length, void *done) {
    IoTask *task = new IoTask(fd, false, const_cast<char *>(buf), offset, length, done);
    submitMutex_.lock();
    tasks_.push_back(task);
    submitMutex_.unlock();
}

int Ext4FileSystemImpl::Write(int fd, const char *buf, uint64_t offset,
                              int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = posixWrapper_->pwrite(fd, buf + relativeOffset, remainLength,
                                        offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < MAX_RETYR_TIME) {
                ++retryTimes;
                continue;
            }
            LOG(ERROR) << "pwrite failed: " << strerror(errno);
            return -errno;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return length;
}

int Ext4FileSystemImpl::Append(int fd,
                               const char *buf,
                               int length) {
    // TODO(yyk)
    return 0;
}

int Ext4FileSystemImpl::Fallocate(int fd,
                                  int op,
                                  uint64_t offset,
                                  int length) {
    int rc = posixWrapper_->fallocate(fd, op, offset, length);
    if (rc < 0) {
        LOG(ERROR) << "fallocate failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Fstat(int fd, struct stat *info) {
    int rc = posixWrapper_->fstat(fd, info);
    if (rc < 0) {
        LOG(ERROR) << "fstat failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

int Ext4FileSystemImpl::Fsync(int fd) {
    int rc = posixWrapper_->fsync(fd);
    if (rc < 0) {
        LOG(ERROR) << "fsync failed: " << strerror(errno);
        return -errno;
    }
    return 0;
}

}  // namespace fs
}  // namespace curve
