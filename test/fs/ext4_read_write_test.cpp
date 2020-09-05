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
 * Created Date: 20200904
 * Author: hzchenwei7
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <dirent.h>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <memory>
#include "src/fs/local_filesystem.h"
#include "src/fs/ext4_filesystem_impl.h"
#include "bthread/bthread.h"

using ::curve::fs::LocalFileSystem;
using ::curve::fs::LocalFileSystemOption;
using ::curve::fs::LocalFsFactory;
using ::curve::fs::FileSystemType;

DEFINE_string(path, "/data/chunkserver0/chunkfilepool/", "test path");
DEFINE_bool(aio, false, "if use aio");

bthread_t bth[128];
int fdList[128];
std::thread th[10];
std::shared_ptr<LocalFileSystem> lfs;

int OpenFileList(int num) {
    std::string filename;
    int index = 0;
    int count = 0;
    for (int i = 0; i < 128; i++) {
        fdList[i] = -1;
    }
    while (count < num && index < 2000) {
        filename = FLAGS_path + std::to_string(index++);
        int fd = lfs->Open(filename, O_RDWR|O_NOATIME|O_DIRECT);
        if (fd > 0) {
            fdList[count] = fd;
            LOG(INFO) << "open file :" << filename;
            count++;
        }
        index++;
    }
    if (count < num) {
        LOG(ERROR) << "open file list fail, path = " << FLAGS_path
                   << ", try times = " << index
                   << ", open count = " << count
                   << ", want open num = " << num;
        return -1;
    }
    LOG(INFO) << "open file num :" << count;
    return 0;
}

void* bthreadRun(void *arg) {
    int *fd = reinterpret_cast<int *>(arg);
    LOG(INFO) << "bthreadRun, fd = " << *fd;
    if (*fd <= 0) {
        return nullptr;
    }
    char *writebuf = nullptr;
    int ret1 = posix_memalign((void **)&writebuf, getpagesize(), 4096);
    // char writebuf[4096] = {0};

    // AIO AND COROUTINE test, filesize 16MB, read write size 400MB
    while (true) {
        int i = std::rand() % 4096;
        int offset = i * 4096;  // offset在16M以内
        LOG_IF(FATAL, 4096 != lfs->Write(*fd, writebuf, offset, 4096))
            << "Write fail";
    }
    free(writebuf);
    return nullptr;
}

int runAioTest() {
    int ret = OpenFileList(128);
    if (ret < 0) {
        return ret;
    }
    for (int i = 0; i < 128; i++) {
        bthread_start_background(&bth[i], NULL, bthreadRun, &fdList[i]);
    }

    for (int i = 0; i < 128; i++) {
        bthread_join(bth[i], nullptr);
    }
    return 0;
}

void pthreadRun(int fd) {
    LOG(INFO) << "pthreadRun, fd = " << fd;
    // char writebuf[4096] = {0};
    char *writebuf = nullptr;
    int ret1 = posix_memalign((void **)&writebuf, getpagesize(), 4096);
    // AIO AND COROUTINE test, filesize 4MB, read write size 400MB
    while (true) {
        int i = std::rand() % 4096;
        int offset = i * 4096;  // offset在16M以内
        LOG_IF(FATAL, 4096 != lfs->Write(fd, writebuf, offset, 4096))
            << "Write fail";
    }
    free(writebuf);
    return;
}

int runPreadwriteTest() {
    int ret = OpenFileList(10);
    if (ret < 0) {
        LOG(ERROR) << "openFileList fail";
        return ret;
    }

    for (int i = 0; i < 10; i++) {
        th[i] = std::move(std::thread(pthreadRun, fdList[i]));
    }
    for (int i = 0; i < 10; i++) {
        th[i].join();
    }
    return 0;
}

void CloseFds() {
    for (int i = 0; i  < 128; i++) {
        if (fdList[i] > 0) {
            lfs->Close(fdList[i]);
        }
    }
}

int main(int argc, char ** argv) {
    google::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);

    // 初始化本地文件系统
    lfs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    LocalFileSystemOption lfsOption;

    LOG(INFO) << "FLAGS_aio = " << FLAGS_aio
              << ", FLAGS_path = " << FLAGS_path;
    if (FLAGS_aio) {
        lfsOption.enableRenameat2 = true;
        lfsOption.enableCoroutine = true;
        lfsOption.enableAio = true;
        lfsOption.maxEvents = 128;
        LOG(INFO) << "use aio";
    } else {
        lfsOption.enableRenameat2 = false;
        lfsOption.enableCoroutine = false;
        lfsOption.enableAio = false;
        lfsOption.maxEvents = -1;
        LOG(INFO) << "use pread pwrite";
    }

    LOG_IF(FATAL, 0 != lfs->Init(lfsOption))
        << "Failed to initialize local filesystem module!";

    LOG(INFO) << "start test";
    if (FLAGS_aio) {
        runAioTest();
    } else {
        runPreadwriteTest();
    }

    LOG(INFO) << "end test";

    CloseFds();
    LOG_IF(FATAL, 0 != lfs->Uninit());
    return 0;
}
