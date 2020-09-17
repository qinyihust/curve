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
DEFINE_bool(usebthread, false, "if use bthread");
DEFINE_int32(threadNum, -1, "test thread num");
DEFINE_uint32(iocount, UINT_MAX, "io count per thread");
DEFINE_uint32(iopsPerDisk, UINT_MAX, "iops per disk");
DEFINE_bool(useepool, false, "if use epool");
DEFINE_bool(dsync, false, "if use dsync");

bthread_t bth[128];
int fdList[128];
std::thread th[128];
std::shared_ptr<LocalFileSystem> lfs;

uint64_t GetTimeofDayUs() {
    timeval now;
    gettimeofday(&now, NULL);
    return now.tv_sec * 1000000L + now.tv_usec;
}

uint64_t GetIntervalUs() {
    uint64_t intervalUs = 0;
    if (FLAGS_iopsPerDisk == UINT_MAX || FLAGS_iopsPerDisk == 0) {
        intervalUs = 0;
    } else {
        intervalUs = 1000000 * FLAGS_threadNum / FLAGS_iopsPerDisk;
    }
    LOG(INFO) << "intervalUs = " << intervalUs;
    return intervalUs;
}

int OpenFileList(int num) {
    std::string filename;
    int index = 0;
    int count = 0;
    for (int i = 0; i < 128; i++) {
        fdList[i] = -1;
    }
    while (count < num && index < 2000) {
        filename = FLAGS_path + std::to_string(index++);
        int fd = 0;
        if (FLAGS_dsync) {
            fd = lfs->Open(filename, O_RDWR|O_NOATIME|O_DSYNC);
        } else {
            fd = lfs->Open(filename, O_RDWR|O_NOATIME|O_DIRECT);
        }

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
    LOG(INFO) << "bthreadRun, fd = " << *fd << ", count = " << FLAGS_iocount;
    if (*fd <= 0) {
        return nullptr;
    }
    char *writebuf = nullptr;
    int ret1 = posix_memalign((void **)&writebuf, getpagesize(), 4096);
    // char writebuf[4096] = {0};

    uint64_t intervalUs = GetIntervalUs();
    uint64_t avaLatancyUs = 0;
    uint64_t totalLatancyUs = 0;
    uint64_t startTimeUs;
    uint64_t endTimeUs = 0;
    uint64_t latancyTimeUs = 0;
    uint32_t count = 0;
    // AIO AND COROUTINE test, filesize 16MB
    while (FLAGS_iocount > count) {
        int i = std::rand() % 4096;
        int offset = i * 4096;  // offset在16M以内
        startTimeUs = GetTimeofDayUs();
        LOG_IF(FATAL, 4096 != lfs->Write(*fd, writebuf, offset, 4096))
            << "Write fail";
        endTimeUs = GetTimeofDayUs();
        latancyTimeUs = endTimeUs - startTimeUs;
        totalLatancyUs += latancyTimeUs;
        count++;
        if (latancyTimeUs < intervalUs) {
            std::this_thread::sleep_for(std::chrono::microseconds(intervalUs - latancyTimeUs));
        }
    }
    avaLatancyUs = totalLatancyUs / count;
    free(writebuf);
    LOG(INFO) << "bthreadRun end, fd = " << *fd
              << ", count = " << count
              << ", avaLatancyUs = " << avaLatancyUs;
    return nullptr;
}

// int runAioTest() {
//     int ret = OpenFileList(128);
//     if (ret < 0) {
//         return ret;
//     }
//     for (int i = 0; i < 128; i++) {
//         bthread_start_background(&bth[i], NULL, bthreadRun, &fdList[i]);
//     }

//     for (int i = 0; i < 128; i++) {
//         bthread_join(bth[i], nullptr);
//     }
//     return 0;
// }

void pthreadRun(int fd) {
    LOG(INFO) << "pthreadRun, fd = " << fd << ", count = " << FLAGS_iocount;
    // char writebuf[4096] = {0};
    char *writebuf = nullptr;
    int ret1 = posix_memalign((void **)&writebuf, getpagesize(), 4096);

    uint64_t intervalUs = GetIntervalUs();
    uint64_t avaLatancyUs = 0;
    uint64_t totalLatancyUs = 0;
    uint64_t startTimeUs;
    uint64_t endTimeUs = 0;
    uint64_t latancyTimeUs = 0;
    uint32_t count = 0;
    // AIO AND COROUTINE test, filesize 16MB
    while (FLAGS_iocount > count) {
        int i = std::rand() % 4096;
        int offset = i * 4096;  // offset在16M以内
        startTimeUs = GetTimeofDayUs();
        LOG_IF(FATAL, 4096 != lfs->Write(fd, writebuf, offset, 4096))
            << "Write fail";
        endTimeUs = GetTimeofDayUs();
        latancyTimeUs = endTimeUs - startTimeUs;
        totalLatancyUs += latancyTimeUs;
        count++;
        if (latancyTimeUs < intervalUs) {
            std::this_thread::sleep_for(std::chrono::microseconds(intervalUs - latancyTimeUs));
        }
    }
    avaLatancyUs = totalLatancyUs / count;
    free(writebuf);
    LOG(INFO) << "pthreadRun end, fd = " << fd
              << ", count = " << count
              << ", avaLatancyUs = " << avaLatancyUs;
    return;
}

// int runPreadwriteTest() {
//     int ret = OpenFileList(10);
//     if (ret < 0) {
//         LOG(ERROR) << "openFileList fail";
//         return ret;
//     }

//     for (int i = 0; i < 10; i++) {
//         th[i] = std::move(std::thread(pthreadRun, fdList[i]));
//     }
//     for (int i = 0; i < 10; i++) {
//         th[i].join();
//     }
//     return 0;
// }
int runBthread(int num) {
    LOG(INFO) << "run bthread, thread num = " << num;
    int ret = OpenFileList(num);
    if (ret < 0) {
        return ret;
    }
    for (int i = 0; i < num; i++) {
        bthread_start_background(&bth[i], NULL, bthreadRun, &fdList[i]);
    }

    for (int i = 0; i < num; i++) {
        bthread_join(bth[i], nullptr);
    }
    return 0;
}

int runPthread(int num) {
    LOG(INFO) << "run pthread, thread num = " << num;
    int ret = OpenFileList(num);
    if (ret < 0) {
        LOG(ERROR) << "openFileList fail";
        return ret;
    }

    for (int i = 0; i < num; i++) {
        th[i] = std::move(std::thread(pthreadRun, fdList[i]));
    }
    for (int i = 0; i < num; i++) {
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

    if (FLAGS_threadNum == 0 || FLAGS_threadNum > 128 || FLAGS_threadNum < -1) {
        LOG(ERROR) << "invalid thread num = " << FLAGS_threadNum;
        return -1;
    }

    // 初始化本地文件系统
    lfs = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
    LocalFileSystemOption lfsOption;

    LOG(INFO) << "FLAGS_aio = " << FLAGS_aio
              << ", FLAGS_path = " << FLAGS_path
              << ", FLAGS_dsync = " << FLAGS_dsync
              << ", FLAGS_useepool = " << FLAGS_useepool
              << ", FLAGS_threadNum = " << FLAGS_threadNum
              << ", FLAGS_usebthread = " << FLAGS_usebthread
              << ", FLAGS_iocount = " << FLAGS_iocount
              << ", FLAGS_iopsPerDisk = " << FLAGS_iopsPerDisk;
    if (FLAGS_aio) {
        lfsOption.enableRenameat2 = true;
        lfsOption.enableCoroutine = true;
        lfsOption.enableAio = true;
        lfsOption.maxEvents = 128;
        lfsOption.enableEpool = FLAGS_useepool;
        LOG(INFO) << "use aio";
    } else {
        lfsOption.enableRenameat2 = false;
        lfsOption.enableCoroutine = false;
        lfsOption.enableAio = false;
        lfsOption.maxEvents = -1;
        lfsOption.enableEpool = false;
        LOG(INFO) << "use pread pwrite";
    }

    LOG_IF(FATAL, 0 != lfs->Init(lfsOption))
        << "Failed to initialize local filesystem module!";

    LOG(INFO) << "start test";
    if (FLAGS_threadNum == -1) {
        if (FLAGS_aio) {
            FLAGS_threadNum = 128;
        } else {
            FLAGS_threadNum = 10;
        }
    }

    if (FLAGS_usebthread) {
        runBthread(FLAGS_threadNum);
    } else {
        runPthread(FLAGS_threadNum);
    }

    LOG(INFO) << "end test";

    CloseFds();
    LOG_IF(FATAL, 0 != lfs->Uninit());
    return 0;
}
