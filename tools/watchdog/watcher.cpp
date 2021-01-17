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
 * Created Date: 2020-12-11
 * Author: qinyi
 */

#include "tools/watchdog/watcher.h"

#include <butil/fd_guard.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include <fstream>
#include <memory>
#include <string>
#include <type_traits>

namespace Curve {
namespace WatchDog {

int ThreadWatcher::Run() {
    char status = 0;
    int ret = getProcessStatus(serverInfo_->pid, &status);
    if (ret < 0) {
        SAFE_LOG_INFO_N(WATCHDOG_DUP_LOG_FREQUENCY,
                        "Failed to get status for process " << serverInfo_->pid
                                                            << ", ignored");
        return 0;
    }

    if (status > 0 && status != 'Z') {
        /* thread is OK */
        return 0;
    } else if (status == 0) {
        SAFE_LOG_ERROR("Erorr, " << getServerName(serverInfo_->serverType)
                                 << " with pid " << serverInfo_->pid
                                 << " is not existed now");
        return -1;
    } else {
        SAFE_LOG_ERROR("Erorr, " << getServerName(serverInfo_->serverType)
                                 << " with pid " << serverInfo_->pid
                                 << " has become ZOMBIE");
        return -1;
    }
}

int DiskSmartWatcher::Run() {
    std::shared_ptr<ChunkServerInfo> info =
        std::dynamic_pointer_cast<ChunkServerInfo>(serverInfo_);

    if (info->device.empty()) {
        SAFE_LOG_ERROR("SMART check failed for "
                       << getServerName(info->serverType) << " " << info->pid
                       << ", device is empty");
        return -1;
    }

    {
        /* test device open */
        int disk = -1;
        pthread_cleanup_push(FdCleaner, &disk);
        SAFE_LOG_INFO("preparefile " << disk);
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, nullptr);
        disk = open(info->device.c_str(), O_RDWR);
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);
        SAFE_LOG_INFO("openfile " << disk);
        pthread_testcancel();
        if (disk < 0) {
            SAFE_LOG_ERROR("SMART check failed for "
                           << getServerName(info->serverType) << " "
                           << info->pid << ", failed to open device "
                           << info->device);
            return -1;
        }
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, nullptr);
        pthread_cleanup_pop(1);
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);
    }

    {
        /* test SMART */
        std::string cmd =
            "smartctl -s on -H " + info->device + " |egrep 'result|Status'";
        string output = "";
        int ret = Exec(cmd, &output);
        if (ret < 0) {
            SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                           "SMART check ignored for " << info->device);
            return 0;
        }

        if (output.empty()) {
            /* print entire smartcl output for debug */
            cmd = "smartctl -s on -H " + info->device;
            int ret = Exec(cmd, &output);
            if (ret < 0) {
                SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                               "SMART check ignored for "
                                   << info->device
                                   << ", failed to get smartctl entire output");
                return 0;
            }

            SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                           "SMART check ignored for "
                               << info->device
                               << ", get unknown smartctl result:" << output);
            return 0;
        }

        if (output.find("OK") != std::string::npos ||
            output.find("PASSED") != std::string::npos) {
            /* SMART test passed */
            return 0;
        } else if (output.find("FAILED") != std::string::npos) {
            /* SMART test failed */
            SAFE_LOG_ERROR("SMART check failed for "
                           << info->device << ", smartctl result:" << output);
            return -1;
        } else {
            /* unknown test result */
            SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                           "SMART check ignored for "
                               << info->device
                               << ", get unknown smartctl result:" << output);
            return 0;
        }
    }

    return 0;
}

int FileOpWatcher::WriteData(int fd, const char* buf, uint64_t offset,
                             int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = pwrite(fd, buf + relativeOffset, remainLength, offset);
        if (ret < 0) {
            if (errno == EINTR && retryTimes < WATCHDOG_FILE_RETRY_TIMES) {
                ++retryTimes;
                continue;
            }
            SAFE_LOG_ERROR("pwrite failed: " << strerror(errno));
            return -1;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }

    return 0;
}

int FileOpWatcher::ReadData(int fd, char* buf, uint64_t offset, int length) {
    int remainLength = length;
    int relativeOffset = 0;
    int retryTimes = 0;
    while (remainLength > 0) {
        int ret = pread(fd, buf + relativeOffset, remainLength, offset);
        // if offset > file length，pread return 0
        if (ret == 0) {
            SAFE_LOG_ERROR("pread returns zero."
                           << "offset: " << offset
                           << ", length: " << remainLength);
            return -1;
        }
        if (ret < 0) {
            if (errno == EINTR && retryTimes < WATCHDOG_FILE_RETRY_TIMES) {
                ++retryTimes;
                continue;
            }
            SAFE_LOG_ERROR("pread failed: " << strerror(errno));
            return -1;
        }
        remainLength -= ret;
        offset += ret;
        relativeOffset += ret;
    }
    return 0;
}

int FileOpWatcher::Run() {
    std::shared_ptr<ChunkServerInfo> info =
        std::dynamic_pointer_cast<ChunkServerInfo>(serverInfo_);

    string testPath = info->storDir + "/watchdog.test";

    /* remove existed test file */
    if (!access(testPath.c_str(), F_OK) && remove(testPath.c_str()) < 0) {
        SAFE_LOG_INFO_N(WATCHDOG_DUP_LOG_FREQUENCY,
                        "Unable to delete existed test file: "
                            << testPath << ", " << strerror(errno));
        SAFE_LOG_INFO_N(WATCHDOG_DUP_LOG_FREQUENCY,
                        "Skip file test in " << info->storDir);
        return 0;
    }

    /* create test file */
    string writeBuf(WATCHDOG_WRITE_BUF_LEN, 'R');
    char readBuf[WATCHDOG_WRITE_BUF_LEN] = {0};
    int ret = 0;
    int fd = -1;
    SAFE_LOG_INFO("preparefile " << fd);
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, nullptr);
    fd = open(testPath.c_str(), O_CREAT | O_RDWR | O_NOATIME | O_DSYNC);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);
    SAFE_LOG_INFO("openfile " << fd);
    if (fd < 0) {
        SAFE_LOG_ERROR("Failed to create test file: " << testPath << ", "
                                                      << strerror(errno));
        ret = -1;
        goto OUT;
    }

    /* write file */
    ret = WriteData(fd, writeBuf.c_str(), 0, WATCHDOG_WRITE_BUF_LEN);
    if (ret < 0) {
        SAFE_LOG_ERROR("Failed to write test file: " << testPath);
        ret = -1;
        goto OUT;
    }

    /* read file */
    ret = ReadData(fd, readBuf, 0, WATCHDOG_WRITE_BUF_LEN);
    if (ret < 0) {
        SAFE_LOG_ERROR("Failed to read test file: " << testPath);
        ret = -1;
        goto OUT;
    }
    if (memcmp(writeBuf.c_str(), readBuf, WATCHDOG_WRITE_BUF_LEN)) {
        SAFE_LOG_ERROR("Read invalid content from " << testPath);
        ret = -1;
        goto OUT;
    }

    if (remove(testPath.c_str()) < 0) {
        SAFE_LOG_ERROR("Failed to remove test file: " << testPath << ", "
                                                      << strerror(errno));
        ret = -1;
        goto OUT;
    }

OUT:
    close(fd);
    /* all file test passed */
    return ret;
}

}  // namespace WatchDog
}  // namespace Curve
