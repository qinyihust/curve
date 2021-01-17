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

#ifndef TOOLS_WATCHDOG_WATCHDOG_COMMON_H_
#define TOOLS_WATCHDOG_WATCHDOG_COMMON_H_

#include <glog/logging.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

namespace Curve {
namespace WatchDog {

#define CHUNKSERVER_CMD_NAME "curve-chunkserver"
#define CHUNKSERVER_CMD_LEN_SHORT 15
#define CHUNKSERVER_CMD_LEN_FULL 17
#define CHUNKSERVER_CMD_DIR_PREFIX "-chunkServerStoreUri="

#define WATCHDOG_MAX_TIME_INTV 100
#define WATCHDOG_DEF_SCAN_CYCLE 30
#define WATCHDOG_DEF_OP_TIMEOUT 30
#define WATCHDOG_DEF_KILL_TIMEOUT 30
#define WATCHDOG_DEF_LOG_PATH "/data/log/watchdog/"
#define WATCHDOG_DUP_LOG_FREQUENCY 256
#define WATCHDOG_MAX_RETRY 5

#define WATCHDOG_LOCKFILE "/var/run/chunkserver_watchdog.pid"
#define WATCHDOG_LOCKMODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

#define WATCHER_EXEC_BUFFER_LEN 4096
#define WATCHER_EXEC_MAX_BUF_LEN 16384

#define WATCHDOG_PATH_LEN 1024
#define WATCHDOG_CMD_BUF_LEN 4096
#define WATCHDOG_WRITE_BUF_LEN 4096
#define WATCHDOG_FILE_RETRY_TIMES 3

/*
 *  !!! dog thread should use this function to  !!!
 *  !!! prevent crash on pthread_cancel  !!!
 */
#define SAFE_LOG_ERROR(x)                                          \
    do {                                                           \
        int oldstate;                                              \
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate); \
        LOG(ERROR) << x;                                           \
        pthread_setcancelstate(oldstate, nullptr);                 \
    } while (0)

/*
 *  !!! dog thread should use this function to  !!!
 *  !!! prevent crash on pthread_cancel  !!!
 */
#define SAFE_LOG_INFO(x)                                           \
    do {                                                           \
        int oldstate;                                              \
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate); \
        LOG(INFO) << x;                                            \
        pthread_setcancelstate(oldstate, nullptr);                 \
    } while (0)

/*
 *  !!! dog thread should use this function to  !!!
 *  !!! prevent crash on pthread_cancel  !!!
 */
#define SAFE_LOG_INFO_N(n, x)                                      \
    do {                                                           \
        int oldstate;                                              \
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate); \
        LOG_EVERY_N(INFO, n) << x;                                 \
        pthread_setcancelstate(oldstate, nullptr);                 \
    } while (0)

/*
 *  !!! dog thread should use this function to  !!!
 *  !!! prevent crash on pthread_cancel  !!!
 */
#define SAFE_LOG_ERR_N(n, x)                                       \
    do {                                                           \
        int oldstate;                                              \
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate); \
        LOG_EVERY_N(ERROR, n) << x;                                \
        pthread_setcancelstate(oldstate, nullptr);                 \
    } while (0)

using std::string;

enum class DogHealth {
    DOG_HEALTHY = 0,
    DOG_SICK,
    DOG_DEAD,
    DOG_IGNORE = 100,
};

enum class ServerType {
    CHUNKSERVER = 0,
    MDS,
    SNAPSHOTCLONESERVER,
    UNKNOWN = 100,
};

struct WatchConf {
    uint32_t scanIntervalSec;  // in seconds
    uint32_t opTimeoutSec;     // in seconds
    uint32_t killTimeoutSec;   // in seconds
};

struct ServerInfo {
    virtual ~ServerInfo() = default;
    ServerType serverType;
    int pid;
    char state;
};

struct ChunkServerInfo : public ServerInfo {
    string device;
    string storDir;
};

void FdCleaner(void* arg);

class PipeGuard {
 public:
    PipeGuard() : fp_(nullptr) {}
    explicit PipeGuard(FILE* fp) : fp_(fp) {}

    ~PipeGuard() noexcept(false) {
        if (fp_) {
            ::pclose(fp_);
            fp_ = nullptr;
        }
    }

    operator FILE*() const {
        return fp_;
    }

 private:
    FILE* fp_;
};

class FdGuard {
 public:
    FdGuard() : fd_(-1) {}
    explicit FdGuard(int fd) : fd_(fd) {}

    ~FdGuard() noexcept(false) {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    operator int() const {
        return fd_;
    }

 private:
    // Copying this makes no sense.
    FdGuard(const FdGuard&);
    void operator=(const FdGuard&);

    int fd_;
};

enum class WatcherName {
    THREAD_WATCHER,
    DISK_SMART_WATCHER,
    FILE_OP_WATCHER,
    FILE_CREATE_WATCHER,
    FILE_STAT_WATCHER,
    FILE_WRITE_WATCHER,
    FILE_READ_WATCHER,
    FILE_RENAME_WATCHER,
    FILE_DEL_WATCHER,
};

int checkEnv();
int initLogPath();
int getProcessStatus(const int pid, char* status);
int lockPidFile();
string getServerName(ServerType type);
int Exec(const string cmd, string* output);
}  // namespace WatchDog
}  // namespace Curve

#endif  // TOOLS_WATCHDOG_WATCHDOG_COMMON_H_
