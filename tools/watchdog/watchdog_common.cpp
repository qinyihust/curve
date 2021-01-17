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

#include "tools/watchdog/watchdog_common.h"

#include <errno.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <fstream>
#include <iostream>
#include <string>

namespace Curve {
namespace WatchDog {

using std::string;

int checkEnv() {
    if (system("smartctl -h > /dev/null")) {
        SAFE_LOG_ERROR("Error, smartctl is not available");
        SAFE_LOG_ERROR(
            "Try to run with root privilege, or install smartmontools");
        return -1;
    }

    return 0;
}

/**
 *  if get process status successfully, return 0;
 *  else return -1 (may be watchdog bug, ignoring is recommended);
 */
int getProcessStatus(const int pid, char* status) {
    string procPath = "/proc/" + std::to_string(pid) + "/status";
    if (access(procPath.c_str(), F_OK) < 0) {
        /* process is not existed */
        *status = 0;
        return 0;
    }

    std::ifstream procFile(procPath);
    if (!procFile.is_open()) {
        SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY, "Failed to open "
                                                       << procPath << ": "
                                                       << strerror(errno));
        return -1;
    }

    string line;
    while (getline(procFile, line)) {
        std::size_t pos = line.find("State:");
        if (pos == string::npos) continue;

        /* found */
        line.erase(0, pos + strlen("State:"));
        while (!line.empty() && (line.front() < 'A' || line.front() > 'Z'))
            line.erase(0, 1);

        if (!line.empty()) {
            *status = line.front();
            procFile.close();
            return 0;
        } else {
            SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                           "Error parse status for process " << pid);
            procFile.close();
            return -1;
        }
    }

    SAFE_LOG_ERR_N(WATCHDOG_DUP_LOG_FREQUENCY,
                   "Failed to get state for process " << pid);
    procFile.close();
    return -1;
}

static int lockFile(int fd) {
    struct flock fl;
    fl.l_type = F_WRLCK;
    fl.l_start = 0;
    fl.l_whence = SEEK_SET;
    fl.l_len = 0;
    return fcntl(fd, F_SETLK, &fl);
}

int lockPidFile() {
    FdGuard fd(open(WATCHDOG_LOCKFILE, O_RDWR | O_CREAT, WATCHDOG_LOCKMODE));
    if (fd < 0) {
        SAFE_LOG_ERROR("can't open " << WATCHDOG_LOCKFILE << ": "
                                     << strerror(errno));
        SAFE_LOG_ERROR("make sure to run with root privilege");

        return -1;
    } else if (lockFile(fd) < 0) {
        SAFE_LOG_ERROR("can't lock " << WATCHDOG_LOCKFILE << ": "
                                     << strerror(errno));
        SAFE_LOG_ERROR("chunkserver_watchdog is already running");
        return -1;
    }

    /* pid file locked */
    ftruncate(fd, 0);
    string pidString = std::to_string(getpid());
    int ret = write(fd, pidString.data(), pidString.length());
    if (ret < 0) {
        SAFE_LOG_ERROR("Failed to write pid file: " << strerror(errno));
        return -1;
    }

    return fd;
}

int initLogPath() {
    if (access(FLAGS_log_dir.c_str(), 0) == -1 &&
        mkdir(FLAGS_log_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)) {
        std::cerr << "Failed to init log dir " << FLAGS_log_dir << ", exit"
                  << std::endl;
        return -1;
    }

    return 0;
}

string getServerName(ServerType type) {
    switch (type) {
        case ServerType::CHUNKSERVER:
            return "ChunkServer";
        case ServerType::MDS:
            return "MDS";
        case ServerType::SNAPSHOTCLONESERVER:
            return "SnapshotCloneServer";
        default:
            SAFE_LOG_ERROR("Unknown server type");
            return "";
    }
}

int Exec(const string cmd, string* output) {
    PipeGuard pipe(popen(cmd.c_str(), "r"));
    if (pipe == nullptr) {
        SAFE_LOG_ERR_N(
            WATCHDOG_DUP_LOG_FREQUENCY,
            "Failed to open pipe for cmd: " << cmd << ", " << strerror(errno));
        return -1;
    }

    output->clear();
    char buffer[WATCHER_EXEC_BUFFER_LEN];
    while (!feof(pipe)) {
        if (fgets(buffer, WATCHER_EXEC_BUFFER_LEN, pipe) != NULL) {
            if (output->size() + strnlen(buffer, WATCHER_EXEC_BUFFER_LEN) >=
                WATCHER_EXEC_MAX_BUF_LEN)
                break;
            *output += buffer;
        }
    }
    return 0;
}

void FdCleaner(void *arg) {
    int *fd = static_cast<int *>(arg);
    if (*fd >= 0) {
        close(*fd);
        SAFE_LOG_INFO("closefile " << *fd);
        *fd = -1;
    } else {
        SAFE_LOG_INFO("invalidfile " << *fd);
    }

    return;
}

}  // namespace WatchDog
}  // namespace Curve
