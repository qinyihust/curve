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

#include "tools/watchdog/chunkserver_watchdog.h"

#include <glog/logging.h>
#include <mntent.h>
#include <string.h>

#include <memory>
#include <string>

#include "src/chunkserver/uri_paser.h"
#include "tools/watchdog/watcher.h"

namespace Curve {
namespace WatchDog {

using std::string;

string ChunkServerWatchDog::pathToDeviceName(string* path) {
    if (path->empty()) return "";

    /* erase last '/' in path */
    while (path->back() == '/') {
        path->pop_back();
    }

    if (path->empty()) return "";

    /* search mount list */
    struct mntent* m;
    FILE* mountFile = setmntent("/proc/mounts", "r");
    if (!mountFile) {
        SAFE_LOG_ERROR("error read /proc/mounts:" << strerror(errno));
        return "";
    }

    string deviceName;
    while ((m = getmntent(mountFile))) {
        if (strstr(m->mnt_dir, path->c_str())) {
            deviceName = m->mnt_fsname;
            break;
        }
    }
    endmntent(mountFile);

    return deviceName;
}

string ChunkServerWatchDog::getChunkServerstorDir(const int pid,
                                                  char** cmdline) {
    int i = 0;
    while (cmdline[i] != nullptr) {
        if (strstr(cmdline[i], CHUNKSERVER_CMD_DIR_PREFIX)) {
            const string uri = cmdline[i];
            return curve::chunkserver::UriParser::GetPathFromUri(uri);
        }
        i++;
    }

    SAFE_LOG_ERROR("failed to get chunkserver path for pid " << pid);
    return "";
}

int ChunkServerWatchDog::InitServerInfo() {
    std::shared_ptr<ChunkServerInfo> info = std::make_shared<ChunkServerInfo>();
    info->pid = procInfo_.tid;
    info->state = procInfo_.state;
    info->storDir = getChunkServerstorDir(procInfo_.tid, procInfo_.cmdline);
    if (info->storDir.empty()) return -1;

    info->device = pathToDeviceName(&info->storDir);
    if (info->device.empty()) return -1;

    SAFE_LOG_INFO("ChunkServer with pid " << info->pid << " detected, "
                                          << "storDir=" << info->storDir
                                          << ", device=" << info->device);
    serverInfo_ = info;
    return 0;
}

int ChunkServerWatchDog::InitWatchers() {
    std::shared_ptr<ThreadWatcher> threadWatcher =
        std::make_shared<ThreadWatcher>(WatcherName::THREAD_WATCHER, &config_,
                                        serverInfo_);
    watchers_.push_back(threadWatcher);

    std::shared_ptr<DiskSmartWatcher> diskSmartWatcher =
        std::make_shared<DiskSmartWatcher>(WatcherName::DISK_SMART_WATCHER,
                                           &config_, serverInfo_);
    watchers_.push_back(diskSmartWatcher);

    std::shared_ptr<FileOpWatcher> fileOpWatcher =
        std::make_shared<FileOpWatcher>(WatcherName::FILE_OP_WATCHER, &config_,
                                        serverInfo_);
    watchers_.push_back(fileOpWatcher);
}

}  // namespace WatchDog
}  // namespace Curve
